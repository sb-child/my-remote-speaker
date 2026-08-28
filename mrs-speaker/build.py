import base64
import json
import re
import shlex
import shutil
import subprocess
import os
import struct
import hashlib
from pathlib import Path
from packaging.version import Version


script_dir = Path(__file__).parent.resolve()

build_android_binary_dest_dir = (
    script_dir / "build" / "mrs-speaker-android-bin"
).resolve()
build_android_library_dest_dir = (
    script_dir / "build" / "mrs-speaker-android-lib"
).resolve()
build_android_entrypoint_jar_dest_dir = (
    script_dir / "build" / "mrs-speaker-android-jar"
).resolve()
build_android_magisk_dest_dir = (
    script_dir / "build" / "mrs-speaker-android-magisk"
).resolve()
build_temp = (script_dir / "build" / "temp").resolve()


def init_build_dir():
    for d in [
        build_android_binary_dest_dir,
        build_android_library_dest_dir,
        build_android_entrypoint_jar_dest_dir,
        build_android_magisk_dest_dir,
        build_temp,
    ]:
        d.mkdir(exist_ok=True, parents=True)
    for i in build_temp.iterdir():
        if i.is_dir() and not i.is_symlink():
            shutil.rmtree(i)
        else:
            i.unlink()
    print("Build dir created.")


EMBED_MAGIC = b"MRS-Data"
EMBED_LEN_FMT = "<Q"
EMBED_TRAILER_LEN = struct.calcsize(EMBED_LEN_FMT) + len(EMBED_MAGIC)


def split_bin(exe: Path):
    with open(exe, "rb") as f:
        d = f.read()
    if len(d) >= EMBED_TRAILER_LEN and d[-len(EMBED_MAGIC) :] == EMBED_MAGIC:
        n = struct.unpack(EMBED_LEN_FMT, d[-EMBED_TRAILER_LEN : -len(EMBED_MAGIC)])[0]
        if n + EMBED_TRAILER_LEN <= len(d):
            return d[: len(d) - EMBED_TRAILER_LEN - n], d[
                len(d) - EMBED_TRAILER_LEN - n : len(d) - EMBED_TRAILER_LEN
            ]
    return d, b""


def embed_bin(exe: Path, payload: bytes):
    base, split = split_bin(exe)
    print(
        f"base {len(base)} bytes, split {len(split)} bytes, replace {len(payload)} bytes"
    )
    with open(exe, "wb") as f:
        f.write(base)
        if payload:
            f.write(payload)
            f.write(struct.pack(EMBED_LEN_FMT, len(payload)))
            f.write(EMBED_MAGIC)


def _ensure_cargo_update_installed() -> bool:
    try:
        r = subprocess.run(
            ["cargo", "install-update", "-V"],
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        print(f"Command failed with exit code {e.returncode}:\n{e.stderr}")
        print(
            "\nEnsure you have installed 'cargo-update' crate by `cargo install cargo-update`."
        )
        return False
    print(f"{r.stdout}")
    return True


def _cargo_install_crate(crate_name: str, upgrade: bool):
    if upgrade:
        cmd = ["cargo", "install", crate_name]
    else:
        if not _ensure_cargo_update_installed():
            return False
        cmd = ["cargo", "install-update", crate_name]
    try:
        _r = subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Command failed with exit code {e.returncode}:\n{e.stderr}")
        return False
    return True


def cargo_install(crate_name: str, min_version: str):
    try:
        result = subprocess.run(
            ["cargo", "install", "--list"],
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        print(f"Failed to run 'cargo install --list':\n{e.stderr}")
        return False
    installed_version = None
    pattern = re.compile(
        rf"^{re.escape(crate_name)}\s+v([0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.]+)?):",
        re.MULTILINE,
    )
    match = pattern.search(result.stdout)
    if match:
        installed_version = match.group(1)
    if installed_version is None:
        print(f"Crate '{crate_name}' is not installed. Installing...")
        return _cargo_install_crate(crate_name, upgrade=False)
    print(
        f"Crate '{crate_name}' is installed (version: {installed_version}). Required: {min_version}."
    )
    if Version(installed_version) < Version(min_version):
        print(
            f"Installed version {installed_version} is lower than required {min_version}. Updating..."
        )
        return _cargo_install_crate(crate_name, upgrade=True)
    print(
        f"Crate '{crate_name}' version {installed_version} satisfies the required minimum version {min_version}."
    )
    return True


def rustup_target_install(target_name: str):
    try:
        _r = subprocess.run(
            ["rustup", "target", "install", target_name],
            check=True,
        )
    except subprocess.CalledProcessError as e:
        print(f"Failed to install target '{target_name}': {e}")
        return False
    return True


def get_ndk_env(target: str) -> dict[str, str]:
    cmd = ["cargo", "ndk-env", "--target", target]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        output = result.stdout
    except (subprocess.SubprocessError, FileNotFoundError) as e:
        raise RuntimeError(f"Failed to execute cargo ndk-env: {e}") from e
    env_vars: dict[str, str] = {}
    for line in output.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or not line.startswith("export "):
            continue
        kv_pair = line[len("export ") :].strip()
        if "=" in kv_pair:
            key, raw_value = kv_pair.split("=", 1)
            key = key.strip()
            try:
                parsed_tokens = shlex.split(raw_value)
                value = parsed_tokens[0] if parsed_tokens else ""
            except ValueError:
                value = raw_value.strip("\"'")
            env_vars[key] = value
    return env_vars


def get_android_ndk_home(target: str) -> str:
    env_vars = get_ndk_env(target)
    path_candidates = [
        env_vars.get("CARGO_NDK_SYSROOT_PATH"),
        env_vars.get("CLANG_PATH"),
        env_vars.get("CARGO_NDK_SYSROOT_LIBS_PATH"),
    ]
    for path_str in path_candidates:
        if not path_str:
            continue
        if "/toolchains/" in path_str:
            ndk_root = path_str.partition("/toolchains/")[0]
            return ndk_root.rstrip("/") + "/"
    for val in env_vars.values():
        if val and "/toolchains/" in val:
            ndk_root = val.partition("/toolchains/")[0]
            return ndk_root.rstrip("/") + "/"
    raise ValueError(f"Failed to find android ndk home for target '{target}'")


def build_android_java_entrypoint():
    platform = "35"
    java_version = "8"
    build_tools_version = f"{platform}.0.0"
    env = os.environ.copy()
    android_home = env.get("ANDROID_HOME")
    if android_home is None:
        raise FileNotFoundError("Failed to find ANDROID_HOME.")
    bootclass = Path(android_home) / "platforms" / f"android-{platform}" / "android.jar"
    code_dir = script_dir / "java-entrypoint"
    main_file = code_dir / "Main.java"
    output_dir = code_dir / "out"
    output_dir.mkdir(exist_ok=True)
    cmd_header = [
        "javac",
        "-source",
        java_version,
        "-target",
        java_version,
        "-Xlint:-options",
    ]
    cmd_bootclass = ["-bootclasspath", str(bootclass.resolve())]
    cmd_files = ["-d", str(output_dir.resolve()), str(main_file.resolve())]
    try:
        _r = subprocess.run(
            cmd_header + cmd_bootclass + cmd_files,
            check=True,
            env=env,
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to build java entrypoint: {e}")
    cmd_executable = [
        (Path(android_home) / "build-tools" / build_tools_version / "d8").resolve()
    ]
    cmd_files = [
        (
            output_dir / "com" / "sbchild" / "mrs_speaker_android" / "Main.class"
        ).resolve(),
        "--output",
        (output_dir / "mrs_speaker_dex.jar").resolve(),
    ]
    try:
        _r = subprocess.run(
            cmd_executable + cmd_files,
            check=True,
            env=env,
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to convert entrypoint to jar: {e}")
    res = (output_dir / "mrs_speaker_dex.jar").resolve()
    if not res.is_file():
        raise FileNotFoundError(f"{res} file not found.")
    return res


def build_android_library():
    if not cargo_install("cargo-ndk", "4.1.2"):
        raise RuntimeError("Failed to install cargo-ndk v4.1.2")
    if not rustup_target_install("aarch64-linux-android"):
        raise RuntimeError("Failed to install aarch64-linux-android target.")
    platform = "35"
    abi = "arm64-v8a"
    env = os.environ.copy() | {
        # https://github.com/DoumanAsh/opusic-sys#android-build
        "ANDROID_NDK_HOME": get_android_ndk_home(abi)
    }
    try:
        _r = subprocess.run(
            [
                "cargo",
                "ndk",
                "--platform",
                platform,
                "-t",
                abi,
                "build",
                "--lib",
                "--release",
                "--no-default-features",
                "--features",
                "android",
            ],
            check=True,
            env=env,
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to build mrs-speaker library: {e}")
    res = (
        script_dir
        / ".."
        / "target"
        / "aarch64-linux-android"
        / "release"
        / "libmrs_speaker.so"
    ).resolve()
    if not res.is_file():
        raise FileNotFoundError(f"{res} file not found.")
    return res


def build_android_binary():
    if not cargo_install("cargo-ndk", "4.1.2"):
        raise RuntimeError("Failed to install cargo-ndk v4.1.2")
    if not rustup_target_install("aarch64-linux-android"):
        raise RuntimeError("Failed to install aarch64-linux-android target.")
    platform = "35"
    abi = "arm64-v8a"
    env = os.environ.copy() | {
        # https://github.com/DoumanAsh/opusic-sys#android-build
        "ANDROID_NDK_HOME": get_android_ndk_home(abi)
    }
    try:
        _r = subprocess.run(
            [
                "cargo",
                "ndk",
                "--platform",
                platform,
                "-t",
                abi,
                "build",
                "--bin",
                "mrs-speaker-android",
                "--release",
                "--no-default-features",
                "--features",
                "android",
            ],
            check=True,
            env=env,
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to build mrs-speaker-android binary: {e}")
    res = (
        script_dir
        / ".."
        / "target"
        / "aarch64-linux-android"
        / "release"
        / "mrs-speaker-android"
    ).resolve()
    if not res.is_file():
        raise FileNotFoundError(f"{res} file not found.")
    return res


def main():
    init_build_dir()
    ep = build_android_java_entrypoint()
    lib_file = build_android_library()
    bin_file = build_android_binary()
    embed_pack = {}
    with open(ep, "rb") as f:
        digest = hashlib.file_digest(f, "sha256")
        f.seek(0)
        embed_pack["jar_digest"] = base64.b64encode(digest.digest()).decode()
        embed_pack["jar_data"] = base64.b64encode(f.read()).decode()
    with open(lib_file, "rb") as f:
        digest = hashlib.file_digest(f, "sha256")
        f.seek(0)
        embed_pack["lib_digest"] = base64.b64encode(digest.digest()).decode()
        embed_pack["lib_data"] = base64.b64encode(f.read()).decode()
    embed_pack_json = json.dumps(embed_pack)
    del embed_pack
    bin_temp = build_temp / "mrs-speaker-android.temp"
    with open(bin_file, "rb") as f:
        with open(bin_temp, "wb") as t:
            t.write(f.read())
    embed_bin(bin_temp, embed_pack_json.encode())
    del embed_pack_json
    bin_release = build_android_binary_dest_dir / "mrs-speaker-android"
    with open(bin_temp, "rb") as f:
        with open(bin_release, "wb") as t:
            t.write(f.read())
    bin_release.chmod(0o755)


if __name__ == "__main__":
    main()
