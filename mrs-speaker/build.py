import re
import shlex
import subprocess
import os
from dataclasses import dataclass
from pathlib import Path
from packaging.version import Version

script_dir = Path(__file__).parent.resolve()


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


@dataclass
class BuildAndroidBinaryOptions:
    pass


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
    java_version = "1.8"
    build_tools_version = f"{platform}.0.0"
    env = os.environ.copy()
    android_home = env.get("ANDROID_HOME")
    if android_home is None:
        print("Failed to find ANDROID_HOME.")
        return False
    bootclass = Path(android_home) / "platforms" / f"android-{platform}" / "android.jar"
    code_dir = script_dir / "java-entrypoint"
    main_file = code_dir / "Main.java"
    output_dir = code_dir / "out"
    output_dir.mkdir(exist_ok=True)
    cmd_header = ["javac", "-source", java_version, "-target", java_version]
    cmd_bootclass = ["-bootclasspath", str(bootclass.resolve())]
    cmd_files = ["-d", str(output_dir.resolve()), str(main_file.resolve())]
    try:
        _r = subprocess.run(
            cmd_header + cmd_bootclass + cmd_files,
            check=True,
            env=env,
        )
    except subprocess.CalledProcessError as e:
        print(f"Failed to build java entrypoint: {e}")
        return False
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
        print(f"Failed to convert entrypoint to jar: {e}")
        return False
    res = (output_dir / "mrs_speaker_dex.jar").resolve()
    if not res.is_file():
        print(f"{res} file not found.")
        return False
    return res


def build_android_library():
    if not cargo_install("cargo-ndk", "4.1.2"):
        print("Failed to install cargo-ndk v4.1.2")
        return False
    if not rustup_target_install("aarch64-linux-android"):
        print("Failed to install aarch64-linux-android target.")
        return False
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
        print(f"Failed to build mrs-speaker library: {e}")
        return False
    res = (
        script_dir
        / ".."
        / "target"
        / "aarch64-linux-android"
        / "release"
        / "libmrs_speaker.so"
    ).resolve()
    if not res.is_file():
        print(f"{res} file not found.")
        return False
    return res


def build_android_binary():
    if not cargo_install("cargo-ndk", "4.1.2"):
        print("Failed to install cargo-ndk v4.1.2")
        return False
    if not rustup_target_install("aarch64-linux-android"):
        print("Failed to install aarch64-linux-android target.")
        return False
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
        print(f"Failed to build mrs-speaker-android binary: {e}")
        return False
    res = (
        script_dir
        / ".."
        / "target"
        / "aarch64-linux-android"
        / "release"
        / "mrs-speaker-android"
    ).resolve()
    if not res.is_file():
        print(f"{res} file not found.")
        return False
    return res


def main():
    build_android_java_entrypoint()
    # build_android_binary()


if __name__ == "__main__":
    main()
