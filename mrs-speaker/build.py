import re
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


def build_android_binary():
    if not cargo_install("cargo-ndk", "4.1.2"):
        print("Failed to install cargo-ndk v4.1.2")
        return False
    if not rustup_target_install("aarch64-linux-android"):
        print("Failed to install aarch64-linux-android target.")
        return False
    platform = "35"
    abi = "arm64-v8a"
    env = os.environ.copy()
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
            ],
            check=True,
            env=env,
        )
    except subprocess.CalledProcessError as e:
        print(f"Failed to build: {e}")
        return False
    return True


def main():
    build_android_binary()


if __name__ == "__main__":
    main()
