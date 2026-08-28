# mrs-speaker

## Build

```sh
# install ninja
sudo dnf install ninja-build -y
# android sdks
android sdk install platforms/android-35 build-tools/35.0.0 ndk/30.0.15729638\

# build for android
python3 build.py
ls build/mrs-speaker-android-bin/mrs-speaker-android
```

## Run on Android

**Termux**:

Known issue: Android imposes restrictions on the use of raw sockets.

```sh
# Place the executable file in the Termux home directory and set the permissions.
mv storage/downloads/mrs-speaker-android .
chmod +x mrs-speaker-android

# Start it. Isolate paths to avoid permission issues.
./mrs-speaker-android daemon --conf-path mrs-speaker-termux-conf --temp-path mrs-speaker-termux-temp
```

**Termux with Shizuku**:

```sh
# Place the executable file in the Termux home directory and set the permissions. See above.

# into shizuku shell
sh rish

# Start it. Isolate paths to avoid permission issues.
./mrs-speaker-android daemon --conf-path mrs-speaker-termux-shizuku-conf --temp-path mrs-speaker-termux-shizuku-temp
```

**ADB**:

```sh
# Place the executable file in /data/local/tmp, and set the permissions.
cd /data/local/tmp
chmod +x mrs-speaker-android

# Start it. Isolate paths to avoid permission issues.
./mrs-speaker-android daemon --conf-path mrs-speaker-adb-conf --temp-path mrs-speaker-adb-temp
```
