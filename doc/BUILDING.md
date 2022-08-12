# Building SurrealDB

This file contains a set of instructions for building SurrealDB on a number of different platforms. Currently, SurrealDB is built for release automatically in a [Github Actions](https://github.com/surrealdb/surrealdb/actions) continuous-integration environment, on macOS, Ubuntu, and Windows.

## Building on macOS

<details><summary>Click to show details</summary>
	
### ✅ Compile for macOS
```bash
# Setup
brew install cmake
rustup target add x86_64-apple-darwin
rustup target add aarch64-apple-darwin
# Compile for x86_64-apple-darwin
cargo build --release --locked --target x86_64-apple-darwin
# Compile for aarch64-apple-darwin
cargo build --release --locked --target aarch64-apple-darwin
```

### ❌ Cross-compile for Linux
<sub>This does not yet build successfully</sub>
```bash
# Setup
brew install cmake
brew tap messense/macos-cross-toolchains
brew install x86_64-unknown-linux-gnu
brew install aarch64-unknown-linux-gnu
rustup target add x86_64-unknown-linux-gnu
rustup target add aarch64-unknown-linux-gnu
# Compile for x86_64-unknown-linux-gnu
export CC_x86_64_unknown_linux_gnu=x86_64-unknown-linux-gnu-gcc
export CXX_x86_64_unknown_linux_gnu=x86_64-unknown-linux-gnu-g++
export AR_x86_64_unknown_linux_gnu=x86_64-unknown-linux-gnu-ar
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-unknown-linux-gnu-gcc
cargo build --release --locked --target x86_64-unknown-linux-gnu
# Compile for aarch64-unknown-linux-gnu
export CC_aarch64_unknown_linux_gnu=aarch64-unknown-linux-gnu-gcc
export CXX_aarch64_unknown_linux_gnu=aarch64-unknown-linux-gnu-g++
export AR_aarch64_unknown_linux_gnu=aarch64-unknown-linux-gnu-ar
export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-unknown-linux-gnu-gcc
cargo build --release --locked --target aarch64-unknown-linux-gnu
```

### ❌ Cross-compile for Windows
<sub>This does not yet build successfully</sub>
```bash
# Setup
brew install cmake mingw-w64
rustup target add x86_64-pc-windows-gnu
# Compile for x86_64-w64-mingw32-gcc
export CC_x86_64_pc_windows_gnu=x86_64-w64-mingw32-gcc
export CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER=x86_64-w64-mingw32-gcc
cargo build --release --locked --target x86_64-pc-windows-gnu
```

### ❌ Cross-compile for Raspberry Pi
<sub>This does not yet build successfully</sub>
```bash
# Setup
brew install cmake arm-linux-gnueabihf-binutils
rustup target add armv7-unknown-linux-musleabihf
# Compile for armv7-unknown-linux-musleabihf
cargo build --target armv7-unknown-linux-musleabihf
```

### ❌ Cross-compile for Linux (Musl)
<sub>This does not yet build successfully</sub>
```bash
docker pull clux/muslrust:stable
docker run --pull --rm -v $PWD:/volume -t clux/muslrust:stable cargo build --release --target x86_64-unknown-linux-musl
```
	
</details>

## Building on Ubuntu 20.04

<details><summary>Click to show details</summary>

### ✅ Compile for Linux
```bash
# Setup
sudo apt-get -y update
sudo apt-get -y install llvm cmake clang-11 binutils
sudo apt-get -y install musl-tools qemu-user libc6-dev-arm64-cross
sudo apt-get -y install g++-aarch64-linux-gnu gcc-aarch64-linux-gnu
rustup target add x86_64-unknown-linux-gnu
rustup target add aarch64-unknown-linux-gnu
# Compile for x86_64-unknown-linux-gnu
cargo build --release --locked --target x86_64-unknown-linux-gnu
# Compile for x86_64-unknown-linux-gnu
cargo build --release --locked --target aarch64-unknown-linux-gnu
```

### ❌ Cross-compile for Windows
<sub>This does not yet build successfully</sub>
```bash
# Setup
sudo apt-get -y update
sudo apt-get -y install llvm cmake clang-11 binutils mingw-w64
rustup target add x86_64-pc-windows-gnu
# Compile for x86_64-pc-windows-gnu
export CC_x86_64_pc_windows_gnu=x86_64-w64-mingw32-gcc
export CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER=x86_64-w64-mingw32-gcc
cargo build --release --locked --target x86_64-pc-windows-gnu
```

### ❌ Cross-compile for Raspberry Pi
<sub>This does not yet build successfully</sub>
```bash
# Setup
sudo apt-get -y update
sudo apt-get -y install llvm cmake clang-11 binutils
sudo apt-get -y install -y g++-arm-linux-gnueabihf gcc-arm-linux-gnueabihf
rustup target add armv7-unknown-linux-gnueabihf
# Compile for armv7-unknown-linux-musleabihf
cargo build --release --locked --target armv7-unknown-linux-musleabihf
```

### ❌ Cross-compile for Linux (Musl)
<sub>This does not yet build successfully</sub>
```bash
docker pull clux/muslrust:stable
docker run --pull --rm -v $PWD:/volume -t clux/muslrust:stable cargo build --release --target x86_64-unknown-linux-musl
```

</details>
