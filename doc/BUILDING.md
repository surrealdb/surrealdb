# Building SurrealDB

This file contains a set of instructions for building SurrealDB on a number of different platforms. Currently, SurrealDB is built for release automatically in a [GitHub Actions](https://github.com/surrealdb/surrealdb/actions) continuous-integration environment, on macOS, Ubuntu, and Windows.

While installing `rustup`, use the default (`stable`) release channel of Rust for best results. If you already have a different release channel, you can run `rustup override set stable` from within the top level directory of this repository.

<!-- -------------------------------------------------- -->
<!-- -------------------------------------------------- -->
<!-- -------------------------------------------------- -->
<!-- -------------------------------------------------- -->
<!-- -------------------------------------------------- -->

## Building on macOS (arm64)

<details><summary>Click to show details</summary>
	
### ✅ Compile for `apple-darwin` (macOS)
```bash
# Setup
brew install cmake protobuf
rustup target add x86_64-apple-darwin
rustup target add aarch64-apple-darwin
# Compile for x86_64-apple-darwin
cargo build --release --locked --target x86_64-apple-darwin
# Compile for aarch64-apple-darwin
cargo build --release --locked --target aarch64-apple-darwin
```

### ✅ Compile for `aarch64-unknown-linux-gnu` (Linux)

```bash
# Run Docker
docker run -it --platform linux/arm64 -v $PWD:/code ubuntu
# Setup
apt-get -y update
apt-get -y install \
	curl \
	llvm \
	cmake \
	binutils \
	clang-11 \
	qemu-user \
	musl-tools \
	libssl-dev \
	pkg-config \
	build-essential \
	protobuf-compiler
# Install rustlang and cargo
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
# Add extra targets for rust
rustup target add aarch64-unknown-linux-gnu
# Compile for aarch64-unknown-linux-gnu
cargo build --release --locked --target aarch64-unknown-linux-gnu
```

### ✅ Compile for `x86_64-unknown-linux-gnu` (Linux)

```bash
# Run Docker
docker run -it --platform linux/amd64 -v $PWD:/code ubuntu
# Setup
apt-get -y update
apt-get -y install \
	curl \
	llvm \
	cmake \
	binutils \
	clang-11 \
	qemu-user \
	musl-tools \
	libssl-dev \
	pkg-config \
	build-essential \
	protobuf-compiler
# Install rustlang and cargo
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
# Add extra targets for rust
rustup target add x86_64-unknown-linux-gnu
# Compile for x86_64-unknown-linux-gnu
cargo build --release --locked --target x86_64-unknown-linux-gnu
```

### ❌ Cross-compile for `x86_64-pc-windows-gnu` (Windows)

<sub>This does not yet build successfully</sub>

```bash
# Setup
brew install cmake mingw-w64 protobuf
rustup target add x86_64-pc-windows-gnu
# Compile for x86_64-w64-mingw32-gcc
export CC_x86_64_pc_windows_gnu=x86_64-w64-mingw32-gcc
export CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER=x86_64-w64-mingw32-gcc
cargo build --release --locked --target x86_64-pc-windows-gnu
```

### ❌ Cross-compile for `x86_64-unknown-linux-musl` (Linux Musl)

<sub>This does not yet build successfully</sub>

```bash
docker pull clux/muslrust:stable
docker run --pull --rm -v $PWD:/volume -t clux/muslrust:stable cargo build --release --target x86_64-unknown-linux-musl
```

</details>

<!-- -------------------------------------------------- -->
<!-- -------------------------------------------------- -->
<!-- -------------------------------------------------- -->
<!-- -------------------------------------------------- -->
<!-- -------------------------------------------------- -->

## Building on Ubuntu 20.04 (arm64)

<details><summary>Click to show details</summary>

### ✅ Compile for `aarch64-unknown-linux-gnu` (Linux)

```bash
# Setup
apt-get -y update
apt-get -y install \
	curl \
	llvm \
	cmake \
	binutils \
	clang-11 \
	qemu-user \
	musl-tools \
	libssl-dev \
	pkg-config \
	build-essential \
	protobuf-compiler
# Install rustlang and cargo
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
# Add extra targets for rust
rustup target add aarch64-unknown-linux-gnu
# Compile for aarch64-unknown-linux-gnu
cargo build --release --locked --target aarch64-unknown-linux-gnu
```

### ✅ Compile for `x86_64-unknown-linux-gnu` (Linux)

```bash
# Setup
apt-get -y update
apt-get -y install \
	curl \
	llvm \
	cmake \
	binutils \
	clang-11 \
	qemu-user \
	musl-tools \
	libssl-dev \
	pkg-config \
	build-essential \
	protobuf-compiler \
	libc6-dev-amd64-cross \
	crossbuild-essential-amd64
# Install rustlang and cargo
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
# Add extra targets for rust
rustup target add x86_64-unknown-linux-gnu
# Compile for x86_64-unknown-linux-gnu
cargo build --release --locked --target x86_64-unknown-linux-gnu
```

### ❌ Cross-compile for `x86_64-pc-windows-gnu` (Windows)

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

### ❌ Cross-compile for `armv7-unknown-linux-musleabihf` (Raspberry Pi)

<sub>This does not yet build successfully</sub>

```bash
# Setup
apt-get -y update
apt-get -y install \
	curl \
	llvm \
	cmake \
	binutils \
	clang-11 \
	qemu-user \
	musl-tools \
	libssl-dev \
	pkg-config \
	build-essential \
	g++-arm-linux-gnueabihf \
	gcc-arm-linux-gnueabihf
# Install rustlang and cargo
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
# Add extra targets for rust
rustup target add armv7-unknown-linux-musleabihf
# Compile for x86_64-unknown-linux-gnu
cargo build --release --locked --target armv7-unknown-linux-musleabihf
```

### ❌ Cross-compile for `x86_64-unknown-linux-musl` (Linux Musl)

<sub>This does not yet build successfully</sub>

```bash
docker pull clux/muslrust:stable
docker run --pull --rm -v $PWD:/volume -t clux/muslrust:stable cargo build --release --target x86_64-unknown-linux-musl
```

</details>

<!-- -------------------------------------------------- -->
<!-- -------------------------------------------------- -->
<!-- -------------------------------------------------- -->
<!-- -------------------------------------------------- -->
<!-- -------------------------------------------------- -->

## Building on Ubuntu 20.04 (amd64)

<details><summary>Click to show details</summary>

### ✅ Compile for `x86_64-unknown-linux-gnu` (Linux)

```bash
# Setup
apt-get -y update
apt-get -y install \
	curl \
	llvm \
	cmake \
	binutils \
	clang-11 \
	qemu-user \
	musl-tools \
	libssl-dev \
	pkg-config \
	build-essential \
	protobuf-compiler
# Install rustlang and cargo
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
# Add extra targets for rust
rustup target add x86_64-unknown-linux-gnu
# Compile for x86_64-unknown-linux-gnu
cargo build --release --locked --target x86_64-unknown-linux-gnu
```

### ✅ Compile for `aarch64-unknown-linux-gnu` (Linux)

```bash
# Setup
apt-get -y update
apt-get -y install \
	curl \
	llvm \
	cmake \
	binutils \
	clang-11 \
	qemu-user \
	musl-tools \
	libssl-dev \
	pkg-config \
	build-essential \
	protobuf-compiler \
	libc6-dev-arm64-cross \
	crossbuild-essential-arm64
# Install rustlang and cargo
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
# Add extra targets for rust
rustup target add aarch64-unknown-linux-gnu
# Compile for x86_64-unknown-linux-gnu
cargo build --release --locked --target aarch64-unknown-linux-gnu
```

### ❌ Cross-compile for `x86_64-pc-windows-gnu` (Windows)

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

### ❌ Cross-compile for `armv7-unknown-linux-musleabihf` (Raspberry Pi)

<sub>This does not yet build successfully</sub>

```bash
# Setup
apt-get -y update
apt-get -y install \
	curl \
	llvm \
	cmake \
	binutils \
	clang-11 \
	qemu-user \
	musl-tools \
	libssl-dev \
	pkg-config \
	build-essential \
	protobuf-compiler \
	g++-arm-linux-gnueabihf \
	gcc-arm-linux-gnueabihf
# Install rustlang and cargo
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
# Add extra targets for rust
rustup target add armv7-unknown-linux-musleabihf
# Compile for x86_64-unknown-linux-gnu
cargo build --release --locked --target armv7-unknown-linux-musleabihf
```

### ❌ Cross-compile for `x86_64-unknown-linux-musl` (Linux Musl)

<sub>This does not yet build successfully</sub>

```bash
docker pull clux/muslrust:stable
docker run --pull --rm -v $PWD:/volume -t clux/muslrust:stable cargo build --release --target x86_64-unknown-linux-musl
```

</details>

<!-- -------------------------------------------------- -->
<!-- -------------------------------------------------- -->
<!-- -------------------------------------------------- -->
<!-- -------------------------------------------------- -->
<!-- -------------------------------------------------- -->

## Building on Debian 13 Trixie (amd64)

<details><summary>Click to show details</summary>

### ✅ Compile for `x86_64-unknown-linux-gnu` (Linux)

```bash
# Setup
sudo apt-get -y update
sudo apt-get -y install \
	curl \
	llvm \
	cmake \
	binutils \
	clang \
	qemu-user \
	musl-tools \
	libssl-dev \
	pkg-config \
	build-essential \
	protobuf-compiler
# Install rustlang and cargo
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
# Add extra targets for rust
rustup target add x86_64-unknown-linux-gnu
# Compile for x86_64-unknown-linux-gnu
cargo build --release --locked --target x86_64-unknown-linux-gnu
```

</details>

## Building on Windows-amd64 (Windows OS)

<details><summary>Click to show details</summary>

### ✅ Compile for `windows-amd64`

This repository builds on Windows using the **MSVC** toolchain (`x86_64-pc-windows-msvc`).

`rustup` installs and manages the Rust toolchain, including both `rustc` and `cargo`.

Install prerequisites:

1. **Visual Studio Build Tools 2022**

    Install the **Desktop development with C++** workload (includes MSVC and Windows SDK).
    This provides `cl.exe`, linker tools, headers, and libraries required by Rust crates
    that compile native code on Windows.

    > Note: this is **not** installed by the helper script (or Scoop here). Install it separately from Microsoft.

2. **Rust toolchain**

    Install Rust via `rustup` (stable channel).

3. **LLVM (with libclang)**

    Needed by `bindgen` (used by RocksDB sys crates such as `surrealdb-librocksdb-sys`).

4. **NASM**

    Needed by `aws-lc-sys` on Windows (used via Rust TLS dependencies).

5. **CMake**

    Needed by native dependencies (for example `aws-lc-sys` C/C++ build steps).

Optional (Scoop):

```powershell
scoop install rustup llvm nasm cmake
```

Recommended Windows workflows:

1. **Manual install path**
    - Install prerequisites yourself (including Visual Studio Build Tools)

2. **Bootstrap path**
    - Install Visual Studio Build Tools
    - Let the script install Scoop + missing user-space tools (`rustup`, `llvm`, `nasm`, `cmake`):

    ```powershell
    pwsh -File .\dev\windows\windows-setup-build-env.ps1 -InstallMissingTools
    ```

After setup, build from your terminal:

```powershell
cargo build --release
```

The helper script:

- ensures Rust stable toolchain is available via `rustup`,
- ensures `LLVM/libclang`, `NASM`, and `CMake` are available,
- installs missing user-space tools automatically

</details>

## Building using the Nix package manager

SurrealDB can be built [using the Nix package manager](../pkg/nix#nix-package-manager).
