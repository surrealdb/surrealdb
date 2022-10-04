# Building SurrealDB

This file contains a set of instructions for building SurrealDB on a number of different platforms. Currently, SurrealDB is built for release automatically in a [Github Actions](https://github.com/surrealdb/surrealdb/actions) continuous-integration environment, on macOS, Ubuntu, and Windows.

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
brew install cmake
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
	build-essential
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
	build-essential
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
brew install cmake mingw-w64
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
	build-essential
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
	build-essential
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


## Building on Windows-amd64 (Windows OS)

<details><summary>Click to show details</summary>

###  ✅ Compile for `windows-amd64`

> Compiling SurrealDB with windows OS requires **Administrator** priveledge!

⚠**Tested on Windows 10 build 22H2(19044.2006)**

1. Install LLVM with Clang 👉 [Download Here](https://github.com/llvm/llvm-project/releases) *look for something end with `amd64.exe`*
2. Install `MYSYS2` 👉 Follow instructions on [their website](https://www.msys2.org/)
3. Add Symlinks for llvm

   ```powershell
   New-Item -Path "C:\Program Files\LLVM\x86_64-w64-mingw32" -ItemType SymbolicLink -Value "C:\msys64\mingw64\x86_64-w64-mingw32"
   New-Item -Path "C:\Program Files\LLVM\i686-w64-mingw32" -ItemType SymbolicLink -Value "C:\msys64\mingw64\i686-w64-mingw32"
    ```

4. Add GCC binary path to environment

    ```powershell
    $PATH += "C:\msys64\mingw64\bin"
    $PATH += "C:\msys64\mingw32\bin"
    $CC_x86_64_pc_windows_gnu = "x86_64-w64-mingw32-gcc"
    $CC_i686_pc_windows_gnu = "i686-w64-mingw32-gcc"
    $HOST_CC = "x86_64-w64-mingw32-gcc"
    ```

5. Install `patch` GNU Util
    Go to GNUWin32 page for [*patch*](http://gnuwin32.sourceforge.net/packages/patch.htm) and
    install the [*patch*](http://gnuwin32.sourceforge.net/downlinks/patch-bin-zip.php)
    binaries.

	> For some bizzare reasons, **patch.exe needs elevated priviledge** to be invoked during
	> compilation

    Add directory containing the `patch.exe` to your PATH

6. Run cargo in an **elevated ⚠** shell

    Running `cargo build` in an **elevated shell** will now build the `SurrealDB` in Windows OS.
</details>

## Building using the Nix package manager

SurrealDB can be built [using the Nix package manager](../pkg/nix#nix-package-manager).
