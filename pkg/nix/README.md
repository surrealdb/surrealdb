# Nix package manager

According to [Wikipedia]

> Nix is a cross-platform package manager that utilizes a purely functional deployment model where software is installed into unique directories generated through cryptographic hashes. It is also the name of the tool's programming language. A package's hash takes into account the dependencies, which is claimed to eliminate dependency hell. This package management model advertises more reliable, reproducible, and portable packages.

SurrealDB has support for the Nix package manager. It makes it easier to build the project from source and to set up development environments.

## Table of Contents

- [Running Nix from Docker](#running-nix-from-docker)
  * [Building a Docker image (recommended)](#building-a-docker-image-recommended)
  * [Building a static binary](#building-a-static-binary)
- [Installing Nix](#installing-nix)
  * [Activating support for Nix Flakes (recommended)](#activating-support-for-nix-flakes-recommended)
  * [Setting up a binary cache (optional)](#setting-up-a-binary-cache-optional)
- [Installing SurrealDB](#installing-surrealdb)
- [Setting up a development environment](#setting-up-a-development-environment)
  * [Setting dependencies up automatically](#setting-dependencies-up-automatically)
  * [Manually installing dependencies](#manually-installing-dependencies)
- [Collecting garbage](#collecting-garbage)

## Running Nix from Docker

If all you want is to build a Docker image or a static linux binary and you already have Docker installed, then you can do so without installing anything on your machine.

First, you need to clone this repo and `cd` into it

```
git clone https://github.com/surrealdb/surrealdb.git
cd surrealdb
```

### Building a Docker image (recommended)

A Docker image is recommended because it supports all the features and storage backends that SurrealDB supports. To build a Docker image, run the following command.

```
docker run -it --rm -v $(pwd):/surrealdb -w /surrealdb nixos/nix sh -c "nix-build -A packages.x86_64-linux.docker-image && mkdir docker && cp -vL ./result docker/surreal.tar.gz && rm result"
```

The image will be saved as `docker/surreal.tar.gz`. You can load it using

```
docker load -i docker/surreal.tar.gz
```

### Building a static binary

Please note that currently a static binary is very limited. It doesn't come with any optional features and it only supports the in-memory store, so you can't persist data with it.

To build the static binary, run

```
docker run -it --rm -v $(pwd):/surrealdb -w /surrealdb nixos/nix sh -c "nix-build -A packages.x86_64-linux.static-binary && mkdir bin && cp -v ./result/bin/surreal bin/ && rm result"
```

The binary will be saved as `bin/surreal`.

## Installing Nix

If you want to develop using Nix or you want to build binaries native to your platform then you may need to install Nix. To do so, please follow the official [installation instructions].

### Activating support for Nix Flakes (recommended)

Nix Flakes are an upcoming feature of the Nix package manager. Officially they are still considered experimental and, as such, are not enabled by default. However, they are already widely adopted by the Nix community. SurrealDB supports Nix both with and without support for Flakes.

To enable support for Flakes, edit either `~/.config/nix/nix.conf` or `/etc/nix/nix.conf` and add:

```
experimental-features = nix-command flakes
```

If the Nix installation is in multi-user mode, you will need to restart the `nix-daemon` after this. If your system uses `systemd`, you can do this by simply running `sudo systemctl restart nix-daemon`.

Because of Flakes' superior user experience, we highly recommend them. For brevity, the rest of this guide will assume that flake support is enabled.

### Setting up a binary cache (optional)

Building SurrealDB and all its dependencies can take a while. To speed up the process, you can take advantage of our binary cache. To do so, you simply need to run the following command:

```
nix run nixpkgs#cachix use surrealdb
```

This will download and run the Cachix command to configure your system to use our binary cache.

## Installing SurrealDB

If all you need to do is run SurrealDB without installing it to your `PATH` then you simply need to run

```
nix run github:surrealdb/surrealdb
```

To install it, use

```
nix profile install github:surrealdb/surrealdb
```

You can target a specific branch, tag or commit by appending it to above commands separated by `/`. For example

```
nix run github:surrealdb/surrealdb/v1.0.0
```

If you just want to build the binary, without running it, you can use `nix build` instead of `nix run`. You will then find the binary in `result/bin`.

**NB**: While Nix is cross-platform, currently only building Linux binaries on Linux is supported. The above commands build the default binary, a dynamically linked binary with support for all SurrealDB features. To build a statically linked binary use `nix build github:surrealdb/surrealdb#static-binary`. To build a Docker image use `nix build github:surrealdb/surrealdb#docker-image`. We plan to add support for more platforms in future, in the meantime you can [run Nix from Docker](#running-nix-from-docker) if you would like to build from a different platform.
 
## Setting up a development environment

Nix can be used to set up C/C++ dependencies for this project in order for Cargo commands to work properly.

If you haven't already done so, you will need to clone this repo and `cd` into it

```
git clone https://github.com/surrealdb/surrealdb.git
cd surrealdb
```

### Setting dependencies up automatically

To make Nix setup dependencies automatically when you switch into this project, you need to install and configure `direnv`. To do so, run the following commands:-

```
nix profile install nixpkgs#direnv
nix profile install nixpkgs#nix-direnv
```

and add `eval "$(direnv hook bash)"` to your `~/.bashrc` or similar file.

Finally, from this project's root directory, run

```
direnv allow
```

### Manually installing dependencies

If you choose not to use `direnv` to automatically setup your dependencies as described above, you can use `nix develop` to manually install the dependencies and configure your environment so that `cargo` commands work normally.

Once your environment is set up, you can use normal `cargo` commands.

## Collecting garbage

Because of the way it works, Nix can use a lot of space on your machine. To reclaim some of that space, you can use `nix-collect-garbage -d`.

[Wikipedia]: https://en.wikipedia.org/wiki/Nix_(package_manager)
[installation instructions]: https://nixos.org/download.html#nix-install-linux
