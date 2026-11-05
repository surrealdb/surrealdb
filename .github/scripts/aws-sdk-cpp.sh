#!/usr/bin/env bash
# The AWS C++ SDK that the downstream's RocksDB cloud build links: the
# rust-rocksdb fork's build.rs (`cloud` + `aws` features) compiles against
# <prefix>/include and links the static libraries under <prefix>/lib or
# <prefix>/lib64, reading the prefix from AWS_SDK_INSTALL_DIR. The SDK is
# immutable for a version and architecture, so aws-sdk-artifact.yml builds it
# once per architecture and publishes the install prefix as an OCI artifact;
# consumers pull that artifact and never compile the SDK themselves.
#
# This file is the one place the pin lives: the version, the build
# configuration, the layout every side verifies, and the artifact's name. A
# consumer and the publisher agree because both run this script.
#
#   version          Prints the pinned SDK version.
#   revision         Prints the build revision.
#   arch             Prints this host's architecture as the artifact names it
#                    (amd64 or arm64).
#   tag [arch]       Prints the artifact's tag:
#                    aws-sdk-<version>-r<revision>-<arch>.
#   file [arch]      Prints the artifact's file name (the OCI layer's title).
#   build <prefix> [workdir]
#                    Clones the pinned tag into <workdir> (a fresh temporary
#                    directory by default), builds it and installs it at
#                    <prefix>. Needs git, cmake, make and a C++ toolchain,
#                    plus the SDK core's link dependencies (libcurl, OpenSSL,
#                    zlib).
#   verify <prefix>  Exits 1 with the gaps named unless <prefix> holds what
#                    build.rs links.
#   pack <prefix> <tar.gz>
#                    Writes the contents of <prefix> as a reproducible tarball
#                    (sorted, zero mtime, numeric root ownership), so the same
#                    prefix always packs to the same bytes.
#   unpack <tar.gz> <prefix>
#                    Extracts a tarball written by pack into <prefix>, which
#                    must not already exist.
set -euo pipefail

# The pinned aws-sdk-cpp release tag and the build revision. Together they
# name the artifact: aws-sdk-<version>-r<revision>-<arch>. Bumping either
# changes the tag, which the publisher creates on the next push to main (or
# dispatch) and every consumer then expects: the publish must succeed before
# a consumer can pass.
#
# The revision exists because a tag is written once. Any change to what the
# artifact contains that is not a version bump (a cmake flag, the component
# list, the clone or pack options, the verified layout) must bump the
# revision, or the publisher finds the old tag published, skips, and every
# consumer keeps the old build with every signal green. Reset it to 1 when
# the version changes.
#
# The build configuration below is what the fork's build.rs was written
# against: only the s3, transfer and core components, static,
# position-independent so they link into a cdylib or PIE binary.
AWS_SDK_CPP_VERSION="1.11.845"
AWS_SDK_CPP_REVISION="1"
AWS_SDK_CPP_REPO="https://github.com/aws/aws-sdk-cpp.git"
# The temporary work directory build creates when given none (see build).
BUILD_TMP=""

version() {
	printf '%s\n' "${AWS_SDK_CPP_VERSION}"
}

revision() {
	printf '%s\n' "${AWS_SDK_CPP_REVISION}"
}

arch() {
	case "$(uname -m)" in
	x86_64 | amd64) printf 'amd64\n' ;;
	aarch64 | arm64) printf 'arm64\n' ;;
	*)
		echo "::error::Unsupported architecture '$(uname -m)': the AWS C++ SDK artifact is published for amd64 and arm64 only." >&2
		exit 1
		;;
	esac
}

# The architecture an optional argument names, else this host's. Only the two
# published values are accepted, so a typo cannot address a tag that no run
# publishes.
arch_arg() {
	local a="${1:-}"
	if [[ -z "${a}" ]]; then
		arch
		return
	fi
	case "${a}" in
	amd64 | arm64) printf '%s\n' "${a}" ;;
	*)
		echo "::error::Unknown architecture '${a}': expected amd64 or arm64." >&2
		exit 1
		;;
	esac
}

tag() {
	local a
	a="$(arch_arg "${1:-}")"
	printf 'aws-sdk-%s-r%s-%s\n' "${AWS_SDK_CPP_VERSION}" "${AWS_SDK_CPP_REVISION}" "${a}"
}

file() {
	local a
	a="$(arch_arg "${1:-}")"
	printf 'aws-sdk-cpp-%s-r%s-linux-%s.tar.gz\n' "${AWS_SDK_CPP_VERSION}" "${AWS_SDK_CPP_REVISION}" "${a}"
}

# lib64/ on distributions whose CMake installs there, lib/ elsewhere; build.rs
# checks for lib64/ first and so does this.
libdir() {
	if [[ -d "$1/lib64" ]]; then
		printf '%s/lib64\n' "$1"
	else
		printf '%s/lib\n' "$1"
	fi
}

build() {
	local prefix="${1:?usage: aws-sdk-cpp.sh build <prefix> [workdir]}"
	local work="${2:-}"
	local tool
	for tool in git cmake make c++; do
		if ! command -v "${tool}" >/dev/null 2>&1; then
			echo "::error::Cannot build the AWS C++ SDK: '${tool}' is not installed." >&2
			exit 1
		fi
	done
	if [[ -z "${work}" ]]; then
		# A temporary work directory this function owns is removed however
		# the build ends; a caller-supplied one is the caller's to keep.
		BUILD_TMP="$(mktemp -d)"
		work="${BUILD_TMP}"
		trap 'rm -rf "${BUILD_TMP}"' EXIT
	fi
	local src="${work}/aws-sdk-cpp" bld="${work}/aws-sdk-build"
	rm -rf "${src}" "${bld}"
	mkdir -p "${work}"
	echo "Building AWS C++ SDK ${AWS_SDK_CPP_VERSION} from source..."
	# The SDK's dependencies are submodules; --depth does not make them
	# shallow on its own, and only the tag's tree is needed.
	git clone --branch "${AWS_SDK_CPP_VERSION}" --depth 1 \
		--recurse-submodules --shallow-submodules \
		"${AWS_SDK_CPP_REPO}" "${src}"
	cmake -S "${src}" -B "${bld}" \
		-DCMAKE_BUILD_TYPE=Release \
		-DCMAKE_INSTALL_PREFIX="${prefix}" \
		-DBUILD_ONLY="s3;transfer;core" \
		-DBUILD_SHARED_LIBS=OFF \
		-DENABLE_TESTING=OFF \
		-DCMAKE_POSITION_INDEPENDENT_CODE=ON \
		-DMINIMIZE_SIZE=ON
	cmake --build "${bld}" --parallel "$(nproc)"
	cmake --install "${bld}"
	# Only the install prefix is kept; the checkout and build tree are large.
	rm -rf "${src}" "${bld}"
	echo "AWS C++ SDK ${AWS_SDK_CPP_VERSION} installed at ${prefix}"
}

# What build.rs reads from the prefix: the header the cloud glue includes,
# the three SDK libraries it links unconditionally, and the thirteen AWS
# common-runtime libraries it links each only when the file exists. That
# conditional is why every one is required here: a prefix missing one would
# verify, publish and then fail every consumer at link time with undefined
# symbols, or quietly drop a capability. The library list mirrors the
# fork's build.rs (link_aws_sdk) and changes only with it. Every path is
# checked before the verdict so a broken artifact names all its gaps at
# once.
verify() {
	local prefix="${1:?usage: aws-sdk-cpp.sh verify <prefix>}"
	local lib missing=()
	lib="$(libdir "${prefix}")"
	local path
	for path in \
		"${prefix}/include/aws/core/Aws.h" \
		"${prefix}/include/aws/s3/S3Client.h" \
		"${prefix}/include/aws/transfer/TransferManager.h" \
		"${lib}/libaws-cpp-sdk-s3.a" \
		"${lib}/libaws-cpp-sdk-transfer.a" \
		"${lib}/libaws-cpp-sdk-core.a" \
		"${lib}/libaws-crt-cpp.a" \
		"${lib}/libaws-c-mqtt.a" \
		"${lib}/libaws-c-event-stream.a" \
		"${lib}/libaws-c-s3.a" \
		"${lib}/libaws-c-auth.a" \
		"${lib}/libaws-c-http.a" \
		"${lib}/libaws-c-io.a" \
		"${lib}/libaws-c-cal.a" \
		"${lib}/libaws-checksums.a" \
		"${lib}/libaws-c-compression.a" \
		"${lib}/libaws-c-sdkutils.a" \
		"${lib}/libaws-c-common.a" \
		"${lib}/libs2n.a"; do
		[[ -f "${path}" ]] || missing+=("${path#"${prefix}"/}")
	done
	if ((${#missing[@]} > 0)); then
		echo "::error::The AWS C++ SDK at ${prefix} is incomplete; missing: ${missing[*]}" >&2
		exit 1
	fi
	echo "AWS C++ SDK layout verified at ${prefix} (libraries in ${lib#"${prefix}"/}/)"
}

pack() {
	local prefix="${1:?usage: aws-sdk-cpp.sh pack <prefix> <tar.gz>}"
	local out="${2:?usage: aws-sdk-cpp.sh pack <prefix> <tar.gz>}"
	if [[ ! -d "${prefix}" ]]; then
		echo "::error::Cannot pack ${prefix}: not a directory." >&2
		exit 1
	fi
	# GNU tar's reproducibility flags; gzip -n leaves out its name and
	# timestamp. Entries are ./include/..., ./lib/... so unpack restores the
	# prefix exactly.
	tar --sort=name --mtime='@0' --owner=0 --group=0 --numeric-owner \
		-C "${prefix}" -cf - . | gzip -n >"${out}"
	echo "Packed ${prefix} into ${out} ($(wc -c <"${out}" | tr -d ' ') bytes)"
}

unpack() {
	local archive="${1:?usage: aws-sdk-cpp.sh unpack <tar.gz> <prefix>}"
	local prefix="${2:?usage: aws-sdk-cpp.sh unpack <tar.gz> <prefix>}"
	if [[ -e "${prefix}" ]]; then
		echo "::error::Cannot unpack into ${prefix}: it already exists." >&2
		exit 1
	fi
	mkdir -p "${prefix}"
	tar -xzf "${archive}" -C "${prefix}"
	echo "Unpacked ${archive} into ${prefix}"
}

case "${1:-}" in
version) version ;;
revision) revision ;;
arch) arch ;;
tag) tag "${2:-}" ;;
file) file "${2:-}" ;;
build) build "${2:-}" "${3:-}" ;;
verify) verify "${2:-}" ;;
pack) pack "${2:-}" "${3:-}" ;;
unpack) unpack "${2:-}" "${3:-}" ;;
*)
	echo "usage: aws-sdk-cpp.sh version | revision | arch | tag [arch] | file [arch] | build <prefix> [workdir] | verify <prefix> | pack <prefix> <tar.gz> | unpack <tar.gz> <prefix>" >&2
	exit 2
	;;
esac
