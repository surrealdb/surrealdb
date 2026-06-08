{ pkgs, target, util }:

{
  inherit target;

  features = with util.features; [ storage-mem ];

  buildSpec = with pkgs; {
    hardeningDisable = [ "fortify" ];

    # GCC 14+ defaults to C23 mode, which introduces versioned symbol aliases
    # (e.g. __isoc23_strtol, __isoc23_sscanf) that musl does not provide.
    # NIX_CFLAGS_COMPILE only reaches Nix's compiler wrapper; Cargo build
    # scripts invoke the cc crate directly, so we must use the aws-lc-sys
    # specific var to force C11 there.
    AWS_LC_SYS_CFLAGS = "-std=c11";

    nativeBuildInputs = with pkgsStatic; [ stdenv.cc openssl ];

    CARGO_BUILD_TARGET = target;

    OPENSSL_NO_VENDOR = "true";
    OPENSSL_STATIC = "true";
    OPENSSL_LIB_DIR = "${pkgsStatic.openssl.out}/lib";
    OPENSSL_INCLUDE_DIR = "${pkgsStatic.openssl.dev}/include";

    PROTOC = "${protobuf}/bin/protoc";
    PROTOC_INCLUDE = "${protobuf}/include";
  };
}
