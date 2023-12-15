{ pkgs, target, util }:

{
  inherit target;

  features = with util.features; [ storage-mem storage-rocksdb scripting http storage-tikv ];

  buildSpec = with pkgs; {
    depsBuildBuild = [ clang protobuf perl ];

    nativeBuildInputs = [ cmake pkg-config ];

    buildInputs = [ openssl libiconv darwin.apple_sdk.frameworks.Security ];

    # From https://github.com/NixOS/nixpkgs/blob/master/pkgs/development/libraries/rocksdb/default.nix#LL43C7-L52C6
    NIX_CFLAGS_COMPILE = toString ([
      "-Wno-error=unused-private-field"
      "-faligned-allocation"
    ]);

    CARGO_BUILD_TARGET = target;
  };
}
