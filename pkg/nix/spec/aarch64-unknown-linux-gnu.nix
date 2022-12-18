{ pkgs, target, util }:

{
  inherit target;

  features = with util.features; [ http ];

  buildSpec = with pkgs; {
    depsBuildBuild = [ clang protobuf perl ];

    nativeBuildInputs = [ pkg-config ];

    buildInputs = [ openssl binutils ];

    CARGO_BUILD_TARGET = target;

    LIBCLANG_PATH = "${llvmPackages.libclang.lib}/lib";

    OPENSSL_NO_VENDOR = "true";
  };
}
