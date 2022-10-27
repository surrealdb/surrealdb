{ pkgs, target, util }:

{
  inherit target;

  buildSpec = with pkgs; {
    nativeBuildInputs = with pkgsStatic; [ stdenv.cc openssl ];

    CARGO_BUILD_TARGET = target;

    OPENSSL_NO_VENDOR = "true";
    OPENSSL_STATIC = "true";
    OPENSSL_LIB_DIR = "${pkgsStatic.openssl.out}/lib";
    OPENSSL_INCLUDE_DIR = "${pkgsStatic.openssl.dev}/include";
  };
}
