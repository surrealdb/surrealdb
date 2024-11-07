{ pkgs, target, util }:

{
  inherit target;

  features = with util.features; [ storage-mem ];

  buildSpec = with pkgs; {
    nativeBuildInputs = with pkgsStatic; [ stdenv.cc openssl onnxruntime ];

    CARGO_BUILD_TARGET = target;

    OPENSSL_NO_VENDOR = "true";
    OPENSSL_STATIC = "true";
    OPENSSL_LIB_DIR = "${pkgsStatic.openssl.out}/lib";
    OPENSSL_INCLUDE_DIR = "${pkgsStatic.openssl.dev}/include";

    PROTOC = "${protobuf}/bin/protoc";
    PROTOC_INCLUDE = "${protobuf}/include";

    ONNXRUNTIME_LIB_PATH = "${onnxruntime.outPath}/lib/libonnxruntime.so";
  };
}
