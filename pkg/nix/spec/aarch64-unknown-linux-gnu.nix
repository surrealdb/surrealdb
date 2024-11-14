{ pkgs, target, util }:

{
  inherit target;

  features = with util.features; [ storage-mem http ];

  buildSpec = with pkgs; {
    depsBuildBuild = [ clang protobuf perl ];

    nativeBuildInputs = [ pkg-config ];

    buildInputs = [ openssl binutils onnxruntime ];

    CARGO_BUILD_TARGET = target;

    LIBCLANG_PATH = "${llvmPackages.libclang.lib}/lib";

    PROTOC = "${protobuf}/bin/protoc";
    PROTOC_INCLUDE = "${protobuf}/include";

    OPENSSL_NO_VENDOR = "true";

    ONNXRUNTIME_LIB_PATH = "${onnxruntime.outPath}/lib/libonnxruntime.so";
  };
}
