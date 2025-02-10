{ pkgs, target, util }:

{
  inherit target;

  features = with util.features;
    [ default storage-tikv ]
    ++ pkgs.lib.lists.optional (util.fdbSupported pkgs.fdbPackages)
    [ storage-fdb-7_1 ];

  buildSpec = with pkgs;
    let crossCompiling = !util.isNative target;
    in {
      depsBuildBuild = [ clang cmake gcc perl protobuf grpc llvm ]
        ++ lib.lists.optional crossCompiling qemu;

      nativeBuildInputs = [ pkg-config ];

      buildInputs = [ openssl onnxruntime ]
        ++ lib.lists.optional (util.fdbSupported fdbPackages)
        (util.fdbPackage fdbPackages);

      LIBCLANG_PATH = "${llvmPackages.libclang.lib}/lib";

      PROTOC = "${protobuf}/bin/protoc";
      PROTOC_INCLUDE = "${protobuf}/include";

      CARGO_BUILD_TARGET = target;

      ONNXRUNTIME_LIB_PATH = "${onnxruntime.outPath}/lib/libonnxruntime.so";
    };
}
