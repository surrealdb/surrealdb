{ pkgs, target, util }:

{
  inherit target;

  features = with util.features; [ storage-mem ];

  buildSpec = with pkgs; {
      nativeBuildInputs = [ pkg-config ];

      buildInputs = [ openssl onnxruntime ];

      CARGO_BUILD_TARGET = target;

      ONNXRUNTIME_LIB_PATH = "${onnxruntime.outPath}/lib/libonnxruntime.so";
    };
}
