{ pkgs, target, util }:

{
  inherit target;

  features = with util.features; [ default ];

  buildSpec = with pkgs; {
    strictDeps = true;

    depsBuildBuild = [ pkgsCross.mingwW64.stdenv.cc ];

    buildInputs = [ pkgsCross.mingwW64.windows.pthreads onnxruntime ];

    CARGO_BUILD_TARGET = target;

    ONNXRUNTIME_LIB_PATH = "${onnxruntime.outPath}/lib/onnxruntime.dll";
  };
}
