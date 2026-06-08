{ pkgs, target, util }:

{
  inherit target;

  features = with util.features;
    [ default ];

  buildSpec = with pkgs;
    let crossCompiling = !util.isNative target;
    in {
      depsBuildBuild = [ clang cmake gcc perl protobuf grpc llvm ]
        ++ lib.lists.optional crossCompiling qemu;

      nativeBuildInputs = [ pkg-config ];

      buildInputs = [ openssl stdenv.cc.cc.lib ];

      LIBCLANG_PATH = "${llvmPackages.libclang.lib}/lib";

      PROTOC = "${protobuf}/bin/protoc";
      PROTOC_INCLUDE = "${protobuf}/include";

      CARGO_BUILD_TARGET = target;

      # GNU ld loads all object files into memory at once during the final link,
      # which OOMs on the 7 GB GitHub runner. lld (already available via llvm in
      # depsBuildBuild) streams object files instead, dramatically reducing peak
      # link memory.
      RUSTFLAGS = "-C link-arg=-fuse-ld=lld";
    };
}
