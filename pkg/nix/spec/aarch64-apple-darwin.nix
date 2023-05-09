{ pkgs, target, util }:

{
  inherit target;

  features = with util.features; [ default storage-tikv ];

  buildSpec = with pkgs; {
    depsBuildBuild = [ cmake clang protobuf perl ];

    nativeBuildInputs = [ pkg-config ];

    buildInputs = [ openssl libiconv darwin.apple_sdk.frameworks.Security ];

    PROTOC = "${protobuf}/bin/protoc";
    PROTOC_INCLUDE = "${protobuf}/include";

    CARGO_BUILD_TARGET = target;
  };
}
