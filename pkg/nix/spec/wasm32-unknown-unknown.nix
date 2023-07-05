{ pkgs, target, util }:

{
  inherit target;

  features = with util.features; [ storage-mem ];

  buildSpec = with pkgs; {
      nativeBuildInputs = [ pkg-config ];

      buildInputs = [ openssl ];

      CARGO_BUILD_TARGET = target;
    };
}
