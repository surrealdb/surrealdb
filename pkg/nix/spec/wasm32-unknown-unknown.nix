{ pkgs, target, util }:

{
  inherit target;

  buildSpec = with pkgs; {
      nativeBuildInputs = [ pkg-config ];

      buildInputs = [ openssl ];

      CARGO_BUILD_TARGET = target;
    };
}
