{ pkgs, spec, util, rustToolchain, crane }:

let

  featureFlags = let
    featureLists = spec.features or [ ];
    features = with pkgs.lib; lists.unique (lists.flatten featureLists);
  in map (feature: "--features=${feature}") features;

  craneLib = (crane.mkLib pkgs).overrideScope (final: prev: {
    cargo = rustToolchain;
    rustc = rustToolchain;
  });

  buildSpec = spec.buildSpec // {
    src = craneLib.cleanCargoSource ../../../.;
    doCheck = false;
    cargoExtraArgs = let flags = [ "--no-default-features" ] ++ featureFlags;
    in builtins.concatStringsSep " " flags;
  };

  cargoArtifacts = craneLib.buildDepsOnly buildSpec;

in craneLib.buildPackage (buildSpec // {
  inherit cargoArtifacts;
  inherit (util) version SURREAL_BUILD_METADATA;

})
