{ pkgs, lib, spec, util, rustToolchain, crane }:

let

  featureFlags = let
    featureLists = spec.features or [ ];
    features = with pkgs.lib; lists.unique (lists.flatten featureLists);
  in map (feature: "--features=${feature}") features;

  craneLib = (crane.mkLib pkgs).overrideScope (final: prev: {
    cargo = rustToolchain;
    rustc = rustToolchain;
  });

  markdownFilter = path: _type: builtins.match ".*md$" path != null;
  markdownOrCargo = path: type:
    (markdownFilter path type) || (craneLib.filterCargoSources path type);
  buildSpec = spec.buildSpec // {
    unfilteredRoot = ../../../.; # The original, unfiltered source
    src = lib.fileset.toSource {
      root = unfilteredRoot;
      fileset = lib.fileset.unions [
        # Default files from crane (Rust and cargo files)
        (craneLib.fileset.commonCargoSources unfilteredRoot)
        # Also keep any markdown files
        (lib.fileset.fileFilter (file: file.hasExt "md") unfilteredRoot)
      ];
    };
    doCheck = false;
    cargoExtraArgs = let flags = [ "--no-default-features" ] ++ featureFlags;
    in builtins.concatStringsSep " " flags;
  };

  cargoArtifacts = craneLib.buildDepsOnly buildSpec;

in craneLib.buildPackage (buildSpec // {
  inherit cargoArtifacts;
  inherit (util) version SURREAL_BUILD_METADATA;

})
