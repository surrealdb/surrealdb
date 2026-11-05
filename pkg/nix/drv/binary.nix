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

  unfilteredRoot = ../../../.; # The original, unfiltered source
  buildSpec = spec.buildSpec // {
    src = lib.fileset.toSource {
      root = unfilteredRoot;
      fileset = lib.fileset.unions [
        # Default files from crane (Rust and cargo files)
        (craneLib.fileset.commonCargoSources unfilteredRoot)
        # Also keep any markdown files
        (lib.fileset.fileFilter (file: file.hasExt "md") unfilteredRoot)
        # Keep the checked-in keyspace map that the datastore key module embeds
        # at compile time via `include_str!`; without it the crate fails to build.
        (lib.fileset.fileFilter (file: file.hasExt "map") unfilteredRoot)
      ];
    };
    doCheck = false;
    cargoExtraArgs = let flags = [ "--no-default-features" ] ++ featureFlags;
    in builtins.concatStringsSep " " flags;
    
    # Add autoPatchelfHook to fix dynamic library linking for non-static builds
    nativeBuildInputs = (spec.buildSpec.nativeBuildInputs or []) 
      ++ lib.lists.optional (pkgs.stdenv.isLinux && !pkgs.stdenv.hostPlatform.isStatic) pkgs.autoPatchelfHook;
  };

  cargoArtifacts = craneLib.buildDepsOnly buildSpec;

in craneLib.buildPackage (buildSpec // {
  inherit cargoArtifacts;
  inherit (util) version SURREAL_BUILD_VERSION SURREAL_BUILD_METADATA;

})
