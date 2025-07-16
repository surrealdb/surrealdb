{
  description =
    "A scalable, distributed, collaborative, document-graph database, for the realtime web";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.05-small";
    flake-utils.url = "github:numtide/flake-utils/v1.0.0";
    crane = {
      url = "github:ipetkov/crane/v0.20.3";
    };
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-compat = {
      url = "github:edolstra/flake-compat";
      flake = false;
    };
  };

  outputs = inputs:
    with inputs;

    # Make systems available as variables to prevent typos
    with flake-utils.lib.system;

    # let-in expressions, very similar to Rust's let bindings.  These names
    # are used to express the output but not themselves paths in the output.
    let

      nativeSystems = [ aarch64-darwin aarch64-linux x86_64-darwin x86_64-linux ];

      # Build the output set for each default system and map system sets into
      # attributes, resulting in paths such as:
      # nix build .#packages.x86_64-linux.<name>
    in flake-utils.lib.eachSystem nativeSystems (system:

      let

        pkgs = import nixpkgs { inherit system; };

        util = import ./pkg/nix/util.nix {
          inherit system;
          inherit (pkgs) lib;
          systems = flake-utils.lib.system;
          flake = self;
        };

        mkRustToolchain = {target, extraComponents ? []}:
          with fenix.packages.${system};
          combine ([
            stable.rustc
            stable.cargo
            targets.${target}.stable.rust-std
            stable.clippy
          ] ++ extraComponents);

        buildPlatform = pkgs.stdenv.buildPlatform.config;

        # Make platforms available as variables to prevent typos
      in with util.platforms;

      rec {
        packages = {
          # nix build
          default =
            packages.${buildPlatform} or packages.x86_64-unknown-linux-gnu;

          # nix build .#docker-image
          docker-image = import ./pkg/nix/drv/docker.nix {
            inherit util;
            inherit (pkgs) cacert dockerTools;
            package = packages.x86_64-unknown-linux-gnu;
          };

          # nix build .#static-binary
          static-binary = packages.x86_64-unknown-linux-musl;

          # nix build .#wasm
          wasm = packages.wasm32-unknown-unknown;

          # nix build .#windows-binary
          windows-binary = packages.x86_64-pc-windows-gnu;
        } // (pkgs.lib.attrsets.mapAttrs (target: _:
          let
            spec =
              import ./pkg/nix/spec/${target}.nix { inherit pkgs target util; };
          in import ./pkg/nix/drv/binary.nix {
            inherit pkgs util spec crane;
            rustToolchain = mkRustToolchain { inherit target; };
          }) util.platforms);

        devShells = {
          # nix develop
          default =
            devShells.${buildPlatform} or devShells.x86_64-unknown-linux-gnu;

          # nix develop .#static-binary
          static-binary = devShells.x86_64-unknown-linux-musl;

          # nix develop .#wasm
          wasm = devShells.wasm32-unknown-unknown;

          # nix develop .#windows-binary
          windows-binary = devShells.x86_64-pc-windows-gnu;
        } // (pkgs.lib.attrsets.mapAttrs (target: _:
          let
            spec = (import ./pkg/nix/spec/${target}.nix) {
              inherit pkgs target util;
            };
            extraComponents = with fenix.packages.${system}; [ targets.${target}.stable.rust-src stable.rust-analyzer targets.${target}.stable.rustfmt ];
            rustToolchain = mkRustToolchain { inherit target extraComponents; };
            buildSpec = spec.buildSpec;
          in pkgs.mkShell (buildSpec // {
            hardeningDisable = [ "fortify" ];

            depsBuildBuild = buildSpec.depsBuildBuild or [ ]
              ++ [ rustToolchain ] ++ (with pkgs; [ nixfmt cargo-watch wasm-pack pre-commit cargo-make]);

            inherit (util) SURREAL_BUILD_METADATA;
          })) util.platforms);

        # nix run
        apps.default = flake-utils.lib.mkApp { drv = packages.default; };

      });
}
