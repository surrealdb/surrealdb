{
  description =
    "A scalable, distributed, collaborative, document-graph database, for the realtime web";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11-small";
    flake-utils.url = "github:numtide/flake-utils/v1.0.0";
    crane = {
      url = "github:ipetkov/crane/v0.23.0";
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

        # Read the channel from rust-toolchain.toml so the Nix toolchain stays in
        # sync automatically. Update rustManifestSha256 whenever the channel is
        # bumped: set it to lib.fakeHash, run `nix build`, then copy the expected
        # hash from the error message.
        rustChannel = (builtins.fromTOML (builtins.readFile ./rust-toolchain.toml)).toolchain.channel;
        rustManifestSha256 = "sha256-gh/xTkxKHL4eiRXzWv8KP7vfjSk61Iq48x47BEDFgfk=";

        nightlyPin = pkgs.lib.strings.trim (builtins.readFile ./rust-toolchain.nightly);
        nightlyDate =
          assert pkgs.lib.assertMsg (pkgs.lib.hasPrefix "nightly-" nightlyPin)
            "rust-toolchain.nightly must start with nightly-";
          pkgs.lib.removePrefix "nightly-" nightlyPin;
        nightlyManifestSha256 = "sha256-2ppS21jOURao7IT5IOE1xz6JqjnpcvLOM865W2MMB54=";
        nightlyComponents = fenix.packages.${system}.toolchainOf {
          channel = "nightly";
          date = nightlyDate;
          sha256 = nightlyManifestSha256;
        };
        nightlyToolchain = fenix.packages.${system}.combine (with nightlyComponents; [
          cargo
          rustc
          rust-std
          rustfmt
        ]);

        revisionLockSource = pkgs.fetchCrate {
          pname = "revision-lock";
          version = "0.2.0";
          registryDl = "https://static.crates.io/crates";
          hash = "sha256-Jx4doNYO5hhAFCoXmfqi/0XeoMszcA6CXRw4IBcz+0c=";
        };

        revisionLockCargoDeps =
          pkgs.runCommand "revision-lock-0.2.0-cargo-deps"
            {
              cargoDeps = pkgs.rustPlatform.importCargoLock {
                lockFile = "${revisionLockSource}/Cargo.lock";
                extraRegistries = {
                  "https://github.com/rust-lang/crates.io-index" = "https://static.crates.io/crates";
                };
              };
            }
            ''
              cp -R "$cargoDeps" "$out"
              chmod u+w "$out/.cargo" "$out/.cargo/config.toml"
              sed -i '/^\[source\."https:\/\/github.com\/rust-lang\/crates.io-index"\]$/,+2d' "$out/.cargo/config.toml"
              sed -i 's|directory = "cargo-vendor-dir"|directory = "@vendor@"|' "$out/.cargo/config.toml"
            '';

        revisionLock = pkgs.rustPlatform.buildRustPackage {
          pname = "revision-lock";
          version = "0.2.0";
          src = revisionLockSource;
          cargoDeps = revisionLockCargoDeps;
        };

        mkRustToolchain = {target, extraComponents ? []}:
          with fenix.packages.${system};
          combine ([
            (fromToolchainFile { file = ./rust-toolchain.toml; sha256 = rustManifestSha256; })
            (targets.${target}.toolchainOf { channel = rustChannel; sha256 = rustManifestSha256; }).rust-std
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
            inherit (pkgs) lib;
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
            extraComponents = with fenix.packages.${system}; [
              (toolchainOf { channel = rustChannel; sha256 = rustManifestSha256; }).rust-src
              (toolchainOf { channel = rustChannel; sha256 = rustManifestSha256; }).rust-analyzer
              (targets.${target}.toolchainOf { channel = rustChannel; sha256 = rustManifestSha256; }).rustfmt
            ];
            rustToolchain = mkRustToolchain { inherit target extraComponents; };
            cargoFmtToolchainShim = pkgs.writeShellScriptBin "cargo-fmt" ''
              if [ -n "''${RUSTUP_TOOLCHAIN:-}" ]; then
                exec ${pkgs.rustup}/bin/rustup run "$RUSTUP_TOOLCHAIN" cargo-fmt "$@"
              fi
              exec ${rustToolchain}/bin/cargo-fmt "$@"
            '';
            buildSpec = spec.buildSpec;
          in pkgs.mkShell (buildSpec // {
            hardeningDisable = [ "fortify" ];

            depsBuildBuild = buildSpec.depsBuildBuild or [ ]
              ++ [ cargoFmtToolchainShim rustToolchain revisionLock ]
              ++ (with pkgs; [
                nixfmt
                cargo-watch
                wasm-pack
                pre-commit
                cargo-make
                cargo-nextest
                rustup
              ]);

            inherit (util) SURREAL_BUILD_VERSION SURREAL_BUILD_METADATA;

            shellHook = (buildSpec.shellHook or "") + ''
              export RUSTUP_HOME="''${XDG_CACHE_HOME:-$HOME/.cache}/surrealdb/rustup"
              mkdir -p "$RUSTUP_HOME"
              nightly_link="$RUSTUP_HOME/toolchains/surrealdb-nightly-2026-05-11"
              if [ "$(readlink "$nightly_link" 2>/dev/null)" != "${nightlyToolchain}" ]; then
                if [ -e "$nightly_link" ] && [ ! -L "$nightly_link" ]; then
                  echo "error: $nightly_link exists and is not a symbolic link" >&2
                  return 1
                fi
                rm -f "$nightly_link"
                rustup toolchain link surrealdb-nightly-2026-05-11 ${nightlyToolchain}
              fi
              export SURREAL_NIGHTLY_RUST_TOOLCHAIN=surrealdb-nightly-2026-05-11
            '';
          })) util.platforms);

        # nix run
        apps.default = flake-utils.lib.mkApp { drv = packages.default; };

      });
}
