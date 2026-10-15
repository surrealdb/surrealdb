# CLI Module Subcommands

Essential context for AI assistants working with the `surreal extension` CLI.

## Overview

The `surreal extension` command (also reachable as `surreal module`) provides tooling for developing, building, and inspecting Surrealism extensions. All subcommands are defined in `mod.rs` via `clap::Subcommand`.

## Subcommands

### `init` (`init_cmd.rs`)

Scaffolds a new Surrealism module project.

- Creates `Cargo.toml`, `package.toml`, `.cargo/config.toml` (Tokio WASIp2 flags), and `src/lib.rs` with starter code.
- Supports interactive and headless (`--headless --org <org>`) modes.
- Pins the `surrealism` SDK dependency to `SURREALISM_VERSION` and `surrealdb-types` to `SURREALDB_VERSION`. The full `surrealdb` crate is omitted by default (commented in `Cargo.toml`) to keep WASM builds fast; most modules only need `surrealdb-types` for `SurrealValue` and related types.
- Only Rust target is currently supported.

### `build` (`build.rs`)

Compiles a Rust project into a distributable `.surli` archive.

Pipeline:
1. Load and validate the package file through `PackageConfig::open`: `package.toml`, or `surrealism.toml` when `package.toml` is absent. When both exist, `surrealism.toml` is ignored and a warning names it. Several `[attach] fs` mounts are assembled into one directory at their guest paths.
2. Read `cargo metadata` to find the WASM target artifact.
3. Run `cargo build --target wasm32-wasip2` (release or debug). No build flags are needed: the guest SDK's `async fn` support comes from the component model's async ABI, not an embedded executor.
4. Validate the `surrealism` SDK version matches the server's expected version.
5. Run `wasm-opt` to optimize the binary (skip in debug mode).
6. Pre-initialise (`snapshot.rs`): run `init` once under `wasmtime-wizer` with the attached `fs/` preopened, write the resulting memory back as initial data, and remove the `init` export. Runs in debug builds too. Network use, a WASI resource left open, or an `init` error or trap fails the build; randomness and clock reads are printed as notes.
7. Instantiate the snapshot with a `DemoHost` to extract the exports manifest (function signatures).
8. Stamp `abi = 4` into the config.
9. Pack into a `.surli` archive via `SurrealismPackage`, which writes `surrealism/MANIFEST`, and print the archive digest. The snapshot precedes this step, so the digest covers the pre-initialised `mod.wasm`.

### `run` (`run.rs`)

Loads a `.surli` archive and invokes a function.

- Uses `DemoHost` (see below) as the `InvocationContext`.
- If `--fnc` is not specified, uses the first (or only) export.
- Prints the return value as SurrealQL.

### `info` (`info.rs`)

Prints module metadata and exports without executing.

- Displays package name, version, SDK version, ABI version.
- Lists all exported functions with their argument and return types.

### `sig` (`sig.rs`)

Prints a single function's signature (argument types and return type).

### `sign` (`sign/`)

Writes `surrealism/SIGNATURE` into a `.surli` archive. The signature covers `sha256(surrealism/MANIFEST)`, which is the extension's `DIGEST`. User documentation: `doc/EXTENSION_SIGNING.md`.

- `archive.rs` reads `MANIFEST` (parsed with `surrealism_runtime::manifest::Manifest`, so a malformed one is refused) and re-packs the archive at the tar-record level. Every other record, including GNU long-name and PAX records, is copied byte for byte. Any existing `SIGNATURE` is dropped and the new one is appended. The output goes through a temporary file, so a failure leaves the target unchanged.
- `keyless.rs` (default) signs through `sigstore-sign`: the CI identity token from `sigstore_oidc::IdentityToken::detect_ambient`, an ephemeral P-256 key and Fulcio certificate, ECDSA over `MANIFEST`, an RFC 3161 timestamp and a Rekor v2 entry. Sigstore's signing config lists only Rekor v1, so the v2 log and the TSA are pinned in `SigningServices`, with flag overrides (`--fulcio-url`, `--rekor-url`, `--tsa-url`). Never log or put in an error the token or key material.
- `ssh.rs` (`--key`) runs `ssh-keygen -Y sign -n surrealism -O hashalg=sha256` (OpenSSH 8.9 or later) over the `MANIFEST` bytes, so the `SSHSIG` `H(message)` equals `DIGEST`. It parses the output and refuses any other namespace or hash algorithm.
- `bundle.rs` (`--bundle`) checks an imported Sigstore bundle's message digest, and the digest in readable `hashedrekord` log entries, against this archive's manifest. It also assembles the keyless v0.3 bundle.
- `trusted_root.rs` loads the Sigstore `trusted_root.json` (`--trusted-root`, or `sigstore_trust_root::TrustedRoot::from_tuf(TufConfig::production())`, a verified TUF fetch) and seals a Sigstore bundle into the SIGNATURE envelope (`surrealism_verify::TrustedRoot::seal`): the bundle plus the root reduced to the CA, log and TSA that verify it, checked with `verify_offline` before anything is written. Keyless and `--bundle` always write the envelope; `--bundle` and a private Sigstore require `--trusted-root`. An SSH signature is written as is.
- `signer.rs` reads `[signer]` from the archive's own `package.toml` (`archive::read_package`, through `PackageConfig`) and refuses, before anything is written, a signature whose claims (`surrealism_verify::claims`) are not that signer: the certificate's issuer, its identity without `@ref`, and the ids in Fulcio extensions `.15` and `.17`; or the SSH key blob. No `[signer]` means nothing is signed.

Before signing, `sign` checks that the archive's files match its `MANIFEST` by the loader's own rules (`surrealism_runtime::package::verify_archive`), so it never signs a manifest that lists other code than the archive holds. It checks no trust anchors. The server verifies the signature against the definition's `SIGNER`.

### `config` (`config_cmd.rs`)

Reads and changes the project's `package.toml`, only through `surrealism_runtime::package_config::PackageConfig`, so every change is validated and comments and formatting of untouched keys survive.

- `show`, `get <key>` (dotted, defaults included), `set <key> <value>`, `add`/`remove <list-key> <value>` for `capabilities.allow_net` and `capabilities.allow_functions`.
- `signer github --repo owner/name --workflow release.yml`: issuer `https://token.actions.githubusercontent.com`, identity `https://github.com/owner/name/.github/workflows/release.yml`, and the repository and owner ids from `GET https://api.github.com/repos/owner/name` (`GITHUB_TOKEN` for a private repository). `--repository-id`/`--owner-id` work offline.
- `signer gitlab --project group/proj [--host]`: issuer the host, identity `{host}/group/proj//.gitlab-ci.yml`, and the project and namespace ids from `{host}/api/v4/projects/group%2Fproj` (`GITLAB_TOKEN`). `--project-id`/`--namespace-id` work offline.
- `signer ssh <file.pub>` stores the key blob without its comment; `signer remove` clears `[signer]`.
- `fs add <dir> --as <path>` and `fs remove <path>` change the `[attach] fs` mounts, which `build` assembles into one directory.

### `install` and `upgrade` (`../extension/`)

`surreal extension install <source> --as <name>` and `surreal extension upgrade <name> [<source>]` live in `cli/extension/`, because they talk to a database rather than to a project. Both split into a read-only `prepare_*` that fetches the archive, checks the four archive rules, computes `DIGEST`, verifies `SIGNATURE` and builds the statement, and a `commit` that runs the statement, the only write either makes. Sources are Silo packages and archives already in a bucket; the installer never uploads an archive. Both connect as the user the standard connection flags name, so that user needs to be allowed to define extensions in the database and to read the bucket. The prompt is rendered from the plan in between.

| File | Purpose |
|------|---------|
| `source.rs` | `silo::org::pkg[<x.y.z>]` (`pkg::<x.y.z>` is read too) or `f"bucket:/path"`; the `FROM` operand |
| `archive.rs` | `inspect_archive`: the runtime loader's archive rules, `DIGEST`, package metadata |
| `verify.rs` | Offline: `verify_envelope` (the trust root a Sigstore SIGNATURE carries, through `surrealism_verify::verify_offline`), or `load_trust_root` + `prove_signer` for a `--trust-root` override; SSH through `prove_signer` |
| `online.rs` | Best effort: is the anchor in Sigstore's public trust root (`sigstore-trust-root` over TUF, verified from the embedded public-good TUF root), and is the entry in the public Rekor v2 log (the served entry, proved by RFC 9162 inclusion with `sigstore-merkle` under a checkpoint the public log key signs, checked with `sigstore_sign::crypto`); each reported ✓/⚠/✗, never silently passed |
| `silo.rs` | `SiloClient`: resolve a version, describe it from the package API or the archive, download it (contract: `surrealism/SILO.md`) |
| `installed.rs` | Reads an installed definition from the `extensions` key of `INFO FOR DB STRUCTURE` |
| `capabilities.rs` | The `CAPABILITIES` object: request, `--with-capability` tokens, upgrade carry-over, diff, high-risk set |
| `signer.rs` | The `SIGNER` clause, same-signer comparison, labels and fingerprints |
| `statement.rs` | `DEFINE EXTENSION` text; `OVERWRITE` for an upgrade |
| `prompt.rs` | The install and upgrade prompts |

## `DemoHost` (`host.rs`)

A minimal `InvocationContext` implementation for CLI use. When a module calls `sql()` or `run()`, it prints the query/function to stdout and reads a result from stdin. This enables interactive testing of modules outside the database.

## Key Files

| File | Purpose |
|------|---------|
| `mod.rs` | `ModuleCommand` enum (clap), dispatcher, `parse_value` helper |
| `init_cmd.rs` | Project scaffolding |
| `build.rs` | Build pipeline: compile, optimize, snapshot, extract exports, pack |
| `snapshot.rs` | Build-time pre-initialisation of `init` under `wasmtime-wizer` and its stub WASI |
| `run.rs` | Load archive, invoke function, print result |
| `info.rs` | Display module metadata and exports |
| `sig.rs` | Display single function signature |
| `sign/` | Write `SIGNATURE`: keyless Sigstore, SSH key, or imported bundle, only as the package's `[signer]` |
| `config_cmd.rs` | `package.toml` reads and changes through `PackageConfig`, signer templates |
| `host.rs` | `DemoHost` — stdin/stdout `InvocationContext` for CLI testing |
