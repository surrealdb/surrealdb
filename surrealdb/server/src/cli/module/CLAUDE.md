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
1. Load and validate the package file: `package.toml`, or `surrealism.toml` when `package.toml` is absent. When both exist, `surrealism.toml` is ignored and a warning names it.
2. Read `cargo metadata` to find the WASM target artifact.
3. Run `cargo build --target wasm32-wasip2` (release or debug). No build flags are needed: the guest SDK's `async fn` support comes from the component model's async ABI, not an embedded executor.
4. Validate the `surrealism` SDK version matches the server's expected version.
5. Run `wasm-opt` to optimize the binary (skip in debug mode).
6. Instantiate with a `DemoHost` to extract the exports manifest (function signatures).
7. Stamp `abi = 4` into the config.
8. Pack into a `.surli` archive via `SurrealismPackage`, which writes `surrealism/MANIFEST`, and print the archive digest.

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

Before signing, `sign` checks that the archive's files match its `MANIFEST` by the loader's own rules (`surrealism_runtime::package::verify_archive`), so it never signs a manifest that lists other code than the archive holds. It checks no trust anchors. The server verifies the signature against the definition's `SIGNER`.

## `DemoHost` (`host.rs`)

A minimal `InvocationContext` implementation for CLI use. When a module calls `sql()` or `run()`, it prints the query/function to stdout and reads a result from stdin. This enables interactive testing of modules outside the database.

## Key Files

| File | Purpose |
|------|---------|
| `mod.rs` | `ModuleCommand` enum (clap), dispatcher, `parse_value` helper |
| `init_cmd.rs` | Project scaffolding |
| `build.rs` | Build pipeline: compile, optimize, extract exports, pack |
| `run.rs` | Load archive, invoke function, print result |
| `info.rs` | Display module metadata and exports |
| `sig.rs` | Display single function signature |
| `sign/` | Write `SIGNATURE`: keyless Sigstore, SSH key, or imported bundle |
| `host.rs` | `DemoHost` — stdin/stdout `InvocationContext` for CLI testing |
