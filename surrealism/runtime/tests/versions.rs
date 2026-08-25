//! Guards the surrealism crate-version lockstep.
//!
//! The module-build toolchain enforces, at user-build time, that a guest
//! project's resolved `surrealism` crate is caret-compatible with
//! `surrealism_runtime::SDK_VERSION` (this crate's own version), and
//! `surreal module init` stamps that same version into scaffolded manifests.
//! That coupling is only sound while all four surrealism crates carry the same
//! version, and nothing in Cargo enforces it: the crates cannot inherit
//! `workspace.package.version` (the workspace rides the 3.x engine train), and
//! no CI gate covers it either. The crate-versions gate compares each crate
//! against its own published version, which says nothing about whether the
//! four agree with each other.

use std::fs;
use std::path::Path;

/// Reads `[package].version` from a manifest with a real TOML parse, so
/// formatting variations (spacing, `version.workspace = true`, comments) are
/// handled by the parser rather than a byte scan.
fn package_version(manifest: &Path) -> String {
	let text = fs::read_to_string(manifest)
		.unwrap_or_else(|e| panic!("failed to read {}: {e}", manifest.display()));
	let doc: toml::Table = toml::from_str(&text)
		.unwrap_or_else(|e| panic!("failed to parse {}: {e}", manifest.display()));
	doc.get("package")
		.and_then(|package| package.get("version"))
		.and_then(|version| version.as_str())
		.unwrap_or_else(|| panic!("no `[package].version` string in {}", manifest.display()))
		.to_string()
}

#[test]
fn surrealism_crate_versions_move_in_lockstep() {
	// This test lives in `surrealism-runtime`, so the workspace root is two
	// levels up from CARGO_MANIFEST_DIR (`surrealism/runtime`).
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
	// The runtime crate is the reference: its version is what the toolchain
	// compiles in as SDK_VERSION and demands of guest projects.
	let reference_manifest = "surrealism/runtime/Cargo.toml";
	let reference_version = package_version(&root.join(reference_manifest));
	for manifest in
		["surrealism/Cargo.toml", "surrealism/macros/Cargo.toml", "surrealism/types/Cargo.toml"]
	{
		let version = package_version(&root.join(manifest));
		assert_eq!(
			version, reference_version,
			"{manifest} is at {version} but {reference_manifest} is at {reference_version}; \
			 the four surrealism crates must be bumped together (the toolchain pins guest \
			 projects to ^surrealism-runtime's version via SDK_VERSION)",
		);
	}
}
