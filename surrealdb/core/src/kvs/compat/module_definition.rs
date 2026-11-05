//! Backwards-compatibility coverage for [`StoredModuleDefinition`] revisions
//! 2, 3 and 4.
//!
//! Revision 3 records what a module may do and which code it may run:
//! `digest`, `signer` and `capabilities`. Revision 2's `unsigned` flag stays,
//! written as `signer.is_none()`, because the 4.0 schema lock numbers it and a
//! required field is never retired; readers decide from `signer` alone. A
//! revision-2 definition was written by a release whose modules were built for
//! an earlier ABI, so it decodes with all three new fields `None`, renders a
//! warning in `INFO … STRUCTURE`, and does not run until it is defined again.
//!
//! The released snapshots next door hold only revision-1 module bytes, and
//! every release that wrote revision 2 or 3 is newer than them, so this module
//! freezes both revisions itself: revision 2 from a mirror of its declaration,
//! revision 3 from the current type with every new field populated. Each value
//! also round-trips through the stored-value grammar a 4.0 store writes, and
//! through the VM value the store instructions carry it as.
//!
//! Regenerate a constant with the ignored tests at the bottom only if its
//! *shape* is restated; the bytes themselves must never be edited by hand.

use revision::revisioned;
use surrealdb_kvs::value::EncodeContext;

use crate::catalog::auth::{AuthLevel, AuthLimit};
use crate::catalog::{
	ExprText, FromStored, ModuleDefinition, ModuleExecutable, SiloExecutable,
	StoredExtensionBinding, StoredModuleDefinition, StoredPermission, SurrealismExecutable,
};
use crate::expr::{
	CapabilityTargets, ExtensionCapabilities, ExtensionSigner, ManifestDigest, SigstoreSigner,
};
use crate::key::KVValue;
use crate::val::Duration;

/// `StoredModuleDefinition` exactly as revision 2 declared it.
///
/// Kept so the frozen bytes below can be regenerated from a readable value
/// rather than transcribed. It is never used outside this module's tests.
#[revisioned(revision = 2)]
#[derive(Clone, Debug, PartialEq)]
struct StoredModuleDefinitionV2 {
	name: Option<String>,
	comment: Option<String>,
	permissions: StoredPermission,
	executable: ModuleExecutable,
	#[revision(start = 2)]
	unsigned: bool,
}

/// The revision-2 value [`MODULE_V2`] was captured from.
fn module_v2() -> StoredModuleDefinitionV2 {
	StoredModuleDefinitionV2 {
		name: Some("color".to_owned()),
		comment: Some("written by 3.x".to_owned()),
		permissions: StoredPermission::Full,
		executable: ModuleExecutable::Surrealism(SurrealismExecutable {
			bucket: "modules".to_owned(),
			key: "/color.surli".to_owned(),
		}),
		unsigned: true,
	}
}

/// FROZEN. Revision-2 `StoredModuleDefinition` bytes. Never edit these by
/// hand and never regenerate them to make a failing test pass.
#[rustfmt::skip]
const MODULE_V2: &[u8] = &[
	2, 1, 5, 99, 111, 108, 111, 114, 1, 14, 119, 114, 105, 116, 116, 101,
	110, 32, 98, 121, 32, 51, 46, 120, 1, 1, 1, 0, 1, 7, 109, 111,
	100, 117, 108, 101, 115, 12, 47, 99, 111, 108, 111, 114, 46, 115, 117, 114,
	108, 105, 1,
];

/// A revision-3 definition with a Sigstore signer and every capability
/// field set away from its default.
fn module_v3_sigstore() -> StoredModuleDefinition {
	StoredModuleDefinition {
		name: Some("color".to_owned()),
		comment: None,
		permissions: StoredPermission::Full,
		executable: ModuleExecutable::Silo(SiloExecutable {
			organisation: "acme".to_owned(),
			package: "color".to_owned(),
			major: 1,
			minor: 0,
			patch: 0,
		}),
		unsigned: false,
		digest: Some(ManifestDigest::sha256([0x5d; 32])),
		signer: Some(ExtensionSigner::Sigstore(SigstoreSigner {
			ca: vec!["Zm9vYmFy".to_owned(), "YmF6cXV4".to_owned()],
			log: "bG9na2V5".to_owned(),
			tsa: vec!["dHNhY2VydA==".to_owned()],
			issuer: "https://token.actions.githubusercontent.com".to_owned(),
			identity:
				"https://github.com/acme/color/.github/workflows/release.yml@refs/tags/v1.0.0"
					.to_owned(),
			repository_id: Some("812345678".to_owned()),
			owner_id: Some("98765432".to_owned()),
		})),
		capabilities: Some(ExtensionCapabilities {
			net: CapabilityTargets::Some(vec!["api.color.io:443".to_owned()]),
			functions: CapabilityTargets::All,
			queries: true,
			scripting: false,
			write: true,
			memory: Some(67_108_864),
			timeout: Some(Duration::from_secs(5)),
			strict_timeout: false,
		}),
		bindings: Vec::new(),
		auth_limit: AuthLimit::default(),
	}
}

/// FROZEN. Revision-3 bytes of [`module_v3_sigstore`].
#[rustfmt::skip]
const MODULE_V3_SIGSTORE: &[u8] = &[
	3, 1, 5, 99, 111, 108, 111, 114, 0, 1, 1, 1, 1, 1, 4, 97,
	99, 109, 101, 5, 99, 111, 108, 111, 114, 1, 0, 0, 0, 1, 1, 0,
	32, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93,
	93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93,
	93, 1, 1, 0, 1, 2, 8, 90, 109, 57, 118, 89, 109, 70, 121, 8,
	89, 109, 70, 54, 99, 88, 86, 52, 8, 98, 71, 57, 110, 97, 50, 86,
	53, 1, 12, 100, 72, 78, 104, 89, 50, 86, 121, 100, 65, 61, 61, 43,
	104, 116, 116, 112, 115, 58, 47, 47, 116, 111, 107, 101, 110, 46, 97, 99,
	116, 105, 111, 110, 115, 46, 103, 105, 116, 104, 117, 98, 117, 115, 101, 114,
	99, 111, 110, 116, 101, 110, 116, 46, 99, 111, 109, 76, 104, 116, 116, 112,
	115, 58, 47, 47, 103, 105, 116, 104, 117, 98, 46, 99, 111, 109, 47, 97,
	99, 109, 101, 47, 99, 111, 108, 111, 114, 47, 46, 103, 105, 116, 104, 117,
	98, 47, 119, 111, 114, 107, 102, 108, 111, 119, 115, 47, 114, 101, 108, 101,
	97, 115, 101, 46, 121, 109, 108, 64, 114, 101, 102, 115, 47, 116, 97, 103,
	115, 47, 118, 49, 46, 48, 46, 48, 1, 9, 56, 49, 50, 51, 52, 53,
	54, 55, 56, 1, 8, 57, 56, 55, 54, 53, 52, 51, 50, 1, 1, 1,
	2, 1, 16, 97, 112, 105, 46, 99, 111, 108, 111, 114, 46, 105, 111, 58,
	52, 52, 51, 1, 1, 1, 0, 1, 1, 252, 0, 0, 0, 4, 1, 1,
	5, 0, 0,
];

/// A revision-3 definition with a key signer and default capabilities.
fn module_v3_key() -> StoredModuleDefinition {
	StoredModuleDefinition {
		name: Some("geo".to_owned()),
		comment: Some("key signed".to_owned()),
		permissions: StoredPermission::None,
		executable: ModuleExecutable::Surrealism(SurrealismExecutable {
			bucket: "modules".to_owned(),
			key: "/geo.surli".to_owned(),
		}),
		unsigned: false,
		digest: Some(ManifestDigest::sha256([0x91; 32])),
		signer: Some(ExtensionSigner::Key(
			"ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIK0w release@acme".to_owned(),
		)),
		capabilities: Some(ExtensionCapabilities::default()),
		bindings: Vec::new(),
		auth_limit: AuthLimit::default(),
	}
}

/// FROZEN. Revision-3 bytes of [`module_v3_key`].
#[rustfmt::skip]
const MODULE_V3_KEY: &[u8] = &[
	3, 1, 3, 103, 101, 111, 1, 10, 107, 101, 121, 32, 115, 105, 103, 110,
	101, 100, 1, 0, 1, 0, 1, 7, 109, 111, 100, 117, 108, 101, 115, 10,
	47, 103, 101, 111, 46, 115, 117, 114, 108, 105, 0, 1, 1, 0, 32, 145,
	145, 145, 145, 145, 145, 145, 145, 145, 145, 145, 145, 145, 145, 145, 145, 145,
	145, 145, 145, 145, 145, 145, 145, 145, 145, 145, 145, 145, 145, 145, 145, 1,
	1, 1, 53, 115, 115, 104, 45, 101, 100, 50, 53, 53, 49, 57, 32, 65,
	65, 65, 65, 67, 51, 78, 122, 97, 67, 49, 108, 90, 68, 73, 49, 78,
	84, 69, 53, 65, 65, 65, 65, 73, 75, 48, 119, 32, 114, 101, 108, 101,
	97, 115, 101, 64, 97, 99, 109, 101, 1, 1, 1, 0, 1, 0, 0, 0,
	0, 0, 0, 1,
];

/// A revision-3 definition with `UNSIGNED` and the narrowest capabilities.
fn module_v3_unsigned() -> StoredModuleDefinition {
	StoredModuleDefinition {
		name: Some("scratch".to_owned()),
		comment: None,
		permissions: StoredPermission::Full,
		executable: ModuleExecutable::Surrealism(SurrealismExecutable {
			bucket: "modules".to_owned(),
			key: "/scratch.surli".to_owned(),
		}),
		unsigned: true,
		digest: Some(ManifestDigest::sha256([0x0b; 32])),
		signer: None,
		capabilities: Some(ExtensionCapabilities {
			functions: CapabilityTargets::Some(vec!["geo::*".to_owned()]),
			..Default::default()
		}),
		bindings: Vec::new(),
		auth_limit: AuthLimit::default(),
	}
}

/// FROZEN. Revision-3 bytes of [`module_v3_unsigned`].
#[rustfmt::skip]
const MODULE_V3_UNSIGNED: &[u8] = &[
	3, 1, 7, 115, 99, 114, 97, 116, 99, 104, 0, 1, 1, 1, 0, 1,
	7, 109, 111, 100, 117, 108, 101, 115, 14, 47, 115, 99, 114, 97, 116, 99,
	104, 46, 115, 117, 114, 108, 105, 1, 1, 1, 0, 32, 11, 11, 11, 11,
	11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
	11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 0, 1, 1, 1,
	0, 1, 2, 1, 6, 103, 101, 111, 58, 58, 42, 0, 0, 0, 0, 0,
	1,
];

/// A revision-4 definition with `BINDINGS` and the definer's auth limit.
fn module_v4_bindings() -> StoredModuleDefinition {
	StoredModuleDefinition {
		name: Some("mailer".to_owned()),
		comment: None,
		permissions: StoredPermission::Full,
		executable: ModuleExecutable::Surrealism(SurrealismExecutable {
			bucket: "modules".to_owned(),
			key: "/mailer.surli".to_owned(),
		}),
		unsigned: true,
		digest: Some(ManifestDigest::sha256([0x4e; 32])),
		signer: None,
		capabilities: Some(ExtensionCapabilities::default()),
		bindings: vec![
			StoredExtensionBinding {
				name: "api_key".to_owned(),
				value: ExprText::from_raw("string::uppercase('mailgun')"),
			},
			StoredExtensionBinding {
				name: "author".to_owned(),
				value: ExprText::from_raw("'noreply@acme.com'"),
			},
		],
		auth_limit: AuthLimit::new(
			AuthLevel::Database("acme".to_owned(), "app".to_owned()),
			Some("Editor".to_owned()),
		),
	}
}

/// FROZEN. Revision-4 bytes of [`module_v4_bindings`].
#[rustfmt::skip]
const MODULE_V4_BINDINGS: &[u8] = &[
	4, 1, 6, 109, 97, 105, 108, 101, 114, 0, 1, 1, 1, 0, 1, 7,
	109, 111, 100, 117, 108, 101, 115, 13, 47, 109, 97, 105, 108, 101, 114, 46,
	115, 117, 114, 108, 105, 1, 1, 1, 0, 32, 78, 78, 78, 78, 78, 78,
	78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 78,
	78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 0, 1, 1, 1, 0, 1,
	0, 0, 0, 0, 0, 0, 1, 2, 1, 7, 97, 112, 105, 95, 107, 101,
	121, 28, 115, 116, 114, 105, 110, 103, 58, 58, 117, 112, 112, 101, 114, 99,
	97, 115, 101, 40, 39, 109, 97, 105, 108, 103, 117, 110, 39, 41, 1, 6,
	97, 117, 116, 104, 111, 114, 18, 39, 110, 111, 114, 101, 112, 108, 121, 64,
	97, 99, 109, 101, 46, 99, 111, 109, 39, 1, 1, 3, 4, 97, 99, 109,
	101, 3, 97, 112, 112, 1, 6, 69, 100, 105, 116, 111, 114,
];

/// Revision-2 bytes decode as a definition that records no digest, signer or
/// capabilities, and that therefore must be reinstalled.
#[test]
fn v2_module_decodes_as_a_definition_to_reinstall() {
	let decoded = StoredModuleDefinition::kv_decode_value(MODULE_V2, ()).unwrap_or_else(|e| {
		panic!(
			"BACKWARDS COMPATIBILITY BROKEN: revision-2 StoredModuleDefinition bytes no longer decode.\n\
			 Error: {e}\n\n\
			 Every module a 3.x release defined is revision 2; a node that cannot decode them \
			 skips the row, and the module disappears on upgrade."
		)
	});
	assert_eq!(MODULE_V2.first(), Some(&2), "the frozen fixture must be revision 2");
	let v2 = module_v2();
	assert_eq!(
		decoded,
		StoredModuleDefinition {
			name: v2.name,
			comment: v2.comment,
			permissions: v2.permissions,
			executable: v2.executable,
			unsigned: v2.unsigned,
			digest: None,
			signer: None,
			capabilities: None,
			bindings: Vec::new(),
			auth_limit: AuthLimit::default(),
		}
	);
	assert!(ModuleDefinition::from_stored(&decoded).unwrap().needs_reinstall());
}

/// Writing a decoded revision-2 or revision-3 definition back produces the
/// current revision and reads back unchanged.
#[test]
fn re_encoding_an_older_module_writes_revision_4() {
	for (name, bytes) in [
		("v2", MODULE_V2),
		("sigstore", MODULE_V3_SIGSTORE),
		("key", MODULE_V3_KEY),
		("unsigned", MODULE_V3_UNSIGNED),
	] {
		let decoded = StoredModuleDefinition::kv_decode_value(bytes, ()).expect("decode");
		let re_encoded = decoded.kv_encode_value().expect("encode");
		assert_eq!(re_encoded.first(), Some(&4), "{name}: an upgraded node must write revision 4");
		let round_tripped =
			StoredModuleDefinition::kv_decode_value(&re_encoded, ()).expect("decode");
		assert_eq!(round_tripped, decoded, "{name}");
	}
}

/// Revision-3 bytes decode to exactly the value they were captured from: no
/// bindings, and the auth limit that grants nothing.
#[test]
fn v3_modules_decode_exact() {
	for (name, bytes, expected) in [
		("sigstore", MODULE_V3_SIGSTORE, module_v3_sigstore()),
		("key", MODULE_V3_KEY, module_v3_key()),
		("unsigned", MODULE_V3_UNSIGNED, module_v3_unsigned()),
	] {
		assert_eq!(bytes.first(), Some(&3), "{name}: the frozen fixture must be revision 3");
		let decoded = StoredModuleDefinition::kv_decode_value(bytes, ()).unwrap_or_else(|e| {
			panic!(
				"BACKWARDS COMPATIBILITY BROKEN: revision-3 StoredModuleDefinition bytes ({name}) \
				 no longer decode.\nError: {e}"
			)
		});
		assert_eq!(decoded, expected, "{name}");
		assert!(!ModuleDefinition::from_stored(&decoded).unwrap().needs_reinstall(), "{name}");
	}
}

/// Revision-4 bytes decode to exactly the value they were captured from, and
/// the current encoder reproduces them byte for byte.
#[test]
fn v4_module_decodes_and_encodes_byte_exact() {
	assert_eq!(MODULE_V4_BINDINGS.first(), Some(&4), "the frozen fixture must be revision 4");
	let decoded =
		StoredModuleDefinition::kv_decode_value(MODULE_V4_BINDINGS, ()).unwrap_or_else(|e| {
			panic!(
				"BACKWARDS COMPATIBILITY BROKEN: revision-4 StoredModuleDefinition bytes no \
				 longer decode.\nError: {e}"
			)
		});
	assert_eq!(decoded, module_v4_bindings());
	let definition = ModuleDefinition::from_stored(&decoded).unwrap();
	assert_eq!(definition.bindings.names().collect::<Vec<_>>(), ["api_key", "author"]);
	assert_eq!(
		module_v4_bindings().kv_encode_value().expect("encode"),
		MODULE_V4_BINDINGS,
		"WIRE STABILITY BROKEN: revision 4 no longer encodes to its frozen bytes"
	);
}

/// Every frozen revision, once decoded, round-trips through the stored-value
/// grammar a 4.0 store writes (an entry starting with `0xFE`) and through the
/// VM value the store instructions carry it as.
#[test]
fn every_revision_round_trips_through_the_stored_grammar_and_the_vm() {
	use crate::catalog::stored::StoredField as _;

	for (name, bytes) in [
		("v2", MODULE_V2),
		("sigstore", MODULE_V3_SIGSTORE),
		("key", MODULE_V3_KEY),
		("unsigned", MODULE_V3_UNSIGNED),
		("bindings", MODULE_V4_BINDINGS),
	] {
		let decoded = StoredModuleDefinition::kv_decode_value(bytes, ()).expect("decode");
		let entry = decoded.kv_encode_value_at(&EncodeContext::CURRENT).expect("encode").bytes;
		assert_eq!(entry.first(), Some(&0xFE), "{name}: a 4.0 store writes a stored-grammar entry");
		let read = StoredModuleDefinition::kv_decode_value(&entry, ()).expect("decode entry");
		assert_eq!(read, decoded, "{name}: the stored-grammar entry reads back differently");

		let value = decoded.clone().into_vm_value().expect("to the VM");
		let back = StoredModuleDefinition::from_vm_value(value).expect("from the VM");
		assert_eq!(back, decoded, "{name}: the VM value reads back differently");
	}
}

fn print_fixture(name: &str, bytes: &[u8]) {
	let body = bytes
		.chunks(16)
		.map(|row| {
			let cells: Vec<String> = row.iter().map(|b| b.to_string()).collect();
			format!("\t{},", cells.join(", "))
		})
		.collect::<Vec<_>>()
		.join("\n");
	println!("const {name}: &[u8] = &[\n{body}\n];");
}

/// Regenerate [`MODULE_V2`]. Ignored so it never runs in CI:
///
/// ```text
/// cargo test -p surrealdb-core --lib kvs::compat::module_definition::generate_v2 -- --ignored --nocapture
/// ```
#[test]
#[ignore = "prints the frozen fixture; run by hand when the revision-2 shape is restated"]
fn generate_v2() {
	print_fixture("MODULE_V2", &revision::to_vec(&module_v2()).expect("encode"));
}

/// Regenerate [`MODULE_V4_BINDINGS`]. Run it only on a checkout that writes
/// revision 4:
///
/// ```text
/// cargo test -p surrealdb-core --lib kvs::compat::module_definition::generate_v4 -- --ignored --nocapture
/// ```
#[test]
#[ignore = "prints the frozen fixture; run by hand on a checkout that writes revision 4"]
fn generate_v4() {
	print_fixture("MODULE_V4_BINDINGS", &module_v4_bindings().kv_encode_value().expect("encode"));
}

/// Regenerate the revision-3 constants from the type as revision 3 declares
/// it. Run it only on a checkout that writes revision 3:
///
/// ```text
/// cargo test -p surrealdb-core --lib kvs::compat::module_definition::generate_v3 -- --ignored --nocapture
/// ```
#[test]
#[ignore = "prints the frozen fixtures; run by hand on a checkout that writes revision 3"]
fn generate_v3() {
	print_fixture("MODULE_V3_SIGSTORE", &module_v3_sigstore().kv_encode_value().expect("encode"));
	print_fixture("MODULE_V3_KEY", &module_v3_key().kv_encode_value().expect("encode"));
	print_fixture("MODULE_V3_UNSIGNED", &module_v3_unsigned().kv_encode_value().expect("encode"));
}
