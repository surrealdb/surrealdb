//! The statements a client writes: `DEFINE EXTENSION` for an install and
//! `DEFINE EXTENSION OVERWRITE` for an upgrade.
//!
//! An upgrade replaces the whole definition rather than altering it: source,
//! digest, signer and capabilities change together, and `ALTER EXTENSION`
//! changes only `COMMENT` and `PERMISSIONS`.
//!
//! Both carry the full decision: the exact source, the `DIGEST` that pins the
//! code, the `SIGNER` (or `UNSIGNED`) the person accepted, and the
//! `CAPABILITIES` they approved. The server re-fetches the archive from its own
//! source and refuses the statement when the digest or signer does not hold.

use anyhow::{Result, bail};

use super::capabilities::Capabilities;
use super::signer::Signer;
use super::source::DefinedSource;

/// Everything a statement records about one extension.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Decision {
	/// The name after `ext::`.
	pub name: String,
	pub source: DefinedSource,
	/// `sha256:<hex>` of the archive's manifest.
	pub digest: String,
	/// `None` writes `UNSIGNED`.
	pub signer: Option<Signer>,
	pub capabilities: Capabilities,
	/// `PERMISSIONS`. An install leaves it out, so the server's default
	/// applies; an upgrade writes back what the installed definition has,
	/// because `OVERWRITE` replaces every clause.
	pub permissions: Option<Permissions>,
	/// `COMMENT`, carried over by an upgrade for the same reason.
	pub comment: Option<String>,
}

impl Decision {
	/// The `PERMISSIONS` operand the statement records; `FULL` when it leaves
	/// the clause out.
	pub fn permissions_sql(&self) -> String {
		self.permissions.as_ref().map_or_else(|| "FULL".into(), Permissions::to_sql)
	}
}

/// Who may call the extension: the `PERMISSIONS` clause.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Permissions {
	Full,
	None,
	/// The guard expression, as the server renders it.
	Where(String),
}

impl Permissions {
	/// The clause's operand, e.g. `FULL` or `WHERE $auth.admin`.
	pub fn to_sql(&self) -> String {
		match self {
			Permissions::Full => "FULL".into(),
			Permissions::None => "NONE".into(),
			Permissions::Where(guard) => format!("WHERE {guard}"),
		}
	}
}

/// Checks that `name` can stand as `ext::<name>`.
pub fn validate_name(name: &str) -> Result<()> {
	// Any name works: the statement escapes it (`ext::`co-lor``). Only
	// nothing, or a control character, has no useful spelling.
	if name.is_empty() || name.chars().any(char::is_control) {
		bail!(
			"extension name `{}` must not be empty or hold control characters",
			name.escape_debug()
		);
	}
	Ok(())
}

fn body(d: &Decision) -> String {
	let signer = match &d.signer {
		Some(s) => format!("SIGNER {}", s.to_sql()),
		None => "UNSIGNED".to_string(),
	};
	let mut out = format!(
		"ext::{} FROM {}\n    DIGEST {}\n    {signer}\n    CAPABILITIES {}",
		super::sql::segment(&d.name),
		d.source,
		super::sql::quote(&d.digest),
		d.capabilities.to_sql(),
	);
	if let Some(comment) = &d.comment {
		out.push_str(&format!("\n    COMMENT {}", super::sql::quote(comment)));
	}
	if let Some(permissions) = &d.permissions {
		out.push_str(&format!("\n    PERMISSIONS {}", permissions.to_sql()));
	}
	out.push(';');
	out
}

pub fn define(d: &Decision) -> Result<String> {
	validate_name(&d.name)?;
	Ok(format!("DEFINE EXTENSION {}", body(d)))
}

pub fn replace(d: &Decision) -> Result<String> {
	validate_name(&d.name)?;
	Ok(format!("DEFINE EXTENSION OVERWRITE {}", body(d)))
}

#[cfg(test)]
mod tests {
	use semver::Version;

	use super::*;
	use crate::cli::extension::signer::SigstoreSigner;
	use crate::cli::extension::source::BucketFile;

	fn silo_decision() -> Decision {
		Decision {
			name: "color".into(),
			source: DefinedSource::Silo {
				org: "acme".into(),
				pkg: "color".into(),
				version: Version::new(1, 0, 0),
			},
			digest: "sha256:5d1a".into(),
			signer: Some(Signer::Sigstore(SigstoreSigner {
				ca: vec!["ROOT".into(), "INT".into()],
				log: "LOG".into(),
				tsa: vec!["TSA".into()],
				issuer: "https://token.actions.githubusercontent.com".into(),
				identity:
					"https://github.com/acme/color/.github/workflows/release.yml@refs/tags/v1.0.0"
						.into(),
				repository_id: Some("812345678".into()),
				owner_id: Some("98765432".into()),
			})),
			capabilities: Capabilities {
				net: vec!["api.color.io:443".into()],
				functions: vec!["http::get".into()],
				write: true,
				..Capabilities::default()
			},
			permissions: None,
			comment: None,
		}
	}

	#[test]
	fn define_silo_signed() {
		assert_eq!(
			define(&silo_decision()).unwrap(),
			r#"DEFINE EXTENSION ext::color FROM silo::acme::color<1.0.0>
    DIGEST 'sha256:5d1a'
    SIGNER { sigstore: { ca: ['ROOT', 'INT'], log: 'LOG', tsa: ['TSA'], issuer: 'https://token.actions.githubusercontent.com', identity: 'https://github.com/acme/color/.github/workflows/release.yml@refs/tags/v1.0.0', repository_id: '812345678', owner_id: '98765432' } }
    CAPABILITIES { net: ['api.color.io:443'], functions: ['http::get'], write: true };"#
		);
	}

	#[test]
	fn define_bucket_key_signed() {
		let d = Decision {
			name: "geo".into(),
			source: DefinedSource::Bucket(BucketFile {
				bucket: "modules".into(),
				key: "/geo.surli".into(),
			}),
			digest: "sha256:91c4".into(),
			signer: Some(Signer::Key("ssh-ed25519 AAAAC3Nz release@acme".into())),
			capabilities: Capabilities {
				functions: vec!["geo::*".into()],
				..Capabilities::default()
			},
			permissions: None,
			comment: None,
		};
		assert_eq!(
			define(&d).unwrap(),
			r#"DEFINE EXTENSION ext::geo FROM f"modules:/geo.surli"
    DIGEST 'sha256:91c4'
    SIGNER { key: 'ssh-ed25519 AAAAC3Nz release@acme' }
    CAPABILITIES { functions: ['geo::*'] };"#
		);
	}

	#[test]
	fn define_unsigned_with_empty_object() {
		let d = Decision {
			signer: None,
			capabilities: Capabilities::default(),
			..silo_decision()
		};
		let sql = define(&d).unwrap();
		assert!(sql.contains("\n    UNSIGNED\n    CAPABILITIES {};"), "{sql}");
	}

	#[test]
	fn names_are_escaped_not_refused() {
		for (name, written) in
			[("co-lor", "ext::`co-lor`"), ("1color", "ext::`1color`"), ("NaN", "ext::`NaN`")]
		{
			let d = Decision {
				name: name.into(),
				..silo_decision()
			};
			let sql = define(&d).unwrap();
			assert!(sql.starts_with(&format!("DEFINE EXTENSION {written} FROM")), "{sql}");
		}
	}

	#[test]
	fn replace_mirrors_define() {
		let d = silo_decision();
		let define = define(&d).unwrap();
		let replace = replace(&d).unwrap();
		assert_eq!(
			replace.strip_prefix("DEFINE EXTENSION OVERWRITE "),
			define.strip_prefix("DEFINE EXTENSION ")
		);
	}

	#[test]
	fn replace_carries_comment_and_permissions() {
		let d = Decision {
			comment: Some("palette \"v2\"".into()),
			permissions: Some(Permissions::Where("$auth.admin = true".into())),
			..silo_decision()
		};
		let sql = replace(&d).unwrap();
		assert!(
			sql.ends_with(
				"\n    COMMENT 'palette \"v2\"'\n    PERMISSIONS WHERE $auth.admin = true;"
			),
			"{sql}"
		);
	}

	#[test]
	fn refuses_names_that_are_not_identifiers() {
		for bad in ["", "co\nlor"] {
			let d = Decision {
				name: bad.into(),
				..silo_decision()
			};
			assert!(define(&d).is_err(), "`{bad}` must be refused");
		}
	}
}
