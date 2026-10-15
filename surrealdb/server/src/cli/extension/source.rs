//! The `<source>` argument of `install` and `upgrade`, and the `FROM` clause it
//! becomes.

use std::fmt;

use anyhow::{Context as _, Result, bail};
use common::fmt::EscapeKwFreeIdent;
use semver::Version;
use surrealdb_types::{ToSql as _, Value};

/// Where the archive comes from, as the person typed it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Source {
	/// `silo::org::pkg` or `silo::org::pkg<x.y.z>`.
	Silo {
		org: String,
		pkg: String,
		version: Option<Version>,
	},
	/// `f"bucket:/path.surli"`, a file already in a bucket of the database,
	/// read with `file::get` over the connection.
	Bucket(BucketFile),
}

/// A file in a database bucket.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BucketFile {
	pub bucket: String,
	/// The key, starting with `/`.
	pub key: String,
}

/// The source as a `FROM` clause records it: always exact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DefinedSource {
	Silo {
		org: String,
		pkg: String,
		version: Version,
	},
	Bucket(BucketFile),
}

impl Source {
	pub fn parse(s: &str) -> Result<Self> {
		if let Some(rest) = s.strip_prefix("silo::") {
			return parse_silo(rest);
		}
		if let Some(inner) = s
			.strip_prefix("f\"")
			.and_then(|r| r.strip_suffix('"'))
			.or_else(|| s.strip_prefix("f'").and_then(|r| r.strip_suffix('\'')))
		{
			return BucketFile::parse(inner).map(Source::Bucket);
		}
		bail!(
			"`{s}` is not a source: install from silo::<org>::<package>[<x.y.z>] or from a file \
			 already in a bucket. Upload the archive to a bucket first, e.g. with file::put, then \
			 install from f\"bucket:/…\""
		)
	}
}

fn parse_silo(rest: &str) -> Result<Source> {
	const EXPECTED: &str = "expected silo::<org>::<package> or silo::<org>::<package><x.y.z>";
	// The version follows the package directly (`pkg<1.0.0>`); the older
	// `pkg::<1.0.0>` spelling is read as well.
	let (path, version) = match rest.find('<') {
		Some(i) => {
			let v = rest[i..].strip_prefix('<').and_then(|v| v.strip_suffix('>')).context(
				"expected the version in angle brackets, as in silo::acme::color<1.0.0>",
			)?;
			let version = Version::parse(v)
				.map_err(|e| anyhow::anyhow!("`{v}` is not an exact x.y.z version: {e}"))?;
			(rest[..i].strip_suffix("::").unwrap_or(&rest[..i]), Some(version))
		}
		None => (rest, None),
	};
	let (org, pkg) = match path.split("::").collect::<Vec<_>>().as_slice() {
		[org, pkg] => (*org, *pkg),
		_ => bail!(EXPECTED),
	};
	// The server's rule for these names, shared with the Silo client.
	super::silo::validate_segment("organisation", org)?;
	super::silo::validate_segment("package", pkg)?;
	if let Some(version) = &version {
		super::silo::ensure_exact(version)?;
	}
	Ok(Source::Silo {
		org: org.into(),
		pkg: pkg.into(),
		version,
	})
}

impl BucketFile {
	/// Parses the inside of a file literal: `bucket:/key`.
	pub fn parse(s: &str) -> Result<Self> {
		let Some((bucket, key)) = s.split_once(':') else {
			bail!("expected a file such as f\"modules:/color.surli\"");
		};
		// A f"…" literal cannot escape a bucket name, so it must be plain.
		let plain = bucket.chars().next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
			&& bucket.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
		if !plain {
			bail!("bucket name `{bucket}` must be letters, digits and underscores");
		}
		if !key.starts_with('/') || key.len() < 2 {
			bail!("the file key must start with `/`, as in f\"{bucket}:/color.surli\"");
		}
		// Exactly what a f"…" literal can hold, so the statement records the
		// file that was inspected.
		if let Some(c) =
			key.chars().find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '/')))
		{
			bail!(
				"the file key `{}` contains `{}`; a file key may hold only letters, digits, `_`, `-`, `.` and `/`",
				key.escape_debug(),
				c.escape_debug()
			);
		}
		Ok(Self {
			bucket: bucket.into(),
			key: key.into(),
		})
	}

	/// The file as a value to bind into `file::get` or `file::put`.
	pub fn to_value(&self) -> Value {
		Value::File(surrealdb_types::File::new(self.bucket.clone(), self.key.clone()))
	}
}

impl fmt::Display for BucketFile {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "f\"{}:{}\"", self.bucket, self.key)
	}
}

impl fmt::Display for DefinedSource {
	/// The `FROM` clause's operand.
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			DefinedSource::Silo {
				org,
				pkg,
				version,
			} => write!(
				f,
				"silo::{}::{}<{}.{}.{}>",
				EscapeKwFreeIdent(org).to_sql(),
				EscapeKwFreeIdent(pkg).to_sql(),
				version.major,
				version.minor,
				version.patch
			),
			DefinedSource::Bucket(file) => file.fmt(f),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parses_silo_sources() {
		assert_eq!(
			Source::parse("silo::acme::color::<1.2.3>").unwrap(),
			Source::Silo {
				org: "acme".into(),
				pkg: "color".into(),
				version: Some(Version::new(1, 2, 3)),
			}
		);
		assert_eq!(
			Source::parse("silo::acme::color").unwrap(),
			Source::Silo {
				org: "acme".into(),
				pkg: "color".into(),
				version: None,
			}
		);
		for bad in [
			"silo::acme",
			"silo::acme::color::1.2.3",
			"silo::acme::color<1.2>",
			"silo::a.b::c",
			"silo::acme::color<1.0.0",
		] {
			assert!(Source::parse(bad).is_err(), "{bad} must be refused");
		}
	}

	#[test]
	fn parses_bucket_sources() {
		let file = BucketFile {
			bucket: "modules".into(),
			key: "/color.surli".into(),
		};
		assert_eq!(
			Source::parse(r#"f"modules:/color.surli""#).unwrap(),
			Source::Bucket(file.clone())
		);
		assert_eq!(Source::parse("f'modules:/color.surli'").unwrap(), Source::Bucket(file.clone()));
		assert_eq!(file.to_string(), r#"f"modules:/color.surli""#);
		for bad in
			[r#"f"modules""#, r#"f"modules:color.surli""#, r#"f"mod-ules:/x""#, r#"f"m:/a"b""#]
		{
			assert!(Source::parse(bad).is_err(), "{bad} must be refused");
		}
	}

	#[test]
	fn a_local_path_is_refused() {
		let err = Source::parse("target/acme-color-1.0.0.surli").unwrap_err();
		assert!(err.to_string().contains("Upload the archive to a bucket first"), "{err}");
	}

	#[test]
	fn silo_names_follow_the_server_rule_and_are_escaped() {
		let Source::Silo {
			org,
			pkg,
			version,
		} = Source::parse("silo::acme-co::9color::<1.0.0>").expect("allowed by Silo")
		else {
			panic!("a silo source");
		};
		let defined = DefinedSource::Silo {
			org,
			pkg,
			version: version.unwrap(),
		};
		assert_eq!(defined.to_string(), "silo::`acme-co`::`9color`<1.0.0>");
		for bad in ["silo::a/b::c", "silo::a::b c", "silo::a::b::<1.0.0-rc.1>"] {
			assert!(Source::parse(bad).is_err(), "{bad}");
		}
	}

	#[test]
	fn file_keys_are_what_a_file_literal_can_hold() {
		assert!(Source::parse(r#"f"modules:/v1/color-1.0.0.surli""#).is_ok());
		for bad in
			[r#"f"modules:/a b.surli""#, r#"f"modules:/a:b.surli""#, r#"f"modules:/ä.surli""#]
		{
			assert!(Source::parse(bad).is_err(), "{bad} must be refused");
		}
	}

	#[test]
	fn renders_from_clause() {
		let silo = DefinedSource::Silo {
			org: "acme".into(),
			pkg: "color".into(),
			version: Version::new(1, 0, 0),
		};
		assert_eq!(silo.to_string(), "silo::acme::color<1.0.0>");
		// Both spellings read as the same source.
		assert_eq!(
			Source::parse("silo::acme::color<1.0.0>").unwrap(),
			Source::parse("silo::acme::color::<1.0.0>").unwrap()
		);
	}
}
