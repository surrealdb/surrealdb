//! Reads an installed extension's definition back from the server, for the
//! upgrade comparison.

use anyhow::{Context as _, Result, bail};
use semver::Version;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb_types::Value;
use tracing::instrument;

use super::capabilities::Capabilities;
use super::signer::Signer;
use super::source::{BucketFile, DefinedSource};
use super::statement::Permissions;

/// What the catalog records for one extension.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Installed {
	pub source: DefinedSource,
	/// `None` for a definition written before `DIGEST` existed.
	pub digest: Option<String>,
	/// `None` is `UNSIGNED`.
	pub signer: Option<Signer>,
	pub capabilities: Capabilities,
	/// `PERMISSIONS`: who may call it.
	pub permissions: Permissions,
	pub comment: Option<String>,
	/// `BINDINGS`: each name and the expression it is bound to.
	pub bindings: Vec<(String, String)>,
}

/// `INFO FOR DB STRUCTURE`, then [`installed_from_info`].
#[instrument(level = "debug", skip(db))]
pub async fn fetch_installed(db: &Surreal<Any>, name: &str) -> Result<Installed> {
	let info: Value = db
		.query("INFO FOR DB STRUCTURE")
		.await
		.context("failed to read the database's definitions")?
		.check()
		.context("failed to read the database's definitions")?
		.take(0)
		.context("failed to read the database's definitions")?;
	installed_from_info(&info, name)
}

/// Finds `name` among the definitions `INFO FOR DB STRUCTURE` returns.
///
/// Each definition is an object with `name`, `executable` (`type: "silo"`
/// with `organisation`, `package`, `major`, `minor`, `patch`, or `type:
/// "surrealism"` with `bucket` and `key`), and the `digest`, `signer` and
/// `capabilities` the statement recorded.
pub fn installed_from_info(info: &Value, name: &str) -> Result<Installed> {
	let Value::Object(info) = info else {
		bail!("INFO FOR DB STRUCTURE did not return an object");
	};
	let items: Vec<&Value> = match info.get("extensions") {
		Some(Value::Array(a)) => a.iter().collect(),
		Some(Value::Object(o)) => o.values().collect(),
		_ => bail!("INFO FOR DB STRUCTURE returned no `extensions`"),
	};
	let found = items.into_iter().find(
		|d| matches!(d, Value::Object(o) if matches!(o.get("name"), Some(Value::String(n)) if n == name)),
	);
	let Some(Value::Object(def)) = found else {
		bail!("no extension ext::{name} is installed in this database");
	};

	let source = match def.get("executable") {
		Some(Value::Object(exe)) => {
			let text = |k: &str| match exe.get(k) {
				Some(Value::String(s)) => Ok(s.clone()),
				_ => bail!("the definition's executable has no `{k}`"),
			};
			let number = |k: &str| match exe.get(k) {
				Some(Value::Number(n)) => {
					n.to_int().map(|i| i as u64).context("not a whole number")
				}
				_ => bail!("the definition's executable has no `{k}`"),
			};
			match exe.get("type") {
				Some(Value::String(t)) if t == "silo" => DefinedSource::Silo {
					org: text("organisation")?,
					pkg: text("package")?,
					version: Version::new(number("major")?, number("minor")?, number("patch")?),
				},
				Some(Value::String(t)) if t == "surrealism" => DefinedSource::Bucket(BucketFile {
					bucket: text("bucket")?,
					key: text("key")?,
				}),
				Some(Value::String(t)) => {
					bail!("ext::{name} has an executable of unknown type `{t}`")
				}
				_ => bail!("the executable of ext::{name} has no `type`"),
			}
		}
		_ => bail!("the definition of ext::{name} has no executable"),
	};
	let digest = match def.get("digest") {
		Some(Value::String(d)) => Some(d.clone()),
		_ => None,
	};
	let signer = Signer::from_value(def.get("signer").unwrap_or(&Value::None))
		.with_context(|| format!("failed to read the SIGNER of ext::{name}"))?;
	let capabilities = match def.get("capabilities") {
		None | Some(Value::None) | Some(Value::Null) => Capabilities::default(),
		Some(v) => Capabilities::from_value(v, &Capabilities::default())
			.with_context(|| format!("failed to read the CAPABILITIES of ext::{name}"))?,
	};
	// `true` is FULL, `false` NONE, and a string the WHERE guard.
	let permissions = match def.get("permissions") {
		Some(Value::Bool(true)) => Permissions::Full,
		// INFO STRUCTURE always lists `permissions`; without it the upgrade
		// would not know what to write back, so it refuses rather than
		// widening to FULL.
		None => bail!("INFO FOR DB STRUCTURE returned no `permissions` for ext::{name}"),
		Some(Value::Bool(false)) => Permissions::None,
		Some(Value::String(guard)) => Permissions::Where(guard.clone()),
		Some(other) => bail!("the PERMISSIONS of ext::{name} are not readable: {}", other.kind()),
	};
	let comment = match def.get("comment") {
		Some(Value::String(c)) => Some(c.clone()),
		_ => None,
	};
	let bindings = match def.get("bindings") {
		None | Some(Value::None) | Some(Value::Null) => Vec::new(),
		Some(Value::Object(o)) => o
			.iter()
			.map(|(binding, expr)| {
				let Value::String(e) = expr else {
					bail!("the BINDINGS of ext::{name} are not readable: `{binding}` is not text");
				};
				Ok((binding.clone(), e.clone()))
			})
			.collect::<Result<_>>()?,
		Some(other) => bail!("the BINDINGS of ext::{name} are not readable: {}", other.kind()),
	};
	Ok(Installed {
		source,
		digest,
		signer,
		capabilities,
		permissions,
		comment,
		bindings,
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	fn parse(s: &str) -> Value {
		surrealdb_expr::parser::value(s).expect("valid SurrealQL value")
	}

	#[test]
	fn reads_a_silo_definition() {
		let info = parse(
			r#"{ extensions: [
				{ name: "other", executable: { type: "surrealism", bucket: "b", key: "/o.surli" } },
				{ name: "color",
				  executable: { type: "silo", organisation: "acme", package: "color", major: 1, minor: 0, patch: 0 },
				  digest: "sha256:5d1a",
				  signer: { key: "ssh-ed25519 AAAA release@acme" },
				  capabilities: { net: ["api.color.io:443"], write: true },
				  permissions: "$auth.admin = true",
				  comment: "palette" }
			] }"#,
		);
		let installed = installed_from_info(&info, "color").expect("found");
		assert_eq!(
			installed,
			Installed {
				bindings: Default::default(),
				source: DefinedSource::Silo {
					org: "acme".into(),
					pkg: "color".into(),
					version: Version::new(1, 0, 0),
				},
				digest: Some("sha256:5d1a".into()),
				signer: Some(Signer::Key("ssh-ed25519 AAAA release@acme".into())),
				capabilities: Capabilities {
					net: vec!["api.color.io:443".into()],
					write: true,
					..Capabilities::default()
				},
				permissions: Permissions::Where("$auth.admin = true".into()),
				comment: Some("palette".into()),
			}
		);
	}

	#[test]
	fn reads_an_unsigned_bucket_definition() {
		let info = parse(
			r#"{ extensions: [ { name: "geo", executable: { type: "surrealism", bucket: "modules", key: "/geo.surli" }, permissions: true, unsigned: true } ] }"#,
		);
		let installed = installed_from_info(&info, "geo").expect("found");
		assert_eq!(installed.signer, None);
		assert_eq!(installed.digest, None);
		assert_eq!(installed.capabilities, Capabilities::default());
		assert_eq!(installed.permissions, Permissions::Full);
		assert_eq!(installed.source.to_string(), r#"f"modules:/geo.surli""#);
	}

	#[test]
	fn an_unknown_executable_type_is_an_error() {
		for (exe, needle) in [
			(r#"{ type: "remote", url: "x" }"#, "unknown type `remote`"),
			(r#"{ bucket: "b", key: "/k" }"#, "has no `type`"),
		] {
			let info =
				parse(&format!(r#"{{ extensions: [ {{ name: "geo", executable: {exe} }} ] }}"#));
			let err = installed_from_info(&info, "geo").unwrap_err();
			assert!(err.to_string().contains(needle), "{err}");
		}
	}

	#[test]
	fn missing_permissions_are_an_error_not_full() {
		let info = parse(
			r#"{ extensions: [ { name: "geo", executable: { type: "surrealism", bucket: "modules", key: "/geo.surli" }, unsigned: true } ] }"#,
		);
		let err = installed_from_info(&info, "geo").unwrap_err();
		assert!(err.to_string().contains("no `permissions`"), "{err}");
	}

	#[test]
	fn reads_only_the_extensions_key() {
		let info = parse(
			r#"{ modules: [ { name: "geo", executable: { type: "surrealism", bucket: "modules", key: "/geo.surli" } } ] }"#,
		);
		assert!(installed_from_info(&info, "geo").is_err());
	}

	#[test]
	fn missing_extension_is_an_error() {
		let err = installed_from_info(&parse("{ extensions: [] }"), "color").unwrap_err();
		assert!(err.to_string().contains("no extension ext::color"), "{err}");
	}
}
