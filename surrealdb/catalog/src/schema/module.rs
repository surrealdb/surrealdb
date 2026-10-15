use anyhow::{Context as _, bail};
use revision::revisioned;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::{
	FromStored, LogicalPermissionBody, ModuleExecutable, Permission, StoredPermission,
};
use crate::expr::logical::LogicalPermission;
use crate::expr::statements::DefineModuleStatement;
use crate::expr::statements::define::DefineKind;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{ExtensionCapabilities, ExtensionSigner, ManifestDigest};
use crate::stored::Stored;
use crate::val::Value;
use crate::{expr as runtime, impl_kv_value_catalog};

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub struct StoredModuleDefinition {
	#[stored(n = 2)]
	pub name: Option<String>,
	#[stored(
		n = 1,
		ignorable,
		independent = "a comment is text for people and no other field reads it"
	)]
	pub comment: Option<String>,
	#[stored(n = 4)]
	pub permissions: StoredPermission,
	#[stored(n = 6)]
	pub executable: ModuleExecutable,
	/// Whether the definition states `UNSIGNED`, rendered back as written.
	/// Whether a definition runs is decided by `digest` and `signer`: one
	/// without either clause has no `DIGEST` (see
	/// [`ModuleDefinition::needs_reinstall`]). A revision-2 definition decodes
	/// as `true`; one an import restored without either clause is `false`.
	#[revision(start = 2, default_fn = "default_unsigned")]
	#[stored(n = 8)]
	pub unsigned: bool,
	/// See [`ModuleDefinition::digest`].
	#[revision(start = 3)]
	#[stored(n = 10)]
	pub digest: Option<ManifestDigest>,
	/// See [`ModuleDefinition::signer`].
	#[revision(start = 3)]
	#[stored(n = 12)]
	pub signer: Option<ExtensionSigner>,
	/// See [`ModuleDefinition::capabilities`].
	#[revision(start = 3)]
	#[stored(n = 14)]
	pub capabilities: Option<ExtensionCapabilities>,
}

impl StoredModuleDefinition {
	fn default_unsigned(_revision: u16) -> Result<bool, revision::Error> {
		Ok(true)
	}
}

impl_kv_value_catalog!(StoredModuleDefinition);

impl StoredModuleDefinition {
	/// The name the catalog keys this extension under: `mod::name`, or for an
	/// unnamed silo definition its package coordinates.
	///
	/// The `mod::` prefix is part of the key, not of the language: SurrealQL
	/// calls the extension `ext::name`, and the key keeps the spelling every
	/// release has stored, so no definition needs re-keying.
	pub fn get_storage_name(&self) -> anyhow::Result<String> {
		if let Some(name) = &self.name {
			Ok(format!("mod::{}", name))
		} else if let ModuleExecutable::Silo(silo) = &self.executable {
			Ok(format!(
				"silo::{}::{}::<{}.{}.{}>",
				silo.organisation, silo.package, silo.major, silo.minor, silo.patch
			))
		} else {
			bail!("A module without a name cannot be stored")
		}
	}
}

impl ModuleDefinition {
	fn to_runtime_definition(&self) -> DefineModuleStatement {
		DefineModuleStatement {
			kind: DefineKind::Default,
			name: self.name.clone(),
			executable: self.executable.clone().into(),
			digest: self.digest.clone(),
			signer: self.signer.clone(),
			unsigned: self.unsigned,
			capabilities: self.capabilities.clone(),
			permissions: self.permissions.clone(),
			comment: self
				.comment
				.clone()
				.map(|x| runtime::Expr::Literal(runtime::Literal::String(x.into())))
				.unwrap_or(runtime::Expr::Literal(runtime::Literal::None)),
		}
	}

	/// The name SurrealQL addresses this extension by: `ext::name`, or for an
	/// unnamed silo definition its package coordinates,
	/// `silo::org::pkg<major.minor.patch>`.
	pub fn get_name(&self) -> anyhow::Result<String> {
		match &self.name {
			Some(name) => Ok(format!("ext::{name}")),
			None => self.get_storage_name().map(|n| crate::extension_name(&n).into_owned()),
		}
	}

	/// See [`StoredModuleDefinition::get_storage_name`]; same derivation on the
	/// compiled form.
	pub fn get_storage_name(&self) -> anyhow::Result<String> {
		if let Some(name) = &self.name {
			Ok(format!("mod::{}", name))
		} else if let ModuleExecutable::Silo(silo) = &self.executable {
			Ok(format!(
				"silo::{}::{}::<{}.{}.{}>",
				silo.organisation, silo.package, silo.major, silo.minor, silo.patch
			))
		} else {
			bail!("A module without a name cannot be stored")
		}
	}
}

/// What `INFO … STRUCTURE` says about a definition stored before revision 3.
pub const REINSTALL_WARNING: &str = "This extension was defined before extensions recorded a DIGEST, a SIGNER and CAPABILITIES, so it does not run. Rebuild it for this release and define it again with DEFINE EXTENSION OVERWRITE.";

impl InfoStructure for ModuleDefinition {
	fn structure(self) -> Value {
		let warnings = if self.needs_reinstall() {
			Some(Value::from(vec![Value::from(REINSTALL_WARNING)]))
		} else {
			None
		};
		Value::from(map! {
			"name", if let Some(name) = self.name => name.into(),
			"executable" => self.executable.structure(),
			"digest", if let Some(v) = self.digest => v.to_string().into(),
			"signer", if let Some(v) = self.signer => v.structure(),
			"unsigned", if self.unsigned => true.into(),
			"capabilities", if let Some(v) = self.capabilities => v.structure(),
			"permissions" => self.permissions.structure(),
			"comment", if let Some(v) = self.comment => v.to_sql().into(),
			"warnings", if let Some(v) = warnings => v,
		})
	}
}

impl ToSql for ModuleDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_runtime_definition().fmt_sql(f, fmt)
	}
}

/// Runtime form of [`StoredModuleDefinition`].
///
/// `permissions` and `permissions_body` name the same clause; a constructor
/// sets both from one source ([`LogicalPermissionBody::of`] for a `DEFINE`,
/// the stored text in `from_stored`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModuleDefinition {
	pub name: Option<String>,
	pub executable: ModuleExecutable,
	/// The hash that pins the module's code: `DIGEST`, or computed from the
	/// archive when the statement left it out. `None` only for a definition
	/// stored before revision 3.
	pub digest: Option<ManifestDigest>,
	/// `SIGNER`.
	pub signer: Option<ExtensionSigner>,
	/// See [`StoredModuleDefinition::unsigned`].
	pub unsigned: bool,
	/// `CAPABILITIES`. `None` only for a definition without `DIGEST`, which
	/// grants nothing and does not run; see [`Self::needs_reinstall`].
	pub capabilities: Option<ExtensionCapabilities>,
	pub permissions: Permission,
	/// The `PERMISSIONS` clause in logical form; see
	/// [`Self::permissions_logical`].
	pub permissions_body: LogicalPermissionBody,
	pub comment: Option<String>,
}

impl ModuleDefinition {
	/// The `PERMISSIONS` clause in logical form, compiling its guard on first
	/// call.
	pub fn permissions_logical(&self) -> anyhow::Result<LogicalPermission> {
		self.permissions_body.logical()
	}

	/// Whether the definition records no `DIGEST`: a catalog row from before
	/// revision 3, or an imported definition a release before revision 3
	/// exported. Either was built for an earlier ABI, pins no code, and does
	/// not run until it is defined again, whatever its `unsigned` flag says.
	pub fn needs_reinstall(&self) -> bool {
		self.digest.is_none()
	}
}

impl FromStored for ModuleDefinition {
	type Stored = StoredModuleDefinition;

	fn from_stored(stored: &StoredModuleDefinition) -> anyhow::Result<ModuleDefinition> {
		fn build(stored: &StoredModuleDefinition) -> anyhow::Result<ModuleDefinition> {
			Ok(ModuleDefinition {
				name: stored.name.clone(),
				executable: stored.executable.clone(),
				digest: stored.digest.clone(),
				signer: stored.signer.clone(),
				unsigned: stored.unsigned,
				capabilities: stored.capabilities.clone(),
				permissions: Permission::from_stored(&stored.permissions)?,
				permissions_body: LogicalPermissionBody::from_stored(&stored.permissions),
				comment: stored.comment.clone(),
			})
		}
		build(stored).with_context(|| {
			format!(
				"the stored definition of extension `{}` no longer compiles",
				stored.name.as_deref().unwrap_or("<unnamed>")
			)
		})
	}
}

impl ModuleDefinition {
	pub fn to_stored(&self) -> StoredModuleDefinition {
		StoredModuleDefinition {
			name: self.name.clone(),
			comment: self.comment.clone(),
			permissions: crate::catalog::StoredPermission::from_runtime(&self.permissions),
			executable: self.executable.clone(),
			unsigned: self.unsigned,
			digest: self.digest.clone(),
			signer: self.signer.clone(),
			capabilities: self.capabilities.clone(),
		}
	}
}
