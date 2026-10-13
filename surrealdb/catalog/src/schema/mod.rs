use revision::revisioned;
use surrealdb_types::ToSql;

mod access;
mod analyzer;
mod api;
pub mod base;
mod bucket;
mod config;
mod event;
mod field;
mod function;
mod index;
mod ml;
mod module;
mod param;
mod reference;
mod sequence;
mod user;
use std::fmt::{Display, Formatter};

pub use access::*;
pub use analyzer::*;
pub use api::*;
pub use bucket::*;
pub use config::*;
pub use event::*;
pub use field::*;
pub use function::*;
pub use index::*;
pub use ml::*;
pub use module::*;
pub use param::*;
pub use reference::*;
pub use sequence::*;
pub use user::*;

use crate::catalog::{ExprText, FromStored};
pub use crate::expr::permission::{Permission, Permissions};
use crate::expr::statements::info::InfoStructure;
use crate::val::Value;

/// Placeholder substituted for a secret when a catalog definition is serialised
/// through `INFO FOR …`. Credentials (Argon2 hashes, SCRAM verifiers, symmetric
/// and issuer keys) are offline-crackable or directly replayable, so metadata
/// surfaces must never return their real values. Export goes through the
/// `from_definition` paths and is intentionally not redacted.
pub const REDACTED: &str = "[REDACTED]";

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum StoredPermission {
	None,
	#[default]
	Full,
	/// Canonical SurrealQL text of the permission's guard expression (the
	/// bare expression, not prefixed with `WHERE`). Compiled on demand by
	/// consumers that need to evaluate or analyze it; see
	/// [`crate::catalog::FromStored`] and `exec::permission`.
	Specific(ExprText),
}

impl InfoStructure for StoredPermission {
	fn structure(self) -> Value {
		match self {
			StoredPermission::None => Value::Bool(false),
			StoredPermission::Full => Value::Bool(true),
			StoredPermission::Specific(text) => text.into(),
		}
	}
}

impl ToSql for StoredPermission {
	fn fmt_sql(&self, f: &mut String, _fmt: surrealdb_types::SqlFormat) {
		match self {
			Self::None => f.push_str("NONE"),
			Self::Full => f.push_str("FULL"),
			Self::Specific(text) => {
				f.push_str("WHERE ");
				f.push_str(text.as_str());
			}
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct StoredPermissions {
	pub select: StoredPermission,
	pub create: StoredPermission,
	pub update: StoredPermission,
	pub delete: StoredPermission,
}

impl StoredPermissions {
	pub fn none() -> Self {
		StoredPermissions {
			select: StoredPermission::None,
			create: StoredPermission::None,
			update: StoredPermission::None,
			delete: StoredPermission::None,
		}
	}
}

impl InfoStructure for StoredPermissions {
	fn structure(self) -> Value {
		Value::from(map! {
			"select" => self.select.structure(),
			"create" => self.create.structure(),
			"update" => self.update.structure(),
			"delete" => self.delete.structure(),
		})
	}
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum PermissionKind {
	Select,
	Create,
	Update,
	Delete,
}

impl PermissionKind {
	fn as_str(&self) -> &str {
		match self {
			PermissionKind::Select => "select",
			PermissionKind::Create => "create",
			PermissionKind::Update => "update",
			PermissionKind::Delete => "delete",
		}
	}
}

impl Display for PermissionKind {
	fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
		f.write_str(self.as_str())
	}
}

impl FromStored for Permission {
	type Stored = StoredPermission;

	fn from_stored(stored: &StoredPermission) -> anyhow::Result<Permission> {
		Ok(match stored {
			StoredPermission::None => Permission::None,
			StoredPermission::Full => Permission::Full,
			StoredPermission::Specific(text) => Permission::Specific(text.compile()?),
		})
	}
}

impl FromStored for Permissions {
	type Stored = StoredPermissions;

	fn from_stored(stored: &StoredPermissions) -> anyhow::Result<Permissions> {
		Ok(Permissions {
			select: Permission::from_stored(&stored.select)?,
			create: Permission::from_stored(&stored.create)?,
			update: Permission::from_stored(&stored.update)?,
			delete: Permission::from_stored(&stored.delete)?,
		})
	}
}

impl StoredPermission {
	pub fn from_runtime(p: &Permission) -> StoredPermission {
		match p {
			Permission::None => StoredPermission::None,
			Permission::Full => StoredPermission::Full,
			Permission::Specific(e) => StoredPermission::Specific(ExprText::new(e)),
		}
	}
}

impl StoredPermissions {
	pub fn from_runtime(p: &Permissions) -> StoredPermissions {
		StoredPermissions {
			select: crate::catalog::StoredPermission::from_runtime(&p.select),
			create: crate::catalog::StoredPermission::from_runtime(&p.create),
			update: crate::catalog::StoredPermission::from_runtime(&p.update),
			delete: crate::catalog::StoredPermission::from_runtime(&p.delete),
		}
	}
}

impl std::convert::From<PermissionKind> for crate::iam::Action {
	fn from(kind: PermissionKind) -> Self {
		match kind {
			PermissionKind::Select => crate::iam::Action::View,
			PermissionKind::Create => crate::iam::Action::Edit,
			PermissionKind::Update => crate::iam::Action::Edit,
			PermissionKind::Delete => crate::iam::Action::Edit,
		}
	}
}
