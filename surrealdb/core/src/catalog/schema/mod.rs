use revision::revisioned;
use surrealdb_types::ToSql;

mod access;
mod analyzer;
mod api;
pub(crate) mod base;
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
pub(crate) use param::*;
pub use reference::*;
pub use sequence::*;
pub use user::*;

use crate::catalog::{ExprText, FromStored};
use crate::expr::Expr;
use crate::expr::statements::info::InfoStructure;
use crate::sql;
use crate::val::Value;

/// Placeholder substituted for a secret when a catalog definition is serialised
/// through `INFO FOR …`. Credentials (Argon2 hashes, SCRAM verifiers, symmetric
/// and issuer keys) are offline-crackable or directly replayable, so metadata
/// surfaces must never return their real values. Export goes through the
/// `from_definition` paths and is intentionally not redacted.
pub(crate) const REDACTED: &str = "[REDACTED]";

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) enum StoredPermission {
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

impl Permission {
	/// Whether this permission clause directly contains a data-modifying
	/// statement, which is not allowed (GHSA-66r2-5gwj-gxm2).
	///
	/// Reads the guard expression directly. The stored twin has to compile its
	/// text first and treat a parse failure as unsafe; a statement carries the
	/// expression it parsed, so there is nothing that can fail here.
	pub(crate) fn has_direct_write(&self) -> bool {
		match self {
			Permission::None | Permission::Full => false,
			Permission::Specific(expr) => expr.has_direct_write(),
		}
	}

	/// Lowers to the sql-side permission for embedding in a
	/// `sql::Define*Statement` at the INFO/export rendering boundary.
	pub(crate) fn to_sql_permission(&self) -> sql::Permission {
		match self {
			Permission::None => sql::Permission::None,
			Permission::Full => sql::Permission::Full,
			Permission::Specific(e) => sql::Permission::Specific(e.clone().into()),
		}
	}
}

impl InfoStructure for Permission {
	fn structure(self) -> Value {
		match self {
			Permission::None => Value::Bool(false),
			Permission::Full => Value::Bool(true),
			Permission::Specific(e) => Value::from(e.to_stored_sql()),
		}
	}
}

/// Renders the guard expression with the statement-covering parenthesization
/// its canonical stored text carries (`Expr::to_stored_sql`), so the output is
/// byte-identical to the stored form's splice.
impl ToSql for Permission {
	fn fmt_sql(&self, f: &mut String, _fmt: surrealdb_types::SqlFormat) {
		match self {
			Self::None => f.push_str("NONE"),
			Self::Full => f.push_str("FULL"),
			Self::Specific(e) => {
				f.push_str("WHERE ");
				f.push_str(&e.to_stored_sql());
			}
		}
	}
}

impl Permissions {
	/// Lowers to the sql-side permissions for embedding in a
	/// `sql::Define*Statement` at the INFO/export rendering boundary.
	pub(crate) fn to_sql_permissions(&self) -> sql::Permissions {
		sql::Permissions {
			select: self.select.to_sql_permission(),
			create: self.create.to_sql_permission(),
			update: self.update.to_sql_permission(),
			delete: self.delete.to_sql_permission(),
		}
	}
}

impl InfoStructure for Permissions {
	fn structure(self) -> Value {
		Value::from(map! {
			"select" => self.select.structure(),
			"create" => self.create.structure(),
			"update" => self.update.structure(),
			"delete" => self.delete.structure(),
		})
	}
}

impl ToSql for Permissions {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		// One renderer, not two: see `FieldDefinition::to_sql_definition`.
		self.to_sql_permissions().fmt_sql(f, fmt)
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct StoredPermissions {
	pub(crate) select: StoredPermission,
	pub(crate) create: StoredPermission,
	pub(crate) update: StoredPermission,
	pub(crate) delete: StoredPermission,
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

/// Runtime form of [`StoredPermission`]: the guard expression parsed.
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) enum Permission {
	None,
	#[default]
	Full,
	Specific(Expr),
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

impl Permission {
	/// See [`StoredPermission::is_none`]; same predicate on the compiled form.
	pub(crate) fn is_none(&self) -> bool {
		matches!(self, Self::None)
	}

	/// See [`StoredPermission::is_specific`]; same predicate on the compiled
	/// form.
	pub(crate) fn is_specific(&self) -> bool {
		matches!(self, Self::Specific(_))
	}

	pub(crate) fn to_stored(&self) -> StoredPermission {
		match self {
			Permission::None => StoredPermission::None,
			Permission::Full => StoredPermission::Full,
			Permission::Specific(e) => StoredPermission::Specific(ExprText::new(e)),
		}
	}
}

/// Runtime form of [`StoredPermissions`].
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct Permissions {
	pub select: Permission,
	pub create: Permission,
	pub update: Permission,
	pub delete: Permission,
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

impl Permissions {
	/// Whether any of the select/create/update/delete clauses directly contains
	/// a data-modifying statement (GHSA-66r2-5gwj-gxm2).
	pub(crate) fn has_direct_write(&self) -> bool {
		self.select.has_direct_write()
			|| self.create.has_direct_write()
			|| self.update.has_direct_write()
			|| self.delete.has_direct_write()
	}

	pub(crate) fn to_stored(&self) -> StoredPermissions {
		StoredPermissions {
			select: self.select.to_stored(),
			create: self.create.to_stored(),
			update: self.update.to_stored(),
			delete: self.delete.to_stored(),
		}
	}
}
