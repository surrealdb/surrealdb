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

use crate::catalog::{ExprText, FromStored, LogicalBody};
use crate::expr::logical::{LogicalPermission, LogicalPermissionRef};
pub use crate::expr::permission::{Permission, Permissions};
use crate::expr::statements::info::InfoStructure;
use crate::val::Value;

/// Placeholder substituted for a secret when a catalog definition is serialised
/// through `INFO FOR …`. Credentials (Argon2 hashes, SCRAM verifiers, symmetric
/// and issuer keys) are offline-crackable or directly replayable, so metadata
/// surfaces must never return their real values.
///
/// Export does not treat its two kinds of secret alike, and a reader deciding
/// whether a dump carries real key material has to know which:
///
/// - an access is redacted (`kvs::export` calls [`DefineAccessStatement::redact`]), so a dump names
///   this placeholder as the signing secret and a database restored from it verifies tokens anyone
///   can forge;
/// - a user is not, so a dump carries the real `PASSHASH` and `PASSSCRAM` — to any principal
///   holding `View` on the database, which is less than `INFO FOR DB` requires of the same values.
///
/// Neither half is the intended end state: the first trades disclosure for
/// forgery, and the second discloses to a lower bar than the metadata surface
/// it mirrors.
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

/// A `PERMISSIONS` clause with its guard compiled to the logical form on first
/// use, for the streaming engine's analyzer.
///
/// Carried on a runtime definition beside the [`Permission`] the legacy engine
/// evaluates. Both are compiled from the same stored text and neither is
/// derived from the other; see [`LogicalBody`].
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum LogicalPermissionBody {
	None,
	#[default]
	Full,
	Specific(LogicalBody),
}

impl LogicalPermissionBody {
	/// The body of a stored permission, compiled lazily.
	pub fn from_stored(stored: &StoredPermission) -> Self {
		match stored {
			StoredPermission::None => LogicalPermissionBody::None,
			StoredPermission::Full => LogicalPermissionBody::Full,
			StoredPermission::Specific(text) => {
				LogicalPermissionBody::Specific(LogicalBody::new(text))
			}
		}
	}

	/// The body of a permission a `DEFINE` statement produced, over the text
	/// `to_stored` writes for it.
	pub fn of(permission: &Permission) -> Self {
		match permission {
			Permission::None => LogicalPermissionBody::None,
			Permission::Full => LogicalPermissionBody::Full,
			Permission::Specific(expr) => LogicalPermissionBody::Specific(LogicalBody::of(expr)),
		}
	}

	/// The permission in logical form, compiling the guard on first call.
	///
	/// The guard is copied out of the body, so this is for a caller that keeps
	/// it — one putting it into a plan. A caller that only reads it takes
	/// [`Self::logical_ref`], which is the same clause without the copy.
	pub fn logical(&self) -> anyhow::Result<LogicalPermission> {
		Ok(match self.logical_ref()? {
			LogicalPermissionRef::None => LogicalPermission::None,
			LogicalPermissionRef::Full => LogicalPermission::Full,
			LogicalPermissionRef::Specific(guard) => LogicalPermission::Specific(guard.clone()),
		})
	}

	/// The permission in logical form, compiling the guard on first call, with
	/// the guard borrowed from this body rather than copied out of it.
	///
	/// A stored clause is judged per evaluation — a `DEFINE PARAM` guard
	/// against the row the parameter is read on, a function's against the
	/// caller's frame — so a caller on that path reads the clause far more
	/// often than any statement plans it, and copying the guard out to read it
	/// puts the copy in a row loop.
	pub fn logical_ref(&self) -> anyhow::Result<LogicalPermissionRef<'_>> {
		Ok(match self {
			LogicalPermissionBody::None => LogicalPermissionRef::None,
			LogicalPermissionBody::Full => LogicalPermissionRef::Full,
			LogicalPermissionBody::Specific(body) => {
				LogicalPermissionRef::Specific(body.logical()?)
			}
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
