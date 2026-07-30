//! Permission guards attached to schema objects.
//!
//! `Permission` is the compiled form of a permission clause: `None`, `Full`,
//! or a guard expression evaluated per row. `Permissions` groups the four
//! CRUD clauses. The stored (text) twins live in the catalog schema layer.

use surrealdb_types::ToSql;

use crate::expr::Expr;
use crate::expr::statements::info::InfoStructure;
use crate::sql;
use crate::val::Value;

/// Runtime form of [`StoredPermission`]: the guard expression parsed.
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum Permission {
	None,
	#[default]
	Full,
	Specific(Expr),
}

impl Permission {
	/// Whether this permission clause directly contains a data-modifying
	/// statement, which is not allowed (GHSA-66r2-5gwj-gxm2).
	///
	/// Reads the guard expression directly. The stored twin has to compile its
	/// text first and treat a parse failure as unsafe; a statement carries the
	/// expression it parsed, so there is nothing that can fail here.
	pub fn has_direct_write(&self) -> bool {
		match self {
			Permission::None | Permission::Full => false,
			Permission::Specific(expr) => expr.has_direct_write(),
		}
	}

	/// Lowers to the sql-side permission for embedding in a
	/// `sql::Define*Statement` at the INFO/export rendering boundary.
	pub fn to_sql_permission(&self) -> sql::Permission {
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

/// Runtime form of [`StoredPermissions`].
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct Permissions {
	pub select: Permission,
	pub create: Permission,
	pub update: Permission,
	pub delete: Permission,
}

impl Permissions {
	/// Lowers to the sql-side permissions for embedding in a
	/// `sql::Define*Statement` at the INFO/export rendering boundary.
	pub fn to_sql_permissions(&self) -> sql::Permissions {
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

impl Permission {
	/// See [`StoredPermission::is_none`]; same predicate on the compiled form.
	pub fn is_none(&self) -> bool {
		matches!(self, Self::None)
	}

	/// See [`StoredPermission::is_specific`]; same predicate on the compiled
	/// form.
	pub fn is_specific(&self) -> bool {
		matches!(self, Self::Specific(_))
	}
}

impl Permissions {
	/// Whether any of the select/create/update/delete clauses directly contains
	/// a data-modifying statement (GHSA-66r2-5gwj-gxm2).
	pub fn has_direct_write(&self) -> bool {
		self.select.has_direct_write()
			|| self.create.has_direct_write()
			|| self.update.has_direct_write()
			|| self.delete.has_direct_write()
	}
}
