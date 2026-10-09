//! Permission guards attached to schema objects.
//!
//! `Permission` is the compiled form of a permission clause: `None`, `Full`,
//! or a guard expression evaluated per row. `Permissions` groups the four
//! CRUD clauses. The stored (text) twins live in the catalog schema layer.

use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::statements::info::InfoStructure;
use crate::expr::{CoverStmts, Expr};
use crate::val::Value;

/// Runtime form of [`StoredPermission`]: the guard expression parsed.
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum Permission {
	None,
	#[default]
	Full,
	Specific(Expr),
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
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		match self {
			Self::None => f.push_str("NONE"),
			Self::Full => f.push_str("FULL"),
			Self::Specific(v) => write_sql!(f, sql_fmt, "WHERE {}", CoverStmts(v)),
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
	pub fn is_none(&self) -> bool {
		matches!(
			(&self.select, &self.create, &self.update, &self.delete),
			(Permission::None, Permission::None, Permission::None, Permission::None)
		)
	}
	pub fn is_full(&self) -> bool {
		matches!(
			(&self.select, &self.create, &self.update, &self.delete),
			(Permission::Full, Permission::Full, Permission::Full, Permission::Full)
		)
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

impl surrealdb_types::ToSql for Permissions {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		self.fmt_sql_actions(f, fmt, true)
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

pub struct FieldPermissions<'a>(pub &'a Permissions);

impl surrealdb_types::ToSql for FieldPermissions<'_> {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		self.0.fmt_sql_actions(f, fmt, false)
	}
}
#[derive(Clone, Copy)]
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
impl Permissions {
	/// Writes the `PERMISSIONS` clause, covering `delete` only when the owner
	/// can express it. See [`FieldPermissions`].
	fn fmt_sql_actions(&self, f: &mut String, fmt: surrealdb_types::SqlFormat, delete: bool) {
		f.push_str("PERMISSIONS");
		if self.is_none() {
			f.push_str(" NONE");
			return;
		}
		if self.is_full() {
			f.push_str(" FULL");
			return;
		}
		let mut lines = Vec::<(Vec<PermissionKind>, &Permission)>::new();
		for (c, permission) in [
			PermissionKind::Select,
			PermissionKind::Create,
			PermissionKind::Update,
			PermissionKind::Delete,
		]
		.into_iter()
		.zip([&self.select, &self.create, &self.update, &self.delete])
		{
			if matches!(c, PermissionKind::Delete) && !delete {
				continue;
			}

			if let Some((existing, _)) = lines.iter_mut().find(|(_, p)| *p == permission) {
				existing.push(c);
			} else {
				lines.push((vec![c], permission));
			}
		}
		if fmt.is_pretty() {
			f.push('\n');
			let inner_fmt = fmt.increment();
			inner_fmt.write_indent(f);
		} else {
			f.push(' ');
		}
		for (i, (kinds, permission)) in lines.into_iter().enumerate() {
			if i > 0 {
				if fmt.is_pretty() {
					f.push(',');
					f.push('\n');
					let inner_fmt = fmt.increment();
					inner_fmt.write_indent(f);
				} else {
					f.push_str(", ");
				}
			}
			f.push_str("FOR ");
			for (i, kind) in kinds.into_iter().enumerate() {
				if i > 0 {
					f.push_str(", ");
				}
				f.push_str(kind.as_str());
			}
			match permission {
				Permission::Specific(v) if fmt.is_pretty() => {
					f.push_str(" WHERE ");
					CoverStmts(v).fmt_sql(f, fmt);
				}
				Permission::None => f.push_str(" NONE"),
				Permission::Full => f.push_str(" FULL"),
				Permission::Specific(v) => {
					f.push_str(" WHERE ");
					CoverStmts(v).fmt_sql(f, fmt);
				}
			}
		}
	}
}
