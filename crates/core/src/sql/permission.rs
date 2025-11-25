use std::fmt::Write;

use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::sql::Expr;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct Permissions {
	pub select: Permission,
	pub create: Permission,
	pub update: Permission,
	pub delete: Permission,
}

impl Permissions {
	pub fn none() -> Self {
		Permissions {
			select: Permission::None,
			create: Permission::None,
			update: Permission::None,
			delete: Permission::None,
		}
	}

	pub fn full() -> Self {
		Permissions {
			select: Permission::Full,
			create: Permission::Full,
			update: Permission::Full,
			delete: Permission::Full,
		}
	}

	pub fn is_none(&self) -> bool {
		matches!(self.select, Permission::None)
			&& matches!(self.create, Permission::None)
			&& matches!(self.update, Permission::None)
			&& matches!(self.delete, Permission::None)
	}

	pub fn is_full(&self) -> bool {
		matches!(self.select, Permission::Full)
			&& matches!(self.create, Permission::Full)
			&& matches!(self.update, Permission::Full)
			&& matches!(self.delete, Permission::Full)
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

impl surrealdb_types::ToSql for Permissions {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
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
					v.fmt_sql(f, fmt);
				}
				Permission::None => f.push_str(" NONE"),
				Permission::Full => f.push_str(" FULL"),
				Permission::Specific(v) => {
					f.push_str(" WHERE ");
					v.fmt_sql(f, fmt);
				}
			}
		}
	}
}

impl From<Permissions> for crate::catalog::Permissions {
	fn from(v: Permissions) -> Self {
		Self {
			select: v.select.into(),
			create: v.create.into(),
			update: v.update.into(),
			delete: v.delete.into(),
		}
	}
}

impl From<crate::catalog::Permissions> for Permissions {
	fn from(v: crate::catalog::Permissions) -> Self {
		Self {
			select: v.select.into(),
			create: v.create.into(),
			update: v.update.into(),
			delete: v.delete.into(),
		}
	}
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) enum Permission {
	None,
	#[default]
	Full,
	Specific(Expr),
}

impl ToSql for Permission {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		match self {
			Self::None => write_sql!(f, sql_fmt, "NONE"),
			Self::Full => write_sql!(f, sql_fmt, "FULL"),
			Self::Specific(v) => write_sql!(f, sql_fmt, "WHERE {v}"),
		}
	}
}

impl From<Permission> for crate::catalog::Permission {
	fn from(v: Permission) -> Self {
		match v {
			Permission::None => Self::None,
			Permission::Full => Self::Full,
			Permission::Specific(v) => Self::Specific(v.into()),
		}
	}
}

impl From<crate::catalog::Permission> for Permission {
	fn from(v: crate::catalog::Permission) -> Self {
		match v {
			crate::catalog::Permission::None => Self::None,
			crate::catalog::Permission::Full => Self::Full,
			crate::catalog::Permission::Specific(v) => Self::Specific(v.into()),
		}
	}
}
