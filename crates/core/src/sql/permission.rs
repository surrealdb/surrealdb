use std::fmt::{self, Display, Formatter, Write};

use crate::sql::Expr;
use crate::sql::fmt::{is_pretty, pretty_indent, pretty_sequence_item};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Permissions {
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

impl Display for PermissionKind {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		f.write_str(self.as_str())
	}
}

impl Display for Permissions {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "PERMISSIONS")?;
		if self.is_none() {
			return write!(f, " NONE");
		}
		if self.is_full() {
			return write!(f, " FULL");
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
			// Alternate permissions display implementation ignores delete permission
			// This display is used to show field permissions, where delete has no effect
			// Displaying the permission could mislead users into thinking it has an effect
			// Additionally, including the permission will cause a parsing error in 3.0.0
			if f.alternate() && matches!(c, PermissionKind::Delete) {
				continue;
			}

			if let Some((existing, _)) = lines.iter_mut().find(|(_, p)| *p == permission) {
				existing.push(c);
			} else {
				lines.push((vec![c], permission));
			}
		}
		let indent = if is_pretty() {
			Some(pretty_indent())
		} else {
			f.write_char(' ')?;
			None
		};
		for (i, (kinds, permission)) in lines.into_iter().enumerate() {
			if i > 0 {
				if is_pretty() {
					pretty_sequence_item();
				} else {
					f.write_str(", ")?;
				}
			}
			write!(f, "FOR ")?;
			for (i, kind) in kinds.into_iter().enumerate() {
				if i > 0 {
					f.write_str(", ")?;
				}
				f.write_str(kind.as_str())?;
			}
			match permission {
				Permission::Specific(_) if is_pretty() => {
					let _indent = pretty_indent();
					Display::fmt(permission, f)?;
				}
				_ => write!(f, " {permission}")?,
			}
		}
		drop(indent);
		Ok(())
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
pub enum Permission {
	None,
	#[default]
	Full,
	Specific(Expr),
}

impl Permission {
	pub fn is_none(&self) -> bool {
		matches!(self, Self::None)
	}

	pub fn is_full(&self) -> bool {
		matches!(self, Self::Full)
	}

	pub fn is_specific(&self) -> bool {
		matches!(self, Self::Specific(_))
	}
}

impl Display for Permission {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::None => f.write_str("NONE"),
			Self::Full => f.write_str("FULL"),
			Self::Specific(v) => write!(f, "WHERE {v}"),
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
