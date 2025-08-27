use std::fmt::{self, Display, Formatter, Write};
use std::str;

use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::expr::Expr;
use crate::expr::fmt::{is_pretty, pretty_indent, pretty_sequence_item};
use crate::expr::statements::info::InfoStructure;
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
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
		self.select == Permission::None
			&& self.create == Permission::None
			&& self.update == Permission::None
			&& self.delete == Permission::None
	}

	pub fn is_full(&self) -> bool {
		self.select == Permission::Full
			&& self.create == Permission::Full
			&& self.update == Permission::Full
			&& self.delete == Permission::Full
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

impl InfoStructure for Permissions {
	fn structure(self) -> Value {
		Value::from(map! {
			"select".to_string() => self.select.structure(),
			"create".to_string() => self.create.structure(),
			"update".to_string() => self.update.structure(),
			// TODO(gguillemas): Do not show this value for fields in 3.0.0.
			"delete".to_string() => self.delete.structure(),
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
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		f.write_str(self.as_str())
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
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

impl InfoStructure for Permission {
	fn structure(self) -> Value {
		match self {
			Permission::None => Value::Bool(false),
			Permission::Full => Value::Bool(true),
			Permission::Specific(v) => v.to_string().into(),
		}
	}
}
