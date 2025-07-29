use crate::sql::Expr;
use crate::sql::fmt::{is_pretty, pretty_indent};

use std::fmt::{self, Display, Formatter, Write};

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

impl Display for Permissions {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "PERMISSIONS")?;
		if self.is_none() {
			return write!(f, " NONE");
		}
		if self.is_full() {
			return write!(f, " FULL");
		}
		let indent = if is_pretty() {
			Some(pretty_indent())
		} else {
			f.write_char(' ')?;
			None
		};

		write!(f, "FOR SELECT ")?;
		match self.select {
			Permission::Specific(_) if is_pretty() => {
				let _indent = pretty_indent();
				self.select.fmt(f)?;
			}
			_ => write!(f, " {}", self.select)?,
		}

		write!(f, "FOR CREATE ")?;
		match self.create {
			Permission::Specific(_) if is_pretty() => {
				let _indent = pretty_indent();
				self.create.fmt(f)?;
			}
			_ => write!(f, " {}", self.create)?,
		}

		write!(f, "FOR UPDATE ")?;
		match self.update {
			Permission::Specific(_) if is_pretty() => {
				let _indent = pretty_indent();
				self.update.fmt(f)?;
			}
			_ => write!(f, " {}", self.update)?,
		}

		write!(f, "FOR DELETE ")?;
		match self.delete {
			Permission::Specific(_) if is_pretty() => {
				let _indent = pretty_indent();
				self.delete.fmt(f)?;
			}
			_ => write!(f, " {}", self.delete)?,
		}

		drop(indent);
		Ok(())
	}
}

impl From<Permissions> for crate::expr::Permissions {
	fn from(v: Permissions) -> Self {
		crate::expr::Permissions {
			select: v.select.into(),
			create: v.create.into(),
			update: v.update.into(),
			delete: v.delete.into(),
		}
	}
}

impl From<crate::expr::Permissions> for Permissions {
	fn from(v: crate::expr::Permissions) -> Self {
		Permissions {
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

impl From<Permission> for crate::expr::Permission {
	fn from(v: Permission) -> Self {
		match v {
			Permission::None => crate::expr::Permission::None,
			Permission::Full => crate::expr::Permission::Full,
			Permission::Specific(v) => crate::expr::Permission::Specific(v.into()),
		}
	}
}

impl From<crate::expr::Permission> for Permission {
	fn from(v: crate::expr::Permission) -> Self {
		match v {
			crate::expr::Permission::None => Self::None,
			crate::expr::Permission::Full => Self::Full,
			crate::expr::Permission::Specific(v) => Self::Specific(v.into()),
		}
	}
}
