use revision::revisioned;

mod access;
mod analyzer;
mod api;
mod bucket;
mod config;
mod event;
mod field;
mod function;
mod index;
mod ml;
mod param;
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
pub use param::*;
pub use sequence::*;
pub use user::*;

use crate::expr::Expr;
use crate::expr::statements::info::InfoStructure;
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum Permission {
	None,
	#[default]
	Full,

	// TODO: This should not be stored on disk as an Expr.
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

	fn to_sql_definition(&self) -> crate::sql::Permission {
		match self {
			Permission::None => crate::sql::Permission::None,
			Permission::Full => crate::sql::Permission::Full,
			Permission::Specific(v) => crate::sql::Permission::Specific(v.clone().into()),
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

impl Display for Permission {
	fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
		self.to_sql_definition().fmt(f)
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Base {
	Root,
	Ns,
	Db,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
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

	pub fn to_sql_definition(&self) -> crate::sql::Permissions {
		self.clone().into()
	}
}

impl Display for Permissions {
	fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
		write!(f, "{}", self.to_sql_definition())
	}
}

impl InfoStructure for Permissions {
	fn structure(self) -> Value {
		Value::from(map! {
			"select".to_string() => self.select.structure(),
			"create".to_string() => self.create.structure(),
			"update".to_string() => self.update.structure(),
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
	fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
		f.write_str(self.as_str())
	}
}
