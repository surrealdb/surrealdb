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
pub use sequence::*;
pub use user::*;

use crate::expr::Expr;
use crate::expr::statements::info::InfoStructure;
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) enum Permission {
	None,
	#[default]
	Full,
	Specific(Expr),
}

impl Permission {
	pub fn is_none(&self) -> bool {
		matches!(self, Self::None)
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
			Permission::Specific(v) => v.to_sql().into(),
		}
	}
}

impl ToSql for Permission {
	fn fmt_sql(&self, f: &mut String, sql_fmt: surrealdb_types::SqlFormat) {
		self.to_sql_definition().fmt_sql(f, sql_fmt);
	}
}

impl ToSql for Permissions {
	fn fmt_sql(&self, f: &mut String, sql_fmt: surrealdb_types::SqlFormat) {
		self.to_sql_definition().fmt_sql(f, sql_fmt);
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct Permissions {
	pub(crate) select: Permission,
	pub(crate) create: Permission,
	pub(crate) update: Permission,
	pub(crate) delete: Permission,
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

	pub fn to_sql_definition(&self) -> crate::sql::Permissions {
		self.clone().into()
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
