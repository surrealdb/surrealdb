use revision::revisioned;

mod access;
mod analyzer;
mod api;
mod bucket;
mod event;
mod field;
mod function;
mod index;
mod param;
mod sequence;
mod user;

pub use access::*;
pub use analyzer::*;
pub use field::*;
pub use user::*;

use crate::{
	expr::{Expr, statements::info::InfoStructure},
	val::Value,
};

#[revisioned(revision = 1)]
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
			Permission::Specific(v) => v.to_string().into(),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct Permissions {
	pub select: Permission,
	pub create: Permission,
	pub update: Permission,
	pub delete: Permission,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Base {
	Root,
	Ns,
	Db,
}
