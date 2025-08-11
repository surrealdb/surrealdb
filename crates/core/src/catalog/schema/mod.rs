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
mod table;
mod user;

pub use access::*;
pub use field::*;

use crate::expr::Expr;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum Permission {
	None,
	#[default]
	Full,
	Specific(Expr),
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
