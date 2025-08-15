use crate::catalog::Permission;
use crate::expr::{Expr, Kind};
use revision::revisioned;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineFunctionStatement {
	pub name: String,
	pub args: Vec<(String, Kind)>,
	pub block: Expr,
	pub comment: Option<String>,
	pub permissions: Permission,
	pub returns: Option<Kind>,
}
