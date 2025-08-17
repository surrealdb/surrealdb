use revision::revisioned;

use crate::catalog::Permission;
use crate::expr::{Expr, Kind};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct FunctionDefinition {
	pub name: String,
	pub args: Vec<(String, Kind)>,
	pub block: Expr,
	pub comment: Option<String>,
	pub permissions: Permission,
	pub returns: Option<Kind>,
}
