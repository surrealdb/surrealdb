use revision::revisioned;

use crate::catalog::Permission;
use crate::expr::Expr;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct DefineBucketStatement {
	pub name: String,
	pub backend: Option<Expr>,
	pub permissions: Permission,
	pub readonly: bool,
	pub comment: Option<String>,
}
