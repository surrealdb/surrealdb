use surrealdb_strand::Strand;

use super::DefineKind;
use crate::expr::Expr;
use crate::expr::permission::Permission;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineParamStatement {
	pub kind: DefineKind,
	pub name: Strand,
	pub value: Expr,
	pub comment: Expr,
	pub permissions: Permission,
}
