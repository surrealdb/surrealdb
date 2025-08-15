use revision::revisioned;

use crate::expr::Expr;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct EventDefinition {
	pub name: String,
	pub target_table: String,
	pub when: Expr,
	pub then: Vec<Expr>,
	pub comment: Option<String>,
}
