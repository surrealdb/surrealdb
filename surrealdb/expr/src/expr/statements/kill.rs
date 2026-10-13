use crate::expr::Expr;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct KillStatement {
	// Uuid of Live Query
	// or Param resolving to Uuid of Live Query
	pub id: Expr,
}
