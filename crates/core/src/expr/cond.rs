use std::fmt;

use super::Expr;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Cond(pub Expr);

impl fmt::Display for Cond {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "WHERE {}", self.0)
	}
}
