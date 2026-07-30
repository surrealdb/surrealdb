use surrealdb_strand::Strand;

use crate::expr::Expr;
use crate::expr::permission::Permission;

/// The api configuration as it is received from ast.

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct ApiConfig {
	pub middleware: Vec<Middleware>,
	pub permissions: Permission,
}

/// The api middleware as it is received from ast.

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct Middleware {
	pub name: Strand,
	pub args: Vec<Expr>,
}
