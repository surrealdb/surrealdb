use crate::expr::{Data, Expr, Output};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct RelateStatement {
	pub only: bool,
	/// When true, update an existing edge record with the same explicit id.
	pub or_update: bool,
	/// The expression resulting in the table through which we create a relation
	pub through: Expr,
	/// The expression the relation is from
	pub from: Expr,
	/// The expression the relation targets.
	pub to: Expr,
	/// The data associated with the relation being created
	pub data: Option<Data>,
	/// What the result of the statement should resemble (i.e. Diff or no result etc).
	pub output: Option<Output>,
	/// The timeout for the statement
	pub timeout: Expr,
}
