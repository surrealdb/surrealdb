use crate::expr::{AssignOperator, Expr, Idiom};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
#[allow(clippy::enum_variant_names)]
pub(crate) enum Data {
	/// Represents an empty expression
	#[default]
	EmptyExpression,
	/// Represents a `UPDATE something SET a = b, x = y` clause
	SetExpression(Vec<Assignment>),
	/// Represents a `UPDATE something UNSET x, y, z` clause
	UnsetExpression(Vec<Idiom>),
	/// Represents a `UPDATE something PATCH { ... }` expression
	PatchExpression(Expr),
	/// Represents a `UPDATE something MERGE { ... }` expression
	MergeExpression(Expr),
	/// Represents a `UPDATE something REPLACE { ... }` expression
	ReplaceExpression(Expr),
	/// Represents a `CREATE something CONTENT { ... }` expression
	ContentExpression(Expr),
	/// Represents a `INSERT INTO table { ... }` expression
	SingleExpression(Expr),
	/// Represents a `INSERT INTO table (... fields ...) VALUES (... values ...)` expression
	ValuesExpression(Vec<Vec<(Idiom, Expr)>>),
	/// Represents a `ON DUPLICATE KEY UPDATE ...` clause
	UpdateExpression(Vec<Assignment>),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct Assignment {
	pub place: Idiom,
	pub operator: AssignOperator,
	pub value: Expr,
}
