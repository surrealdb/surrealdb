use crate::expr::{AssignOperator, Expr, Idiom};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash, priority_lfu::DeepSizeOf)]
#[allow(clippy::enum_variant_names)]
pub(crate) enum Data {
	#[default]
	EmptyExpression,
	SetExpression(Vec<Assignment>),
	UnsetExpression(Vec<Idiom>),
	PatchExpression(Expr),
	MergeExpression(Expr),
	ReplaceExpression(Expr),
	ContentExpression(Expr),
	SingleExpression(Expr),
	ValuesExpression(Vec<Vec<(Idiom, Expr)>>),
	UpdateExpression(Vec<Assignment>),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, priority_lfu::DeepSizeOf)]
pub(crate) struct Assignment {
	pub place: Idiom,
	pub operator: AssignOperator,
	pub value: Expr,
}
