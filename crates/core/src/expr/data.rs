use std::fmt::{self, Display, Formatter};

use anyhow::Result;
use reblessive::tree::Stk;

use super::FlowResultExt as _;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::expr::expression::VisitExpression;
use crate::expr::{AssignOperator, Expr, Idiom, Literal, Part, Value};
use crate::fmt::Fmt;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Data {
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

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Assignment {
	pub place: Idiom,
	pub operator: AssignOperator,
	pub value: Expr,
}

impl VisitExpression for Assignment {
	fn visit<F>(&self, visitor: &mut F)
	where
		F: FnMut(&Expr),
	{
		self.place.visit(visitor);
		self.value.visit(visitor);
	}
}

impl Display for Assignment {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "{} {} {}", self.place, self.operator, self.value)
	}
}

impl Default for Data {
	fn default() -> Self {
		Self::EmptyExpression
	}
}

impl Data {
	/// THIS FUNCTION IS BROKEN, DON'T USE IT ANYWHERE WHERE IT ISN'T ALREADY
	/// BEING USED.
	///
	/// See [`Data::pick`] for why it is broken.
	///
	/// Fetch the 'id' field if one has been specified
	pub(crate) async fn rid(&self, stk: &mut Stk, ctx: &Context, opt: &Options) -> Result<Value> {
		self.pick(stk, ctx, opt, "id").await
	}

	/// THIS FUNCTION IS BROKEN, DON'T USE IT ANYWHERE WHERE IT ISN'T ALREADY
	/// BEING USED.
	///
	/// Fetch a field path value if one is specified
	///
	/// This function computes the expression it has again. This is a mistake. I
	/// causes issues with subqueries where queries are executed twice if they
	/// are in a field picked by this method.
	///
	/// Take `CREATE foo SET id = (CREATE bar:1)`. This query will complain
	/// about br:1 being created twice, because it is. the subquery create is
	/// being computed twice. This issue cannot be fixed without a proper and
	/// major restructuring of the executor.
	pub(crate) async fn pick(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		path: &str,
	) -> Result<Value> {
		match self {
			Self::MergeExpression(v) | Self::ReplaceExpression(v) | Self::ContentExpression(v) => {
				match v {
					Expr::Param(_) => {
						Ok(stk
							.run(|stk| v.compute(stk, ctx, opt, None))
							.await
							.catch_return()?
							// Bad unwrap but this function should be removed anyway and it works
							// with the current calls.
							.pick(&[Part::Field(path.to_owned())]))
					}
					Expr::Literal(Literal::Object(x)) => {
						// Find the field manually, done to replicate previous behavior.
						if let Some(x) = x.iter().find(|x| x.key == path) {
							stk.run(|stk| x.value.compute(stk, ctx, opt, None)).await.catch_return()
						} else {
							Ok(Value::None)
						}
					}
					_ => Ok(Value::None),
				}
			}
			Self::SetExpression(v) => match v.iter().find(|f| f.place.is_field(Some(path))) {
				Some(ass) => {
					stk.run(|stk| ass.value.compute(stk, ctx, opt, None)).await.catch_return()
				}
				// This SET expression does not have this field
				_ => Ok(Value::None),
			},
			// Return nothing
			_ => Ok(Value::None),
		}
	}
}

impl VisitExpression for Data {
	fn visit<F>(&self, visitor: &mut F)
	where
		F: FnMut(&Expr),
	{
		match self {
			Data::EmptyExpression => {}
			Data::SetExpression(x) | Data::UpdateExpression(x) => {
				x.iter().for_each(|a| a.visit(visitor));
			}
			Data::UnsetExpression(x) => {
				x.iter().for_each(|x| x.visit(visitor));
			}
			Data::ValuesExpression(x) => {
				x.iter().for_each(|x| {
					x.iter().for_each(|(i, e)| {
						i.visit(visitor);
						e.visit(visitor);
					})
				});
			}
			Data::PatchExpression(e)
			| Data::MergeExpression(e)
			| Data::ReplaceExpression(e)
			| Data::ContentExpression(e)
			| Data::SingleExpression(e) => {
				e.visit(visitor);
			}
		}
	}
}
impl Display for Data {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::EmptyExpression => Ok(()),
			Self::SetExpression(v) => write!(f, "SET {}", Fmt::comma_separated(v.iter())),
			Self::UnsetExpression(v) => write!(
				f,
				"UNSET {}",
				Fmt::comma_separated(v.iter().map(|args| Fmt::new(args, |l, f| write!(f, "{l}",))))
			),
			Self::PatchExpression(v) => write!(f, "PATCH {v}"),
			Self::MergeExpression(v) => write!(f, "MERGE {v}"),
			Self::ReplaceExpression(v) => write!(f, "REPLACE {v}"),
			Self::ContentExpression(v) => write!(f, "CONTENT {v}"),
			Self::SingleExpression(v) => Display::fmt(v, f),
			Self::ValuesExpression(v) => write!(
				f,
				"({}) VALUES {}",
				Fmt::comma_separated(v.first().unwrap().iter().map(|(v, _)| v)),
				Fmt::comma_separated(v.iter().map(|v| Fmt::new(v, |v, f| write!(
					f,
					"({})",
					Fmt::comma_separated(v.iter().map(|(_, v)| v))
				))))
			),
			Self::UpdateExpression(v) => {
				write!(f, "ON DUPLICATE KEY UPDATE {}", Fmt::comma_separated(v.iter()))
			}
		}
	}
}
