use std::fmt::{self, Display, Formatter};

use crate::expr::expression::VisitExpression;
use crate::expr::{AssignOperator, Expr, Idiom};
use crate::fmt::Fmt;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
#[allow(clippy::enum_variant_names)]
pub(crate) enum Data {
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
pub(crate) struct Assignment {
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
				Fmt::comma_separated(
					v.first().expect("values expression is non-empty").iter().map(|(v, _)| v)
				),
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
