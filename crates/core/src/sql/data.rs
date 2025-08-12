use std::fmt::{self, Display, Formatter};

use crate::sql::fmt::Fmt;
use crate::sql::{AssignOperator, Expr, Idiom};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Assignment {
	pub place: Idiom,
	pub operator: AssignOperator,
	pub value: Expr,
}

impl From<Assignment> for crate::expr::data::Assignment {
	fn from(value: Assignment) -> Self {
		crate::expr::data::Assignment {
			place: value.place.into(),
			operator: value.operator.into(),
			value: value.value.into(),
		}
	}
}
impl From<crate::expr::data::Assignment> for Assignment {
	fn from(value: crate::expr::data::Assignment) -> Self {
		Assignment {
			place: value.place.into(),
			operator: value.operator.into(),
			value: value.value.into(),
		}
	}
}

impl Default for Data {
	fn default() -> Self {
		Self::EmptyExpression
	}
}

impl Display for Data {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::EmptyExpression => Ok(()),
			Self::SetExpression(v) => write!(
				f,
				"SET {}",
				Fmt::comma_separated(v.iter().map(|args| Fmt::new(args, |arg, f| write!(
					f,
					"{} {} {}",
					arg.place, arg.operator, arg.value
				))))
			),
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
			Self::UpdateExpression(v) => write!(
				f,
				"ON DUPLICATE KEY UPDATE {}",
				Fmt::comma_separated(v.iter().map(|args| Fmt::new(args, |arg, f| write!(
					f,
					"{} {} {}",
					arg.place, arg.operator, arg.value
				))))
			),
		}
	}
}

impl From<Data> for crate::expr::Data {
	fn from(v: Data) -> Self {
		match v {
			Data::EmptyExpression => Self::EmptyExpression,
			Data::SetExpression(v) => Self::SetExpression(v.into_iter().map(Into::into).collect()),
			Data::UnsetExpression(v) => {
				Self::UnsetExpression(v.into_iter().map(Into::into).collect())
			}
			Data::PatchExpression(v) => Self::PatchExpression(v.into()),
			Data::MergeExpression(v) => Self::MergeExpression(v.into()),
			Data::ReplaceExpression(v) => Self::ReplaceExpression(v.into()),
			Data::ContentExpression(v) => Self::ContentExpression(v.into()),
			Data::SingleExpression(v) => Self::SingleExpression(v.into()),
			Data::ValuesExpression(v) => Self::ValuesExpression(
				v.into_iter()
					.map(|v| v.into_iter().map(|(a, b)| (a.into(), b.into())).collect())
					.collect(),
			),
			Data::UpdateExpression(v) => {
				Self::UpdateExpression(v.into_iter().map(Into::into).collect())
			}
		}
	}
}
impl From<crate::expr::Data> for Data {
	fn from(v: crate::expr::Data) -> Self {
		match v {
			crate::expr::Data::EmptyExpression => Self::EmptyExpression,
			crate::expr::Data::SetExpression(v) => {
				Self::SetExpression(v.into_iter().map(Into::into).collect())
			}
			crate::expr::Data::UnsetExpression(v) => {
				Self::UnsetExpression(v.into_iter().map(Into::into).collect())
			}
			crate::expr::Data::PatchExpression(v) => Self::PatchExpression(v.into()),
			crate::expr::Data::MergeExpression(v) => Self::MergeExpression(v.into()),
			crate::expr::Data::ReplaceExpression(v) => Self::ReplaceExpression(v.into()),
			crate::expr::Data::ContentExpression(v) => Self::ContentExpression(v.into()),
			crate::expr::Data::SingleExpression(v) => Self::SingleExpression(v.into()),
			crate::expr::Data::ValuesExpression(v) => Self::ValuesExpression(
				v.into_iter()
					.map(|v| v.into_iter().map(|(a, b)| (a.into(), b.into())).collect())
					.collect(),
			),
			crate::expr::Data::UpdateExpression(v) => {
				Self::UpdateExpression(v.into_iter().map(Into::into).collect())
			}
		}
	}
}
