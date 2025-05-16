use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::sql::fmt::Fmt;
use crate::sql::idiom::Idiom;
use crate::sql::operator::Operator;
use crate::sql::part::Part;
use crate::sql::paths::ID;
use crate::sql::value::Value;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

use super::FlowResultExt as _;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Data {
	EmptyExpression,
	SetExpression(Vec<(Idiom, Operator, Value)>),
	UnsetExpression(Vec<Idiom>),
	PatchExpression(Value),
	MergeExpression(Value),
	ReplaceExpression(Value),
	ContentExpression(Value),
	SingleExpression(Value),
	ValuesExpression(Vec<Vec<(Idiom, Value)>>),
	UpdateExpression(Vec<(Idiom, Operator, Value)>),
}

impl Default for Data {
	fn default() -> Self {
		Self::EmptyExpression
	}
}

impl From<Data> for crate::expr::Data {
	fn from(v: Data) -> Self {
		match v {
			Data::EmptyExpression => Self::EmptyExpression,
			Data::SetExpression(v) => Self::SetExpression(v.into_iter().map(|(l, o, r)| (l.into(), o.into(), r.into())).collect()),
			Data::UnsetExpression(v) => Self::UnsetExpression(v.into_iter().map(Into::into).collect()),
			Data::PatchExpression(v) => Self::PatchExpression(v.into()),
			Data::MergeExpression(v) => Self::MergeExpression(v.into()),
			Data::ReplaceExpression(v) => Self::ReplaceExpression(v.into()),
			Data::ContentExpression(v) => Self::ContentExpression(v.into()),
			Data::SingleExpression(v) => Self::SingleExpression(v.into()),
			Data::ValuesExpression(v) => Self::ValuesExpression(
				v.into_iter()
					.map(|v| v.into_iter().map(|(i, v)| (i.into(), v.into())).collect())
					.collect(),
			),
			Data::UpdateExpression(v) => Self::UpdateExpression(
				v.into_iter().map(|(l, o, r)| (l.into(), o.into(), r.into())).collect(),
			),
		}
	}
}
impl From<crate::expr::Data> for Data {
	fn from(v: crate::expr::Data) -> Self {
		match v {
			crate::expr::Data::EmptyExpression => Self::EmptyExpression,
			crate::expr::Data::SetExpression(v) => Self::SetExpression(
				v.into_iter().map(|(l, o, r)| (l.into(), o.into(), r.into())).collect(),
			),
			crate::expr::Data::UnsetExpression(v) => Self::UnsetExpression(
				v.into_iter().map(Into::into).collect(),
			),
			crate::expr::Data::PatchExpression(v) => Self::PatchExpression(v.into()),
			crate::expr::Data::MergeExpression(v) => Self::MergeExpression(v.into()),
			crate::expr::Data::ReplaceExpression(v) => Self::ReplaceExpression(v.into()),
			crate::expr::Data::ContentExpression(v) => Self::ContentExpression(v.into()),
			crate::expr::Data::SingleExpression(v) => Self::SingleExpression(v.into()),
			crate::expr::Data::ValuesExpression(v) => Self::ValuesExpression(
				v.into_iter()
					.map(|v| v.into_iter().map(|(i, v)| (i.into(), v.into())).collect())
					.collect(),
			),
			crate::expr::Data::UpdateExpression(v) => Self::UpdateExpression(
				v.into_iter().map(|(l, o, r)| (l.into(), o.into(), r.into())).collect(),
			),
		}
	}
}

crate::sql::impl_display_from_sql!(Data);

impl crate::sql::DisplaySql for Data {
	fn fmt_sql(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::EmptyExpression => Ok(()),
			Self::SetExpression(v) => write!(
				f,
				"SET {}",
				Fmt::comma_separated(
					v.iter().map(|args| Fmt::new(args, |(l, o, r), f| write!(f, "{l} {o} {r}",)))
				)
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
				Fmt::comma_separated(
					v.iter().map(|args| Fmt::new(args, |(l, o, r), f| write!(f, "{l} {o} {r}",)))
				)
			),
		}
	}
}
