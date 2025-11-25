use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::sql::{AssignOperator, Expr, Idiom};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct Assignment {
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

impl ToSql for Data {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		match self {
			Self::EmptyExpression => {}
			Self::SetExpression(v) => {
				f.push_str("SET ");
				for (i, arg) in v.iter().enumerate() {
					if i > 0 {
						f.push_str(", ");
					}
					write_sql!(f, sql_fmt, "{} {} ", arg.place, arg.operator);
					arg.value.fmt_sql(f, sql_fmt);
				}
			}
			Self::UnsetExpression(v) => {
				f.push_str("UNSET ");
				for (i, idiom) in v.iter().enumerate() {
					if i > 0 {
						f.push_str(", ");
					}
					write_sql!(f, sql_fmt, "{}", idiom);
				}
			}
			Self::PatchExpression(v) => {
				write_sql!(f, sql_fmt, "PATCH {v}");
			}
			Self::MergeExpression(v) => {
				write_sql!(f, sql_fmt, "MERGE {v}");
			}
			Self::ReplaceExpression(v) => {
				write_sql!(f, sql_fmt, "REPLACE {v}");
			}
			Self::ContentExpression(v) => {
				write_sql!(f, sql_fmt, "CONTENT {v}");
			}
			Self::SingleExpression(v) => v.fmt_sql(f, sql_fmt),
			Self::ValuesExpression(v) => {
				f.push('(');
				if let Some(first) = v.first() {
					for (i, (idiom, _)) in first.iter().enumerate() {
						if i > 0 {
							f.push_str(", ");
						}
						write_sql!(f, sql_fmt, "{idiom}");
					}
				}
				f.push_str(") VALUES ");
				for (i, row) in v.iter().enumerate() {
					if i > 0 {
						f.push_str(", ");
					}
					f.push('(');
					for (j, (_, expr)) in row.iter().enumerate() {
						if j > 0 {
							f.push_str(", ");
						}
						expr.fmt_sql(f, sql_fmt);
					}
					f.push(')');
				}
			}
			Self::UpdateExpression(v) => {
				for (i, arg) in v.iter().enumerate() {
					if i > 0 {
						f.push_str(", ");
					}
					write_sql!(f, sql_fmt, "{} {} ", arg.place, arg.operator);
					arg.value.fmt_sql(f, sql_fmt);
				}
			}
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
