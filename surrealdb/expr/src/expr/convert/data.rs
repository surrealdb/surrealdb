//! `sql` -> `expr` conversions for [`crate::sql::data`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::data::*;

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
