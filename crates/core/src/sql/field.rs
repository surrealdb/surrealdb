use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;

use crate::sql::{Idiom, Part, SqlValue, fmt::Fmt};
use crate::syn;
use anyhow::Result;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::fmt::{self, Display, Formatter, Write};
use std::ops::Deref;

use super::paths::ID;
use super::{Array, FlowResultExt as _};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Fields(pub Vec<Field>, pub bool);

impl Fields {
	/// Create a new `*` field projection
	pub(crate) fn all() -> Self {
		Self(vec![Field::All], false)
	}
	/// Check to see if this field is a `*` projection
	pub fn is_all(&self) -> bool {
		self.0.iter().any(|v| matches!(v, Field::All))
	}
	/// Create a new `VALUE id` field projection
	pub(crate) fn value_id() -> Self {
		Self(
			vec![Field::Single {
				expr: SqlValue::Idiom(Idiom(ID.to_vec())),
				alias: None,
			}],
			true,
		)
	}
	/// Get all fields which are not an `*` projection
	pub fn other(&self) -> impl Iterator<Item = &Field> {
		self.0.iter().filter(|v| !matches!(v, Field::All))
	}
	/// Check to see if this field is a single VALUE clause
	pub fn single(&self) -> Option<&Field> {
		match (self.0.len(), self.1) {
			(1, true) => match self.0.first() {
				Some(Field::All) => None,
				Some(v) => Some(v),
				_ => None,
			},
			_ => None,
		}
	}
	/// Check if the fields are only about counting
	pub(crate) fn is_count_all_only(&self) -> bool {
		let mut is_count_only = false;
		for field in &self.0 {
			if let Field::Single {
				expr: SqlValue::Function(func),
				..
			} = field
			{
				if func.is_count_all() {
					is_count_only = true;
					continue;
				}
			}
			return false;
		}
		is_count_only
	}
}

impl From<Fields> for crate::expr::field::Fields {
	fn from(v: Fields) -> Self {
		Self(v.0.into_iter().map(Into::into).collect(), v.1)
	}
}

impl From<crate::expr::field::Fields> for Fields {
	fn from(v: crate::expr::field::Fields) -> Self {
		Self(v.0.into_iter().map(Into::into).collect(), false)
	}
}

impl Deref for Fields {
	type Target = Vec<Field>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl IntoIterator for Fields {
	type Item = Field;
	type IntoIter = std::vec::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl Display for Fields {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self.single() {
			Some(v) => write!(f, "VALUE {}", &v),
			None => Display::fmt(&Fmt::comma_separated(&self.0), f),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Field {
	/// The `*` in `SELECT * FROM ...`
	#[default]
	All,
	/// The 'rating' in `SELECT rating FROM ...`
	Single {
		expr: SqlValue,
		/// The `quality` in `SELECT rating AS quality FROM ...`
		alias: Option<Idiom>,
	},
}

impl Display for Field {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::All => f.write_char('*'),
			Self::Single {
				expr,
				alias,
			} => {
				Display::fmt(expr, f)?;
				if let Some(alias) = alias {
					f.write_str(" AS ")?;
					Display::fmt(alias, f)
				} else {
					Ok(())
				}
			}
		}
	}
}

impl From<Field> for crate::expr::field::Field {
	fn from(v: Field) -> Self {
		match v {
			Field::All => Self::All,
			Field::Single {
				expr,
				alias,
			} => Self::Single {
				expr: expr.into(),
				alias: alias.map(Into::into),
			},
		}
	}
}

impl From<crate::expr::field::Field> for Field {
	fn from(v: crate::expr::field::Field) -> Self {
		match v {
			crate::expr::field::Field::All => Self::All,
			crate::expr::field::Field::Single {
				expr,
				alias,
			} => Self::Single {
				expr: expr.into(),
				alias: alias.map(Into::into),
			},
		}
	}
}
