use std::ops::Deref;

use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::idiom::Idioms as ExprIdioms;
use crate::fmt::{EscapeIdent, Fmt};
use crate::sql::{Expr, Literal, Part};

// TODO: Remove unnecessary newtype.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[allow(dead_code)]
pub(crate) struct Idioms(pub(crate) Vec<Idiom>);

impl Deref for Idioms {
	type Target = Vec<Idiom>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl IntoIterator for Idioms {
	type Item = Idiom;
	type IntoIter = std::vec::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl ToSql for Idioms {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "{}", Fmt::comma_separated(&self.0))
	}
}

impl From<Idioms> for ExprIdioms {
	fn from(v: Idioms) -> Self {
		ExprIdioms(v.0.into_iter().map(Into::into).collect())
	}
}
impl From<ExprIdioms> for Idioms {
	fn from(v: ExprIdioms) -> Self {
		Idioms(v.0.into_iter().map(Into::into).collect())
	}
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Idiom(pub(crate) Vec<Part>);

impl Idiom {
	/// Simplifies this Idiom for use in object keys
	pub(crate) fn simplify(&self) -> Idiom {
		Idiom(
			self.0
				.iter()
				.filter(|&p| matches!(p, Part::Field(_) | Part::Value(_) | Part::Graph(_)))
				.cloned()
				.collect(),
		)
	}

	pub fn field(name: String) -> Self {
		Idiom(vec![Part::Field(name)])
	}
}

impl From<Idiom> for crate::expr::Idiom {
	fn from(v: Idiom) -> Self {
		crate::expr::Idiom(v.0.into_iter().map(Into::into).collect())
	}
}

impl From<crate::expr::Idiom> for Idiom {
	fn from(v: crate::expr::Idiom) -> Self {
		Idiom(v.0.into_iter().map(Into::into).collect())
	}
}

impl surrealdb_types::ToSql for Idiom {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		let mut iter = self.0.iter().enumerate();
		match iter.next() {
			Some((_, Part::Field(v))) => EscapeIdent(v).fmt_sql(f, fmt),
			Some((0, Part::Value(x))) => {
				// First Part::Value: format as expression without brackets
				if x.needs_parentheses()
					|| matches!(x, Expr::Binary { .. } | Expr::Prefix { .. } | Expr::Postfix { .. })
				{
					write_sql!(f, fmt, "({x})");
				} else if let Expr::Literal(Literal::Decimal(d)) = x
					&& d.is_sign_negative()
				{
					write_sql!(f, fmt, "({x})");
				} else if let Expr::Literal(Literal::Integer(i)) = x
					&& i.is_negative()
				{
					write_sql!(f, fmt, "({x})");
				} else if let Expr::Literal(Literal::Float(float)) = x
					&& float.is_sign_negative()
				{
					write_sql!(f, fmt, "({x})");
				} else {
					write_sql!(f, fmt, "{x}");
				}
			}
			Some((_, x)) => x.fmt_sql(f, fmt),
			None => {}
		};
		for (_, p) in iter {
			p.fmt_sql(f, fmt);
		}
	}
}
