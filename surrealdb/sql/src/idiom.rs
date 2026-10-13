use common::fmt::EscapeIdent;
use surrealdb_strand::Strand;
use surrealdb_types::write_sql;

use crate::{Expr, Literal, Part};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Idiom(pub Vec<Part>);

impl Idiom {
	/// Simplifies this Idiom for use in object keys
	pub fn simplify(&self) -> Idiom {
		Idiom(
			self.0
				.iter()
				.filter(|&p| matches!(p, Part::Field(_) | Part::Start(_) | Part::Graph(_)))
				.cloned()
				.collect(),
		)
	}

	pub fn field(name: impl Into<Strand>) -> Self {
		Idiom(vec![Part::Field(name.into())])
	}
}

impl surrealdb_types::ToSql for Idiom {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		let mut iter = self.0.iter();
		match iter.next() {
			Some(Part::Field(v)) => EscapeIdent(v).fmt_sql(f, fmt),
			Some(Part::Start(x)) => {
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
			Some(x) => x.fmt_sql(f, fmt),
			None => {}
		};
		for p in iter {
			p.fmt_sql(f, fmt);
		}
	}
}
