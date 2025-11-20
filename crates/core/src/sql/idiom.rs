use std::fmt::{self, Display, Formatter};
use std::ops::Deref;

use crate::expr::idiom::Idioms as ExprIdioms;
use crate::fmt::{EscapeIdent, Fmt};
use crate::sql::{Expr, Part};

// TODO: Remove unnessacry newtype.
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

impl Display for Idioms {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&Fmt::comma_separated(&self.0), f)
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
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Idiom(pub(crate) Vec<Part>);

impl Idiom {
	/// Simplifies this Idiom for use in object keys
	pub(crate) fn simplify(&self) -> Idiom {
		Idiom(
			self.0
				.iter()
				.filter(|&p| matches!(p, Part::Field(_) | Part::Start(_) | Part::Graph(_)))
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

impl Display for Idiom {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let mut iter = self.0.iter();
		match iter.next() {
			Some(Part::Field(v)) => EscapeIdent(v).fmt(f)?,
			Some(Part::Start(x)) => match x {
				Expr::Block(_)
				| Expr::Literal(_)
				| Expr::Table(_)
				| Expr::Mock(_)
				| Expr::Constant(_)
				| Expr::Param(_) => x.fmt(f)?,
				_ => {
					write!(f, "({x})")?;
				}
			},
			Some(x) => x.fmt(f)?,
			None => {}
		};
		for p in iter {
			p.fmt(f)?;
		}
		Ok(())
	}
}
