use std::fmt::{self, Display, Formatter};
use std::ops::Deref;

use crate::sql::fmt::{Fmt, fmt_separated_by};
use crate::sql::{Ident, Part};

// TODO: Remove unnessacry newtype.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Idioms(pub Vec<Idiom>);

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

impl From<Idioms> for crate::expr::Idioms {
	fn from(v: Idioms) -> Self {
		crate::expr::Idioms(v.0.into_iter().map(Into::into).collect())
	}
}
impl From<crate::expr::Idioms> for Idioms {
	fn from(v: crate::expr::Idioms) -> Self {
		Idioms(v.0.into_iter().map(Into::into).collect())
	}
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Idiom(pub Vec<Part>);

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

	pub fn field(name: Ident) -> Self {
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
		Display::fmt(
			&Fmt::new(
				self.0.iter().enumerate().map(|args| {
					Fmt::new(args, |(i, p), f| match (i, p) {
						(0, Part::Field(v)) => Display::fmt(v, f),
						_ => Display::fmt(p, f),
					})
				}),
				fmt_separated_by(""),
			),
			f,
		)
	}
}
