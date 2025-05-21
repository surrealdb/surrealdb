use crate::sql::{
	Part,
	fmt::{Fmt, fmt_separated_by},
};
use revision::revisioned;
use serde::{Deserialize, Serialize};

use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::str;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Idiom";

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
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

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Idiom")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Idiom(pub Vec<Part>);

impl Deref for Idiom {
	type Target = [Part];
	fn deref(&self) -> &Self::Target {
		self.0.as_slice()
	}
}

impl From<String> for Idiom {
	fn from(v: String) -> Self {
		Self(vec![Part::from(v)])
	}
}

impl From<&str> for Idiom {
	fn from(v: &str) -> Self {
		Self(vec![Part::from(v)])
	}
}

impl From<Vec<Part>> for Idiom {
	fn from(v: Vec<Part>) -> Self {
		Self(v)
	}
}

impl From<&[Part]> for Idiom {
	fn from(v: &[Part]) -> Self {
		Self(v.to_vec())
	}
}

impl From<Part> for Idiom {
	fn from(v: Part) -> Self {
		Self(vec![v])
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

impl Idiom {
	/// Appends a part to the end of this Idiom
	pub(crate) fn push(mut self, n: Part) -> Idiom {
		self.0.push(n);
		self
	}
	/// Convert this Idiom to a JSON Path string
	pub(crate) fn to_path(&self) -> String {
		format!("/{self}").replace(']', "").replace(&['.', '['][..], "/")
	}
	/// Simplifies this Idiom for use in object keys
	pub(crate) fn simplify(&self) -> Idiom {
		self.0
			.iter()
			.filter(|&p| matches!(p, Part::Field(_) | Part::Start(_) | Part::Graph(_)))
			.cloned()
			.collect::<Vec<_>>()
			.into()
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
