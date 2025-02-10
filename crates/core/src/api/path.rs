use std::{
	fmt::{self, Display, Formatter},
	ops::Deref,
};

use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::sql::{
	fmt::{fmt_separated_by, Fmt},
	Kind, Object, Value,
};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Path(pub Vec<Segment>);

impl<'a> Path {
	pub fn fit(&'a self, segments: impl Into<&'a [&'a str]>) -> Option<Object> {
		let mut obj = Object::default();
		let segments: &'a [&'a str] = segments.into();
		for (i, segment) in self.iter().enumerate() {
			if let Some(res) = segment.fit(&segments[i..]) {
				if let Some((k, v)) = res {
					obj.insert(k, v);
				}
			} else {
				return None;
			}
		}

		if segments.len() == self.len() || matches!(self.last(), Some(Segment::Rest(_))) {
			Some(obj)
		} else {
			None
		}
	}

	pub fn to_url(&'a self) -> String {
		format!("/{}", self)
	}

	pub fn specifity(&self) -> u8 {
		self.iter().map(|s| s.specificity()).sum()
	}
}

impl From<Vec<Segment>> for Path {
	fn from(segments: Vec<Segment>) -> Self {
		Path(segments)
	}
}

impl Deref for Path {
	type Target = Vec<Segment>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl IntoIterator for Path {
	type Item = Segment;
	type IntoIter = std::vec::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl Display for Path {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&Fmt::new(self.iter(), fmt_separated_by("/")), f)
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Segment {
	Fixed(String),
	Dynamic(String, Option<Kind>),
	Rest(String),
}

pub const MAX_PATH_SPECIFICITY: u8 = 255;
pub const MAX_PATH_SEGMENTS: u8 = MAX_PATH_SPECIFICITY / 3; // 3 is the maximum specificity of a segment

impl Segment {
	fn fit(&self, segments: &[&str]) -> Option<Option<(String, Value)>> {
		if let Some(current) = segments.first() {
			match self {
				Self::Fixed(x) if x == current => Some(None),
				Self::Dynamic(x, k) => {
					let val: Value = current.to_owned().into();
					let val: Option<Value> = match k {
						None => Some(val),
						Some(k) => match val.convert_to(k) {
							Ok(val) => Some(val),
							_ => None,
						},
					};

					val.map(|val| Some((x.to_owned(), val)))
				}
				Self::Rest(x) => Some(Some((x.to_owned(), segments.to_vec().into()))),
				_ => None,
			}
		} else {
			None
		}
	}

	fn specificity(&self) -> u8 {
		match self {
			Self::Fixed(_) => 3,
			Self::Dynamic(_, _) => 2,
			Self::Rest(_) => 1,
		}
	}
}

impl Display for Segment {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Fixed(v) => write!(f, "{v}"),
			Self::Dynamic(v, k) => {
				write!(f, ":{v}")?;
				if let Some(k) = k {
					write!(f, "<{k}>")?;
				}

				Ok(())
			}
			Self::Rest(v) => write!(f, "*{v}"),
		}
	}
}
