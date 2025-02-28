use std::{
	fmt::{self, Display, Formatter},
	ops::Deref,
	str::FromStr,
};

use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::{
	err::Error,
	sql::{
		fmt::{fmt_separated_by, Fmt},
		Kind, Object, Value,
	},
	syn,
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
		write!(f, "/")?;
		Display::fmt(&Fmt::new(self.iter(), fmt_separated_by("/")), f)
	}
}

impl FromStr for Path {
	type Err = Error;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		if s.is_empty() {
			return Err(Error::InvalidPath("Path cannot be empty".into()));
		}

		let mut chars = s.chars().peekable();
		let mut segments: Vec<Segment> = Vec::new();

		while let Some(c) = chars.next() {
			if c != '/' {
				return Err(Error::InvalidPath("Segment should start with /".into()));
			}

			let mut scratch = String::new();
			let mut kind: Option<Kind> = None;

			'segment: while let Some(c) = chars.peek() {
				match c {
					'/' if scratch.is_empty() => {
						chars.next();
						continue 'segment;
					}

					// We allow the first character to be an escape character to ignore potential otherwise instruction characters
					'\\' if scratch.is_empty() => {
						chars.next();
						if let Some(x @ ':' | x @ '*') = chars.next() {
							scratch.push('\\');
							scratch.push(x);
							continue 'segment;
						} else {
							return Err(Error::InvalidPath("Expected an instruction symbol `:` or `*` to follow after an escape character".into()));
						}
					}

					// Valid segment characters
					x if x.is_ascii_alphanumeric() => (),
					'.' | '-' | '_' | '~' | '!' | '$' | '&' | '\'' | '(' | ')' | '*' | '+'
					| ',' | ';' | '=' | ':' | '@' => (),

					// We found a kind
					'<' if scratch.starts_with(':') => {
						if scratch.len() == 1 {
							return Err(Error::InvalidPath(
								"Encountered a type, but expected a name or content for this segment first".into(),
							));
						}

						// Eat the '<'
						chars.next();

						let mut balance = 0;
						let mut inner = String::new();

						'kind: loop {
							let Some(c) = chars.next() else {
								return Err(Error::InvalidPath(
									"Kind segment did not close".into(),
								));
							};

							// Keep track of the balance
							if c == '<' {
								balance += 1;
							} else if c == '>' {
								if balance == 0 {
									break 'kind;
								} else {
									balance -= 1;
								}
							}

							inner.push(c);
						}

						kind =
							Some(syn::kind(&inner).map_err(|e| Error::InvalidPath(e.to_string()))?);

						break 'segment;
					}

					// We did not encounter a valid character
					_ => {
						break 'segment;
					}
				}

				if let Some(c) = chars.next() {
					scratch.push(c);
				} else {
					return Err(Error::Unreachable(
						"Expected to find a character as we peeked it before".into(),
					));
				}
			}

			let (segment, done) = if scratch.is_empty() {
				break;
			} else if (scratch.starts_with(':')
				|| scratch.starts_with('*')
				|| scratch.starts_with('\\'))
				&& scratch[1..].is_empty()
			{
				// We encountered a segment which starts with an instruction, but is empty
				// Let's error
				return Err(Error::InvalidPath(
					"Expected a name or content for this segment".into(),
				));
			} else if let Some(name) = scratch.strip_prefix(':') {
				let segment = Segment::Dynamic(name.to_string(), kind);
				(segment, false)
			} else if let Some(name) = scratch.strip_prefix('*') {
				let segment = Segment::Rest(name.to_string());
				(segment, true)
			} else if let Some(name) = scratch.strip_prefix('\\') {
				let segment = Segment::Fixed(name.to_string());
				(segment, false)
			} else {
				let segment = Segment::Fixed(scratch.to_string());
				(segment, false)
			};

			segments.push(segment);

			if done {
				break;
			}
		}

		if chars.peek().is_some() {
			return Err(Error::InvalidPath("Path not finished".into()));
		}

		if segments.len() > MAX_PATH_SEGMENTS as usize {
			return Err(Error::InvalidPath(format!(
				"Path cannot have more than {MAX_PATH_SEGMENTS} segments"
			)));
		}

		Ok(Self(segments))
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
						Some(k) => val.convert_to(k).ok(),
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
