//! The component names of an array record id.
//!
//! `DEFINE FIELD id ON reading TYPE [city: string, time: datetime]` names the
//! elements of the record id `reading:[city, time]`. The names are syntax over
//! the tuple kind: the kind itself stays an ordinary tuple, and the names ride
//! beside it as a [`KeyNames`] value that the statements and the catalog carry
//! and print.

use std::fmt::{self, Display, Formatter};

use common::fmt::{EscapeIdent, Fmt};
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::Kind;

/// The names a record id's components are given, by position: `names[i]` names
/// `id[i]`. Names are held raw, as the identifier reads once unescaped, and are
/// non-empty in number, pairwise distinct, and never one of the reserved record
/// field names.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct KeyNames(Box<[Strand]>);

/// Why a list of component names is not a valid [`KeyNames`].
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum KeyNamesError {
	/// No names were given.
	#[error("a record id with named components needs at least one component")]
	Empty,
	/// Two components have the same name.
	#[error("the record id component `{0}` is named twice")]
	Duplicate(String),
	/// A component has the name of a field every record already has.
	#[error("the record id component name `{0}` is reserved")]
	Reserved(String),
	/// The names do not match the tuple kind element for element.
	#[error("{names} component names were given for a record id of {kinds} components")]
	Arity {
		names: usize,
		kinds: usize,
	},
}

/// The field names a record id component cannot take.
const RESERVED: [&str; 3] = ["id", "in", "out"];

impl KeyNames {
	/// Validates `names` for a tuple of `arity` elements.
	///
	/// # Errors
	///
	/// [`KeyNamesError`] when `names` is empty, repeats a name, holds a
	/// reserved name, or has a length other than `arity`.
	pub fn new(names: Vec<Strand>, arity: usize) -> Result<Self, KeyNamesError> {
		if names.is_empty() {
			return Err(KeyNamesError::Empty);
		}
		for (i, name) in names.iter().enumerate() {
			if RESERVED.contains(&name.as_str()) {
				return Err(KeyNamesError::Reserved(name.as_str().to_owned()));
			}
			if names[..i].iter().any(|earlier| earlier == name) {
				return Err(KeyNamesError::Duplicate(name.as_str().to_owned()));
			}
		}
		if names.len() != arity {
			return Err(KeyNamesError::Arity {
				names: names.len(),
				kinds: arity,
			});
		}
		Ok(Self(names.into_boxed_slice()))
	}

	/// Whether `name` is a name no component may take.
	pub fn is_reserved(name: &str) -> bool {
		RESERVED.contains(&name)
	}

	/// The position of the component called `name`.
	pub fn position(&self, name: &str) -> Option<u32> {
		self.0.iter().position(|n| n.as_str() == name).map(|i| i as u32)
	}

	/// The number of components.
	pub fn len(&self) -> usize {
		self.0.len()
	}

	/// Always false: a [`KeyNames`] holds at least one name.
	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	/// The names in position order.
	pub fn iter(&self) -> impl Iterator<Item = &str> {
		self.0.iter().map(Strand::as_str)
	}

	/// The names as stored, in position order.
	pub fn as_slice(&self) -> &[Strand] {
		&self.0
	}

	/// The tuple type written with its names: `[city: string, time: datetime]`.
	/// Names print through the identifier escaper, so a keyword or a name with
	/// spaces reads back as the same name. `kinds` pairs with the names
	/// element for element.
	pub fn fmt_keyed<'a>(&'a self, kinds: &'a [Kind]) -> KeyedKinds<'a> {
		KeyedKinds {
			names: self,
			kinds,
		}
	}
}

/// A tuple type with its component names, as [`KeyNames::fmt_keyed`] renders
/// it.
pub struct KeyedKinds<'a> {
	names: &'a KeyNames,
	kinds: &'a [Kind],
}

impl ToSql for KeyedKinds<'_> {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push('[');
		let fmt = fmt.increment();
		write_sql!(
			f,
			fmt,
			"{}",
			Fmt::pretty_comma_separated(self.names.0.iter().zip(self.kinds.iter()).map(|pair| {
				Fmt::new(pair, |(name, kind), f, fmt| {
					write_sql!(f, fmt, "{}: {}", EscapeIdent(name.as_str()), kind)
				})
			}))
		);
		f.push(']');
	}
}

impl Display for KeyedKinds<'_> {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		f.write_str(&self.to_sql())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn names(list: &[&str]) -> Vec<Strand> {
		list.iter().map(Strand::new).collect()
	}

	#[test]
	fn new_accepts_distinct_names_matching_the_arity() {
		let n = KeyNames::new(names(&["city", "time"]), 2).unwrap();
		assert_eq!(n.len(), 2);
		assert_eq!(n.position("city"), Some(0));
		assert_eq!(n.position("time"), Some(1));
		assert_eq!(n.position("value"), None);
		assert_eq!(n.iter().collect::<Vec<_>>(), ["city", "time"]);
	}

	#[test]
	fn new_refuses_each_invalid_list() {
		assert_eq!(KeyNames::new(vec![], 0), Err(KeyNamesError::Empty));
		assert_eq!(
			KeyNames::new(names(&["a", "a"]), 2),
			Err(KeyNamesError::Duplicate("a".to_owned()))
		);
		for reserved in ["id", "in", "out"] {
			assert_eq!(
				KeyNames::new(names(&["a", reserved]), 2),
				Err(KeyNamesError::Reserved(reserved.to_owned()))
			);
		}
		assert_eq!(
			KeyNames::new(names(&["a", "b"]), 3),
			Err(KeyNamesError::Arity {
				names: 2,
				kinds: 3
			})
		);
	}

	#[test]
	fn fmt_keyed_escapes_keywords_and_spaces() {
		let n = KeyNames::new(names(&["my city", "value", "plain"]), 3).unwrap();
		let kinds = [Kind::String, Kind::Int, Kind::Datetime];
		assert_eq!(
			n.fmt_keyed(&kinds).to_string(),
			"[`my city`: string, `value`: int, plain: datetime]"
		);
	}
}
