
use crate::sql::{
	fmt::{fmt_separated_by, Fmt},
	paths::{ID, IN, META, OUT},
	Part,
};
use md5::{Digest, Md5};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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

impl From<Idioms> for crate::expr::Idioms {
	fn from(v: Idioms) -> Self {
		Self(v.0.into_iter().map(Into::into).collect())
	}
}

impl From<crate::expr::Idioms> for Idioms {
	fn from(v: crate::expr::Idioms) -> Self {
		Self(v.0.into_iter().map(Into::into).collect())
	}
}

crate::sql::impl_display_from_sql!(Idioms);

impl crate::sql::DisplaySql for Idioms {
	fn fmt_sql(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&Fmt::comma_separated(&self.0), f)
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
		Self(v.0.into_iter().map(Into::into).collect())
	}
}

impl From<crate::expr::Idiom> for Idiom {
	fn from(v: crate::expr::Idiom) -> Self {
		Self(v.0.into_iter().map(Into::into).collect())
	}
}

impl Idiom {
	/// Appends a part to the end of this Idiom
	pub(crate) fn push(mut self, n: Part) -> Idiom {
		self.0.push(n);
		self
	}
	/// Convert this Idiom to a unique hash
	pub(crate) fn to_hash(&self) -> String {
		let mut hasher = Md5::new();
		hasher.update(self.to_string().as_str());
		format!("{:x}", hasher.finalize())
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
	/// Check if this Idiom is an 'id' field
	pub(crate) fn is_id(&self) -> bool {
		self.0.len() == 1 && self.0[0].eq(&ID[0])
	}
	/// Check if this Idiom is a special field such as `id`, `in`, `out` or `meta`.
	pub(crate) fn is_special(&self) -> bool {
		self.0.len() == 1 && [&ID[0], &IN[0], &OUT[0], &META[0]].contains(&&self.0[0])
	}
	/// Check if this Idiom is an specific field
	pub(crate) fn is_field(&self, other: &[Part]) -> bool {
		self.as_ref().eq(other)
	}
	/// Check if this is an expression with multiple yields
	pub(crate) fn is_multi_yield(&self) -> bool {
		self.iter().any(Self::split_multi_yield)
	}
	/// Check if the path part is a yield in a multi-yield expression
	pub(crate) fn split_multi_yield(v: &Part) -> bool {
		matches!(v, Part::Graph(g) if g.alias.is_some())
	}
	/// Check if the path part is a yield in a multi-yield expression
	pub(crate) fn remove_trailing_all(&mut self) {
		if self.ends_with(&[Part::All]) {
			self.0.truncate(self.len() - 1);
		}
	}
	/// Check if this Idiom starts with a specific path part
	pub(crate) fn starts_with(&self, other: &[Part]) -> bool {
		self.0.starts_with(other)
	}
}

impl Idiom {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		self.0.iter().any(|v| v.writeable())
	}
}

crate::sql::impl_display_from_sql!(Idiom);

impl crate::sql::DisplaySql for Idiom {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
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



/// A trie structure for storing idioms.
///
/// This is used for efficient searching and retrieval of idioms based on their path parts.
///
/// Note: This is a simplified version of a trie and does not implement all the features of a full trie.
#[derive(Debug)]
pub(crate) struct IdiomTrie<T> {
	/// The children of this node, indexed by their path part.
	pub(crate) children: HashMap<Part, IdiomTrie<T>>,
	/// The data associated with this node, if any.
	pub(crate) data: Option<T>,
}

impl<T: Clone + std::fmt::Debug> IdiomTrie<T> {
	/// Creates a new empty [`IdiomTrie`].
	pub(crate) fn new() -> Self {
		IdiomTrie {
			children: HashMap::new(),
			data: None,
		}
	}

	/// Inserts a new path and associated data into the trie.
	pub(crate) fn insert(&mut self, path: &[Part], data: T) {
		let mut node = self;
		for part in path {
			node = node.children.entry(part.clone()).or_insert_with(IdiomTrie::new);
		}
		node.data = Some(data);
	}

	/// Checks if the trie contains a path and returns the associated data.
	///
	/// If the path is found, it returns [`IdiomTrieContains::Exact`].
	/// If the path is not found but an ancestor is found, it returns [`IdiomTrieContains::Ancestor`].
	/// If an ancestor is not found, it returns [`IdiomTrieContains::None`].
	pub(crate) fn contains(&self, path: &[Part]) -> IdiomTrieContains<T> {
		let mut node = self;
		let mut last_node_had_data = false;

		for part in path {
			if let Some(child) = node.children.get(part) {
				last_node_had_data = child.data.is_some();
				node = child;
			} else {
				// No more children, stop searching
				last_node_had_data = false;
				break;
			}
		}

		if let Some(data) = node.data.as_ref() {
			if last_node_had_data {
				IdiomTrieContains::Exact(data.clone())
			} else {
				IdiomTrieContains::Ancestor(data.clone())
			}
		} else {
			IdiomTrieContains::None
		}
	}
}

/// The result of a search in the [`IdiomTrie`].
pub(crate) enum IdiomTrieContains<T> {
	/// The path was not found and none of it had no ancestors in the trie.
	None,
	/// The path was found and the data is associated with it.
	Exact(T),
	/// The path was not found, but an ancestor was found.
	Ancestor(T),
}
