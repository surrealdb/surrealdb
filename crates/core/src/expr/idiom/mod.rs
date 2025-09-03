use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::str::FromStr;

use md5::{Digest, Md5};
use reblessive::Stack;
use reblessive::tree::Stk;
use revision::Revisioned;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::fmt::Fmt;
use crate::expr::part::{Next, NextMethod};
use crate::expr::paths::{ID, IN, OUT};
use crate::expr::statements::info::InfoStructure;
use crate::expr::{FlowResult, Ident, Part, Value};

pub mod recursion;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
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

impl InfoStructure for Idioms {
	fn structure(self) -> Value {
		self.to_string().into()
	}
}

/// An idiom defines a way to reference a field, reference, or other part of the document graph.
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct Idiom(pub Vec<Part>);

impl Deref for Idiom {
	type Target = [Part];
	fn deref(&self) -> &Self::Target {
		self.0.as_slice()
	}
}

impl From<Vec<Part>> for Idiom {
	fn from(v: Vec<Part>) -> Self {
		Self(v)
	}
}

impl Idiom {
	/// Returns an idiom for a field of the given name.
	pub fn field(field_name: Ident) -> Self {
		Idiom(vec![Part::Field(field_name)])
	}

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

	/// Simplifies this Idiom for use in object keys
	pub(crate) fn simplify(&self) -> Idiom {
		self.0
			.iter()
			.filter(|&p| matches!(p, Part::Field(_) | Part::Start(_) | Part::Lookup(_)))
			.cloned()
			.collect::<Vec<_>>()
			.into()
	}
	/// Check if this Idiom is an 'id' field
	pub(crate) fn is_id(&self) -> bool {
		self.0.len() == 1 && self.0[0].eq(&ID[0])
	}
	/// Check if this Idiom is a special field such as `id`, `in` or `out`.
	pub(crate) fn is_special(&self) -> bool {
		self.0.len() == 1 && [&ID[0], &IN[0], &OUT[0]].contains(&&self.0[0])
	}
	/// Check if this Idiom is an specific field
	pub(crate) fn is_field(&self, other: &str) -> bool {
		if self.len() != 1 {
			return false;
		}

		let Part::Field(ref x) = self.0[0] else {
			return false;
		};

		x.as_str() == other
	}

	/// Returns a raw string representation of this idiom without any escaping.
	pub(crate) fn to_raw_string(&self) -> String {
		let mut s = String::new();

		let mut iter = self.0.iter();
		match iter.next() {
			Some(Part::Field(v)) => s.push_str(&v.to_raw_string()),
			Some(x) => s.push_str(&x.to_raw_string()),
			None => {}
		};

		for p in iter {
			s.push_str(&p.to_raw_string());
		}

		s
	}

	/// Check if this is an expression with multiple yields
	pub(crate) fn is_multi_yield(&self) -> bool {
		self.iter().any(Self::part_is_multi_yield)
	}
	/// Check if the path part is a yield in a multi-yield expression
	pub(crate) fn part_is_multi_yield(v: &Part) -> bool {
		matches!(v, Part::Lookup(g) if g.alias.is_some())
	}

	/// Check if this Idiom starts with a specific path part
	pub(crate) fn starts_with(&self, other: &[Part]) -> bool {
		self.0.starts_with(other)
	}

	/// Check if we require a writeable transaction
	pub(crate) fn read_only(&self) -> bool {
		self.0.iter().all(|v| v.read_only())
	}
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> FlowResult<Value> {
		match self.first() {
			// The starting part is a value
			Some(Part::Start(v)) => {
				stk.run(|stk| v.compute(stk, ctx, opt, doc))
					.await?
					.get(stk, ctx, opt, doc, self.as_ref().next())
					.await
			}
			// Otherwise use the current document
			_ => match doc {
				// There is a current document
				Some(v) => v.doc.as_ref().get(stk, ctx, opt, doc, self).await,
				// There isn't any document
				None => Value::None.get(stk, ctx, opt, doc, self.next_method()).await,
			},
		}
	}
}

impl Display for Idiom {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let mut iter = self.0.iter();
		// TODO: Look at why the first Part::Field is formatted differently.
		match iter.next() {
			Some(Part::Field(v)) => v.fmt(f)?,
			Some(x) => x.fmt(f)?,
			None => {}
		};
		for p in iter {
			p.fmt(f)?;
		}
		Ok(())
	}
}

impl FromStr for Idiom {
	type Err = revision::Error;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let buf = s.as_bytes();
		let mut stack = Stack::new();
		let mut parser = crate::syn::parser::Parser::new_with_experimental(buf, true);
		let expr = stack
			.enter(|stk| parser.parse_expr(stk))
			.finish()
			.map_err(|err| revision::Error::Conversion(format!("{err:?}")))?;

		match expr {
			crate::sql::Expr::Idiom(idiom) => Ok(idiom.into()),
			_ => Err(revision::Error::Conversion("Expected an idiom".to_string())),
		}
	}
}

impl Revisioned for Idiom {
	fn revision() -> u16 {
		1
	}

	fn serialize_revisioned<W: std::io::Write>(
		&self,
		writer: &mut W,
	) -> Result<(), revision::Error> {
		self.to_raw_string().serialize_revisioned(writer)?;
		Ok(())
	}

	fn deserialize_revisioned<R: std::io::Read>(reader: &mut R) -> Result<Self, revision::Error> {
		let s: String = Revisioned::deserialize_revisioned(reader)?;
		let idiom =
			Idiom::from_str(&s).map_err(|err| revision::Error::Conversion(format!("{err:?}")))?;
		Ok(idiom)
	}
}

impl InfoStructure for Idiom {
	fn structure(self) -> Value {
		self.to_string().into()
	}
}

/// A trie structure for storing idioms.
///
/// This is used for efficient searching and retrieval of idioms based on their
/// path parts.
///
/// Note: This is a simplified version of a trie and does not implement all the
/// features of a full trie.
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
	/// If the path is not found but an ancestor is found, it returns
	/// [`IdiomTrieContains::Ancestor`]. If an ancestor is not found, it
	/// returns [`IdiomTrieContains::None`].
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

#[cfg(test)]
mod tests {
	use rstest::rstest;

	use super::*;
	use crate::val::Strand;

	#[rstest]
	#[case(Idiom::from(vec![Part::Field(Ident::from_strand(Strand::new_lossy("name".to_string())))]), "name")]
	#[case(Idiom::from(vec![Part::Field(Ident::from_strand(Strand::new_lossy("nested".to_string()))), Part::Field(Ident::from_strand(Strand::new_lossy("nested".to_string()))), Part::Field(Ident::from_strand(Strand::new_lossy("name".to_string())))]), "nested.nested.name")]
	#[case(Idiom::from(vec![Part::Field(Ident::from_strand(Strand::new_lossy("nested".to_string()))), Part::Field(Ident::from_strand(Strand::new_lossy("nested".to_string()))), Part::Field(Ident::from_strand(Strand::new_lossy("value".to_string())))]), "nested.nested.`value`")]
	#[case(Idiom::from(vec![Part::Field(Ident::from_strand(Strand::new_lossy("value".to_string())))]), "`value`")]
	fn test_idiom_to_string(#[case] idiom: Idiom, #[case] expected: &'static str) {
		assert_eq!(idiom.to_string(), expected.to_string());
	}

	#[rstest]
	#[case(Idiom::from(vec![Part::Field(Ident::from_strand(Strand::new_lossy("name".to_string())))]), "name")]
	#[case(Idiom::from(vec![Part::Field(Ident::from_strand(Strand::new_lossy("nested".to_string()))), Part::Field(Ident::from_strand(Strand::new_lossy("nested".to_string()))), Part::Field(Ident::from_strand(Strand::new_lossy("name".to_string())))]), "nested.nested.name")]
	#[case(Idiom::from(vec![Part::Field(Ident::from_strand(Strand::new_lossy("nested".to_string()))), Part::Field(Ident::from_strand(Strand::new_lossy("nested".to_string()))), Part::Field(Ident::from_strand(Strand::new_lossy("value".to_string())))]), "nested.nested.value")]
	#[case(Idiom::from(vec![Part::Field(Ident::from_strand(Strand::new_lossy("value".to_string())))]), "value")]
	fn test_idiom_to_raw_string(#[case] idiom: Idiom, #[case] expected: &'static str) {
		assert_eq!(idiom.to_raw_string(), expected.to_string());
	}
}
