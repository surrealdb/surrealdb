use std::cmp::Ordering;
use std::collections::HashMap;
use std::ops::Deref;
use std::str::FromStr;

use reblessive::Stack;
use reblessive::tree::Stk;
use revision::{DeserializeRevisioned, Revisioned, SerializeRevisioned};
use surrealdb_types::{SqlFormat, ToSql};

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::part::{Next, NextMethod};
use crate::expr::paths::{ID, IN, OUT};
use crate::expr::statements::info::InfoStructure;
use crate::expr::{FlowResult, Part, Value};
use crate::fmt::EscapeKwFreeIdent;

pub mod recursion;

/// An idiom defines a way to reference a field, reference, or other part of the document graph.
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct Idiom(pub(crate) Vec<Part>);

impl Idiom {
	/// Returns an idiom for a field of the given name.
	pub fn field(field_name: String) -> Self {
		Idiom(vec![Part::Field(field_name)])
	}

	/// Appends a part to the end of this Idiom
	pub(crate) fn push(mut self, n: Part) -> Idiom {
		self.0.push(n);
		self
	}
	/// Simplifies this Idiom for use in object keys
	/// Simplifies this Idiom for use in object keys.
	/// Keeps only leading Part::Value (formerly Start) and then only Field/Lookup parts after.
	pub(crate) fn simplify(&self) -> Idiom {
		let mut iter = self.0.iter().peekable();
		let mut simplified = Vec::new();

		// Retain a single leading Part::Value, if present.
		if let Some(Part::Value(_)) = iter.peek()
			&& let Some(p) = iter.next()
		{
			simplified.push(p.clone());
		}

		// Retain only Field/Lookup parts after an initial Value.
		for p in iter {
			match p {
				Part::Field(_) | Part::Lookup(_) => simplified.push(p.clone()),
				_ => {}
			}
		}

		Idiom(simplified)
	}
	/// Check if this Idiom is an 'id' field
	pub(crate) fn is_id(&self) -> bool {
		self.0.len() == 1 && self.0[0].eq(&ID[0])
	}
	/// Check if this Idiom is a special field such as `id`, `in` or `out`.
	pub(crate) fn is_special(&self) -> bool {
		self.0.len() == 1 && [&ID[0], &IN[0], &OUT[0]].contains(&&self.0[0])
	}

	/// Returns a raw string representation of this idiom without any escaping.
	pub(crate) fn to_raw_string(&self) -> String {
		use std::fmt::Write;

		let mut s = String::new();

		let mut iter = self.0.iter();
		match iter.next() {
			Some(Part::Field(v)) => {
				write!(&mut s, "{}", EscapeKwFreeIdent(v).to_sql()).expect("writing to string")
			}
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
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> FlowResult<Value> {
		match self.first() {
			// The starting part is a value
			Some(Part::Value(v)) => {
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

impl PartialOrd for Idiom {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for Idiom {
	fn cmp(&self, other: &Self) -> Ordering {
		for (a, b) in self.0.iter().zip(other.0.iter()) {
			let o = a.partial_cmp(b).unwrap_or(Ordering::Equal);
			if o != Ordering::Equal {
				return o;
			}
		}

		// If all parts match so far, shorter idiom comes first
		// This ensures that `a` < `a.b`
		self.0.len().cmp(&other.0.len())
	}
}

impl ToSql for Idiom {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		let sql_idiom: crate::sql::Idiom = self.clone().into();
		sql_idiom.fmt_sql(f, sql_fmt);
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
}

impl SerializeRevisioned for Idiom {
	fn serialize_revisioned<W: std::io::Write>(
		&self,
		writer: &mut W,
	) -> Result<(), revision::Error> {
		SerializeRevisioned::serialize_revisioned(&self.to_raw_string(), writer)
	}
}

impl DeserializeRevisioned for Idiom {
	fn deserialize_revisioned<R: std::io::Read>(reader: &mut R) -> Result<Self, revision::Error> {
		let s: String = DeserializeRevisioned::deserialize_revisioned(reader)?;
		let idiom =
			Idiom::from_str(&s).map_err(|err| revision::Error::Conversion(format!("{err:?}")))?;
		Ok(idiom)
	}
}

impl InfoStructure for Idiom {
	fn structure(self) -> Value {
		self.to_sql().into()
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

	#[rstest]
	#[case(Idiom::from(vec![Part::Field("name".to_string())]), "name")]
	#[case(Idiom::from(vec![Part::Field("nested".to_string()), Part::Field("nested".to_string()), Part::Field("name".to_string())]), "nested.nested.name")]
	#[case(Idiom::from(vec![Part::Field("nested".to_string()), Part::Field("nested".to_string()), Part::Field("value".to_string())]), "nested.nested.value")]
	#[case(Idiom::from(vec![Part::Field("value".to_string())]), "`value`")]
	fn test_idiom_to_string(#[case] idiom: Idiom, #[case] expected: &'static str) {
		assert_eq!(idiom.to_sql(), expected.to_string());
	}

	#[rstest]
	#[case(Idiom::from(vec![Part::Field("name".to_string())]), "name")]
	#[case(Idiom::from(vec![Part::Field("nested".to_string()), Part::Field("nested".to_string()), Part::Field("name".to_string())]), "nested.nested.name")]
	#[case(Idiom::from(vec![Part::Field("nested".to_string()), Part::Field("nested".to_string()), Part::Field("value".to_string())]), "nested.nested.value")]
	#[case(Idiom::from(vec![Part::Field("value".to_string())]), "value")]
	fn test_idiom_to_raw_string(#[case] idiom: Idiom, #[case] expected: &'static str) {
		assert_eq!(idiom.to_raw_string(), expected.to_string());
	}

	#[rstest]
	// Test b, a ==> a, b (alphabetical ordering)
	#[case(
		vec![Idiom::from(vec![Part::Field("b".to_string())]), Idiom::from(vec![Part::Field("a".to_string())])],
		vec![Idiom::from(vec![Part::Field("a".to_string())]), Idiom::from(vec![Part::Field("b".to_string())])]
	)]
	// Test a.b, a ==> a, a.b (prefix comes first)
	#[case(
		vec![Idiom::from(vec![Part::Field("a".to_string()), Part::Field("b".to_string())]), Idiom::from(vec![Part::Field("a".to_string())])],
		vec![Idiom::from(vec![Part::Field("a".to_string())]), Idiom::from(vec![Part::Field("a".to_string()), Part::Field("b".to_string())])]
	)]
	// Test complex nested case: author.company, author ==> author, author.company
	#[case(
		vec![
			Idiom::from(vec![Part::Field("author".to_string()), Part::Field("company".to_string())]),
			Idiom::from(vec![Part::Field("author".to_string())])
		],
		vec![
			Idiom::from(vec![Part::Field("author".to_string())]),
			Idiom::from(vec![Part::Field("author".to_string()), Part::Field("company".to_string())])
		]
	)]
	// Test deeply nested: author.company.address, author, author.company ==> author,
	// author.company, author.company.address
	#[case(
		vec![
			Idiom::from(vec![Part::Field("author".to_string()), Part::Field("company".to_string()), Part::Field("address".to_string())]),
			Idiom::from(vec![Part::Field("author".to_string())]),
			Idiom::from(vec![Part::Field("author".to_string()), Part::Field("company".to_string())])
		],
		vec![
			Idiom::from(vec![Part::Field("author".to_string())]),
			Idiom::from(vec![Part::Field("author".to_string()), Part::Field("company".to_string())]),
			Idiom::from(vec![Part::Field("author".to_string()), Part::Field("company".to_string()), Part::Field("address".to_string())])
		]
	)]
	// Test mixed alphabetical and nested: d, a.b.c, b, a, a.b ==> a, a.b, a.b.c, b, d
	#[case(
		vec![
			Idiom::from(vec![Part::Field("d".to_string())]),
			Idiom::from(vec![Part::Field("a".to_string()), Part::Field("b".to_string()), Part::Field("c".to_string())]),
			Idiom::from(vec![Part::Field("b".to_string())]),
			Idiom::from(vec![Part::Field("a".to_string())]),
			Idiom::from(vec![Part::Field("a".to_string()), Part::Field("b".to_string())])
		],
		vec![
			Idiom::from(vec![Part::Field("a".to_string())]),
			Idiom::from(vec![Part::Field("a".to_string()), Part::Field("b".to_string())]),
			Idiom::from(vec![Part::Field("a".to_string()), Part::Field("b".to_string()), Part::Field("c".to_string())]),
			Idiom::from(vec![Part::Field("b".to_string())]),
			Idiom::from(vec![Part::Field("d".to_string())])
		]
	)]
	// Test with different Part variants: Field comes before All
	#[case(
		vec![
			Idiom::from(vec![Part::Field("a".to_string()), Part::All]),
			Idiom::from(vec![Part::Field("a".to_string()), Part::Field("b".to_string())])
		],
		vec![
			Idiom::from(vec![Part::Field("a".to_string()), Part::Field("b".to_string())]),
			Idiom::from(vec![Part::Field("a".to_string()), Part::All])
		]
	)]
	fn test_idiom_sorting(#[case] mut idioms: Vec<Idiom>, #[case] expected: Vec<Idiom>) {
		idioms.sort();
		assert_eq!(idioms, expected);
	}
}
