use std::cmp::Ordering;
use std::collections::HashMap;
use std::ops::Deref;
use std::str::FromStr;

use reblessive::Stack;
use reblessive::tree::Stk;
use revision::{DeserializeRevisioned, Revisioned, SerializeRevisioned};
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::part::{Next, NextMethod};
use crate::expr::paths::{ID, IN, OUT};
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Expr, FlowResult, FlowResultExt, Literal, Part, Value};
use crate::fmt::EscapeKwFreeIdent;
use crate::val::Number;

pub mod recursion;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
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

impl PartialOrd for Idioms {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for Idioms {
	fn cmp(&self, other: &Self) -> Ordering {
		for (a, b) in self.0.iter().zip(other.0.iter()) {
			let o = a.cmp(b);
			if o != Ordering::Equal {
				return o;
			}
		}
		Ordering::Equal
	}
}

/// An idiom defines a way to reference a field, reference, or other part of the document graph.
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct Idiom(pub(crate) Vec<Part>);

impl Idiom {
	/// Returns an idiom for a field of the given name.
	pub fn field(field_name: impl Into<Strand>) -> Self {
		Idiom(vec![Part::Field(field_name.into())])
	}

	/// Appends a part to the end of this Idiom
	pub(crate) fn push(mut self, n: Part) -> Idiom {
		self.0.push(n);
		self
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

	/// Validate that this idiom is a valid "local" field path. Returns an
	/// error describing the offending part otherwise. The `into` argument is
	/// used in the error message to identify the kind of path being checked
	/// (e.g. "field name").
	pub(crate) fn validate_local(&self, into: &str) -> anyhow::Result<()> {
		for part in self.iter() {
			match part {
				Part::Field(_) | Part::All | Part::Flatten | Part::First | Part::Last => {}
				Part::Value(Expr::Literal(lit)) => match lit {
					Literal::Integer(_) | Literal::String(_) => {}
					_ => {
						return Err(anyhow::anyhow!(
							"Invalid {into}: Field path index must evaluate to an integer or string"
						));
					}
				},
				_ => {
					return Err(anyhow::anyhow!(
						"Invalid {into}: contains a part that is not allowed in a field path"
					));
				}
			}
		}
		Ok(())
	}

	/// Walk this idiom and substitute any [`Part::Value`] containing a
	/// non-literal expression by computing it and replacing it with a literal.
	/// Used to resolve parameterized indices such as `foo[$n]` to a static
	/// `foo[4]` form before storing the idiom as a schema key.
	///
	/// String substitutions are canonicalised to [`Part::Field`] so that
	/// `addr[$key]` with `$key = "city"` stores as `addr.city` rather than
	/// `addr['city']`.
	///
	/// Returns an error if a substituted index does not evaluate to an
	/// integer (array index) or string (object key).
	pub(crate) async fn substitute_indices(
		self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> anyhow::Result<Idiom> {
		let mut out = Vec::with_capacity(self.0.len());
		for part in self.0 {
			let part = match part {
				Part::Value(Expr::Literal(Literal::String(s))) => Part::Field(s),
				Part::Value(Expr::Literal(lit)) => Part::Value(Expr::Literal(lit)),
				Part::Value(expr) => {
					let value =
						stk.run(|stk| expr.compute(stk, ctx, opt, doc)).await.catch_return()?;
					match value {
						Value::Number(Number::Int(i)) => {
							Part::Value(Expr::Literal(Literal::Integer(i)))
						}
						Value::String(s) => Part::Field(s),
						other => {
							return Err(anyhow::anyhow!(
								"Field path index must evaluate to an integer or string, found {}",
								other.kind_of()
							));
						}
					}
				}
				other => other,
			};
			out.push(part);
		}
		Ok(Idiom(out))
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
		// The Pratt parser exits as soon as it hits a token with no continuation
		// binding power. Without this check, trailing content after a valid
		// idiom prefix would be silently dropped (e.g. "foo bogus" would parse
		// to just "foo"), which is a footgun for any caller that resolves a
		// string at runtime and re-parses it as an idiom.
		parser.assert_finished().map_err(|err| revision::Error::Conversion(format!("{err:?}")))?;

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

impl revision::SkipRevisioned for Idiom {
	fn skip_revisioned<R: std::io::Read>(reader: &mut R) -> Result<(), revision::Error> {
		<String as revision::SkipRevisioned>::skip_revisioned(reader)
	}
}

impl revision::WalkRevisioned for Idiom {
	type Walker<'r, R: revision::BorrowedReader + 'r> = revision::LeafWalker<'r, Idiom, R>;

	fn walk_revisioned<'r, R: revision::BorrowedReader>(
		reader: &'r mut R,
	) -> Result<Self::Walker<'r, R>, revision::Error> {
		Ok(revision::LeafWalker::new(reader))
	}
}

impl revision::LengthPrefixedBytes for Idiom {}

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
	#[case(Idiom::from(vec![Part::Field(Strand::new_static("name"))]), "name")]
	#[case(Idiom::from(vec![Part::Field(Strand::new_static("nested")), Part::Field(Strand::new_static("nested")), Part::Field(Strand::new_static("name"))]), "nested.nested.name")]
	#[case(Idiom::from(vec![Part::Field(Strand::new_static("nested")), Part::Field(Strand::new_static("nested")), Part::Field(Strand::new_static("value"))]), "nested.nested.value")]
	#[case(Idiom::from(vec![Part::Field(Strand::new_static("value"))]), "`value`")]
	fn test_idiom_to_string(#[case] idiom: Idiom, #[case] expected: &'static str) {
		assert_eq!(idiom.to_sql(), expected.to_string());
	}

	#[rstest]
	#[case(Idiom::from(vec![Part::Field(Strand::new_static("name"))]), "name")]
	#[case(Idiom::from(vec![Part::Field(Strand::new_static("nested")), Part::Field(Strand::new_static("nested")), Part::Field(Strand::new_static("name"))]), "nested.nested.name")]
	#[case(Idiom::from(vec![Part::Field(Strand::new_static("nested")), Part::Field(Strand::new_static("nested")), Part::Field(Strand::new_static("value"))]), "nested.nested.value")]
	#[case(Idiom::from(vec![Part::Field(Strand::new_static("value"))]), "value")]
	fn test_idiom_to_raw_string(#[case] idiom: Idiom, #[case] expected: &'static str) {
		assert_eq!(idiom.to_raw_string(), expected.to_string());
	}

	#[rstest]
	// Test b, a ==> a, b (alphabetical ordering)
	#[case(
		vec![Idiom::from(vec![Part::Field(Strand::new_static("b"))]), Idiom::from(vec![Part::Field(Strand::new_static("a"))])],
		vec![Idiom::from(vec![Part::Field(Strand::new_static("a"))]), Idiom::from(vec![Part::Field(Strand::new_static("b"))])]
	)]
	// Test a.b, a ==> a, a.b (prefix comes first)
	#[case(
		vec![Idiom::from(vec![Part::Field(Strand::new_static("a")), Part::Field(Strand::new_static("b"))]), Idiom::from(vec![Part::Field(Strand::new_static("a"))])],
		vec![Idiom::from(vec![Part::Field(Strand::new_static("a"))]), Idiom::from(vec![Part::Field(Strand::new_static("a")), Part::Field(Strand::new_static("b"))])]
	)]
	// Test complex nested case: author.company, author ==> author, author.company
	#[case(
		vec![
			Idiom::from(vec![Part::Field(Strand::new_static("author")), Part::Field(Strand::new_static("company"))]),
			Idiom::from(vec![Part::Field(Strand::new_static("author"))])
		],
		vec![
			Idiom::from(vec![Part::Field(Strand::new_static("author"))]),
			Idiom::from(vec![Part::Field(Strand::new_static("author")), Part::Field(Strand::new_static("company"))])
		]
	)]
	// Test deeply nested: author.company.address, author, author.company ==> author,
	// author.company, author.company.address
	#[case(
		vec![
			Idiom::from(vec![Part::Field(Strand::new_static("author")), Part::Field(Strand::new_static("company")), Part::Field(Strand::new_static("address"))]),
			Idiom::from(vec![Part::Field(Strand::new_static("author"))]),
			Idiom::from(vec![Part::Field(Strand::new_static("author")), Part::Field(Strand::new_static("company"))])
		],
		vec![
			Idiom::from(vec![Part::Field(Strand::new_static("author"))]),
			Idiom::from(vec![Part::Field(Strand::new_static("author")), Part::Field(Strand::new_static("company"))]),
			Idiom::from(vec![Part::Field(Strand::new_static("author")), Part::Field(Strand::new_static("company")), Part::Field(Strand::new_static("address"))])
		]
	)]
	// Test mixed alphabetical and nested: d, a.b.c, b, a, a.b ==> a, a.b, a.b.c, b, d
	#[case(
		vec![
			Idiom::from(vec![Part::Field(Strand::new_static("d"))]),
			Idiom::from(vec![Part::Field(Strand::new_static("a")), Part::Field(Strand::new_static("b")), Part::Field(Strand::new_static("c"))]),
			Idiom::from(vec![Part::Field(Strand::new_static("b"))]),
			Idiom::from(vec![Part::Field(Strand::new_static("a"))]),
			Idiom::from(vec![Part::Field(Strand::new_static("a")), Part::Field(Strand::new_static("b"))])
		],
		vec![
			Idiom::from(vec![Part::Field(Strand::new_static("a"))]),
			Idiom::from(vec![Part::Field(Strand::new_static("a")), Part::Field(Strand::new_static("b"))]),
			Idiom::from(vec![Part::Field(Strand::new_static("a")), Part::Field(Strand::new_static("b")), Part::Field(Strand::new_static("c"))]),
			Idiom::from(vec![Part::Field(Strand::new_static("b"))]),
			Idiom::from(vec![Part::Field(Strand::new_static("d"))])
		]
	)]
	// Test with different Part variants: Field comes before All
	#[case(
		vec![
			Idiom::from(vec![Part::Field(Strand::new_static("a")), Part::All]),
			Idiom::from(vec![Part::Field(Strand::new_static("a")), Part::Field(Strand::new_static("b"))])
		],
		vec![
			Idiom::from(vec![Part::Field(Strand::new_static("a")), Part::Field(Strand::new_static("b"))]),
			Idiom::from(vec![Part::Field(Strand::new_static("a")), Part::All])
		]
	)]
	fn test_idiom_sorting(#[case] mut idioms: Vec<Idiom>, #[case] expected: Vec<Idiom>) {
		idioms.sort();
		assert_eq!(idioms, expected);
	}

	// Tests for Idiom::validate_local

	fn field(name: &'static str) -> Part {
		Part::Field(Strand::new_static(name))
	}
	fn lit_int(n: i64) -> Part {
		Part::Value(Expr::Literal(Literal::Integer(n)))
	}
	fn lit_str(s: &'static str) -> Part {
		Part::Value(Expr::Literal(Literal::String(Strand::new_static(s))))
	}
	fn lit_bool(b: bool) -> Part {
		Part::Value(Expr::Literal(Literal::Bool(b)))
	}

	fn lit_float(f: f64) -> Part {
		Part::Value(Expr::Literal(Literal::Float(f)))
	}

	#[rstest]
	// Plain field
	#[case(Idiom::from(vec![field("foo")]), true)]
	// Nested fields
	#[case(Idiom::from(vec![field("foo"), field("bar")]), true)]
	// Integer index
	#[case(Idiom::from(vec![field("foo"), lit_int(0)]), true)]
	// String index
	#[case(Idiom::from(vec![field("foo"), lit_str("city")]), true)]
	// Wildcard
	#[case(Idiom::from(vec![field("foo"), Part::All]), true)]
	// Flatten / First / Last markers
	#[case(Idiom::from(vec![field("foo"), Part::Flatten]), true)]
	#[case(Idiom::from(vec![field("foo"), Part::First]), true)]
	#[case(Idiom::from(vec![field("foo"), Part::Last]), true)]
	// Optional is not a static schema-key concept
	#[case(Idiom::from(vec![field("foo"), Part::Optional]), false)]
	// Float / decimal indices are not meaningful as array indices or object keys
	#[case(Idiom::from(vec![field("foo"), lit_float(1.5)]), false)]
	// Bool index is not a valid local idiom
	#[case(Idiom::from(vec![field("foo"), lit_bool(true)]), false)]
	// Non-literal Part::Value (un-substituted) is not a valid local idiom
	#[case(Idiom::from(vec![field("foo"), Part::Value(Expr::Param(crate::expr::Param::from("n".to_owned())))]), false)]
	// Where clause is not allowed in a field path
	#[case(Idiom::from(vec![field("foo"), Part::Where(Expr::Literal(Literal::Bool(true)))]), false)]
	// Method call is not allowed in a field path
	#[case(Idiom::from(vec![field("foo"), Part::Method(Strand::new_static("bar"), vec![])]), false)]
	fn test_idiom_validate_local(#[case] idiom: Idiom, #[case] expected: bool) {
		assert_eq!(idiom.validate_local("field name").is_ok(), expected);
	}

	#[test]
	fn test_validate_local_error_message_for_bracket() {
		let idiom = Idiom::from(vec![field("foo"), lit_bool(true)]);
		let err = idiom.validate_local("field name").unwrap_err().to_string();
		assert!(
			err.contains("Field path index must evaluate to an integer"),
			"unexpected error: {err}"
		);
	}

	#[test]
	fn test_validate_local_error_message_for_disallowed_part() {
		let idiom =
			Idiom::from(vec![field("foo"), Part::Method(Strand::new_static("bar"), vec![])]);
		let err = idiom.validate_local("field name").unwrap_err().to_string();
		assert!(err.contains("not allowed in a field path"), "unexpected error: {err}");
	}

	#[test]
	fn idiom_with_bytes_matches_serialize() {
		use revision::{SerializeRevisioned, WalkRevisioned};

		let idiom = Idiom::from(vec![Part::Field(Strand::new_static("foo"))]);
		let mut bytes = Vec::new();
		idiom.serialize_revisioned(&mut bytes).unwrap();
		let expected_raw = idiom.to_raw_string();
		let mut r = bytes.as_slice();
		let walker = Idiom::walk_revisioned(&mut r).unwrap();
		let observed = walker.with_bytes(|raw| raw.to_vec()).unwrap();
		assert_eq!(observed.as_slice(), expected_raw.as_bytes());
		assert!(r.is_empty());
	}

	// Round-trip via SerializeRevisioned -> DeserializeRevisioned exercises the
	// `Idiom::from_str` path used to read stored idioms back from the catalog.
	// Covers every parser-producible part kind that can be persisted (via
	// DEFINE FIELD, DEFINE INDEX FIELDS, etc.) so the strict `assert_finished`
	// check in `from_str` can't silently regress on a part whose
	// `to_raw_string` round-trip drifts. `Part::First` is omitted because the
	// parser produces `Part::Value(Literal::Integer(0))` for `[0]` instead.
	#[rstest]
	#[case::field(Idiom::from(vec![field("foo")]))]
	#[case::nested_field(Idiom::from(vec![field("foo"), field("bar")]))]
	#[case::int_index(Idiom::from(vec![field("data"), lit_int(0)]))]
	#[case::all(Idiom::from(vec![field("foo"), Part::All]))]
	#[case::last(Idiom::from(vec![field("foo"), Part::Last]))]
	#[case::flatten(Idiom::from(vec![field("foo"), Part::Flatten]))]
	#[case::method(Idiom::from(vec![field("id"), Part::Method(Strand::new_static("id"), vec![]), field("val")]))]
	#[case::destructure(Idiom::from(vec![
		field("addr"),
		Part::Destructure(vec![
			crate::expr::part::DestructurePart::Field(Strand::new_static("city")),
			crate::expr::part::DestructurePart::Field(Strand::new_static("zip")),
		]),
	]))]
	fn idiom_revisioned_roundtrip(#[case] idiom: Idiom) {
		let mut bytes = Vec::new();
		idiom.serialize_revisioned(&mut bytes).unwrap();
		let parsed = Idiom::deserialize_revisioned(&mut bytes.as_slice())
			.unwrap_or_else(|e| panic!("round-trip failed for {idiom:?}: {e}"));
		assert_eq!(idiom, parsed, "round-trip mismatch for {idiom:?}");
	}
}
