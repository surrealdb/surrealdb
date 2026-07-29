//! Typed canonical-SurrealQL-text storage for catalog definitions.
//!
//! Catalog definitions store everything the user wrote as an expression,
//! type annotation, idiom path, field list, block, or API route as canonical
//! text (the same rendering those constructs' wire formats have always
//! used). Every grammar stored this way parses on its own, so a definition
//! that is made of several clauses stores one text per clause rather than one
//! text for the whole construct. [`SurqlText<T>`] wraps that text together with
//! the type `T` it compiles back to, so definition text can never be passed
//! where a plain `String` is expected (or vice versa), and so re-parsing is
//! only reachable through one funnel: [`SurqlText::compile`], driven by the
//! per-grammar [`SurqlTarget`] impls in this module. No other code may parse
//! stored definition text.
//!
//! Wire format: a `SurqlText<T>` encodes byte-for-byte as its inner `String`
//! (varint length + UTF-8), which is itself byte-identical to the
//! text-on-the-wire encodings of `Expr`, `Idiom`, and `Block`. It must NEVER
//! gain a `#[revisioned]` derive: the derive writes a u16 revision header
//! that bare `String` (and the hand-written `Revisioned` impls of the
//! expression types this storage replaced) does not have, so adding one
//! would silently break compatibility with every stored definition. The
//! hand-written impls below delegate to `String`'s encoding directly.
//!
//! # No AST type is part of a live stored shape
//!
//! Every field of every `Stored*` definition now holds a scalar, a vocabulary
//! enum, or one of these text wrappers. The AST types that still carry
//! `Revisioned` impls — `Expr` and `Idiom` (hand-written), `Kind`, `Fields`,
//! `Groups`, `Fetchs` and their nested parts (derived), and
//! `catalog::aggregation`'s analysis types — are reachable on the wire only
//! from a `#[revision(end = N, convert_fn = ...)]` field, which exists to read
//! bytes written before this migration and cannot be reached by an encode.
//!
//! That is the invariant to protect when adding a field: an AST type appearing
//! in a *live* stored field is a bug, not a shortcut. The types whose
//! `Revisioned` impls do not descend from an AST — `ChangeFeed`, `Filter`,
//! `Tokenizer`, `Language`, `Operation`, and everything in `val` — are
//! genuinely stored and unaffected.

use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

use revision::{
	DeserializeRevisioned, Revisioned, SerializeRevisioned, SkipRevisioned, WalkRevisioned,
};
use surrealdb_types::{SqlFormat, ToSql};

use crate::api::path::Path;
use crate::expr::{Block, Cond, Expr, Fields, Idiom, Kind};
use crate::val::Value;

/// A type that round-trips through the canonical SurrealQL text stored in
/// catalog definitions.
///
/// `render_canonical` produces the exact stored form (which for each impl
/// matches the rendering that type's wire format has always used), and
/// `parse_canonical` re-parses it. Every SurrealQL grammar here reads under the
/// storage wire contract's parser profile,
/// [`crate::syn::parser::ParserSettings::STORED_TEXT`] — the full grammar with
/// unbounded limits, deliberately independent of the datastore's live
/// capabilities and configuration. The text was already validated once, when
/// the `DEFINE`/`ALTER`/`LIVE` statement that produced it ran; see the
/// constant's documentation for the full reasoning. [`Path`] is the one target
/// whose grammar is its own rather than SurrealQL's; its impl documents what
/// that changes.
pub(crate) trait SurqlTarget: Sized {
	/// Render this value to its canonical stored SurrealQL text.
	fn render_canonical(&self) -> String;
	/// Re-parse canonical stored text back into this type.
	fn parse_canonical(text: &str) -> anyhow::Result<Self>;
}

/// Canonical SurrealQL text stored in a catalog definition, tagged with the
/// type `T` it compiles back to.
///
/// Construction is deliberately narrow: [`SurqlText::new`] renders a value
/// through its [`SurqlTarget`] impl (the DEFINE/ALTER path), and
/// [`SurqlText::from_raw`] wraps text that is already known to be canonical
/// (revision decode `convert_fn`s and test fixtures). There is no
/// `From<String>`, no `Deref<Target = str>`, and no way to treat one
/// grammar's text as another's.
pub(crate) struct SurqlText<T> {
	text: String,
	/// `fn() -> T` keeps auto traits (`Send`/`Sync`) and variance independent
	/// of `T`; the parameter exists only to pin the compile target.
	target: PhantomData<fn() -> T>,
}

/// Canonical text of an expression (`VALUE`/`ASSERT`/`WHEN`/`THEN`/
/// permission clauses, API actions, fetch targets, ...).
pub(crate) type ExprText = SurqlText<Expr>;
/// Canonical text of a kind-grammar type annotation (`TYPE`, function
/// argument/return types).
pub(crate) type KindText = SurqlText<Kind>;
/// Canonical text of an idiom path (index columns).
pub(crate) type IdiomText = SurqlText<Idiom>;
/// Canonical text of a `SELECT`-clause field list (live subscriptions).
pub(crate) type FieldsText = SurqlText<Fields>;
/// Canonical text of a `{ ... }` block (function bodies).
pub(crate) type BlockText = SurqlText<Block>;
/// Canonical text of an API's route path (`/users/:id<int>/*rest`).
pub(crate) type PathText = SurqlText<Path>;

impl<T> SurqlText<T> {
	/// Wraps text that is already canonical: the revision-decode
	/// `convert_fn` path (old structured values render through the same
	/// canonical rendering) and test fixtures. Everything else should
	/// construct via [`SurqlText::new`].
	pub(crate) fn from_raw(text: impl Into<String>) -> Self {
		Self {
			text: text.into(),
			target: PhantomData,
		}
	}

	pub(crate) fn as_str(&self) -> &str {
		&self.text
	}
}

impl<T: SurqlTarget> SurqlText<T> {
	/// Renders a value to its canonical stored text.
	pub(crate) fn new(value: &T) -> Self {
		Self::from_raw(value.render_canonical())
	}

	/// Re-parses the stored text back into its compile target.
	///
	/// The error is the parser's own, unwrapped: several consumers propagate
	/// it to users verbatim, and the stored text parsing successfully is an
	/// internal invariant anyway (it parsed when the statement that produced
	/// it ran).
	pub(crate) fn compile(&self) -> anyhow::Result<T> {
		T::parse_canonical(&self.text)
	}
}

// Manual impls so `T` needs no bounds (it is phantom).
impl<T> Clone for SurqlText<T> {
	fn clone(&self) -> Self {
		Self::from_raw(self.text.clone())
	}
}

impl<T> fmt::Debug for SurqlText<T> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_tuple("SurqlText").field(&self.text).finish()
	}
}

impl<T> PartialEq for SurqlText<T> {
	fn eq(&self, other: &Self) -> bool {
		self.text == other.text
	}
}

impl<T> Eq for SurqlText<T> {}

impl<T> Hash for SurqlText<T> {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.text.hash(state)
	}
}

/// Splices the raw stored text (zero-parse), exactly as the bare-`String`
/// fields this type replaced rendered via `ToSql for String`.
impl<T> ToSql for SurqlText<T> {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str(&self.text);
	}
}

impl<T> fmt::Display for SurqlText<T> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(&self.text)
	}
}

/// The `INFO FOR ...` representation of stored text is the text itself.
impl<T> From<SurqlText<T>> for Value {
	fn from(v: SurqlText<T>) -> Self {
		Value::String(v.text.into())
	}
}

impl<T> Revisioned for SurqlText<T> {
	fn revision() -> u16 {
		1
	}
}

impl<T> SerializeRevisioned for SurqlText<T> {
	#[inline]
	fn serialize_revisioned<W: std::io::Write>(
		&self,
		writer: &mut W,
	) -> Result<(), revision::Error> {
		SerializeRevisioned::serialize_revisioned(&self.text, writer)
	}
}

impl<T> DeserializeRevisioned for SurqlText<T> {
	#[inline]
	fn deserialize_revisioned<R: std::io::Read>(reader: &mut R) -> Result<Self, revision::Error> {
		<String as DeserializeRevisioned>::deserialize_revisioned(reader).map(Self::from_raw)
	}
}

impl<T> SkipRevisioned for SurqlText<T> {
	#[inline]
	fn skip_revisioned<R: std::io::Read>(reader: &mut R) -> Result<(), revision::Error> {
		<String as SkipRevisioned>::skip_revisioned(reader)
	}
}

impl<T> WalkRevisioned for SurqlText<T> {
	type Walker<'r, R: revision::BorrowedReader + 'r> = revision::LeafWalker<'r, SurqlText<T>, R>;

	#[inline]
	fn walk_revisioned<'r, R: revision::BorrowedReader>(
		reader: &'r mut R,
	) -> Result<Self::Walker<'r, R>, revision::Error> {
		Ok(revision::LeafWalker::new(reader))
	}
}

impl SurqlTarget for Expr {
	fn render_canonical(&self) -> String {
		self.to_stored_sql()
	}

	fn parse_canonical(text: &str) -> anyhow::Result<Self> {
		Ok(crate::syn::expr_for_definition(text)?.into())
	}
}

impl SurqlTarget for Cond {
	fn render_canonical(&self) -> String {
		self.0.to_stored_sql()
	}

	fn parse_canonical(text: &str) -> anyhow::Result<Self> {
		Ok(Cond(crate::syn::expr_for_definition(text)?.into()))
	}
}

impl SurqlTarget for Kind {
	fn render_canonical(&self) -> String {
		self.to_sql()
	}

	fn parse_canonical(text: &str) -> anyhow::Result<Self> {
		Ok(crate::syn::kind_for_definition(text)?.into())
	}
}

impl SurqlTarget for Idiom {
	fn render_canonical(&self) -> String {
		self.to_raw_string()
	}

	fn parse_canonical(text: &str) -> anyhow::Result<Self> {
		Ok(crate::syn::idiom_for_definition(text)?.into())
	}
}

impl SurqlTarget for Fields {
	fn render_canonical(&self) -> String {
		self.to_sql()
	}

	fn parse_canonical(text: &str) -> anyhow::Result<Self> {
		Ok(crate::syn::fields_for_definition(text)?.into())
	}
}

impl SurqlTarget for Block {
	fn render_canonical(&self) -> String {
		self.to_sql()
	}

	fn parse_canonical(text: &str) -> anyhow::Result<Self> {
		Ok(crate::syn::block_for_definition(text)?.into())
	}
}

/// The one target whose grammar is not SurrealQL: an API route is `/`-separated
/// segments, and only a typed segment's `<kind>` annotation goes through the
/// parser. `Path`'s own `FromStr` owns that grammar, and it settles the limits
/// question by using the default parser settings rather than
/// [`ParserSettings::STORED_TEXT`](crate::syn::parser::ParserSettings::STORED_TEXT)
/// — unchanged from when the path was decoded rather than compiled, so a route
/// whose kind annotation exceeds the configured depth has always been
/// unreadable, and this is not the place to start changing that.
impl SurqlTarget for Path {
	fn render_canonical(&self) -> String {
		self.to_sql()
	}

	fn parse_canonical(text: &str) -> anyhow::Result<Self> {
		Ok(text.parse()?)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn wire_format_is_bare_string() {
		// The whole compatibility story rests on this: a SurqlText encodes
		// exactly like the String it wraps (no revision header).
		let text = ExprText::from_raw("$auth.role = 'admin'");
		let mut wrapped = Vec::new();
		SerializeRevisioned::serialize_revisioned(&text, &mut wrapped).unwrap();
		let mut bare = Vec::new();
		SerializeRevisioned::serialize_revisioned(&"$auth.role = 'admin'".to_string(), &mut bare)
			.unwrap();
		assert_eq!(wrapped, bare);

		let decoded: ExprText =
			DeserializeRevisioned::deserialize_revisioned(&mut wrapped.as_slice()).unwrap();
		assert_eq!(decoded, text);
	}

	#[test]
	fn round_trips_each_grammar() {
		let e: Expr = ExprText::from_raw("$value + 1").compile().unwrap();
		assert_eq!(ExprText::new(&e).as_str(), "$value + 1");

		let k: Kind = KindText::from_raw("array<record<person>>").compile().unwrap();
		assert_eq!(KindText::new(&k).as_str(), "array<record<person>>");

		let i: Idiom = IdiomText::from_raw("field[0]").compile().unwrap();
		assert_eq!(IdiomText::new(&i).as_str(), "field[0]");
	}

	#[test]
	fn a_top_level_bare_identifier_compiles_to_a_field() {
		// `expr_for_definition` parses in field context, which is what every
		// clause stored as expression text means: `VALUE person` selects the
		// document's `person`, not the table.
		//
		// Repointing that funnel at table context would silently reinterpret
		// every already-stored clause whose top level is a bare identifier, so
		// this is pinned rather than left to the funnel's implementation.
		let compiled = ExprText::from_raw("person").compile().unwrap();
		assert!(
			matches!(&compiled, Expr::Idiom(_)),
			"a top-level bare identifier must compile to an idiom, got {compiled:?}"
		);
	}

	#[test]
	fn a_nested_table_position_survives_the_field_context_funnel() {
		// The funnel starts in field context, but the context setters save and
		// restore rather than latch, so a statement inside stored text
		// re-establishes table context for its own source list. This is what
		// makes one field-context funnel correct for every stored grammar;
		// `StoredEventDefinition.then` stores exactly this shape.
		let compiled = ExprText::from_raw("(CREATE person)").compile().unwrap();
		let Expr::Create(stmt) = &compiled else {
			panic!("expected a CREATE statement, got {compiled:?}");
		};
		assert_eq!(
			stmt.what,
			vec![Expr::Table("person".into())],
			"a statement's source list must re-establish table context"
		);
	}

	#[test]
	fn a_statement_shaped_expression_round_trips_through_stored_text() {
		// Rendering applies `CoverStmts` parenthesization and compiling parses
		// it away, so the pair is an identity for the statement shapes that
		// reach stored text. Without this, the two halves could drift apart
		// and only a definition that is written *and then read* would notice.
		for text in ["(CREATE person)", "(SELECT * FROM person)", "(UPDATE person)"] {
			let compiled = ExprText::from_raw(text).compile().unwrap();
			assert_eq!(
				ExprText::new(&compiled).as_str(),
				text,
				"stored text for {text} did not survive compile then render"
			);
		}
	}

	#[test]
	fn stored_idiom_text_escapes_more_weakly_than_idiom_rendering() {
		// `IdiomText` renders through `Idiom::to_raw_string` (`EscapeKwFreeIdent`,
		// which backticks only a name that would not lex as an identifier),
		// while rendering the same idiom into a statement goes through
		// `EscapeIdent` (which also backticks reserved words). So an index
		// column named after a keyword is stored bare and displayed
		// backticked.
		//
		// Both re-parse to the same path, because the idiom parser accepts a
		// keyword-shaped token as a part. It is pinned because harmonising the
		// two would change the stored bytes of every index column on disk.
		let idiom: Idiom = IdiomText::from_raw("select").compile().unwrap();
		assert_eq!(IdiomText::new(&idiom).as_str(), "select");
		assert_eq!(idiom.to_sql(), "`select`");
	}

	#[test]
	fn trailing_content_is_rejected() {
		// Every stored-text grammar is parsed by a routine that stops at the
		// first token it cannot continue with (the Pratt expression parser
		// exits on a token with no continuation binding power; the idiom,
		// field-list and kind parsers stop at the first token that is not a
		// further part/field/union arm). Without an end-of-input check the
		// tail would be silently dropped, so stored text that is not entirely
		// consumed must fail to compile rather than compile to its prefix.
		for text in ["$value + 1 bogus", "1, 2"] {
			assert!(
				ExprText::from_raw(text).compile().is_err(),
				"expression text {text:?} should not compile to its prefix"
			);
		}
		for text in ["a.b bogus", "a.b, c"] {
			assert!(
				IdiomText::from_raw(text).compile().is_err(),
				"idiom text {text:?} should not compile to its prefix"
			);
		}
		for text in ["int bogus", "int, string"] {
			assert!(
				KindText::from_raw(text).compile().is_err(),
				"kind text {text:?} should not compile to its prefix"
			);
		}
		for text in ["a, b bogus", "VALUE a bogus"] {
			assert!(
				FieldsText::from_raw(text).compile().is_err(),
				"field-list text {text:?} should not compile to its prefix"
			);
		}
	}

	/// Beyond the stored-text ceiling, compiling fails rather than overflowing.
	///
	/// The ceiling exists because "it already survived this lowering when it
	/// was written" holds within one process at one configuration and not
	/// across a cluster: a node with a raised
	/// `SURREAL_MAX_EXPRESSION_PARSING_DEPTH` can write a definition that a
	/// default-configured reader re-parses on every catalog-cache miss. An
	/// unbounded re-parse there is a SIGSEGV, which nothing can catch; an error
	/// is something every stored-text consumer already degrades from.
	///
	/// Runs on a default-sized thread on purpose: the point is that the depth
	/// is rejected before the lowering recurses, so no unusual stack is needed.
	#[test]
	fn compile_is_bounded_so_it_cannot_overflow_the_stack() {
		let past_ceiling = format!("{}1{}", "[".repeat(5_000), "]".repeat(5_000));
		let compiled = ExprText::from_raw(past_ceiling).compile();
		assert!(
			compiled.is_err(),
			"stored text past the ceiling must fail to compile rather than recurse into it"
		);
	}

	#[test]
	fn compile_is_not_bound_by_fresh_input_limits() {
		// The parser depth limits are env-tunable (SURREAL_MAX_*_PARSING_DEPTH),
		// so a definition legally written under raised limits can exceed the
		// defaults. Stored text parses under `ParserSettings::STORED_TEXT`
		// (unbounded), matching `Expr`'s wire decode; only fresh input is
		// bounded by the configured limits.
		//
		// The fresh-input rejections run on the test thread (the parser fails
		// fast at the depth limit). The deep compiles run on a thread with an
		// explicit large stack: parsing is heap-stacked (reblessive), but the
		// `sql -> expr` lowering after it recurses per node on the call stack,
		// and a definition this deep would only ever have been written by a
		// server whose statement execution performed that same lowering.

		// 200-deep array nesting: past the default object_recursion_limit (100).
		let deep_array = format!("{}1{}", "[".repeat(200), "]".repeat(200));
		// 300-term operator chain: past the default expr_recursion_limit (128).
		let deep_chain = format!("{}1", "1 + ".repeat(300));

		assert!(
			crate::syn::expr(&deep_array).is_err(),
			"fresh-input parsing should still enforce the default depth limits"
		);
		assert!(
			crate::syn::expr(&deep_chain).is_err(),
			"fresh-input parsing should still enforce the default expression depth limit"
		);

		std::thread::Builder::new()
			.stack_size(16 * 1024 * 1024)
			.spawn(move || {
				let compiled = ExprText::from_raw(deep_array).compile();
				assert!(
					compiled.is_ok(),
					"stored-text compile must not be depth-limited: {:?}",
					compiled.err()
				);

				let compiled = ExprText::from_raw(deep_chain).compile();
				assert!(
					compiled.is_ok(),
					"stored-text compile must not be expression-depth-limited: {:?}",
					compiled.err()
				);
			})
			.expect("spawning the deep-compile thread")
			.join()
			.expect("deep-compile thread panicked");
	}
}
