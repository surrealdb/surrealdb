//! The settings a parse runs under, and the configuration they derive from.
//!
//! [`ParserConfig`] holds the three configurable depth limits. They live here
//! because this crate is the lowest layer that reads them: core's
//! capability-gated wrappers, the GQL dialect, the API-request decoder and
//! the server's body decoders all read them downward from here. Each limit
//! keeps a recursive walk within a conservative worker stack, so raising one
//! trades a wider accepted grammar for a smaller margin against stack
//! exhaustion from query text alone.
//!
//! [`ParserSettings`] is what one parse runs under: those limits, the
//! optional grammars, and the literal rules for legacy strings and record ids.

use surrealdb_cnf as cnf;
use surrealdb_parser_next::Limits;

use super::Options;
use super::entry::Settings;

/// Configurable parser depth limits.
#[derive(Clone, Debug)]
pub struct ParserConfig {
	/// How deep the parser will parse nested objects and arrays. Type
	/// annotations (`array<option<array<int>>>`) count against this too.
	pub max_object_parsing_depth: u32,
	/// How deep the parser will parse recursive queries — a statement nested
	/// inside another statement, as in subqueries and blocks.
	pub max_query_parsing_depth: u32,
	/// How deep the parser will build an expression operator tree. Bounds
	/// left-associative operator spines (`1 + 1 + 1 + …`) and prefix/postfix
	/// chains, which consume neither of the other two budgets and would
	/// otherwise build an arbitrarily deep tree that later recursive walks
	/// (`Drop`, `ToSql`, the lowering to `expr::Expr`) overflow the stack on.
	pub max_expression_parsing_depth: u32,
}

impl Default for ParserConfig {
	fn default() -> Self {
		Self {
			max_object_parsing_depth: 100,
			max_query_parsing_depth: 20,
			max_expression_parsing_depth: 128,
		}
	}
}

impl cnf::Config for ParserConfig {
	fn parse(&mut self, map: &cnf::ConfigMap) {
		map.parse_key("max_object_parsing_depth", &mut self.max_object_parsing_depth)
			.parse_key("max_query_parsing_depth", &mut self.max_query_parsing_depth)
			.parse_key("max_expression_parsing_depth", &mut self.max_expression_parsing_depth);
	}
}

#[derive(Clone, Copy, Debug)]
pub struct ParserSettings {
	/// Parse strand like the old parser where a strand which looks like a UUID,
	/// Record-Id, Or a DateTime will be parsed as a date-time.
	pub legacy_strands: bool,
	/// Set whether to allow record-id's which don't adheare to regular ident
	/// rules. Setting this to true will allow parsing of, for example,
	/// `foo:0bar`. This would be rejected by normal identifier rules as most
	/// identifiers can't start with a number.
	pub flexible_record_id: bool,
	/// Disallow a query to have objects deeper than the limit.
	/// Arrays also count towards objects. So `[{foo: [] }]` would be 3 deep.
	/// Type annotation nesting depth (e.g. `array<option<array<int>>>` in
	/// `DEFINE FIELD TYPE`, cast expressions, etc.) also counts against this limit.
	///
	/// This is the budget a bracketed level is charged to, and the only one
	/// that bounds how deeply a query's text may nest collections — together
	/// with `expr_recursion_limit`, which the pratt parser charges for the same
	/// level, so the effective ceiling is the smaller of the two less the
	/// framing the enclosing statement contributes. An object entry's value, an
	/// array element and a set element re-enter the expression parser without
	/// charging `query_recursion_limit`, because that budget bounds statement
	/// nesting and a collection literal is not a statement.
	pub object_recursion_limit: usize,
	/// Disallow a query from being deeper than the give limit.
	/// A query recurses when a statement contains another statement within
	/// itself. Examples are subquery and blocks like block statements and if
	/// statements and such.
	///
	/// A collection literal is not one of those, so it is charged to
	/// `object_recursion_limit` instead and this budget does not bound how
	/// deeply a query's text may nest objects and arrays. A parenthesised
	/// expression is charged here, being the same re-entry a subquery is.
	pub query_recursion_limit: usize,
	/// Disallow an expression tree from being deeper than the given limit.
	///
	/// This bounds the depth of the operator tree built by the pratt parser,
	/// and is charged once per node — including once per bracketed level,
	/// which charges `object_recursion_limit` alongside it. What it bounds that
	/// the other budgets do not is flat operator chains: left-associative infix spines
	/// (e.g. `1 + 1 + 1 + ...`), prefix chains (e.g. `!!!...x`) and postfix
	/// chains consume neither of the other budgets, so without this limit a
	/// flat chain of operators in the query text builds an arbitrarily deep
	/// `Expr` tree. That tree is later walked recursively (its `Drop`,
	/// `ToSql`, and the `sql::Expr -> expr::Expr` lowering all recurse once
	/// per node), overflowing the call stack on long enough chains — a denial
	/// of service reachable from query text alone.
	pub expr_recursion_limit: usize,
	/// Whether the files feature is enabled
	pub files_enabled: bool,
	/// Whether the surrealism feature is enabled
	pub surrealism_enabled: bool,
	/// Whether to allow JSON-style UTF-16 surrogate pairs in \uXXXX escapes.
	/// Only enabled when parsing JSON input, not SurrealQL.
	pub json_string_escapes: bool,
	/// Whether the text is an import. See [`Limits::import`]: the definition
	/// forms an export or a 3.x dump writes parse only under it.
	pub import: bool,
}

impl Default for ParserSettings {
	fn default() -> Self {
		ParserSettings {
			legacy_strands: false,
			flexible_record_id: true,
			object_recursion_limit: 100,
			query_recursion_limit: 20,
			expr_recursion_limit: 128,
			files_enabled: false,
			surrealism_enabled: false,
			json_string_escapes: false,
			import: false,
		}
	}
}

impl ParserSettings {
	/// Settings for re-parsing canonical SurrealQL text that this engine
	/// previously rendered and persisted: the text-on-the-wire encodings of
	/// `surrealdb_expr::expr::Expr` and `expr::Block`, and every text field a
	/// catalog definition stores (see `surrealdb_catalog::SurqlText`).
	///
	/// This profile is part of the storage wire contract and is deliberately
	/// independent of the datastore's live capabilities and configuration:
	///
	/// - The full grammar is enabled (`files_enabled`, `surrealism_enabled`) because
	///   experimental-syntax gates apply to *fresh statements* at the query boundary. Stored text
	///   already passed that gate when the statement that produced it ran; re-gating it under the
	///   *current* capabilities would make stored definitions undecodable after a capability
	///   downgrade, while adding no enforcement — execution of gated features is guarded at runtime
	///   by `Capabilities::allows_experimental` checks at each point of use.
	/// - The recursion limits are far above the configured defaults, because the text fit within
	///   whatever (env-tunable) limits applied where it was written and re-parsing under today's
	///   limits would reject data that was legal to store. They are not unbounded, though, and the
	///   difference matters: parsing keeps its nesting on the heap, but materializing runtime
	///   expressions or logical operators recurses on the call stack, as does the value's own
	///   `Drop`. "It already survived that lowering when it was written" holds within one process
	///   at one configuration; it does not hold across a cluster or across a config change. A node
	///   with a raised `SURREAL_MAX_EXPRESSION_PARSING_DEPTH` can write a definition that a
	///   default-configured reader then re-parses on every catalog-cache miss for that table — and
	///   a Rust stack overflow is a SIGSEGV, not an error anything can catch or degrade from.
	///
	///   So decode gets an absolute ceiling, independent of live config. The numbers are several
	///   times the defaults (100 / 20 / 128) — past anything a real schema reaches, and past the
	///   depths `catalog::text` pins as legal — while staying bounded. Text deeper than this fails
	///   to compile, which every stored-text consumer already handles: a permission fails closed,
	///   a view degrades, a subscription goes inert. All of those are recoverable; a stack
	///   overflow is not.
	pub const STORED_TEXT: ParserSettings = ParserSettings {
		legacy_strands: false,
		flexible_record_id: true,
		object_recursion_limit: 400,
		query_recursion_limit: 400,
		expr_recursion_limit: 512,
		files_enabled: true,
		surrealism_enabled: true,
		json_string_escapes: false,
		import: false,
	};

	/// Settings for reading back a value this engine stored, in the SurrealQL
	/// an export renders it as — `INSERT [ { … } ];`.
	///
	/// `value_depth` is the deepest value the storage layer accepts, the cap
	/// enforced on every record encode. Deriving the parser's ceiling from it
	/// is what makes an export importable: a value that was legal to write is
	/// legal to read back, so the two ends cannot drift apart into an export
	/// the importer rejects.
	///
	/// That guarantee covers a dump's record values, and only those. A dump's
	/// other half is catalog definition text — a stored permission, a view's
	/// `AS SELECT`, a computed field, an idiom — which is re-parsed at
	/// [`Self::STORED_TEXT`], a strictly higher ceiling. A definition stored at
	/// a depth between the two is rendered into a dump this profile then
	/// refuses. Raising this ceiling to meet `STORED_TEXT` would close that,
	/// at the cost of admitting a deeper statement tree on every import,
	/// including the embedded targets whose stack is smallest.
	///
	/// All three budgets take the same number because a bracketed level is
	/// charged to the object budget and the expression budget alike, and the
	/// statement framing around a record is charged to the query budget — see
	/// [`ParserSettings::object_recursion_limit`]. The
	/// margin above `value_depth` covers the framing an export wraps a record
	/// in — the `INSERT` array and the statement expression around it — with
	/// room to spare, so that framing can change without capping the round
	/// trip again.
	///
	/// Like [`Self::STORED_TEXT`], the ceiling is bounded and independent of
	/// live configuration, and for the same reason: parsing keeps its nesting
	/// on the heap, but the lowerings that follow, `ToSql`, and the value's
	/// own `Drop` recurse on the call stack,
	/// where exhaustion is a SIGSEGV rather than an error. Callers pass a
	/// compile-time constant, so no configuration can put a value in storage
	/// that this ceiling cannot read back.
	///
	/// Unlike [`Self::STORED_TEXT`] the grammar gates are left off. Import
	/// text is a fresh statement at the query boundary rather than text this
	/// engine previously rendered, so it is subject to the experimental-syntax
	/// gates; callers set `files_enabled` and `surrealism_enabled` from the
	/// live capabilities.
	pub const fn for_value_depth(value_depth: usize) -> Self {
		// Framing allowance, well above the two levels an export's
		// `INSERT [ … ];` actually adds.
		let limit = value_depth + 16;
		ParserSettings {
			legacy_strands: false,
			flexible_record_id: true,
			object_recursion_limit: limit,
			query_recursion_limit: limit,
			expr_recursion_limit: limit,
			files_enabled: false,
			surrealism_enabled: false,
			json_string_escapes: false,
			import: false,
		}
	}

	pub fn default_with_experimental(enabled: bool) -> Self {
		ParserSettings {
			files_enabled: enabled,
			surrealism_enabled: enabled,
			..Self::default()
		}
	}
}

impl ParserSettings {
	/// Parser limits taken from configuration, with every optional grammar
	/// disabled. Core layers the capability-gated grammars on top.
	pub fn from_config(config: &ParserConfig) -> Self {
		ParserSettings {
			object_recursion_limit: config.max_object_parsing_depth as usize,
			query_recursion_limit: config.max_query_parsing_depth as usize,
			expr_recursion_limit: config.max_expression_parsing_depth as usize,
			..Default::default()
		}
	}

	/// Configured limits with every optional grammar enabled, matching what an
	/// unrestricted capability set produces.
	pub fn all_features(config: &ParserConfig) -> Self {
		ParserSettings {
			files_enabled: true,
			surrealism_enabled: true,
			..Self::from_config(config)
		}
	}
}

impl ParserSettings {
	/// The settings parser-next and its lowering read.
	pub fn next(&self) -> Settings {
		let s = self;
		let c = |v: usize| v.min(u16::MAX as usize) as u16;
		Settings {
			limits: Limits {
				expr_depth: c(s.expr_recursion_limit),
				query_depth: c(s.query_recursion_limit),
				object_depth: c(s.object_recursion_limit),
				strict_record_ids: !s.flexible_record_id,
				builtin: Some(super::is_builtin),
				regex: Some(super::is_regex),
				no_files: !s.files_enabled,
				no_surrealism: !s.surrealism_enabled,
				import: s.import,
			},
			opts: Options {
				legacy_strands: s.legacy_strands,
				files: s.files_enabled,
				surrealism: s.surrealism_enabled,
			},
		}
	}
}
