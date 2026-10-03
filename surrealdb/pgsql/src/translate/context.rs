//! Translation context and configuration options.

/// Parameter rewriting style when translating PostgreSQL `$1..$n` positional parameters.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ParamStyle {
	/// Rewrite `$1`, `$2`, ... to `$_1`, `$_2`, ...
	/// This directly matches SurrealDB server's PostgreSQL wire protocol convention.
	#[default]
	UnderscoreIndexed,
	/// Preserve `$1`, `$2`, ... verbatim.
	Verbatim,
	/// Prefix parameters with a custom prefix, e.g. `$p1`, `$p2`.
	NamedPrefix(String),
}

/// Context and configuration options governing the translation process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranslationContext {
	/// Parameter rewriting style.
	pub param_style: ParamStyle,
	/// Whether to map PostgreSQL system and catalog functions/tables to SurrealQL equivalents.
	pub map_pg_catalog: bool,
}

impl Default for TranslationContext {
	fn default() -> Self {
		Self {
			param_style: ParamStyle::UnderscoreIndexed,
			map_pg_catalog: true,
		}
	}
}

impl TranslationContext {
	/// Create a new translation context with default settings.
	pub fn new() -> Self {
		Self::default()
	}

	/// Set parameter rewriting style.
	pub fn with_param_style(mut self, style: ParamStyle) -> Self {
		self.param_style = style;
		self
	}

	/// Set catalog mapping behavior.
	pub fn with_map_pg_catalog(mut self, map: bool) -> Self {
		self.map_pg_catalog = map;
		self
	}
}
