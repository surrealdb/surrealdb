//! Translation context and configuration options for MySQL to SurrealQL.

/// Prefix used for positional parameters when translating to SurrealQL (e.g. `$_1`).
pub const POSITIONAL_PARAM_PREFIX: &str = "_";

/// Generate a parameter identifier for a 1-based positional parameter index.
#[inline]
pub fn positional_param_name(id: usize) -> String {
	format!("{POSITIONAL_PARAM_PREFIX}{id}")
}

/// Parameter rewriting style when translating MySQL `?` positional parameters.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ParamStyle {
	/// Rewrite `?` to `$_1`, `$_2`, ...
	#[default]
	UnderscoreIndexed,
	/// Rewrite `?` to `$p1`, `$p2`, ...
	NamedPrefix(String),
}

/// Context and configuration options governing the translation process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranslationContext {
	/// Parameter rewriting style.
	pub param_style: ParamStyle,
	/// Current default database if known.
	pub default_database: Option<String>,
}

impl Default for TranslationContext {
	fn default() -> Self {
		Self {
			param_style: ParamStyle::UnderscoreIndexed,
			default_database: None,
		}
	}
}

impl TranslationContext {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn with_param_style(mut self, style: ParamStyle) -> Self {
		self.param_style = style;
		self
	}

	pub fn with_default_database(mut self, db: Option<String>) -> Self {
		self.default_database = db;
		self
	}
}
