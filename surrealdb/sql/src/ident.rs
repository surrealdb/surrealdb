use std::fmt::{self, Display};

use common::fmt::EscapeKwFreeIdent;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

/// A single identifier naming a definition, as it appears in the syntax tree.
///
/// This is the form that renders: [`ToSql`] quotes the name whenever emitting
/// it bare would read back as something else, so a definition's name survives
/// being rendered and re-parsed. Statements hold this rather than a `String`
/// or a [`Strand`] because both of those render their contents verbatim, which
/// makes an unquoted name the silent default and a round-trip failure the
/// result.
///
/// Escaping is decided here, once, so a statement can interpolate the name
/// directly and be correct. A statement that reaches for an escaper itself is
/// choosing a different rule than every other definition name.
///
/// Every position this appears in is introduced by a sigil or a path separator
/// (`$name`, `mod::name`, `ml::name`), so a reserved word is unambiguous there
/// and is emitted bare. A name in a position where it could instead be read as
/// a keyword needs [`crate::TableName`]'s stricter rule, not this one.
///
/// This is for names that are a single identifier. It is not the right type for
/// a `::`-separated path such as a function name, whose segments are escaped
/// individually, and not for a name that may be computed at runtime, which is
/// an `Expr`.
#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[repr(transparent)]
pub struct Ident(Strand);

impl Ident {
	pub fn new(s: impl Into<Strand>) -> Ident {
		Ident(s.into())
	}

	pub fn as_str(&self) -> &str {
		self.0.as_str()
	}

	pub fn into_string(self) -> String {
		self.0.into()
	}
}

impl From<String> for Ident {
	fn from(value: String) -> Self {
		Ident(value.into())
	}
}

impl From<&str> for Ident {
	fn from(value: &str) -> Self {
		Ident(value.into())
	}
}

impl From<Strand> for Ident {
	fn from(value: Strand) -> Self {
		Ident(value)
	}
}

impl From<Ident> for String {
	fn from(value: Ident) -> Self {
		value.0.into()
	}
}

impl From<Ident> for Strand {
	fn from(value: Ident) -> Self {
		value.0
	}
}

/// Renders the name unescaped. Use [`ToSql`] to produce SurrealQL.
impl Display for Ident {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl ToSql for Ident {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		EscapeKwFreeIdent(self.as_str()).fmt_sql(f, sql_fmt);
	}
}
