//! Column naming and reserved-name validation.
//!
//! Implements the naming rules of `doc/gql/V2_DESIGN.md` §8: the `__`
//! prefix is reserved for the hidden binding names (`__e<n>`, `__v<n>`),
//! engine-bound parameter names are rejected, and a `RETURN` item is named by
//! its explicit alias or, failing that, by the verbatim source text of its
//! expression. The validations are unchanged from v1.

use crate::gql::ast::{Ident, ReturnItem};
use crate::syn::error::{SyntaxError, bail};
use crate::syn::token::Span;

/// Parameter names which the engine binds itself and which therefore cannot
/// be supplied as GQL parameters.
const RESERVED_PARAMS: &[&str] = &[
	"this", "self", "parent", "value", "before", "after", "event", "auth", "session", "token",
	"access",
];

/// Validates a GQL parameter name.
pub(super) fn validate_param_name(name: &str, span: Span) -> Result<(), SyntaxError> {
	if name.starts_with("__") {
		bail!(
			"Parameter names starting with `__` are reserved for internal use",
			@span => "rename the parameter"
		);
	}
	if RESERVED_PARAMS.contains(&name) {
		bail!(
			"The parameter name `${name}` is reserved by the engine",
			@span => "rename the parameter"
		);
	}
	Ok(())
}

/// Validates a pattern variable name.
pub(super) fn validate_var(ident: &Ident) -> Result<(), SyntaxError> {
	if ident.name.starts_with("__") {
		bail!(
			"Variable names starting with `__` are reserved for internal use",
			@ident.span => "rename the variable"
		);
	}
	Ok(())
}

/// Validates a `RETURN … AS alias` name, which becomes a column name.
pub(super) fn validate_alias(ident: &Ident) -> Result<(), SyntaxError> {
	if ident.name.starts_with("__") {
		bail!(
			"Aliases starting with `__` are reserved for internal use",
			@ident.span => "rename the alias"
		);
	}
	Ok(())
}

/// Returns the column name of a `RETURN` item and the span it originates
/// from: an explicit alias wins, unaliased items are named by the verbatim
/// source text of their expression (`doc/gql/V2_DESIGN.md` §8).
pub(super) fn column_name(item: &ReturnItem) -> Result<(String, Span), SyntaxError> {
	match &item.alias {
		Some(alias) => {
			validate_alias(alias)?;
			Ok((alias.name.clone(), alias.span))
		}
		None => Ok((item.text.clone(), item.expr.span())),
	}
}
