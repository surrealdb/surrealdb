pub(crate) mod access;
pub(crate) mod analyzer;
pub(crate) mod api;
pub(crate) mod bucket;
pub(crate) mod config;
pub(crate) mod database;
pub(crate) mod event;
pub(crate) mod field;
pub(crate) mod function;
pub(crate) mod index;
pub(crate) mod model;
pub(crate) mod module;
pub(crate) mod namespace;
pub(crate) mod param;
pub(crate) mod sequence;
pub(crate) mod table;
pub(crate) mod user;

use anyhow::Result;

use crate::exec::Error as ExecError;

/// Validate a user-supplied `GRAPHQL_ALIAS "..."` value against the GraphQL
/// `Name` production (`/[_A-Za-z][_0-9A-Za-z]*/`).
///
/// Returning a clear definition-time error here is preferable to silently
/// falling back at schema-generation time — a typo like
/// `GRAPHQL_ALIAS "first name"` would otherwise produce a working schema with
/// the un-aliased name and no feedback to the user.
pub(crate) fn validate_graphql_alias(alias: &Option<String>, kind: &str) -> Result<()> {
	let Some(alias) = alias.as_deref() else {
		return Ok(());
	};
	let mut chars = alias.chars();
	let first_ok = chars.next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_');
	let rest_ok = chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
	if !first_ok || !rest_ok {
		return Err(ExecError::InvalidStatement(format!(
			"GRAPHQL_ALIAS `{alias}` on {kind} is not a valid GraphQL Name; \
			 expected /[_A-Za-z][_0-9A-Za-z]*/"
		))
		.into());
	}
	Ok(())
}
