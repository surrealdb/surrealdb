//! Naming-convention helpers for auto-generated GraphQL schema names.
//!
//! SurrealDB exposes a single, opinionated GraphQL schema style modelled on
//! [Apollo's schema-naming conventions][apollo]:
//!
//! - Type / mutation-case identifiers are PascalCase (`Customer`, `Order`).
//! - Query / mutation / field identifiers are camelCase (`firstName`, `customerOrder`).
//! - Single-record fetch is the singular table name; the list query is the pluralised form (`store`
//!   / `stores`). Bulk mutations follow the same pattern (`createStore` / `createStores`).
//!
//! Per-definition aliases declared via `GRAPHQL <ident>` (see GitHub issue
//! #4537) always take precedence over the convention.
//!
//! [apollo]: https://www.apollographql.com/docs/technotes/TN0002-schema-naming-conventions/

use super::tables::is_valid_gql_identifier_pub as is_valid_gql_identifier;
use crate::catalog::{FieldDefinition, TableDefinition};
use crate::expr::Idiom;
use crate::expr::part::Part;

/// Convert `snake_case` or `kebab-case` text to `camelCase`. Already-camelCase
/// input is left alone. Empty segments are collapsed.
pub(crate) fn to_camel_case(s: &str) -> String {
	let mut out = String::with_capacity(s.len());
	let mut upper_next = false;
	let mut first = true;
	for ch in s.chars() {
		if ch == '_' || ch == '-' {
			upper_next = !first;
			continue;
		}
		if first {
			out.push(ch.to_ascii_lowercase());
			first = false;
		} else if upper_next {
			out.push(ch.to_ascii_uppercase());
			upper_next = false;
		} else {
			out.push(ch);
		}
	}
	out
}

/// Convert any text to `PascalCase`.
pub(crate) fn to_pascal_case(s: &str) -> String {
	let camel = to_camel_case(s);
	let mut chars = camel.chars();
	match chars.next() {
		Some(c) => c.to_ascii_uppercase().to_string() + chars.as_str(),
		None => String::new(),
	}
}

/// Naive English pluralisation. Good enough for the common cases that show up
/// in table names; users with irregular plurals are expected to set
/// `GRAPHQL <alias>` on the table.
///
/// Rules:
/// - empty → empty
/// - already ends in `s` → unchanged
/// - ends in `y` preceded by a consonant → trim `y`, append `ies`
/// - ends in `x`, `z`, `ch`, `sh`, `ss` → append `es`
/// - otherwise → append `s`
pub(crate) fn pluralize(s: &str) -> String {
	if s.is_empty() {
		return String::new();
	}
	let lower = s.to_ascii_lowercase();
	if lower.ends_with('s') {
		return s.to_string();
	}
	if lower.ends_with("ch")
		|| lower.ends_with("sh")
		|| lower.ends_with('x')
		|| lower.ends_with('z')
	{
		return format!("{s}es");
	}
	if let Some(stripped) = lower.strip_suffix('y')
		&& let Some(prev) = stripped.chars().last()
		&& !matches!(prev, 'a' | 'e' | 'i' | 'o' | 'u')
	{
		// Preserve case: take s minus the trailing `y` and append `ies`.
		let mut out: String = s[..s.len() - 1].to_string();
		out.push_str("ies");
		return out;
	}
	format!("{s}s")
}

/// Effective base name for a table — the explicit `GRAPHQL_ALIAS` value when
/// set, otherwise the raw table name. The alias is validated at DEFINE-time
/// (see `expr::statements::define::validate_graphql_alias`), so we don't need
/// to re-check it here. Casing is left up to the caller (the query/mutation
/// helpers below handle Apollo-specific shaping).
pub(crate) fn table_base_name(tb: &TableDefinition) -> &str {
	match tb.graphql_alias.as_deref() {
		Some(alias) if is_valid_gql_identifier(alias) => alias,
		_ => tb.name.as_str(),
	}
}

/// Effective base name for a field. Multi-part idioms still flow through the
/// sanitised idiom form because they need to remain a single GraphQL
/// identifier. The alias is validated at DEFINE-time, but the
/// `is_valid_gql_identifier` guard remains so legacy catalog entries from
/// before that validation still degrade gracefully.
pub(crate) fn field_base_name(fd: &FieldDefinition) -> String {
	if let Some(ref alias) = fd.graphql_alias
		&& is_valid_gql_identifier(alias)
	{
		return alias.clone();
	}
	idiom_to_default_name(&fd.name)
}

/// Field name exposed on the generated GraphQL Object / input type.
///
/// Honours an explicit `GRAPHQL <ident>` alias (#4537) when valid; otherwise
/// falls back to the raw SurrealQL field name. Apollo-style field naming
/// conventions (`camelCase`) are NOT applied automatically — keeping the
/// field name aligned with the SurrealQL idiom avoids surprising rewrites
/// for snake_case columns. Users who want camelCase can set an explicit
/// `GRAPHQL` alias per field.
pub(crate) fn field_gql_name(fd: &FieldDefinition) -> String {
	field_base_name(fd)
}

/// Plural query field name — the table list query (e.g. `stores`).
///
/// When `GRAPHQL <alias>` is set on the table the alias is treated as the
/// singular form and pluralised here, so a single alias still produces a
/// distinct pair `(<alias>, <pluralAlias>)` for `get` / `list`.
pub(crate) fn list_field_name(tb: &TableDefinition) -> String {
	let base = table_base_name(tb);
	pluralize(&to_camel_case(base))
}

/// Singular fetch-by-id field name (e.g. `store`).
///
/// For tables whose name is *already* plural (`likes`, `follows`, `users`) the
/// naive pluralisation rule is a no-op, so list and single fetch would collide.
/// Fall back to the legacy `_get_<name>` form in that case — users who want a
/// cleaner singular name should set `GRAPHQL <alias>` on the table.
pub(crate) fn get_field_name(tb: &TableDefinition) -> String {
	let base = table_base_name(tb);
	if tb.graphql_alias.as_deref().is_some_and(is_valid_gql_identifier) {
		return base.to_string();
	}
	let camel = to_camel_case(base);
	if pluralize(&camel) == camel {
		// Already plural-looking; can't reuse the same name for both. Fall
		// back to the legacy `_get_<name>` form.
		format!("_get_{camel}")
	} else {
		camel
	}
}

/// Capitalised form used inside mutation field names (`create<Cap>` etc.).
pub(crate) fn mutation_cap_name(tb: &TableDefinition) -> String {
	let base = table_base_name(tb);
	if tb.graphql_alias.as_deref().is_some_and(is_valid_gql_identifier) {
		// Alias is authoritative; just upper-case the first letter so
		// `createAlias` reads naturally regardless of how it was written.
		let mut chars = base.chars();
		return match chars.next() {
			Some(c) => c.to_ascii_uppercase().to_string() + chars.as_str(),
			None => String::new(),
		};
	}
	to_pascal_case(base)
}

/// Compose a description string for a GraphQL field by joining an optional
/// SurrealQL `COMMENT` and an optional `GRAPHQL_DEPRECATED "reason"` into a
/// single human-readable blob.
///
/// async-graphql 7.2.1's dynamic builder does not expose a public setter for
/// the `@deprecated` directive on `Field` / `InputValue` (the inner field is
/// `pub(crate)`). Until that's fixed upstream we surface the deprecation in
/// the description so it still appears under `__schema { fields { description } }`.
pub(crate) fn description_with_deprecation(
	comment: Option<&str>,
	deprecation: Option<&str>,
) -> Option<String> {
	match (comment, deprecation) {
		(Some(c), Some(r)) => Some(format!("[Deprecated: {r}]\n\n{c}")),
		(None, Some(r)) => Some(format!("[Deprecated: {r}]")),
		(Some(c), None) => Some(c.to_owned()),
		(None, None) => None,
	}
}

/// Suffix appended to a verb (`create`, `update`, `upsert`, `delete`) to form
/// the bulk mutation field name.
///
/// For tables whose Pascal-cased name pluralises cleanly this is just the
/// plural form (`Stores`), so the bulk mutation reads `createStores`. For
/// already-plural names like `likes` the suffix falls back to `ManyLikes`
/// (`createManyLikes`) to keep single and bulk mutation names distinct.
pub(crate) fn mutation_cap_plural_name(tb: &TableDefinition) -> String {
	let cap = mutation_cap_name(tb);
	let plural = pluralize(&cap);
	if plural == cap {
		format!("Many{cap}")
	} else {
		plural
	}
}

// ---------------------------------------------------------------------------
// Internal helpers (kept here so the `gql::naming` module is self-contained).
// ---------------------------------------------------------------------------

fn idiom_to_default_name(idiom: &Idiom) -> String {
	if idiom.0.len() == 1
		&& let Part::Field(name) = &idiom.0[0]
	{
		return name.as_str().to_owned();
	}
	// Multi-part fallback — sanitised string. Matches the behaviour of
	// `tables::idiom_to_gql_name` for the rare multi-part case.
	let raw = surrealdb_types::ToSql::to_sql(idiom);
	let mut out = String::with_capacity(raw.len());
	for (i, c) in raw.chars().enumerate() {
		let ok = if i == 0 {
			c.is_ascii_alphabetic() || c == '_'
		} else {
			c.is_ascii_alphanumeric() || c == '_'
		};
		out.push(if ok {
			c
		} else {
			'_'
		});
	}
	if out.is_empty() {
		out.push('_');
	}
	out
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn camel_case_basics() {
		assert_eq!(to_camel_case("first_name"), "firstName");
		assert_eq!(to_camel_case("firstName"), "firstName");
		assert_eq!(to_camel_case("a_b_c"), "aBC");
		assert_eq!(to_camel_case("user-profile"), "userProfile");
		assert_eq!(to_camel_case(""), "");
	}

	#[test]
	fn pascal_case_basics() {
		assert_eq!(to_pascal_case("first_name"), "FirstName");
		assert_eq!(to_pascal_case("store"), "Store");
		assert_eq!(to_pascal_case("customer_order"), "CustomerOrder");
	}

	#[test]
	fn pluralization_rules() {
		assert_eq!(pluralize("store"), "stores");
		assert_eq!(pluralize("box"), "boxes");
		assert_eq!(pluralize("watch"), "watches");
		assert_eq!(pluralize("dish"), "dishes");
		assert_eq!(pluralize("city"), "cities");
		assert_eq!(pluralize("day"), "days"); // vowel before y
		assert_eq!(pluralize("Person"), "Persons"); // naive, no irregulars
		assert_eq!(pluralize("orders"), "orders"); // already plural
		assert_eq!(pluralize(""), "");
	}
}
