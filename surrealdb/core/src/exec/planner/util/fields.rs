//! Field-name and field-path derivation, and GROUP BY validation.
//!
//! Two related concerns:
//!
//! - **Output naming.** Given a SELECT field expression (with or without alias), produce a
//!   canonical name and structured path for the output document. `idiom_to_field_path` is the
//!   single source of truth used by both projection and aggregation; `idiom_to_field_name` derives
//!   the flat string form.
//! - **GROUP BY validation.** Reject row-scoped parameters (`$this`, `$self`, `$parent`) in grouped
//!   projections, where the iteration document has been collapsed away.

use crate::err::Error;
use crate::exec::field_path::{FieldPath, FieldPathPart};
use crate::expr::field::{Field, Fields};
use crate::expr::{Expr, Idiom};

/// Derive a field name from an expression for projection output.
pub(crate) fn derive_field_name(expr: &Expr) -> String {
	match expr {
		Expr::Idiom(idiom) => idiom_to_field_name(idiom),
		Expr::Param(param) => param.as_str().to_string(),
		Expr::FunctionCall(call) => {
			let idiom: crate::expr::idiom::Idiom = call.receiver.to_idiom();
			idiom_to_field_name(&idiom)
		}
		_ => {
			use surrealdb_types::ToSql;
			expr.to_sql()
		}
	}
}

/// Extract a field name from an idiom.
pub(crate) fn idiom_to_field_name(idiom: &Idiom) -> String {
	use surrealdb_types::ToSql;

	use crate::expr::part::Part;

	for part in idiom.0.iter() {
		if let Part::Lookup(lookup) = part
			&& let Some(alias) = &lookup.alias
		{
			return idiom_to_field_name(alias);
		}
	}

	let simplified = idiom.simplify();

	if simplified.len() == 1
		&& let Some(Part::Field(name)) = simplified.first()
	{
		return name.as_str().to_owned();
	}
	simplified.to_sql()
}

/// Extract a field path from an idiom for nested output construction.
///
/// The resulting [`FieldPath`] is built by walking the idiom's [`Part`]s
/// directly, so the structural distinction between a multi-part idiom
/// (`foo.bar` → nested path `[foo, bar]`) and a single-part idiom whose
/// identifier happens to contain a dot (`` `foo.bar` `` → flat key
/// `"foo.bar"`) is preserved all the way through projection.
///
/// Execution-only parts (array filters, indices, method calls, etc.) are
/// dropped via [`Idiom::simplify`], matching the historical behaviour of
/// only considering "simple" paths for nested output: for example,
/// `tags[WHERE type = 'library'][0].value` still nests under
/// `tags.value`.
pub(crate) fn idiom_to_field_path(idiom: &Idiom) -> FieldPath {
	use surrealdb_types::ToSql;

	use crate::expr::part::Part;

	// Graph traversals with an alias collapse to a single flat field name
	// (the alias), matching how lookups materialise into the result.
	for part in idiom.0.iter() {
		if let Part::Lookup(lookup) = part
			&& lookup.alias.is_some()
		{
			return FieldPath::field(idiom_to_field_name(idiom));
		}
	}

	// Walk the simplified idiom parts directly. Do NOT stringify and split
	// on '.', because that conflates `AS foo.bar` (multi-part idiom, nested
	// output) with `` AS `foo.bar` `` (single Part::Field whose identifier
	// contains a dot, flat output key).
	let simplified = idiom.simplify();
	let mut parts = Vec::with_capacity(simplified.0.len());
	for part in simplified.0.iter() {
		match part {
			Part::Field(name) => parts.push(FieldPathPart::Field(name.as_str().to_owned())),
			Part::Lookup(lookup) => parts.push(FieldPathPart::Lookup(lookup.to_sql())),
			// Unsupported part kinds (e.g. `Part::Start` for parameter
			// starts) fall back to a single flat field derived from the
			// idiom's field name, matching the previous behaviour.
			_ => return FieldPath::field(idiom_to_field_name(idiom)),
		}
	}

	if parts.is_empty() {
		return FieldPath::field(idiom_to_field_name(idiom));
	}

	FieldPath(parts)
}

/// Check if fields contain `$this` or `$parent` parameters (invalid in GROUP BY).
///
/// Delegates row-scope detection to [`super::super::row_scope`] so the
/// SELECT-subquery scoping rule is enforced consistently with other
/// plan-time consumers; produces the historic per-param error messages.
pub(crate) fn check_forbidden_group_by_params(fields: &Fields) -> Result<(), Error> {
	match fields {
		Fields::Value(selector) => check_expr_for_forbidden_params(&selector.expr),
		Fields::Select(field_list) => {
			for field in field_list {
				match field {
					Field::All => {}
					Field::Single(selector) => {
						check_expr_for_forbidden_params(&selector.expr)?;
					}
				}
			}
			Ok(())
		}
	}
}

fn check_expr_for_forbidden_params(expr: &Expr) -> Result<(), Error> {
	use super::super::row_scope::{AnalysisScope, RowScopeKind, first_row_scoped_reference};
	// `AnalysisScope::Recursive` is load-bearing here. A grouped projection
	// is invalid whenever any field expression mentions a row-scoped
	// parameter, *including* one that nominally rescopes through a SELECT
	// subquery (`(SELECT $parent FROM ...) AS x`) — because that
	// subquery's `$parent` resolves to the outer SELECT's `$this`, which
	// the GROUP BY has collapsed away. The current-scope variant used for
	// `WherePart::needs_parent` would miss this, breaking
	// `language/statements/select/group/parent.surql`.
	match first_row_scoped_reference(expr, AnalysisScope::Recursive) {
		None => Ok(()),
		Some(RowScopeKind::This | RowScopeKind::Self_) => Err(Error::Query {
			message: "Found a `$this` parameter refering to the document of a group by select statement\nSelect statements with a group by currently have no defined document to refer to".to_string(),
		}),
		Some(RowScopeKind::Parent) => Err(Error::Query {
			message: "Found a `$parent` parameter refering to the document of a GROUP select statement\nSelect statements with a GROUP BY or GROUP ALL currently have no defined document to refer to".to_string(),
		}),
	}
}
