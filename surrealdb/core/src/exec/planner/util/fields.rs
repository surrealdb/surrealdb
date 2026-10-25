//! Field-name and field-path derivation, path linearisation, and GROUP BY
//! validation.
//!
//! Four related concerns:
//!
//! - **Output naming.** [`LogicalExpr::output_path`] is the single source of truth for where a
//!   projected value lands; `OutputPath::name` is its flat string form.
//! - **Paths the operators can walk.** `field_path_from_expr` builds a [`FieldPath`] from a path
//!   expression, rejecting the shapes the operators cannot express (conditions, method calls).
//! - **Path steps.** `path_steps` lists a path's steps in the order they apply, for consumers that
//!   compare a path part by part against a catalog idiom.
//! - **GROUP BY validation.** Reject row-scoped parameters (`$this`, `$self`, `$parent`) in grouped
//!   projections, where the iteration document has been collapsed away.

use surrealdb_types::ToSql;

use crate::err::Error;
use crate::exec::Error as ExecError;
use crate::exec::field_path::{FieldPath, FieldPathPart};
use crate::expr::logical::{LogicalExpr, LogicalField, LogicalFields, LogicalLiteral};

/// Whether the expression is exactly a read of the row's `id` field.
pub(crate) fn idiom_is_id(expr: &LogicalExpr) -> bool {
	matches!(expr, LogicalExpr::Field(name) if name.as_str() == "id")
}

/// The steps of a path in the order they apply, root first.
///
/// Every element is the node of one step (a [`LogicalExpr::Access`], an
/// [`LogicalExpr::Index`], …) or the root the path starts from: a
/// [`LogicalExpr::Field`], a [`LogicalExpr::Doc`], or the non-path expression
/// a `(e).rest` path begins with. The forms that only sequence other steps
/// contribute no element of their own: a [`LogicalExpr::Broadcast`] is
/// replaced by the steps of its body, a [`LogicalExpr::Current`] root is
/// omitted, and the implicit [`LogicalExpr::Flatten`] placed between a lookup
/// and a filter is dropped. An [`LogicalExpr::Optional`] is one step followed
/// by the steps of its `then` body. A [`LogicalExpr::Recurse`] is one step,
/// whichever way its body was written.
///
/// An expression that is not a path is its own single element.
pub(crate) fn path_steps(expr: &LogicalExpr) -> Vec<&LogicalExpr> {
	let mut steps = Vec::new();
	collect_steps(expr, &mut steps);
	steps
}

fn collect_steps<'a>(expr: &'a LogicalExpr, out: &mut Vec<&'a LogicalExpr>) {
	// Walk the base chain iteratively so a long idiom does not recurse once
	// per step; only a body nests a further walk.
	let mut chain = Vec::new();
	let mut current = expr;
	let root = loop {
		match current.step_base() {
			Some(base) => {
				chain.push(current);
				current = base;
			}
			None => break current,
		}
	};
	if !matches!(root, LogicalExpr::Current) {
		out.push(root);
	}
	for step in chain.into_iter().rev() {
		match step {
			LogicalExpr::Broadcast {
				body,
				..
			} => collect_steps(body, out),
			LogicalExpr::Optional {
				then,
				..
			} => {
				out.push(step);
				collect_steps(then, out);
			}
			LogicalExpr::Flatten {
				explicit: false,
				..
			} => {}
			LogicalExpr::Literal(_)
			| LogicalExpr::Array(_)
			| LogicalExpr::Set(_)
			| LogicalExpr::Object(_)
			| LogicalExpr::RecordId(_)
			| LogicalExpr::Param(_)
			| LogicalExpr::Table(_)
			| LogicalExpr::Mock(_)
			| LogicalExpr::Constant(_)
			| LogicalExpr::Field(_)
			| LogicalExpr::Doc
			| LogicalExpr::Current
			| LogicalExpr::Access {
				..
			}
			| LogicalExpr::Index {
				..
			}
			| LogicalExpr::First(_)
			| LogicalExpr::Last(_)
			| LogicalExpr::Each(_)
			| LogicalExpr::Flatten {
				..
			}
			| LogicalExpr::Filter {
				..
			}
			| LogicalExpr::Method {
				..
			}
			| LogicalExpr::Destructure {
				..
			}
			| LogicalExpr::Lookup {
				..
			}
			| LogicalExpr::Recurse(_)
			| LogicalExpr::RepeatRecurse(_)
			| LogicalExpr::SetField {
				..
			}
			| LogicalExpr::Prefix {
				..
			}
			| LogicalExpr::Postfix {
				..
			}
			| LogicalExpr::Invoke {
				..
			}
			| LogicalExpr::Binary {
				..
			}
			| LogicalExpr::Call(_)
			| LogicalExpr::Closure(_)
			| LogicalExpr::If(_)
			| LogicalExpr::Throw(_)
			| LogicalExpr::Break
			| LogicalExpr::Continue
			| LogicalExpr::Subquery(_) => out.push(step),
		}
	}
}

/// Builds a [`FieldPath`] from a row-rooted path expression, rejecting shapes
/// the streaming operators cannot express: a non-row root, a `WHERE` filter, a
/// method call, a wildcard, a computed index, an optional chain, or a
/// recursion. A destructure does not affect the output path's structure and is
/// skipped. An expression that is not a row-rooted path is rejected.
pub(crate) fn field_path_from_expr(expr: &LogicalExpr) -> Result<FieldPath, Error> {
	let unsupported = || -> Error {
		ExecError::Query {
			message: format!(
				"FieldPath cannot contain complex parts like where clauses or method calls. \
				 Only simple field access (a.b.c), literal indices ([0], [$]), and graph traversals are supported. \
				 Got: {}",
				expr.to_sql()
			),
		}
		.into()
	};
	if !expr.is_path() {
		return Err(unsupported());
	}
	let steps = path_steps(expr);
	let mut parts = Vec::with_capacity(steps.len());
	for step in steps {
		match step {
			LogicalExpr::Field(name)
			| LogicalExpr::Access {
				field: name,
				..
			} => parts.push(FieldPathPart::Field(name.as_str().to_owned())),
			LogicalExpr::First(_) => parts.push(FieldPathPart::First),
			LogicalExpr::Last(_) => parts.push(FieldPathPart::Last),
			LogicalExpr::Index {
				index,
				..
			} => {
				if let LogicalExpr::Literal(LogicalLiteral::Integer(i)) = index.as_ref()
					&& *i >= 0
				{
					parts.push(FieldPathPart::Index(*i as usize));
				} else {
					return Err(unsupported());
				}
			}
			// A graph traversal keys the output by its rendering, `->table`.
			LogicalExpr::Lookup {
				lookup,
				..
			} => parts.push(FieldPathPart::Lookup(lookup.to_sql())),
			LogicalExpr::Destructure {
				..
			} => {}
			LogicalExpr::Literal(_)
			| LogicalExpr::Array(_)
			| LogicalExpr::Set(_)
			| LogicalExpr::Object(_)
			| LogicalExpr::RecordId(_)
			| LogicalExpr::Param(_)
			| LogicalExpr::Table(_)
			| LogicalExpr::Mock(_)
			| LogicalExpr::Constant(_)
			| LogicalExpr::Doc
			| LogicalExpr::Current
			| LogicalExpr::Each(_)
			| LogicalExpr::Flatten {
				..
			}
			| LogicalExpr::Filter {
				..
			}
			| LogicalExpr::Method {
				..
			}
			| LogicalExpr::Optional {
				..
			}
			| LogicalExpr::Recurse(_)
			| LogicalExpr::RepeatRecurse(_)
			| LogicalExpr::Broadcast {
				..
			}
			| LogicalExpr::SetField {
				..
			}
			| LogicalExpr::Prefix {
				..
			}
			| LogicalExpr::Postfix {
				..
			}
			| LogicalExpr::Invoke {
				..
			}
			| LogicalExpr::Binary {
				..
			}
			| LogicalExpr::Call(_)
			| LogicalExpr::Closure(_)
			| LogicalExpr::If(_)
			| LogicalExpr::Throw(_)
			| LogicalExpr::Break
			| LogicalExpr::Continue
			| LogicalExpr::Subquery(_) => return Err(unsupported()),
		}
	}
	Ok(FieldPath(parts))
}

/// Check if fields contain `$this` or `$parent` parameters (invalid in GROUP BY).
///
/// Delegates row-scope detection to [`super::super::row_scope`] so the
/// SELECT-subquery scoping rule is enforced consistently with other
/// plan-time consumers; produces the historic per-param error messages.
pub(crate) fn check_forbidden_group_by_params(fields: &LogicalFields) -> Result<(), Error> {
	match fields {
		LogicalFields::Value {
			expr,
			..
		} => check_expr_for_forbidden_params(expr),
		LogicalFields::Select(field_list) => {
			for field in field_list {
				match field {
					LogicalField::All => {}
					LogicalField::Single {
						expr,
						..
					} => {
						check_expr_for_forbidden_params(expr)?;
					}
				}
			}
			Ok(())
		}
	}
}

fn check_expr_for_forbidden_params(expr: &LogicalExpr) -> Result<(), Error> {
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
		Some(RowScopeKind::This | RowScopeKind::Self_) => Err(ExecError::Query {
			message: "Found a `$this` parameter refering to the document of a group by select statement\nSelect statements with a group by currently have no defined document to refer to".to_string(),
		}.into()),
		Some(RowScopeKind::Parent) => Err(ExecError::Query {
			message: "Found a `$parent` parameter refering to the document of a GROUP select statement\nSelect statements with a GROUP BY or GROUP ALL currently have no defined document to refer to".to_string(),
		}.into()),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::expr::logical::lower_expr;

	fn path(sql: &str) -> LogicalExpr {
		lower_expr(crate::syn::expr(sql).expect("expression")).expect("lowers")
	}

	fn steps(sql: &str) -> Vec<String> {
		let expr = path(sql);
		path_steps(&expr)
			.into_iter()
			.map(|step| match step {
				LogicalExpr::Field(name) => format!("field {name}"),
				LogicalExpr::Access {
					field,
					..
				} => format!("access {field}"),
				LogicalExpr::Each(_) => "each".to_owned(),
				LogicalExpr::Index {
					..
				} => "index".to_owned(),
				LogicalExpr::Filter {
					..
				} => "filter".to_owned(),
				LogicalExpr::Lookup {
					..
				} => "lookup".to_owned(),
				LogicalExpr::Optional {
					..
				} => "optional".to_owned(),
				other @ LogicalExpr::Literal(_)
				| other @ LogicalExpr::Array(_)
				| other @ LogicalExpr::Set(_)
				| other @ LogicalExpr::Object(_)
				| other @ LogicalExpr::RecordId(_)
				| other @ LogicalExpr::Param(_)
				| other @ LogicalExpr::Table(_)
				| other @ LogicalExpr::Mock(_)
				| other @ LogicalExpr::Constant(_)
				| other @ LogicalExpr::Doc
				| other @ LogicalExpr::Current
				| other @ LogicalExpr::First(_)
				| other @ LogicalExpr::Last(_)
				| other @ LogicalExpr::Flatten {
					..
				}
				| other @ LogicalExpr::Method {
					..
				}
				| other @ LogicalExpr::Destructure {
					..
				}
				| other @ LogicalExpr::Recurse(_)
				| other @ LogicalExpr::RepeatRecurse(_)
				| other @ LogicalExpr::Broadcast {
					..
				}
				| other @ LogicalExpr::SetField {
					..
				}
				| other @ LogicalExpr::Prefix {
					..
				}
				| other @ LogicalExpr::Postfix {
					..
				}
				| other @ LogicalExpr::Invoke {
					..
				}
				| other @ LogicalExpr::Binary {
					..
				}
				| other @ LogicalExpr::Call(_)
				| other @ LogicalExpr::Closure(_)
				| other @ LogicalExpr::If(_)
				| other @ LogicalExpr::Throw(_)
				| other @ LogicalExpr::Break
				| other @ LogicalExpr::Continue
				| other @ LogicalExpr::Subquery(_) => other.to_sql(),
			})
			.collect()
	}

	#[test]
	fn steps_follow_the_written_order_through_broadcasts_and_bodies() {
		assert_eq!(steps("a"), ["field a"]);
		assert_eq!(steps("a.b.c"), ["field a", "access b", "access c"]);
		assert_eq!(steps("a.b[0].c"), ["field a", "access b", "index", "access c"]);
		assert_eq!(steps("a.*.b"), ["field a", "each", "access b"]);
		assert_eq!(steps("->likes[WHERE x]->person"), ["lookup", "filter", "lookup"]);
		assert_eq!(steps("a.?.b"), ["field a", "optional", "access b"]);
		assert_eq!(steps("$p.x"), ["$p", "access x"]);
	}

	#[test]
	fn field_paths_accept_walkable_shapes_only() {
		let walk = |sql: &str| field_path_from_expr(&path(sql)).map(|p| p.to_string());
		assert_eq!(walk("a.b").unwrap(), "a.b");
		assert_eq!(walk("a[0].b").unwrap(), "a[0].b");
		assert_eq!(walk("a.b[0]").unwrap(), "a.b[0]");
		assert_eq!(walk("a[$]").unwrap(), "a[$]");
		assert!(walk("a[WHERE x]").is_err());
		assert!(walk("a.len()").is_err());
		assert!(walk("a.*").is_err());
		assert!(field_path_from_expr(&LogicalExpr::Literal(LogicalLiteral::Integer(1))).is_err());
	}

	#[test]
	fn output_names_come_from_the_output_path() {
		assert_eq!(path("a").output_path().name(), "a");
		assert_eq!(path("a.b").output_path().name(), "a.b");
		assert_eq!(path("tags[WHERE x][0].value").output_path().name(), "tags.value");
		assert!(idiom_is_id(&path("id")));
		assert!(!idiom_is_id(&path("id.x")));
	}
}
