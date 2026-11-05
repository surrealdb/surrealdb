//! The row fields under which a select's rows hold its columns.

use std::collections::HashMap;

use crate::ir::{BoundExpr, BoundTarget};

/// The name a projected target has when no other target shares it: its alias, else the column,
/// function, window function, user variable or system variable it reads, which is the name the
/// engine gives the field of a select that is not keyed. `None` for any other expression, which
/// has no name of its own.
fn natural_name(target: &BoundTarget) -> Option<String> {
	if let Some(alias) = &target.alias {
		return Some(alias.clone());
	}
	match &target.expr {
		BoundExpr::Column(col) if col.column != "*" => Some(col.column.clone()),
		BoundExpr::FunctionCall {
			name,
			..
		}
		| BoundExpr::WindowFunction {
			name,
			..
		} => Some(name.clone()),
		// A field of a composite value, `(c).f`, is named by the field, as PostgreSQL names it.
		BoundExpr::Access {
			field,
			..
		} => Some(field.clone()),
		BoundExpr::UserVar(name) => Some(format!("@{name}")),
		BoundExpr::SystemVar {
			name,
			is_global,
		} => Some(if *is_global {
			format!("@@global.{name}")
		} else {
			format!("@@{name}")
		}),
		BoundExpr::Column(_)
		| BoundExpr::Literal(_)
		| BoundExpr::Param {
			..
		}
		| BoundExpr::Binary {
			..
		}
		| BoundExpr::Unary {
			..
		}
		| BoundExpr::Case {
			..
		}
		| BoundExpr::Between {
			..
		}
		| BoundExpr::InList {
			..
		}
		| BoundExpr::IsNull {
			..
		}
		| BoundExpr::Cast {
			..
		}
		| BoundExpr::ScalarSubquery(_)
		| BoundExpr::Exists(_)
		| BoundExpr::InSubquery {
			..
		}
		| BoundExpr::ArraySubquery(_) => None,
	}
}

/// The row field that holds each column of a select, in select-list order, or `None` when the
/// select has a `*` or `t.*` the binder cannot list.
///
/// A client reads a result by position, while the engine holds a row as an object with one value
/// per field, and a select list may repeat a name (`SELECT a.k, b.k`, `SELECT x AS n, y AS n`) or
/// leave a column without one (`SELECT 1, 'a'`). A column therefore keeps its natural name as its
/// field only when no other column has it; every other column is held under `?column?N`, N being
/// its 1-based position, extended with `_` until no column's natural name is the same. The keys
/// of one select are distinct.
///
/// The name a client is shown for a column is the descriptor's business and does not depend on the
/// key.
///
/// The columns of a select with an unlisted wildcard are not known before it runs, so its rows
/// are shaped by the names the engine gives their fields and no column has a key.
pub(crate) fn output_keys(targets: &[BoundTarget]) -> Option<Vec<String>> {
	if targets.iter().any(|t| t.is_wildcard || t.is_unlisted_wildcard()) {
		return None;
	}
	let naturals: Vec<Option<String>> = targets.iter().map(natural_name).collect();
	let mut uses: HashMap<&str, usize> = HashMap::with_capacity(naturals.len());
	for name in naturals.iter().flatten() {
		*uses.entry(name).or_default() += 1;
	}
	Some(
		naturals
			.iter()
			.enumerate()
			.map(|(index, natural)| {
				if let Some(name) = natural
					&& uses.get(name.as_str()) == Some(&1)
				{
					return name.clone();
				}
				let mut key = format!("?column?{}", index + 1);
				while uses.contains_key(key.as_str()) {
					key.push('_');
				}
				key
			})
			.collect(),
	)
}

#[cfg(test)]
mod tests;
