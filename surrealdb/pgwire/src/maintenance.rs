//! `VACUUM` and `ANALYZE`, which a client sends to reclaim space and refresh planner statistics.
//!
//! The datastore keeps no dead tuples and no planner statistics for a client to refresh, so both
//! commands succeed without effect, and answer with their command tag as PostgreSQL does.

use super::sqlprepare::first_keyword;

/// The command tag `stmt` completes with when it is a `VACUUM` or `ANALYZE` command, and `None`
/// for any other statement. Only a command made of words, names, commas, dots and parentheses is
/// accepted, so a statement of another shape falls through to the parser and its error.
pub(crate) fn command_tag(stmt: &str) -> Option<&'static str> {
	let first = first_keyword(stmt);
	let tag = if first.eq_ignore_ascii_case("vacuum") {
		"VACUUM"
	} else if first.eq_ignore_ascii_case("analyze") || first.eq_ignore_ascii_case("analyse") {
		"ANALYZE"
	} else {
		return None;
	};
	let shaped = stmt.trim_end_matches(';').chars().all(|c| {
		c.is_ascii_alphanumeric()
			|| c.is_whitespace()
			|| matches!(c, '_' | ',' | '.' | '(' | ')' | '"' | '=' | '-' | '*')
	});
	shaped.then_some(tag)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn the_two_commands_complete_under_their_own_tags() {
		assert_eq!(command_tag("VACUUM"), Some("VACUUM"));
		assert_eq!(command_tag("vacuum analyze pgbench_branches"), Some("VACUUM"));
		assert_eq!(command_tag("VACUUM (FULL, VERBOSE) public.t, \"u\" (a, b)"), Some("VACUUM"));
		assert_eq!(command_tag("ANALYZE"), Some("ANALYZE"));
		assert_eq!(command_tag("analyse t (a)"), Some("ANALYZE"));
		assert_eq!(command_tag("VACUUM \"my-table\", parent*"), Some("VACUUM"));
	}

	#[test]
	fn other_statements_and_other_shapes_are_not_taken() {
		assert_eq!(command_tag("SELECT 1"), None);
		assert_eq!(command_tag("EXPLAIN ANALYZE SELECT 1"), None);
		assert_eq!(command_tag("VACUUM t; DROP TABLE t"), None);
		assert_eq!(command_tag("VACUUM 'x'"), None);
	}
}
