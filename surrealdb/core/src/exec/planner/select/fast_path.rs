//! The pipelines that compile to a single scan.
//!
//! A fast path is a claim about a pipeline's *shape*: that the operators it
//! replaces are not in the tree, so one scan can answer the statement. Each
//! matcher here writes that shape out over the clauses the fold collected and
//! the sources it stopped at, checks it, and hands back the parts the shape
//! guarantees — so the compiler that follows never re-derives them and never
//! needs a guard for a case the match already excluded.
//!
//! Every matcher destructures [`PipelineClauses`] with no `..`. Adding a
//! clause to the collection therefore fails to compile here until each fast
//! path states its position on it, which is the whole point: a clause nobody
//! placed would otherwise be dropped silently by a path that skips the
//! operator carrying it.
//!
//! `EXPLAIN` never reaches a matcher: the fold applies it to whatever operator
//! the pipeline compiles to.

use surrealdb_strand::TableName;

use super::super::fold::{GroupClause, PipelineClauses};
use crate::exec::operators::CountOutput;
use crate::expr::Function;
use crate::expr::logical::{
	LogicalExpr, LogicalField, LogicalFields, LogicalOp, LogicalPermissionFilter,
	LogicalRecordIdKey, LogicalSource, OutputPath, SourceRef,
};
use crate::expr::with::With;

/// `SELECT count() FROM <source> GROUP ALL` — a plan of
///
/// ```text
/// [Limit] ← Aggregate { GROUP ALL, count() } ← Source
/// ```
///
/// A grouped statement projects inside its aggregation, so there is no
/// `Project` operator unless it wrote `OMIT` — which this shape rejects,
/// leaving the aggregation's own fields as the only projection.
///
/// `LIMIT` and `START` are matched but not consumed: the aggregation yields
/// one row and the caller pages it with a `Limit` operator over the scan.
pub(crate) struct CountAll<'a> {
	/// The single source to count: a table, a record id, or a record-id range.
	pub(crate) source: &'a SourceRef,
	/// The shape the counted row is answered in.
	pub(crate) output: CountOutput,
}

/// Match [`CountAll`], or `None` when the plan carries an operator a plain
/// key-range count cannot answer.
pub(crate) fn match_count_all<'a>(
	sources: &'a [LogicalOp],
	clauses: &PipelineClauses,
) -> Option<CountAll<'a>> {
	let PipelineClauses {
		project,
		group,
		cond,
		split,
		order,
		fetch,
		// Paging is applied over the counted row, not inside the count.
		limit: _,
		start: _,
		// The tail the caller wraps around whatever this compiles to.
		only: _,
		version: _,
		timeout: _,
		// `ORDER BY` is absent below, so there is no sort to spill.
		tempfiles: _,
	} = clauses;

	// `GROUP ALL` with `count()` and nothing else, not a grouping that
	// produces a row per key.
	let Some(GroupClause {
		group,
		fields,
	}) = group
	else {
		return None;
	};
	if !group.is_empty() || !is_count_all_only(fields) {
		return None;
	}
	// A `Project` above a grouped pipeline carries an `OMIT`, which removes a
	// field after the count.
	if project.is_some() {
		return None;
	}
	// A predicate, a split or a fetch changes which rows are counted, and an
	// ordering has rows to sort only once they are materialised. Each is an
	// operator between the aggregation and the source that a key-range count
	// would skip. A `WHERE` is answered by [`match_indexed_count_all`].
	if cond.is_some() || split.is_some() || order.is_some() || !fetch.is_empty() {
		return None;
	}
	// One source, and one whose rows are a contiguous key range.
	let [
		LogicalOp::Source(LogicalSource {
			source,
			..
		}),
	] = sources
	else {
		return None;
	};
	// A source the planner can prove names a key range holding every row it
	// yields. `CountScan` counts the keys of that range and answers with the
	// count whether or not the range holds anything, so a source that only
	// might name such a range stays on the full pipeline, which counts
	// whatever rows the value yields: a parameter still bound at run time,
	// which a block-local `LET` leaves unsubstituted; and a postfix call or
	// method call, which answers with whatever its body returns.
	//
	// Spelling a range is not naming one. A dynamic source is an expression
	// the plan evaluates against no row, and a value source yields its value
	// as a single row — a range value included, which is why `SELECT * FROM
	// (t:1)..` answers one row holding `t:1..`. So `(t:1)..` counts 1, and
	// only the record-id range `t:1..`, which the parser resolves to a source
	// naming a key range, counts the keys.
	//
	// A record id whose key is a single one names one row rather than a range,
	// so it stays on the full pipeline, which counts the rows the source
	// yields. Either plan answers the same number — `GROUP ALL` reports its
	// group whether or not a row reaches it — so this is a choice of plan
	// rather than of answer. A record id whose key is a range names a range
	// like any other.
	let proven = match source {
		SourceRef::Table(_) => true,
		SourceRef::RecordId(record) => matches!(record.key, LogicalRecordIdKey::Range(_)),
		SourceRef::Dynamic(_) | SourceRef::Param(_) | SourceRef::Mock(_) => false,
	};
	if !proven {
		return None;
	}
	Some(CountAll {
		source,
		output: count_output(fields)?,
	})
}

/// `SELECT count() FROM <table> WHERE <cond> GROUP ALL` — a plan of
///
/// ```text
/// [Limit] ← Aggregate { GROUP ALL, count() } ← Filter ← Source { Table }
/// ```
///
/// Narrower than [`CountAll`] in its source: `IndexCountScan` counts index
/// entries, and an index is defined on a table. A record id or a range names
/// rows directly and has no index slice to walk, and a parameter has not been
/// resolved to a table at this point in planning.
pub(crate) struct IndexedCountAll<'a> {
	/// The table whose indexes may answer the predicate.
	pub(crate) table: &'a TableName,
	/// The predicate to count matches of.
	pub(crate) cond: &'a LogicalExpr,
	/// The source's index hint, which constrains which indexes may be used.
	pub(crate) with: Option<&'a With>,
	/// The shape the counted row is answered in.
	pub(crate) output: CountOutput,
}

/// Match [`IndexedCountAll`], or `None` when no index-assisted count can apply.
///
/// A match makes the count *eligible*; whether an index actually answers the
/// predicate is resolved against the catalog by the caller, which falls back
/// to the full pipeline when none does.
pub(crate) fn match_indexed_count_all<'a>(
	sources: &'a [LogicalOp],
	clauses: &'a PipelineClauses,
) -> Option<IndexedCountAll<'a>> {
	let PipelineClauses {
		project,
		group,
		cond,
		split,
		order,
		fetch,
		// Paging is applied over the counted row, not inside the count.
		limit: _,
		start: _,
		// The tail the caller wraps around whatever this compiles to.
		only: _,
		version: _,
		timeout: _,
		// `ORDER BY` is absent below, so there is no sort to spill.
		tempfiles: _,
	} = clauses;

	let Some(GroupClause {
		group,
		fields,
	}) = group
	else {
		return None;
	};
	if !group.is_empty() || !is_count_all_only(fields) || project.is_some() {
		return None;
	}
	if split.is_some() || order.is_some() || !fetch.is_empty() {
		return None;
	}
	// The predicate is what distinguishes this shape from [`CountAll`]; a
	// plan without one has no index slice to count.
	let cond = cond.as_ref()?;
	let [
		LogicalOp::Source(LogicalSource {
			source: SourceRef::Table(table),
			with,
			..
		}),
	] = sources
	else {
		return None;
	};
	Some(IndexedCountAll {
		table,
		cond,
		with: with.as_ref(),
		output: count_output(fields)?,
	})
}

/// The source a count shape is over when the analyzer put the table's
/// permission above it — `PermissionFilter` nodes, one or more, evaluated as
/// the caller, directly over a table or record-id `Source`:
///
/// ```text
/// Aggregate { GROUP ALL, count() } ← [Filter] ← PermissionFilter+ ← Source
/// ```
///
/// A count reads no field, so the table permission is the only node the
/// analyzer places above a counted source. `CountScan` and `IndexCountScan`
/// resolve that same permission against the catalog when they run — a table
/// the caller may not see counts to nothing, and a conditional permission
/// counts the records it admits, each judged in the permission frame — so the
/// scan stands in for the node, and the count shapes above are matched over
/// the source it returns. A `PermissionFilter` evaluated as another principal
/// (`auth` set) is not the caller's permission and is left in place.
pub(crate) fn permission_filtered_source(node: &LogicalOp) -> Option<&LogicalOp> {
	let mut cur = node;
	let mut depth = 0usize;
	while let LogicalOp::PermissionFilter(LogicalPermissionFilter {
		input,
		auth: None,
		..
	}) = cur
	{
		depth += 1;
		cur = input;
	}
	match cur {
		LogicalOp::Source(LogicalSource {
			source: SourceRef::Table(_) | SourceRef::RecordId(_),
			..
		}) if depth > 0 => Some(cur),
		_ => None,
	}
}

/// Whether every field of a projection is a bare `count()` call, and there is
/// at least one.
fn is_count_all_only(fields: &LogicalFields) -> bool {
	fn expr_is_count(expr: &LogicalExpr) -> bool {
		let LogicalExpr::Call(call) = expr else {
			return false;
		};
		call.args.is_empty() && matches!(&call.receiver, Function::Normal(name) if name == "count")
	}
	match fields {
		LogicalFields::Value {
			expr,
			..
		} => expr_is_count(expr),
		LogicalFields::Select(fields) => {
			!fields.is_empty()
				&& fields.iter().all(|f| match f {
					LogicalField::All => false,
					LogicalField::Single {
						expr,
						..
					} => expr_is_count(expr),
				})
		}
	}
}

/// The shape the counted row is answered in: the count itself under `VALUE`,
/// otherwise one output name per `count()` field, in order — its alias when
/// the statement wrote one, else the name derived from the call.
///
/// A count scan stands in for the projection as well as the aggregation, so
/// the shortcut has to carry what that projection would have done. `VALUE`
/// asks for the count rather than for a row holding it, and the alias it may
/// carry names nothing.
///
/// `None` where an alias names a path rather than a key (`AS foo.bar`), which
/// declines the shortcut. A count scan builds its row as a flat map of name to
/// value, while the aggregation it stands in for places such an alias as
/// `{ foo: { bar: … } }`; a shortcut may only answer what the pipeline it
/// replaces would. A backtick-quoted `` AS `foo.bar` `` is one key and is
/// answered here.
///
/// Only reachable behind a match that proved every field is a `count()` call.
fn count_output(fields: &LogicalFields) -> Option<CountOutput> {
	fn name(expr: &LogicalExpr, alias: Option<&OutputPath>) -> Option<String> {
		match alias {
			Some(alias) => (alias.parts().len() == 1).then(|| alias.name()),
			None => Some(expr.output_path().name()),
		}
	}
	match fields {
		LogicalFields::Value {
			..
		} => Some(CountOutput::Value),
		LogicalFields::Select(field_list) => field_list
			.iter()
			.map(|f| match f {
				LogicalField::Single {
					expr,
					alias,
					..
				} => name(expr, alias.as_ref()),
				LogicalField::All => None,
			})
			.collect::<Option<Vec<_>>>()
			.map(CountOutput::Fields),
	}
}

#[cfg(test)]
mod tests {
	use super::super::super::fold::{PipelineStop, collect_pipeline};
	use super::super::clause_is_absent;
	use super::*;
	use crate::expr::logical::{
		EvalFrame, LogicalLiteral, LogicalProject, LogicalUnionAll, lower_select,
	};
	use crate::syn;

	/// The sources and clauses of `sql` as the physical planner receives
	/// them: lowered to a plan and collected off it.
	fn shape_of(sql: &str) -> (Vec<LogicalOp>, PipelineClauses) {
		let ast = syn::parse(sql).expect("parse");
		let mut exprs = ast.expressions;
		assert_eq!(exprs.len(), 1, "expected one statement in {sql:?}");
		let surrealdb_expr::parser::TopLevelExpr::Expr(crate::expr::Expr::Select(select)) =
			exprs.remove(0)
		else {
			panic!("`{sql}` is not a SELECT");
		};
		let (clauses, stop) = collect_pipeline(lower_select(*select).expect("SELECT lowers"));
		let sources = match stop {
			PipelineStop::Leaf(LogicalOp::UnionAll(LogicalUnionAll {
				inputs,
			})) => inputs,
			PipelineStop::Leaf(leaf) => vec![leaf],
			PipelineStop::Node(node) => panic!("`{sql}` stopped at {}", node.name()),
		};
		(sources, clauses)
	}

	#[test]
	fn count_all_matches_a_bare_grouped_count() {
		let (sources, clauses) = shape_of("SELECT count() FROM t GROUP ALL");
		let matched = match_count_all(&sources, &clauses).expect("matches");
		assert_eq!(matched.output, CountOutput::Fields(vec!["count".to_string()]));
		assert!(matches!(matched.source, SourceRef::Table(_)));
		// The same plan is not an *indexed* count: there is no predicate.
		assert!(match_indexed_count_all(&sources, &clauses).is_none());
	}

	#[test]
	fn count_all_names_the_alias_the_statement_wrote() {
		let (sources, clauses) = shape_of("SELECT count() AS total FROM t GROUP ALL");
		let matched = match_count_all(&sources, &clauses).expect("matches");
		assert_eq!(matched.output, CountOutput::Fields(vec!["total".to_string()]));
	}

	/// `VALUE` asks for the count itself, so the shortcut carries the value
	/// shape rather than a name — including over the alias `VALUE` discards.
	#[test]
	fn count_all_answers_a_value_projection_with_the_count_itself() {
		for sql in [
			"SELECT VALUE count() FROM t GROUP ALL",
			"SELECT VALUE count() AS total FROM t GROUP ALL",
		] {
			let (sources, clauses) = shape_of(sql);
			let matched = match_count_all(&sources, &clauses).expect("matches");
			assert_eq!(matched.output, CountOutput::Value, "{sql}");
		}
		let (sources, clauses) = shape_of("SELECT VALUE count() FROM t WHERE a > 1 GROUP ALL");
		let matched = match_indexed_count_all(&sources, &clauses).expect("matches");
		assert_eq!(matched.output, CountOutput::Value);
	}

	#[test]
	fn count_all_survives_paging_for_the_caller_to_apply() {
		for sql in [
			"SELECT count() FROM t GROUP ALL LIMIT 0",
			"SELECT count() FROM t GROUP ALL START 5",
			"SELECT count() FROM t GROUP ALL LIMIT 1 START 1",
			"SELECT count() FROM t:1..5 GROUP ALL",
			// An open range over a record id names a key range too.
			"SELECT count() FROM t:1.. GROUP ALL",
			"SELECT count() FROM t:1>.. GROUP ALL",
		] {
			let (sources, clauses) = shape_of(sql);
			assert!(match_count_all(&sources, &clauses).is_some(), "`{sql}` should match");
		}
	}

	#[test]
	fn count_all_declines_an_operator_it_would_skip() {
		for sql in [
			// Each of these is an operator between the aggregation and the
			// source, or a projection the count does not produce.
			"SELECT count() FROM t WHERE a > 1 GROUP ALL",
			"SELECT count() FROM t GROUP ALL ORDER BY count",
			"SELECT count() FROM t GROUP ALL FETCH a",
			"SELECT count() OMIT a FROM t GROUP ALL",
			"SELECT count(), a FROM t GROUP ALL",
			"SELECT count(), a FROM t GROUP BY a",
			"SELECT count() FROM t",
			"SELECT count() FROM t, u GROUP ALL",
			"SELECT count() FROM (SELECT * FROM t) GROUP ALL",
			"SELECT count() FROM [1, 2] GROUP ALL",
			// A source the planner cannot prove names a key range. The
			// parameter may hold an array, a string or an object at run time,
			// none of which a key count can answer; an open range is a key
			// range only over a record id written down as one; and a postfix
			// call answers with whatever its body returns.
			"SELECT count() FROM $tb GROUP ALL",
			"SELECT count() FROM 5.. GROUP ALL",
			"SELECT count() FROM $p.. GROUP ALL",
			"SELECT count() FROM (|| [1, 2])() GROUP ALL",
			// A single record id names one row, not a range. A row that is
			// not there leaves the aggregation with nothing to report, where
			// counting its key range would answer `0`.
			"SELECT count() FROM t:1 GROUP ALL",
		] {
			let (sources, clauses) = shape_of(sql);
			assert!(match_count_all(&sources, &clauses).is_none(), "`{sql}` should not match");
		}
	}

	/// The permission node the analyzer puts over a counted table is stood
	/// in for by the count scan; any other node, or a permission evaluated as
	/// another principal, is not.
	#[test]
	fn a_count_sees_the_table_beneath_its_permission_node() {
		use crate::expr::logical::Schema;
		let source = || {
			LogicalOp::Source(LogicalSource {
				source: SourceRef::Table(TableName::from("t")),
				schema: Some(std::sync::Arc::new(Schema::open())),
				with: None,
				for_update: false,
			})
		};
		let permission = |input: LogicalOp| {
			LogicalOp::PermissionFilter(LogicalPermissionFilter {
				input: Box::new(input),
				predicate: LogicalExpr::Literal(LogicalLiteral::Bool(false)),
				auth: None,
				guard: crate::expr::logical::GuardKind::Read,
			})
		};

		let one = permission(source());
		assert!(matches!(
			permission_filtered_source(&one),
			Some(LogicalOp::Source(LogicalSource {
				source: SourceRef::Table(t),
				..
			})) if t.as_str() == "t"
		));
		let two = permission(permission(source()));
		assert!(permission_filtered_source(&two).is_some());

		// A bare source is a leaf, not a node: nothing to stand in for.
		assert!(permission_filtered_source(&source()).is_none());
		// A projection layer between the permission and the source changes
		// the rows.
		let layered = permission(LogicalOp::Project(LogicalProject {
			input: Box::new(source()),
			fields: LogicalFields::Select(vec![
				LogicalField::All,
				LogicalField::column(
					"c",
					LogicalExpr::Literal(LogicalLiteral::Integer(1)),
					None,
					EvalFrame::computed(None),
				),
			]),
			omit: vec![],
		}));
		assert!(
			matches!(&layered, LogicalOp::PermissionFilter(LogicalPermissionFilter { input, .. }) if input.is_layer()),
			"the test shape must be a layer"
		);
		assert!(permission_filtered_source(&layered).is_none());
	}

	#[test]
	fn indexed_count_all_matches_a_grouped_count_over_a_predicate() {
		let (sources, clauses) = shape_of("SELECT count() FROM t WHERE a > 1 GROUP ALL");
		let matched = match_indexed_count_all(&sources, &clauses).expect("matches");
		assert_eq!(matched.table.as_str(), "t");
		assert_eq!(matched.output, CountOutput::Fields(vec!["count".to_string()]));
		assert!(matched.with.is_none());
		// The predicate rules out the key-range count.
		assert!(match_count_all(&sources, &clauses).is_none());
	}

	#[test]
	fn indexed_count_all_needs_a_table_to_hold_the_index() {
		for sql in [
			"SELECT count() FROM t:1 WHERE a > 1 GROUP ALL",
			"SELECT count() FROM $tb WHERE a > 1 GROUP ALL",
			"SELECT count() FROM (SELECT * FROM t) WHERE a > 1 GROUP ALL",
		] {
			let (sources, clauses) = shape_of(sql);
			assert!(
				match_indexed_count_all(&sources, &clauses).is_none(),
				"`{sql}` should not match"
			);
		}
	}

	#[test]
	fn indexed_count_all_declines_an_operator_it_would_skip() {
		for sql in [
			"SELECT count() FROM t WHERE a > 1 GROUP ALL ORDER BY count",
			"SELECT count() FROM t WHERE a > 1 GROUP ALL FETCH b",
			"SELECT count() OMIT b FROM t WHERE a > 1 GROUP ALL",
			"SELECT count(), b FROM t WHERE a > 1 GROUP ALL",
			"SELECT count(), b FROM t WHERE a > 1 GROUP BY b",
			"SELECT count() FROM t WHERE a > 1",
			"SELECT count() FROM t, u WHERE a > 1 GROUP ALL",
		] {
			let (sources, clauses) = shape_of(sql);
			assert!(
				match_indexed_count_all(&sources, &clauses).is_none(),
				"`{sql}` should not match"
			);
		}
	}

	#[test]
	fn indexed_count_all_carries_the_index_hint() {
		let (sources, clauses) =
			shape_of("SELECT count() FROM t WITH NOINDEX WHERE a > 1 GROUP ALL");
		let matched = match_indexed_count_all(&sources, &clauses).expect("matches");
		assert!(matches!(matched.with, Some(With::NoIndex)));
	}

	#[test]
	fn a_clause_folded_to_none_reads_as_absent() {
		assert!(clause_is_absent(None));
		assert!(clause_is_absent(Some(&LogicalExpr::Literal(LogicalLiteral::None))));
		assert!(!clause_is_absent(Some(&LogicalExpr::Literal(LogicalLiteral::Integer(1)))));
	}
}
