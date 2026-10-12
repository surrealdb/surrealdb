//! The conjuncts that read no layer column move beneath the layers.
//!
//! Over a chain of layers the resolve pass built above a source — a stack of
//! [`LogicalOp::is_layer`] projections, possibly over the table's permission
//! nodes, possibly with a filter this rule placed earlier — a
//! [`LogicalOp::Filter`] holds the user's predicate above the whole stack.
//! A conjunct of that predicate that reads no column of any layer it would
//! cross, by name or otherwise, judges the same value on either side of those
//! layers: the layers pass every other field through unchanged. Such a
//! conjunct moves to a filter directly above the source, or directly above
//! the topmost permission node when the chain has one, merging with a filter
//! already there. The physical planner offers that filter's predicate to the
//! source's access path as the scan predicate, and compiles it in place above
//! a permission node.
//!
//! A conjunct stays above the stack when it reads a layer column
//! ([`reads::reads_beneath`] against every column of the layers crossed —
//! a nested-field guard on `a.b` is a column on `a`, so any read under `a`
//! stays), reads the row other than by name ([`reads::reads_row_opaquely`]),
//! or holds a `MATCHES` or `KNN`, which the statement's own access path
//! answers. A literal `false` moves only with every other conjunct, so that
//! the statement's own predicate keeps the cue that it admits nothing; a
//! literal `true` is dropped. Nothing crosses a permission node: the
//! conjuncts land above it, and the rule does not look beneath it.
//!
//! A column that stands for a component of the record id
//! ([`reads::key_part_columns`]) is the exception to the first of those: it
//! holds an element of the id, which every layer passes through, so a read of
//! it is the same read of the id (`city` is `id[0]`). A conjunct is judged in
//! those terms, and what moves is the conjunct written over the id, which reads
//! no column of any layer and which the access path of a source can recognise.
//! A conjunct that stays is left as it was written. Only a conjunct is
//! rewritten: the keys of a `Sort` are not, because an `ORDER BY` name is
//! resolved against the user's projection before it is read from the row, which
//! this rule cannot see, and the key of an ordering is read as a path, which
//! yields nothing for an element of a record id; and neither is a `Project`
//! item or a group key, which name what the statement returns.
//!
//! A conjunct also stays where one written ahead of it stays and can raise.
//! Moving it would put its judgement before that conjunct's, and `AND`
//! reaches them left to right: a row the moved conjunct rejects never reaches
//! the one that would have raised on it, so the statement reports no rows
//! where it owes an error. Once a raising conjunct is retained, every
//! conjunct after it is retained too — the terms ahead of it still move, so a
//! predicate whose raising term is written last pushes as before.
//!
//! The rule is a no-op when no layer stands between the filter and the place
//! the conjuncts would land, so a filter directly above a source or a
//! permission node is left as it is.

use super::reads::{
	KeyColumn, key_part_columns, layer_column_names, reads_beneath, reads_row_opaquely,
	substitute_key_parts,
};
use super::{
	Outcome, RuleContext, RuleDef, and_all, fire, is_false, is_permission_node, is_true, may_raise,
	mentions_index_operator, split_and,
};
use crate::expr::logical::{
	FieldPath, LogicalExpr, LogicalFilter, LogicalOp, LogicalProject, LogicalSource, OpKind,
};

pub(super) const RULE: RuleDef = RuleDef {
	name: "layers",
	kinds: &[OpKind::Filter],
	apply,
};

fn apply(node: &mut LogicalOp, cx: &mut RuleContext<'_>) -> bool {
	fire(node, |node| {
		let LogicalOp::Filter(LogicalFilter {
			input,
			predicate,
		}) = node
		else {
			unreachable!("the rule fires on a filter")
		};
		let unchanged = |input: Box<LogicalOp>, predicate: LogicalExpr| {
			Outcome::Unchanged(LogicalOp::Filter(LogicalFilter {
				input,
				predicate,
			}))
		};
		let Some(chain) = Chain::of(&input) else {
			return unchanged(input, predicate);
		};
		if chain.layers == 0 {
			return unchanged(input, predicate);
		}
		let mut moved = Vec::new();
		let mut kept = Vec::new();
		let mut falses = Vec::new();
		// A conjunct that moves is judged before the conjuncts written ahead
		// of it, which `AND` would have reached first and could have refused
		// the row at. That is invisible while those conjuncts only compute,
		// and decides what the statement reports as soon as one of them can
		// raise: the row a moved conjunct rejects never reaches the `THROW`
		// written before it. So a term moves only while every conjunct ahead
		// of it either moves too or is total.
		let mut raising_stands = false;
		for term in split_and(predicate) {
			if is_true(&term) {
				continue;
			}
			if is_false(&term) {
				falses.push(term);
				continue;
			}
			// A read of a column that stands for a component of the record id
			// is judged as the read of that element of the id; the conjunct
			// that moves is written that way, and one that stays is not.
			let over_the_id = substitute_key_parts(&term, &chain.keys);
			let judged = over_the_id.as_ref().unwrap_or(&term);
			if mentions_index_operator(judged)
				|| reads_row_opaquely(judged, cx.registry)
				|| reads_beneath(judged, &chain.columns)
			{
				raising_stands |= may_raise(&term, cx.permission_checked_params);
				kept.push(term);
			} else if raising_stands {
				kept.push(term);
			} else {
				moved.push(over_the_id.unwrap_or(term));
			}
		}
		if kept.is_empty() {
			moved.extend(falses);
		} else {
			kept.extend(falses);
		}
		if moved.is_empty() {
			return match and_all(kept) {
				Some(predicate) => unchanged(input, predicate),
				// Every conjunct was `true`.
				None => Outcome::Rewritten(*input),
			};
		}
		let beneath = place(*input, moved);
		Outcome::Rewritten(match and_all(kept) {
			Some(predicate) => LogicalOp::Filter(LogicalFilter {
				input: Box::new(beneath),
				predicate,
			}),
			None => beneath,
		})
	})
}

/// What stands between a filter and the place its conjuncts would land.
struct Chain {
	/// The columns of every layer crossed, as the field each lands under.
	columns: Vec<FieldPath>,
	/// The columns among them that stand for a component of the record id.
	keys: Vec<KeyColumn>,
	/// How many layers are crossed.
	layers: usize,
}

impl Chain {
	/// The chain beneath a filter whose input is `op`: layers and earlier
	/// pushed filters down to a source or a permission node. `None` for any
	/// other shape.
	fn of(mut op: &LogicalOp) -> Option<Self> {
		let mut chain = Self {
			columns: Vec::new(),
			keys: Vec::new(),
			layers: 0,
		};
		loop {
			if op.is_layer() {
				let LogicalOp::Project(LogicalProject {
					input,
					fields,
					..
				}) = op
				else {
					unreachable!("a layer is a projection")
				};
				chain.columns.extend(layer_column_names(fields));
				chain.keys.extend(key_part_columns(fields));
				chain.layers += 1;
				op = input;
				continue;
			}
			if is_permission_node(op) {
				return Some(chain);
			}
			match op {
				LogicalOp::Filter(LogicalFilter {
					input,
					..
				}) => op = input,
				LogicalOp::Source(LogicalSource {
					..
				}) => return Some(chain),
				LogicalOp::Values(_)
				| LogicalOp::Match(_)
				| LogicalOp::Write(_)
				| LogicalOp::Subquery(_)
				| LogicalOp::UnionAll(_)
				| LogicalOp::Join(_)
				| LogicalOp::PermissionFilter(_)
				| LogicalOp::Split(_)
				| LogicalOp::Aggregate(_)
				| LogicalOp::Distinct(_)
				| LogicalOp::Sort(_)
				| LogicalOp::Limit(_)
				| LogicalOp::Project(_)
				| LogicalOp::Fetch(_)
				| LogicalOp::Timeout(_)
				| LogicalOp::VersionScope(_)
				| LogicalOp::Only(_)
				| LogicalOp::Explain(_)
				| LogicalOp::Eval(_)
				| LogicalOp::If(_)
				| LogicalOp::Throw(_)
				| LogicalOp::Break(_)
				| LogicalOp::Continue(_)
				| LogicalOp::Sequence(_)
				| LogicalOp::Let(_)
				| LogicalOp::Return(_)
				| LogicalOp::Foreach(_)
				| LogicalOp::Info(_)
				| LogicalOp::Ddl(_)
				| LogicalOp::Sleep(_) => return None,
			}
		}
	}
}

/// Whether `op` is where moved conjuncts land above: the source, or the
/// topmost permission node.
fn is_landing(op: &LogicalOp) -> bool {
	matches!(op, LogicalOp::Source(_)) || is_permission_node(op)
}

/// `op`, a chain [`Chain::of`] accepted, with `moved` placed in a filter
/// directly above its landing node, merged after the conjuncts of a filter
/// already there.
fn place(op: LogicalOp, moved: Vec<LogicalExpr>) -> LogicalOp {
	if op.is_layer() {
		let LogicalOp::Project(LogicalProject {
			input,
			fields,
			omit,
		}) = op
		else {
			unreachable!("a layer is a projection")
		};
		return LogicalOp::Project(LogicalProject {
			input: Box::new(place(*input, moved)),
			fields,
			omit,
		});
	}
	if is_landing(&op) {
		return LogicalOp::Filter(LogicalFilter {
			input: Box::new(op),
			predicate: and_all(moved).expect("at least one conjunct moves"),
		});
	}
	let LogicalOp::Filter(LogicalFilter {
		input,
		predicate,
	}) = op
	else {
		unreachable!("Chain::of accepted only layers, filters and a landing node")
	};
	if is_landing(&input) {
		let mut terms = split_and(predicate);
		terms.extend(moved);
		return LogicalOp::Filter(LogicalFilter {
			input,
			predicate: and_all(terms).expect("at least one conjunct moves"),
		});
	}
	LogicalOp::Filter(LogicalFilter {
		input: Box::new(place(*input, moved)),
		predicate,
	})
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use surrealdb_strand::TableName;

	use super::super::super::lower_test as lower;
	use super::super::test_support::rewritten;
	use super::*;
	use crate::expr::Kind;
	use crate::expr::logical::{
		EvalFrame, FieldSchema, GuardKind, LogicalField, LogicalFields, LogicalPermissionFilter,
		Schema, SourceRef,
	};

	fn source() -> LogicalOp {
		LogicalOp::Source(LogicalSource {
			source: SourceRef::Table(TableName::from("t")),
			schema: Some(Arc::new(Schema::closed(vec![
				FieldSchema::new(FieldPath::field("a"), Kind::Int),
				FieldSchema::new(FieldPath::field("b"), Kind::Int),
			]))),
			with: None,
			for_update: false,
		})
	}

	/// A computed-field layer setting `name` from `expr` over `input`.
	fn layer(input: LogicalOp, name: &str, expr: LogicalExpr) -> LogicalOp {
		LogicalOp::Project(LogicalProject {
			input: Box::new(input),
			fields: LogicalFields::Select(vec![
				LogicalField::All,
				LogicalField::column(name, expr, Some(Kind::Int), EvalFrame::computed(None)),
			]),
			omit: vec![],
		})
	}

	fn permission(input: LogicalOp) -> LogicalOp {
		LogicalOp::PermissionFilter(LogicalPermissionFilter {
			input: Box::new(input),
			predicate: LogicalExpr::Binary {
				left: Box::new(LogicalExpr::Field("b".into())),
				op: crate::expr::BinaryOperator::MoreThan,
				right: Box::new(LogicalExpr::Literal(
					crate::expr::logical::LogicalLiteral::Integer(0),
				)),
			},
			auth: None,
			guard: GuardKind::Read,
		})
	}

	/// The predicate of `SELECT * FROM t WHERE <predicate>`.
	fn predicate(sql: &str) -> LogicalExpr {
		let LogicalOp::Project(LogicalProject {
			input,
			..
		}) = lower(sql)
		else {
			panic!("expected a projection");
		};
		let LogicalOp::Filter(LogicalFilter {
			predicate,
			..
		}) = *input
		else {
			panic!("expected a filter");
		};
		predicate
	}

	fn filter(input: LogicalOp, sql: &str) -> LogicalOp {
		LogicalOp::Filter(LogicalFilter {
			input: Box::new(input),
			predicate: predicate(sql),
		})
	}

	#[test]
	fn conjuncts_reading_no_layer_column_move_directly_above_the_source() {
		let plan = filter(
			layer(source(), "c", LogicalExpr::Field("a".into())),
			"SELECT * FROM t WHERE c = 4 AND a = 2 AND b > 1",
		);
		assert_eq!(
			rewritten(plan).to_string(),
			"\
Filter[c = 4]
  Project[*, a AS c : int @computed]
    Filter[a = 2 AND b > 1]
      Source[t]"
		);
	}

	#[test]
	fn a_filter_whose_every_conjunct_moves_is_removed_from_above() {
		let plan = filter(
			layer(source(), "c", LogicalExpr::Field("a".into())),
			"SELECT * FROM t WHERE a = 2",
		);
		assert_eq!(
			rewritten(plan).to_string(),
			"\
Project[*, a AS c : int @computed]
  Filter[a = 2]
    Source[t]"
		);
	}

	#[test]
	fn a_conjunct_reading_a_layer_column_or_the_row_stays() {
		let layered = layer(source(), "c", LogicalExpr::Field("a".into()));
		for sql in [
			"SELECT * FROM t WHERE c = 4",
			"SELECT * FROM t WHERE c.x = 4",
			"SELECT * FROM t WHERE $this.a = 2",
			"SELECT * FROM t WHERE type::field('a') = 2",
			"SELECT * FROM t WHERE a IN (SELECT VALUE x FROM u LIMIT 1)",
			"SELECT * FROM t WHERE a @@ 'x'",
		] {
			let plan = filter(layered.clone(), sql);
			let after = rewritten(plan.clone());
			assert!(after.to_string().starts_with("Filter["), "`{sql}`:\n{after}");
			assert!(
				matches!(&after, LogicalOp::Filter(LogicalFilter { input, .. }) if input.is_layer()),
				"`{sql}`:\n{after}"
			);
		}
	}

	#[test]
	fn nothing_crosses_a_permission_node() {
		// The conjunct lands beneath the layer and above the permission
		// filter; the permission filter keeps the source.
		let plan = filter(
			layer(permission(source()), "c", LogicalExpr::Field("a".into())),
			"SELECT * FROM t WHERE a = 2",
		);
		assert_eq!(
			rewritten(plan).to_string(),
			"\
Project[*, a AS c : int @computed]
  Filter[a = 2]
    PermissionFilter[b > 0]
      Source[t]"
		);
		// Directly above the permission node there is nothing to cross.
		let plan = filter(permission(source()), "SELECT * FROM t WHERE a = 2");
		assert_eq!(rewritten(plan.clone()), plan);
	}

	#[test]
	fn moved_conjuncts_merge_after_an_earlier_pushed_filter() {
		let pushed = LogicalOp::Filter(LogicalFilter {
			input: Box::new(source()),
			predicate: predicate("SELECT * FROM t WHERE b > 1"),
		});
		let plan = filter(
			layer(pushed, "c", LogicalExpr::Field("a".into())),
			"SELECT * FROM t WHERE a = 2 AND c = 1",
		);
		assert_eq!(
			rewritten(plan).to_string(),
			"\
Filter[c = 1]
  Project[*, a AS c : int @computed]
    Filter[b > 1 AND a = 2]
      Source[t]"
		);
	}

	#[test]
	fn a_false_conjunct_moves_only_with_every_other() {
		let layered = layer(source(), "c", LogicalExpr::Field("a".into()));
		let plan = filter(layered.clone(), "SELECT * FROM t WHERE c = 1 AND false");
		let after = rewritten(plan).to_string();
		assert!(after.starts_with("Filter[c = 1 AND false]"), "{after}");
		let plan = filter(layered, "SELECT * FROM t WHERE a = 1 AND false");
		let after = rewritten(plan).to_string();
		assert!(after.starts_with("Project["), "{after}");
		assert!(after.contains("Filter[a = 1 AND false]"), "{after}");
	}

	#[test]
	fn a_filter_over_a_plain_source_is_left_alone() {
		let plan = filter(source(), "SELECT * FROM t WHERE a = 2");
		assert_eq!(rewritten(plan.clone()), plan);
	}

	// --- columns that stand for a component of the record id ---

	/// `id[position]`: what a column standing for a component of the record id
	/// evaluates.
	fn id_element(position: i64) -> LogicalExpr {
		LogicalExpr::Index {
			base: Box::new(LogicalExpr::Field("id".into())),
			index: Box::new(LogicalExpr::Literal(crate::expr::logical::LogicalLiteral::Integer(
				position,
			))),
		}
	}

	/// A layer deriving each of `names` as the element of the record id at its
	/// position over `input`.
	fn key_layer(input: LogicalOp, names: &[&str]) -> LogicalOp {
		let mut fields = vec![LogicalField::All];
		fields.extend(names.iter().zip(0_i64..).map(|(name, position)| {
			LogicalField::column(
				*name,
				id_element(position),
				Some(Kind::Int),
				EvalFrame::key_part(),
			)
		}));
		LogicalOp::Project(LogicalProject {
			input: Box::new(input),
			fields: LogicalFields::Select(fields),
			omit: vec![],
		})
	}

	/// Whether `op` is a filter directly over a layer that stands directly over
	/// the source: the conjuncts did not move.
	fn stays_over_the_layer(op: &LogicalOp) -> bool {
		let LogicalOp::Filter(LogicalFilter {
			input,
			..
		}) = op
		else {
			return false;
		};
		let LogicalOp::Project(LogicalProject {
			input: beneath,
			..
		}) = &**input
		else {
			return false;
		};
		input.is_layer() && matches!(&**beneath, LogicalOp::Source(_))
	}

	#[test]
	fn a_conjunct_over_a_key_column_moves_as_a_read_of_the_id() {
		let plan = filter(
			key_layer(source(), &["city", "time"]),
			"SELECT * FROM t WHERE city = 'x' AND a = 2",
		);
		assert_eq!(
			rewritten(plan).to_string(),
			"\
Project[*, id[0] AS city : int @computed @key, id[1] AS time : int @computed @key]
  Filter[id[0] = 'x' AND a = 2]
    Source[t]"
		);
	}

	#[test]
	fn a_conjunct_over_a_key_column_lands_above_the_permission_node() {
		let plan =
			filter(key_layer(permission(source()), &["city"]), "SELECT * FROM t WHERE city = 'x'");
		assert_eq!(
			rewritten(plan).to_string(),
			"\
Project[*, id[0] AS city : int @computed @key]
  Filter[id[0] = 'x']
    PermissionFilter[b > 0]
      Source[t]"
		);
	}

	#[test]
	fn a_read_nested_in_a_call_is_substituted_too() {
		let plan = filter(
			key_layer(source(), &["city"]),
			"SELECT * FROM t WHERE string::lowercase(city) = 'x'",
		);
		let after = rewritten(plan).to_string();
		assert!(after.starts_with("Project[*, id[0] AS city"), "{after}");
		assert!(after.contains("Filter[string::lowercase(id[0]) = 'x']"), "{after}");
	}

	#[test]
	fn a_field_of_a_key_column_is_a_field_of_the_element() {
		let plan = filter(key_layer(source(), &["city"]), "SELECT * FROM t WHERE city.x = 4");
		let after = rewritten(plan).to_string();
		assert!(after.starts_with("Project[*, id[0] AS city"), "{after}");
		assert!(after.contains("Filter[id[0].x = 4]"), "{after}");
	}

	#[test]
	fn a_key_column_beside_a_computed_column_moves_alone() {
		let layered = layer(key_layer(source(), &["city"]), "c", LogicalExpr::Field("a".into()));
		// `c` is set by a layer above the key layer, so the conjunct over it
		// stays; the one over `city` and the one over the stored `a` move.
		let plan = filter(layered.clone(), "SELECT * FROM t WHERE city = 'x' AND c = 1 AND a = 2");
		assert_eq!(
			rewritten(plan).to_string(),
			"\
Filter[c = 1]
  Project[*, a AS c : int @computed]
    Project[*, id[0] AS city : int @computed @key]
      Filter[id[0] = 'x' AND a = 2]
        Source[t]"
		);
		// A disjunction that reads both is one conjunct: it stays, written as
		// the statement wrote it.
		let plan = filter(layered, "SELECT * FROM t WHERE city = 'x' OR c = 1");
		let after = rewritten(plan.clone());
		assert_eq!(after, plan, "{after}");
	}

	#[test]
	fn a_read_of_the_row_other_than_by_name_stays_over_the_layer() {
		let layered = key_layer(source(), &["city"]);
		for sql in [
			"SELECT * FROM t WHERE $this.city = 'x'",
			"SELECT * FROM t WHERE type::field('city') = 'x'",
			"SELECT * FROM t WHERE city IN (SELECT VALUE x FROM u LIMIT 1)",
		] {
			let after = rewritten(filter(layered.clone(), sql));
			assert!(stays_over_the_layer(&after), "`{sql}`:\n{after}");
		}
	}

	#[test]
	fn a_read_of_the_name_in_a_body_is_not_a_read_of_the_column() {
		// The `city` inside the filter body names a field of the element being
		// filtered. The one outside is the column; the conjunct reads both, so it
		// stays as written.
		let layered = key_layer(source(), &["city"]);
		let plan =
			filter(layered.clone(), "SELECT * FROM t WHERE city = tags[WHERE city = 'x'][0]");
		let after = rewritten(plan.clone());
		assert_eq!(after, plan, "{after}");
		// With nothing else naming the column the conjunct stays too: the body's
		// read is not substituted, and a read of a column anywhere in a
		// conjunct is a read of the column.
		let plan = filter(layered, "SELECT * FROM t WHERE tags[WHERE city = 'x'] != []");
		let after = rewritten(plan.clone());
		assert_eq!(after, plan, "{after}");
	}

	#[test]
	fn a_conjunct_that_stays_and_can_raise_keeps_the_one_after_it() {
		let layered = layer(key_layer(source(), &["city"]), "c", LogicalExpr::Field("a".into()));
		// `c + 1` reads a column and can raise, so `city = 'x'` after it is not
		// judged ahead of it.
		let plan = filter(layered.clone(), "SELECT * FROM t WHERE c + 1 = 2 AND city = 'x'");
		let after = rewritten(plan.clone());
		assert_eq!(after, plan, "{after}");
		// A conjunct that stays and cannot raise holds nothing back.
		let plan = filter(layered, "SELECT * FROM t WHERE c = 1 AND city = 'x'");
		assert_eq!(
			rewritten(plan).to_string(),
			"\
Filter[c = 1]
  Project[*, a AS c : int @computed]
    Project[*, id[0] AS city : int @computed @key]
      Filter[id[0] = 'x']
        Source[t]"
		);
	}

	#[test]
	fn the_keys_of_a_sort_are_not_substituted() {
		// `ORDER BY` names are resolved against the projection before the row,
		// and the key of an ordering is read as a path; the rule leaves them.
		let layered = key_layer(source(), &["city"]);
		let plan = lower("SELECT * FROM t ORDER BY city");
		let Ok(plan) = plan.transform_up(&mut |node| {
			Ok::<_, std::convert::Infallible>(if matches!(node, LogicalOp::Source(_)) {
				layered.clone()
			} else {
				node
			})
		});
		let after = rewritten(plan).to_string();
		assert!(after.contains("Sort[ORDER BY city"), "{after}");
		assert_eq!(after.matches("id[0]").count(), 1, "only the column holds the read:\n{after}");
	}

	#[test]
	fn a_substituted_move_is_a_fixpoint() {
		let plan = filter(
			key_layer(source(), &["city", "time"]),
			"SELECT * FROM t WHERE city = 'x' AND time > 1 AND a = 2",
		);
		let once = rewritten(plan);
		assert_eq!(rewritten(once.clone()), once, "{once}");
	}

	#[test]
	fn a_chain_with_no_key_column_is_judged_as_it_was_written() {
		// A computed column whose body is `id[0]` is not a key column: the
		// conjunct over it stays.
		let layered = layer(source(), "c", id_element(0));
		let plan = filter(layered, "SELECT * FROM t WHERE c = 'x'");
		let after = rewritten(plan.clone());
		assert_eq!(after, plan, "{after}");
	}
}

/// What binding a statement over a table whose record id names its components
/// puts beneath the layers.
#[cfg(all(test, feature = "kv-mem"))]
mod bound {
	use super::super::super::test_fixture::{Fixture, owner};

	const READINGS: &str = "DEFINE TABLE reading SCHEMAFULL PERMISSIONS FULL;
		DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
		DEFINE FIELD temp ON reading TYPE int;";

	/// Where `needle` first stands in `plan`, which the test requires.
	fn at(plan: &str, needle: &str) -> usize {
		plan.find(needle).unwrap_or_else(|| panic!("`{needle}` is not in:\n{plan}"))
	}

	#[tokio::test]
	async fn a_conjunct_over_a_component_is_judged_beneath_the_layer() {
		let fx = Fixture::new(READINGS).await;
		let plan = fx.bind_as(&owner(), "SELECT * FROM reading WHERE city = 'x'").await;
		// The layer still derives the column the `*` returns; the conjunct is
		// over the id, beneath it, and is not left above.
		assert!(at(&plan, "id[0] AS city") < at(&plan, "Filter[id[0] = 'x']"), "{plan}");
		assert!(!plan.contains("Filter[city"), "{plan}");
	}

	#[tokio::test]
	async fn a_layer_nothing_reads_once_the_conjunct_moved_is_elided() {
		let fx = Fixture::new(READINGS).await;
		let plan = fx.bind_as(&owner(), "SELECT temp FROM reading WHERE city = 'x'").await;
		assert!(!plan.contains("@key"), "{plan}");
		assert!(plan.contains("Filter[id[0] = 'x']"), "{plan}");
		assert!(!plan.contains("AS city"), "{plan}");
		// The same for a `VALUE` projection.
		let plan = fx.bind_as(&owner(), "SELECT VALUE temp FROM reading WHERE city = 'x'").await;
		assert!(!plan.contains("@key"), "{plan}");
		assert!(plan.contains("Filter[id[0] = 'x']"), "{plan}");
	}

	#[tokio::test]
	async fn a_component_the_projection_reads_keeps_its_column_and_its_name() {
		let fx = Fixture::new(READINGS).await;
		let plan = fx.bind_as(&owner(), "SELECT city FROM reading WHERE city = 'x'").await;
		assert!(plan.starts_with("Project[city]"), "{plan}");
		assert!(at(&plan, "id[0] AS city") < at(&plan, "Filter[id[0] = 'x']"), "{plan}");
	}

	#[tokio::test]
	async fn a_component_a_sort_or_a_grouping_reads_keeps_its_column() {
		let fx = Fixture::new(READINGS).await;
		let plan = fx.bind_as(&owner(), "SELECT temp FROM reading ORDER BY time").await;
		assert!(plan.contains("id[1] AS time"), "{plan}");
		assert!(plan.contains("Sort[ORDER BY time"), "{plan}");
		let plan = fx.bind_as(&owner(), "SELECT city, count() FROM reading GROUP BY city").await;
		assert!(plan.contains("id[0] AS city"), "{plan}");
		assert!(!plan.contains("AS time"), "{plan}");
	}
}
