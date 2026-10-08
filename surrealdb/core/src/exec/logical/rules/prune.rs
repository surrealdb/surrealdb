//! A projection that does nothing goes; so does a layer column nothing above
//! reads.
//!
//! - **An identity projection over a plain closed schema is dropped.** `SELECT *` returns each row
//!   unchanged when the row is a record whose declared fields cannot hold a record link — `*`
//!   dereferences a link — so the projection does nothing and goes. Over an open schema it stays: a
//!   row there may be a record id or an object carrying links. A layer is never taken for one; it
//!   has columns beside `*`.
//! - **A layer column nothing above reads is dropped.** At the user's projection or a `GROUP BY`,
//!   the fields the node reads off its row, by root name, are its *demand*. The demand descends
//!   through the row-preserving operators beneath — a filter, split, sort, paging, fetch, a join
//!   (down its left input), a permission filter, a relational subquery — each adding what its own
//!   expressions read, until it reaches a layer, whose columns not in the demand are removed; a
//!   layer left with no column goes. A column the layer keeps that is read by another kept column
//!   of the same layer is kept too, since a re-derived computed field is judged against the row
//!   with the layer's guards landed. Beneath a layer the demand is what its kept columns read plus
//!   what passed through. Any expression that reads the row other than by name —
//!   [`reads::reads_row_opaquely`], `*`, a closure, a `DISTINCT` — makes the demand the whole row,
//!   and nothing beneath it is pruned.
//!
//! The demand is what resolve prunes by when it plans the layers; this rule
//! re-applies it after the other rules have moved predicates beneath layers,
//! and removes nothing resolve's own accounting would have kept for a read
//! that is still there.

use std::collections::HashSet;

use super::{Outcome, RuleContext, RuleDef, fire, named_reads};
use crate::expr::logical::{
	LogicalAggregate, LogicalExpr, LogicalField, LogicalFields, LogicalFilter, LogicalJoin,
	LogicalOp, LogicalOrdering, LogicalPermissionFilter, LogicalProject, LogicalSort, LogicalSplit,
	LogicalSubquery, LogicalUnionAll, OpKind, Schema,
};
use crate::expr::{Kind, KindLiteral};

pub(super) const RULE: RuleDef = RuleDef {
	name: "prune",
	kinds: &[OpKind::Project, OpKind::Aggregate],
	apply,
};

#[expect(
	clippy::wildcard_enum_match_arm,
	reason = "the kinds this rule fires on are declared in `RULE`"
)]
fn apply(node: &mut LogicalOp, cx: &mut RuleContext<'_>) -> bool {
	fire(node, |node| match node {
		LogicalOp::Project(project) => {
			if project.is_layer() {
				return Outcome::Unchanged(LogicalOp::Project(project));
			}
			let LogicalProject {
				input,
				fields,
				omit,
			} = project;
			if omit.is_empty()
				&& super::filter::is_identity_projection(&fields)
				&& rows_are_plain(&input.schema())
			{
				return Outcome::Rewritten(*input);
			}
			if !holds_layer(&input) {
				return Outcome::Unchanged(LogicalOp::Project(LogicalProject {
					input,
					fields,
					omit,
				}));
			}
			let mut demand = Demand::of_fields(&fields, cx);
			for expr in &omit {
				demand = demand.join(Demand::of(expr, cx));
			}
			match prune(*input, demand, cx) {
				Outcome::Rewritten(input) => {
					Outcome::Rewritten(LogicalOp::Project(LogicalProject {
						input: Box::new(input),
						fields,
						omit,
					}))
				}
				Outcome::Unchanged(input) => {
					Outcome::Unchanged(LogicalOp::Project(LogicalProject {
						input: Box::new(input),
						fields,
						omit,
					}))
				}
			}
		}
		LogicalOp::Aggregate(LogicalAggregate {
			input,
			group,
			fields,
		}) => {
			if !holds_layer(&input) {
				return Outcome::Unchanged(LogicalOp::Aggregate(LogicalAggregate {
					input,
					group,
					fields,
				}));
			}
			let mut demand = Demand::of_fields(&fields, cx);
			for expr in &group {
				demand = demand.join(Demand::of(expr, cx));
			}
			match prune(*input, demand, cx) {
				Outcome::Rewritten(input) => {
					Outcome::Rewritten(LogicalOp::Aggregate(LogicalAggregate {
						input: Box::new(input),
						group,
						fields,
					}))
				}
				Outcome::Unchanged(input) => {
					Outcome::Unchanged(LogicalOp::Aggregate(LogicalAggregate {
						input: Box::new(input),
						group,
						fields,
					}))
				}
			}
		}
		// The rule fires on no other kind; see `RULE`.
		other => Outcome::Unchanged(other),
	})
}

/// What the operators above a point read off its rows.
#[derive(Clone)]
enum Demand {
	/// The row as a whole: nothing beneath may be pruned.
	Whole,
	/// These root fields, and nothing else.
	Fields(HashSet<String>),
}

impl Demand {
	fn of(expr: &LogicalExpr, cx: &RuleContext<'_>) -> Self {
		match named_reads(expr, cx.registry) {
			Some(fields) => Demand::Fields(fields),
			None => Demand::Whole,
		}
	}

	fn of_fields(fields: &LogicalFields, cx: &RuleContext<'_>) -> Self {
		match fields {
			LogicalFields::Value {
				expr,
				..
			} => Demand::of(expr, cx),
			LogicalFields::Select(fields) => {
				let mut demand = Demand::Fields(HashSet::new());
				for field in fields {
					match field {
						LogicalField::All => return Demand::Whole,
						LogicalField::Single {
							expr,
							..
						} => demand = demand.join(Demand::of(expr, cx)),
					}
				}
				demand
			}
		}
	}

	fn join(self, other: Demand) -> Demand {
		match (self, other) {
			(Demand::Fields(mut a), Demand::Fields(b)) => {
				a.extend(b);
				Demand::Fields(a)
			}
			(Demand::Whole, _) | (_, Demand::Whole) => Demand::Whole,
		}
	}

	fn add_all<'a>(
		self,
		exprs: impl IntoIterator<Item = &'a LogicalExpr>,
		cx: &RuleContext<'_>,
	) -> Demand {
		exprs.into_iter().fold(self, |demand, expr| demand.join(Demand::of(expr, cx)))
	}
}

/// Whether the demand could reach a layer from `op`, following the same
/// inputs [`prune`] follows and stopping where it stops.
///
/// Building a demand walks every expression of the projection and allocates,
/// and a plan with no layer beneath is the common case, so the rule asks this
/// first. Keep the arms in step with [`prune`]'s: one missed here costs a
/// pruning opportunity, never a wrong plan.
#[expect(
	clippy::wildcard_enum_match_arm,
	reason = "the demand descends only through the operators listed, and stops at every other"
)]
fn holds_layer(op: &LogicalOp) -> bool {
	if op.is_layer() {
		return true;
	}
	match op {
		LogicalOp::Filter(LogicalFilter {
			input,
			..
		})
		| LogicalOp::PermissionFilter(LogicalPermissionFilter {
			input,
			..
		})
		| LogicalOp::Split(LogicalSplit {
			input,
			..
		})
		| LogicalOp::Sort(LogicalSort {
			input,
			..
		})
		| LogicalOp::Fetch(crate::expr::logical::LogicalFetch {
			input,
			..
		})
		| LogicalOp::Limit(crate::expr::logical::LogicalLimit {
			input,
			..
		})
		| LogicalOp::Timeout(crate::expr::logical::LogicalTimeout {
			input,
			..
		})
		| LogicalOp::Only(crate::expr::logical::LogicalOnly {
			input,
			..
		})
		| LogicalOp::Subquery(LogicalSubquery {
			input,
		}) => holds_layer(input),
		LogicalOp::Join(join) => holds_layer(&join.inputs[0]),
		LogicalOp::UnionAll(LogicalUnionAll {
			inputs,
		}) => inputs.iter().any(holds_layer),
		_ => false,
	}
}

/// `op` with every layer the demand reaches pruned: `Rewritten` when a
/// column or a layer was removed, `Unchanged` with `op` as it was when
/// nothing was.
#[expect(
	clippy::wildcard_enum_match_arm,
	reason = "the demand descends only through the operators listed, and stops at every other"
)]
fn prune(op: LogicalOp, demand: Demand, cx: &RuleContext<'_>) -> Outcome {
	let Demand::Fields(needed) = demand else {
		return Outcome::Unchanged(op);
	};
	if op.is_layer() {
		let LogicalOp::Project(LogicalProject {
			input,
			fields,
			omit,
		}) = op
		else {
			unreachable!("a layer is a projection")
		};
		let LogicalFields::Select(columns) = fields else {
			unreachable!("a layer is a field list")
		};
		let (kept, below, changed) = prune_columns(columns, &needed, cx);
		let (input, pruned) = match prune(*input, below, cx) {
			Outcome::Rewritten(input) => (input, true),
			Outcome::Unchanged(input) => (input, false),
		};
		let changed = changed || pruned;
		let out = if kept.len() == 1 {
			// Only `*` remains: the layer sets nothing and goes.
			input
		} else {
			LogicalOp::Project(LogicalProject {
				input: Box::new(input),
				fields: LogicalFields::Select(kept),
				omit,
			})
		};
		return if changed {
			Outcome::Rewritten(out)
		} else {
			Outcome::Unchanged(out)
		};
	}
	let demand = Demand::Fields(needed);
	match op {
		LogicalOp::Filter(LogicalFilter {
			input,
			predicate,
		}) => {
			let below = demand.add_all([&predicate], cx);
			rebuild(*input, below, cx, |input| {
				LogicalOp::Filter(LogicalFilter {
					input: Box::new(input),
					predicate,
				})
			})
		}
		LogicalOp::PermissionFilter(LogicalPermissionFilter {
			input,
			predicate,
			auth,
			guard,
		}) => {
			let below = demand.add_all([&predicate], cx);
			rebuild(*input, below, cx, |input| {
				LogicalOp::PermissionFilter(LogicalPermissionFilter {
					input: Box::new(input),
					predicate,
					auth,
					guard,
				})
			})
		}
		LogicalOp::Split(LogicalSplit {
			input,
			exprs,
		}) => {
			let below = demand.add_all(&exprs, cx);
			rebuild(*input, below, cx, |input| {
				LogicalOp::Split(LogicalSplit {
					input: Box::new(input),
					exprs,
				})
			})
		}
		LogicalOp::Sort(LogicalSort {
			input,
			ordering,
			tempfiles,
		}) => {
			let below = match &ordering {
				LogicalOrdering::Random => demand,
				LogicalOrdering::Order(keys) => demand.add_all(keys.iter().map(|k| &k.value), cx),
			};
			rebuild(*input, below, cx, |input| {
				LogicalOp::Sort(LogicalSort {
					input: Box::new(input),
					ordering,
					tempfiles,
				})
			})
		}
		LogicalOp::Fetch(crate::expr::logical::LogicalFetch {
			input,
			fetches,
		}) => {
			let below = demand.add_all(&fetches, cx);
			rebuild(*input, below, cx, |input| {
				LogicalOp::Fetch(crate::expr::logical::LogicalFetch {
					input: Box::new(input),
					fetches,
				})
			})
		}
		LogicalOp::Limit(crate::expr::logical::LogicalLimit {
			input,
			limit,
			start,
		}) => {
			let below = demand.add_all(limit.iter().chain(start.iter()), cx);
			rebuild(*input, below, cx, |input| {
				LogicalOp::Limit(crate::expr::logical::LogicalLimit {
					input: Box::new(input),
					limit,
					start,
				})
			})
		}
		LogicalOp::Timeout(crate::expr::logical::LogicalTimeout {
			input,
			timeout,
		}) => rebuild(*input, demand, cx, |input| {
			LogicalOp::Timeout(crate::expr::logical::LogicalTimeout {
				input: Box::new(input),
				timeout,
			})
		}),
		LogicalOp::Only(crate::expr::logical::LogicalOnly {
			input,
		}) => rebuild(*input, demand, cx, |input| {
			LogicalOp::Only(crate::expr::logical::LogicalOnly {
				input: Box::new(input),
			})
		}),
		LogicalOp::Subquery(LogicalSubquery {
			input,
		}) => rebuild(*input, demand, cx, |input| {
			LogicalOp::Subquery(LogicalSubquery {
				input: Box::new(input),
			})
		}),
		LogicalOp::Join(join) => {
			let mut below = demand;
			for (left, right) in &join.on {
				below = below.add_all([left, right], cx);
			}
			below = below.add_all(join.filter.iter(), cx);
			let LogicalJoin {
				inputs,
				kind,
				on,
				filter,
				alias,
				frame,
			} = join;
			let [left, right] = *inputs;
			rebuild(left, below, cx, |left| {
				LogicalOp::Join(LogicalJoin::new(left, right, kind, on, filter, alias, frame))
			})
		}
		LogicalOp::UnionAll(LogicalUnionAll {
			inputs,
		}) => {
			let mut changed = false;
			let inputs = inputs
				.into_iter()
				.map(|input| match prune(input, demand.clone(), cx) {
					Outcome::Rewritten(input) => {
						changed = true;
						input
					}
					Outcome::Unchanged(input) => input,
				})
				.collect();
			let out = LogicalOp::UnionAll(LogicalUnionAll {
				inputs,
			});
			if changed {
				Outcome::Rewritten(out)
			} else {
				Outcome::Unchanged(out)
			}
		}
		// The user's projection and a grouping reshape the row: what is
		// beneath them is theirs to prune when the rule reaches them. A
		// distinct compares rows whole. The leaves and the statements hold
		// no layer. Anything else the demand does not descend through
		// either: it reaches a layer only through an operator that hands
		// its row on unchanged.
		other => Outcome::Unchanged(other),
	}
}

/// [`prune`] over `input`, the result wrapped back into its parent by `wrap`.
fn rebuild(
	input: LogicalOp,
	demand: Demand,
	cx: &RuleContext<'_>,
	wrap: impl FnOnce(LogicalOp) -> LogicalOp,
) -> Outcome {
	match prune(input, demand, cx) {
		Outcome::Rewritten(input) => Outcome::Rewritten(wrap(input)),
		Outcome::Unchanged(input) => Outcome::Unchanged(wrap(input)),
	}
}

/// The columns of a layer the demand keeps, the demand beneath the layer, and
/// whether a column was removed.
fn prune_columns(
	columns: Vec<LogicalField>,
	needed: &HashSet<String>,
	cx: &RuleContext<'_>,
) -> (Vec<LogicalField>, Demand, bool) {
	let name_of = |column: &LogicalField| {
		column.output_path().and_then(|path| path.as_field().map(str::to_owned))
	};
	// What each column reads: its expression's named reads, with `$value`
	// standing for the column's own field. `None` reads the row whole.
	let reads_of = |column: &LogicalField| -> Option<HashSet<String>> {
		let LogicalField::Single {
			expr,
			..
		} = column
		else {
			return Some(HashSet::new());
		};
		let mut out = named_reads(expr, cx.registry)?;
		if expr.any(|e| matches!(e, LogicalExpr::Param(param) if param.as_str() == "value")) {
			out.extend(name_of(column));
		}
		Some(out)
	};
	// The kept set closes over the reads kept columns make of their siblings.
	let mut keep: HashSet<String> = needed.clone();
	let mut reads_whole = false;
	loop {
		let mut grew = false;
		for column in &columns {
			let Some(name) = name_of(column) else {
				continue;
			};
			if !keep.contains(&name) {
				continue;
			}
			match reads_of(column) {
				Some(reads) => {
					for read in reads {
						if columns.iter().any(|c| name_of(c).as_deref() == Some(read.as_str()))
							&& keep.insert(read)
						{
							grew = true;
						}
					}
				}
				None => reads_whole = true,
			}
		}
		if !grew {
			break;
		}
	}
	if reads_whole {
		let all: Vec<LogicalField> = columns;
		return (all, Demand::Whole, false);
	}
	let mut kept = Vec::new();
	let mut removed = false;
	// Beneath the layer: the passthrough names — demanded names no kept
	// column sets — and everything the kept columns read.
	let mut kept_names: HashSet<String> = HashSet::new();
	let mut reads: HashSet<String> = HashSet::new();
	for column in columns {
		match &column {
			LogicalField::All => kept.push(column),
			LogicalField::Single {
				..
			} => {
				let Some(name) = name_of(&column) else {
					// A column landing at no single field is beyond the
					// accounting: keep it, and what it reads.
					if let Some(read) = reads_of(&column) {
						reads.extend(read);
					}
					kept.push(column);
					continue;
				};
				if keep.contains(&name) {
					if let Some(read) = reads_of(&column) {
						reads.extend(read);
					}
					kept_names.insert(name);
					kept.push(column);
				} else {
					removed = true;
				}
			}
		}
	}
	let mut below: HashSet<String> =
		needed.iter().filter(|name| !kept_names.contains(*name)).cloned().collect();
	below.extend(reads);
	(kept, Demand::Fields(below), removed)
}

/// Whether every row of `schema` is a record none of whose fields can hold a
/// record link, so that `*` has nothing to dereference and nothing to reshape.
/// Only a closed schema can say so; `id` is the record's own link and is
/// exempt.
fn rows_are_plain(schema: &Schema) -> bool {
	if schema.is_open() {
		return false;
	}
	schema.fields.iter().all(|field| {
		field.path.as_field().is_some_and(|name| name.as_str() == "id")
			|| (!field.flexible && !kind_may_hold_record(&field.kind))
	})
}

/// Whether a value of `kind` may be or contain a record id.
fn kind_may_hold_record(kind: &Kind) -> bool {
	match kind {
		Kind::Any | Kind::Record(_) | Kind::Object => true,
		Kind::Array(inner, _) | Kind::Set(inner, _) => kind_may_hold_record(inner),
		Kind::Either(kinds) => kinds.iter().any(kind_may_hold_record),
		Kind::Literal(literal) => match literal {
			KindLiteral::Array(kinds) => kinds.iter().any(kind_may_hold_record),
			KindLiteral::Object(entries) => entries.values().any(kind_may_hold_record),
			KindLiteral::String(_)
			| KindLiteral::Integer(_)
			| KindLiteral::Float(_)
			| KindLiteral::Decimal(_)
			| KindLiteral::Duration(_)
			| KindLiteral::Bool(_) => false,
		},
		Kind::None
		| Kind::Null
		| Kind::Bool
		| Kind::Bytes
		| Kind::Datetime
		| Kind::Decimal
		| Kind::Duration
		| Kind::Float
		| Kind::Int
		| Kind::Number
		| Kind::String
		| Kind::Uuid
		| Kind::Regex
		| Kind::Table(_)
		| Kind::Geometry(_)
		| Kind::Function(..)
		| Kind::Range
		| Kind::File(_) => false,
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use surrealdb_strand::TableName;

	use super::super::super::lower_test as lower;
	use super::super::test_support::{rewritten, rewritten_sql};
	use super::*;
	use crate::expr::logical::{
		EvalFrame, FieldPath, FieldSchema, LogicalSource, OutputPath, SourceRef,
	};

	fn plain() -> Vec<FieldSchema> {
		vec![
			FieldSchema::new(FieldPath::field("id"), Kind::Record(vec![TableName::from("t")])),
			FieldSchema::new(FieldPath::field("a"), Kind::Int),
			FieldSchema::new(FieldPath::field("b"), Kind::option(Kind::String)),
		]
	}

	fn source(schema: Schema) -> LogicalOp {
		LogicalOp::Source(LogicalSource {
			source: SourceRef::Table(TableName::from("t")),
			schema: Some(Arc::new(schema)),
			with: None,
			for_update: false,
		})
	}

	/// `SELECT * FROM t` with `t` resolved to `schema`.
	fn star_over(schema: Schema) -> LogicalOp {
		LogicalOp::Project(LogicalProject {
			input: Box::new(source(schema)),
			fields: LogicalFields::all(),
			omit: vec![],
		})
	}

	/// A layer of `columns`, each `(name, expr, frame)`, over `input`: a
	/// computed column carries the kind `int`, a guard none.
	fn layer(input: LogicalOp, columns: Vec<(&str, LogicalExpr, EvalFrame)>) -> LogicalOp {
		let mut fields = vec![LogicalField::All];
		fields.extend(columns.into_iter().map(|(name, expr, frame)| {
			let kind =
				(frame.mode == crate::expr::logical::EvalMode::Computed).then_some(Kind::Int);
			LogicalField::column(name, expr, kind, frame)
		}));
		LogicalOp::Project(LogicalProject {
			input: Box::new(input),
			fields: LogicalFields::Select(fields),
			omit: vec![],
		})
	}

	fn select(input: LogicalOp, names: &[&str]) -> LogicalOp {
		LogicalOp::Project(LogicalProject {
			input: Box::new(input),
			fields: LogicalFields::Select(
				names
					.iter()
					.map(|n| LogicalField::user(LogicalExpr::Field((*n).into()), None))
					.collect(),
			),
			omit: vec![],
		})
	}

	fn field(name: &str) -> LogicalExpr {
		LogicalExpr::Field(name.into())
	}

	// --- identity projection ---

	#[test]
	fn an_identity_projection_over_a_plain_closed_schema_is_dropped() {
		let plan = rewritten(star_over(Schema::closed(plain())));
		assert_eq!(plan.name(), "Source", "{plan}");
	}

	#[test]
	fn an_identity_projection_over_an_open_schema_stays() {
		let plan = rewritten(star_over(Schema::open()));
		assert_eq!(plan.name(), "Project", "{plan}");
		// An unresolved source is open.
		let plan = rewritten_sql("SELECT * FROM t");
		assert!(plan.starts_with("Project[*]"), "{plan}");
	}

	#[test]
	fn a_field_that_may_hold_a_record_keeps_the_projection() {
		let linked = |kind: Kind| {
			let mut fields = plain();
			fields.push(FieldSchema::new(FieldPath::field("x"), kind));
			Schema::closed(fields)
		};
		for kind in [
			Kind::Record(vec![TableName::from("u")]),
			Kind::Any,
			Kind::Object,
			Kind::Array(Box::new(Kind::Record(vec![])), None),
			Kind::option(Kind::Record(vec![])),
		] {
			let plan = rewritten(star_over(linked(kind.clone())));
			assert_eq!(plan.name(), "Project", "a field of kind {kind:?} may hold a link:\n{plan}");
		}
		let mut flexible = plain();
		let mut f = FieldSchema::new(FieldPath::field("x"), Kind::String);
		f.flexible = true;
		flexible.push(f);
		let plan = rewritten(star_over(Schema::closed(flexible)));
		assert_eq!(plan.name(), "Project", "{plan}");
	}

	#[test]
	fn a_layer_is_never_dropped_as_an_identity_projection() {
		// The layer over a plain closed schema keeps its place; a `*` above it
		// is dropped instead, since the layer's schema is plain as well.
		let layered = layer(
			source(Schema::closed(plain())),
			vec![("c", field("a"), EvalFrame::computed(None))],
		);
		assert_eq!(rewritten(layered.clone()), layered);
		let plan = rewritten(LogicalOp::Project(LogicalProject {
			input: Box::new(layered.clone()),
			fields: LogicalFields::all(),
			omit: vec![],
		}));
		assert_eq!(plan, layered, "{plan}");
	}

	#[test]
	fn a_projected_layer_column_is_typed_by_its_declared_kind() {
		// `SELECT * FROM (SELECT c FROM t)` with `c` a layer column above the
		// source: a column declared a plain kind leaves the outer `*` nothing
		// to dereference; an untyped column whose body says nothing keeps it.
		let inner = |kind: Option<Kind>| {
			LogicalOp::Subquery(LogicalSubquery {
				input: Box::new(LogicalOp::Project(LogicalProject {
					input: Box::new(LogicalOp::Project(LogicalProject {
						input: Box::new(source(Schema::closed(plain()))),
						fields: LogicalFields::Select(vec![
							LogicalField::All,
							LogicalField::column(
								"c",
								LogicalExpr::Param(crate::expr::Param::from("p".to_string())),
								kind,
								EvalFrame::computed(None),
							),
						]),
						omit: vec![],
					})),
					fields: LogicalFields::Select(vec![LogicalField::user(
						field("c"),
						Some(OutputPath::field("c")),
					)]),
					omit: vec![],
				})),
			})
		};
		let star = |input: LogicalOp| {
			LogicalOp::Project(LogicalProject {
				input: Box::new(input),
				fields: LogicalFields::all(),
				omit: vec![],
			})
		};
		let plan = rewritten(star(inner(Some(Kind::Int))));
		assert_eq!(plan.name(), "Subquery", "{plan}");
		let plan = rewritten(star(inner(None)));
		assert_eq!(plan.name(), "Project", "{plan}");
	}

	// --- layer pruning ---

	#[test]
	fn a_layer_column_nothing_above_reads_is_removed_and_an_empty_layer_goes() {
		let layered = layer(
			source(Schema::closed(plain())),
			vec![
				("c", field("a"), EvalFrame::computed(None)),
				("d", field("b"), EvalFrame::computed(None)),
			],
		);
		let plan = rewritten(select(layered.clone(), &["a", "c"]));
		assert_eq!(
			plan.to_string(),
			"\
Project[a, c]
  Project[*, a AS c : int @computed]
    Source[t]"
		);
		let plan = rewritten(select(layered, &["a"]));
		assert_eq!(
			plan.to_string(),
			"\
Project[a]
  Source[t]"
		);
	}

	#[test]
	fn the_demand_descends_through_a_filter_and_a_sort() {
		let layered = layer(
			source(Schema::closed(plain())),
			vec![
				("c", field("a"), EvalFrame::computed(None)),
				("d", field("b"), EvalFrame::computed(None)),
			],
		);
		// `SELECT a FROM t WHERE d = 1 ORDER BY b`: the filter reads `d`, the
		// sort reads `b`; `c` is read by nothing.
		let plan = lower("SELECT a FROM t WHERE d = 1 ORDER BY b");
		let Ok(plan) = plan.transform_up(&mut |node| {
			Ok::<_, std::convert::Infallible>(if matches!(node, LogicalOp::Source(_)) {
				layered.clone()
			} else {
				node
			})
		});
		let after = rewritten(plan).to_string();
		assert!(after.contains("Project[*, b AS d : int @computed]"), "{after}");
		assert!(!after.contains("AS c"), "{after}");
	}

	#[test]
	fn a_whole_row_read_above_keeps_every_column() {
		let layered = layer(
			source(Schema::closed(plain())),
			vec![("c", field("a"), EvalFrame::computed(None))],
		);
		for reader in [
			LogicalOp::Project(LogicalProject {
				input: Box::new(layered.clone()),
				fields: LogicalFields::Select(vec![LogicalField::user(
					LogicalExpr::Param(crate::expr::Param::from("this".to_string())),
					Some(OutputPath::field("row")),
				)]),
				omit: vec![],
			}),
			LogicalOp::Project(LogicalProject {
				input: Box::new(LogicalOp::Distinct(crate::expr::logical::LogicalDistinct {
					input: Box::new(layered),
				})),
				fields: LogicalFields::Select(vec![LogicalField::user(field("a"), None)]),
				omit: vec![],
			}),
		] {
			let after = rewritten(reader.clone());
			assert_eq!(after, reader, "{after}");
		}
	}

	#[test]
	fn a_kept_column_keeps_the_sibling_it_reads() {
		// The re-derived computed field `d` is judged against the row with
		// the guard on `a` landed, so pruning the guard would change `d`.
		let layered = layer(
			source(Schema::closed(plain())),
			vec![
				("a", field("a"), EvalFrame::permission(None)),
				("d", field("a"), EvalFrame::computed(None)),
				("e", field("b"), EvalFrame::computed(None)),
			],
		);
		let plan = rewritten(select(layered, &["d"]));
		assert_eq!(
			plan.to_string(),
			"\
Project[d]
  Project[*, a AS a @permission, a AS d : int @computed]
    Source[t]"
		);
	}

	#[test]
	fn a_self_dereference_reads_the_record_whole() {
		// `id.secret` fetches the record by its id and reads the reduced view,
		// so the guard on `secret` stays although no read names `secret`.
		let layered = layer(
			source(Schema::closed(plain())),
			vec![("b", field("b"), EvalFrame::permission(None))],
		);
		let plan = LogicalOp::Project(LogicalProject {
			input: Box::new(layered),
			fields: LogicalFields::Select(vec![LogicalField::user(
				LogicalExpr::Access {
					base: Box::new(field("id")),
					field: "b".into(),
				},
				None,
			)]),
			omit: vec![],
		});
		assert_eq!(rewritten(plan.clone()), plan);
	}

	#[test]
	fn a_grouping_prunes_what_it_does_not_read() {
		let layered = layer(
			source(Schema::closed(plain())),
			vec![
				("c", field("a"), EvalFrame::computed(None)),
				("d", field("b"), EvalFrame::computed(None)),
			],
		);
		let plan = lower("SELECT c, count() FROM t GROUP BY c");
		let Ok(plan) = plan.transform_up(&mut |node| {
			Ok::<_, std::convert::Infallible>(if matches!(node, LogicalOp::Source(_)) {
				layered.clone()
			} else {
				node
			})
		});
		let after = rewritten(plan).to_string();
		assert!(after.contains("Project[*, a AS c : int @computed]"), "{after}");
		assert!(!after.contains("AS d"), "{after}");
	}
}
