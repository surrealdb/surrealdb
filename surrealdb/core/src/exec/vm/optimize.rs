//! Rewrites over the physical plan.
//!
//! Every rule is an equivalence under the laws of
//! `doc/executor/PHYSICAL_PLAN_REQUIREMENTS.md` §1: the rewritten plan
//! delivers the same rows in the same order, raises the same errors and
//! evaluates every expression as often as the plan it replaced. Rules run
//! bottom-up, repeatedly, until none applies.

use super::plan::{
	IndexScope, JoinBuild, JoinKey, NestedFacts, PhysicalPlan, Plan, SortKeyRead, Strategy,
};
use crate::exec::field_path::FieldPath;
use crate::expr::logical::{LogicalExpr, LogicalLiteral};

/// A rewrite of one node, given its already-rewritten children.
trait Rule {
	/// The rewritten node, or `None` when the rule does not apply.
	fn rewrite(&self, plan: &Plan) -> Option<Plan>;
}

const RULES: &[&dyn Rule] =
	&[&DeliveredOrder, &BoundSort, &AnsweredByLookup, &AnsweredByIndex, &ConjunctOrder];

/// How many passes a plan gets before the driver stops. Every rule makes a
/// plan strictly more annotated, so a fixed point comes long before this.
const MAX_PASSES: usize = 8;

pub(super) fn optimize(mut plan: PhysicalPlan) -> PhysicalPlan {
	for _ in 0..MAX_PASSES {
		let (root, changed) = rewrite_tree(plan.root);
		plan.root = root;
		if !changed {
			break;
		}
	}
	for nested in &mut plan.nested {
		let nested = std::sync::Arc::make_mut(nested);
		nested.plan = optimize(std::mem::replace(&mut nested.plan, empty_plan()));
		nested.strategy = strategy(nested.facts, nested.join.is_some());
		// An index over the join's field turns a hashed or per-row read into
		// an index nested loop, which reads per evaluation only the records
		// the index lists under its key. A read that could be hashed keeps its
		// probe for the evaluations past the first few of a run.
		if matches!(nested.strategy, Strategy::Hash | Strategy::Apply)
			&& let (Some(join), Some(lookup)) = (&nested.join, &nested.lookup)
		{
			let mut looked_up = nested.plan.clone();
			if insert_lookup(&mut looked_up.root, join, lookup) {
				if nested.strategy == Strategy::Hash {
					let mut hashed = nested.plan.clone();
					if insert_probe(&mut hashed.root, join) {
						nested.hashed = Some(hashed);
					}
				}
				nested.plan = looked_up;
				nested.strategy = Strategy::Lookup;
			}
		}
		if nested.strategy == Strategy::Hash {
			let join = nested.join.clone().expect("a hash strategy has a join key");
			if !insert_probe(&mut nested.plan.root, &join) {
				nested.strategy = Strategy::Apply;
			}
		}
	}
	plan
}

/// A plan with nothing in it, held while a nested plan is rewritten.
fn empty_plan() -> PhysicalPlan {
	PhysicalPlan {
		catalog: None,
		root: Plan::Union {
			inputs: Vec::new(),
		},
		dependencies: Vec::new(),
		tail: Default::default(),
		sources: Vec::new(),
		nested: Vec::new(),
		scope: IndexScope::default(),
	}
}

/// How a nested statement runs.
///
/// Once, when evaluating it for the first row that reaches it answers for
/// every row: it reads rows, reads nothing of the row it is evaluated for,
/// is repeatable, and the statement it is written in writes nothing it could
/// read. The first evaluation is where the per-row form's first evaluation
/// is, so a statement that fails fails at the same row, and one no row
/// reaches is never run.
///
/// Hashed, when it reads rows and the statement it is written in writes
/// nothing it could read, and it reads the row only through a join key that
/// leaves it repeatable (`joined`, [`JoinKey`]): what lies beneath the join
/// predicate is then the same for every row, read once where the first
/// evaluation reads it and kept, and each evaluation runs the rest of the
/// statement over the kept rows its key may equal. Applied per row, such a statement
/// reads its table whole for each row, since no index access takes a key
/// evaluated per row; the probe reads it once and holds its rows, so it
/// costs one such read and a map of its rows whatever the number of rows
/// evaluating it.
///
/// Every other statement is applied per evaluation.
fn strategy(facts: NestedFacts, joined: bool) -> Strategy {
	if !facts.reads_rows || facts.enclosing_writes {
		Strategy::Apply
	} else if !facts.correlated && facts.repeatable {
		Strategy::Once
	} else if facts.correlated && joined {
		Strategy::Hash
	} else {
		Strategy::Apply
	}
}

/// The first conjunct of `predicate`, the one evaluated first, when it is
/// `field = outer` or `outer = field` with `outer` a path into `$parent`:
/// the field and the path.
pub(super) fn join_conjunct(
	predicate: &LogicalExpr,
) -> Option<(&surrealdb_strand::Strand, &LogicalExpr)> {
	let mut first = predicate;
	while let LogicalExpr::Binary {
		left,
		op: crate::expr::BinaryOperator::And,
		..
	} = first
	{
		first = left;
	}
	let LogicalExpr::Binary {
		left,
		op: crate::expr::BinaryOperator::Equal,
		right,
	} = first
	else {
		return None;
	};
	match (left.as_ref(), right.as_ref()) {
		(LogicalExpr::Field(field), outer) | (outer, LogicalExpr::Field(field))
			if parent_path(outer) =>
		{
			Some((field, outer))
		}
		_ => None,
	}
}

/// Whether `expr` is `$parent` or a field path into it.
fn parent_path(expr: &LogicalExpr) -> bool {
	match expr {
		LogicalExpr::Param(param) => param.as_str() == "parent",
		LogicalExpr::Access {
			base,
			..
		} => parent_path(base),
		_ => false,
	}
}

/// Put a [`Plan::Probe`] beneath the innermost predicate of `plan`, keyed by
/// `join`, where that predicate's first conjunct is the join's and only a
/// table's permission and layers lie between it and a read of storage.
/// Answers whether it did; `plan` is unchanged where it did not.
fn insert_probe(plan: &mut Plan, join: &JoinKey) -> bool {
	if let Plan::Filter {
		input,
		predicate,
		..
	} = plan
		&& !holds_filter(input)
	{
		let keyed = join_conjunct(predicate)
			.is_some_and(|(field, outer)| *field == join.field && *outer == join.outer);
		if !keyed || !storage_beneath(input) {
			return false;
		}
		let empty = Plan::Union {
			inputs: Vec::new(),
		};
		let build = std::mem::replace(input, Box::new(empty));
		**input = Plan::Probe {
			build,
			field: join.field.clone(),
			outer: join.outer.clone(),
		};
		return true;
	}
	match plan {
		Plan::Filter {
			input,
			..
		}
		| Plan::PermissionFilter {
			input,
			..
		}
		| Plan::Layer {
			input,
			..
		}
		| Plan::Split {
			input,
			..
		}
		| Plan::Fetch {
			input,
			..
		}
		| Plan::Sort {
			input,
			..
		}
		| Plan::Limit {
			input,
			..
		}
		| Plan::Aggregate {
			input,
			..
		}
		| Plan::Project {
			input,
			..
		} => insert_probe(input, join),
		_ => false,
	}
}

/// Read the table beneath the innermost predicate of `plan` through
/// `lookup`'s index, keyed by `join`'s path ([`Plan::IndexLookup`]), where
/// that predicate's first conjunct is the join's and only a table's
/// permission, layers and enforcement lie between it and a forward scan of
/// the table the index belongs to. Answers whether it did; `plan` is
/// unchanged where it did not.
fn insert_lookup(
	plan: &mut Plan,
	join: &JoinKey,
	lookup: &crate::exec::index::access_path::IndexRef,
) -> bool {
	if let Plan::Filter {
		input,
		predicate,
		..
	} = plan
		&& !holds_filter(input)
	{
		let keyed = join_conjunct(predicate)
			.is_some_and(|(field, outer)| *field == join.field && *outer == join.outer);
		return keyed && lookup_beneath(input, join, lookup);
	}
	match plan {
		Plan::Filter {
			input,
			..
		}
		| Plan::PermissionFilter {
			input,
			..
		}
		| Plan::Layer {
			input,
			..
		}
		| Plan::Split {
			input,
			..
		}
		| Plan::Fetch {
			input,
			..
		}
		| Plan::Sort {
			input,
			..
		}
		| Plan::Limit {
			input,
			..
		}
		| Plan::Aggregate {
			input,
			..
		}
		| Plan::Project {
			input,
			..
		} => insert_lookup(input, join, lookup),
		_ => false,
	}
}

/// Replace the forward scan beneath `plan`'s table permission, layers and
/// enforcement with a lookup through `lookup`'s index. Answers whether it
/// did. A top-k threshold the scan took from a sort above is dropped with it:
/// the sort still keeps only the rows its limit can reach.
fn lookup_beneath(
	plan: &mut Plan,
	join: &JoinKey,
	lookup: &crate::exec::index::access_path::IndexRef,
) -> bool {
	match plan {
		Plan::PermissionFilter {
			input,
			..
		}
		| Plan::Layer {
			input,
			..
		}
		| Plan::Enforced {
			input,
			..
		} => lookup_beneath(input, join, lookup),
		Plan::TableScan {
			table,
			direction: crate::kvs::Direction::Forward,
			schema,
			..
		} if *table == lookup.table_name => {
			*plan = Plan::IndexLookup {
				table: table.clone(),
				index: lookup.clone(),
				field: join.field.clone(),
				key: super::plan::LookupKey::Parent(join.outer.clone()),
				schema: schema.clone(),
			};
			true
		}
		_ => false,
	}
}

/// Whether a predicate lies beneath `plan`'s table permission and layers.
fn holds_filter(plan: &Plan) -> bool {
	match plan {
		Plan::Filter {
			..
		} => true,
		Plan::PermissionFilter {
			input,
			..
		}
		| Plan::Layer {
			input,
			..
		} => holds_filter(input),
		_ => false,
	}
}

/// Whether `plan` is a read of storage under a table's permission and
/// layers alone.
fn storage_beneath(plan: &Plan) -> bool {
	match plan {
		Plan::PermissionFilter {
			input,
			..
		}
		| Plan::Layer {
			input,
			..
		} => storage_beneath(input),
		Plan::TableScan {
			..
		}
		| Plan::IndexScan {
			..
		}
		| Plan::IndexLookup {
			..
		}
		| Plan::IndexUnion {
			..
		}
		| Plan::Bitmap {
			..
		}
		| Plan::RecordScan {
			..
		}
		| Plan::FullTextScan {
			..
		}
		| Plan::KnnScan {
			..
		} => true,
		_ => false,
	}
}

fn rewrite_tree(plan: Plan) -> (Plan, bool) {
	let (mut plan, mut changed) = rewrite_children(plan);
	for rule in RULES {
		if let Some(rewritten) = rule.rewrite(&plan) {
			plan = rewritten;
			changed = true;
		}
	}
	(plan, changed)
}

fn rewrite_children(plan: Plan) -> (Plan, bool) {
	let rewrite = |input: Box<Plan>| {
		let (input, changed) = rewrite_tree(*input);
		(Box::new(input), changed)
	};
	match plan {
		Plan::TableScan {
			..
		}
		| Plan::RecordScan {
			..
		}
		| Plan::IndexScan {
			..
		}
		| Plan::IndexLookup {
			..
		}
		| Plan::IndexUnion {
			..
		}
		| Plan::Bitmap {
			..
		}
		| Plan::FullTextScan {
			..
		}
		| Plan::KnnScan {
			..
		}
		| Plan::Count {
			..
		}
		| Plan::Enforced {
			..
		}
		| Plan::Union {
			..
		}
		| Plan::Values {
			..
		}
		| Plan::Subquery {
			..
		}
		| Plan::Probe {
			..
		}
		| Plan::Write {
			..
		}
		| Plan::Eval {
			..
		}
		| Plan::Ddl {
			..
		}
		| Plan::Info {
			..
		}
		| Plan::Control {
			..
		}
		| Plan::Refused {
			..
		} => (plan, false),
		Plan::Filter {
			input,
			predicate,
			once,
		} => {
			let (input, changed) = rewrite(input);
			(
				Plan::Filter {
					input,
					predicate,
					once,
				},
				changed,
			)
		}
		Plan::PermissionFilter {
			input,
			predicate,
			auth,
			guard,
			hides_access_paths,
			computed,
			once,
		} => {
			let (input, changed) = rewrite(input);
			(
				Plan::PermissionFilter {
					input,
					predicate,
					auth,
					guard,
					hides_access_paths,
					computed,
					once,
				},
				changed,
			)
		}
		Plan::Layer {
			input,
			fields,
			input_schema,
		} => {
			let (input, changed) = rewrite(input);
			(
				Plan::Layer {
					input,
					fields,
					input_schema,
				},
				changed,
			)
		}
		Plan::KnnTopK {
			input,
			field,
			vector,
			k,
			distance,
		} => {
			let (input, changed) = rewrite(input);
			(
				Plan::KnnTopK {
					input,
					field,
					vector,
					k,
					distance,
				},
				changed,
			)
		}
		Plan::Sort {
			input,
			keys,
			keep,
			pushdown,
			random,
			spill,
		} => {
			let (input, changed) = rewrite(input);
			(
				Plan::Sort {
					input,
					keys,
					keep,
					pushdown,
					random,
					spill,
				},
				changed,
			)
		}
		Plan::Limit {
			input,
			limit,
			start,
		} => {
			let (input, changed) = rewrite(input);
			(
				Plan::Limit {
					input,
					limit,
					start,
				},
				changed,
			)
		}
		Plan::Fetch {
			input,
			fetches,
		} => {
			let (input, changed) = rewrite(input);
			(
				Plan::Fetch {
					input,
					fetches,
				},
				changed,
			)
		}
		Plan::Split {
			input,
			targets,
		} => {
			let (input, changed) = rewrite(input);
			(
				Plan::Split {
					input,
					targets,
				},
				changed,
			)
		}
		Plan::Step {
			input,
			mut step,
		} => {
			let (input, mut changed) = rewrite(input);
			// A join's build side is a plan of its own.
			if let super::binding::BindingStep::Join(join) = &mut *step {
				let (build, build_changed) = rewrite_tree(std::mem::replace(
					&mut *join.build,
					Plan::Union {
						inputs: Vec::new(),
					},
				));
				*join.build = build;
				changed |= build_changed;
			}
			(
				Plan::Step {
					input,
					step,
				},
				changed,
			)
		}
		Plan::Aggregate {
			input,
			group,
			fields,
			first_value,
			spill,
		} => {
			let (input, changed) = rewrite(input);
			(
				Plan::Aggregate {
					input,
					group,
					fields,
					first_value,
					spill,
				},
				changed,
			)
		}
		Plan::Project {
			input,
			projection,
		} => {
			let (input, changed) = rewrite(input);
			(
				Plan::Project {
					input,
					projection,
				},
				changed,
			)
		}
		Plan::Distinct {
			input,
			spill,
		} => {
			let (input, changed) = rewrite(input);
			(
				Plan::Distinct {
					input,
					spill,
				},
				changed,
			)
		}
		Plan::Join {
			left,
			right,
			kind,
			keys,
			residual,
			alias,
			spill,
			lookup,
			build,
		} => {
			let (left, left_changed) = rewrite(left);
			let (right, right_changed) = rewrite(right);
			let (lookup, lookup_changed) = match lookup {
				Some(super::plan::JoinLookup {
					key,
					right,
				}) => {
					let (right, changed) = rewrite(right);
					(
						Some(super::plan::JoinLookup {
							key,
							right,
						}),
						changed,
					)
				}
				None => (None, false),
			};
			(
				Plan::Join {
					left,
					right,
					kind,
					keys,
					residual,
					alias,
					spill,
					lookup,
					build,
				},
				left_changed || right_changed || lookup_changed,
			)
		}
	}
}

/// A sort whose input already delivers its order is removed.
///
/// Filters, permissions, layers and enforcement keep their input's order, so
/// the order reaching the sort is the source's. A table scan delivers `id`
/// order along its direction. An index scan that covers the pipeline's ordering delivers
/// it over the record fields it names, ties in the index's key order.
/// Only a sort whose keys read exactly those fields is removed: a key that a
/// projection renamed reads another value under the same name, and a
/// computed key would go unevaluated.
struct DeliveredOrder;

impl Rule for DeliveredOrder {
	fn rewrite(&self, plan: &Plan) -> Option<Plan> {
		let Plan::Sort {
			input,
			keys,
			random: false,
			..
		} = plan
		else {
			return None;
		};
		let mut source = &**input;
		loop {
			source = match source {
				Plan::Filter {
					input,
					..
				}
				| Plan::PermissionFilter {
					input,
					..
				}
				| Plan::Layer {
					input,
					..
				}
				| Plan::Enforced {
					input,
					..
				} => input,
				// A table scan delivers the records in key order along its
				// direction: by `id`, ascending when it reads forward.
				Plan::TableScan {
					direction,
					..
				} => {
					let by_id = matches!(
						keys.as_slice(),
						[key] if matches!(&key.read, SortKeyRead::Path(path) if *path == FieldPath::field("id"))
							&& !key.collate && !key.numeric
							&& key.ascending == matches!(direction, crate::kvs::Direction::Forward)
					);
					return by_id.then(|| (**input).clone());
				}
				Plan::IndexScan {
					ordered_by: Some(fields),
					..
				}
				| Plan::IndexUnion {
					ordered_by: Some(fields),
					..
				} => {
					let read_by_key = keys.len() == fields.len()
						&& keys.iter().zip(fields).all(
							|(key, field)| matches!(&key.read, SortKeyRead::Path(path) if path == field),
						);
					return read_by_key.then(|| (**input).clone());
				}
				_ => return None,
			};
		}
	}
}

/// A sort under a literal `LIMIT` keeps only the rows the limit can reach.
///
/// Sound because the sort is stable: the first `start + limit` rows of the
/// order are the same whether the rest were kept or not. The sort's input is
/// still drained whole, so every key and predicate is evaluated as before.
struct BoundSort;

impl Rule for BoundSort {
	fn rewrite(&self, plan: &Plan) -> Option<Plan> {
		let Plan::Limit {
			input,
			limit: Some(limit),
			start,
		} = plan
		else {
			return None;
		};
		let Plan::Sort {
			input: sorted,
			keys,
			keep: None,
			random,
			..
		} = &**input
		else {
			return None;
		};
		let keep = match start {
			Some(start) => literal_count(limit)?.checked_add(literal_count(start)?)?,
			None => literal_count(limit)?,
		};

		// The scan compares a row's first key against the threshold as plain
		// values; a `COLLATE` or `NUMERIC` key orders by another comparison,
		// which the scan does not replicate, so such a sort publishes no
		// threshold.
		let (new_sorted, pushdown) = if let Some(first_key) = keys.first()
			&& !first_key.collate
			&& !first_key.numeric
			&& let super::plan::SortKeyRead::Path(ref path) = first_key.read
			&& let Some(segments) = crate::exec::topk_pushdown::field_path_wire_segments(path)
		{
			let direction = if first_key.ascending {
				crate::exec::operators::SortDirection::Asc
			} else {
				crate::exec::operators::SortDirection::Desc
			};
			let key = super::plan::TopKKey {
				path: segments.into(),
				direction,
				single_key: keys.len() == 1,
				depth_limit: 256,
			};
			let updated = install_topk_key(sorted, key);
			(updated, true)
		} else {
			((**sorted).clone(), false)
		};

		Some(Plan::Limit {
			input: Box::new(Plan::Sort {
				input: Box::new(new_sorted),
				keys: keys.clone(),
				keep: Some(keep),
				pushdown,
				random: *random,
				spill: super::plan::Spill::of_sort(Some(keep)),
			}),
			limit: Some(limit.clone()),
			start: start.clone(),
		})
	}
}

fn install_topk_key(plan: &Plan, key: super::plan::TopKKey) -> Plan {
	match plan {
		Plan::TableScan {
			table,
			direction,
			schema,
			topk: _,
		} => Plan::TableScan {
			table: table.clone(),
			direction: *direction,
			schema: schema.clone(),
			topk: Some(key),
		},
		Plan::Filter {
			predicate,
			input,
			once,
		} => Plan::Filter {
			predicate: predicate.clone(),
			input: Box::new(install_topk_key(input, key)),
			once: once.clone(),
		},
		other => other.clone(),
	}
}

/// A predicate's conjuncts are ordered by cost class (`vm::conjunct`): within
/// each run of total conjuncts, field comparisons before builtin calls. A
/// conjunct that may raise or has an effect keeps its place, and none moves
/// across it, so a row raises, short-circuits and is kept exactly as before.
struct ConjunctOrder;

impl Rule for ConjunctOrder {
	fn rewrite(&self, plan: &Plan) -> Option<Plan> {
		let Plan::Filter {
			input,
			predicate,
			once,
		} = plan
		else {
			return None;
		};
		Some(Plan::Filter {
			input: input.clone(),
			predicate: super::conjunct::ordered(predicate)?,
			once: once.clone(),
		})
	}
}

/// A conjunct the record lookup beneath a predicate answers is dropped:
/// `id = r` over the point lookup of `r` holds of the one row the lookup can
/// yield. A table's permission, layers and enforcement keep a row's `id`, so
/// they may stand between. A predicate left with nothing to judge is removed.
struct AnsweredByLookup;

impl Rule for AnsweredByLookup {
	fn rewrite(&self, plan: &Plan) -> Option<Plan> {
		let Plan::Filter {
			input,
			predicate,
			once,
		} = plan
		else {
			return None;
		};
		let mut source = &**input;
		let record = loop {
			source = match source {
				Plan::PermissionFilter {
					input,
					..
				}
				| Plan::Layer {
					input,
					..
				}
				| Plan::Enforced {
					input,
					..
				} => input,
				Plan::RecordScan {
					record,
					..
				} => break record,
				_ => return None,
			};
		};
		let left = super::conjunct::without_answered(predicate, &|conjunct| {
			super::conjunct::pins_record(conjunct, record)
		})?;
		Some(match (left, once) {
			(Some(predicate), _) => Plan::Filter {
				input: input.clone(),
				predicate,
				once: once.clone(),
			},
			// The conjuncts judged once still judge the rows.
			(None, Some(_)) => Plan::Filter {
				input: input.clone(),
				predicate: LogicalExpr::Literal(LogicalLiteral::Bool(true)),
				once: once.clone(),
			},
			(None, None) => (**input).clone(),
		})
	}
}

/// A conjunct an exact B-tree scan beneath a predicate answers is dropped:
/// the scan lists exactly the records whose columns satisfy it
/// ([`Plan::IndexScan`]'s `exact`). Only the table's permission may stand
/// between, which keeps a row's fields as stored; a layer or enforcement may
/// give a field another value than the one the index was keyed by. A
/// predicate left with nothing to judge is removed.
struct AnsweredByIndex;

impl Rule for AnsweredByIndex {
	fn rewrite(&self, plan: &Plan) -> Option<Plan> {
		let Plan::Filter {
			input,
			predicate,
			once,
		} = plan
		else {
			return None;
		};
		let mut source = &**input;
		let (index, access) = loop {
			source = match source {
				Plan::PermissionFilter {
					input,
					..
				} => input,
				Plan::IndexScan {
					index,
					access,
					exact: true,
					..
				} => break (index, access),
				_ => return None,
			};
		};
		let left = super::conjunct::without_answered(predicate, &|conjunct| {
			crate::exec::planner::util::strip_index_conditions(conjunct, access, &index.cols)
				.is_none()
		})?;
		Some(match (left, once) {
			(Some(predicate), _) => Plan::Filter {
				input: input.clone(),
				predicate,
				once: once.clone(),
			},
			// The conjuncts judged once still judge the rows.
			(None, Some(_)) => Plan::Filter {
				input: input.clone(),
				predicate: LogicalExpr::Literal(LogicalLiteral::Bool(true)),
				once: once.clone(),
			},
			(None, None) => (**input).clone(),
		})
	}
}

/// Judge once per run of the statement the conjuncts of each predicate and
/// permission in `plan` that read nothing of the row (`once` on
/// [`Plan::Filter`] and [`Plan::PermissionFilter`]), and per row only the
/// rest.
///
/// A conjunct is judged once where it reads nothing of the row
/// ([`row_independent`]) and every conjunct written before it that stays is
/// total ([`super::conjunct::total`]): judged per row, it would answer the
/// same at every row; it is judged at the first row the node judges, before
/// the conjuncts left per row, where the predicate judged per row judges it
/// first or after conjuncts that can neither raise nor refuse anything a
/// caller observes, so the first error raised is the same. A verdict that
/// refuses admits no row, as the predicate refuses every row; one no row
/// reaches judges nothing, as the predicate judges nothing. A conjunct of
/// literals alone is left per row, where judging it costs nothing.
pub(super) fn judge_invariants_once(
	plan: &mut Plan,
	registry: &crate::exec::function::FunctionRegistry,
) {
	match plan {
		Plan::Filter {
			predicate,
			once: once @ None,
			..
		}
		| Plan::PermissionFilter {
			predicate,
			once: once @ None,
			..
		} => {
			if let Some((hoisted, left)) = split_invariants(predicate, registry) {
				*once = Some(hoisted);
				*predicate = left.unwrap_or(LogicalExpr::Literal(LogicalLiteral::Bool(true)));
			}
		}
		_ => {}
	}
	let recurse = |plan: &mut Plan| judge_invariants_once(plan, registry);
	match plan {
		Plan::Join {
			left,
			right,
			lookup,
			..
		} => {
			recurse(left);
			recurse(right);
			if let Some(lookup) = lookup {
				recurse(&mut lookup.right);
			}
		}
		Plan::Union {
			inputs,
		} => inputs.iter_mut().for_each(recurse),
		Plan::Subquery {
			plan,
		} => recurse(&mut plan.root),
		Plan::Probe {
			build,
			..
		} => recurse(build),
		Plan::Count {
			ordinary: Some(ordinary),
			..
		} => recurse(&mut ordinary.root),
		Plan::Write {
			subjects: super::plan::WriteSubjects::Rows(rows),
			..
		} => recurse(rows),
		Plan::Control {
			control,
		} => judge_control(control, registry),
		Plan::Filter {
			input,
			..
		}
		| Plan::PermissionFilter {
			input,
			..
		}
		| Plan::Layer {
			input,
			..
		}
		| Plan::Enforced {
			input,
			..
		}
		| Plan::Project {
			input,
			..
		}
		| Plan::Fetch {
			input,
			..
		}
		| Plan::Split {
			input,
			..
		}
		| Plan::Sort {
			input,
			..
		}
		| Plan::Aggregate {
			input,
			..
		}
		| Plan::Distinct {
			input,
			..
		}
		| Plan::KnnTopK {
			input,
			..
		}
		| Plan::Limit {
			input,
			..
		} => recurse(input),
		_ => {}
	}
}

/// [`judge_invariants_once`] over the statements a block runs.
fn judge_control(
	control: &mut super::plan::Control,
	registry: &crate::exec::function::FunctionRegistry,
) {
	use super::plan::Control;
	match control {
		Control::Seq(controls) => {
			controls.iter_mut().for_each(|control| judge_control(control, registry))
		}
		Control::Let {
			value,
			..
		}
		| Control::Return {
			value,
		}
		| Control::Throw {
			value,
		} => judge_control(value, registry),
		Control::For {
			range,
			first,
			body,
			..
		} => {
			judge_control(range, registry);
			if let Some(first) = first {
				judge_control(first, registry);
			}
			judge_control(body, registry);
		}
		Control::If {
			branches,
			otherwise,
		} => {
			for (condition, then) in branches {
				judge_control(condition, registry);
				judge_control(then, registry);
			}
			if let Some(otherwise) = otherwise {
				judge_control(otherwise, registry);
			}
		}
		Control::Statement {
			plan,
			..
		} => judge_invariants_once(&mut plan.root, registry),
		Control::Sleep {
			..
		}
		| Control::Value(_)
		| Control::Break
		| Control::Continue => {}
	}
}

/// `predicate` split into the conjuncts [`judge_invariants_once`] judges
/// once, conjoined, and the rest, conjoined, in written order; `None` where
/// no conjunct is judged once.
fn split_invariants(
	predicate: &LogicalExpr,
	registry: &crate::exec::function::FunctionRegistry,
) -> Option<(LogicalExpr, Option<LogicalExpr>)> {
	let mut once = Vec::new();
	let mut left = Vec::new();
	// Whether every conjunct left per row so far is total.
	let mut total = true;
	for conjunct in super::conjunct::conjuncts(predicate) {
		let invariant = total && row_independent(conjunct, registry);
		if invariant {
			once.push(conjunct.clone());
		} else {
			total &= super::conjunct::total(conjunct);
			left.push(conjunct.clone());
		}
	}
	let once = super::conjunct::conjoin(once)?;
	Some((once, super::conjunct::conjoin(left)))
}

/// Whether `expr` answers the same for every row of a run of the statement,
/// and is more than a literal: it reads no field, no document, no row
/// parameter, no graph edge, no statement, no closure and no record — a
/// record read derives its `COMPUTED` fields, which may answer afresh per
/// read — generates no record id, calls only builtins, none an index
/// function, and its effects are deterministic. A session value the binding
/// folded into the predicate is a literal of it.
fn row_independent(expr: &LogicalExpr, registry: &crate::exec::function::FunctionRegistry) -> bool {
	use crate::expr::logical::{EffectSet, LogicalRecordIdKey};
	if matches!(expr, LogicalExpr::Literal(_)) {
		return false;
	}
	let refused = expr.any(|node| match node {
		LogicalExpr::Field(_)
		| LogicalExpr::Doc
		| LogicalExpr::Current
		| LogicalExpr::Subquery(_)
		| LogicalExpr::Closure(_)
		| LogicalExpr::Invoke {
			..
		}
		| LogicalExpr::Lookup {
			..
		} => true,
		LogicalExpr::RecordId(record) => matches!(record.key, LogicalRecordIdKey::Generate(_)),
		LogicalExpr::Param(name) => crate::expr::logical::ROW_PARAMS.contains(&name.as_str()),
		LogicalExpr::Call(call) => match &call.receiver {
			crate::expr::Function::Normal(name) => registry.is_index_function(name),
			_ => true,
		},
		_ => false,
	});
	if refused {
		return false;
	}
	let effects = crate::exec::logical::effects_of_body(expr, registry);
	effects.is_deterministic() && !effects.intersects(EffectSet::READS_ROW | EffectSet::READS_DATA)
}

/// Move the conjuncts of each predicate in `plan` directly above a join that
/// read only the join's left row onto its left input, beneath the join.
///
/// A conjunct moves where it is total and reads nothing the pairing holds
/// beyond the left row ([`left_only`]), and only from the run of total
/// conjuncts the predicate starts with, so no conjunct that may raise or has
/// an effect is evaluated for fewer rows. A pairing keeps every field of its
/// left row but the alias, which such a conjunct does not read, so it judges
/// the left row as it judged each pairing of it — and a `LEFT` join's row
/// with `NULL` under the alias — and the join pairs the rows it kept in the
/// same order. A left row the conjunct rejects reaches the join no more,
/// which may leave the right input unread; the rule applies only where
/// reading it raises no error and has no effect ([`total_read`]), so nothing
/// a caller observes changes. A predicate left with nothing to judge is
/// removed.
pub(super) fn push_into_left(plan: &mut Plan, inert: &[crate::val::TableName]) {
	if let Plan::Filter {
		input,
		predicate,
		once: None,
	} = plan
		&& let Plan::Join {
			left,
			right,
			alias,
			..
		} = input.as_mut()
		&& total_read(right, inert)
	{
		// The rows of the joins beneath the left input are fields of its rows.
		let mut nested = Vec::new();
		left_aliases(left, &mut nested);
		let nested: Vec<String> = nested.into_iter().map(str::to_owned).collect();
		let nested: Vec<&str> = nested.iter().map(String::as_str).collect();
		let conjuncts = super::conjunct::conjuncts(predicate);
		let total = conjuncts.iter().take_while(|conjunct| total_paired(conjunct, &nested)).count();
		let (moved, kept): (Vec<&LogicalExpr>, Vec<&LogicalExpr>) =
			conjuncts[..total].iter().partition(|conjunct| left_only(conjunct, alias));
		if !moved.is_empty() {
			let kept: Vec<LogicalExpr> =
				kept.into_iter().chain(conjuncts[total..].iter().copied()).cloned().collect();
			let moved = super::conjunct::conjoin(moved.into_iter().cloned().collect());
			if let Some(moved) = moved {
				let held = std::mem::replace(left.as_mut(), empty_input());
				**left = Plan::Filter {
					input: Box::new(held),
					predicate: moved,
					once: None,
				};
			}
			let join = std::mem::replace(input.as_mut(), empty_input());
			*plan = match super::conjunct::conjoin(kept) {
				Some(predicate) => Plan::Filter {
					input: Box::new(join),
					predicate,
					once: None,
				},
				None => join,
			};
		}
	}
	match plan {
		Plan::Join {
			left,
			right,
			..
		} => {
			push_into_left(left, inert);
			push_into_left(right, inert);
		}
		Plan::Union {
			inputs,
		} => inputs.iter_mut().for_each(|input| push_into_left(input, inert)),
		Plan::Subquery {
			plan,
		} => push_into_left(&mut plan.root, inert),
		Plan::Filter {
			input,
			..
		}
		| Plan::PermissionFilter {
			input,
			..
		}
		| Plan::Layer {
			input,
			..
		}
		| Plan::Enforced {
			input,
			..
		}
		| Plan::Project {
			input,
			..
		}
		| Plan::Fetch {
			input,
			..
		}
		| Plan::Split {
			input,
			..
		}
		| Plan::Sort {
			input,
			..
		}
		| Plan::Aggregate {
			input,
			..
		}
		| Plan::Distinct {
			input,
			..
		}
		| Plan::Limit {
			input,
			..
		} => push_into_left(input, inert),
		_ => {}
	}
}

/// A node held in place of one being moved.
fn empty_input() -> Plan {
	Plan::Union {
		inputs: Vec::new(),
	}
}

/// Whether `conjunct`, judged over a pairing of join `alias`, reads nothing
/// of it but the left row's fields: no field named `alias`, not the row
/// whole, and no parameter naming the row.
fn left_only(conjunct: &LogicalExpr, alias: &str) -> bool {
	!conjunct.any(|node| match node {
		LogicalExpr::Field(name) => name.as_str() == alias,
		LogicalExpr::Doc => true,
		LogicalExpr::Param(name) => crate::expr::logical::ROW_PARAMS.contains(&name.as_str()),
		_ => false,
	})
}

/// Hold the one row of each join's left input in `plan` that yields at most
/// one, and stream its right input past it ([`JoinBuild::Left`]): the build
/// side is the side a unique key constrains, which holds one row where a
/// build table over the right input holds all of them.
///
/// Not applied to a semi join, which hands on the left row itself where its
/// right input holds a match, not a pairing. Applied where the join reads no index for its right
/// input (an index nested loop already reads only what the one row's key reaches), reading
/// the right input raises no error and has no effect ([`total_read`]), and
/// its keys and the residual raise none and read only the rows paired
/// ([`total_paired`]), and where `writes` — asked at the first join the rule
/// would apply to — answers that the statement writes nothing, so no row the
/// right input yields was written by a pairing it was streamed past. A table
/// whose enforcement the right input's rows are put through counts as read
/// totally where it is among `inert`: the tables whose enforcement the
/// catalog shows evaluates nothing ([`enforced_candidates`]). The right input is read only once the
/// left row reached the join, as the build table is, and every pairing is the build table's
/// in its order, so the rows, their order and the errors raised are those of
/// [`JoinBuild::Right`].
pub(super) fn hold_single_left(
	plan: &mut Plan,
	inert: &[crate::val::TableName],
	writes: &mut dyn FnMut() -> bool,
) {
	if let Plan::Join {
		left,
		right,
		keys,
		residual,
		alias,
		kind,
		lookup: None,
		build,
		spill,
		..
	} = plan
		&& *build == JoinBuild::Right
		&& *kind != super::plan::JoinMatch::Semi
		&& at_most_one_row(left)
		&& total_read(right, inert)
	{
		let mut aliases = vec![alias.as_str()];
		left_aliases(left, &mut aliases);
		if keys.iter().all(|(_, key)| total_paired(key, &aliases))
			&& residual.as_ref().is_none_or(|residual| total_paired(residual, &aliases))
			&& !writes()
		{
			*build = JoinBuild::Left;
			*spill = super::plan::Spill::Bounded;
		}
	}
	match plan {
		Plan::Join {
			left,
			right,
			..
		} => {
			hold_single_left(left, inert, writes);
			hold_single_left(right, inert, writes);
		}
		Plan::Union {
			inputs,
		} => inputs.iter_mut().for_each(|input| hold_single_left(input, inert, writes)),
		Plan::Subquery {
			plan,
		} => hold_single_left(&mut plan.root, inert, writes),
		Plan::Filter {
			input,
			..
		}
		| Plan::PermissionFilter {
			input,
			..
		}
		| Plan::Layer {
			input,
			..
		}
		| Plan::Enforced {
			input,
			..
		}
		| Plan::Project {
			input,
			..
		}
		| Plan::Fetch {
			input,
			..
		}
		| Plan::Split {
			input,
			..
		}
		| Plan::Sort {
			input,
			..
		}
		| Plan::Aggregate {
			input,
			..
		}
		| Plan::Distinct {
			input,
			..
		}
		| Plan::Limit {
			input,
			..
		} => hold_single_left(input, inert, writes),
		_ => {}
	}
}

/// The tables whose enforcement the right input of a join in `plan` puts its
/// rows through: those whose catalog the caller reads to tell which of them
/// evaluate nothing, for [`push_into_left`] and [`hold_single_left`].
pub(super) fn enforced_candidates(plan: &Plan, tables: &mut Vec<crate::val::TableName>) {
	match plan {
		Plan::Join {
			left,
			right,
			..
		} => {
			enforced_on_chain(right, tables);
			enforced_candidates(left, tables);
			enforced_candidates(right, tables);
		}
		Plan::Union {
			inputs,
		} => inputs.iter().for_each(|input| enforced_candidates(input, tables)),
		Plan::Subquery {
			plan,
		} => enforced_candidates(&plan.root, tables),
		Plan::Filter {
			input,
			..
		}
		| Plan::PermissionFilter {
			input,
			..
		}
		| Plan::Layer {
			input,
			..
		}
		| Plan::Enforced {
			input,
			..
		}
		| Plan::Project {
			input,
			..
		}
		| Plan::Fetch {
			input,
			..
		}
		| Plan::Split {
			input,
			..
		}
		| Plan::Sort {
			input,
			..
		}
		| Plan::Aggregate {
			input,
			..
		}
		| Plan::Distinct {
			input,
			..
		}
		| Plan::Limit {
			input,
			..
		} => enforced_candidates(input, tables),
		_ => {}
	}
}

/// The tables enforced on the chain of single inputs [`total_read`] walks.
fn enforced_on_chain(plan: &Plan, tables: &mut Vec<crate::val::TableName>) {
	match plan {
		Plan::Enforced {
			input,
			table,
		} => {
			if !tables.contains(table) {
				tables.push(table.clone());
			}
			enforced_on_chain(input, tables);
		}
		Plan::Filter {
			input,
			..
		}
		| Plan::PermissionFilter {
			input,
			..
		}
		| Plan::Project {
			input,
			..
		} => enforced_on_chain(input, tables),
		_ => {}
	}
}

/// Whether `plan` yields at most one row whatever the data: a read of one
/// record by a literal record id, an equality on every column of a `UNIQUE`
/// index with a key that is neither `NONE` nor `NULL` — the index lists such
/// keys of several records — and no array or object, a `GROUP ALL`, a
/// `LIMIT` of at most one, or a statement in `FROM` that yields at most one,
/// beneath nodes that yield at most one row per row they are handed.
fn at_most_one_row(plan: &Plan) -> bool {
	match plan {
		Plan::Limit {
			limit: Some(limit),
			..
		} if literal_count(limit).is_some_and(|limit| limit <= 1) => true,
		Plan::Filter {
			input,
			..
		}
		| Plan::PermissionFilter {
			input,
			..
		}
		| Plan::Layer {
			input,
			..
		}
		| Plan::Enforced {
			input,
			..
		}
		| Plan::Project {
			input,
			..
		}
		| Plan::Fetch {
			input,
			..
		}
		| Plan::Sort {
			input,
			..
		}
		| Plan::Distinct {
			input,
			..
		}
		| Plan::Limit {
			input,
			..
		} => at_most_one_row(input),
		Plan::Aggregate {
			group,
			..
		} => group.is_empty(),
		Plan::Subquery {
			plan,
		} => at_most_one_row(&plan.root),
		Plan::RecordScan {
			record,
			..
		} => super::conjunct::literal_record(record),
		Plan::IndexScan {
			index,
			access,
			..
		} if index.is_unique() => {
			use crate::exec::index::access_path::BTreeAccess;
			let pinned = match access {
				BTreeAccess::Equality(key) => std::slice::from_ref(key),
				BTreeAccess::Compound {
					prefix,
					range: None,
				} => prefix.as_slice(),
				_ => return false,
			};
			pinned.len() == index.cols.len()
				&& pinned.iter().all(|key| {
					!matches!(
						key,
						crate::val::Value::None
							| crate::val::Value::Null
							| crate::val::Value::Array(_)
							| crate::val::Value::Object(_)
					)
				})
		}
		_ => false,
	}
}

/// Whether reading `plan` raises no error and has no effect: a scan of a
/// table, an index or a literal record, beneath predicates and permissions
/// that are total ([`super::conjunct::total`]), the identity projection, and
/// the enforcement of a table among `inert`.
fn total_read(plan: &Plan, inert: &[crate::val::TableName]) -> bool {
	match plan {
		Plan::Filter {
			input,
			predicate,
			once,
		}
		| Plan::PermissionFilter {
			input,
			predicate,
			once,
			..
		} => {
			super::conjunct::total(predicate)
				&& once.as_ref().is_none_or(super::conjunct::total)
				&& total_read(input, inert)
		}
		Plan::Project {
			input,
			projection: super::plan::Projection::All,
		} => total_read(input, inert),
		Plan::Enforced {
			input,
			table,
		} => inert.contains(table) && total_read(input, inert),
		Plan::TableScan {
			..
		}
		| Plan::IndexScan {
			..
		} => true,
		Plan::RecordScan {
			record,
			for_update: false,
			..
		} => super::conjunct::literal_record(record),
		_ => false,
	}
}

/// The aliases a row of join input `plan` holds the rows of the joins
/// beneath it under.
fn left_aliases<'a>(plan: &'a Plan, aliases: &mut Vec<&'a str>) {
	match plan {
		Plan::Join {
			left,
			alias,
			..
		} => {
			aliases.push(alias);
			left_aliases(left, aliases);
		}
		Plan::Filter {
			input,
			..
		}
		| Plan::Project {
			input,
			projection: super::plan::Projection::All,
		} => left_aliases(input, aliases),
		_ => {}
	}
}

/// Whether evaluating `expr` over a pairing raises no error, has no effect
/// and reads nothing but the pairing's fields, the fields of the rows it
/// holds under `aliases`, parameters and literals: what
/// [`super::conjunct::total`] admits, a field of a row under an alias, and
/// an `IF` whose conditions and values are such.
fn total_paired(expr: &LogicalExpr, aliases: &[&str]) -> bool {
	match expr {
		LogicalExpr::Access {
			base,
			..
		} => matches!(base.as_ref(), LogicalExpr::Field(name) if aliases.contains(&name.as_str())),
		LogicalExpr::Binary {
			left,
			op,
			right,
		} => {
			super::conjunct::total_operator(op)
				&& total_paired(left, aliases)
				&& total_paired(right, aliases)
		}
		LogicalExpr::Prefix {
			op: crate::expr::operator::PrefixOperator::Not,
			expr,
		} => total_paired(expr, aliases),
		LogicalExpr::If(choice) => {
			choice.branches.iter().all(|(condition, value)| {
				total_paired(condition, aliases) && total_paired(value, aliases)
			}) && choice.otherwise.as_ref().is_none_or(|value| total_paired(value, aliases))
		}
		other => super::conjunct::total(other),
	}
}

/// The count a paging clause denotes when it is a non-negative integer literal.
fn literal_count(expr: &LogicalExpr) -> Option<usize> {
	match expr {
		LogicalExpr::Literal(LogicalLiteral::Integer(n)) => usize::try_from(*n).ok(),
		_ => None,
	}
}
