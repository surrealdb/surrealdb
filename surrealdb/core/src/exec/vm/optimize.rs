//! Rewrites over the physical plan.
//!
//! Every rule is an equivalence under the laws of
//! `doc/executor/PHYSICAL_PLAN_REQUIREMENTS.md` §1: the rewritten plan
//! delivers the same rows in the same order, raises the same errors and
//! evaluates every expression as often as the plan it replaced. Rules run
//! bottom-up, repeatedly, until none applies.

use super::plan::{IndexScope, JoinKey, NestedFacts, PhysicalPlan, Plan, SortKeyRead, Strategy};
use crate::exec::field_path::FieldPath;
use crate::expr::logical::{LogicalExpr, LogicalLiteral};

/// A rewrite of one node, given its already-rewritten children.
trait Rule {
	/// The rewritten node, or `None` when the rule does not apply.
	fn rewrite(&self, plan: &Plan) -> Option<Plan>;
}

const RULES: &[&dyn Rule] = &[&DeliveredOrder, &BoundSort];

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
		} => {
			let (input, changed) = rewrite(input);
			(
				Plan::Filter {
					input,
					predicate,
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
		} => {
			let (input, changed) = rewrite(input);
			(
				Plan::Sort {
					input,
					keys,
					keep,
					pushdown,
					random,
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
		} => {
			let (input, changed) = rewrite(input);
			(
				Plan::Aggregate {
					input,
					group,
					fields,
					first_value,
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
		} => {
			let (input, changed) = rewrite(input);
			(
				Plan::Distinct {
					input,
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
		} => {
			let (left, left_changed) = rewrite(left);
			let (right, right_changed) = rewrite(right);
			(
				Plan::Join {
					left,
					right,
					kind,
					keys,
					residual,
					alias,
				},
				left_changed || right_changed,
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
		} => Plan::Filter {
			predicate: predicate.clone(),
			input: Box::new(install_topk_key(input, key)),
		},
		other => other.clone(),
	}
}

/// The count a paging clause denotes when it is a non-negative integer literal.
fn literal_count(expr: &LogicalExpr) -> Option<usize> {
	match expr {
		LogicalExpr::Literal(LogicalLiteral::Integer(n)) => usize::try_from(*n).ok(),
		_ => None,
	}
}
