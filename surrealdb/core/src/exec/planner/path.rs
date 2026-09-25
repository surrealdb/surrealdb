//! Path compilation for the planner.
//!
//! A logical path — a nest of step forms over a root — compiles to one
//! [`Step`] holding the part evaluators in application order, a
//! [`Broadcast`] where the logical plan placed one, or a [`Current`] for the
//! bare current value. The steps of a chain are compiled iteratively, so a
//! long idiom does not recurse once per step; recursion happens only where
//! the logical form nests an expression (a body, an index, a predicate, an
//! argument).
//!
//! Consecutive lookup hops are fused into one [`LookupPart`] whose plan is a
//! single operator chain, with a fast-path-eligible `->edge->vertex` pair
//! collapsed into one scan.
//!
//! A chain of plain field accesses over a constant root (`$session.data.orgs`,
//! `{ a: { b: 1 } }.a.b`) compiles to a [`FoldedPath`] holding the value it
//! reads, so the value is picked once rather than on every row. A bound plan
//! has had its parameters substituted and such chains folded by the logical
//! fold pass before it reaches the planner, so this reaches the scalars
//! compiled outside binding: a stored permission body that a runtime-enforcing
//! scan compiles directly, and the runtime re-entry through `compile_expr`.
//! Access paths are chosen on the logical plan before any scalar is compiled,
//! so nothing folded here changes one; what it removes is the per-row clone of
//! the whole `$session` value.

use std::sync::Arc;

use surrealdb_strand::Strand;
use surrealdb_types::ToSql;

use super::Planner;
use crate::err::{EngineError, Error};
use crate::exec::operators::{CurrentValueSource, RecursionOp};
use crate::exec::parts::{
	AllPart, ClosureFieldCallPart, DestructureField, DestructurePart, DocPart, FieldPart,
	FirstPart, FlattenPart, IndexPart, LastPart, LookupDirection, LookupPart, MethodPart,
	OptionalChainPart, PhysicalRecurseInstruction, RecursePart, RepeatRecursePart, WherePart,
};
use crate::exec::physical_expr::{Broadcast, Current, FoldedPath, Step};
use crate::exec::{Error as ExecError, ExecOperator, PhysicalExpr};
use crate::expr::logical::{
	LogicalDestructureField, LogicalExpr, LogicalLookup, LogicalRecurse, LogicalRecurseInstruction,
	LogicalRecursePath,
};
use crate::expr::lookup::LookupKind;
use crate::expr::part::Recurse;
use crate::val::Value;

// ============================================================================
// impl Planner — Path Compilation
// ============================================================================

impl<'ctx> Planner<'ctx> {
	/// Compile a path form of [`LogicalExpr`] into a physical expression.
	///
	/// Every path form is accepted; an expression that is not a path is
	/// compiled as the root of an empty chain, which is itself. Callers
	/// normally reach this through [`Self::physical_expr`], which dispatches
	/// the path forms here.
	pub(crate) fn physical_path(
		&self,
		expr: LogicalExpr,
	) -> crate::exec::BoxFut<'_, Result<Arc<dyn PhysicalExpr>, Error>> {
		Box::pin(async move {
			let display = expr.to_sql();

			if let Some((root, fields)) = constant_access_chain(&expr)
				&& let Some(root) = self.constant_root_value(root)
				&& let Some(value) = fold_constant_field_chain(&root, &fields)
			{
				let folded: Arc<dyn PhysicalExpr> = Arc::new(FoldedPath::new(display, value));
				return Ok(folded);
			}

			if let LogicalExpr::Broadcast {
				base,
				body,
				flatten,
			} = expr
			{
				let base = self.physical_expr(*base).await?;
				let body = self.physical_expr(*body).await?;
				let broadcast: Arc<dyn PhysicalExpr> =
					Arc::new(Broadcast::new(display, base, body, flatten));
				return Ok(broadcast);
			}

			// Peel the chain from its outermost step down to its root,
			// compiling each part as it is reached. The parts come out in
			// reverse application order.
			let mut parts_rev: Vec<Arc<dyn PhysicalExpr>> = Vec::new();
			let mut current = expr;
			let root: Option<Arc<dyn PhysicalExpr>> = loop {
				current = match current {
					LogicalExpr::Field(name) => {
						parts_rev.push(Arc::new(FieldPart {
							name: name.into_string(),
						}));
						break None;
					}
					LogicalExpr::Doc => {
						parts_rev.push(Arc::new(DocPart));
						break None;
					}
					LogicalExpr::Current => break None,
					LogicalExpr::Access {
						base,
						field,
					} => {
						parts_rev.push(Arc::new(FieldPart {
							name: field.into_string(),
						}));
						*base
					}
					LogicalExpr::Index {
						base,
						index,
					} => {
						let expr = self.physical_expr(*index).await?;
						parts_rev.push(Arc::new(IndexPart {
							expr,
						}));
						*base
					}
					LogicalExpr::First(base) => {
						parts_rev.push(Arc::new(FirstPart));
						*base
					}
					LogicalExpr::Last(base) => {
						parts_rev.push(Arc::new(LastPart));
						*base
					}
					LogicalExpr::Each(base) => {
						parts_rev.push(Arc::new(AllPart));
						*base
					}
					LogicalExpr::Flatten {
						base,
						..
					} => {
						parts_rev.push(Arc::new(FlattenPart));
						*base
					}
					LogicalExpr::Filter {
						base,
						predicate,
					} => {
						let needs_parent = super::row_scope::references_parent(&predicate);
						let predicate = self.physical_expr(*predicate).await?;
						parts_rev.push(Arc::new(WherePart {
							predicate,
							needs_parent,
						}));
						*base
					}
					LogicalExpr::Method {
						base,
						name,
						args,
					} => {
						parts_rev.push(self.physical_method(name, args).await?);
						*base
					}
					LogicalExpr::Destructure {
						base,
						fields,
					} => {
						let fields = self.physical_destructure(fields).await?;
						parts_rev.push(Arc::new(DestructurePart {
							fields,
						}));
						*base
					}
					LogicalExpr::Optional {
						base,
						then,
					} => {
						let then = self.physical_expr(*then).await?;
						parts_rev.push(Arc::new(OptionalChainPart {
							then,
						}));
						*base
					}
					LogicalExpr::Lookup {
						base,
						lookup,
					} => {
						// Collect the run of consecutive hops, innermost first.
						let mut run = vec![*lookup];
						let mut inner = *base;
						while let LogicalExpr::Lookup {
							base,
							lookup,
						} = inner
						{
							run.push(*lookup);
							inner = *base;
						}
						run.reverse();
						parts_rev.push(self.physical_lookup_run(run).await?);
						inner
					}
					LogicalExpr::Recurse(recurse) => {
						let LogicalRecurse {
							base,
							recurse,
							path,
							instruction,
						} = *recurse;
						parts_rev.push(self.physical_recurse(recurse, path, instruction).await?);
						base
					}
					LogicalExpr::RepeatRecurse(base) => {
						parts_rev.push(Arc::new(RepeatRecursePart));
						*base
					}
					// A broadcast reached beneath a step is the expression a
					// `(e).rest` path started from; it compiles as its own
					// node, as does any other starting expression.
					root @ (LogicalExpr::Broadcast {
						..
					}
					| LogicalExpr::Literal(_)
					| LogicalExpr::Array(_)
					| LogicalExpr::Set(_)
					| LogicalExpr::Object(_)
					| LogicalExpr::RecordId(_)
					| LogicalExpr::Param(_)
					| LogicalExpr::Table(_)
					| LogicalExpr::Constant(_)
					| LogicalExpr::Prefix {
						..
					}
					| LogicalExpr::SetField {
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
					| LogicalExpr::Subquery(_)) => break Some(self.physical_expr(root).await?),
				};
			};

			if parts_rev.is_empty() {
				return Ok(match root {
					Some(root) => root,
					None => Arc::new(Current),
				});
			}
			parts_rev.reverse();
			Ok(Arc::new(Step::new(display, root, parts_rev)))
		})
	}

	/// Compile a method step: a registered name dispatches on the receiver's
	/// type; any other name calls a closure stored under it on the receiver.
	async fn physical_method(
		&self,
		name: Strand,
		args: Vec<LogicalExpr>,
	) -> Result<Arc<dyn PhysicalExpr>, Error> {
		let args = self.physical_args(args).await?;
		let registry = self.function_registry();
		match registry.get_method(name.as_str()) {
			Some(descriptor) => Ok(Arc::new(MethodPart {
				descriptor: Arc::clone(descriptor),
				args,
			})),
			None => Ok(Arc::new(ClosureFieldCallPart {
				field: name.into_string(),
				args,
			})),
		}
	}

	/// Compile a run of consecutive lookup hops, innermost first, into one
	/// [`LookupPart`].
	///
	/// The hops thread into a single operator chain — the output of one scan
	/// is the input of the next — rather than each running from its own
	/// `CurrentValueSource`:
	/// `GraphEdgeScan(->person, GraphEdgeScan(->likes, CurrentValueSource))`.
	/// A consecutive `->edge->vertex` pair that is fast-path eligible (no
	/// edge-side filters or projections, edge table with `PERMISSIONS FULL`)
	/// collapses into a single scan in `TargetVertex` mode that reads the
	/// target vertex from the adjacency key; see
	/// [`Planner::try_fast_path_pair`]. The part's metadata is that of the
	/// last hop, so the result shape matches the two-scan chain's.
	async fn physical_lookup_run(
		&self,
		lookups: Vec<LogicalLookup>,
	) -> Result<Arc<dyn PhysicalExpr>, Error> {
		if lookups.is_empty() {
			return Err(EngineError::Internal(
				"physical_lookup_run: a lookup run has at least one hop".into(),
			)
			.into());
		}
		let only = lookups.iter().any(|l| l.only);

		let mut chain: Arc<dyn ExecOperator> = Arc::new(CurrentValueSource::new());
		let mut direction = LookupDirection::Out;
		let mut extract_id = false;

		let mut idx = 0;
		while idx < lookups.len() {
			let advance = if idx + 1 < lookups.len() {
				self.try_fast_path_pair(&lookups[idx], &lookups[idx + 1]).await?
			} else {
				None
			};
			if let Some(plan) = advance {
				chain = self.plan_target_vertex_scan(chain, plan).await?;
				let (d, e) = lookup_metadata(&lookups[idx + 1]);
				direction = d;
				extract_id = e;
				idx += 2;
			} else {
				let lu = lookups[idx].clone();
				let (d, e) = lookup_metadata(&lu);
				direction = d;
				extract_id = e;
				chain = self.plan_lookup_with_input(chain, lu).await?;
				idx += 1;
			}
		}

		Ok(Arc::new(LookupPart {
			direction,
			plan: chain,
			extract_id,
			only,
		}))
	}

	/// Compile a recursion step.
	///
	/// The body is one physical expression over the current value, held by
	/// the [`RecursionOp`] as its path. A trailing body (`.{n}.rest`) that
	/// carries both an instruction and a `.@` is rejected: the instruction
	/// decides what the recursion collects, and `.@` decides where it
	/// re-enters, and the two cannot be honoured together.
	async fn physical_recurse(
		&self,
		recurse: Recurse,
		path: Option<LogicalRecursePath>,
		instruction: Option<LogicalRecurseInstruction>,
	) -> Result<Arc<dyn PhysicalExpr>, Error> {
		let (min_depth, max_depth) = self.recurse_bounds(&recurse)?;

		let (path, has_repeat_recurse) = match path {
			Some(path) => {
				let has_repeat_recurse = contains_repeat_recurse(path.body());
				if instruction.is_some()
					&& has_repeat_recurse
					&& matches!(path, LogicalRecursePath::Trailing(_))
				{
					return Err(Error::Exec(ExecError::RecursionInstructionPlanConflict));
				}
				let body = self.physical_expr(recurse_body(path)).await?;
				(vec![body], has_repeat_recurse)
			}
			None => (vec![], false),
		};

		let inclusive = is_inclusive_recurse(&instruction);
		let instr = self.physical_recurse_instruction(instruction).await?;

		let body_op = extract_body_operator(&path);
		let op: Arc<dyn ExecOperator> = Arc::new(RecursionOp::new(
			body_op,
			path,
			min_depth,
			max_depth,
			instr,
			inclusive,
			has_repeat_recurse,
		));
		Ok(Arc::new(RecursePart {
			op,
		}))
	}

	/// The `(min, max)` depth of a recursion, validated against the
	/// configured idiom recursion limit: the minimum is at least 1 and the
	/// maximum, when given, no more than `idiom_recursion_limit`.
	fn recurse_bounds(&self, recurse: &Recurse) -> Result<(u32, Option<u32>), Error> {
		let system_limit = self.ctx.config.exec.idiom_recursion_limit;
		let (min_depth, max_depth) = match *recurse {
			Recurse::Fixed(n) => (n, Some(n)),
			Recurse::Range(min, max) => (min.unwrap_or(1), max),
		};
		if min_depth < 1 {
			return Err(ExecError::InvalidBound {
				found: min_depth.to_string(),
				expected: "at least 1".into(),
			}
			.into());
		}
		if let Some(max) = max_depth
			&& max > system_limit
		{
			return Err(ExecError::InvalidBound {
				found: max.to_string(),
				expected: format!("{} at most", system_limit),
			}
			.into());
		}
		Ok((min_depth, max_depth))
	}

	/// Compile destructure fields. An alias body is one physical expression
	/// evaluated with the destructured value as the current value.
	pub(crate) fn physical_destructure(
		&self,
		fields: Vec<LogicalDestructureField>,
	) -> crate::exec::BoxFut<'_, Result<Vec<DestructureField>, Error>> {
		Box::pin(async move {
			let mut out = Vec::with_capacity(fields.len());
			for field in fields {
				let field = match field {
					LogicalDestructureField::All(name) => DestructureField::All(name),
					LogicalDestructureField::Field(name) => DestructureField::Field(name),
					LogicalDestructureField::Aliased(name, body) => DestructureField::Aliased {
						field: name,
						body: self.physical_expr(body).await?,
					},
					LogicalDestructureField::Destructure(name, nested) => {
						DestructureField::Nested {
							field: name,
							parts: self.physical_destructure(nested).await?,
						}
					}
					LogicalDestructureField::Rest => DestructureField::Rest,
				};
				out.push(field);
			}
			Ok(out)
		})
	}

	/// Compile a [`LogicalRecurseInstruction`] to a [`PhysicalRecurseInstruction`].
	pub(crate) async fn physical_recurse_instruction(
		&self,
		instruction: Option<LogicalRecurseInstruction>,
	) -> Result<PhysicalRecurseInstruction, Error> {
		match instruction {
			None => Ok(PhysicalRecurseInstruction::Default),
			Some(LogicalRecurseInstruction::Collect {
				..
			}) => Ok(PhysicalRecurseInstruction::Collect),
			Some(LogicalRecurseInstruction::Path {
				..
			}) => Ok(PhysicalRecurseInstruction::Path),
			Some(LogicalRecurseInstruction::Shortest {
				expects,
				..
			}) => {
				let target = self.physical_expr(expects).await?;
				Ok(PhysicalRecurseInstruction::Shortest {
					target,
				})
			}
		}
	}
}

// ============================================================================
// Free Functions
// ============================================================================

/// The body of a recursion path, however it was written.
fn recurse_body(path: LogicalRecursePath) -> LogicalExpr {
	match path {
		LogicalRecursePath::Nested(body) | LogicalRecursePath::Trailing(body) => body,
	}
}

/// The direction and `extract_id` metadata of a lookup.
///
/// `extract_id` is set when the scan reads whole edge records for a `WHERE`
/// or `SPLIT` but no projection is asked for, so the result is projected back
/// to record ids.
fn lookup_metadata(lookup: &LogicalLookup) -> (LookupDirection, bool) {
	let direction = match &lookup.kind {
		LookupKind::Graph(dir) => LookupDirection::from(dir),
		LookupKind::Reference => LookupDirection::Reference,
	};
	let needs_full_pipeline = lookup.expr.is_some() || lookup.group.is_some();
	let needs_full_records = needs_full_pipeline || lookup.cond.is_some() || lookup.split.is_some();
	let extract_id = needs_full_records && !needs_full_pipeline;
	(direction, extract_id)
}

/// Whether a recurse instruction has the `inclusive` flag set.
fn is_inclusive_recurse(instruction: &Option<LogicalRecurseInstruction>) -> bool {
	matches!(
		instruction,
		Some(LogicalRecurseInstruction::Path {
			inclusive: true,
			..
		}) | Some(LogicalRecurseInstruction::Collect {
			inclusive: true,
			..
		}) | Some(LogicalRecurseInstruction::Shortest {
			inclusive: true,
			..
		})
	)
}

/// Extract operator chain from body parts for EXPLAIN display.
///
/// Collects embedded operators from all path parts (LookupParts expose
/// their fused chains). Returns the chain if exactly one is found.
fn extract_body_operator(path: &[Arc<dyn PhysicalExpr>]) -> Option<Arc<dyn ExecOperator>> {
	let embedded: Vec<_> =
		path.iter().flat_map(|p| p.embedded_operators()).map(|(_, op)| Arc::clone(op)).collect();
	if embedded.len() == 1 {
		Some(embedded.into_iter().next().expect("embedded operator should be present"))
	} else {
		None
	}
}

/// Whether a recursion body re-enters through `.@` anywhere along its path:
/// on the chain of steps itself, inside a broadcast or optional body, inside
/// a destructure alias body, or inside a nested recursion's own body. The
/// expressions a step holds beside its base — an index, a filter predicate,
/// method arguments — are not part of the path and are not searched.
fn contains_repeat_recurse(expr: &LogicalExpr) -> bool {
	fn destructure_contains(fields: &[LogicalDestructureField]) -> bool {
		fields.iter().any(|field| match field {
			LogicalDestructureField::Aliased(_, body) => contains_repeat_recurse(body),
			LogicalDestructureField::Destructure(_, nested) => destructure_contains(nested),
			LogicalDestructureField::All(_)
			| LogicalDestructureField::Field(_)
			| LogicalDestructureField::Rest => false,
		})
	}
	let mut current = expr;
	loop {
		match current {
			LogicalExpr::RepeatRecurse(_) => return true,
			LogicalExpr::Destructure {
				base,
				fields,
			} => {
				if destructure_contains(fields) {
					return true;
				}
				current = base;
			}
			LogicalExpr::Recurse(recurse) => {
				if recurse.path.as_ref().is_some_and(|p| contains_repeat_recurse(p.body())) {
					return true;
				}
				current = &recurse.base;
			}
			LogicalExpr::Optional {
				base,
				then,
			} => {
				if contains_repeat_recurse(then) {
					return true;
				}
				current = base;
			}
			LogicalExpr::Broadcast {
				base,
				body,
				..
			} => {
				if contains_repeat_recurse(body) {
					return true;
				}
				current = base;
			}
			LogicalExpr::Access {
				base,
				..
			}
			| LogicalExpr::Index {
				base,
				..
			}
			| LogicalExpr::Flatten {
				base,
				..
			}
			| LogicalExpr::Filter {
				base,
				..
			}
			| LogicalExpr::Method {
				base,
				..
			}
			| LogicalExpr::Lookup {
				base,
				..
			} => current = base,
			LogicalExpr::First(base) | LogicalExpr::Last(base) | LogicalExpr::Each(base) => {
				current = base
			}
			LogicalExpr::Field(_)
			| LogicalExpr::Doc
			| LogicalExpr::Current
			| LogicalExpr::Literal(_)
			| LogicalExpr::Array(_)
			| LogicalExpr::Set(_)
			| LogicalExpr::Object(_)
			| LogicalExpr::RecordId(_)
			| LogicalExpr::Param(_)
			| LogicalExpr::Table(_)
			| LogicalExpr::Constant(_)
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
			| LogicalExpr::Subquery(_) => return false,
		}
	}
}

impl Planner<'_> {
	/// The constant value a path's root denotes, when it denotes one.
	///
	/// Two shapes qualify, and only two:
	///
	/// * A literal — already a constant in the plan, so reading it needs no environment at all.
	/// * `$session` — read out of the very context this plan will run in, so the value folded in is
	///   the one the evaluator would have read. `LET`, `UNSET`, `db.set` and RPC `set` all refuse
	///   the name (it is in `PROTECTED_PARAM_NAMES`), so no *statement* can rebind it mid-plan, and
	///   the constructs that can — a `FOR` loop parameter, a closure parameter, a user-defined
	///   function argument — are recorded by the planner they compile their body with, so a read
	///   beneath one of them is left to the evaluator
	///   ([`Planner::param_is_shadowed`](super::Planner::param_is_shadowed)).
	///
	/// Every other parameter is left alone. A `DEFINE PARAM` carries permissions
	/// that belong at runtime, and an ordinary binding may be rebound by an
	/// enclosing construct after the plan is built.
	///
	/// SECURITY: `$session` is **not** unshadowable, and nothing here should be
	/// read as claiming it is. `PROTECTED_PARAM_NAMES` is consulted only by the
	/// statement forms listed above; the constructs that bind a user-chosen
	/// parameter name — `FOR $session IN [...]`, closure parameters, user-defined
	/// function arguments — bind it unchecked, and `Context::value` prefers a
	/// local insert over the parent chain. Folding beneath one of them would
	/// read the ambient session where the evaluator reads the binding, so the
	/// planner records those names and declines. The same routes shadow `$auth`,
	/// which every record permission already depends on, so folding `$session`
	/// here neither adds nor removes trust: it inherits exactly `$auth`'s
	/// boundary.
	///
	/// SECURITY: folding bakes one session's values into a physical plan, which
	/// is sound only because a plan belongs to the session it was built for. A
	/// plan cache has to either key on the folded session values or decline to
	/// cache a plan that carries them; sharing one across sessions would hand
	/// the first session's `$session` to the second.
	fn constant_root_value(&self, root: &LogicalExpr) -> Option<Value> {
		match root {
			LogicalExpr::Literal(_) | LogicalExpr::Array(_) | LogicalExpr::Object(_) => {
				super::util::try_literal_to_value(root)
			}
			// Resolved from the ambient planning context. `None` when no
			// session is attached, or when an enclosing `FOR`, closure or
			// user-defined function binds the name, in which case nothing
			// folds.
			LogicalExpr::Param(name)
				if name.as_str() == "session" && !self.param_is_shadowed("session") =>
			{
				self.ctx.value("session").cloned()
			}
			_ => None,
		}
	}
}

/// The root and field names of a path that is a chain of plain field accesses
/// (`root.a.b`), in application order; `None` for a bare root or for a chain
/// holding any other step.
fn constant_access_chain(expr: &LogicalExpr) -> Option<(&LogicalExpr, Vec<&str>)> {
	let mut fields = Vec::new();
	let mut current = expr;
	while let LogicalExpr::Access {
		base,
		field,
	} = current
	{
		fields.push(field.as_str());
		current = base;
	}
	if fields.is_empty() {
		return None;
	}
	fields.reverse();
	Some((current, fields))
}

/// Walk a chain of plain field accesses over constant objects.
///
/// Deliberately narrow: every value stepped through must be a `Value::Object`.
/// Requiring an object at each step is what preserves record-link semantics —
/// `$auth.email` starts at a `Value::RecordId`, whose field access is a *fetch
/// of the stored row at runtime*, so it never folds.
///
/// A missing key folds to `NONE`, which is what the field access evaluates to,
/// but only as the last step: anything after it operates on NONE and has its
/// own behaviour there (`{}.prop.is_bool()` is `false`, not NONE), so the rest
/// is left to the evaluator.
///
/// Not guarded by `substitution_is_lossy`: that predicate exists for the
/// params pass, which substitutes a *literal* into the plan, and `Value::Set`'s
/// literal form is an array literal. Nothing here goes through SQL text — the
/// folded value is handed back verbatim — so a set folds as a set, which is
/// the payload shape a `CONTEXT` clause returning a set produces.
fn fold_constant_field_chain(root: &Value, fields: &[&str]) -> Option<Value> {
	let mut current = root;
	for (i, name) in fields.iter().enumerate() {
		let Value::Object(object) = current else {
			return None;
		};
		match object.get(name) {
			Some(next) => current = next,
			None if i + 1 == fields.len() => return Some(Value::None),
			None => return None,
		}
	}
	Some(current.clone())
}

#[cfg(test)]
mod fold_tests {
	use super::*;
	use crate::val::{Object, RecordId, Value};

	fn object(pairs: Vec<(&str, Value)>) -> Value {
		let mut obj = Object::default();
		for (k, v) in pairs {
			obj.insert(k, v);
		}
		Value::Object(obj)
	}

	fn parse_path(src: &str) -> LogicalExpr {
		crate::exec::operators::test_util::parse_expr(src)
	}

	#[test]
	fn an_access_chain_is_read_root_first() {
		let path = parse_path("$session.data.org");
		let (root, fields) = constant_access_chain(&path).expect("a chain");
		assert!(matches!(root, LogicalExpr::Param(name) if name.as_str() == "session"));
		assert_eq!(fields, vec!["data", "org"]);
		assert!(constant_access_chain(&parse_path("$session")).is_none());
		assert!(constant_access_chain(&parse_path("$session.data[0]")).is_none());
		assert!(constant_access_chain(&parse_path("$session.data.*")).is_none());
	}

	#[test]
	fn folds_a_chain_of_object_fields() {
		let root = object(vec![("data", object(vec![("org", Value::from("acme"))]))]);
		assert_eq!(fold_constant_field_chain(&root, &["data", "org"]), Some(Value::from("acme")));
	}

	#[test]
	fn a_missing_key_folds_to_none_only_as_the_last_step() {
		let root = object(vec![("data", Value::None)]);
		assert_eq!(fold_constant_field_chain(&root, &["absent"]), Some(Value::None));
		assert_eq!(fold_constant_field_chain(&root, &["absent", "deeper"]), None);
	}

	/// The stopping rule that preserves record-link semantics: `$auth.email`
	/// must stay a runtime fetch of the stored row, never a plan-time pick.
	#[test]
	fn refuses_to_step_through_a_record_link() {
		let rid = Value::RecordId(RecordId::new(crate::val::TableName::new("user"), 1));
		let root = object(vec![("rd", rid.clone())]);
		assert_eq!(fold_constant_field_chain(&root, &["rd", "email"]), None);
		assert_eq!(fold_constant_field_chain(&rid, &["email"]), None);
	}

	/// A set folds, and folds as a set: nothing here goes through SQL text.
	#[test]
	fn folds_a_set_without_flattening_it_to_an_array() {
		let mut set = crate::val::Set::new();
		set.insert(Value::from("acme"));
		set.insert(Value::from("globex"));
		let folded =
			fold_constant_field_chain(&object(vec![("orgs", Value::Set(set.clone()))]), &["orgs"]);
		assert_eq!(folded, Some(Value::Set(set.clone())));
		assert!(!matches!(folded, Some(Value::Array(_))), "a set must not fold to an array");

		let root = object(vec![(
			"data",
			object(vec![("orgs", Value::Set(set.clone())), ("name", Value::from("alice"))]),
		)]);
		assert_eq!(fold_constant_field_chain(&root, &["data", "name"]), Some(Value::from("alice")));
		assert_eq!(
			fold_constant_field_chain(&root, &["data"]),
			Some(object(vec![("orgs", Value::Set(set)), ("name", Value::from("alice"))]))
		);
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::operators::test_util::{TestDb, eval, parse_expr, root_ctx, val};

	#[test]
	fn repeat_recurse_is_found_along_the_path_and_in_alias_bodies_only() {
		let body = |src: &str| match parse_expr(src) {
			LogicalExpr::Recurse(recurse) => recurse.path.expect("a body").body().clone(),
			other => panic!("expected a recursion, got {other:?}"),
		};
		assert!(contains_repeat_recurse(&body("a.{..}.b.@")));
		assert!(contains_repeat_recurse(&body("a.{..}.{ id, kids: b.@ }")));
		assert!(contains_repeat_recurse(&body("a.{..}(.b.@)")));
		assert!(!contains_repeat_recurse(&body("a.{..}.b")));
		// An expression a step holds beside its base is not on the path.
		assert!(!contains_repeat_recurse(&body("a.{..}.b[WHERE c = (d.@)]")));
	}

	#[tokio::test]
	async fn an_instruction_and_a_repeat_recurse_in_one_trailing_body_conflict() {
		let ctx = root_ctx();
		let planner = Planner::new(ctx.ctx(), ctx.function_registry());
		let err = planner
			.physical_expr(parse_expr("a.{..+collect}.b.@"))
			.await
			.expect_err("an instruction and `.@` cannot be honoured together");
		assert!(
			matches!(err, Error::Exec(ExecError::RecursionInstructionPlanConflict)),
			"got {err:?}"
		);
	}

	#[tokio::test]
	async fn a_recursion_body_is_one_expression_over_the_value_at_each_depth() {
		let db = TestDb::new(
			"DEFINE TABLE node SCHEMALESS;
			 INSERT [
			   { id: node:a, next: node:b },
			   { id: node:b, next: node:c },
			   { id: node:c }
			 ];",
		)
		.await;
		let ctx = db.exec_ctx().await;
		assert_eq!(eval("node:a.{2}(.next)", &ctx).await.unwrap(), val("node:c").await);
		assert_eq!(eval("node:a.{2}.next", &ctx).await.unwrap(), val("node:c").await);
		assert_eq!(
			eval("node:a.{1}.{ id, next: next.@ }", &ctx).await.unwrap(),
			val("{ id: node:a, next: node:b }").await
		);
	}
}
