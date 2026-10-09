//! Source and lookup planning for the planner.
//!
//! Handles graph/reference lookups, index functions, range bounds, and order conversion.

use std::ops::Bound;
use std::sync::Arc;

use surrealdb_strand::TableName;

use super::Planner;
use super::util::extract_table_from_matches;
use crate::err::Error;
use crate::exec::Error as ExecError;
use crate::exec::operators::EdgeTableSpec;
use crate::exec::parts::LookupDirection;
use crate::expr::Dir;
use crate::expr::logical::{LogicalExpr, LogicalLiteral, LogicalLookup, LogicalLookupSubject};
use crate::expr::lookup::LookupKind;

/// Planned representation of a `->edge->vertex` fast-path collapse.
pub(crate) struct TargetVertexPlan {
	pub direction: LookupDirection,
	pub edge_tables: Vec<EdgeTableSpec>,
	pub target_tables: Vec<TableName>,
}

// ============================================================================
// impl Planner — Source Planning
// ============================================================================

impl<'ctx> Planner<'ctx> {
	/// Resolve the index context declared by a function without compiling argument evaluators.
	pub(super) fn index_function_context(
		&self,
		name: &str,
		args: &mut Vec<LogicalExpr>,
	) -> Result<crate::exec::function::IndexContext, Error> {
		use crate::exec::function::{IndexContext, IndexContextKind};
		let registry = self.function_registry();
		let func = registry.get_index_function(name).ok_or_else(|| ExecError::Query {
			message: format!("Index function '{}' not found in registry", name),
		})?;

		// Resolve the appropriate index context based on the function's declared kind
		let index_ctx = match func.index_context_kind() {
			IndexContextKind::FullText => {
				// FullText functions must declare which argument is the index ref
				let ref_idx = func.index_ref_arg_index().ok_or_else(|| ExecError::Query {
					message: format!(
						"Index function '{}': FullText functions must declare an index_ref_arg_index",
						name
					),
				})?;

				if ref_idx >= args.len() {
					return Err(ExecError::Query {
						message: format!(
							"Index function '{}' requires at least {} arguments",
							name,
							ref_idx + 1
						),
					}
					.into());
				}

				// Extract the match_ref argument at plan time (not passed at runtime)
				let match_ref = match args.remove(ref_idx) {
					LogicalExpr::Literal(LogicalLiteral::Integer(n)) if (0..=255).contains(&n) => {
						n as u8
					}
					LogicalExpr::Literal(LogicalLiteral::Float(n))
						if (0.0..=255.0).contains(&n) && n.fract() == 0.0 =>
					{
						n as u8
					}
					_ => {
						return Err(ExecError::Query {
							message: format!(
								"Index function '{}': index_ref argument must be a literal integer in range 0..255",
								name
							),
						}
						.into());
					}
				};

				// Resolve the MatchContext from the MATCHES context
				let matches_ctx =
					self.matches_context.as_ref().ok_or_else(|| ExecError::Query {
						message: format!(
							"Index function '{}': no MATCHES clause found in WHERE condition",
							name
						),
					})?;

				let match_ctx = matches_ctx
					.resolve(match_ref, extract_table_from_matches(matches_ctx))
					.map_err(|e| ExecError::Query {
						message: format!("Index function '{}': {}", name, e),
					})?;

				IndexContext::FullText(match_ctx)
			}
			IndexContextKind::Knn => {
				// KNN functions: retrieve the KNN context from the planning context.
				// If there's a ref argument, extract it (currently unused for single-KNN queries).
				if let Some(ref_idx) = func.index_ref_arg_index()
					&& ref_idx < args.len()
				{
					args.remove(ref_idx);
				}

				let knn_ctx = self.knn_context.as_ref().ok_or_else(|| ExecError::Query {
					message: format!(
						"Index function '{}': no KNN operator found in WHERE condition",
						name
					),
				})?;
				IndexContext::Knn(Arc::clone(knn_ctx))
			}
		};

		Ok(index_ctx)
	}

	/// [`Self::graph_predicate_payload_spec`], with the definition and the
	/// fields of each table `what` names as `defs` answers them: `None` for a
	/// table it holds no definition of.
	pub(crate) fn graph_predicate_payload_spec_from(
		&self,
		cond: &LogicalExpr,
		pred: &crate::exec::clause::ScalarMetadata,
		what: &[LogicalLookupSubject],
		direction: LookupDirection,
		defs: &dyn Fn(&TableName) -> Option<TableDefs>,
	) -> Option<crate::exec::operators::scan::PayloadPredicateSpec> {
		use crate::exec::operators::scan::{PayloadPredicateSpec, PayloadTableSpec};

		let (Some(_), Some(ns), Some(db)) = (&self.txn, &self.ns, &self.db) else {
			return None;
		};
		if self.version.is_some() || what.is_empty() {
			return None;
		}
		if pred.access_mode() != crate::exec::AccessMode::ReadOnly {
			return None;
		}
		let roots = static_field_roots(cond, self.function_registry())?;
		let needs_id = roots.iter().any(|f| f == "id");
		let needs_in = roots.iter().any(|f| f == "in");
		let needs_out = roots.iter().any(|f| f == "out");
		let needed: Vec<&String> =
			roots.iter().filter(|f| !matches!(f.as_str(), "id" | "in" | "out")).collect();
		if needed.is_empty() {
			// A pure id/in/out predicate is key-resident: every field it
			// reads is carried by the adjacency entry itself, so it prunes
			// before any fetch, on any table, with no inline payload, no
			// per-table snapshot and no permission precondition — an edge it
			// drops would have been dropped by the same predicate after the
			// fetch, and every survivor still resolves through the
			// permission-checked record path.
			// A comparison-shaped conjunction additionally compiles to a
			// synchronous matcher the scan runs in place of the evaluator.
			let matcher = crate::exec::operators::scan::key_matcher::KeyPredicateMatcher::compile(
				cond, direction,
			)
			.map(Arc::new);
			return Some(PayloadPredicateSpec {
				tables: std::collections::HashMap::new(),
				needs_id,
				needs_in,
				needs_out,
				key_only: true,
				matcher,
			});
		}
		// When the actor's runtime permission checks are provably skipped,
		// the permission gates below don't apply; otherwise every involved
		// permission must be `Full`, because payload evaluation reads raw
		// stored values.
		let perms_skipped = !self.should_check_perms_for_view(ns, db);
		let mut tables = std::collections::HashMap::new();
		for subject in what {
			let table = match subject {
				LogicalLookupSubject::Table {
					table,
					..
				}
				| LogicalLookupSubject::Range {
					table,
					..
				} => table,
			};
			let (def, fields) = defs(table)?;
			if !perms_skipped && !matches!(def.permissions.select, crate::catalog::Permission::Full)
			{
				return None;
			}
			match &def.table_type {
				crate::catalog::TableType::Relation(rel) if !rel.lightweight => {}
				_ => return None,
			}
			let inline = crate::doc::inline_fields(&fields);
			let names: Vec<String> =
				inline.iter().map(|f| crate::doc::inline_field_name(f)).collect();
			for field in &needed {
				names.iter().position(|n| &n == field)?;
			}
			// Payload evaluation reads the referenced fields' raw stored
			// values — including everything nested inside them — so every
			// field definition rooted at a referenced field must be
			// SELECT-Full: a `meta.secret` clause under an inline `meta`
			// would otherwise let hidden sub-values steer the results.
			if !perms_skipped {
				for fd in fields.iter() {
					let Some(crate::expr::Part::Field(root)) = fd.name.0.first() else {
						continue;
					};
					if needed.iter().any(|n| n.as_str() == root.as_str())
						&& !matches!(fd.select_permission, crate::catalog::Permission::Full)
					{
						return None;
					}
				}
			}
			let referenced = names.iter().map(|n| needed.contains(&n)).collect::<Vec<_>>();
			tables.insert(
				def.name.clone(),
				PayloadTableSpec {
					generation: def.graph_inline_gen,
					fields: names,
					referenced,
				},
			);
		}
		Some(PayloadPredicateSpec {
			tables,
			needs_id,
			needs_in,
			needs_out,
			key_only: false,
			matcher: None,
		})
	}

	/// The collapse [`Self::try_fast_path_pair`] serves the pair with, where
	/// its shape admits one, before the edge tables' permissions are
	/// checked: the caller admits it only where each edge table's `SELECT`
	/// permission is `Full` or the session's reads go unchecked.
	pub(crate) fn fast_path_pair_shape(
		&self,
		edge: &LogicalLookup,
		vertex: &LogicalLookup,
	) -> Option<TargetVertexPlan> {
		// Both hops must be graph traversal in the same direction. The
		// `d1 == d2` guard means the vertex-hop direction is exactly
		// `edge_dir`, so we only bind the one we use.
		let edge_dir = match (&edge.kind, &vertex.kind) {
			(LookupKind::Graph(d1), LookupKind::Graph(d2)) if d1 == d2 => d1,
			_ => return None,
		};
		// SECURITY: time-travel queries (`VERSION <ts>`) must enforce the
		// edge table's SELECT permissions as they stood at the requested
		// version, not the current catalog. The eligibility check below
		// inspects the *current* `Permission::Full` state, so a query that
		// targets a historical version where the edge had a row-level WHERE
		// could be admitted to the fast path -- which then skips the edge
		// record entirely and bypasses that WHERE. Always take the slow
		// path when a VERSION expression is present so the historical
		// permission gate is honoured, whether the planner holds the
		// expression or only the stamp it denotes.
		if self.version.is_some()
			|| !matches!(self.plan_time_version, super::PlanTimeVersion::Current)
		{
			return None;
		}
		// Bidirectional (`<->`) traversal has the slow path scan the edge's
		// own adjacency in *both* directions on the second hop, so each
		// edge contributes both endpoint vertices to the result -- the
		// embedded target plus the originating source vertex. The fast
		// path emits only the embedded target, dropping the source-side
		// duplicate. Force the slow path on `<->` to preserve semantics;
		// `->` and `<-` keep the optimisation.
		if matches!(edge_dir, Dir::Both) {
			return None;
		}

		// Neither hop may carry edge-side filtering, projection, or other
		// clauses that depend on reading the edge / vertex record.
		let lookup_is_plain = |l: &LogicalLookup| {
			l.expr.is_none()
				&& l.cond.is_none()
				&& l.split.is_none()
				&& l.group.is_none()
				&& l.order.is_none()
				&& l.limit.is_none()
				&& l.start.is_none()
				&& l.alias.is_none()
				&& !l.only
		};
		if !lookup_is_plain(edge) || !lookup_is_plain(vertex) {
			return None;
		}

		// Only unbounded `Table` subjects are supported on both hops. Range
		// subjects on the edge would change the bound semantics (and would
		// need the trailing-target-aware bound construction); they stay on
		// the slow path.
		let mut edge_tables: Vec<EdgeTableSpec> = Vec::with_capacity(edge.what.len());
		for s in &edge.what {
			let LogicalLookupSubject::Table {
				table,
				..
			} = s
			else {
				return None;
			};
			edge_tables.push(EdgeTableSpec {
				table: table.clone(),
				range_start: Bound::Unbounded,
				range_end: Bound::Unbounded,
			});
		}
		// The edge `what` list must be non-empty so we can resolve each
		// edge table's permissions. The unbounded "any edge table" case
		// stays on the slow path because we don't know which tables to
		// permission-check at plan time.
		if edge_tables.is_empty() {
			return None;
		}

		let mut target_tables: Vec<TableName> = Vec::with_capacity(vertex.what.len());
		for s in &vertex.what {
			let LogicalLookupSubject::Table {
				table,
				..
			} = s
			else {
				return None;
			};
			target_tables.push(table.clone());
		}
		// Mirror the edge `what` check above: the unbounded "any vertex
		// table" case (empty `what`) stays on the slow path. The scan
		// operator would otherwise accept every embedded target without a
		// table filter, leaving the per-target permission check as the
		// only gate; keep the eligibility contract symmetric so plan-time
		// reasoning matches the edge side.
		if target_tables.is_empty() {
			return None;
		}

		Some(TargetVertexPlan {
			direction: LookupDirection::from(edge_dir),
			edge_tables,
			target_tables,
		})
	}
}

/// A table's definition and its fields.
pub(crate) type TableDefs =
	(Arc<crate::catalog::TableDefinition>, Arc<[crate::catalog::FieldDefinition]>);

/// The root field names `expr` reads off the row, sorted, or `None` when
/// some construct in it may read the row other than by a static field name:
/// a parameter, a row parameter or the document, a nested plan, a lookup, a
/// recursion, a closure or method call, a projection function, a path part
/// evaluated per element, or a dynamic index. Such an expression may reach
/// any field, so the set cannot be known.
fn static_field_roots(
	expr: &LogicalExpr,
	registry: &crate::exec::function::FunctionRegistry,
) -> Option<Vec<String>> {
	use std::collections::BTreeSet;
	use std::ops::ControlFlow;

	use crate::expr::Function;

	let mut roots = BTreeSet::new();
	let complete = expr
		.visit(&mut |e| {
			let known = match e {
				LogicalExpr::Field(name) => {
					roots.insert(name.to_string());
					true
				}
				LogicalExpr::Literal(_)
				| LogicalExpr::Array(_)
				| LogicalExpr::Set(_)
				| LogicalExpr::Object(_)
				| LogicalExpr::RecordId(_)
				| LogicalExpr::Table(_)
				| LogicalExpr::Mock(_)
				| LogicalExpr::Constant(_)
				| LogicalExpr::Access {
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
				| LogicalExpr::If(_) => true,
				// The index is evaluated against the row, so a literal one is
				// as static as a field name; any other may read anything.
				LogicalExpr::Index {
					index,
					..
				} => matches!(index.as_ref(), LogicalExpr::Literal(_)),
				LogicalExpr::Call(call) => match &call.receiver {
					Function::Normal(name) => !registry.is_projection(name),
					Function::Custom(_)
					| Function::Script(_)
					| Function::Model(_)
					| Function::Module(..)
					| Function::Silo {
						..
					} => false,
				},
				LogicalExpr::Param(_)
				| LogicalExpr::Current
				| LogicalExpr::Doc
				| LogicalExpr::Subquery(_)
				| LogicalExpr::Lookup {
					..
				}
				| LogicalExpr::Recurse(_)
				| LogicalExpr::RepeatRecurse(_)
				| LogicalExpr::Closure(_)
				| LogicalExpr::Method {
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
				| LogicalExpr::Destructure {
					..
				}
				| LogicalExpr::Optional {
					..
				}
				| LogicalExpr::Broadcast {
					..
				}
				| LogicalExpr::SetField {
					..
				}
				| LogicalExpr::Throw(_)
				| LogicalExpr::Break
				| LogicalExpr::Continue => false,
			};
			if known {
				ControlFlow::Continue(())
			} else {
				ControlFlow::Break(())
			}
		})
		.is_continue();
	complete.then(|| roots.into_iter().collect())
}
