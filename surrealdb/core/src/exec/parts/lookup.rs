//! Lookup part -- graph/reference traversal.

use std::sync::Arc;

use futures::StreamExt;
use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::fan_out::evaluate_each;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, BoxFut, ContextLevel, Error as ExecError, ExecOperator};
use crate::expr::FlowResult;
use crate::val::Value;

/// Direction for lookup operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupDirection {
	/// Outgoing edges: `->`
	Out,
	/// Incoming edges: `<-`
	In,
	/// Both directions: `<->`
	Both,
	/// Record references: `<~`
	Reference,
}

impl From<crate::expr::Dir> for LookupDirection {
	fn from(dir: crate::expr::Dir) -> Self {
		match dir {
			crate::expr::Dir::Out => LookupDirection::Out,
			crate::expr::Dir::In => LookupDirection::In,
			crate::expr::Dir::Both => LookupDirection::Both,
		}
	}
}

impl From<&crate::expr::Dir> for LookupDirection {
	fn from(dir: &crate::expr::Dir) -> Self {
		match dir {
			crate::expr::Dir::Out => LookupDirection::Out,
			crate::expr::Dir::In => LookupDirection::In,
			crate::expr::Dir::Both => LookupDirection::Both,
		}
	}
}

/// Graph/reference lookup - `->edge->target`, `<-edge<-source`, `<~table`.
#[derive(Debug, Clone)]
pub struct LookupPart {
	/// The direction of the lookup (In, Out, Both for graph; Reference for <~)
	pub direction: LookupDirection,

	/// The pre-planned operator tree for executing the lookup.
	/// This includes GraphEdgeScan/ReferenceScan + optional Filter, Sort, Limit, Project.
	pub plan: Arc<dyn ExecOperator>,

	/// When true, extract just the RecordId from result objects.
	/// This is set when the scan uses FullEdge mode for WHERE/SPLIT filtering
	/// but no explicit SELECT clause is present, so the final result should be
	/// RecordIds rather than full objects.
	pub extract_id: bool,

	/// Whether this LookupPart contains a fused chain of multiple consecutive lookups.
	/// When true, the continuation logic in `evaluate_parts_with_continuation` maps
	/// per-element over non-lookup arrays even when this is the last part in the idiom.
	pub fused: bool,

	/// When true, unwrap the result array into a single value (FROM ONLY semantics).
	/// Empty results yield NONE; more than one result is an error.
	pub only: bool,
}
impl PhysicalExpr for LookupPart {
	fn name(&self) -> &'static str {
		"Lookup"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		// Lookups need database context, combined with the child plan's context
		self.plan.required_context().max(ContextLevel::Database)
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			let value = ctx.current_value.unwrap_or(&Value::NONE);
			Ok(evaluate_lookup(value, self, ctx).await?)
		})
	}

	/// Batch evaluation for graph/reference lookups.
	///
	/// Each row runs the lookup plan, so the mode is the plan's own: a lookup
	/// whose plan mutates is evaluated one row at a time.
	fn evaluate_batch<'a>(
		&'a self,
		ctx: EvalContext<'a>,
		values: &'a [Value],
	) -> BoxFut<'a, FlowResult<Vec<Value>>> {
		Box::pin(evaluate_each(ctx.exec_ctx, self.access_mode(), values, move |value| {
			self.evaluate(ctx.with_value(value))
		}))
	}

	fn access_mode(&self) -> AccessMode {
		self.plan.access_mode()
	}

	fn embedded_operators(&self) -> Vec<(&str, &Arc<dyn ExecOperator>)> {
		vec![("lookup", &self.plan)]
	}

	fn is_fused_lookup(&self) -> bool {
		self.fused
	}
}

impl ToSql for LookupPart {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		match self.direction {
			LookupDirection::Out => f.push_str("->..."),
			LookupDirection::In => f.push_str("<-..."),
			LookupDirection::Both => f.push_str("<->..."),
			LookupDirection::Reference => f.push_str("<~..."),
		}
	}
}

/// Lookup evaluation - graph/reference traversal.
async fn evaluate_lookup(
	value: &Value,
	lookup: &LookupPart,
	ctx: EvalContext<'_>,
) -> anyhow::Result<Value> {
	match value {
		Value::RecordId(_) | Value::Object(_) => {
			// Execute the lookup plan with this value as the current_value.
			// The CurrentValueSource operator at the leaf of the plan will
			// yield this value, and GraphEdgeScan/ReferenceScan will extract
			// RecordIds from it (including extracting `id` from Objects).
			evaluate_lookup_for_value(value, lookup, ctx).await
		}
		Value::Array(arr) => {
			// Apply lookup to each element and flatten results
			// This matches SurrealDB semantics: `->edge` on an array of records
			// returns a flat array of all targets, not nested arrays
			let mut results = Vec::new();
			for item in arr.iter() {
				let result = Box::pin(evaluate_lookup(item, lookup, ctx.clone())).await?;
				// Flatten: extend results with array elements, or push single values
				match result {
					Value::Array(inner) => results.extend(inner),
					other => results.push(other),
				}
			}
			Ok(Value::Array(results.into()))
		}
		_ => Ok(Value::None),
	}
}

/// Perform graph/reference lookup for a specific value by executing the pre-planned operator tree.
///
/// Sets `current_value` on the `ExecutionContext` so that the `CurrentValueSource`
/// operator at the leaf of the plan yields this value into the stream.
async fn evaluate_lookup_for_value(
	value: &Value,
	lookup: &LookupPart,
	ctx: EvalContext<'_>,
) -> anyhow::Result<Value> {
	// Create a new execution context with the current value set.
	// The CurrentValueSource operator reads this to seed the operator chain.
	let bound_ctx = ctx.exec_ctx.with_current_value(value.clone());
	// Bind $parent from the enclosing row so that graph WHERE clauses
	// (e.g. `->edge[WHERE out=$parent.field]`) reference the current SELECT's
	// row. Always overrides any existing binding from an outer subquery --
	// graph [WHERE] $parent scoping is per-SELECT, not per-subquery-nesting.
	let bound_ctx = if let Some(parent) = ctx.document_root {
		bound_ctx.with_param("parent", parent.clone())
	} else {
		bound_ctx
	};
	let bound_ctx = if ctx.skip_fetch_perms {
		bound_ctx.with_skip_fetch_perms(true)
	} else {
		bound_ctx
	};

	// Execute the lookup plan
	let mut stream = lookup.plan.execute(&bound_ctx).map_err(|e| match e {
		crate::expr::ControlFlow::Err(e) => e,
		crate::expr::ControlFlow::Return(v) => {
			anyhow::anyhow!("Unexpected return in lookup: {:?}", v)
		}
		crate::expr::ControlFlow::Break => anyhow::anyhow!("Unexpected break in lookup"),
		crate::expr::ControlFlow::Continue => anyhow::anyhow!("Unexpected continue in lookup"),
	})?;

	// Collect all results into an array
	let mut results = Vec::new();

	while let Some(batch_result) = stream.next().await {
		let batch = batch_result.map_err(|e| match e {
			crate::expr::ControlFlow::Err(e) => e,
			crate::expr::ControlFlow::Return(v) => {
				anyhow::anyhow!("Unexpected return in lookup: {:?}", v)
			}
			crate::expr::ControlFlow::Break => anyhow::anyhow!("Unexpected break in lookup"),
			crate::expr::ControlFlow::Continue => {
				anyhow::anyhow!("Unexpected continue in lookup")
			}
		})?;
		results.extend(batch.into_values());
	}

	// When extract_id is set, the scan used FullEdge mode for WHERE/SPLIT filtering
	// but no explicit SELECT clause was present. Project results back to RecordIds.
	if lookup.extract_id {
		let results: Vec<Value> = results
			.into_iter()
			.filter_map(|v| match v {
				Value::Object(ref obj) => {
					obj.get("id").filter(|id| matches!(id, Value::RecordId(_))).cloned()
				}
				Value::RecordId(_) => Some(v),
				_ => None,
			})
			.collect();

		if lookup.only {
			return match results.len() {
				0 => Ok(Value::None),
				1 => Ok(results.into_iter().next().expect("Exactly one result in this branch")),
				_ => Err(anyhow::anyhow!(ExecError::SingleOnlyOutput)),
			};
		}
		return Ok(Value::Array(results.into()));
	}

	if lookup.only {
		return match results.len() {
			0 => Ok(Value::None),
			1 => Ok(results.into_iter().next().expect("Exactly one result in this branch")),
			_ => Err(anyhow::anyhow!(ExecError::SingleOnlyOutput)),
		};
	}

	Ok(Value::Array(results.into()))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::dbs::Session;
	use crate::exec::operators::test_util::{
		TestDb, ValuesOperator, eval, eval_on, physical_expr, val,
	};
	use crate::exec::physical_expr::IdiomExpr;
	use crate::exec::{ExecutionContext, PhysicalExpr};
	use crate::kvs::TransactionType;

	/// Two people Tobie knows, one of whom (Sam) knows nobody.
	///
	/// The edge ids are given explicitly: traversal order follows the adjacency
	/// key, which is ordered by edge id, so generated ids would make the result
	/// order arbitrary.
	async fn graph_db() -> TestDb {
		TestDb::new(
			"DEFINE TABLE person SCHEMALESS;
			 DEFINE TABLE knows SCHEMALESS TYPE RELATION;
			 CREATE person:tobie SET name = 'Tobie';
			 CREATE person:jaime SET name = 'Jaime';
			 CREATE person:sam SET name = 'Sam';
			 RELATE person:tobie->knows:e1->person:jaime SET since = 2020;
			 RELATE person:tobie->knows:e2->person:sam SET since = 2021;",
		)
		.await
	}

	fn array_of(value: &Value) -> &crate::val::Array {
		match value {
			Value::Array(arr) => arr,
			other => panic!("expected an array, got {other:?}"),
		}
	}

	/// Compile `src` and pull out the single `LookupPart` it contains.
	async fn compile_lookup(src: &str, ctx: &ExecutionContext) -> Arc<dyn PhysicalExpr> {
		let expr = physical_expr(src, ctx).await;
		let idiom = expr.downcast_ref::<IdiomExpr>().expect("source should compile to an idiom");
		let part = idiom
			.parts
			.iter()
			.find(|p| p.downcast_ref::<LookupPart>().is_some())
			.expect("source should contain a lookup part");
		Arc::clone(part)
	}

	// =========================================================================
	// Direction
	// =========================================================================

	#[tokio::test]
	async fn an_outgoing_lookup_reaches_the_edge_targets() {
		let db = graph_db().await;
		let ctx = db.exec_ctx().await;
		let out = eval("person:tobie->knows->person", &ctx).await.unwrap();
		assert_eq!(out, val("[person:jaime, person:sam]").await);
	}

	#[tokio::test]
	async fn an_incoming_lookup_walks_the_same_edge_backwards() {
		let db = graph_db().await;
		let ctx = db.exec_ctx().await;
		let out = eval("person:jaime<-knows<-person", &ctx).await.unwrap();
		assert_eq!(out, val("[person:tobie]").await);

		// The outgoing direction from the target finds nothing -- direction is
		// not symmetric.
		let out = eval("person:jaime->knows->person", &ctx).await.unwrap();
		assert_eq!(out, val("[]").await);
	}

	#[tokio::test]
	async fn a_bidirectional_lookup_covers_edges_in_both_directions() {
		let db = graph_db().await;
		let ctx = db.exec_ctx().await;

		// Jaime has only an inbound edge, yet `<->` finds it.
		let out = eval("person:jaime<->knows<->person", &ctx).await.unwrap();
		let tobie = val("person:tobie").await;
		let reached = array_of(&out);
		assert!(
			reached.contains(&tobie),
			"a bidirectional lookup must traverse the inbound edge, got {reached:?}"
		);
	}

	#[tokio::test]
	async fn the_direction_metadata_maps_from_the_parsed_direction() {
		assert_eq!(LookupDirection::from(crate::expr::Dir::Out), LookupDirection::Out);
		assert_eq!(LookupDirection::from(crate::expr::Dir::In), LookupDirection::In);
		assert_eq!(LookupDirection::from(crate::expr::Dir::Both), LookupDirection::Both);
		assert_eq!(LookupDirection::from(&crate::expr::Dir::Out), LookupDirection::Out);
		assert_eq!(LookupDirection::from(&crate::expr::Dir::In), LookupDirection::In);
		assert_eq!(LookupDirection::from(&crate::expr::Dir::Both), LookupDirection::Both);
	}

	// =========================================================================
	// Input shapes
	// =========================================================================

	#[tokio::test]
	async fn a_record_with_no_matching_edge_yields_an_empty_array() {
		// The distinction matters: an empty array says "this record was traversed
		// and had no edges", which is not the same as NONE.
		let db = graph_db().await;
		let ctx = db.exec_ctx().await;
		let out = eval("person:sam->knows->person", &ctx).await.unwrap();
		assert_eq!(out, val("[]").await);
	}

	#[tokio::test]
	async fn a_lookup_from_a_non_record_value_yields_none_not_an_empty_array() {
		let db = graph_db().await;
		let ctx = db.exec_ctx().await;

		for start in ["'text'", "42", "true", "NONE"] {
			let row = val(&format!("{{ v: {start} }}")).await;
			let out = eval_on("v->knows->person", &row, &ctx).await.unwrap();
			assert_eq!(out, Value::None, "a lookup from {start} should be NONE");
		}
	}

	#[tokio::test]
	async fn a_lookup_from_an_object_uses_its_id_field() {
		// Objects are accepted so that a lookup can run against a decoded row
		// rather than only against a bare record id.
		let db = graph_db().await;
		let ctx = db.exec_ctx().await;
		let row = val("{ v: { id: person:tobie, name: 'Tobie' } }").await;
		let out = eval_on("v->knows->person", &row, &ctx).await.unwrap();
		assert_eq!(out, val("[person:jaime, person:sam]").await);
	}

	#[tokio::test]
	async fn a_lookup_over_an_array_flattens_the_per_element_results() {
		// One flat array of all targets, not an array of per-element arrays.
		let db = graph_db().await;
		let ctx = db.exec_ctx().await;
		let row = val("{ ids: [person:tobie, person:sam] }").await;

		let out = eval_on("ids->knows", &row, &ctx).await.unwrap();
		let edges = array_of(&out);
		assert_eq!(edges.len(), 2, "expected Tobie's two edges flattened in, got {edges:?}");
		assert!(
			edges.iter().all(|v| matches!(v, Value::RecordId(_))),
			"flattening must not leave nested arrays behind: {edges:?}"
		);
	}

	// =========================================================================
	// Edge and target filtering
	// =========================================================================

	#[tokio::test]
	async fn a_condition_on_the_edge_filters_which_edges_are_followed() {
		let db = graph_db().await;
		let ctx = db.exec_ctx().await;
		let out = eval("person:tobie->(knows WHERE since > 2020)->person", &ctx).await.unwrap();
		assert_eq!(out, val("[person:sam]").await);
	}

	#[tokio::test]
	async fn a_condition_on_the_target_filters_the_reached_records() {
		let db = graph_db().await;
		let ctx = db.exec_ctx().await;
		let out = eval("person:tobie->knows->(person WHERE name = 'Sam')", &ctx).await.unwrap();
		assert_eq!(out, val("[person:sam]").await);
	}

	#[tokio::test]
	async fn a_filtered_edge_lookup_is_projected_back_to_record_ids() {
		// A condition forces the scan to read whole edge records, but with no
		// projection asked for the result must still be record ids.
		let db = graph_db().await;
		let ctx = db.exec_ctx().await;
		let out = eval("person:tobie->(knows WHERE since > 2020)", &ctx).await.unwrap();
		let edges = array_of(&out);
		assert_eq!(edges.len(), 1, "only one edge matches, got {edges:?}");
		assert!(
			matches!(edges.first(), Some(Value::RecordId(rid)) if rid.table.as_str() == "knows"),
			"expected a `knows` record id, got {edges:?}"
		);
	}

	#[tokio::test]
	async fn a_projection_on_the_edge_keeps_the_selected_object() {
		// With an explicit field list the pipeline is authoritative, so the
		// objects it produced are returned rather than being reduced to ids.
		let db = graph_db().await;
		let ctx = db.exec_ctx().await;
		let out =
			eval("person:tobie->(SELECT since FROM knows WHERE since > 2020)", &ctx).await.unwrap();
		assert_eq!(out, val("[{ since: 2021 }]").await);
	}

	// =========================================================================
	// $parent binding for lookup conditions
	// =========================================================================

	#[tokio::test]
	async fn a_lookup_condition_reads_parent_from_the_enclosing_row() {
		let db = graph_db().await;
		let ctx = db.exec_ctx().await;
		let row = val("{ id: person:tobie, want: person:sam }").await;

		let out = eval_on("id->knows->(person WHERE id = $parent.want)", &row, &ctx).await.unwrap();
		assert_eq!(out, val("[person:sam]").await);
	}

	#[tokio::test]
	async fn the_row_binding_of_parent_overrides_an_outer_one() {
		// `$parent` scoping inside a lookup condition is per-row, so a binding
		// already present on the execution context must not win.
		let db = graph_db().await;
		let ctx = db.exec_ctx().await;
		let ctx = ctx.with_param("parent", val("{ want: person:jaime }").await);
		let row = val("{ id: person:tobie, want: person:sam }").await;

		let out = eval_on("id->knows->(person WHERE id = $parent.want)", &row, &ctx).await.unwrap();
		assert_eq!(out, val("[person:sam]").await, "the outer $parent leaked into the condition");
	}

	// =========================================================================
	// FROM ONLY semantics
	// =========================================================================

	#[tokio::test]
	async fn an_only_lookup_unwraps_a_single_result() {
		let db = graph_db().await;
		let ctx = db.exec_ctx().await;
		let out = eval("person:tobie->(SELECT since FROM ONLY knows WHERE since > 2020)", &ctx)
			.await
			.unwrap();
		assert_eq!(out, val("{ since: 2021 }").await);
	}

	#[tokio::test]
	async fn an_only_lookup_with_no_result_is_none() {
		let db = graph_db().await;
		let ctx = db.exec_ctx().await;
		let out = eval("person:sam->(SELECT since FROM ONLY knows)", &ctx).await.unwrap();
		assert_eq!(out, Value::None);
	}

	#[tokio::test]
	async fn an_only_lookup_with_several_results_is_an_error() {
		let db = graph_db().await;
		let ctx = db.exec_ctx().await;
		let err = eval("person:tobie->(SELECT since FROM ONLY knows)", &ctx).await.unwrap_err();
		assert!(
			err.to_string().contains("single result output"),
			"expected SingleOnlyOutput, got {err}"
		);
	}

	// =========================================================================
	// Permission propagation
	// =========================================================================

	#[tokio::test]
	async fn the_edge_tables_select_permission_gates_the_traversal() {
		let db = TestDb::new_with_auth(
			"DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
			 DEFINE TABLE knows SCHEMALESS TYPE RELATION PERMISSIONS FOR select NONE;
			 CREATE person:tobie SET name = 'Tobie';
			 CREATE person:jaime SET name = 'Jaime';
			 RELATE person:tobie->knows:e1->person:jaime SET since = 2020;",
		)
		.await;
		let record = Session::for_record(
			"test",
			"test",
			"user",
			crate::types::PublicValue::String("user:tobie".to_owned()),
		);
		let ctx = db.exec_ctx_as(&record, TransactionType::Read).await;

		let out = eval("person:tobie->knows->person", &ctx).await.unwrap();
		assert_eq!(out, val("[]").await, "a select-denied edge table must yield no targets");

		// Root Owner is exempt, so the edge really is there.
		let owner_ctx = db.exec_ctx().await;
		let out = eval("person:tobie->knows->person", &owner_ctx).await.unwrap();
		assert_eq!(out, val("[person:jaime]").await);
	}

	#[tokio::test]
	async fn skip_fetch_perms_is_carried_into_the_lookup_plan() {
		// Permission predicates evaluate with `skip_fetch_perms` set to stop a
		// conditional table permission from re-entering itself through a link.
		// The flag has to reach the lookup's own plan or the traversal inside such
		// a predicate would be filtered by the very permission being evaluated.
		let db = TestDb::new_with_auth(
			"DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
			 DEFINE TABLE knows SCHEMALESS TYPE RELATION PERMISSIONS FOR select NONE;
			 CREATE person:tobie SET name = 'Tobie';
			 CREATE person:jaime SET name = 'Jaime';
			 RELATE person:tobie->knows:e1->person:jaime SET since = 2020;",
		)
		.await;
		let record = Session::for_record(
			"test",
			"test",
			"user",
			crate::types::PublicValue::String("user:tobie".to_owned()),
		);
		let ctx = db.exec_ctx_as(&record, TransactionType::Read).await;

		let expr = physical_expr("person:tobie->knows->person", &ctx).await;
		let bypass = EvalContext {
			skip_fetch_perms: true,
			..EvalContext::from_exec_ctx(&ctx)
		};
		let out = expr.evaluate(bypass).await.unwrap();
		assert_eq!(out, val("[person:jaime]").await);
	}

	// =========================================================================
	// Batch evaluation
	// =========================================================================

	#[tokio::test]
	async fn batch_evaluation_preserves_row_order_and_matches_row_by_row_results() {
		// A read-only lookup runs the rows concurrently; the results must still
		// line up with the input rows one for one.
		let db = graph_db().await;
		let ctx = db.exec_ctx().await;
		let part = compile_lookup("v->knows->person", &ctx).await;

		let rows =
			vec![val("person:jaime").await, val("person:tobie").await, val("person:sam").await];
		let base = EvalContext::from_exec_ctx(&ctx);
		let batched = part.evaluate_batch(base.clone(), &rows).await.unwrap();

		let mut one_at_a_time = Vec::new();
		for row in &rows {
			one_at_a_time.push(part.evaluate(base.with_value(row)).await.unwrap());
		}

		assert_eq!(batched, one_at_a_time);
		assert_eq!(
			batched,
			vec![val("[]").await, val("[person:jaime, person:sam]").await, val("[]").await,]
		);
	}

	#[tokio::test]
	async fn a_single_row_batch_takes_the_sequential_path_with_the_same_result() {
		let db = graph_db().await;
		let ctx = db.exec_ctx().await;
		let part = compile_lookup("v->knows->person", &ctx).await;

		let rows = vec![val("person:tobie").await];
		let base = EvalContext::from_exec_ctx(&ctx);
		let batched = part.evaluate_batch(base.clone(), &rows).await.unwrap();
		assert_eq!(batched, vec![val("[person:jaime, person:sam]").await]);
	}

	// =========================================================================
	// Plan metadata the engine acts on
	// =========================================================================

	#[tokio::test]
	async fn a_lookup_always_requires_database_context_even_over_a_root_level_plan() {
		// The executor validates `required_context` before evaluating, and a
		// traversal always needs the catalog and a transaction.
		let part = LookupPart {
			direction: LookupDirection::Out,
			plan: ValuesOperator::new(vec![]),
			extract_id: false,
			fused: false,
			only: false,
		};
		assert_eq!(part.plan.required_context(), ContextLevel::Root, "fixture must be Root-level");
		assert_eq!(part.required_context(), ContextLevel::Database);
	}

	#[tokio::test]
	async fn fusion_and_the_embedded_plan_are_reported_for_continuation_and_explain() {
		// `is_fused_lookup` tells the idiom continuation logic to map per element
		// even when the lookup is the last part; `embedded_operators` is what
		// EXPLAIN walks to print the scan chain underneath the expression.
		let db = graph_db().await;
		let ctx = db.exec_ctx().await;

		let fused = compile_lookup("v->knows->person", &ctx).await;
		assert!(fused.is_fused_lookup());
		let embedded = fused.embedded_operators();
		assert_eq!(embedded.len(), 1);
		assert_eq!(embedded[0].0, "lookup");

		let single = compile_lookup("v->knows", &ctx).await;
		assert!(!single.is_fused_lookup());
	}

	#[tokio::test]
	async fn sql_rendering_shows_the_direction() {
		let plan = ValuesOperator::new(vec![]);
		let render = |direction| {
			LookupPart {
				direction,
				plan: Arc::clone(&plan),
				extract_id: false,
				fused: false,
				only: false,
			}
			.to_sql()
		};
		assert_eq!(render(LookupDirection::Out), "->...");
		assert_eq!(render(LookupDirection::In), "<-...");
		assert_eq!(render(LookupDirection::Both), "<->...");
		assert_eq!(render(LookupDirection::Reference), "<~...");
	}
}
