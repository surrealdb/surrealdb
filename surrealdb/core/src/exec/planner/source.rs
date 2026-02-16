//! Source and lookup planning for the planner.
//!
//! Handles graph/reference lookups, index functions, range bounds, and order conversion.

use std::sync::Arc;

use super::Planner;
use super::util::{extract_table_from_context, key_lit_to_expr};
use crate::err::Error;
use crate::exec::ExecOperator;
use crate::exec::operators::{
	CurrentValueSource, EdgeTableSpec, Filter, GraphEdgeScan, GraphScanOutput, Limit, OrderByField,
	ReferenceScan, ReferenceScanOutput, SortDirection,
};
use crate::exec::parts::LookupDirection;
use crate::exec::planner::select::SelectPipelineConfig;
use crate::expr::{Expr, Literal};

// ============================================================================
// impl Planner — Source Planning
// ============================================================================

impl<'ctx> Planner<'ctx> {
	/// Plan an index function call.
	///
	/// Dispatches generically based on [`IndexContextKind`] declared by the
	/// function -- no hardcoded function names. The function declares what kind
	/// of index context it needs, and the planner resolves it:
	///
	/// - **FullText**: extracts the index_ref argument, resolves via MATCHES context
	/// - **Knn**: retrieves the KNN context from the planning context
	pub(crate) async fn plan_index_function(
		&self,
		name: &str,
		mut ast_args: Vec<Expr>,
	) -> Result<Arc<dyn crate::exec::PhysicalExpr>, Error> {
		use crate::exec::function::{IndexContext, IndexContextKind};
		use crate::exec::physical_expr::function::IndexFunctionExec;

		let registry = self.function_registry();
		let func = registry.get_index_function(name).ok_or_else(|| Error::Query {
			message: format!("Index function '{}' not found in registry", name),
		})?;

		// Resolve the appropriate index context based on the function's declared kind
		let index_ctx = match func.index_context_kind() {
			IndexContextKind::FullText => {
				// FullText functions must declare which argument is the index ref
				let ref_idx = func.index_ref_arg_index().ok_or_else(|| Error::Query {
					message: format!(
						"Index function '{}': FullText functions must declare an index_ref_arg_index",
						name
					),
				})?;

				if ref_idx >= ast_args.len() {
					return Err(Error::Query {
						message: format!(
							"Index function '{}' requires at least {} arguments",
							name,
							ref_idx + 1
						),
					});
				}

				// Extract the match_ref argument at plan time (not passed at runtime)
				let match_ref_ast = ast_args.remove(ref_idx);
				let match_ref = match match_ref_ast {
					Expr::Literal(Literal::Integer(n)) => n as u8,
					Expr::Literal(Literal::Float(n)) => n as u8,
					_ => {
						return Err(Error::Query {
							message: format!(
								"Index function '{}': index_ref argument must be a literal integer",
								name
							),
						});
					}
				};

				// Resolve the MatchContext from the MATCHES context
				let matches_ctx = self.ctx.get_matches_context().ok_or_else(|| Error::Query {
					message: format!(
						"Index function '{}': no MATCHES clause found in WHERE condition",
						name
					),
				})?;

				let match_ctx = matches_ctx
					.resolve(match_ref, extract_table_from_context(self.ctx))
					.map_err(|e| Error::Query {
						message: format!("Index function '{}': {}", name, e),
					})?;

				IndexContext::FullText(match_ctx)
			}
			IndexContextKind::Knn => {
				// KNN functions: retrieve the KNN context from the planning context.
				// If there's a ref argument, extract it (currently unused for single-KNN queries).
				if let Some(ref_idx) = func.index_ref_arg_index()
					&& ref_idx < ast_args.len()
				{
					ast_args.remove(ref_idx);
				}

				let knn_ctx = self.ctx.get_knn_context().ok_or_else(|| Error::Query {
					message: format!(
						"Index function '{}': no KNN operator found in WHERE condition",
						name
					),
				})?;
				IndexContext::Knn(knn_ctx.clone())
			}
		};

		// Compile remaining arguments to physical expressions
		let mut phys_args = Vec::with_capacity(ast_args.len());
		for arg in ast_args {
			phys_args.push(self.physical_expr(arg).await?);
		}

		let func_ctx = func.required_context();

		Ok(Arc::new(IndexFunctionExec {
			name: name.to_string(),
			arguments: phys_args,
			index_ctx,
			func_required_context: func_ctx,
		}))
	}

	/// Convert an `OrderList` to a `Vec<OrderByField>`.
	pub(crate) async fn convert_order_list(
		&self,
		order_list: crate::expr::order::OrderList,
	) -> Result<Vec<OrderByField>, Error> {
		let mut fields = Vec::with_capacity(order_list.len());
		for order_field in order_list {
			let expr: Arc<dyn crate::exec::PhysicalExpr> =
				self.convert_idiom(order_field.value).await?;

			let direction = if order_field.direction {
				SortDirection::Asc
			} else {
				SortDirection::Desc
			};

			fields.push(OrderByField {
				expr,
				direction,
				collate: order_field.collate,
				numeric: order_field.numeric,
			});
		}
		Ok(fields)
	}

	/// Convert a `Bound<RecordIdKeyLit>` to a `Bound<Arc<dyn PhysicalExpr>>`.
	pub(crate) async fn convert_range_bound(
		&self,
		bound: &std::ops::Bound<crate::expr::RecordIdKeyLit>,
	) -> Result<std::ops::Bound<Arc<dyn crate::exec::PhysicalExpr>>, Error> {
		match bound {
			std::ops::Bound::Unbounded => Ok(std::ops::Bound::Unbounded),
			std::ops::Bound::Included(lit) => {
				let expr = key_lit_to_expr(lit)?;
				let phys = self.physical_expr(expr).await?;
				Ok(std::ops::Bound::Included(phys))
			}
			std::ops::Bound::Excluded(lit) => {
				let expr = key_lit_to_expr(lit)?;
				let phys = self.physical_expr(expr).await?;
				Ok(std::ops::Bound::Excluded(phys))
			}
		}
	}

	/// Plan a Lookup operation (graph edge or reference traversal).
	///
	/// Builds a streaming operator chain rooted at `CurrentValueSource`.
	/// At execution time, `LookupPart` sets `current_value` on the
	/// `ExecutionContext` before executing this chain, so `CurrentValueSource`
	/// yields the appropriate RecordId into the stream.
	pub(crate) async fn plan_lookup(
		&self,
		lookup: crate::expr::lookup::Lookup,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let input: Arc<dyn ExecOperator> = Arc::new(CurrentValueSource::new());
		self.plan_lookup_with_input(input, lookup).await
	}

	/// Plan a Lookup operation with a specific input operator.
	///
	/// This is the core of lookup planning. When fusing consecutive lookups
	/// into a single operator chain, the planner passes the output of one
	/// lookup as the `input` to the next, instead of always creating a fresh
	/// `CurrentValueSource`.
	pub(crate) async fn plan_lookup_with_input(
		&self,
		input: Arc<dyn ExecOperator>,
		crate::expr::lookup::Lookup {
			kind,
			expr,
			what,
			cond,
			split,
			group,
			order,
			limit,
			start,
			alias: _,
		}: crate::expr::lookup::Lookup,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let needs_full_pipeline = expr.is_some() || group.is_some();
		let needs_full_records = needs_full_pipeline || cond.is_some() || split.is_some();
		let output_mode = if needs_full_records {
			GraphScanOutput::FullEdge
		} else {
			GraphScanOutput::TargetId
		};

		let base_scan: Arc<dyn ExecOperator> = match &kind {
			crate::expr::lookup::LookupKind::Graph(dir) => {
				let mut edge_tables: Vec<EdgeTableSpec> = Vec::with_capacity(what.len());
				for s in what {
					let spec = match s {
						crate::expr::lookup::LookupSubject::Table {
							table,
							..
						} => EdgeTableSpec {
							table,
							range_start: std::ops::Bound::Unbounded,
							range_end: std::ops::Bound::Unbounded,
						},
						crate::expr::lookup::LookupSubject::Range {
							table,
							range,
							..
						} => {
							let range_start = self.convert_range_bound(&range.start).await?;
							let range_end = self.convert_range_bound(&range.end).await?;
							EdgeTableSpec {
								table,
								range_start,
								range_end,
							}
						}
					};
					edge_tables.push(spec);
				}

				Arc::new(GraphEdgeScan::new(
					input,
					LookupDirection::from(dir),
					edge_tables,
					output_mode,
				))
			}
			crate::expr::lookup::LookupKind::Reference => {
				let (referencing_table, referencing_field, range_start, range_end) =
					if let Some(subject) = what.first() {
						match subject {
							crate::expr::lookup::LookupSubject::Table {
								table,
								referencing_field,
							} => (
								Some(table.clone()),
								referencing_field.clone(),
								std::ops::Bound::Unbounded,
								std::ops::Bound::Unbounded,
							),
							crate::expr::lookup::LookupSubject::Range {
								table,
								referencing_field,
								range,
							} => {
								let rs = self.convert_range_bound(&range.start).await?;
								let re = self.convert_range_bound(&range.end).await?;
								(Some(table.clone()), referencing_field.clone(), rs, re)
							}
						}
					} else {
						(None, None, std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)
					};

				let ref_output_mode = if needs_full_records {
					ReferenceScanOutput::FullRecord
				} else {
					ReferenceScanOutput::RecordId
				};

				Arc::new(ReferenceScan::new(
					input,
					referencing_table,
					referencing_field,
					ref_output_mode,
					range_start,
					range_end,
				))
			}
		};

		if needs_full_pipeline {
			let config = SelectPipelineConfig {
				cond,
				split,
				group,
				order,
				limit,
				start,
				omit: vec![],
				is_value_source: false,
				tempfiles: false,
				filter_pushed: false,
				precompiled_predicate: None,
			};
			self.plan_pipeline(base_scan, expr, config).await
		} else {
			let filtered: Arc<dyn ExecOperator> = if let Some(cond) = cond {
				let predicate = self.physical_expr(cond.0).await?;
				Arc::new(Filter::new(base_scan, predicate))
			} else {
				base_scan
			};

			let split_op: Arc<dyn ExecOperator> = if let Some(splits) = split {
				Arc::new(crate::exec::operators::Split::new(
					filtered,
					splits.into_iter().map(|s| s.0).collect(),
				))
			} else {
				filtered
			};

			let sorted: Arc<dyn ExecOperator> =
				if let Some(crate::expr::order::Ordering::Order(order_list)) = order {
					let order_by = self.convert_order_list(order_list).await?;
					Arc::new(crate::exec::operators::Sort::new(split_op, order_by))
				} else {
					split_op
				};

			let limited: Arc<dyn ExecOperator> = if limit.is_some() || start.is_some() {
				let limit_expr = match limit {
					Some(l) => Some(self.physical_expr(l.0).await?),
					None => None,
				};
				let offset_expr = match start {
					Some(s) => Some(self.physical_expr(s.0).await?),
					None => None,
				};
				Arc::new(Limit::new(sorted, limit_expr, offset_expr))
			} else {
				sorted
			};

			Ok(limited)
		}
	}
}
