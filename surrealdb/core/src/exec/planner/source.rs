//! Source and lookup planning for the planner.
//!
//! Handles graph/reference lookups, index functions, range bounds, and order conversion.

use std::sync::Arc;

use super::Planner;
use super::util::{extract_table_from_context, key_lit_to_expr};
use crate::err::Error;
use crate::exec::ExecOperator;
use crate::exec::operators::{
	EdgeTableSpec, Filter, GraphEdgeScan, GraphScanOutput, Limit, OrderByField, ReferenceScan,
	ReferenceScanOutput, SortDirection,
};
use crate::exec::parts::LookupDirection;
use crate::exec::planner::select::SelectPipelineConfig;
use crate::expr::{Expr, Literal};

/// Special parameter name for passing the lookup source at execution time.
pub(crate) const LOOKUP_SOURCE_PARAM: &str = "__lookup_source__";

// ============================================================================
// impl Planner — Source Planning
// ============================================================================

impl<'ctx> Planner<'ctx> {
	/// Plan an index function call (search::highlight, search::score, search::offsets).
	pub(crate) fn plan_index_function(
		&self,
		name: &str,
		mut ast_args: Vec<Expr>,
	) -> Result<Arc<dyn crate::exec::PhysicalExpr>, Error> {
		use crate::exec::physical_expr::function::IndexFunctionExec;

		let registry = self.function_registry();
		let func = registry.get_index_function(name).ok_or_else(|| {
			Error::Unimplemented(format!("Index function '{}' not found in registry", name))
		})?;

		let match_ref_idx = func.match_ref_arg_index();

		if match_ref_idx >= ast_args.len() {
			return Err(Error::Unimplemented(format!(
				"Index function '{}' requires at least {} arguments",
				name,
				match_ref_idx + 1
			)));
		}

		let match_ref_ast = ast_args.remove(match_ref_idx);

		let match_ref = match match_ref_ast {
			Expr::Literal(Literal::Integer(n)) => n as u8,
			Expr::Literal(Literal::Float(n)) => n as u8,
			_ => {
				return Err(Error::Unimplemented(format!(
					"Index function '{}': match_ref argument must be a literal integer",
					name
				)));
			}
		};

		let mut phys_args = Vec::with_capacity(ast_args.len());
		for arg in ast_args {
			phys_args.push(self.physical_expr(arg)?);
		}

		let matches_ctx = self.ctx.get_matches_context().ok_or_else(|| {
			Error::Unimplemented(format!(
				"Index function '{}': no MATCHES clause found in WHERE condition",
				name
			))
		})?;

		let match_ctx = matches_ctx
			.resolve(match_ref, extract_table_from_context(self.ctx))
			.map_err(|e| Error::Unimplemented(format!("Index function '{}': {}", name, e)))?;

		let func_ctx = func.required_context();

		Ok(Arc::new(IndexFunctionExec {
			name: name.to_string(),
			arguments: phys_args,
			match_ctx,
			func_required_context: func_ctx,
		}))
	}

	/// Convert an `OrderList` to a `Vec<OrderByField>`.
	pub(crate) fn convert_order_list(
		&self,
		order_list: crate::expr::order::OrderList,
	) -> Result<Vec<OrderByField>, Error> {
		let mut fields = Vec::with_capacity(order_list.len());
		for order_field in order_list {
			let expr: Arc<dyn crate::exec::PhysicalExpr> = self.convert_idiom(order_field.value)?;

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
	pub(crate) fn convert_range_bound(
		&self,
		bound: &std::ops::Bound<crate::expr::RecordIdKeyLit>,
	) -> Result<std::ops::Bound<Arc<dyn crate::exec::PhysicalExpr>>, Error> {
		match bound {
			std::ops::Bound::Unbounded => Ok(std::ops::Bound::Unbounded),
			std::ops::Bound::Included(lit) => {
				let expr = key_lit_to_expr(lit)?;
				let phys = self.physical_expr(expr)?;
				Ok(std::ops::Bound::Included(phys))
			}
			std::ops::Bound::Excluded(lit) => {
				let expr = key_lit_to_expr(lit)?;
				let phys = self.physical_expr(expr)?;
				Ok(std::ops::Bound::Excluded(phys))
			}
		}
	}

	/// Plan a Lookup operation (graph edge or reference traversal).
	pub(crate) fn plan_lookup(
		&self,
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
		let source_expr: Arc<dyn crate::exec::PhysicalExpr> =
			Arc::new(crate::exec::physical_expr::Param(LOOKUP_SOURCE_PARAM.into()));

		let needs_full_pipeline = expr.is_some() || group.is_some();
		let needs_full_records = needs_full_pipeline || cond.is_some() || split.is_some();
		let output_mode = if needs_full_records {
			GraphScanOutput::FullEdge
		} else {
			GraphScanOutput::TargetId
		};

		let base_scan: Arc<dyn ExecOperator> = match &kind {
			crate::expr::lookup::LookupKind::Graph(dir) => {
				let edge_tables: Vec<EdgeTableSpec> = what
					.into_iter()
					.map(|s| match s {
						crate::expr::lookup::LookupSubject::Table {
							table,
							..
						} => Ok(EdgeTableSpec {
							table,
							range_start: std::ops::Bound::Unbounded,
							range_end: std::ops::Bound::Unbounded,
						}),
						crate::expr::lookup::LookupSubject::Range {
							table,
							range,
							..
						} => {
							let range_start = self.convert_range_bound(&range.start)?;
							let range_end = self.convert_range_bound(&range.end)?;
							Ok(EdgeTableSpec {
								table,
								range_start,
								range_end,
							})
						}
					})
					.collect::<Result<Vec<_>, Error>>()?;

				Arc::new(GraphEdgeScan {
					source: source_expr,
					direction: LookupDirection::from(dir),
					edge_tables,
					output_mode,
				})
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
								let rs = self.convert_range_bound(&range.start)?;
								let re = self.convert_range_bound(&range.end)?;
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

				Arc::new(ReferenceScan {
					source: source_expr,
					referencing_table,
					referencing_field,
					output_mode: ref_output_mode,
					range_start,
					range_end,
				})
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
			};
			self.plan_pipeline(base_scan, expr, config)
		} else {
			let filtered: Arc<dyn ExecOperator> = if let Some(cond) = cond {
				let predicate = self.physical_expr(cond.0)?;
				Arc::new(Filter {
					input: base_scan,
					predicate,
				})
			} else {
				base_scan
			};

			let split_op: Arc<dyn ExecOperator> = if let Some(splits) = split {
				Arc::new(crate::exec::operators::Split {
					input: filtered,
					idioms: splits.into_iter().map(|s| s.0).collect(),
				})
			} else {
				filtered
			};

			let sorted: Arc<dyn ExecOperator> =
				if let Some(crate::expr::order::Ordering::Order(order_list)) = order {
					let order_by = self.convert_order_list(order_list)?;
					Arc::new(crate::exec::operators::Sort {
						input: split_op,
						order_by,
					})
				} else {
					split_op
				};

			let limited: Arc<dyn ExecOperator> = if limit.is_some() || start.is_some() {
				let limit_expr = limit.map(|l| self.physical_expr(l.0)).transpose()?;
				let offset_expr = start.map(|s| self.physical_expr(s.0)).transpose()?;
				Arc::new(Limit {
					input: sorted,
					limit: limit_expr,
					offset: offset_expr,
				})
			} else {
				sorted
			};

			Ok(limited)
		}
	}
}
