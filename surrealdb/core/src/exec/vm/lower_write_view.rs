//! The views stage lowered to instructions: ProcessTableViews.
//!
//! A table's views are lowered from the definitions the catalog holds when the
//! program is lowered. Each view's clauses are lowered from their logical
//! forms with a planner of the writer's catalog and authority, permissions
//! suppressed as the view frame suppresses them, and each becomes a host
//! pipeline over the image it is about. A clause with no logical form is not
//! covered.
//!
//! The row a view computes is written through the view table's own order,
//! which is lowered here too, from the view table's definitions, as a unit of
//! the view-row form ([`super::lower_view_rows`]). A view of a view lowers that
//! view table's order in turn. A view table's order is a unit of the
//! statement's arena, found again by what it is lowered with, so one reached
//! again while it is being lowered — a cycle of views — closes on that unit.
//! What the view tables' orders read of the catalog is part of what the
//! program read.
//!
//! An aggregated view's recomputation of an extreme a removed record held is a
//! statement lowered here as a unit, its group's key values the parameters it
//! reads.

use std::sync::Arc;

use super::write_observe::{ClauseCode, Observed};
use super::{Lowered, Lowering, NotCovered, lower_view_rows};
use crate::catalog::ViewDefinition;
use crate::catalog::aggregation::{
	AggregateFields, AggregationAnalysis, AggregationStat, Recalculation,
};
use crate::doc::Action;
use crate::exec::operators::write::stages::view::ViewSources;
use crate::exec::planner::Planner;
use crate::exec::vm::program::{Instr, UnitHead, UnitSig, WriteInstr};
use crate::exec::vm::write::{
	LoweredView, LoweredViews, ObserveInstr, Recalc, ViewClause, ViewRows,
};
use crate::expr::logical::LogicalExpr;
use crate::val::TableName;

impl<'ctx> Lowering<'_, 'ctx> {
	/// Emit `ProcessTableViews` for `action`: the views of the one table the
	/// statement writes to, their clauses and view tables' orders lowered now.
	pub(super) async fn views_stage(&mut self, action: Action) -> Lowered<()> {
		let lowered = match &self.views_of {
			Some(lowered) => Arc::clone(lowered),
			None => {
				let lowered = crate::exec::vm::host::out_of_line(|| self.lowered_views()).await?;
				self.views_of = Some(Arc::clone(&lowered));
				lowered
			}
		};
		self.emit(Instr::Write(WriteInstr::Observe(ObserveInstr::Views {
			action,
			lowered,
		})));
		Ok(())
	}

	/// The views of the one table the statement writes to, as its plan read
	/// them.
	async fn lowered_views(&mut self) -> Lowered<Arc<LoweredViews>> {
		let Some(table) = self.write_table.clone() else {
			return Err(NotCovered::internal("the views of a write that names no single table"));
		};
		let Some(txn) = self.planner.txn().cloned() else {
			return Err(NotCovered::internal("the views of a write lowered with no transaction"));
		};
		let definitions = match self.catalog.written(&txn, &table)? {
			Some((_, written)) => Arc::clone(&written.views),
			None => Arc::from(Vec::new()),
		};
		// In the view frame, whose enforcement is off.
		let mut planner = self.planner.shadowing(std::iter::empty());
		planner.perms_suppressed = true;
		let mut views = Vec::with_capacity(definitions.len());
		for definition in definitions.iter() {
			let view = definition.view.as_ref().ok_or_else(|| {
				NotCovered::internal(format!(
					"{} is listed among the views of {table} and holds no view",
					definition.name
				))
			})?;
			let lowered = match view {
				ViewDefinition::Materialized {
					fields,
					condition,
					..
				} => {
					let rows =
						crate::exec::vm::host::out_of_line(|| self.view_rows(&definition.name))
							.await?;
					let condition = match (condition, view.materialized_condition_logical()) {
						(Some(_), Some(logical)) => {
							let logical = logical.map_err(|e| {
								NotCovered::internal(format!(
									"a view's condition has no logical form: {e}"
								))
							})?;
							Some(
								crate::exec::vm::host::out_of_line(|| {
									self.view_clauses(&planner, logical)
								})
								.await?,
							)
						}
						_ => None,
					};
					let logical =
						crate::expr::logical::lower_fields(fields.clone()).map_err(|e| {
							NotCovered::internal(format!(
								"a view's fields have no logical form: {e}"
							))
						})?;
					let projection = crate::exec::vm::map::projection(&logical, Vec::new())?;
					let pipeline = self
						.clause_in(&planner, None, ClauseCode::Projection(&projection), false)
						.await?;
					LoweredView::Materialized {
						condition,
						projection: ViewClause {
							object: pipeline,
							any: pipeline,
						},
						rows,
					}
				}
				ViewDefinition::Aggregated {
					analysis,
					condition: stored,
					tables,
					..
				} => {
					let AggregateFields::Fields(items) = &analysis.fields else {
						return Err(NotCovered::internal("a VALUE selector on an aggregated view"));
					};
					let rows =
						crate::exec::vm::host::out_of_line(|| self.view_rows(&definition.name))
							.await?;
					let condition = match stored {
						Some(expr) => Some(self.view_clauses(&planner, &logical_of(expr)?).await?),
						None => None,
					};
					let mut group = Vec::with_capacity(analysis.group_expressions.len());
					for expr in &analysis.group_expressions {
						group.push(self.view_clauses(&planner, &logical_of(expr)?).await?);
					}
					let mut arguments = Vec::with_capacity(analysis.aggregate_arguments.len());
					for expr in &analysis.aggregate_arguments {
						arguments.push(self.view_clauses(&planner, &logical_of(expr)?).await?);
					}
					let mut fields = Vec::with_capacity(items.len());
					for (name, expr) in items {
						let place =
							self.place_steps(name).await.ok_or_else(|| self.place_refusal())?;
						let pipeline = self.view_clauses(&planner, &logical_of(expr)?).await?;
						fields.push((place, pipeline));
					}
					let sources = ViewSources {
						analysis,
						tables,
						condition: stored.as_ref(),
					};
					let recalc = crate::exec::vm::host::out_of_line(|| {
						self.recalculation(&planner, &sources)
					})
					.await?;
					LoweredView::Aggregated {
						condition,
						group,
						arguments,
						fields,
						recalc,
						rows,
					}
				}
				ViewDefinition::Select {
					..
				} => LoweredView::Unmaintained,
			};
			views.push(lowered);
		}
		Ok(Arc::new(LoweredViews {
			definitions,
			views,
		}))
	}

	/// The host pipelines of a view's clause: one for an image that is an
	/// object and one for any value.
	async fn view_clauses(
		&mut self,
		planner: &Planner<'ctx>,
		expr: &LogicalExpr,
	) -> Lowered<ViewClause> {
		let object = crate::exec::vm::host::out_of_line(|| {
			self.clause_in(planner, None, ClauseCode::Expr(expr), true)
		})
		.await?;
		let any = crate::exec::vm::host::out_of_line(|| {
			self.clause_in(planner, None, ClauseCode::Expr(expr), false)
		})
		.await?;
		Ok(ViewClause {
			object,
			any,
		})
	}

	/// The unit that recomputes, over the rows an aggregated view holds a
	/// group of, every extreme the view's stats keep, with the group's key
	/// values bound to the parameters its statement reads; `None` for a view
	/// that keeps none.
	async fn recalculation(
		&mut self,
		planner: &Planner<'ctx>,
		sources: &ViewSources<'_>,
	) -> Lowered<Option<Recalc>> {
		let extremes = extremes_of(sources.analysis);
		if extremes.is_empty() {
			return Ok(None);
		}
		let names: Vec<String> = (0..sources.analysis.group_expressions.len())
			.map(|index| format!("__view_group_{index}"))
			.collect();
		let recalculations: Vec<Recalculation> = extremes
			.iter()
			.map(|(function, arg)| Recalculation {
				function: function.clone(),
				stat: 0,
				arg: *arg,
			})
			.collect();
		let op = crate::exec::operators::write::stages::view::recalculation_of(
			sources,
			&recalculations,
			&names,
		)
		.map_err(|_| {
			NotCovered::internal("an aggregated view's recalculation has no logical form")
		})?;
		// The group's key values are read from the frame the unit runs in.
		let planner = planner.shadowing(names.iter().cloned());
		let bound = crate::exec::planner::bind_statement(&planner, op).await.map_err(|e| {
			NotCovered::internal(format!("an aggregated view's recalculation did not bind: {e}"))
		})?;
		let spec =
			crate::exec::vm::host::out_of_line(|| self.subquery_unit(&planner, &bound)).await?;
		Ok(Some(Recalc {
			unit: spec.unit,
			extremes,
		}))
	}

	/// The order of view table `table`'s rows, a unit of the arena this
	/// lowering builds: the one linked for `table` and this lowering's planner,
	/// which a view of a view reaching `table` again while its order is being
	/// lowered finds, or one lowered now.
	async fn view_rows(&mut self, table: &TableName) -> Lowered<ViewRows> {
		let of = Observed::ViewRows(table.clone());
		if let Some(unit) = self.observer_unit(&of) {
			return Ok(ViewRows {
				unit,
			});
		}
		let id = self.link_observer(of)?;
		let mut arena = self.take_arena();
		let lowered = Box::pin(lower_view_rows(
			self.planner,
			self.ahead,
			table.clone(),
			Some(&mut arena),
			&self.catalog,
		))
		.await;
		self.put_arena(arena);
		let mut program = match lowered {
			Ok(program) => program,
			Err(refused) => {
				self.rewind_arena(id.0 as usize);
				return Err(refused);
			}
		};
		self.delegate_context = self.delegate_context.max(program.required_context);
		program.unit = Some(UnitHead {
			sig: UnitSig {
				params: Vec::new(),
				takes_row: false,
			},
			frame: Self::statement_frame(self.planner),
			entry: None,
			closure: None,
		});
		self.units[id.0 as usize] = Arc::new(program);
		Ok(ViewRows {
			unit: id,
		})
	}
}

/// The extremes `analysis`'s stats keep, each as the aggregate function that
/// recomputes it and the aggregate argument it reads.
fn extremes_of(analysis: &AggregationAnalysis) -> Vec<(String, usize)> {
	let mut extremes: Vec<(String, usize)> = Vec::new();
	for aggregation in &analysis.aggregations {
		let extreme = match aggregation.to_stat() {
			AggregationStat::NumberMax {
				arg,
				..
			} => ("math::max", arg),
			AggregationStat::NumberMin {
				arg,
				..
			} => ("math::min", arg),
			AggregationStat::TimeMax {
				arg,
				..
			} => ("time::max", arg),
			AggregationStat::TimeMin {
				arg,
				..
			} => ("time::min", arg),
			AggregationStat::Count {
				..
			}
			| AggregationStat::CountValue {
				..
			}
			| AggregationStat::Sum {
				..
			}
			| AggregationStat::Mean {
				..
			}
			| AggregationStat::StdDev {
				..
			}
			| AggregationStat::Variance {
				..
			}
			| AggregationStat::Accumulate {
				..
			} => continue,
		};
		let extreme = (extreme.0.to_owned(), extreme.1);
		if !extremes.contains(&extreme) {
			extremes.push(extreme);
		}
	}
	extremes
}

/// The logical form of a stored view clause.
fn logical_of(expr: &crate::expr::Expr) -> Lowered<LogicalExpr> {
	crate::expr::logical::lower_expr(expr.clone())
		.map_err(|e| NotCovered::internal(format!("a view's clause has no logical form: {e}")))
}
