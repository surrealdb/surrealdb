//! A delete's cascades lowered to instructions: the edge cascade, and the
//! loop over the reference keys naming the deleted record, each repaired as
//! the field naming it directs.
//!
//! Everything a cascade runs is lowered when the statement arrives. The
//! statements a purge performs on its own behalf — the delete of the edges
//! joined to the record, the delete of a referencing record, the read and the
//! updates that take a link out of a referencing record — are built from the
//! definitions the catalog holds, bound for the frame each runs in, and
//! lowered as units of the arena of the statement being lowered. A unit is
//! found again by what it is and the frame it is lowered for, so a cascade that
//! reaches itself through the references of its own records closes on the unit
//! being lowered. The edge cascade, the same statement for every delete of a
//! database, is a bundle of an arena of its own that the node keeps
//! ([`super::super::units::CatalogUnit::PurgeEdges`]). Its delete may reach any
//! table, and loads the bundle of each edge table's stages by name when it
//! writes a record of it ([`super::super::fault::serve_stage_bundle`]): an
//! edge cascade those stages make for the same actor is the cascade running
//! them, which their arena does not hold, and the instruction that runs it
//! names it as the one the context it runs in carries. The repairs of a delete
//! are every `REFERENCE` field of the database that can name a record of the
//! table written, which the node keeps too ([`super::super::units::referencing`]),
//! and the instruction that runs them holds, for each field, the unit or the
//! host pipeline that repairs it.

use std::sync::Arc;

use surrealdb_types::ToSql;

use super::write_observe::ClauseCode;
use super::{Lowered, Lowering, NotCovered};
use crate::exec::operators::write::stages::purge::{
	self, PURGE_DELETED, PURGE_REFERENCING, PURGE_VERTEX, REFERENCE,
};
use crate::exec::planner::Planner;
use crate::exec::vm::dispatch::Reach;
use crate::exec::vm::program::{Instr, Program, UnitHead, UnitId, UnitSig, WriteInstr};
use crate::exec::vm::write::{ReferenceInstr, RepairStrategy, StorageInstr, TableRepairs};
use crate::expr::logical::LogicalOp;
use crate::expr::reference::ReferenceDeleteStrategy;
use crate::val::TableName;

/// A statement a purge performs on its own behalf.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum CascadeShape {
	/// `DELETE $__purge_vertex<->`: the edges joined to a deleted record.
	Edges,
	/// `DELETE $__purge_referencing`: a record of `table` that references the
	/// deleted record.
	Delete {
		table: TableName,
	},
	/// `SELECT * FROM $__purge_referencing`: a record of `table`, as a repair
	/// sees it.
	Read {
		table: TableName,
	},
	/// `UPDATE $__purge_referencing UNSET field`: a record of `table` whose
	/// `field` holds the deleted record.
	Unset {
		table: TableName,
		field: crate::expr::Idiom,
	},
	/// `UPDATE $__purge_referencing SET field -= $__purge_deleted`: a record
	/// of `table` whose `field` holds the deleted record among a collection.
	Subtract {
		table: TableName,
		field: crate::expr::Idiom,
	},
}

/// A cascade statement linked into the arena being lowered: what it is, the
/// frame it is lowered for, and its unit.
pub(super) struct CascadeLink {
	shape: CascadeShape,
	suppressed: bool,
	caller: bool,
	unit: UnitId,
}

impl CascadeLink {
	/// Whether the unit the link names is among the first `len` of the arena.
	pub(super) fn names_before(&self, len: usize) -> bool {
		(self.unit.0 as usize) < len
	}
}

impl<'ctx> Lowering<'_, 'ctx> {
	/// Emit the cascades of the record's delete: the records joined to it by
	/// an edge deleted, then each reference key naming it taken in turn and
	/// its referencing record repaired, then the keys removed.
	pub(super) async fn cascades(&mut self) -> Lowered<()> {
		let references = |instr| Instr::Write(WriteInstr::Storage(StorageInstr::References(instr)));
		// The stages an edge cascade loads by name run beneath the cascade, and
		// their own edge cascade, lowered for the same actor, is it.
		let edges = if self.edges_enclosing == Some(self.caller_suppressed) {
			None
		} else {
			Some(
				crate::exec::vm::host::out_of_line(|| self.cascade_unit(CascadeShape::Edges))
					.await?,
			)
		};
		let tables = crate::exec::vm::host::out_of_line(|| self.repair_tables()).await?;
		self.emit(references(ReferenceInstr::PurgeEdges {
			unit: edges,
		}));
		let none = self.emit(references(ReferenceInstr::Drain {
			target: 0,
		}));
		let top = self.here();
		let taken = self.emit(references(ReferenceInstr::Next {
			target: 0,
		}));
		self.emit(references(ReferenceInstr::Repair {
			tables,
		}));
		self.emit(Instr::Jump {
			target: top,
		});
		let here = self.here();
		self.patch(taken, here);
		self.emit(references(ReferenceInstr::Release));
		let end = self.here();
		self.patch(none, end);
		Ok(())
	}

	/// The repairs of every referencing table that can name a record of the
	/// table being written, or of any table where the unit stands for one the
	/// catalog does not hold: for each `REFERENCE` field, the strategy its
	/// definition directs, lowered.
	async fn repair_tables(&mut self) -> Lowered<Arc<[TableRepairs]>> {
		let Some(txn) = self.planner.txn().cloned() else {
			return Err(NotCovered::internal("repairs lowered with no transaction"));
		};
		// A database the catalog does not hold has no `REFERENCE` field to
		// repair.
		let Some((ns, db)) = self.database_ids()? else {
			return Ok(Arc::from(Vec::new()));
		};
		let target = self.write_table.clone().filter(|_| !self.generic_table);
		let referencing = self.catalog.referencing(&txn, target.as_ref())?;
		super::super::units::note_referencing(&txn, (ns, db), target.as_ref(), &referencing);
		// The `THEN` repairs are lowered as the purge compiles them: the
		// writer's catalog and authority, permissions off, `$reference` left
		// to each repair.
		let mut clause_planner = self.planner.shadowing([REFERENCE.to_owned()]);
		clause_planner.perms_suppressed = true;
		let mut repairs = Vec::new();
		for (table, fields) in referencing.iter() {
			let mut declared = Vec::new();
			for field in fields.iter() {
				let Some(reference) = &field.reference else {
					continue;
				};
				let strategy = match &reference.on_delete {
					ReferenceDeleteStrategy::Ignore => RepairStrategy::Ignore,
					ReferenceDeleteStrategy::Reject => RepairStrategy::Reject,
					ReferenceDeleteStrategy::Cascade => RepairStrategy::Cascade {
						unit: crate::exec::vm::host::out_of_line(|| {
							self.cascade_unit(CascadeShape::Delete {
								table: table.clone(),
							})
						})
						.await?,
					},
					ReferenceDeleteStrategy::Unset => RepairStrategy::Unset {
						field: field.name.clone(),
						read: crate::exec::vm::host::out_of_line(|| {
							self.cascade_unit(CascadeShape::Read {
								table: table.clone(),
							})
						})
						.await?,
						unset: crate::exec::vm::host::out_of_line(|| {
							self.cascade_unit(CascadeShape::Unset {
								table: table.clone(),
								field: field.name.clone(),
							})
						})
						.await?,
						subtract: crate::exec::vm::host::out_of_line(|| {
							self.cascade_unit(CascadeShape::Subtract {
								table: table.clone(),
								field: field.name.clone(),
							})
						})
						.await?,
					},
					ReferenceDeleteStrategy::Custom(expr) => RepairStrategy::Custom {
						clause: crate::exec::vm::host::out_of_line(|| {
							self.custom_clause(&clause_planner, expr)
						})
						.await?,
					},
				};
				declared.push((field.name.to_sql(), strategy));
			}
			if !declared.is_empty() {
				repairs.push(TableRepairs {
					table: table.clone(),
					fields: declared,
				});
			}
		}
		Ok(repairs.into())
	}

	/// The unit that runs statement `shape`, lowered for the frame the
	/// cascade runs it in, and the unit already linked for the same shape and
	/// frame when there is one, which is how a cascade that reaches itself
	/// closes.
	async fn cascade_unit(&mut self, shape: CascadeShape) -> Lowered<UnitId> {
		// The frame each statement runs in: an edge cascade restores the
		// actor's own permission state, a reference cascade suppresses
		// enforcement for the record it deletes while keeping the actor's for
		// whatever that delete triggers, and a repair reads and updates with
		// permissions off.
		let (suppressed, caller) = match &shape {
			CascadeShape::Edges => (self.caller_suppressed, self.caller_suppressed),
			CascadeShape::Delete {
				..
			} => (true, self.caller_suppressed),
			CascadeShape::Read {
				..
			}
			| CascadeShape::Unset {
				..
			}
			| CascadeShape::Subtract {
				..
			} => (true, true),
		};
		if let Some(link) = self.cascade_links.iter().find(|link| {
			link.shape == shape && link.suppressed == suppressed && link.caller == caller
		}) {
			return Ok(link.unit);
		}
		let (op, names, reach) = statement_of(&shape)?;
		let mut planner = self.planner.shadowing(names.iter().map(|name| (*name).to_owned()));
		planner.perms_suppressed = suppressed;
		// The edge cascade is the same statement for every delete of the
		// database: a bundle the node keeps, linked here.
		if shape == CascadeShape::Edges {
			let unit = super::super::units::CatalogUnit::PurgeEdges {
				caller_suppressed: caller,
			};
			let bundle = super::super::units::catalog_unit(
				&planner,
				unit,
				edge_bundle(&planner, op, &names, caller),
			)
			.await?;
			if let Some(entry) = bundle.units.first() {
				self.delegate_context = self.delegate_context.max(entry.required_context);
			}
			let id = u32::try_from(self.units.len())
				.map_err(|_| NotCovered::internal("more units than a program holds"))?;
			self.units.push(bundle);
			self.cascade_links.push(CascadeLink {
				shape,
				suppressed,
				caller,
				unit: UnitId(id),
			});
			return Ok(UnitId(id));
		}
		let bound = crate::exec::vm::host::out_of_line(|| planner.bind_scoped(op, &names))
			.await
			.map_err(|e| NotCovered::internal(format!("a cascade statement did not bind: {e}")))?;
		let physical =
			super::super::optimize::optimize(super::super::map::map(&planner, &bound).await?);
		let id = u32::try_from(self.units.len())
			.map_err(|_| NotCovered::internal("more units than a program holds"))?;
		// The slot is held while the statement lowers, so a cascade reaching
		// it from inside finds it.
		self.units.push(Arc::new(Lowering::new(&planner).finish()?));
		self.cascade_links.push(CascadeLink {
			shape: shape.clone(),
			suppressed,
			caller,
			unit: UnitId(id),
		});
		let built = match &shape {
			// A read is a statement of its own, which serves the reads of
			// records by id it makes, all of them records of its table.
			CascadeShape::Read {
				table,
			} => {
				let known = std::collections::BTreeSet::from([table.clone()]);
				crate::exec::vm::host::out_of_line(|| {
					super::lower_knowing(&planner, &physical, known)
				})
				.await
			}
			_ => {
				let mut arena = self.take_arena();
				let built = crate::exec::vm::host::out_of_line(|| {
					lower_cascade_in(
						&planner,
						&physical,
						&mut arena,
						caller,
						reach,
						self.edges_enclosing,
					)
				})
				.await;
				self.put_arena(arena);
				built
			}
		};
		// A statement that does not lower leaves no unit, and no link to one,
		// behind it.
		let mut program = match built {
			Ok(program) => program,
			Err(refused) => {
				self.rewind_arena(id as usize);
				return Err(refused);
			}
		};
		self.delegate_context = self.delegate_context.max(program.required_context);
		program.unit = Some(UnitHead {
			sig: UnitSig {
				params: Vec::new(),
				takes_row: false,
			},
			frame: Self::statement_frame(&planner),
			entry: None,
			closure: None,
		});
		self.units[id as usize] = Arc::new(program);
		Ok(UnitId(id))
	}

	/// A `THEN` repair's expression as a host pipeline over the referencing
	/// record's id, lowered from its logical form with `planner`: a statement
	/// a program of the VM's own, run as a subquery, and any other expression
	/// the pipeline's instructions. An expression with no logical form is not
	/// covered.
	async fn custom_clause(
		&mut self,
		planner: &Planner<'ctx>,
		expr: &crate::expr::Expr,
	) -> Lowered<u16> {
		let logical = crate::expr::logical::lower_expr(expr.clone().into())
			.map_err(|e| NotCovered::internal(format!("a THEN repair has no logical form: {e}")))?;
		crate::exec::vm::host::out_of_line(|| {
			self.clause_in(planner, None, ClauseCode::Expr(&logical), false)
		})
		.await
	}
}

/// The statement `shape` is, the names it binds per run, and the tables its
/// records belong to where the shape knows them.
fn statement_of(shape: &CascadeShape) -> Lowered<(LogicalOp, Vec<&'static str>, Option<Reach>)> {
	use crate::expr::{Expr, Param};
	let built = |op: Result<LogicalOp, crate::expr::ControlFlow>| {
		op.map_err(|_| NotCovered::internal("a cascade statement has no logical form"))
	};
	let referencing = || Expr::Param(Param::from(PURGE_REFERENCING.to_owned()));
	let only =
		|table: &TableName| Some(Reach::Tables(std::collections::BTreeSet::from([table.clone()])));
	Ok(match shape {
		CascadeShape::Edges => (built(purge::purge_edges_statement())?, vec![PURGE_VERTEX], None),
		CascadeShape::Delete {
			table,
		} => (built(purge::delete_of(referencing()))?, vec![PURGE_REFERENCING], only(table)),
		CascadeShape::Read {
			table,
		} => (purge::select_all_of(referencing()), vec![PURGE_REFERENCING], only(table)),
		CascadeShape::Unset {
			table,
			field,
		} => (
			built(purge::update_of(referencing(), purge::unset_data(field)))?,
			vec![PURGE_REFERENCING, PURGE_DELETED],
			only(table),
		),
		CascadeShape::Subtract {
			table,
			field,
		} => (
			built(purge::update_of(referencing(), purge::subtract_data(field)))?,
			vec![PURGE_REFERENCING, PURGE_DELETED],
			only(table),
		),
	})
}

/// The edge cascade `op` binds `names` per run of, lowered by `planner` as a
/// bundle: a unit holding an arena of its own, whose entry, the statement, is
/// the arena's first unit. The statement and the cascades beneath it are units
/// of that arena alone, so an edge whose delete cascades to edges of its own
/// closes on the entry.
async fn edge_bundle(
	planner: &Planner<'_>,
	op: LogicalOp,
	names: &[&'static str],
	caller: bool,
) -> Lowered<Program> {
	let bound = crate::exec::vm::host::out_of_line(|| planner.bind_scoped(op, names))
		.await
		.map_err(|e| NotCovered::internal(format!("a cascade statement did not bind: {e}")))?;
	let physical = super::super::optimize::optimize(super::super::map::map(planner, &bound).await?);
	let frame = Lowering::statement_frame(planner);
	let mut arena = super::function::Arena::default();
	// The slot is held while the statement lowers, so a cascade reaching it
	// from inside finds it.
	arena.units.push(Arc::new(Lowering::new(planner).finish()?));
	arena.cascades.push(CascadeLink {
		shape: CascadeShape::Edges,
		suppressed: planner.perms_suppressed,
		caller,
		unit: UnitId(0),
	});
	let mut statement = crate::exec::vm::host::out_of_line(|| {
		lower_edge_statement(planner, &physical, &mut arena, caller)
	})
	.await?;
	statement.unit = Some(UnitHead {
		sig: UnitSig {
			params: Vec::new(),
			takes_row: false,
		},
		frame: frame.clone(),
		entry: None,
		closure: None,
	});
	arena.units[0] = Arc::new(statement);
	let mut holder = Lowering::new(planner);
	holder.units = arena.units;
	let mut bundle = holder.finish()?;
	bundle.unit = Some(UnitHead {
		sig: UnitSig {
			params: Vec::new(),
			takes_row: false,
		},
		frame,
		entry: Some(UnitId(0)),
		closure: None,
	});
	Ok(bundle)
}

/// `plan`, a statement a purge performs, as a unit of `arena`, the arena of
/// the statement being lowered: its stages and the cascades beneath them are
/// units of the same arena, and the program holds none of its own. The
/// statement is the one group of the program, so a run is made over a state
/// kept from one run to the next.
async fn lower_cascade_in<'c>(
	planner: &Planner<'c>,
	plan: &super::PhysicalPlan,
	arena: &mut super::function::Arena<'c>,
	caller_suppressed: bool,
	reach: Option<Reach>,
	edges_enclosing: Option<bool>,
) -> Lowered<Program> {
	let mut l = Lowering::new(planner);
	l.reach_override = reach;
	l.edges_enclosing = edges_enclosing;
	lower_cascade_with(l, planner, plan, arena, caller_suppressed).await
}

/// The edge cascade `plan` as the entry of `arena`, the arena of its bundle:
/// a statement lowered as [`lower_cascade_in`] lowers it, whose write, which
/// may reach any table, loads each edge table's stages by name, whose own
/// edge cascades are this one.
async fn lower_edge_statement<'c>(
	planner: &Planner<'c>,
	plan: &super::PhysicalPlan,
	arena: &mut super::function::Arena<'c>,
	caller_suppressed: bool,
) -> Lowered<Program> {
	let mut l = Lowering::new(planner);
	l.stage_sets = true;
	l.edge_statement = true;
	lower_cascade_with(l, planner, plan, arena, caller_suppressed).await
}

/// [`lower_cascade_in`] by the lowering `l`.
async fn lower_cascade_with<'c>(
	mut l: Lowering<'_, 'c>,
	planner: &Planner<'c>,
	plan: &super::PhysicalPlan,
	arena: &mut super::function::Arena<'c>,
	caller_suppressed: bool,
) -> Lowered<Program> {
	crate::exec::compiles::note();
	l.ahead = true;
	l.caller_suppressed = caller_suppressed;
	l.put_arena(std::mem::take(arena));
	let grouped = crate::exec::vm::host::out_of_line(|| l.group(planner, plan)).await;
	*arena = l.take_arena();
	let (_, access_mode, _) = grouped?;
	l.scalar = true;
	l.writes = access_mode != crate::exec::AccessMode::ReadOnly
		|| l.groups.iter().any(|group| group.write.is_some());
	l.finish()
}
