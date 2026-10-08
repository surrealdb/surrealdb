//! The host's side of a delete's cascades run as instructions.
//!
//! A deleted record's cascades are the records joined to it by an edge, which
//! are deleted, and the records referencing it by a `REFERENCE` field, each
//! repaired as the field's `ON DELETE` clause directs. The edge cascade is one
//! statement the host runs. The references are the body's own loop: the keys
//! naming the record are drained before any repair, each is taken in turn as
//! the referencing record in flight, and the repair its field declares runs;
//! the keys are removed once every one is repaired.
//!
//! What a field's repair is, was decided when the statement was lowered
//! ([`TableRepairs`]): nothing for `IGNORE`, a refusal for `REJECT`, the
//! referencing record deleted for `CASCADE`, the link taken out for `UNSET`,
//! and for `THEN` the definition's own expression as a host pipeline,
//! evaluated with the referencing record's id as the current value. The
//! statements the repairs run are units of the program; the host binds the
//! record each is about and runs it in the frame its repair calls for, one
//! level deeper than the delete. A failed repair is reported as the
//! referential failure it is, naming the deleted record.
//!
//! A key naming a table the repairs do not hold is an internal error: the
//! repairs are every table of the database with a field that can name the
//! record's table, as the catalog held them when the statement was lowered. A
//! key naming a field of such a table that it does not hold fails as the
//! catalog's lookup of the field does.

use std::sync::Arc;

use surrealdb_types::ToSql;

use super::{PooledState, State, internal, run_image};
use crate::catalog::Error as CatalogError;
use crate::doc::Error as DocError;
use crate::err::EngineError;
use crate::exec::operators::write::scope::KeptContexts;
use crate::exec::operators::write::stages::purge;
use crate::exec::vm::program::{Label, Program, UnitId};
use crate::exec::{Error as ExecError, ExecutionContext, FlowResult};
use crate::expr::{ControlFlow, Idiom};
use crate::key::schema::ReferenceKey;
use crate::val::{RecordId, TableName, Value};

/// An instruction of a delete's cascades.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::exec::vm) enum ReferenceInstr {
	/// Delete the records joined to the deleted record by a graph edge, as the
	/// purge's edge cascade deletes them: statement `unit`, run with the
	/// deleted record bound, in the frame that restores the actor's own
	/// permissions. `None` names the edge cascade running the stages the
	/// instruction is one of, which their arena does not hold: the stages an
	/// edge cascade loads by name, run only beneath it.
	PurgeEdges {
		unit: Option<UnitId>,
	},
	/// Drain the reference keys naming the deleted record, when a field of the
	/// database can target its table; jump to `target` when there are none.
	Drain {
		target: Label,
	},
	/// Take the next drained key's record as the referencing record in
	/// flight, or jump to `target` once every key is taken.
	Next {
		target: Label,
	},
	/// Repair the referencing record in flight as its field declares.
	Repair {
		tables: Arc<[TableRepairs]>,
	},
	/// Remove the drained reference keys.
	Release,
}

assert_plain!(ReferenceInstr);

/// The repairs of one referencing table: for each of its `REFERENCE` fields
/// that can name a record of the table being deleted from, by the name a
/// reference key carries for the field (`SyncReferences` writes it), its
/// strategy.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::exec::vm) struct TableRepairs {
	pub(in crate::exec::vm) table: TableName,
	pub(in crate::exec::vm) fields: Vec<(String, RepairStrategy)>,
}

assert_plain!(TableRepairs);

/// What a `REFERENCE` field's `ON DELETE` clause does to a referencing record.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::exec::vm) enum RepairStrategy {
	/// Leave the reference as it is: a dangling link is what the schema
	/// asked for.
	Ignore,
	/// Refuse the delete: the referencing record holds on to the record.
	Reject,
	/// Delete the referencing record: statement `unit`, run in the frame that
	/// suppresses enforcement for that record while keeping the actor's own
	/// for whatever the delete triggers.
	Cascade {
		unit: UnitId,
	},
	/// Take the deleted record out of the referencing record's `field`. The
	/// record is read by `read`; a field holding a link is updated by `unset`
	/// and one holding a collection by `subtract`, which removes the deleted
	/// record from it. A field holding nothing needs no repair.
	Unset {
		field: Idiom,
		read: UnitId,
		unset: UnitId,
		subtract: UnitId,
	},
	/// Run the `THEN` expression of host pipeline `clause`, with the
	/// referencing record's id as the current value and `$reference` the
	/// deleted record.
	Custom {
		clause: u16,
	},
}

assert_plain!(RepairStrategy);

#[cfg(test)]
thread_local! {
	/// How many statements of a cascade this thread has run as units, for
	/// tests that must know a cascade ran as bytecode.
	pub(in crate::exec::vm) static CASCADE_UNITS_RUN: std::cell::Cell<usize> =
		const { std::cell::Cell::new(0) };
}

impl ReferenceInstr {
	/// The jump target the instruction holds, for the lowering to patch.
	pub(in crate::exec::vm) fn target_mut(&mut self) -> Option<&mut Label> {
		match self {
			ReferenceInstr::Drain {
				target,
			}
			| ReferenceInstr::Next {
				target,
			} => Some(target),
			ReferenceInstr::PurgeEdges {
				..
			}
			| ReferenceInstr::Repair {
				..
			}
			| ReferenceInstr::Release => None,
		}
	}
}

/// The reference keys of the record in flight's delete, drained, and the one
/// being repaired.
pub(super) struct References {
	range: surrealdb_kvs::key::TypedRange<()>,
	keys: std::vec::IntoIter<ReferenceKey<'static>>,
	current: Option<ReferenceKey<'static>>,
}

/// Take `instr` for the record in flight, and answer the instruction the body
/// continues at.
pub(super) async fn step(
	program: &Program,
	instr: &ReferenceInstr,
	next: usize,
	state: &mut State,
) -> FlowResult<usize> {
	match instr {
		ReferenceInstr::PurgeEdges {
			unit,
		} => {
			purge_edges(program, *unit, state).await?;
			return Ok(next);
		}
		ReferenceInstr::Repair {
			tables,
		} => {
			repair(program, tables, state).await?;
			return Ok(next);
		}
		_ => {}
	}
	let write =
		state.write.as_deref_mut().ok_or_else(|| internal("a write body with no write state"))?;
	let (row, ctx) = write.in_flight()?;
	let rid = deleted(row)?;
	match instr {
		ReferenceInstr::Drain {
			target,
		} => {
			let Some((range, keys)) = purge::referencing_keys(&ctx, &rid).await? else {
				return Ok(*target as usize);
			};
			write.references = Some(Box::new(References {
				range,
				keys: keys.into_iter(),
				current: None,
			}));
		}
		ReferenceInstr::Next {
			target,
		} => {
			yield_now!();
			let references = references(write)?;
			references.current = references.keys.next();
			if references.current.is_none() {
				return Ok(*target as usize);
			}
		}
		ReferenceInstr::Release => {
			let references =
				write.references.take().ok_or_else(|| internal("references released twice"))?;
			ctx.txn().delr(references.range).await.map_err(ControlFlow::Err)?;
		}
		ReferenceInstr::PurgeEdges {
			..
		}
		| ReferenceInstr::Repair {
			..
		} => return Err(internal("a cascade step taken out of order")),
	}
	Ok(next)
}

/// The record whose delete the cascades are of.
fn deleted(row: &crate::exec::operators::write::row::WriteRow) -> FlowResult<RecordId> {
	row.id.clone().ok_or_else(|| {
		ControlFlow::Err(anyhow::Error::new(EngineError::unreachable(
			"Expected a document id to be present",
		)))
	})
}

fn references(write: &mut super::WriteState) -> FlowResult<&mut References> {
	write.references.as_deref_mut().ok_or_else(|| internal("a reference taken before a drain"))
}

/// The referencing record in flight.
fn referencing(write: &mut super::WriteState) -> FlowResult<RecordId> {
	let key = references(write)?
		.current
		.as_ref()
		.ok_or_else(|| internal("a repair with no referencing record"))?;
	Ok(RecordId {
		table: key.foreign_table.clone().into_owned(),
		key: key.foreign_key.clone().into_owned(),
	})
}

/// Delete the records joined to the record in flight by an edge, when it has
/// any: statement `unit` over the record as the vertex, in the frame that
/// restores the actor's own permissions.
async fn purge_edges(program: &Program, unit: Option<UnitId>, state: &mut State) -> FlowResult<()> {
	let write =
		state.write.as_deref_mut().ok_or_else(|| internal("a write body with no write state"))?;
	let (row, ctx) = write.in_flight()?;
	let rid = deleted(row)?;
	if !purge::has_edges(&ctx, &rid).await? {
		return Ok(());
	}
	let caller = ctx.exec().with_caller_perms();
	let vertex = Value::from(rid);
	let Some(unit) = unit else {
		// The cascade running these stages, which the context names.
		let cascade = caller
			.vm_edge_cascade()
			.cloned()
			.ok_or_else(|| internal("an edge cascade's stages run beneath no edge cascade"))?;
		let frame = caller.with_param(purge::PURGE_VERTEX, vertex);
		let exec = frame.dive().map_err(ControlFlow::Err)?;
		let key = (Arc::as_ptr(&cascade).cast::<()>() as usize, UnitId(u32::MAX));
		let admitted = state.write.as_deref().is_some_and(|write| write.admitted.contains(&key));
		if !admitted {
			super::super::host::admitted_unit(std::slice::from_ref(&cascade), UnitId(0), &exec)?;
			if let Some(write) = state.write.as_deref_mut() {
				write.admitted.push(key);
			}
		}
		let units = Arc::clone(&cascade.units);
		return run_found(cascade, units, key, exec, state).await.map(|_| ());
	};
	// The cascade is named to the stages it runs, whose own edge cascades
	// are it.
	let units = super::super::host::unit_table(program, state, ctx.exec())?;
	let cascade = units
		.get(unit.0 as usize)
		.cloned()
		.ok_or_else(|| internal("an edge cascade the table does not hold"))?;
	let frame = caller.with_param_in_edge_cascade(purge::PURGE_VERTEX, vertex, cascade);
	run_statement(program, unit, frame, state).await.map(|_| ())
}

/// Repair the referencing record in flight as its field's strategy directs.
async fn repair(program: &Program, tables: &[TableRepairs], state: &mut State) -> FlowResult<()> {
	let write =
		state.write.as_deref_mut().ok_or_else(|| internal("a write body with no write state"))?;
	let (row, ctx) = write.in_flight()?;
	let rid = deleted(row)?;
	let referencing = referencing(write)?;
	let field = references(write)?
		.current
		.as_ref()
		.map(|key| key.foreign_field.clone().into_owned())
		.ok_or_else(|| internal("a repair with no referencing record"))?;
	let repairs =
		tables.iter().find(|repairs| repairs.table == referencing.table).ok_or_else(|| {
			internal(&format!("a referencing table outside the repairs: {}", referencing.table))
		})?;
	// A key naming a field the table does not key that way is the failure the
	// catalog's own lookup of it reports.
	let (_, strategy) =
		repairs.fields.iter().find(|(name, _)| *name == field).ok_or_else(|| {
			ControlFlow::Err(anyhow::anyhow!(CatalogError::FdNotFound {
				name: field.clone(),
			}))
		})?;
	match strategy {
		RepairStrategy::Ignore => Ok(()),
		RepairStrategy::Reject => Err(ControlFlow::Err(anyhow::anyhow!(
			DocError::DeleteRejectedByReference(rid.to_sql(), referencing.to_sql())
		))),
		RepairStrategy::Cascade {
			unit,
		} => {
			let frame = ctx
				.exec()
				.for_reference_cascade()
				.with_param(purge::PURGE_REFERENCING, Value::from(referencing));
			run_statement(program, *unit, frame, state)
				.await
				.map(|_| ())
				.map_err(|err| purge::wrap(&rid, err))
		}
		RepairStrategy::Unset {
			field,
			read,
			unset,
			subtract,
		} => unset_link(program, [*read, *unset, *subtract], field, &rid, &referencing, state)
			.await
			.map_err(|err| purge::wrap(&rid, err)),
		RepairStrategy::Custom {
			clause,
		} => custom(program, *clause, &rid, referencing, state).await,
	}
}

/// Take the deleted record `rid` out of `field` of `referencing`.
///
/// Which statement that is depends on what the field holds, and the field's
/// declared kind does not settle it: a redefined kind leaves old rows behind.
/// So the referencing record is read and the shape decided per record. A
/// field holding nothing needs no repair; any shape but a link and a
/// collection of links is the refusal that the reference key names a field
/// whose value cannot hold one.
async fn unset_link(
	program: &Program,
	[read, unset, subtract]: [UnitId; 3],
	field: &Idiom,
	rid: &RecordId,
	referencing: &RecordId,
	state: &mut State,
) -> FlowResult<()> {
	let exec = held_exec(state)?;
	let frame = exec
		.with_perms_suppressed(true)
		.with_param(purge::PURGE_REFERENCING, Value::from(referencing.clone()));
	let rows = run_statement(program, read, frame.clone(), state).await?;
	let Value::Array(rows) = rows else {
		return Err(internal("a read of the referencing record that answered no list"));
	};
	let Some(row) = rows.0.into_iter().next() else {
		return Ok(());
	};
	let held = row.pick(field);
	if matches!(held, Value::RecordId(_)) {
		run_statement(program, unset, frame, state).await.map(|_| ())
	} else if matches!(held, Value::Array(_) | Value::Set(_)) {
		let frame = frame.with_param(purge::PURGE_DELETED, Value::from(rid.clone()));
		run_statement(program, subtract, frame, state).await.map(|_| ())
	} else if matches!(held, Value::None) {
		Ok(())
	} else {
		Err(ControlFlow::Err(anyhow::anyhow!(EngineError::unreachable(format!(
			"Expected either a record id, array, set or none, found {}",
			held.to_sql()
		)))))
	}
}

/// The execution context of the write in flight.
fn held_exec(state: &mut State) -> FlowResult<ExecutionContext> {
	let write =
		state.write.as_deref_mut().ok_or_else(|| internal("a write body with no write state"))?;
	let (_, ctx) = write.in_flight()?;
	Ok(ctx.exec().clone())
}

/// Run statement `unit` of the program's units in `frame`, one level deeper
/// than the frame the delete runs in, and answer its rows, or its one value
/// when its value is one row.
///
/// The depth is the budget `max_computation_depth` bounds a cascade by, however
/// its hops are split between the engines. The unit runs on the frame stack of
/// the statement's driver, so a cascade many levels deep does not hold its
/// levels on the native stack. It runs only in a frame that matches the one it
/// was lowered for.
async fn run_statement(
	program: &Program,
	unit: UnitId,
	frame: ExecutionContext,
	state: &mut State,
) -> FlowResult<Value> {
	let exec = frame.dive().map_err(ControlFlow::Err)?;
	let units = super::super::host::unit_table(program, state, &exec)?;
	let key = (super::arena_of(&units), unit);
	let admitted = state.write.as_deref().is_some_and(|write| write.admitted.contains(&key));
	let statement = if admitted {
		let held = units.get(unit.0 as usize);
		Arc::clone(held.ok_or_else(|| internal("a cascade statement the table does not hold"))?)
	} else {
		let statement = Arc::clone(super::super::host::admitted_unit(&units, unit, &exec)?);
		if let Some(write) = state.write.as_deref_mut() {
			write.admitted.push(key);
		}
		statement
	};
	run_found(statement, units, key, exec, state).await
}

/// Run `statement`, a unit of `units` admitted for the frame `exec` runs in
/// and kept under `key`, as [`run_statement`] runs it.
async fn run_found(
	statement: Arc<Program>,
	units: super::super::UnitTable,
	key: (usize, UnitId),
	exec: ExecutionContext,
	state: &mut State,
) -> FlowResult<Value> {
	#[cfg(test)]
	CASCADE_UNITS_RUN.with(|runs| runs.set(runs.get() + 1));
	// A unit that holds an arena of its own runs the statement it names in
	// that arena; a statement that holds no arena of its own finds the units
	// its ids name in the table it was found in.
	let (statement, units) = match statement.unit.as_ref().and_then(|head| head.entry) {
		Some(entry) => {
			let body = statement
				.units
				.get(entry.0 as usize)
				.ok_or_else(|| internal("a cascade bundle that holds no statement"))?;
			(Arc::clone(body), Arc::clone(&statement.units))
		}
		None => (statement, units),
	};
	let exec = if statement.units.is_empty() {
		exec.with_vm_units(units)
	} else {
		exec
	};
	if statement.groups.first().is_some_and(|group| group.write.is_some()) {
		return run_write(statement, key, exec, state).await.map(|()| Value::None);
	}
	crate::exec::trampoline::nest(super::super::host::gather_program(statement, exec)).await
}

/// The states a write statement of a cascade runs over, kept between its
/// runs: a run takes one, so the write state it holds keeps the stage programs
/// and catalog reads of earlier records, and a run reached while another is in
/// flight, around a cycle, takes a state of its own. The states are kept only
/// while the transaction's catalog is unchanged.
pub(super) struct CascadePool {
	/// The statement's unit, by the arena it is a unit of and its id there.
	unit: (usize, UnitId),
	/// The transaction's catalog stamp the states were kept under.
	stamp: Option<u64>,
	states: Vec<PooledState>,
	/// The write contexts every state's write shares, so a level a cascade
	/// reaches first takes what another level resolved.
	contexts: Arc<KeptContexts>,
}

/// Run `statement`, the write statement unit `unit` of its arena is, in `exec`
/// over a state kept from an earlier run when there is one.
async fn run_write(
	statement: Arc<Program>,
	unit: (usize, UnitId),
	exec: ExecutionContext,
	state: &mut State,
) -> FlowResult<()> {
	let stamp = exec.try_txn().map(|txn| txn.catalog_stamps());
	let (kept, contexts) = {
		let write = state
			.write
			.as_deref_mut()
			.ok_or_else(|| internal("a write body with no write state"))?;
		let outer = write.kept_contexts();
		let at = match write.cascades.iter().position(|pool| pool.unit == unit) {
			Some(at) => at,
			None => {
				write.cascades.push(CascadePool {
					unit,
					stamp,
					states: Vec::new(),
					contexts: outer.unwrap_or_default(),
				});
				write.cascades.len() - 1
			}
		};
		let pool = &mut write.cascades[at];
		if pool.stamp != stamp {
			pool.states.clear();
			pool.contexts = Arc::default();
			pool.stamp = stamp;
		}
		(pool.states.pop(), Arc::clone(&pool.contexts))
	};
	let mut running = match kept {
		Some(running) => running,
		None => fresh_state(&statement, &exec, contexts)?,
	};
	running.permissions_bypassed = exec.root().skip_fetch_perms;
	let caller = exec.clone();
	let (running, ran) = crate::exec::trampoline::nest(async move {
		let (_, ran) = super::super::host::run_group(&statement, 0, &mut running, &exec).await;
		(running, ran)
	})
	.await;
	ran?;
	// A run that changed the catalog leaves its state behind.
	let after = caller.try_txn().map(|txn| txn.catalog_stamps());
	if after == stamp
		&& let Some(write) = state.write.as_deref_mut()
		&& let Some(pool) = write.cascades.iter_mut().find(|pool| pool.unit == unit)
		&& pool.stamp == stamp
	{
		pool.states.push(running);
	}
	Ok(())
}

/// A state for a run of `statement`, whose write keeps what it resolves in
/// `contexts`.
fn fresh_state(
	statement: &Program,
	exec: &ExecutionContext,
	contexts: Arc<KeptContexts>,
) -> FlowResult<PooledState> {
	let mut running = super::super::host::statement_state(statement, exec)?;
	if let Some(spec) = statement.groups.first().and_then(|group| group.write.as_ref()) {
		let mut write = Box::new(super::WriteState::new(spec, exec)?);
		write.keeping_in(contexts);
		write.group = Some(0);
		running.group_writes[0] = Some(write);
	}
	Ok(running)
}

/// Run a `THEN` repair: host pipeline `clause` running it, in the frame the
/// purge evaluates it in — permissions off, `$reference` the deleted record,
/// one level deeper — with the referencing record's id as the current value. A
/// `RETURN` out of it finishes the repair; a `BREAK` or `CONTINUE` has no loop
/// to leave.
async fn custom(
	program: &Program,
	clause: u16,
	rid: &RecordId,
	referencing: RecordId,
	state: &mut State,
) -> FlowResult<()> {
	let exec = held_exec(state)?;
	let frame = exec
		.with_perms_suppressed(true)
		.with_param(purge::REFERENCE, Value::from(rid.clone()))
		.dive()
		.map_err(|err| purge::wrap(rid, ControlFlow::Err(err)))?;
	let ran = run_image(program, clause, Value::from(referencing), Some(frame), state, &exec).await;
	match ran {
		Ok(_) | Err(ControlFlow::Return(_)) => Ok(()),
		Err(ControlFlow::Break | ControlFlow::Continue) => Err(purge::wrap(
			rid,
			ControlFlow::Err(anyhow::Error::new(ExecError::InvalidControlFlow)),
		)),
		Err(failure) => Err(purge::wrap(rid, failure)),
	}
}
