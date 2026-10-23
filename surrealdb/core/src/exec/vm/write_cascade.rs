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
//! A field's repair is fixed when the statement is lowered
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

// Instructions the IR lowering emits.
#![allow(dead_code)]

use std::sync::Arc;

use surrealdb_types::ToSql;

use super::{PooledState, State, internal, run_image};
use crate::catalog::{Error as CatalogError, Permission};
use crate::doc::Error as DocError;
use crate::err::EngineError;
use crate::exec::operators::write::scope::KeptContexts;
use crate::exec::operators::write::stages::purge;
use crate::exec::permission::action_is_checked;
use crate::exec::vm::program::{Label, Program, UnitId};
use crate::exec::{Error as ExecError, ExecutionContext, FlowResult};
use crate::expr::{ControlFlow, Idiom};
use crate::iam::Action;
use crate::key::schema::ReferenceKey;
use crate::val::{RecordId, TableName, Value};

/// An instruction of a delete's cascades.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::exec::vm) enum ReferenceInstr {
	/// Delete the records joined to the deleted record by a graph edge, as the
	/// purge's edge cascade deletes them: statement `unit`, run with the
	/// deleted record bound, in the frame that restores the actor's own
	/// permissions. `None` means the stages this instruction belongs to are run
	/// by an edge cascade, which their arena does not hold. Such stages run
	/// only beneath an edge cascade.
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
	/// Keep the lists of the written table's `MATERIALIZE INTO` fields in step with the record in
	/// flight: with the change from its before-image to its after-image, or, when `release`,
	/// with its removal.
	Materialize {
		steps: Arc<[MaterializeStep]>,
		release: bool,
	},
}

assert_plain!(ReferenceInstr);

/// The maintenance of the list one `MATERIALIZE INTO` field keeps.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::exec::vm) struct MaterializeStep {
	/// The referencing field of the written table.
	pub(in crate::exec::vm) field: Idiom,
	/// The referenced table that holds the list.
	pub(in crate::exec::vm) table: TableName,
	/// The statement that judges the clause's `WHERE` condition on an image of the record, when
	/// the clause has one.
	pub(in crate::exec::vm) filter: Option<UnitId>,
	/// The statement that adds the record to a list, or takes it out.
	pub(in crate::exec::vm) write: UnitId,
}

assert_plain!(MaterializeStep);

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
	/// Number of cascade statements this thread has run as units. Tests
	/// read it to confirm that a cascade ran as bytecode.
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
			| ReferenceInstr::Materialize {
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
		ReferenceInstr::Materialize {
			steps,
			release,
		} => {
			materialize(program, steps, *release, state).await?;
			return Ok(next);
		}
		ReferenceInstr::Drain {
			..
		}
		| ReferenceInstr::Next {
			..
		}
		| ReferenceInstr::Release => {}
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
			crate::exec::vm::fuel::tick(ctx.exec(), 1).await?;
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
		}
		| ReferenceInstr::Materialize {
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
		let frame = caller.with_params([(purge::PURGE_VERTEX, vertex)]);
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
	// The stages named here belong to the cascade they run in, so their
	// own edge cascades are that cascade.
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
	// A key naming a field the table does not hold fails with the
	// catalog's own field lookup error.
	let (_, strategy) =
		repairs.fields.iter().find(|(name, _)| *name == field).ok_or_else(|| {
			ControlFlow::Err(anyhow::anyhow!(CatalogError::FdNotFound {
				name: field.clone(),
			}))
		})?;
	// The id to name in any error this repair raises is the caller's to see. A
	// caller whose reads are permission checked is shown an id only for a record
	// it can read, taken here as one whose table grants `SELECT` in full: a table
	// that hides rows (`FOR select NONE`, or a row predicate) may hide this
	// record, and a bare-table delete or a cascade can reach a record the caller
	// never selected, so its id is withheld rather than crossing the SELECT
	// boundary. A caller whose reads are not checked (a system or root actor) is
	// shown every id. The gate is the principal's role and level alone
	// ([`principal_reads_checked`]), never the frame's enforcement state.
	let checked = principal_reads_checked(ctx.exec())?;
	let shown = if checked {
		table_grants_full_select(ctx.exec(), &rid.table).await?.then(|| rid.to_sql())
	} else {
		Some(rid.to_sql())
	};
	// A UNIQUE-index conflict a repair's write raises is redacted before
	// `purge::wrap` stringifies it into the referential failure: the wrapper
	// embeds the inner error's text and drops the typed `IndexExists`, so the
	// stored record and its indexed value would otherwise leak to a caller whose
	// reads are permission checked (GHSA-cff7-q66j-4hc8). The caller's own status
	// decides it, taken from the write boundary rather than this suppressed
	// cascade frame, which holds whether the caller's reads are checked however
	// deep the repair runs beneath events or other cascades.
	let hide = ctx.exec().caller_view_checked();
	match strategy {
		RepairStrategy::Ignore => Ok(()),
		RepairStrategy::Reject => {
			// The referencing record is always withheld from a permission-checked
			// caller: its existence is the integrity signal, its id is not. The
			// deleted record is named only when `shown` carries it. A caller whose
			// reads are not checked sees both specific ids.
			let err = if checked {
				DocError::DeleteRejectedByHiddenReference(shown)
			} else {
				DocError::DeleteRejectedByReference(rid.to_sql(), referencing.to_sql())
			};
			Err(ControlFlow::Err(anyhow::anyhow!(err)))
		}
		RepairStrategy::Cascade {
			unit,
		} => {
			let frame = ctx.exec().for_reference_cascade().with_params([
				(purge::PURGE_REFERENCING, Value::from(referencing)),
				(purge::PURGE_DELETED, Value::from(rid.clone())),
			]);
			run_statement(program, *unit, frame, state)
				.await
				.map(|_| ())
				.map_err(|err| purge::wrap(shown, super::hide_conflict_record(err, hide)))
		}
		RepairStrategy::Unset {
			field,
			read,
			unset,
			subtract,
		} => unset_link(program, [*read, *unset, *subtract], field, &rid, &referencing, state)
			.await
			.map_err(|err| purge::wrap(shown, super::hide_conflict_record(err, hide))),
		RepairStrategy::Custom {
			clause,
		} => custom(program, *clause, &rid, referencing, shown, state).await,
	}
}

/// Whether the principal a repair's error reaches has its reads permission
/// checked, by its role and actor level alone ([`action_is_checked`]).
///
/// The frame's enforcement state is deliberately not consulted. A repair can
/// run beneath a frame that suppresses enforcement for the definer's own work —
/// an ON DELETE CASCADE, an ON DELETE THEN clause, a `DEFINE EVENT` body — and
/// the error it raises still propagates to the session's principal, so whether
/// that principal may see an id is a property of the principal, not of the
/// frame. An `AUTH LIMIT` on the way can only narrow the principal, and so can
/// only withhold more.
fn principal_reads_checked(exec: &ExecutionContext) -> FlowResult<bool> {
	let db = exec.database().map_err(|e| ControlFlow::Err(e.into()))?;
	Ok(action_is_checked(
		exec.auth_enabled(),
		exec.auth(),
		Action::View,
		Some(db.ns_name()),
		Some(db.db_name()),
	))
}

/// Whether `table` grants `SELECT` in full, so a permission-checked caller may
/// read every record it holds and naming one in an error discloses nothing it
/// could not already see. A table that hides rows (`FOR select NONE`, or a row
/// predicate) does not qualify: this is a table-level test, so a record a row
/// predicate might admit is treated conservatively as unreadable rather than
/// evaluated here, and its id is withheld.
async fn table_grants_full_select(exec: &ExecutionContext, table: &TableName) -> FlowResult<bool> {
	let db = exec.database().map_err(|e| ControlFlow::Err(e.into()))?;
	let def = db.get_table_def(table, None).await.map_err(ControlFlow::Err)?;
	Ok(def.is_some_and(|def| matches!(def.permissions.select, Permission::Full)))
}

/// Take the deleted record `rid` out of `field` of `referencing`.
///
/// Which statement applies depends on what the field holds, and the field's
/// declared kind does not settle it, because rows written before a kind change keep their old
/// shape. So the referencing record is read and the shape decided per record. A
/// field holding nothing needs no repair; any shape but a link and a
/// collection of links fails, since the reference key names a field
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
		.with_params([(purge::PURGE_REFERENCING, Value::from(referencing.clone()))]);
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
		let frame = frame.with_params([(purge::PURGE_DELETED, Value::from(rid.clone()))]);
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
	// A run that changed the catalog does not return its state to the pool.
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

/// Run a `THEN` repair: host pipeline `clause`, with the referencing record's
/// id as the current value, `$reference` the deleted record, and one level
/// deeper. The repair is a user-supplied expression, so it runs under the
/// deleting caller's own permissions
/// ([`ExecutionContext::with_caller_perms`]), not the referential-integrity
/// suppression the cascade mechanics run under: the record cleanup those
/// mechanics perform is system-internal, the expression is not. A `RETURN` out
/// of it finishes the repair; a `BREAK` or `CONTINUE` has no loop to leave.
async fn custom(
	program: &Program,
	clause: u16,
	rid: &RecordId,
	referencing: RecordId,
	shown: Option<String>,
	state: &mut State,
) -> FlowResult<()> {
	let exec = held_exec(state)?;
	// A UNIQUE-index conflict the clause raises is redacted before `purge::wrap`
	// stringifies it, so the stored record and its indexed value do not leak to a
	// caller whose reads are permission checked (GHSA-cff7-q66j-4hc8). The
	// caller's own status decides it, taken from the write boundary rather than
	// this suppressed cascade frame.
	let hide = exec.caller_view_checked();
	let frame = exec
		.with_caller_perms()
		.with_params([(purge::REFERENCE, Value::from(rid.clone()))])
		.dive()
		.map_err(|err| purge::wrap(shown.clone(), ControlFlow::Err(err)))?;
	let ran = run_image(program, clause, Value::from(referencing), Some(frame), state, &exec).await;
	match ran {
		Ok(_) | Err(ControlFlow::Return(_)) => Ok(()),
		Err(ControlFlow::Break | ControlFlow::Continue) => Err(purge::wrap(
			shown,
			ControlFlow::Err(anyhow::Error::new(ExecError::InvalidControlFlow)),
		)),
		Err(failure) => Err(purge::wrap(shown, super::hide_conflict_record(failure, hide))),
	}
}

/// What one `MATERIALIZE INTO` field of the record in flight needs done, read off the record before
/// the statements that do it run.
struct Pending {
	step: usize,
	previous: Value,
	current: Value,
	/// The images the clause's condition is judged on, when it has one: the record as it was and
	/// as it is, each absent where the record was not or is no longer there.
	before: Option<Value>,
	after: Option<Value>,
}

/// Keep the lists of the `MATERIALIZE INTO` fields of the record in flight in step with it.
///
/// A record is in the list of the record it references while its condition holds, so a field's
/// list changes by the difference between the records the before-image listed itself with and
/// the ones the after-image does. That is one-sided in each direction: a record leaves the lists
/// it was in and no longer belongs to, and joins the ones it was not in. A deleted record
/// (`release`) leaves every list it was in. An import restores the lists with the records they
/// belong to, and a record deleted by the cascade of the delete of the record whose list it is in
/// leaves nothing to keep.
async fn materialize(
	program: &Program,
	steps: &[MaterializeStep],
	release: bool,
	state: &mut State,
) -> FlowResult<()> {
	let write =
		state.write.as_deref_mut().ok_or_else(|| internal("a write body with no write state"))?;
	let (row, ctx) = write.in_flight()?;
	let exec = ctx.exec().clone();
	if exec.options().is_some_and(|options| options.import) {
		return Ok(());
	}
	let child = deleted(row)?;
	let creating = row.initial.is_none();
	if !release && row.initial == row.current {
		return Ok(());
	}
	let mut pending = Vec::new();
	for (at, step) in steps.iter().enumerate() {
		let (previous, current) = if release {
			(row.current.pick(&step.field), Value::None)
		} else {
			(row.initial.pick(&step.field), row.current.pick(&step.field))
		};
		if step.filter.is_none() && previous == current {
			continue;
		}
		let (before, after) = match (step.filter, release) {
			(None, _) => (None, None),
			(Some(_), true) => (Some(row.current.clone()), None),
			(Some(_), false) => {
				((!creating).then(|| row.initial.clone()), Some(row.current.clone()))
			}
		};
		pending.push(Pending {
			step: at,
			previous,
			current,
			before,
			after,
		});
	}
	let deleting = exec.ctx().value(purge::PURGE_DELETED).cloned();
	for work in pending {
		let step = &steps[work.step];
		let listed = |value: &Value| -> Vec<RecordId> {
			let mut ids: Vec<&RecordId> = if let Value::RecordId(id) = value {
				vec![id]
			} else if let Value::Array(items) = value {
				items.iter().filter_map(|v| v.as_record()).collect()
			} else if let Value::Set(items) = value {
				items.iter().filter_map(|v| v.as_record()).collect()
			} else {
				Vec::new()
			};
			ids.retain(|id| id.table == step.table);
			ids.sort();
			ids.dedup();
			ids.into_iter().cloned().collect()
		};
		let was = match (step.filter, work.before) {
			(Some(unit), Some(image)) => {
				if judged(program, unit, &exec, image, state).await? {
					listed(&work.previous)
				} else {
					Vec::new()
				}
			}
			(Some(_), None) => Vec::new(),
			(None, _) => listed(&work.previous),
		};
		let now = match (step.filter, work.after) {
			(Some(unit), Some(image)) => {
				if judged(program, unit, &exec, image, state).await? {
					listed(&work.current)
				} else {
					Vec::new()
				}
			}
			(Some(_), None) => Vec::new(),
			(None, _) => listed(&work.current),
		};
		let left: Vec<&RecordId> = was.iter().filter(|id| !now.contains(id)).collect();
		let joined: Vec<&RecordId> = now.iter().filter(|id| !was.contains(id)).collect();
		for (adding, ids) in [(false, left), (true, joined)] {
			for parent in ids {
				if deleting.as_ref().is_some_and(|deleted| deleted == &Value::from(parent.clone()))
				{
					continue;
				}
				let frame = exec.with_perms_suppressed(true).with_params([
					(crate::exec::materialize::PARENT, Value::from(parent.clone())),
					(crate::exec::materialize::CHILD, Value::from(child.clone())),
					(crate::exec::materialize::ADD, Value::Bool(adding)),
					crate::exec::materialize::mark(),
				]);
				run_statement(program, step.write, frame, state).await?;
			}
		}
	}
	Ok(())
}

/// Whether the condition of a `MATERIALIZE INTO` clause, statement `unit`, holds for `image`.
async fn judged(
	program: &Program,
	unit: UnitId,
	exec: &ExecutionContext,
	image: Value,
	state: &mut State,
) -> FlowResult<bool> {
	let frame = exec
		.with_perms_suppressed(true)
		.with_params([(crate::exec::materialize::IMAGE, image)])
		.refusing_writes(crate::dbs::NoWriteFrame::ComputedField);
	let rows = run_statement(program, unit, frame, state).await.map_err(|err| {
		if let ControlFlow::Err(err) = err {
			ControlFlow::Err(match err.downcast::<ExecError>() {
				Ok(err) => {
					if let ExecError::SchemaClauseEffect {
						construct,
						..
					} = err
					{
						ExecError::SchemaClauseEffect {
							clause: "MATERIALIZE INTO".to_owned(),
							construct,
						}
						.into()
					} else {
						err.into()
					}
				}
				Err(err) => err,
			})
		} else {
			err
		}
	})?;
	Ok(if let Value::Array(rows) = &rows {
		rows.0.first().is_some_and(Value::is_truthy)
	} else {
		rows.is_truthy()
	})
}
