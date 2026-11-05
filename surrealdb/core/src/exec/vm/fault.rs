//! Compile-on-miss paths for the run side: the units a closure invocation or
//! a table read needs that the compile cache does not hold.
//!
//! An instruction that passes a closure value to a callee links the closure's
//! unit first ([`super::interp::Step::NeedUnit`], [`super::closure::link`]).
//! The unit is loaded from the compile cache, keyed by the closure as written
//! and the frame of the invocation ([`serve_unit`]). On a miss, the host
//! compiles the body for that frame (its transaction, namespace and database,
//! principal class, permission contract and `VERSION` stamp) from a statement
//! that builds the literal ([`lower_closure`]). The cache stores the unit with
//! the catalog state the compile read. A body that lowers to no unit is stored
//! with the failure each invocation reports: the compile error if compilation
//! fails, otherwise the reason the VM does not run the body.
//!
//! A read of a record or table loads the table's read unit from the compile
//! cache by name ([`super::dispatch::Deref`]). If no unit is stored for the
//! table's catalog version, or the read is at a `VERSION` stamp, the unit is
//! compiled here ([`link_read`]) for the frame the program started in, the
//! same way a plan made there would be. A table read that does not lower
//! breaks an invariant: every table read the planner reaches has a read unit
//! the VM lowers.
//!
//! The backfill of a view loads the order of the view table's rows from the
//! compile cache by the table's catalog version ([`view_rows`]): the statement
//! that defines the view changes that catalog before the backfill runs, so no
//! plan made before the run can hold the order. On a miss the order is
//! compiled here for the frame the backfill runs in.
//!
//! These are the only compiles a running program causes. Each one is counted
//! once by [`crate::exec::compiles::serving_fault`].

use std::sync::Arc;

use super::closure::{depth_of, names_of};
use crate::exec::catalog_deps::CatalogDeps;
use crate::exec::planner::Planner;
use crate::exec::vm::calls::CompiledClosure;
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;

/// The unit of `closure`'s body for values that capture `captured` and the row
/// captures `rows`. The body is compiled for the frame of an invocation in
/// `exec`, over its transaction and at its `VERSION` stamp
/// ([`super::compile::closure_body`]). If no unit is lowered for the frame, the
/// result is the failure every invocation of the body reports. The catalog
/// state the compile read is returned too, where it can be stored with the unit
/// ([`super::closure::link`]). Returns an error if the compile breaks an
/// invariant of every program, or if `exec` holds no transaction.
pub(super) async fn lower_closure(
	closure: Arc<crate::expr::logical::LogicalClosure>,
	captured: Vec<String>,
	rows: super::program::RowBits,
	depth: u32,
	exec: &ExecutionContext,
) -> FlowResult<(Result<super::units::ClosureUnit, surrealdb_types::Error>, Option<CatalogDeps>)> {
	let Some(txn) = exec.ctx().try_tx().map(Arc::clone) else {
		return Err(internal("a closure body lowered with no transaction".to_owned()));
	};
	tracing::trace!("closure unit compile");
	let (ns, db) = names_of(exec);
	// The captured names are bound where the value runs, so none is folded.
	let planner = Planner::with_txn(exec.ctx(), exec.function_registry(), Arc::clone(&txn), ns, db)
		.with_depth(depth)
		.with_auth(Arc::clone(&exec.root().auth))
		.with_perms_suppressed(exec.root().perms_suppressed || exec.root().skip_fetch_perms)
		.at_stamp(exec.version_stamp())
		.reading_session_values()
		.shadowing(captured.iter().cloned());
	let recording = txn.record_catalog_reads();
	let lowered = super::host::out_of_line(|| {
		super::compile::closure_body(&planner, &closure, &captured, rows)
	})
	.await;
	let reads = recording.finish();
	let unit = match lowered {
		Ok(unit) => Ok(unit),
		Err(refused) if refused.kind == super::NotCoveredKind::Internal => {
			return Err(internal(refused.reason));
		}
		Err(refused) => Err(failure_of(&planner, &closure, refused).await),
	};
	let deps = match &unit {
		Ok(_) if !txn.speculating() => CatalogDeps::resolve(&txn, &reads).await,
		// A body with no lowered unit has no catalog dependencies.
		Err(_) => Some(CatalogDeps::default()),
		_ => None,
	};
	Ok((unit, deps))
}

/// The failure every invocation of `closure`'s body reports when `planner`
/// lowers no unit of it, for `refused`. It is the error the planner reports
/// when the body fails to compile. Otherwise it is the error for a statement
/// the VM does not run ([`super::declined`]).
async fn failure_of(
	planner: &Planner<'_>,
	closure: &crate::expr::logical::LogicalClosure,
	refused: super::NotCovered,
) -> surrealdb_types::Error {
	let args = closure.args.iter().map(|(arg, _)| arg.as_str().to_owned());
	let compiled = planner.shadowing(args).scalar_metadata(closure.body.clone()).await;
	let error = match compiled {
		Err(e) => anyhow::Error::new(e),
		Ok(_) => anyhow::Error::new(super::declined(refused)),
	};
	crate::err::anyhow_to_types_error(error)
}

/// The re-entry depth `closure`'s body is lowered at for an invocation in
/// `exec`. A closure that dives ([`CompiledClosure::dives`]) uses the depth the
/// frame has spent of its budget. Any other closure is lowered at depth 0, so
/// all depths share one unit.
fn unit_depth(closure: &CompiledClosure, exec: &ExecutionContext) -> u32 {
	if closure.dives() {
		depth_of(exec)
	} else {
		0
	}
}

/// The compile cache key for the unit of `closure`'s body, for values that
/// capture `captured` and the row captures `rows`, in an invocation in `exec`.
/// The key holds the closure, which the query text carries, and the depth the
/// body is lowered at ([`super::compile_cache::UnitObject::Closure`]). Returns
/// `None` where `exec` runs in no database.
pub(super) fn unit_key(
	closure: &CompiledClosure,
	captured: &[String],
	rows: super::program::RowBits,
	exec: &ExecutionContext,
) -> Option<super::compile_cache::UnitKey> {
	use super::compile_cache::{BuildId, ObjectVersion, UnitKey, UnitObject};
	let db_ctx = exec.database().ok()?;
	Some(UnitKey {
		ns: db_ctx.ns_name().to_owned(),
		db: db_ctx.db_name().to_owned(),
		ids: (db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id),
		object: UnitObject::Closure {
			closure: super::compile_cache::ClosureId {
				closure: Arc::clone(closure.logical()),
				digest: closure.digest(),
			},
			captured: captured.to_vec(),
			rows,
			depth: unit_depth(closure, exec),
		},
		version: ObjectVersion::Inline,
		build: BuildId::current(exec.ctx().get_capabilities()),
	})
}

/// The unit of `closure`'s body for values that capture `captured` and hold
/// the row captures of `closure`, for an invocation in `exec` over `txn` in
/// `frame`. The unit comes from the compile cache when one is stored for the
/// transaction. On a miss it is compiled ([`lower_closure`]) and stored. If no
/// unit is lowered for the frame, the result is the failure every invocation
/// reports. A speculating transaction, a transaction with no database, and an
/// invocation at a `VERSION` stamp compile the unit for that invocation only.
pub(super) async fn serve_unit(
	closure: &CompiledClosure,
	captured: &[String],
	frame: &super::program::UnitFrame,
	exec: &ExecutionContext,
	txn: &Arc<crate::kvs::Transaction>,
) -> FlowResult<Result<super::units::ClosureUnit, surrealdb_types::Error>> {
	let rows = closure.rows();
	let compile = || {
		crate::exec::compiles::serving_fault(lower_closure(
			Arc::clone(closure.logical()),
			captured.to_vec(),
			rows,
			unit_depth(closure, exec),
			exec,
		))
	};
	let cache = exec
		.ctx()
		.compiled_units()
		.filter(|_| !txn.speculating() && exec.version_stamp().is_none());
	match (cache, unit_key(closure, captured, rows, exec)) {
		(Some(cache), Some(key)) => {
			crate::exec::compiles::seam(cache.closure_units().get_or_compile(
				&key,
				frame,
				txn,
				exec.ctx(),
				|| async {
					let (unit, deps) = compile().await?;
					let deps = deps.map(|deps| (deps, super::units::ContextDeps::default()));
					Ok::<_, ControlFlow>((unit, deps))
				},
			))
			.await
		}
		_ => Ok(compile().await?.0),
	}
}

/// The bundle of the stages that `set`, a write that may reach any table,
/// applies to the records of `table`, for the frame `exec` runs in. The bundle
/// is served from the compile cache under the table's catalog version. On a
/// miss it is compiled and stored. A table the catalog does not define takes
/// the generic bundle. A table with no catalog version, or a speculating
/// transaction, compiles the bundle for this run only.
pub(super) async fn serve_stage_bundle(
	set: &Arc<super::units::StageSetUnit>,
	table: &surrealdb_strand::TableName,
	exec: &ExecutionContext,
) -> FlowResult<Arc<super::program::Program>> {
	use super::compile_cache::{BuildId, ObjectVersion, UnitKey, UnitObject};
	use crate::catalog::providers::TableProvider;
	let txn = exec
		.ctx()
		.try_tx()
		.map(Arc::clone)
		.ok_or_else(|| internal("a write's stages loaded with no transaction".to_owned()))?;
	let db_ctx = exec.database().map_err(|e| ControlFlow::Err(e.into()))?;
	let (ns, db) = (db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id);
	let (defined, version) = crate::exec::compiles::seam(async {
		let defined = txn.get_tb(ns, db, table, None).await?.is_some();
		let version = txn.catalog_version(ns, db, table).await?;
		Ok::<_, anyhow::Error>((defined, version))
	})
	.await
	.map_err(ControlFlow::Err)?;
	let generic = !defined;
	#[cfg(test)]
	STAGE_FAULTS.with(|faults| faults.set(faults.get() + 1));
	let frame = super::closure::frame_of(exec);
	// When the frame suppresses enforcement for an actor that keeps its own, as the delete of a
	// referencing record does, the stages are lowered for that actor. The cascades the stages
	// trigger restore that actor's enforcement.
	let root = exec.root();
	let actor = (root.caller_perms_suppressed != root.perms_suppressed)
		.then_some(root.caller_perms_suppressed);
	let compile = || {
		crate::exec::compiles::serving_fault(async {
			let (names_ns, names_db) = names_of(exec);
			// The bundle is planned for the frame it is kept under. It serves every run of the
			// write, so a name its statement reads is read when the stages run rather than
			// folded from this run's context.
			let planner = Planner::with_txn(
				exec.ctx(),
				exec.function_registry(),
				Arc::clone(&txn),
				names_ns,
				names_db,
			)
			.with_auth(Arc::clone(&exec.root().auth))
			.with_perms_suppressed(frame.perms_suppressed)
			.reading_session_values()
			.shadowing(set.param_names());
			let recording = (!txn.speculating()).then(|| txn.record_catalog_reads());
			let lowered = crate::exec::vm::host::out_of_line(|| {
				super::compile::compile_stage_bundle(&planner, &set.write, table, actor)
			})
			.await;
			let reads = recording.map(|recording| recording.finish());
			let program = lowered.map_err(|refused| match (&refused.refusal, refused.kind) {
				(Some(refusal), _) => ControlFlow::Err(anyhow::Error::new(refusal.error())),
				(None, super::NotCoveredKind::Internal) => internal(refused.reason),
				(None, _) => ControlFlow::Err(anyhow::Error::new(super::declined(refused))),
			})?;
			// The run indexes the bundle's pipelines by the sites of the write that loads it.
			if program.pipelines.len() != set.sites.len() {
				return Err(internal(
					"the stages of a table that do not number the sites of their write".to_owned(),
				));
			}
			let deps = match reads {
				Some(reads) => match CatalogDeps::resolve(&txn, &reads).await {
					Some(deps) => super::units::ContextDeps::resolve(
						&txn,
						&reads,
						planner.frozen(),
						Default::default(),
					)
					.await
					.map(|context| {
						(deps, context.without(crate::exec::planner::UNIT_RUNTIME_PARAMS))
					}),
					None => None,
				},
				None => None,
			};
			Ok::<_, ControlFlow>((Arc::new(program), deps))
		})
	};
	let cache = exec.ctx().compiled_units().filter(|_| !txn.speculating());
	match (cache, version) {
		(Some(cache), Some(version)) if !generic => {
			let key = UnitKey {
				ns: db_ctx.ns_name().to_owned(),
				db: db_ctx.db_name().to_owned(),
				ids: (ns, db),
				object: UnitObject::StageBundle {
					set: Arc::clone(set),
					table: table.clone(),
					generic,
					actor,
				},
				version: ObjectVersion::Stamped(version),
				build: BuildId::current(exec.ctx().get_capabilities()),
			};
			let compiled = std::sync::atomic::AtomicBool::new(false);
			let served = crate::exec::compiles::seam(cache.stage_bundles().get_or_compile(
				&key,
				&frame,
				&txn,
				exec.ctx(),
				|| {
					compiled.store(true, std::sync::atomic::Ordering::Relaxed);
					compile()
				},
			))
			.await;
			note_catalog_unit(compiled.load(std::sync::atomic::Ordering::Relaxed));
			served
		}
		_ => {
			note_catalog_unit(true);
			Ok(compile().await?.0)
		}
	}
}

/// The order of view table `table`'s rows, for the frame `exec` runs in, from
/// the definitions its transaction holds: the program a view's maintenance runs
/// each row it settled through, one pipeline per action
/// ([`super::compile::compile_view_rows`]). The backfill of a view whose table
/// only the run names takes it here, and the backfill of a view a statement
/// defines takes it here too, because the statement changes the catalog the
/// order is read from before the backfill runs. The order is served from the
/// compile cache under the table's catalog version, and compiled and stored on
/// a miss. A table with no catalog version, a speculating transaction, or a
/// read at a `VERSION` stamp compiles the order for this run only.
pub(super) async fn view_rows(
	table: &surrealdb_strand::TableName,
	exec: &ExecutionContext,
) -> FlowResult<Arc<super::program::Program>> {
	use super::compile_cache::{BuildId, ObjectVersion, UnitKey, UnitObject};
	let txn = exec
		.ctx()
		.try_tx()
		.map(Arc::clone)
		.ok_or_else(|| internal("a view's order lowered with no transaction".to_owned()))?;
	let db_ctx = exec.database().map_err(|e| ControlFlow::Err(e.into()))?;
	let (ns, db) = (db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id);
	let version = crate::exec::compiles::seam(txn.catalog_version(ns, db, table))
		.await
		.map_err(ControlFlow::Err)?;
	let frame = super::closure::frame_of(exec);
	let compile = || {
		crate::exec::compiles::serving_fault(async {
			let (names_ns, names_db) = names_of(exec);
			let planner = Planner::with_txn(
				exec.ctx(),
				exec.function_registry(),
				Arc::clone(&txn),
				names_ns,
				names_db,
			)
			.with_auth(Arc::clone(&exec.root().auth))
			.with_perms_suppressed(frame.perms_suppressed)
			.reading_session_values();
			let recording = (!txn.speculating()).then(|| txn.record_catalog_reads());
			let lowered = crate::exec::vm::host::out_of_line(|| {
				super::compile::compile_view_rows(&planner, table)
			})
			.await;
			let reads = recording.map(|recording| recording.finish());
			let program = lowered.map_err(|refused| match (&refused.refusal, refused.kind) {
				(Some(refusal), _) => ControlFlow::Err(anyhow::Error::new(refusal.error())),
				(None, super::NotCoveredKind::Internal) => internal(refused.reason),
				(None, _) => ControlFlow::Err(anyhow::Error::new(super::declined(refused))),
			})?;
			let deps = match reads {
				Some(reads) => match CatalogDeps::resolve(&txn, &reads).await {
					Some(deps) => super::units::ContextDeps::resolve(
						&txn,
						&reads,
						planner.frozen(),
						Default::default(),
					)
					.await
					.map(|context| {
						(deps, context.without(crate::exec::planner::UNIT_RUNTIME_PARAMS))
					}),
					None => None,
				},
				None => None,
			};
			Ok::<_, ControlFlow>((Arc::new(program), deps))
		})
	};
	let cache = exec
		.ctx()
		.compiled_units()
		.filter(|_| !txn.speculating() && exec.version_stamp().is_none());
	match (cache, version) {
		(Some(cache), Some(version)) => {
			let key = UnitKey {
				ns: db_ctx.ns_name().to_owned(),
				db: db_ctx.db_name().to_owned(),
				ids: (ns, db),
				object: UnitObject::ViewRows {
					table: table.clone(),
				},
				version: ObjectVersion::Stamped(version),
				build: BuildId::current(exec.ctx().get_capabilities()),
			};
			let compiled = std::sync::atomic::AtomicBool::new(false);
			let served = crate::exec::compiles::seam(cache.stage_bundles().get_or_compile(
				&key,
				&frame,
				&txn,
				exec.ctx(),
				|| {
					compiled.store(true, std::sync::atomic::Ordering::Relaxed);
					compile()
				},
			))
			.await;
			note_catalog_unit(compiled.load(std::sync::atomic::Ordering::Relaxed));
			served
		}
		_ => {
			note_catalog_unit(true);
			Ok(compile().await?.0)
		}
	}
}

/// Count one catalog unit this thread compiled, or took from the compile
/// cache, for tests that count them ([`super::units::CATALOG_UNITS`]).
fn note_catalog_unit(compiled: bool) {
	#[cfg(test)]
	super::units::CATALOG_UNITS.with(|units| {
		let (c, l) = units.get();
		units.set(if compiled {
			(c + 1, l)
		} else {
			(c, l + 1)
		});
	});
	#[cfg(not(test))]
	let _ = compiled;
}

/// The planner the read unit of a table is compiled with, for a program that
/// started in `start`. It uses the frame of `start` at its `VERSION` stamp.
/// Permission checks are waived where `unchecked` is set and the body is
/// checked for the principal. Otherwise the principal's reads are checked, as
/// the units the program's plan asked for were compiled. Returns `None` where
/// `start` has no transaction to compile against.
fn read_planner(
	start: &ExecutionContext,
	unchecked: bool,
	stamp: Option<u64>,
) -> Option<Planner<'_>> {
	let txn = start.ctx().try_tx().map(Arc::clone)?;
	let (ns, db) = names_of(start);
	let mut planner = Planner::with_txn(start.ctx(), start.function_registry(), txn, ns, db)
		.with_auth(Arc::clone(&start.root().auth))
		.with_perms_suppressed(start.root().perms_suppressed)
		.reading_session_values();
	// A read at a `VERSION` stamp is compiled from the definitions the stamp
	// holds, and the compile cache keeps no unit of it.
	if let Some(stamp) = stamp {
		planner.plan_time_version = crate::exec::planner::PlanTimeVersion::At(stamp);
	}
	Some(if unchecked && planner.body_is_checked(planner.auth.as_ref()) {
		planner.waiving_permissions()
	} else {
		planner
	})
}

fn internal(why: String) -> ControlFlow {
	ControlFlow::Err(anyhow::Error::new(crate::err::Error::Engine(
		crate::err::EngineError::Internal(why),
	)))
}

/// The unit that reads `table` for a program that started in `start`, waiving
/// permission checks where `unchecked` is set, for a read in the frame `reader`
/// runs in. It is compiled in one fault where the compile cache holds no unit
/// ([`super::dispatch::table_unit`]). Returns `None` where the table is not
/// defined or no planner resolves the read's database. In that case the
/// record-fetch helpers of `operators::fetch` make the read. A refusal from the
/// planner becomes an error that names the refusal. A table read that does not
/// lower is an internal error.
pub(super) async fn link_read(
	start: &ExecutionContext,
	reader: &ExecutionContext,
	table: &surrealdb_strand::TableName,
	unchecked: bool,
) -> FlowResult<Option<Arc<super::program::Program>>> {
	use crate::catalog::providers::TableProvider;
	crate::exec::compiles::serving_fault(async {
		tracing::trace!(%table, unchecked, "table read fault");
		#[cfg(test)]
		READ_FAULTS.with(|faults| faults.set(faults.get() + 1));
		let Some(planner) = read_planner(start, unchecked, reader.version_stamp()) else {
			return Ok(None);
		};
		let (Some((ns, db)), Some(txn)) = (planner.ns_db_ids().await, planner.txn()) else {
			return Ok(None);
		};
		if txn.get_tb(ns, db, table, None).await.map_err(ControlFlow::Err)?.is_none() {
			return Ok(None);
		}
		super::dispatch::table_unit(&planner, table).await.map(Some).map_err(
			|refused| match refused.refusal {
				Some(refusal) => ControlFlow::Err(anyhow::Error::new(refusal.error())),
				None => ControlFlow::Err(anyhow::Error::new(super::declined(refused))),
			},
		)
	})
	.await
}

#[cfg(test)]
thread_local! {
	/// Counts the table-read faults on this thread, whether they were served
	/// from the node's units or lowered. Tests use it to tell table-read
	/// faults from closure faults.
	pub(super) static READ_FAULTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	/// Counts the stage bundles of a write that may reach any table loaded on this thread for a
	/// table its records were found in.
	pub(super) static STAGE_FAULTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
