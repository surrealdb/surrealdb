//! The compile cache's misses on the run side: compiling what a closure
//! invocation or a read of a table needs that the cache does not hold.
//!
//! An instruction that hands a callee a closure value links its unit first
//! ([`super::interp::Step::NeedUnit`], [`super::closure::link`]). The unit is
//! loaded from the compile cache by the closure as written and the frame the
//! invocation runs in ([`serve_unit`]); on a miss the host lowers the body
//! through the physical planner for that frame — its transaction, namespace
//! and database, principal class and permission contract, and the `VERSION`
//! stamp it reads at — into a unit of an arena of its own ([`lower_closure`]),
//! and the cache keeps it with the catalog state the lowering read. A body the
//! host lowers no unit of is kept with the failure every invocation of it
//! reports: the error compiling it reports, where it fails to compile, and
//! otherwise the reason the VM does not run it.
//!
//! A read of a record or a table loads the table's read unit from the compile
//! cache by name ([`super::dispatch::Deref`]); a load that finds no unit kept
//! for the table's catalog version, or a read at a `VERSION` stamp, compiles
//! it here ([`link_read`]) for the frame the program started in, as a plan made
//! there would. A table whose read does not lower is an invariant break: every
//! table read the planner reaches has a read unit the VM lowers.
//!
//! These are the one kind of compile a running program causes: each is
//! counted, once, by [`crate::exec::compiles::serving_fault`].

use std::sync::Arc;

use super::closure::{depth_of, names_of};
use crate::exec::catalog_deps::CatalogDeps;
use crate::exec::planner::Planner;
use crate::exec::vm::calls::CompiledClosure;
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;

/// The unit of `closure`'s body, for values that capture `captured`, lowered
/// for the frame of an invocation in `exec` over its transaction and at its
/// `VERSION` stamp, or the failure every invocation of a body no unit of
/// which is lowered for the frame reports; and the catalog state the
/// lowering read where it can be kept: what the compile cache compiles on a
/// miss ([`super::closure::link`]). An error where the lowering broke an
/// invariant of every program.
pub(super) async fn lower_closure(
	closure: Arc<crate::expr::logical::LogicalClosure>,
	captured: Vec<String>,
	depth: u32,
	exec: &ExecutionContext,
) -> FlowResult<(Result<super::units::ClosureUnit, surrealdb_types::Error>, Option<CatalogDeps>)> {
	let Some(txn) = exec.ctx().try_tx().map(Arc::clone) else {
		return Err(internal("a closure body lowered with no transaction".to_owned()));
	};
	tracing::trace!("closure unit compile");
	let (ns, db) = names_of(exec);
	let planner = Planner::with_txn(exec.ctx(), exec.function_registry(), Arc::clone(&txn), ns, db)
		.with_depth(depth)
		.with_auth(Arc::clone(&exec.root().auth))
		.with_perms_suppressed(exec.root().perms_suppressed || exec.root().skip_fetch_perms)
		.at_stamp(exec.version_stamp())
		.reading_session_values();
	let recording = txn.record_catalog_reads();
	let lowered = crate::exec::vm::host::out_of_line(|| {
		super::lower::lower_closure_body(&planner, &closure, captured)
	})
	.await;
	let reads = recording.finish();
	let unit = match lowered {
		Ok(unit) => Ok(unit),
		Err(refused) if refused.internal => {
			return Err(ControlFlow::Err(anyhow::Error::new(crate::err::Error::Engine(
				crate::err::EngineError::Internal(refused.reason),
			))));
		}
		Err(refused) => Err(failure_of(&planner, &closure, refused).await),
	};
	let deps = match &unit {
		Ok(_) if !txn.speculating() => CatalogDeps::resolve(&txn, &reads).await,
		// A body no unit of which is lowered depends on nothing.
		Err(_) => Some(CatalogDeps::default()),
		_ => None,
	};
	Ok((unit, deps))
}

/// The failure every invocation of `closure`'s body reports where `planner`
/// lowers no unit of it, for `refused`: the error the planner reports
/// compiling the body, where it fails to compile, and otherwise the error a
/// statement the VM does not run fails with ([`super::declined`]).
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
/// `exec`: what the frame has spent of its budget for a call, and that of a
/// statement for a stored clause evaluated with no call made
/// ([`CompiledClosure::dives`]), whose unit every depth shares.
fn unit_depth(closure: &CompiledClosure, exec: &ExecutionContext) -> u32 {
	if closure.dives() {
		depth_of(exec)
	} else {
		0
	}
}

/// The key the compile cache keeps the unit of `closure`'s body, for values
/// that capture `captured`, under for an invocation in `exec`: the closure,
/// which the query text carries, and the depth its body is lowered at
/// ([`super::compile_cache::UnitObject::Closure`]); `None` where `exec` runs
/// in no database.
pub(super) fn unit_key(
	closure: &CompiledClosure,
	captured: &[String],
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
			depth: unit_depth(closure, exec),
		},
		version: ObjectVersion::Inline,
		build: BuildId::current(exec.ctx().get_capabilities()),
	})
}

/// The unit of `closure`'s body, for values that capture `captured`, for an
/// invocation in `exec` over `txn` in `frame`, from the compile cache: one
/// kept that stands for the transaction, or, on a miss, one compiled now
/// ([`lower_closure`]) and kept; or the failure every invocation of a body no
/// unit of which is lowered for the frame reports. A speculating transaction,
/// one in no database, and an invocation read at a `VERSION` stamp compile it
/// for the invocation alone.
pub(super) async fn serve_unit(
	closure: &CompiledClosure,
	captured: &[String],
	frame: &super::program::UnitFrame,
	exec: &ExecutionContext,
	txn: &Arc<crate::kvs::Transaction>,
) -> FlowResult<Result<super::units::ClosureUnit, surrealdb_types::Error>> {
	let compile = || {
		crate::exec::compiles::serving_fault(lower_closure(
			Arc::clone(closure.logical()),
			captured.to_vec(),
			unit_depth(closure, exec),
			exec,
		))
	};
	let cache = exec
		.ctx()
		.compiled_units()
		.filter(|_| !txn.speculating() && exec.version_stamp().is_none());
	match (cache, unit_key(closure, captured, exec)) {
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

/// The bundle of the stages `set`, a write that may reach any table, applies
/// to the records of `table`, for the frame `exec` runs in, from the compile
/// cache: one kept for the table's catalog version that stands for the
/// transaction, or, on a miss, one compiled now
/// ([`super::lower::stage_bundle_for`]) and kept. A table the catalog does
/// not hold takes the generic bundle, and one never stamped, or a speculating
/// transaction, compiles it for the run alone.
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
			.with_perms_suppressed(exec.root().perms_suppressed)
			.reading_session_values();
			match super::lower::stage_bundle_for(&planner, set, table, generic).await {
				Ok((bundle, deps)) => Ok::<_, ControlFlow>((
					bundle,
					deps.map(|(deps, context)| ((*deps).clone(), (*context).clone())),
				)),
				Err(refused) => Err(internal(refused.reason)),
			}
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

/// The order of view table `table`'s rows, lowered for the frame `exec` runs
/// in from the definitions its transaction holds: what the backfill of a view
/// whose table only the run names runs each row it stores through. It is
/// lowered for the backfill alone.
pub(super) async fn view_rows(
	table: &surrealdb_strand::TableName,
	exec: &ExecutionContext,
) -> FlowResult<Arc<super::program::Program>> {
	let txn = exec
		.ctx()
		.try_tx()
		.map(Arc::clone)
		.ok_or_else(|| internal("a view's order lowered with no transaction".to_owned()))?;
	note_catalog_unit(true);
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
		.with_perms_suppressed(exec.root().perms_suppressed)
		.reading_session_values();
		let lowered = async {
			let catalog = Arc::new(
				crate::kvs::catalog_reads::sealed(
					false,
					super::plan_catalog::read_view_table(&planner, table),
				)
				.await?,
			);
			super::lower::view_rows_order(&planner, table.clone(), &catalog).await
		};
		match lowered.await {
			Ok(program) => Ok(Arc::new(program)),
			Err(refused) => Err(internal(refused.reason)),
		}
	})
	.await
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

/// The planner the read unit of a table is compiled with for a program that
/// started in `start`: the frame of `start`, at its `VERSION` stamp, waiving
/// permission checks where
/// `unchecked` and the principal's reads are checked, as the units the
/// program's plan asked for were compiled. `None` where `start` has no
/// transaction to compile against.
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

/// The unit that reads `table` for a program that started in `start`,
/// waiving permission checks where `unchecked`, for a read in the frame
/// `reader` runs in, compiled in one fault where the compile cache held none
/// ([`super::dispatch::table_unit`]). `None` where the table is not defined
/// or no planner resolves the read's database, in which case the read is
/// made by the record-fetch helpers of `operators::fetch`. The error a refusal
/// the planner raises for the read names, and an internal error where the
/// table's read does not lower.
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
				None => internal(refused.reason),
			},
		)
	})
	.await
}

#[cfg(test)]
thread_local! {
	/// How many links of a table's read this thread's runs faulted for, each
	/// served from the node's units or lowered, for tests that tell them from
	/// a closure's faults.
	pub(super) static READ_FAULTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
