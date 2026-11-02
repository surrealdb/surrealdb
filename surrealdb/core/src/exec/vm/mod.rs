//! The bytecode VM that runs a planned statement.
//!
//! A bound [`LogicalOp`] is planned by [`plan_request`](crate::exec::ir::plan_request) into a
//! container past the physical boundary, and [`ir_lower`] lowers that container natively into a
//! [`Program`](program::Program), planning nothing: every decision is an attribute of the
//! physical dialect. The compile driver ([`ir_compile`]) plans and lowers every unit a program
//! needs before the program runs ([`compile`]).
//!
//! The program is a sequence of pipelines, each a synchronous loop over the
//! rows of its source, between the nodes that hold rows — sorts and
//! aggregates. Everything that awaits — reading the next rows from storage,
//! evaluating an expression the VM has no instruction for — is a point where
//! the interpreter hands control back to the async host ([`host`]), which does
//! the work and resumes it. A pipeline with nothing to hand back runs inside
//! the storage cursor, on the stored bytes.
//!
//! The VM is the one engine: a statement whose container holds a construct the
//! planning or the lowering does not cover fails with an error that names the
//! construct ([`plan_checked`]), and nothing else runs it.

#[macro_use]
pub(crate) mod plain;

pub(crate) mod access;
pub(crate) mod calls;
pub(crate) mod degree;
pub(crate) mod matches;
pub(crate) mod subquery;

mod binding;
mod block;
mod call_unit;
pub(crate) mod closure;
mod compile;
pub(crate) mod compile_cache;
mod conjunct;
mod count;
pub(crate) mod descriptor;
pub(crate) mod dispatch;
mod entries;
pub(crate) mod footprint;
mod manifest;
pub(crate) mod serve;
pub(crate) use dispatch::Deref;
pub(crate) use leaf::Class as LeafClass;
pub(crate) use morsel::MorselMetrics;
#[cfg(test)]
pub(crate) use morsel::{note_encoded_ahead, note_txn_read_on_worker, note_write_ended};
pub(crate) use program::ClosureDigest;
pub(crate) use speculate::catalog_within;
pub(crate) mod codec;
pub(crate) mod depth;
pub(crate) mod explain;
mod external;
mod fault;
#[allow(dead_code)]
pub(crate) mod fold_host;
pub(crate) mod fuel;
mod fulltext;
mod function;
mod graph;
mod groups;
mod host;
pub(crate) use host::{out_of_line, out_of_line_boxed};
mod info;
mod inline;
mod interp;
pub(crate) mod ir_catalog;
#[allow(dead_code)]
mod ir_compile;
pub(crate) use ir_compile::PlanSession;
mod ir_exec;
#[allow(dead_code)]
mod ir_host;
mod ir_instr;
#[allow(dead_code)]
pub(crate) mod ir_lower;
mod knn;
mod leaf;
mod ledger;
mod measure;
pub(crate) mod row_frame;
mod row_json;
#[cfg(feature = "shaped")]
mod row_shaped;
pub(crate) use write::{notify_at, notify_unit};
#[cfg(test)]
mod golden;
pub(crate) mod live;
mod membership;
mod nested;
mod omit;
mod param_unit;
pub(crate) mod payload;
pub(crate) mod program;
pub(crate) mod request;
#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod tests_body_scope;
#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod tests_epoch_catalog;
#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod tests_frontier;
#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod tests_id_list;
#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod tests_late_layers;
#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod tests_morsel;
#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod tests_once;
#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod tests_placement;
#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod tests_principal;
#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod tests_range_limit;
#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod tests_read_ahead;
#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod tests_request_wakes;
#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod tests_running_compiles;
#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod tests_stack;
#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod tests_statement_cache;
#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod tests_statement_cache_literals;
#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod tests_statement_cache_writes;
#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod tests_write_fanout;
#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod tests_write_targets;
// The golden is recorded from a build that runs no script and no model.

/// The units of a root program or a function bundle, which the ids of the
/// programs beneath it index.
pub(crate) type UnitTable = Arc<[Arc<program::Program>]>;

/// The late-bound statements of a program, as written, by
/// [`program::Program::served`] index: kept beside the program by the
/// operator that runs it, since a program holds no logical plan.
pub(crate) type ServedStatements = Arc<[Arc<LogicalOp>]>;

mod arena;
mod backfill;
mod batch;
mod distinct_spill;
mod join_spill;
pub(crate) mod morsel;
mod partial;
mod partition;
mod read_ahead;
mod row;
pub(in crate::exec) mod row_packed;
mod scalar;
pub(crate) mod schema_host;
mod services;
mod sort;
pub(crate) mod speculate;
mod spill;
pub(crate) mod store;
#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod tests_enforced;
#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod tests_expression_ranges;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_index_in_place;
#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod tests_link_window;
#[cfg(all(test, feature = "search"))]
mod tests_match_context;
#[cfg(test)]
mod tests_methods;
#[cfg(all(test, feature = "kv-mem"))]
pub(in crate::exec) mod tests_packed_reads;
#[cfg(all(test, feature = "kv-mem"))]
pub(super) mod tests_packed_transcode;
#[cfg(test)]
mod tests_plain;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_record_format;
#[cfg(all(test, feature = "kv-mem", feature = "shaped"))]
mod tests_shaped;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_view_reentry;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_write_prefilter;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_write_scan;
pub(crate) mod units;
mod window;
mod window_step;
mod write;

use std::sync::Arc;

use crate::exec::ExecOperator;
use crate::exec::ir::error::IrError;
use crate::exec::ir::timers::compile_phase;
use crate::exec::planner::Planner;
use crate::expr::logical::LogicalOp;

/// A lowering's answer: the value, or why the VM does not run the statement.
pub(super) type Lowered<T> = Result<T, NotCovered>;

/// The frame a whole statement planned by `planner` is lowered in. Reads are checked or not by
/// the permission ops in its plan, whatever frame the statement runs in. Whether the principal's
/// reads are checked ([`Planner::reads_are_checked`]) is not part of the frame: the frame's
/// principal class, names and suppression already fix it.
pub(super) fn statement_frame(planner: &Planner<'_>) -> program::UnitFrame {
	program::UnitFrame {
		class_digest: planner.class_digest(planner.auth.as_deref()),
		ns: planner.ns().map(str::to_owned),
		db: planner.db().map(str::to_owned),
		perms_suppressed: planner.perms_suppressed,
		checked: true,
	}
}

/// The error a statement fails with when the VM does not run it.
///
/// A decline is a failure: nothing else executes a statement. The error names
/// the construct the lowering did not cover, and is a statement-level
/// `PlannerUnsupported`, not an internal error. A lowering that broke an
/// invariant of every program ([`NotCovered::internal`]) is an internal error,
/// and a statement the planner refuses as it plans it
/// ([`NotCovered::refusal`]) fails with the refusal.
pub(super) fn declined(not_covered: NotCovered) -> crate::err::Error {
	let NotCovered {
		kind,
		reason,
		refusal,
	} = not_covered;
	if kind == NotCoveredKind::Internal {
		return crate::err::Error::Engine(crate::err::EngineError::Internal(reason));
	}
	if let Some(refusal) = refusal {
		return refusal.error();
	}
	tracing::debug!(%reason, "the VM does not run the statement");
	crate::err::Error::Exec(crate::exec::Error::PlannerUnsupported {
		reason,
	})
}

/// Whether `not_covered`, the answer of planning `op` with `planner`, plans `op` again late-bound
/// after its schema statements: a statement holding a schema statement, or reaching code that may
/// run one, whose effect the plan cannot know. What follows such a statement loads each catalog
/// object it names, and resolves the session's names again, when it runs. Nothing is planned once
/// the program runs. An internal refusal is an invariant the planning broke, and a language-level
/// refusal is the statement's error; neither is retried.
fn plans_late(planner: &Planner<'_>, op: &LogicalOp, not_covered: &NotCovered) -> bool {
	!matches!(not_covered.kind, NotCoveredKind::Internal | NotCoveredKind::Refused)
		&& !planner.binds_late_after_schema()
		&& (not_covered.kind == NotCoveredKind::LateBind
			|| crate::exec::planner::fold::changes_the_catalog(op))
}

/// Plan `op`, already bound, to run on the VM, or fail with the reason the VM
/// does not run it.
///
/// `unbound` is the statement `op` was bound from, when it is a block, `FOR`,
/// `LET` or expression the planning may plan again late-bound after its
/// schema statements: each statement it holds is bound again where it is
/// planned, the ones after a change late-bound, from the statement as
/// written. A block a statement of which may change the catalog through code
/// no speculative run models ([`speculate::hides_a_change_point`]) is planned
/// that way from the start: its statements after the change are planned
/// when the block reaches them.
pub(crate) fn plan_checked<'a>(
	planner: &'a Planner<'_>,
	op: &'a LogicalOp,
	unbound: Option<&'a LogicalOp>,
) -> crate::exec::BoxFut<'a, Result<Arc<dyn ExecOperator>, crate::err::Error>> {
	// A boxed `dyn` future: what planning awaits nests deeply, and a caller
	// proving a statement's future `Send` stops at the box.
	Box::pin(async move {
		// Nothing of the explained statement runs under a plain `EXPLAIN`: the
		// ids its planning held are released once it is rendered.
		let explained = if let LogicalOp::Explain(explain) = op
			&& !explain.analyze
		{
			planner.speculation.as_ref().map(|speculating| speculating.held.mark())
		} else {
			None
		};
		if speculate::hides_a_change_point(planner, op).await {
			let late = planner.binding_late_after_schema();
			return plan_statement(&late, unbound.unwrap_or(op)).await.map_err(declined);
		}
		let not_covered = match plan_statement(planner, op).await {
			Ok(plan) => {
				if let (Some(speculating), Some(mark)) = (&planner.speculation, explained) {
					speculating.held.release_since(mark);
				}
				return Ok(plan);
			}
			Err(not_covered) => not_covered,
		};
		if plans_late(planner, op, &not_covered) {
			// What the first planning held belongs to no program.
			if let Some(speculating) = &planner.speculation {
				speculating.held.release_all();
			}
			let late = planner.binding_late_after_schema();
			return plan_statement(&late, unbound.unwrap_or(op)).await.map_err(declined);
		}
		Err(declined(not_covered))
	})
}

/// The statements of a request compiled so far.
#[derive(Default)]
pub(crate) struct RequestBuilder {
	/// Each statement's program, which holds every unit it needs.
	statements: Vec<Arc<program::Program>>,
	/// Each statement's late-bound block statements, as written, which its
	/// blocks serve when they reach them.
	served: Vec<Option<ServedStatements>>,
	/// The catalog ids each statement's planning held, which its run takes.
	held: Vec<Arc<speculate::HeldIds>>,
}

/// Where a statement of a request was compiled to.
pub(crate) enum RequestEntry {
	/// Statement `index` of the request's program.
	Statement(u32),
	/// An operator of its own, planned on a path that lowers no statement
	/// into the request.
	Operator(Arc<dyn ExecOperator>),
}

impl RequestBuilder {
	/// Plan `op`, bound by `planner`, as the next statement of the request,
	/// as [`plan_checked`] plans a statement on its own.
	pub(crate) async fn plan(
		&mut self,
		planner: &Planner<'_>,
		op: &LogicalOp,
		unbound: Option<&LogicalOp>,
	) -> Result<RequestEntry, crate::err::Error> {
		// A plain `EXPLAIN` answers with the rendering of another statement's
		// plan, which its own operator makes.
		if matches!(op, LogicalOp::Explain(_)) {
			return plan_checked(planner, op, unbound).await.map(RequestEntry::Operator);
		}
		if speculate::hides_a_change_point(planner, op).await {
			let late = planner.binding_late_after_schema();
			return self
				.plan_statement(&late, unbound.unwrap_or(op))
				.await
				.map(RequestEntry::Statement)
				.map_err(declined);
		}
		let not_covered = match self.plan_statement(planner, op).await {
			Ok(index) => return Ok(RequestEntry::Statement(index)),
			Err(not_covered) => not_covered,
		};
		if plans_late(planner, op, &not_covered) {
			if let Some(speculating) = &planner.speculation {
				speculating.held.release_all();
			}
			let late = planner.binding_late_after_schema();
			return self
				.plan_statement(&late, unbound.unwrap_or(op))
				.await
				.map(RequestEntry::Statement)
				.map_err(declined);
		}
		Err(declined(not_covered))
	}

	/// Plan and compile `op` into the request, answering its index, or the
	/// reason the VM does not cover it.
	async fn plan_statement(&mut self, planner: &Planner<'_>, op: &LogicalOp) -> Lowered<u32> {
		let mut compiled = compile::compile(planner, op).await?;
		compile::seed_statement(planner, &mut compiled.program).await?;
		if leaf::reporting() {
			leaf::report(op, &compiled.program, planner.function_registry());
		}
		compile_phase!("finish", {
			let Ok(index) = u32::try_from(self.statements.len()) else {
				return Err(NotCovered::internal("more statements than a request holds"));
			};
			self.statements.push(Arc::new(compiled.program));
			self.served.push((!compiled.served.is_empty()).then(|| compiled.served.into()));
			if let Some(speculating) = &planner.speculation
				&& !speculating.held.is_empty()
			{
				self.held.push(Arc::clone(&speculating.held));
			}
			Ok(index)
		})
	}

	/// The program of the request: `steps`, and the statements compiled into
	/// it, each with the units it holds.
	pub(crate) fn finish(
		self,
		steps: Vec<program::RequestStep>,
	) -> Result<request::CompiledRequest, crate::err::Error> {
		let request = program::RequestCode {
			steps,
			statements: self.statements,
		};
		let program =
			ir_lower::request_program(Arc::from(Vec::new()), request).map_err(declined)?;
		#[cfg(test)]
		request::LAST.with(|last| {
			*last.borrow_mut() = program.request.as_ref().map(|request| {
				(request.steps.len(), request.statements.len(), program.units.len())
			});
		});
		Ok(request::CompiledRequest {
			program: Arc::new(program),
			served: self.served,
			held: self.held,
		})
	}
}

/// The refusal of the region-constraint rejection of the bound statement `op`, the statement at
/// `position` of its request; `None` when every region admits what it holds, and when `op` does
/// not bind.
pub(crate) async fn rejection(
	planner: &Planner<'_>,
	op: &LogicalOp,
	position: usize,
) -> Option<payload::Refusal> {
	compile::rejection(planner, op, position).await.ok().flatten()
}

/// Plan and compile the bound statement `op` to an operator, or the reason
/// the VM does not cover it. A statement the planner refuses answers the
/// refusal, before any of it runs.
async fn plan_statement(planner: &Planner<'_>, op: &LogicalOp) -> Lowered<Arc<dyn ExecOperator>> {
	let mut compiled = compile::compile(planner, op).await?;
	compile::seed_statement(planner, &mut compiled.program).await?;
	if leaf::reporting() {
		leaf::report(op, &compiled.program, planner.function_registry());
	}
	tracing::debug!("VM planned the statement");
	Ok(operator(planner, compiled))
}

/// `compiled`, planned by `planner`, as the operator the planner and the executor hold. It keeps
/// the catalog ids the statement's planning held for as long as it or a run of it lasts, since
/// the program's draws take them.
fn operator(planner: &Planner<'_>, compiled: ir_compile::Compiled) -> Arc<dyn ExecOperator> {
	let held = planner
		.speculation
		.as_ref()
		.map(|speculating| &speculating.held)
		.filter(|held| !held.is_empty())
		.cloned();
	Arc::new(host::VmOperator::new(compiled.program).holding(held).serving(compiled.served))
}

/// Plan `op`, a plan a front end emitted rather than a statement, to run on
/// the VM.
///
/// The plan is compiled with no binding step: the front end owns what it
/// asks for, and its tables are read as an unbound statement's are, each row
/// put through the table's enforcement. A GQL `MATCH` is folded in the IR
/// binder to the binding steps of its stages; a relational plan (the SQL
/// front ends') is planned and lowered as a statement is, its joins,
/// `DISTINCT` and set operations among the pipeline's own ops. A plan the VM
/// does not cover fails as a statement it does not cover does, and no other
/// engine compiles it. What the planner refuses as it compiles the plan is
/// raised before any of it runs.
pub(crate) async fn plan_front_end(
	planner: &Planner<'_>,
	op: LogicalOp,
) -> Result<Arc<dyn ExecOperator>, crate::err::Error> {
	plan_statement(planner, &op).await.map_err(declined)
}

/// How a statement the VM does not run is handled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NotCoveredKind {
	/// The statement's plan depends on DDL it holds: it is planned again
	/// late-bound.
	LateBind,
	/// The planner's language-level refusal: the statement fails with
	/// [`NotCovered::refusal`], which is `Some`.
	Refused,
	/// A construct the lowering does not cover: the statement fails with
	/// `PlannerUnsupported`, and is planned again late-bound only where it
	/// changes the catalog.
	Unsupported,
	/// An invariant of every program broke: the statement fails with an
	/// internal error and is never planned again.
	Internal,
}

/// Why the VM does not run a statement. The statement fails with it
/// ([`declined`]), as its [`kind`](NotCoveredKind) says. The reason is message
/// text only: control flow reads the kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NotCovered {
	pub(crate) kind: NotCoveredKind,
	pub(crate) reason: String,
	/// The error the planner raises for a statement it refuses to plan: the
	/// statement fails with it. `Some` exactly when `kind` is
	/// [`NotCoveredKind::Refused`].
	pub(crate) refusal: Option<payload::Refusal>,
}

impl NotCovered {
	/// A statement whose plan depends on DDL it holds, which is planned again
	/// late-bound.
	pub(crate) fn late_bind() -> Self {
		Self {
			kind: NotCoveredKind::LateBind,
			reason: speculate::DATA_DEPENDENT_DDL.to_owned(),
			refusal: None,
		}
	}

	/// A statement the planner refuses to plan, for `refusal`.
	pub(crate) fn refused(refusal: payload::Refusal) -> Self {
		Self {
			kind: NotCoveredKind::Refused,
			reason: "a statement the planner refuses to plan".to_owned(),
			refusal: Some(refusal),
		}
	}

	/// A construct the lowering does not cover, named by `reason`.
	pub(crate) fn unsupported(reason: impl Into<String>) -> Self {
		Self {
			kind: NotCoveredKind::Unsupported,
			reason: reason.into(),
			refusal: None,
		}
	}

	/// A construct whose native planning or lowering is not built, named by
	/// `what`: the statement fails with `PlannerUnsupported`, as one the
	/// lowering does not cover does, and nothing else runs it.
	pub(crate) fn not_yet(what: impl std::fmt::Display) -> Self {
		Self::unsupported(format!("{NOT_YET}: {what}"))
	}

	/// A lowering that broke an invariant of every program, for `reason`.
	pub(crate) fn internal(reason: impl Into<String>) -> Self {
		Self {
			kind: NotCoveredKind::Internal,
			reason: reason.into(),
			refusal: None,
		}
	}
}

/// The text every reason [`NotCovered::not_yet`] gives starts with.
pub(crate) const NOT_YET: &str = "IR entry point not implemented";

/// An IR invariant that broke is an internal outcome, with the error's
/// message as the reason. A request the language rejects is the refusal it
/// carries, which the statement fails with as it does where the planner
/// refuses it; a construct the planner declines to cover is the unsupported
/// outcome with the reason it states, and a construct whose planning or
/// lowering is not built is [`NotCovered::not_yet`]. A plan that depends on a
/// catalog change is the one outcome the executor plans again, late-bound,
/// when the statement is reached.
impl From<IrError> for NotCovered {
	fn from(error: IrError) -> Self {
		match error {
			IrError::Rejected(refusal) => Self::refused(*refusal),
			IrError::Unsupported {
				reason,
			} => Self::unsupported(reason),
			IrError::LateBind {
				..
			} => Self::late_bind(),
			IrError::NotYet {
				what,
			} => Self::not_yet(what),
			other @ (IrError::Stale(_)
			| IrError::Refused(_)
			| IrError::HasUses(_)
			| IrError::TagChanged(_)
			| IrError::Capacity(_)
			| IrError::Verify(_)
			| IrError::NotLoaded {
				..
			}
			| IrError::Invariant {
				..
			}
			| IrError::CatalogUnreadable {
				..
			}) => Self::internal(other.to_string()),
		}
	}
}

/// Invoke a stored function from an API stage through its catalog-stamped VM call unit.
/// The unit applies the function's capability, AUTH LIMIT, argument kinds and permission gate.
pub(crate) async fn invoke_stored_function(
	name: &str,
	depth: u32,
	args: Vec<crate::val::Value>,
	ctx: &crate::exec::EvalContext<'_>,
) -> crate::exec::FlowResult<crate::val::Value> {
	call_unit::call(name, depth, args, ctx, &mut None).await
}
