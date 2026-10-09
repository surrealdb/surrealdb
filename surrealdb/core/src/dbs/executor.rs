use std::collections::HashMap;
use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use async_channel::Sender;
use futures::{Stream, StreamExt};
use surrealdb_datastore::close::CommitAction;
use surrealdb_types::{Error as TypesError, QueryError, ToSql};
use web_time::Instant;

use crate::catalog::providers::{
	CatalogProvider, DatabaseProvider, NamespaceProvider, RootProvider,
};
use crate::ctx::reason::Reason;
use crate::ctx::{Context, FrozenContext};
use crate::dbs::{
	DefaultBroker, Dispatch, Force, MessageBroker, Options, QueryResult, QueryStreamItem,
	QueryType, RoutedNotification, StatementCounters, TopLevelStatement, items_for_result,
};
use crate::err::{EngineError, Error};
use crate::exec::function::FunctionRegistry;
use crate::exec::planner::plan_statement_inner;
use crate::exec::{Error as ExecError, OutputShape};
use crate::expr::logical::{
	Emission, LogicalExpr, LogicalOp, LogicalSource, MisplacedControl, RequestGrouper, SourceRef,
	StatementPlace, StatementRole, lower,
};
use crate::expr::paths::{DB, NS};
use crate::expr::statements::{OptionStatement, UseStatement};
use crate::expr::visit::{Visit, Visitor};
use crate::expr::{Base, ControlFlow, Expr, FlowResult};
use crate::iam::{Action, ResourceKind};
use crate::kvs::slowlog::{ParamVisitor, SlowLogVisit};
use crate::kvs::{Datastore, DatastoreError, Transaction, TransactionType};
use crate::observe::{
	Outcome, QueryCounters, QueryEvent, QueryEventSafe, StatementEvent, StatementEventCtx,
	StatementEventSafe, StatementType,
};
use crate::rpc::types_error_from_anyhow;
use crate::val::{Array, Value, convert_result_rows_to_public, convert_value_to_public_value};
use crate::{err, sql};

const TARGET: &str = "surrealdb::core::dbs";

/// How many times a bare `LIVE` or `KILL` statement runs, the first attempt
/// included, when it loses a retryable transaction conflict. Both statements
/// read and then write their table's live-query stamp, so two that overlap on
/// one table conflict at commit, and a conflict means the attempt's writes did
/// not apply: running the statement again is safe. The count is bounded so a
/// statement that keeps losing the race still reports the conflict to its
/// caller rather than holding the connection indefinitely.
const LIVE_KILL_CONFLICT_ATTEMPTS: u32 = 5;

/// The wait before each re-run of a bare `LIVE` or `KILL`, multiplied by the
/// number of conflicts the statement has lost. It yields the connection's task
/// so the transaction this attempt lost to, which runs on the same task, gets
/// to commit instead of being raced again immediately.
const LIVE_KILL_CONFLICT_BACKOFF: Duration = Duration::from_millis(1);

/// Classify an [`anyhow::Error`] into one of the bounded `error_class`
/// constants by downcasting to the well-known concrete error types
/// produced by the executor / kvs layers.
///
/// Mapping rules (first match wins):
///
/// - Downcasts to [`crate::kvs::Error`]: retryable variants →
///   [`crate::observe::error_class::TXN_CONFLICT`]; everything else from the kvs layer →
///   [`crate::observe::error_class::STORAGE`].
/// - Downcasts to [`surrealdb_types::Error`]: delegates to
///   [`crate::observe::error_class::classify_types_error`].
/// - Anything else: [`crate::observe::error_class::INTERNAL`].
///
/// Lives in `dbs` rather than the observe crate because it downcasts to
/// [`crate::kvs::Error`], a core-only type the leaf crate cannot depend on.
/// It stays `pub` and re-exported as `crate::observe::classify_anyhow_error`,
/// which is where it used to live: an out-of-tree observer filling the
/// `error_class` attribute has only an `anyhow::Error` to work from, and the
/// alternative is re-deriving the kvs-is-retryable rule that this function
/// exists to keep in one place.
pub fn classify_anyhow_error(err: &anyhow::Error) -> &'static str {
	use crate::observe::error_class::{INTERNAL, STORAGE, TXN_CONFLICT, classify_types_error};
	// Both shapes reach here. A bare `kvs::Error` comes straight from the
	// store; the `err::Error::Kvs`-wrapped one comes from every `Transaction`
	// write, which goes through `map_err(Error::from)`. Matching only the bare
	// one classified a write conflict as `internal`.
	if let Some(kvs_err) = crate::kvs::storage_error(err) {
		if kvs_err.is_retryable() {
			return TXN_CONFLICT;
		}
		return STORAGE;
	}
	if let Some(types_err) = err.downcast_ref::<surrealdb_types::Error>() {
		return classify_types_error(types_err);
	}
	INTERNAL
}

/// Turns the values a plan produced into the statement's result value.
///
/// The plan declares the shape and the number of values that arrived does not
/// get a vote: a consumer that forwards values as they are produced knows the
/// shape long before it knows the count, so the count cannot be what decides.
/// An operator that declares [`OutputShape::Scalar`] and then emits anything
/// but one value has a bug; reporting it is what keeps that bug from reaching
/// the caller as a silently rearranged result.
fn shape_statement_value(shape: OutputShape, mut values: Vec<Value>) -> Result<Value> {
	match shape {
		OutputShape::Scalar if values.len() == 1 => {
			Ok(values.pop().expect("values verified non-empty"))
		}
		OutputShape::Scalar => Err(anyhow!(EngineError::unreachable(format!(
			"A scalar plan produced {} values instead of one",
			values.len()
		)))),
		OutputShape::Rows => Ok(Value::Array(Array::from(values))),
	}
}

/// A batch of rows encoded in `format`.
fn encode_rows(
	format: surrealdb_rpc::RowFormat,
	batch: &crate::exec::ValueBatch,
) -> Result<surrealdb_rpc::EncodedRows> {
	let mut frame = crate::exec::vm::row_frame::RowFrame::with_format(format);
	for row in batch.values() {
		frame.push_value(row)?;
	}
	Ok(frame.take_encoded())
}

/// Where a streaming execution sends its output.
///
/// Only rows stream: a statement's `QueryResult` is one small entry either way,
/// so [`Executor::results`] still accumulates them, and this sends
/// [`QueryStreamItem`]s alongside. That is what keeps the transaction-block
/// bookkeeping — which rewrites already-produced results when a block fails —
/// working unchanged.
///
/// The ordering it maintains is the one the wire requires: a statement's rows
/// go out while it runs, and its `Finished` only once its outcome is final.
///
/// The rows of a held transaction never stream: its statements reach the sink
/// only through [`Executor::finalise_results`], once the transaction resolves.
struct StreamSink {
	items: Sender<QueryStreamItem>,
	/// Statements whose rows have already been sent, and how many went out.
	///
	/// Keyed by statement index rather than held in a single slot because
	/// finalisation can lag several statements behind: inside a
	/// `BEGIN … COMMIT` block every statement's rows go out as it runs, but
	/// none of them is final until the block resolves.
	streamed: HashMap<usize, u64>,
	/// The encoding rows are sent in, as
	/// [`QueryStreamItem::Frame`]s; `None` sends them as values.
	row_format: Option<surrealdb_rpc::RowFormat>,
	/// How many of [`Executor::results`] have been finalised. Results at or
	/// past this index are still provisional — inside an unresolved
	/// `BEGIN … COMMIT` block, they may yet be rewritten into errors.
	finalised: usize,
}

impl StreamSink {
	/// Send one item, ignoring a consumer that has stopped reading.
	///
	/// A closed channel is not an error here: the caller has already been told
	/// to stop by [`Self::is_closed`], and an item produced in the meantime has
	/// nowhere to go.
	async fn send(&self, item: QueryStreamItem) {
		let _ = self.items.send(item).await;
	}

	/// Whether the consumer has gone away.
	fn is_closed(&self) -> bool {
		self.items.is_closed()
	}
}

struct PreparedBroker {
	receiver: async_channel::Receiver<RoutedNotification>,
	delivery: Arc<dyn MessageBroker>,
}

/// Flush a batch's buffered live-query notifications once the caller-owned
/// transaction that produced them commits.
///
/// Only the owner commits such a transaction, so the executor has no commit of
/// its own to flush after. Queued on the transaction, the notifications go out
/// with the commit and are dropped with a cancel or a failed commit.
struct FlushOnCommit(PreparedBroker);

impl CommitAction for FlushOnCommit {
	fn run(self: Box<Self>) -> Pin<Box<dyn Future<Output = ()> + Send>> {
		crate::exec::vm::out_of_line(|| async move {
			Executor::flush_live_query_notifications(self.0);
		})
	}
}

/// Executes the statements of a query, one at a time, on the bytecode VM.
pub struct Executor {
	results: Vec<QueryResult>,
	opt: Options,
	ctx: FrozenContext,
	/// The datastore's function registry. Seated on the `RootContext` of every
	/// statement this executor runs, so the planner and every nested
	/// context derived from it resolve function names through one registry.
	function_registry: Arc<FunctionRegistry>,
	/// Cached session info to avoid re-extracting from context on every query.
	/// Session values don't change between statements in the same executor batch.
	cached_session: Option<Arc<crate::exec::context::SessionInfo>>,
	/// Set when [`Executor::prepare_broker`] installs a [`DefaultBroker`]. Left false when a
	/// broker was already present (higher layer) or this statement skipped installation.
	/// Drives conditional [`clear_broker`] so we never remove an externally supplied broker.
	broker_owned_by_executor: bool,
	/// Present when this execution streams its results.
	stream: Option<StreamSink>,
	/// The output shape of the statement currently being finalised, recorded from
	/// the plan that ran it. It drives how the result is converted to a public
	/// value: a `Rows` result is an array-of-rows envelope whose rows are each
	/// converted as their own root (matching the storage guard and the streaming
	/// read path), a `Scalar` result is converted as a single value. Reset per
	/// statement in `execute_plan_in_transaction` and set in
	/// `execute_operator_plan`; statements that never reach a plan stay `Scalar`.
	result_shape: OutputShape,
	/// The statements of a request that arrived whole, compiled together the
	/// first time one of them is planned; `None` for a streamed request,
	/// whose statements are planned as each is reached.
	request: Option<RequestPlan>,
	/// Whether the statement running is inside `BEGIN … COMMIT`.
	in_block: bool,
	/// The bare `LIVE` statement converted as it arrived, with the unit
	/// compiled for it in a read transaction before its write transaction
	/// opened. [`Executor::run_live`] takes it, so the statement registers
	/// under the id and unit it arrived with; it is empty outside the one
	/// statement that set it.
	arrived_live: Option<(
		crate::expr::statements::LiveStatement,
		Option<crate::exec::vm::lower_live::Arrived>,
	)>,
	/// Why the transaction-control statement about to run cannot act where it
	/// stands, as request lowering marked it. Set by a driver immediately
	/// before it hands the statement to [`Executor::run_surface_statement`]
	/// and taken there; a driver clears it once the statement has run.
	misplaced_control: Option<MisplacedControl>,
	/// The emission request lowering gives the explicit transactions of the
	/// request this executor drives.
	explicit_emission: Emission,
	/// True while a held explicit transaction runs, which is exactly the span of
	/// [`Executor::execute_begin_statement`]. [`Executor::streams_rows`] is
	/// false for as long as it is set.
	holding: bool,
}

/// What a request's statement needs to be compiled with the rest of the
/// request, by its kind as the driver acts on it.
enum RequestEntry {
	Begin,
	Commit,
	Cancel,
	Option(OptionStatement),
	/// `USE`: the namespace and database it names where both are written as
	/// names, and `None` where either is computed when it runs.
	Use(Option<(Option<String>, Option<String>)>),
	/// A top-level `LET`: its name, the kind it declares, and its value,
	/// lowered.
	Let {
		name: String,
		kind: Option<crate::expr::Kind>,
		value: Option<Result<LogicalOp, Error>>,
		read_only: bool,
	},
	/// A statement an engine runs, lowered.
	Run {
		op: Option<Result<LogicalOp, Error>>,
		read_only: bool,
	},
	/// A plan a front end emitted.
	Plan {
		op: Option<LogicalOp>,
		read_only: bool,
	},
	/// `LIVE` or `ACCESS`: the statements after it may meet what it stores.
	Stores,
	/// `KILL` or `SHOW`: nothing a later statement is compiled against.
	Inert,
}

/// How a statement of the request was planned when the request arrived.
enum Planned {
	/// Compiled into the request's program, or to an operator of its own, or
	/// the error compiling it raised.
	Compiled(Result<crate::exec::vm::RequestEntry, Error>),
	/// Served when it is reached, planned for the catalog its transaction
	/// reads then ([`StatementPlan::Served`]).
	Served {
		op: LogicalOp,
		front_end: bool,
	},
	/// Planned when it is reached, for that run alone
	/// ([`StatementPlan::Reached`]).
	Reached {
		op: LogicalOp,
		front_end: bool,
	},
}

/// A request's statement as planned when the request arrived.
struct PlannedStatement {
	entry: Planned,
	/// The plan's logical form, kept for the slow log when one is configured.
	logged: Option<LogicalOp>,
}

impl PlannedStatement {
	/// The statement of the request's program it was lowered to.
	fn index(&self) -> Option<u32> {
		match &self.entry {
			Planned::Compiled(Ok(crate::exec::vm::RequestEntry::Statement(index))) => Some(*index),
			Planned::Compiled(Ok(crate::exec::vm::RequestEntry::Operator(_)) | Err(_))
			| Planned::Served {
				..
			}
			| Planned::Reached {
				..
			} => None,
		}
	}
}

/// How a statement the driver runs is planned.
enum StatementPlan {
	/// Planned already: the operator that runs it.
	Ready(Arc<dyn crate::exec::ExecOperator>),
	/// Planned for the catalog its transaction reads when it runs, served
	/// from the compile cache by its text (`vm::serve`): `op` as written, a
	/// front end's plan where `front_end`.
	Served {
		op: LogicalOp,
		front_end: bool,
	},
	/// Planned when it is reached, for the catalog its transaction reads then
	/// and for this run alone: an `EXPLAIN`, whose operator renders what this
	/// run of it measured, and a statement holding a schema statement, which
	/// planning models in the transaction it runs in.
	Reached {
		op: LogicalOp,
		front_end: bool,
	},
}

/// A statement of the request as the driver runs it: how it is planned, or
/// the error planning raised.
struct CompiledStatement {
	plan: Result<StatementPlan, Error>,
	/// The plan's logical form, kept for the slow log when one is configured.
	logged: Option<LogicalOp>,
}

/// A request whose statements arrived together, and their compilation.
///
/// The request is compiled once, inside the transaction of the first
/// statement that plans, before that statement runs, into one program
/// (`vm::request`). A statement is compiled there where it runs in that
/// transaction and no statement before it may have changed the catalog; every
/// other statement is served when it is reached, planned for the catalog its
/// own transaction reads then and kept in the compile cache by its text, so
/// a schema change the request made, a table a write defined, and another
/// client's schema change committed in between are each met as planning the
/// statement then meets them, and a repeated request finds the plan kept.
struct RequestPlan {
	entries: Vec<RequestEntry>,
	/// Each statement's planning, by position, once the request has been
	/// compiled; `None` where the statement is planned when reached.
	compiled: Option<Vec<Option<PlannedStatement>>>,
	/// The request's program, once compiled.
	program: Option<crate::exec::vm::request::CompiledRequest>,
	/// How many statements the driver has taken from the request.
	taken: Arc<std::sync::atomic::AtomicUsize>,
}

#[cfg(test)]
thread_local! {
	/// A future a test runs between a bare `LIVE` statement's arrival and the
	/// opening of its write transaction, taken by the next arrival.
	pub(crate) static AFTER_LIVE_ARRIVAL: std::cell::RefCell<Option<Pin<Box<dyn Future<Output = ()> + Send>>>> =
		const { std::cell::RefCell::new(None) };
}

/// Mutable access to a frozen context.
///
/// Takes the field rather than the executor so the borrow covers the context
/// alone — `USE` mutates `Options` while holding this reference, which a
/// method on `&mut self` would forbid.
///
/// The context is shared as an `Arc` while a statement runs, so it can only be
/// unfrozen between statements; a live reference here means a statement held
/// on to one past its own execution.
fn context_mut(ctx: &mut Arc<Context>) -> Result<&mut Context, anyhow::Error> {
	Arc::get_mut(ctx)
		.ok_or_else(|| {
			EngineError::unreachable("Tried to unfreeze a Context with multiple references")
		})
		.map_err(anyhow::Error::new)
}

impl Executor {
	/// Install a per-statement notification broker when `writable` is true and the session has
	/// notifications enabled. Read-only bare statements skip installation — they cannot emit
	/// LIVE/KILL notifications — avoiding allocation and stale-broker leaks on the hot path.
	#[inline]
	fn prepare_broker(
		&mut self,
		writable: bool,
		delivery: Option<Arc<dyn MessageBroker>>,
	) -> Option<PreparedBroker> {
		if !writable {
			return None;
		}
		let delivery = delivery?;
		// If a broker is already provided by a higher layer, don't override it here.
		if self.ctx.broker().is_some() {
			return None;
		}
		let (send, recv) = async_channel::unbounded();
		let Some(ctx) = Arc::get_mut(&mut self.ctx) else {
			debug_assert!(false, "prepare_broker: ctx Arc was contended at statement boundary");
			static WARNED: AtomicBool = AtomicBool::new(false);
			if !WARNED.swap(true, Ordering::Relaxed) {
				tracing::warn!(
					target: TARGET,
					"prepare_broker: ctx Arc contended at statement boundary; \
					 LIVE notifications for this batch will be skipped. This is a bug — \
					 future occurrences will not log."
				);
			}
			return None;
		};
		ctx.set_broker(Some(DefaultBroker::new(send, Arc::clone(&delivery))));
		self.broker_owned_by_executor = true;
		Some(PreparedBroker {
			receiver: recv,
			delivery,
		})
	}

	fn flush_live_query_notifications(prepared: PreparedBroker) {
		let PreparedBroker {
			receiver,
			delivery,
		} = prepared;
		// Every sender is gone and nothing is buffered, so no item can arrive.
		if receiver.is_empty() && receiver.is_closed() {
			return;
		}
		common::rt::spawn_detached(async move {
			while let Ok(item) = receiver.recv().await {
				delivery.send(item).await;
			}
		});
	}

	#[inline]
	fn clear_broker(&mut self) {
		let Some(ctx) = Arc::get_mut(&mut self.ctx) else {
			debug_assert!(false, "clear_broker: ctx Arc was contended at statement boundary");
			static WARNED: AtomicBool = AtomicBool::new(false);
			if !WARNED.swap(true, Ordering::Relaxed) {
				tracing::warn!(
					target: TARGET,
					"clear_broker: ctx Arc contended at statement boundary; \
					 broker leaked for this batch and may affect notification \
					 semantics for the next batch. This is a bug — \
					 future occurrences will not log."
				);
			}
			return;
		};
		ctx.set_broker(None);
		self.broker_owned_by_executor = false;
	}
}

impl Executor {
	pub fn new(kvs: &Datastore, ctx: FrozenContext, opt: Options) -> Self {
		Executor {
			results: Vec::new(),
			opt,
			ctx,
			function_registry: Arc::clone(kvs.function_registry()),
			cached_session: None,
			broker_owned_by_executor: false,
			stream: None,
			result_shape: OutputShape::Scalar,
			request: None,
			in_block: false,
			arrived_live: None,
			misplaced_control: None,
			explicit_emission: Emission::Stream,
			holding: false,
		}
	}

	/// Build an executor that streams its results into `items` as it produces
	/// them, instead of only accumulating them.
	pub(crate) fn new_streaming(
		kvs: &Datastore,
		ctx: FrozenContext,
		opt: Options,
		items: Sender<QueryStreamItem>,
		row_format: Option<surrealdb_rpc::RowFormat>,
	) -> Self {
		let mut this = Self::new(kvs, ctx, opt);
		this.stream = Some(StreamSink {
			items,
			row_format,
			streamed: HashMap::new(),
			finalised: 0,
		});
		this
	}

	/// Whether a streaming execution has lost its consumer.
	///
	/// The remaining statements are abandoned when it has. Stopping only the
	/// statement that was producing rows would leave the ones after it to run
	/// and commit — a dropped `SELECT * FROM big; DELETE FROM important` would
	/// still perform the delete — so the check belongs at the statement
	/// boundary, beside the executor's other stop conditions.
	fn consumer_gone(&self) -> bool {
		self.stream.as_ref().is_some_and(StreamSink::is_closed)
	}

	/// Whether the statement about to run should stream its rows.
	///
	/// False once the consumer has gone away, so a dropped stream stops the
	/// work rather than producing rows nothing will read. False inside a held
	/// transaction too: its statements keep their values, and
	/// [`Self::finalise_results`] sends them once the transaction resolves.
	fn streams_rows(&self) -> bool {
		!self.holding && self.stream.as_ref().is_some_and(|sink| !sink.is_closed())
	}

	/// The index of the statement currently running.
	///
	/// Its result has not been pushed yet, so it is the next slot in
	/// [`Self::results`]. Statements run one at a time, which is what makes
	/// that identification sound.
	fn current_index(&self) -> usize {
		self.results.len()
	}

	/// How many rows the statement at `index` streamed, if it streamed.
	///
	/// `Some` means its rows are already on their way to the consumer, so its
	/// value must not be sent again — and it is the row count for that
	/// statement, which cannot be recovered from its result: a streamed
	/// statement's value is empty precisely because the rows went elsewhere.
	fn streamed_rows(&self, index: usize) -> Option<u64> {
		self.stream.as_ref().and_then(|sink| sink.streamed.get(&index).copied())
	}

	/// Converts a finished statement's internal result value to a public value,
	/// honouring the shape recorded for it in [`Executor::result_shape`]. A
	/// `Rows` result is an array-of-rows envelope, so each row is converted as
	/// its own root at depth 1 - the same anchor the storage write guard and the
	/// streaming read path use - rather than charging the envelope a nesting
	/// level. A `Scalar` result is the single value and is converted whole.
	fn convert_statement_result(&self, value: Value) -> Result<surrealdb_types::Value> {
		match self.result_shape {
			OutputShape::Rows => convert_result_rows_to_public(value),
			OutputShape::Scalar => convert_value_to_public_value(value),
		}
	}

	/// Send the results that have become final, in order.
	///
	/// Called once a statement's outcome can no longer change: immediately for
	/// a statement outside a transaction block, and at the block's resolution
	/// for every statement inside one, since until then a failure can still
	/// rewrite them into errors. A held statement's value was kept, so it goes
	/// out as its payload followed by its `Finished`, or as a `Finished` that
	/// carries the error when the block was rolled back.
	async fn finalise_results(&mut self) {
		let Some(sink) = self.stream.as_mut() else {
			return;
		};
		while sink.finalised < self.results.len() {
			let index = sink.finalised;
			sink.finalised += 1;
			let result = &mut self.results[index];
			// A statement whose rows already went out is owed only its terminal
			// item; one that failed is owed only that too, since its rows (if
			// any) are retracted. Anything else contributes its value as well.
			let items = if result.result.is_ok() && sink.streamed.contains_key(&index) {
				vec![QueryStreamItem::Finished {
					index,
					time: result.time,
					query_type: result.query_type,
					error: None,
				}]
			} else {
				// Take the value rather than copy it: it can be a whole
				// statement's results, and the only
				// caller that reads them back wants a `LIVE`/`KILL` id, which
				// is left in place.
				let owned = match result.query_type {
					QueryType::Live | QueryType::Kill => result.clone(),
					QueryType::Other => QueryResult {
						time: result.time,
						result: std::mem::replace(
							&mut result.result,
							Ok(surrealdb_types::Value::None),
						),
						query_type: result.query_type,
					},
				};
				items_for_result(index, owned)
			};
			for item in items {
				sink.send(item).await;
			}
		}
	}

	/// Install a fresh [`StatementCounters`] on the executor's context for
	/// the next statement and return a clone of the handle so the caller
	/// can read the counts after the statement returns. The iterator
	/// (deep in the execution path) inherits the same handle through the
	/// child contexts it derives from `self.ctx`.
	///
	/// Between statements the executor holds the only strong reference to
	/// `self.ctx`, so `Arc::get_mut` succeeds. If it ever fails — i.e.
	/// some child kept a strong reference past statement completion —
	/// the iterator would record into the previous statement's
	/// counters (or `None`) while `count_result_rows` reads zero from
	/// the new one, silently underreporting `RETURN NONE` DML row
	/// counts (the very case `StatementCounters` exists to fix).
	///
	/// We treat that as a programmer error: in debug builds it
	/// panics so tests fail loud; in release builds it logs a `warn`
	/// (deduped per process) so production telemetry surfaces the bug
	/// without turning a row-count discrepancy into an outage.
	fn install_statement_counters(&mut self) -> Arc<StatementCounters> {
		let counters = StatementCounters::new();
		if let Some(ctx) = Arc::get_mut(&mut self.ctx) {
			ctx.set_statement_counters(Some(Arc::clone(&counters)));
		} else {
			debug_assert!(
				false,
				"install_statement_counters: ctx Arc was contended at statement boundary"
			);
			static WARNED: std::sync::atomic::AtomicBool =
				std::sync::atomic::AtomicBool::new(false);
			if !WARNED.swap(true, std::sync::atomic::Ordering::Relaxed) {
				tracing::warn!(
					target: "surrealdb::core::dbs",
					"install_statement_counters: ctx Arc contended at statement boundary; \
					 result_rows on RETURN NONE DML may underreport. This is a bug — \
					 future occurrences will not log."
				);
			}
		}
		counters
	}

	fn execute_option_statement(&mut self, stmt: &OptionStatement) -> Result<()> {
		// Allowed to run?
		self.ctx.is_allowed(&self.opt, Action::Edit, ResourceKind::Option, Base::Db)?;

		if stmt.name.eq_ignore_ascii_case("IMPORT") {
			self.opt.set_import(stmt.what);
		} else if stmt.name.eq_ignore_ascii_case("FORCE") {
			let force = if stmt.what {
				Force::All
			} else {
				Force::None
			};
			self.opt.force = force;
		}

		Ok(())
	}

	/// If slow logging is configured in the current context, evaluate whether the
	/// statement exceeded the slow threshold and emit a log entry if so.
	///
	/// `stm` is the text the entry prints; `params` is walked for the `$param`
	/// references whose values the entry lists. They are one value for a
	/// statement in expression form, and a surface statement paired with its
	/// plan for one the engine ran.
	fn check_slow_log<T: ToSql, P: SlowLogVisit>(&self, start: &Instant, stm: &T, params: &P) {
		if let Some(slow_log) = self.ctx.slow_log() {
			slow_log.check_log(&self.ctx, start, stm, params);
		}
	}

	/// Dispatch a [`QueryEvent`] to the datastore's installed observer at
	/// the boundary of a query batch.
	///
	/// Aggregates per-statement outcomes captured by the executor while
	/// running the batch. Counters are derived from `results`: any
	/// `QueryResult` whose `result` is `Err` is classified as a statement
	/// error, and the batch outcome is [`Outcome::Error`] if any
	/// statement errored, otherwise [`Outcome::Success`]. Callers pass
	/// the slice of [`QueryResult`]s produced by the current batch
	/// (excluding any from outer batches).
	fn emit_query_event_for_results(
		&self,
		kvs: &Datastore,
		batch_start: Instant,
		results: &[QueryResult],
	) {
		let total = results.len() as u32;
		let err = results.iter().filter(|r| r.result.is_err()).count() as u32;
		let ok = total - err;
		let outcome = if err > 0 {
			Outcome::Error
		} else {
			Outcome::Success
		};
		// Classify the batch error using the FIRST errored statement's
		// structured `surrealdb_types::Error`. Operators see the
		// dominant error class for the batch on `surrealdb.query.*`;
		// per-statement classification happens via
		// `surrealdb.statement.*` once executor emit sites are wired
		// to populate it (today only the unexecuted-statement paths
		// do so explicitly).
		let error_class = results
			.iter()
			.find_map(|r| r.result.as_ref().err())
			.map(crate::observe::error_class::classify_types_error);
		kvs.observer().on_query_complete(&QueryEvent {
			safe: QueryEventSafe {
				outcome,
				duration: batch_start.elapsed(),
				counters: QueryCounters {
					total,
					ok,
					err,
				},
				error_class,
			},
			ctx: self.ctx.tenant_identity().map(|t| t.to_query_ctx()).unwrap_or_default(),
		});
	}

	/// Dispatch a [`StatementEvent`] to the datastore's installed observer.
	///
	/// Classification fields (`kind`, `read_only`, optional `sql`) are
	/// captured up-front by the caller before the originating expression is
	/// moved into the execution path. This keeps `on_statement_complete`
	/// reachable from every control-flow branch (early `return`, `continue`,
	/// or fall-through) without re-examining the consumed statement.
	///
	/// `result_rows` is the number of rows the statement returned (SELECT)
	/// or affected (CREATE / UPDATE / UPSERT / DELETE / RELATE / INSERT).
	/// Non-DML statements pass `0`. For DML statements with `RETURN NONE`
	/// the post-RETURN value is empty; the actual affected count is
	/// captured by the iterator on a per-statement [`StatementCounters`]
	/// and supplied here by the caller.
	#[allow(clippy::too_many_arguments)]
	fn emit_statement_event_cached(
		&self,
		kvs: &Datastore,
		kind: StatementType,
		read_only: bool,
		sql: Option<String>,
		start: &Instant,
		outcome: Outcome,
		result_rows: u64,
		error_class: Option<&'static str>,
	) {
		kvs.observer().on_statement_complete(&StatementEvent {
			safe: StatementEventSafe {
				kind,
				outcome,
				duration: start.elapsed(),
				read_only,
				result_rows,
				error_class,
			},
			ctx: self.ctx.tenant_identity().map(|t| t.to_statement_ctx(sql.clone())).unwrap_or(
				StatementEventCtx {
					sql,
					..Default::default()
				},
			),
		});
	}

	/// Emit a `StatementEvent` for a statement that was rejected
	/// before execution (transaction-create failure, parent-batch
	/// timeout, ctx cancellation, …). Centralised so every
	/// `QueryResult::Err(NotExecuted | TimedOut | Cancelled)` push
	/// has a matching telemetry event and dashboards do not undercount
	/// `surrealdb_statement_total{outcome="error"}` on these paths.
	///
	/// Duration is `Duration::ZERO` because the statement never ran;
	/// `read_only` is conservatively `false` because we cannot inspect
	/// the consumed expression.
	fn emit_statement_event_unexecuted(
		&self,
		kvs: &Datastore,
		kind: StatementType,
		error_class: &'static str,
	) {
		kvs.observer().on_statement_complete(&StatementEvent {
			safe: StatementEventSafe {
				kind,
				outcome: Outcome::Error,
				duration: Duration::ZERO,
				read_only: false,
				result_rows: 0,
				error_class: Some(error_class),
			},
			ctx: self.ctx.tenant_identity().map(|t| t.to_statement_ctx(None)).unwrap_or_default(),
		});
	}

	/// Compute the row count for a completed statement based on its kind,
	/// the per-statement counter snapshot, and the result the executor
	/// produced.
	///
	/// For DML statements (CREATE / UPDATE / UPSERT / DELETE / RELATE /
	/// INSERT) the iterator-side [`StatementCounters::affected`] is the
	/// source of truth: it correctly reports records mutated even when
	/// `RETURN NONE` (or a mode whose per-document value happens to be
	/// `Value::None`) suppresses the post-RETURN payload. The value
	/// shape is consulted only when no counter snapshot is available,
	/// which never happens on the production hot path but keeps the
	/// helper safe to call.
	///
	/// For non-DML statements the counter is not consulted and the value
	/// shape is interpreted as before:
	///
	/// - `Value::Array` — `array.len()` (SELECT rows after START/LIMIT);
	/// - `Value::None` / `Value::Null` — `0`;
	/// - anything else — `1`;
	/// - `None` (error path with no value to inspect) — `0`.
	fn count_result_rows(
		kind: StatementType,
		counters: Option<&StatementCounters>,
		value: Option<&Value>,
	) -> u64 {
		if kind.is_dml() {
			if let Some(counters) = counters {
				return counters.affected();
			}
			// Fallback to the value-shape heuristic if no counter was
			// installed (test paths).
			return match value {
				Some(Value::Array(arr)) => arr.len() as u64,
				Some(Value::None) | Some(Value::Null) | None => 0,
				Some(_) => 1,
			};
		}
		match value {
			Some(Value::Array(arr)) => arr.len() as u64,
			Some(Value::None) | Some(Value::Null) | None => 0,
			Some(_) => 1,
		}
	}

	/// Get the cached session info, extracting it on first call.
	///
	/// Session values don't change between statements in the same
	/// executor batch, so we extract once and reuse.
	fn get_session_info(&mut self) -> Option<Arc<crate::exec::context::SessionInfo>> {
		if let Some(ref cached) = self.cached_session {
			return Some(Arc::clone(cached));
		}
		let session = self.extract_session_info();
		self.cached_session.clone_from(&session);
		session
	}

	/// Extract session information from the FrozenContext.
	///
	/// The session is stored as a Value object in the context with keys like
	/// "ns", "db", "id", "ip", "or", "ac", "rd", "tk".
	fn extract_session_info(&self) -> Option<std::sync::Arc<crate::exec::context::SessionInfo>> {
		use crate::exec::context::SessionInfo;
		use crate::expr::paths::{AC, DB, ID, IP, NS, OR, RD, TK};

		let session_value = self.ctx.value("session")?;

		// Extract fields from the session Value. `Value::String` holds a `Strand`, so we just
		// move it into `SessionInfo` without the `Strand -> String -> Strand` round-trip the
		// previous `into_string()` chain incurred on every batch.
		let ns = match session_value.pick(NS.as_ref()) {
			Value::String(s) => Some(s),
			_ => None,
		};

		let db = match session_value.pick(DB.as_ref()) {
			Value::String(s) => Some(s),
			_ => None,
		};

		let id = match session_value.pick(ID.as_ref()) {
			Value::Uuid(u) => Some(*u.as_ref()),
			_ => None,
		};

		let ip = match session_value.pick(IP.as_ref()) {
			Value::String(s) => Some(s),
			_ => None,
		};

		let origin = match session_value.pick(OR.as_ref()) {
			Value::String(s) => Some(s),
			_ => None,
		};

		let ac = match session_value.pick(AC.as_ref()) {
			Value::String(s) => Some(s),
			_ => None,
		};

		let rd = match session_value.pick(RD.as_ref()) {
			Value::None => None,
			v => Some(v),
		};

		let token = match session_value.pick(TK.as_ref()) {
			Value::None => None,
			v => Some(v),
		};

		// Note: exp is not in the session object, it's in the Session struct
		// For now, we leave it as None
		let exp = None;

		Some(std::sync::Arc::new(SessionInfo {
			ns,
			db,
			id,
			ip,
			origin,
			ac,
			rd,
			token,
			exp,
		}))
	}

	/// Execute an OperatorPlan and collect results into a Value.
	///
	/// This builds an ExecutionContext from the current session state and executes
	/// the operator plan, collecting all results into an array.
	/// `stream_rows` is set by the callers that run a **top-level** statement,
	/// whose values are the statement's own result. It is false where the
	/// values are consumed rather than returned — a `LET` binding them to a
	/// param — because those are not the statement's rows and must not reach
	/// the consumer as such.
	async fn execute_operator_plan(
		&mut self,
		plan: StatementPlan,
		txn: Arc<Transaction>,
		stream_rows: bool,
	) -> FlowResult<Value> {
		use tokio_util::sync::CancellationToken;

		use crate::exec::context::ContextLevel;

		/// Guard that stops the timeout task when dropped, so it does not
		/// outlive the execution it was bounding.
		///
		/// The task has to be spawned rather than raced inside the query
		/// future: the point is for it to fire while the executor is inside an
		/// operator that is not otherwise yielding to check the deadline.
		#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
		struct TimeoutGuard(common::rt::JoinHandle<()>);
		#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
		impl Drop for TimeoutGuard {
			fn drop(&mut self) {
				self.0.abort();
			}
		}

		/// The browser executor returns no handle to abort, so there the task
		/// is stopped by a token it waits on alongside the timer.
		#[cfg(all(target_family = "wasm", target_os = "unknown"))]
		struct TimeoutGuard(CancellationToken);
		#[cfg(all(target_family = "wasm", target_os = "unknown"))]
		impl Drop for TimeoutGuard {
			fn drop(&mut self) {
				self.0.cancel();
			}
		}

		/// Cancels `token` once `timeout` elapses, unless the returned guard is
		/// dropped first.
		fn spawn_timeout(timeout: Duration, token: CancellationToken) -> TimeoutGuard {
			#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
			{
				TimeoutGuard(common::rt::spawn(async move {
					common::time::sleep(timeout).await;
					token.cancel();
				}))
			}
			#[cfg(all(target_family = "wasm", target_os = "unknown"))]
			{
				let finished = CancellationToken::new();
				let stop = finished.clone();
				common::rt::spawn_detached(async move {
					tokio::select! {
						_ = common::time::sleep(timeout) => token.cancel(),
						_ = stop.cancelled() => {}
					}
				});
				TimeoutGuard(finished)
			}
		}

		// Derive the engine cancellation token from the
		// context's awaitable cancel handle when one is installed (the WS
		// RPC layer installs it; embedded callers do not). `child_token()`
		// gives parent-cancels-child semantics, so the same connection
		// disconnect that trips the `Context::done`
		// flag also fires the engine operators' `select!` against
		// their token (e.g. a `SLEEP`, long-running scans) — without
		// linking the two we would silently lose WS cancellation on the
		// engine path.
		let cancellation = match self.ctx.cancel_token() {
			Some(parent) => parent.child_token(),
			None => CancellationToken::new(),
		};

		// If a query timeout is configured, cancel the token when it expires.
		// This lets operators that check the cancellation token (e.g.
		// a `SLEEP`, long-running scans) stop promptly instead of running to
		// completion. The guard stops the task when execution finishes.
		let _timeout_guard =
			self.ctx.timeout().map(|timeout| spawn_timeout(timeout, cancellation.clone()));

		let plan = match plan {
			StatementPlan::Ready(plan) => plan,
			// A statement served for the catalog it meets is planned here, in a
			// context of the session's namespace and database as far as they
			// exist, and run in one of the level the plan requires.
			StatementPlan::Served {
				op,
				front_end,
			} => {
				let probe =
					self.statement_context(&txn, &cancellation, true, ContextLevel::Root).await?;
				let principal = Some(Arc::clone(&self.opt.auth));
				let served = match front_end {
					true => {
						crate::exec::vm::serve::front_end(&probe, op, principal, 0, Vec::new())
							.await
					}
					false => crate::exec::vm::serve::request_statement(&probe, op, principal).await,
				};
				match served {
					Ok(plan) => plan,
					Err(e) => {
						let e = match e.downcast::<Error>() {
							Ok(error) => self.planning_failed(&txn, error).await,
							Err(e) => e,
						};
						return Err(ControlFlow::Err(e));
					}
				}
			}
			StatementPlan::Reached {
				op,
				front_end,
			} => {
				let planned = async {
					match front_end {
						true => {
							crate::exec::planner::plan_logical_inner(
								op,
								&self.ctx,
								&self.function_registry,
								Arc::clone(&txn),
								Some(Arc::clone(&self.opt.auth)),
								0,
							)
							.await
						}
						false => self.plan_bare_expression(op, Arc::clone(&txn)).await,
					}
				}
				.await;
				match planned {
					Ok(plan) => plan,
					Err(e) => return Err(ControlFlow::Err(self.planning_failed(&txn, e).await)),
				}
			}
		};
		let exec_ctx = self
			.statement_context(
				&txn,
				&cancellation,
				plan.access_mode() != crate::exec::AccessMode::ReadOnly,
				plan.required_context(),
			)
			.await?;

		// A statement whose rows stream to a consumer that takes them encoded
		// is run to hand them on encoded, which its operators may do without
		// building a value for a row.
		let encoded_format = (stream_rows && matches!(plan.output_shape(), OutputShape::Rows))
			.then(|| self.stream.as_ref().and_then(|sink| sink.row_format))
			.flatten();

		// Execute the plan
		// Handle control flow signals from execute()
		let executed = match encoded_format {
			Some(format) => plan.execute_encoded(&exec_ctx, format),
			None => plan.execute(&exec_ctx),
		};
		let mut stream = match executed {
			Ok(s) => s,
			Err(crate::expr::ControlFlow::Return(v)) => {
				// RETURN - propagate as control flow signal
				return Err(ControlFlow::Return(v));
			}
			Err(crate::expr::ControlFlow::Break) => {
				return Err(ControlFlow::Break);
			}
			Err(crate::expr::ControlFlow::Continue) => {
				return Err(ControlFlow::Continue);
			}
			Err(crate::expr::ControlFlow::Err(e)) => {
				return Err(ControlFlow::Err(e));
			}
		};

		// Record the shape so the collected result is converted to public with
		// the same per-row anchor the streaming branch below uses, rather than
		// charging the result-array envelope a nesting level.
		self.result_shape = plan.output_shape();

		// A row-shaped statement whose rows nothing is waiting to collect can
		// send each batch onward as it arrives, rather than building the whole
		// array first. A scalar one cannot: its value IS the single row, so
		// there is nothing to stream and one item carries it.
		let streaming = stream_rows && matches!(plan.output_shape(), OutputShape::Rows);
		let index = self.current_index();

		// Collect all results
		let mut results = Vec::new();
		let mut streamed: u64 = 0;
		while let Some(batch_result) = stream.next().await {
			match batch_result {
				Ok(batch) => {
					if streaming {
						let sink = self.stream.as_ref().expect("streaming implies a sink");
						let mut batch = batch;
						let item = match sink.row_format {
							// Rows the operators encoded, then any they handed on
							// as values, encoded here: no public value is built
							// for them.
							Some(format) => {
								let rows = match batch.take_encoded() {
									Some(mut rows) if rows.format() == format => {
										if !batch.is_empty() {
											let values = encode_rows(format, &batch)
												.map_err(ControlFlow::Err)?;
											rows.extend(&values)
												.map_err(|e| ControlFlow::Err(e.into()))?;
										}
										rows
									}
									Some(_) => {
										return Err(ControlFlow::Err(anyhow::anyhow!(
											"Rows encoded in a format other than the stream's"
										)));
									}
									None => {
										encode_rows(format, &batch).map_err(ControlFlow::Err)?
									}
								};
								streamed += rows.len() as u64;
								QueryStreamItem::Frame {
									index,
									rows,
								}
							}
							None => {
								let values = batch
									.into_iter()
									.map(convert_value_to_public_value)
									.collect::<Result<Vec<_>>>()
									.map_err(ControlFlow::Err)?;
								streamed += values.len() as u64;
								QueryStreamItem::Rows {
									index,
									values,
								}
							}
						};
						sink.send(item).await;
						// A consumer that stopped reading wants no more rows, and
						// the statement is about to be abandoned anyway.
						if sink.is_closed() {
							break;
						}
					} else {
						results.extend(batch.into_values());
					}
				}
				Err(crate::expr::ControlFlow::Err(e)) => {
					return Err(ControlFlow::Err(e));
				}
				Err(crate::expr::ControlFlow::Return(v)) => {
					// RETURN - propagate as control flow signal
					return Err(ControlFlow::Return(v));
				}
				Err(crate::expr::ControlFlow::Break) => {
					return Err(ControlFlow::Break);
				}
				Err(crate::expr::ControlFlow::Continue) => {
					return Err(ControlFlow::Continue);
				}
			}
		}

		if streaming {
			// The rows are already on their way, so the statement's own value is
			// empty. `streamed` is what tells the caller not to send it again.
			self.stream
				.as_mut()
				.expect("streaming implies a sink")
				.streamed
				.insert(index, streamed);
			return Ok(Value::None);
		}

		shape_statement_value(plan.output_shape(), results).map_err(ControlFlow::Err)
	}

	/// The context a statement runs in: a fresh snapshot of the session's
	/// context with `txn`, cancelled through `cancellation`, at the highest
	/// level the session's namespace and database support — failing where
	/// either is missing at or below `required_level`, the level the plan
	/// requires.
	async fn statement_context(
		&mut self,
		txn: &Arc<Transaction>,
		cancellation: &tokio_util::sync::CancellationToken,
		writes: bool,
		required_level: crate::exec::context::ContextLevel,
	) -> FlowResult<crate::exec::context::ExecutionContext> {
		use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
		use crate::exec::context::{
			ContextLevel, DatabaseContext, ExecutionContext, NamespaceContext, RootContext,
		};
		// Build the root context using cached session info. The context
		// snapshot must be fresh per-query because it contains the
		// transaction reference which changes between statements.
		let root_ctx = RootContext {
			ctx: Context::snapshot(&self.ctx).freeze(),
			options: Some(self.opt.clone()),
			datastore: None,
			cancellation: Arc::new(cancellation.clone()),
			auth: Arc::clone(&self.opt.auth),
			session: self.get_session_info(),
			current_value: None,
			current_row: None,
			skip_fetch_perms: false,
			perms_suppressed: false,
			caller_perms_suppressed: false,
			computing_record: None,
			no_write: None,
			version_stamp: None,
			zero_limits: Vec::new(),
			topk_thresholds: None,
			guard_verdicts: Arc::new(crate::exec::guard_verdicts::GuardVerdicts::for_statement(
				writes,
			)),
			writing: false,
		};

		// The plan's declared context level is the minimum it needs (see
		// `ContextLevel`), not a construction target: materialize the highest
		// level the session's ns/db selection supports. A closure that
		// arrives as a runtime value (`LET $f = || (SELECT ...); $f()`)
		// hides its body from the planner, so a database-touching body must
		// find the database context already in place even though the plan
		// only declared root. Above the declared minimum the upgrade is
		// opportunistic — a session pointing at a missing ns/db falls back
		// to the declared level rather than failing a statement that never
		// needed more. At or below the minimum, resolution failures surface
		// exactly as before.
		//
		// SECURITY: `USE` no longer implicitly creates namespaces for
		// sessions without `DEFINE NAMESPACE`-equivalent authorization, so
		// this must not paper over a missing target by writing a fresh
		// namespace into the catalog on the read path (which would also
		// fail on a read-only transaction). `expect_ns_by_name` /
		// `expect_db_by_name` surface a clean `NsNotFound` / `DbNotFound`
		// instead.

		let ns_def = if required_level >= ContextLevel::Namespace {
			Some(txn.expect_ns_by_name(self.opt.ns()?).await?)
		} else if let Ok(ns_name) = self.opt.ns() {
			txn.expect_ns_by_name(ns_name).await.ok()
		} else {
			None
		};

		let db_def = match &ns_def {
			Some(_) if required_level >= ContextLevel::Database => {
				Some(txn.expect_db_by_name(self.opt.ns()?, self.opt.db()?).await?)
			}
			Some(_) => match (self.opt.ns(), self.opt.db()) {
				(Ok(ns_name), Ok(db_name)) => txn.expect_db_by_name(ns_name, db_name).await.ok(),
				_ => None,
			},
			None => None,
		};

		let exec_ctx = match (ns_def, db_def) {
			(Some(ns_def), Some(db_def)) => ExecutionContext::Database(DatabaseContext {
				ns_ctx: NamespaceContext {
					root: root_ctx,
					ns: ns_def,
				},
				db: db_def,
				field_state_cache: std::sync::Arc::new(tokio::sync::RwLock::new(
					std::collections::HashMap::new(),
				)),
				table_def_cache: std::sync::Arc::new(tokio::sync::RwLock::new(
					std::collections::HashMap::new(),
				)),
				index_def_cache: std::sync::Arc::new(tokio::sync::RwLock::new(
					std::collections::HashMap::new(),
				)),
				select_permission_cache: std::sync::Arc::new(tokio::sync::RwLock::new(
					std::collections::HashMap::new(),
				)),
			}),
			(Some(ns_def), None) => ExecutionContext::Namespace(NamespaceContext {
				root: root_ctx,
				ns: ns_def,
			}),
			(None, _) => ExecutionContext::Root(root_ctx),
		};
		Ok(exec_ctx)
	}

	/// Run a bare expression on the engine.
	///
	/// Lowering or planning can refuse a statement — a nested DML subquery or a
	/// shape no operator exists for — and that refusal is the statement's
	/// error: `PlannerUnsupported`, carrying the reason.
	async fn run_on_streaming_engine(
		&mut self,
		expr: sql::Expr,
		txn: Arc<Transaction>,
		start: &Instant,
	) -> FlowResult<Value> {
		let (planned, logged) = match self.compiled_statement(&txn).await {
			Some(compiled) => (compiled.plan, compiled.logged),
			None => {
				let lowered = self.lower_bare_expression(&expr);
				// Planning consumes the plan, and the slow log reads the
				// statement's parameters from it after the query has run; keep a
				// copy only when a slow log is configured.
				let logged = match &lowered {
					Ok(op) if self.ctx.slow_log().is_some() => Some(op.clone()),
					_ => None,
				};
				let planned = match lowered {
					Ok(op) => self.plan_bare_expression(op, Arc::clone(&txn)).await,
					Err(e) => Err(e),
				};
				(planned.map(StatementPlan::Ready), logged)
			}
		};
		match planned {
			Ok(plan) => {
				context_mut(&mut self.ctx)?.set_transaction(Arc::clone(&txn));
				// Every caller of this method runs a top-level statement, so
				// these values are the statement's own result and may stream.
				let stream_rows = self.streams_rows();
				let exec_result =
					self.execute_operator_plan(plan, Arc::clone(&txn), stream_rows).await;
				// Logged through the lowered form, never the surface text: a
				// schema statement's surface text names the credential the
				// conversion has already replaced with its derivation
				// (`DEFINE USER ... PASSWORD`), and one can sit anywhere an
				// expression can: inside a block, an `IF` branch, a function
				// body. Converting here makes the entry safe wherever it sits
				// rather than only where the statement is one. The parameters
				// are read from the plan, whose scalars name every `$param`
				// the statement references.
				if let Some(op) = logged {
					self.check_slow_log(start, &Expr::from(expr.clone()), &PlanParams(&op));
				}
				exec_result
			}
			Err(e) => Err(ControlFlow::Err(self.planning_failed(&txn, e).await)),
		}
	}

	/// The error a statement the engine did not plan fails with. A decline met
	/// in a session whose namespace or database does not exist is that
	/// namespace or database not found, as the context check of a statement
	/// that reaches it raises it; any other failure is its own.
	async fn planning_failed(&self, txn: &Transaction, error: Error) -> anyhow::Error {
		if matches!(error, Error::Exec(ExecError::PlannerUnsupported { .. }))
			&& let (Ok(ns), Ok(db)) = (self.opt.ns(), self.opt.db())
		{
			if let Err(missing) = txn.expect_ns_by_name(ns).await {
				return missing;
			}
			if let Err(missing) = txn.expect_db_by_name(ns, db).await {
				return missing;
			}
		}
		anyhow::Error::new(error)
	}

	/// Lower a bare expression to its logical plan.
	///
	/// No expression-layer value is built on this path. A shape with no
	/// logical form is reported as `PlannerUnsupported`, the same error the
	/// planner raises. A statement that lowers but holds a write the planner
	/// cannot compile is refused the same way, by
	/// [`crate::exec::logical::unplannable_write`] inside `plan_statement_inner`.
	///
	/// A `SELECT` is checked against the `FOR UPDATE` rules before lowering,
	/// on the surface form the parser produced.
	fn lower_bare_expression(&self, expr: &sql::Expr) -> Result<LogicalOp, Error> {
		if let sql::Expr::Select(select) = expr {
			crate::dbs::validate_for_update_ast(select)?;
		}
		lower(expr.clone()).map_err(|reason| {
			ExecError::PlannerUnsupported {
				reason: reason.to_string(),
			}
			.into()
		})
	}

	/// Bind a lowered bare expression for `txn` and compile it into the
	/// operator tree that runs it.
	async fn plan_bare_expression(
		&self,
		op: LogicalOp,
		txn: Arc<Transaction>,
	) -> Result<Arc<dyn crate::exec::ExecOperator>, Error> {
		plan_statement_inner(
			op,
			&self.ctx,
			&self.function_registry,
			txn,
			Some(Arc::clone(&self.opt.auth)),
			0,
			Some(Arc::new(self.opt.clone())),
		)
		.await
	}

	/// Executes a statement which needs a transaction with the supplied
	/// transaction.
	///
	/// A statement is converted no further than it requires, and only once it
	/// is about to run: a batch that ends before reaching a statement converts
	/// nothing of it, and a statement the engine plans is never
	/// converted whole. What a surface statement converts is stated on
	/// [`Self::run_surface_statement`]; a plan converts nothing and goes to
	/// [`Self::run_logical_plan`].
	///
	/// The transaction is already open by the time this runs, so whatever does
	/// convert converts inside it and counts against the transaction timeout.
	/// That is free for most statements and is not for
	/// `DEFINE USER`/`ALTER USER ... PASSWORD`, whose conversion derives an
	/// Argon2 hash and a SCRAM verifier: deliberately expensive, and with no
	/// yield point. A schema statement converts once, during lowering.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(
			level = "debug",
			name = "executor",
			target = "surrealdb::core::dbs",
			skip_all
		)
	)]
	async fn execute_plan_in_transaction(
		&mut self,
		txn: Arc<Transaction>,
		start: &Instant,
		stmt: TopLevelStatement,
	) -> FlowResult<Value> {
		// A statement's result shape defaults to scalar; a statement that reaches
		// a plan (`execute_operator_plan`) overrides it. It
		// selects how the result is converted to a public value, so it is reset per
		// statement here rather than inherited from the previous one.
		self.result_shape = OutputShape::Scalar;
		let res = match stmt {
			TopLevelStatement::Ast(stmt) => self.run_surface_statement(stmt, txn, start).await,
			TopLevelStatement::Plan(op) => self.run_logical_plan(op, txn, start).await,
		};

		// Catch cancellation during running.
		match self.ctx.done(true)? {
			None => res,
			Some(Reason::Timedout(d)) => {
				Err(ControlFlow::from(anyhow::anyhow!(EngineError::QueryTimedout(d.0))))
			}
			Some(Reason::Canceled) => {
				Err(ControlFlow::from(anyhow::anyhow!(EngineError::QueryCancelled)))
			}
		}
	}

	/// Run a statement in the form the parser produced it.
	///
	/// Each arm converts its own payload and nothing else, so a statement pays
	/// only for the conversion it needs: `KILL` converts a `KillStatement`
	/// rather than the statement enclosing it, and a bare expression is
	/// lowered by the engine without being converted whole.
	async fn run_surface_statement(
		&mut self,
		stmt: sql::TopLevelExpr,
		txn: Arc<Transaction>,
		start: &Instant,
	) -> FlowResult<Value> {
		match stmt {
			sql::TopLevelExpr::Use(stmt) => self.run_use(stmt.into(), txn).await,
			sql::TopLevelExpr::Option(_) => Self::option_reached_an_engine(),
			sql::TopLevelExpr::Expr(sql::Expr::Let(stmt)) => self.run_let(*stmt, txn, start).await,
			sql::TopLevelExpr::Begin | sql::TopLevelExpr::Commit | sql::TopLevelExpr::Cancel => {
				match self.misplaced_control.take() {
					Some(misplaced) => Self::transaction_control_error(misplaced.message()),
					None => Err(ControlFlow::Err(anyhow::Error::new(EngineError::unreachable(
						"a transaction-control statement reached an engine without a misplaced \
						 mark from request lowering",
					)))),
				}
			}
			sql::TopLevelExpr::Kill(stmt) => self.run_kill(stmt.into(), txn).await,
			sql::TopLevelExpr::Live(stmt) => self.run_live(*stmt, txn).await,
			sql::TopLevelExpr::Show(stmt) => self.run_show(stmt.into(), txn).await,
			sql::TopLevelExpr::Access(stmt) => self.run_access((*stmt).into(), txn).await,
			sql::TopLevelExpr::Expr(expr) => self.run_bare_expression(expr, txn, start).await,
		}
	}

	/// The error a transaction-control statement raises where no driver acted
	/// on it: `BEGIN` inside a transaction, `COMMIT` or `CANCEL` outside one.
	/// Request lowering marks each such statement with its message.
	fn transaction_control_error(message: &str) -> FlowResult<Value> {
		Err(ControlFlow::Err(anyhow::Error::new(ExecError::InvalidStatement(message.to_string()))))
	}

	/// `OPTION` changes the options every later statement in the batch runs
	/// under, so a driver applies it and never hands it to an engine.
	fn option_reached_an_engine() -> FlowResult<Value> {
		Err(ControlFlow::Err(anyhow::Error::new(EngineError::unreachable(
			"TopLevelExpr::Option should have been handled by a calling function",
		))))
	}

	/// Run a bare expression on the engine.
	async fn run_bare_expression(
		&mut self,
		expr: sql::Expr,
		txn: Arc<Transaction>,
		start: &Instant,
	) -> FlowResult<Value> {
		self.run_on_streaming_engine(expr, txn, start).await
	}

	/// Run a statement that arrived already planned. There is no lowering step,
	/// so the plan is compiled and run or the query fails.
	async fn run_logical_plan(
		&mut self,
		op: Box<crate::expr::logical::LogicalOp>,
		txn: Arc<Transaction>,
		start: &Instant,
	) -> FlowResult<Value> {
		let (planned, logged) = match self.compiled_statement(&txn).await {
			Some(compiled) => (compiled.plan, compiled.logged.map(Box::new)),
			None => {
				// Planning consumes the plan, and the slow log needs it after
				// the query has run; keep a copy only when one is configured.
				let logged = self.ctx.slow_log().is_some().then(|| op.clone());
				let planned = crate::exec::planner::plan_logical_inner(
					*op,
					&self.ctx,
					&self.function_registry,
					Arc::clone(&txn),
					Some(Arc::clone(&self.opt.auth)),
					0,
				)
				.await;
				(planned.map(StatementPlan::Ready), logged)
			}
		};
		match planned {
			Ok(plan) => {
				context_mut(&mut self.ctx)?.set_transaction(Arc::clone(&txn));
				let stream_rows = self.streams_rows();
				let exec_result =
					self.execute_operator_plan(plan, Arc::clone(&txn), stream_rows).await;
				if let Some(op) = logged {
					self.check_slow_log(start, &op, &PlanParams(&op));
				}
				exec_result
			}
			Err(e) => Err(ControlFlow::Err(self.planning_failed(&txn, e).await)),
		}
	}

	/// Resolve one `USE` operand to the name it gives.
	///
	/// A bare identifier or table name is the name as written; anything else
	/// is evaluated against the statement's transaction and must coerce to a
	/// string, the refusal naming `into`.
	async fn use_ident(&self, expr: &crate::expr::Expr, into: &str) -> FlowResult<String> {
		let exec = crate::ddl::frame(&self.ctx, &self.opt, None).await;
		crate::ddl::scalar::ident(&exec, expr, into).await.map_err(ControlFlow::Err)
	}

	/// Run a `USE`.
	async fn run_use(&mut self, stmt: UseStatement, txn: Arc<Transaction>) -> FlowResult<Value> {
		// The namespace and database arguments are expressions, and an
		// expression may read or write the datastore. They are computed
		// against `self.ctx` below, so the statement's transaction has
		// to be installed on it first: `Context::tx()` has no fallback
		// for a context without one.
		context_mut(&mut self.ctx)?.set_transaction(Arc::clone(&txn));

		let (use_ns, use_db) = match stmt {
			UseStatement::Default => {
				if let Some(x) = txn.get_default_config().await? {
					(x.namespace.clone(), x.database.clone())
				} else {
					(None, None)
				}
			}
			UseStatement::Ns(ns) => {
				let ns = self.use_ident(&ns, "namespace").await?;

				(Some(ns), None)
			}
			UseStatement::Db(db) => {
				let db = self.use_ident(&db, "database").await?;

				(None, Some(db))
			}
			UseStatement::NsDb(ns, db) => {
				let ns = self.use_ident(&ns, "namespace").await?;

				let db = self.use_ident(&db, "database").await?;

				(Some(ns), Some(db))
			}
		};

		// SECURITY (GHSA-2v9j): `USE` moves the session's *selected*
		// namespace/database to caller-supplied operands. The
		// authenticated Auth level — not the (about-to-be-overwritten)
		// selected ns/db — is the source of truth for tenant scope, so
		// the principal must be verified as authorized for the target
		// BEFORE the switch takes effect (SECURITY_GUIDE section 3).
		// Without this gate a Database- or Namespace-level principal
		// could pivot into a foreign tenant and reach any table there
		// whose PERMISSIONS are satisfiable (e.g. `PERMISSIONS FULL`),
		// collapsing the namespace/database isolation boundary. This
		// mirrors the custom-API tenant gate added for
		// GHSA-848m-r628-vrxw (`api/invocation.rs`, `kvs/ds.rs`), which
		// had never been applied to the `USE` / `/sql` executor path.
		// A switch that only changes one of ns/db keeps the other from
		// the current session (`or(self.opt...)`); Root and anonymous
		// (`Level::No`) principals are unrestricted by `can_access_ns_db`.
		let target_ns = use_ns.as_deref().or(self.opt.ns.as_deref());
		let target_db = use_db.as_deref().or(self.opt.db.as_deref());
		if let Some(ns_name) = target_ns {
			let db_name = target_db.unwrap_or_default();
			if !self.opt.auth.can_access_ns_db(ns_name, db_name) {
				// Report the narrowest boundary that failed: if the
				// namespace itself is in scope, the database is the
				// offending operand, otherwise the namespace is.
				let err = if self.opt.auth.level().ns() == Some(ns_name) {
					ExecError::DbNotAllowed {
						db: db_name.to_owned(),
					}
				} else {
					ExecError::NsNotAllowed {
						ns: ns_name.to_owned(),
					}
				};
				return Err(ControlFlow::Err(anyhow::Error::new(err)));
			}
		}

		// SECURITY: implicit creation of a namespace or database via
		// `USE` requires the same authorization as the explicit
		// `DEFINE NAMESPACE` / `DEFINE DATABASE` statements
		// (SECURITY_GUIDE section 3). Re-selecting an existing
		// namespace or database is unrestricted; downstream
		// operations against a non-existent resource surface
		// `NsNotFound` / `DbNotFound` naturally rather than a
		// silently auto-created resource the session was never
		// allowed to create.
		let create_ns = if let Some(ns_name) = use_ns.as_deref() {
			if txn.get_ns_by_name(ns_name, None).await?.is_some()
				|| (!self.ctx.auth_enabled() && self.opt.auth.is_anon())
			{
				true
			} else {
				self.opt.auth.is_allowed(Action::Edit, &ResourceKind::Namespace.on_root()).is_ok()
			}
		} else {
			false
		};
		let create_db = if let Some(db_name) = use_db.as_deref() {
			let target_ns = use_ns.as_deref().or(self.opt.ns.as_deref());
			if let Some(ns_name) = target_ns {
				if txn.get_db_by_name(ns_name, db_name, None).await?.is_some()
					|| (!self.ctx.auth_enabled() && self.opt.auth.is_anon())
				{
					true
				} else if txn.get_ns_by_name(ns_name, None).await?.is_none()
					&& self
						.opt
						.auth
						.is_allowed(Action::Edit, &ResourceKind::Namespace.on_root())
						.is_err()
				{
					// SECURITY: `ensure_ns_db` is
					// `get_or_add_db_upwards(..., upwards = true)`
					// and silently `get_or_add_ns`-es a missing
					// parent (see `kvs/tx.rs::get_or_add_db_upwards`).
					// Without this gate, a namespace-level Editor —
					// including one on a stale token whose
					// namespace has since been dropped — could
					// recreate the parent namespace as a side
					// effect of `USE NS dropped DB anything` or
					// `USE DB anything` against a stale
					// session-level ns.
					false
				} else {
					self.opt
						.auth
						.is_allowed(Action::Edit, &ResourceKind::Database.on_ns(ns_name))
						.is_ok()
				}
			} else {
				false
			}
		} else {
			false
		};

		let ctx = context_mut(&mut self.ctx)?;

		// Apply new namespace
		if let Some(ns) = use_ns {
			if create_ns {
				txn.get_or_add_ns(Some(ctx), &ns).await?;
			}

			let mut session = ctx.value("session").unwrap_or(&Value::None).clone();
			self.opt.set_ns(Some(ns.as_str().into()));
			session.put(NS.as_ref(), ns.into());
			ctx.add_value("session", session.into());
		}

		// Apply new database
		if let Some(db) = use_db {
			let Some(ns) = &self.opt.ns else {
				return Err(ControlFlow::Err(anyhow::anyhow!(
					"Cannot use database without namespace"
				)));
			};

			if create_db {
				txn.ensure_ns_db(Some(ctx), ns, &db).await?;
			}

			let mut session = ctx.value("session").unwrap_or(&Value::None).clone();
			self.opt.set_db(Some(db.as_str().into()));
			session.put(DB.as_ref(), db.into());
			ctx.add_value("session", session.into());
		}

		// Invalidate cached session info since USE changes ns/db
		self.cached_session = None;

		// Return the current namespace and database
		Ok(Value::from(map! {
			"namespace" => self.opt.ns.as_deref().map(|x| Value::String(x.into())).unwrap_or(Value::None),
			"database" => self.opt.db.as_deref().map(|x| Value::String(x.into())).unwrap_or(Value::None),
		}))
	}

	/// Run a top-level `LET`, binding its result into the batch's scope.
	///
	/// The right-hand side is what runs, not the `LET` itself: the value is
	/// computed here and bound into `self.ctx`, which is the batch's scope. A
	/// side the planner refuses fails the statement with `PlannerUnsupported`.
	async fn run_let(
		&mut self,
		stm: sql::statements::SetStatement,
		txn: Arc<Transaction>,
		start: &Instant,
	) -> FlowResult<Value> {
		// Reject protected names first, before any work: avoids planning
		// or computing a value we'll throw away.
		if surrealdb_cnf::PROTECTED_PARAM_NAMES.contains(&stm.name.as_str()) {
			return Err(ControlFlow::from(anyhow::Error::new(ExecError::InvalidParam {
				name: stm.name.to_string(),
			})));
		}
		context_mut(&mut self.ctx)?.set_transaction(Arc::clone(&txn));

		let (planned, logged) = match self.compiled_statement(&txn).await {
			Some(compiled) => (compiled.plan, compiled.logged),
			None => {
				// The slow log reads the statement's parameters from the plan,
				// which planning consumes; a copy is kept only when a slow log
				// is configured.
				let op = lower(stm.what.clone()).map_err(|reason| {
					ControlFlow::Err(anyhow::Error::new(ExecError::PlannerUnsupported {
						reason: reason.to_string(),
					}))
				})?;
				let logged = self.ctx.slow_log().is_some().then(|| op.clone());
				(
					self.plan_bare_expression(op, Arc::clone(&txn)).await.map(StatementPlan::Ready),
					logged,
				)
			}
		};
		let plan = match planned {
			Ok(plan) => plan,
			Err(e) => return Err(ControlFlow::Err(self.planning_failed(&txn, e).await)),
		};
		// The RHS binds to a param rather than being returned, so its
		// values are not the statement's rows and must not stream.
		let res = self.execute_operator_plan(plan, Arc::clone(&txn), false).await;

		let res = res?;
		let result = match &stm.kind {
			Some(kind) => {
				let kind = crate::expr::Kind::from(kind.clone());
				res.coerce_to_kind(&kind)
					.map_err(|e| ExecError::SetCoerce {
						name: stm.name.to_string(),
						error: Box::new(e),
					})
					.map_err(anyhow::Error::new)?
			}
			None => res,
		};

		// SECURITY: a top-level LET binds into the request-scoped context and is
		// not reset between statements, so a `LET $a = [$a]` chain across
		// statements grows the bound value's nesting without bound - the same
		// accumulation vector as fold/reduce (a body LET inside a FOR is
		// iteration-scoped and does not), and equally fatal on the recursive clone
		// that reads `$a` back. Cap it here, as the storage guard caps a written
		// value.
		crate::val::check_value_depth(&result)?;

		// Set the parameter
		context_mut(&mut self.ctx)?.add_value(stm.name.clone(), result.into());

		// Logged through the expression layer, for the reason the statement
		// path is: a `DEFINE USER ... PASSWORD` in the value names a credential
		// the conversion has already replaced with its derivation. The plan
		// names every `$param` the statement references.
		if let Some(op) = logged {
			let stm = crate::expr::statements::SetStatement::from(stm);
			self.check_slow_log(start, &stm, &PlanParams(&op));
		}
		// Finalise transaction, returning nothing unless it couldn't commit
		Ok(Value::None)
	}

	/// Run a `KILL`.
	async fn run_kill(
		&mut self,
		s: crate::expr::statements::KillStatement,
		txn: Arc<Transaction>,
	) -> FlowResult<Value> {
		Arc::get_mut(&mut self.ctx)
			.ok_or_else(|| {
				err::Error::unreachable("Tried to unfreeze a Context with multiple references")
			})
			.map_err(anyhow::Error::new)?
			.set_transaction(txn);
		crate::exec::kill::kill_statement_compute(&s, &self.ctx, &self.opt)
			.await
			.map_err(ControlFlow::Err)
	}

	/// Run a `LIVE SELECT`.
	///
	/// A bare statement registers the conversion and unit it arrived with. A
	/// statement inside a transaction block or a caller-owned transaction has
	/// none, and registers without a unit.
	async fn run_live(
		&mut self,
		stmt: sql::statements::LiveStatement,
		txn: Arc<Transaction>,
	) -> FlowResult<Value> {
		let (s, arrived) = match self.arrived_live.take() {
			Some((s, arrived)) => (s, arrived),
			None => (stmt.into(), None),
		};
		context_mut(&mut self.ctx)?.set_transaction(txn);
		crate::exec::live::live_statement_compute(&s, &self.ctx, &self.opt, arrived)
			.await
			.map_err(ControlFlow::Err)
	}

	/// Run a `SHOW CHANGES`.
	async fn run_show(
		&mut self,
		s: crate::expr::statements::ShowStatement,
		txn: Arc<Transaction>,
	) -> FlowResult<Value> {
		context_mut(&mut self.ctx)?.set_transaction(txn);
		crate::cf::reader::show_changes(&s, &self.ctx, &self.opt).await.map_err(ControlFlow::Err)
	}

	/// Run an `ACCESS` statement.
	async fn run_access(
		&mut self,
		s: crate::expr::statements::AccessStatement,
		txn: Arc<Transaction>,
	) -> FlowResult<Value> {
		context_mut(&mut self.ctx)?.set_transaction(txn);
		crate::ddl::access_statement_compute(&s, &self.ctx, &self.opt)
			.await
			.map_err(ControlFlow::Err)
	}

	/// Convert a bare `LIVE` statement as it arrives and compile its unit in a
	/// read transaction of its own, before the write transaction that
	/// registers it opens. The read transaction is cancelled before this
	/// returns, so nothing of it is held while the registration runs.
	///
	/// Arrival is best effort and never fails the statement: where the node
	/// holds no unit cache, realtime is off, no read transaction opens or the
	/// context cannot hold one, the statement arrives without a unit and
	/// registration compiles nothing. A node without a unit cache opens no
	/// transaction here.
	async fn arrive_live(
		&mut self,
		kvs: &Datastore,
		converted: crate::expr::statements::LiveStatement,
	) {
		let arrived = if self.ctx.live_units().is_some() && self.ctx.realtime().is_ok() {
			self.compile_arrived_live(kvs, &converted).await
		} else {
			None
		};
		#[cfg(test)]
		if let Some(hook) = AFTER_LIVE_ARRIVAL.with(|hook| hook.borrow_mut().take()) {
			hook.await;
		}
		self.arrived_live = Some((converted, arrived));
	}

	/// Compile `converted`'s unit in a read transaction of its own. `None`
	/// where no read transaction opens or the context cannot hold it, and
	/// wherever [`crate::exec::live::live_statement_arrive`] yields none.
	async fn compile_arrived_live(
		&mut self,
		kvs: &Datastore,
		converted: &crate::expr::statements::LiveStatement,
	) -> Option<crate::exec::vm::lower_live::Arrived> {
		let txn = kvs.transaction(TransactionType::Read).await.ok()?;
		let txn = Arc::new(txn.with_tenant_identity(self.ctx.tenant_identity().cloned()));
		let Ok(ctx) = context_mut(&mut self.ctx) else {
			let _ = txn.cancel().await;
			return None;
		};
		ctx.set_transaction(Arc::clone(&txn));
		let arrived =
			crate::exec::live::live_statement_arrive(converted, &self.ctx, &self.opt).await;
		let _ = txn.cancel().await;
		arrived
	}

	/// Execute a query not wrapped in a transaction block.
	async fn execute_bare_statement(
		&mut self,
		kvs: &Datastore,
		start: &Instant,
		stmt: TopLevelStatement,
	) -> Result<Value> {
		// Don't even try to run if the query should already be finished.
		match self.ctx.done(true)? {
			None => {}
			Some(Reason::Timedout(d)) => {
				bail!(EngineError::QueryTimedout(d.0));
			}
			Some(Reason::Canceled) => {
				bail!(EngineError::QueryCancelled);
			}
		}

		self.execute_plan_impl(kvs, start, stmt).await
	}

	async fn execute_plan_impl(
		&mut self,
		kvs: &Datastore,
		start: &Instant,
		stmt: TopLevelStatement,
	) -> Result<Value> {
		self.broker_owned_by_executor = false;
		let result = self.execute_plan_impl_inner(kvs, start, stmt).await;
		// An arrival belongs to the statement that made it, whichever way that
		// statement ended.
		self.arrived_live = None;
		if self.broker_owned_by_executor {
			self.clear_broker();
		}
		// The statement's transaction has ended, committed or cancelled.
		if let Some(memory) = self.ctx.memory() {
			memory.transaction_ended();
		}
		result
	}

	async fn execute_plan_impl_inner(
		&mut self,
		kvs: &Datastore,
		start: &Instant,
		stmt: TopLevelStatement,
	) -> Result<Value> {
		// The mode is chosen before anything is converted: the predicate
		// answers from the statement's syntax alone, so which kind of
		// transaction to open is known without building the expression tree.
		let transaction_type = if stmt.read_only() {
			TransactionType::Read
		} else {
			TransactionType::Write
		};
		// A bare `LIVE` or `KILL` that loses a retryable conflict on its
		// table's live-query stamp runs again in a fresh transaction, up to
		// `LIVE_KILL_CONFLICT_ATTEMPTS` times, and a re-run `LIVE` registers
		// under the subscription id of its first attempt. Inside a transaction
		// block or a caller-owned transaction a statement never reaches this
		// point, and the conflict surfaces to the caller. The last conflict is
		// returned unchanged when the attempts run out.
		match &stmt {
			TopLevelStatement::Ast(sql::TopLevelExpr::Live(live)) => {
				match self.ctx.done(true)? {
					None => {}
					Some(Reason::Timedout(d)) => {
						bail!(EngineError::QueryTimedout(d.0));
					}
					Some(Reason::Canceled) => {
						bail!(EngineError::QueryCancelled);
					}
				}
				// The id is minted by this conversion, so every attempt
				// registers a clone of the one converted statement.
				let converted: crate::expr::statements::LiveStatement = (**live).clone().into();
				let mut attempt = 1;
				loop {
					if attempt > 1 {
						self.before_conflict_retry(attempt).await?;
					}
					// Each attempt compiles its unit in a read transaction of
					// its own, since an arrival is pinned to the catalog
					// version it compiled against.
					self.arrive_live(kvs, converted.clone()).await;
					let attempt_stmt =
						TopLevelStatement::Ast(sql::TopLevelExpr::Live(live.clone()));
					// A `LIVE` registration's write transaction runs without
					// cooperative yields: the statements of one connection share
					// a task and its coop budget, so a budget-forced yield
					// inside the window would let the next `LIVE` on that task
					// commit the table's definition first and fail this one at
					// commit. A storage read that is offloaded still yields.
					let result = tokio::task::unconstrained(self.execute_in_own_transaction(
						kvs,
						start,
						attempt_stmt,
						transaction_type,
					))
					.await;
					match result {
						Err(e)
							if attempt < LIVE_KILL_CONFLICT_ATTEMPTS
								&& Self::conflict_retryable(&e) =>
						{
							debug!(target: TARGET, attempt, "LIVE lost a retryable transaction conflict");
							attempt += 1;
						}
						result => return result,
					}
				}
			}
			TopLevelStatement::Ast(sql::TopLevelExpr::Kill(kill)) => {
				let mut attempt = 1;
				loop {
					if attempt > 1 {
						self.before_conflict_retry(attempt).await?;
					}
					let attempt_stmt =
						TopLevelStatement::Ast(sql::TopLevelExpr::Kill(kill.clone()));
					let result = self
						.execute_in_own_transaction(kvs, start, attempt_stmt, transaction_type)
						.await;
					match result {
						Err(e)
							if attempt < LIVE_KILL_CONFLICT_ATTEMPTS
								&& Self::conflict_retryable(&e) =>
						{
							debug!(target: TARGET, attempt, "KILL lost a retryable transaction conflict");
							attempt += 1;
						}
						result => return result,
					}
				}
			}
			_ => {}
		}
		self.execute_in_own_transaction(kvs, start, stmt, transaction_type).await
	}

	/// Whether `e` is a conflict whose attempt did not apply, which is the
	/// only failure a bare `LIVE` or `KILL` is run again for. A commit whose
	/// outcome is unknown may have applied, so it is never re-run.
	fn conflict_retryable(e: &anyhow::Error) -> bool {
		crate::kvs::is_retryable_transaction_conflict(e) && !crate::kvs::is_indeterminate_commit(e)
	}

	/// Return the executor to the state a statement starts from, then wait out
	/// the backoff, before attempt number `attempt` (2 or later) of a bare
	/// `LIVE` or `KILL`.
	///
	/// The broker the previous attempt installed is cleared: a broker still on
	/// the context is reused by the next attempt, and its receiver is gone, so
	/// the `Killed` notification of a re-run `KILL` would go into a closed
	/// channel and its registration would never be dropped. The previous
	/// attempt's arrival is dropped with it. A cancelled or timed-out query
	/// stops here instead of running again.
	async fn before_conflict_retry(&mut self, attempt: u32) -> Result<()> {
		if self.broker_owned_by_executor {
			self.clear_broker();
		}
		self.broker_owned_by_executor = false;
		self.arrived_live = None;
		match self.ctx.done(true)? {
			None => {}
			Some(Reason::Timedout(d)) => {
				bail!(EngineError::QueryTimedout(d.0));
			}
			Some(Reason::Canceled) => {
				bail!(EngineError::QueryCancelled);
			}
		}
		// Outside the `unconstrained` wrapper around a `LIVE` attempt: this
		// wait is the yield that lets a competing transaction commit.
		common::time::sleep(LIVE_KILL_CONFLICT_BACKOFF * (attempt - 1)).await;
		Ok(())
	}

	/// Run `stmt` in a transaction of `transaction_type` opened for it alone,
	/// committing a write transaction it completes and cancelling it otherwise.
	async fn execute_in_own_transaction(
		&mut self,
		kvs: &Datastore,
		start: &Instant,
		stmt: TopLevelStatement,
		transaction_type: TransactionType,
	) -> Result<Value> {
		#[cfg(test)]
		let live_or_kill = matches!(
			&stmt,
			TopLevelStatement::Ast(sql::TopLevelExpr::Live(_) | sql::TopLevelExpr::Kill(_))
		);
		let txn = Arc::new(
			kvs.transaction(transaction_type)
				.await?
				.with_tenant_identity(self.ctx.tenant_identity().cloned())
				.with_write_keys_limit(kvs.transaction_max_write_keys()),
		);
		let receiver = self.prepare_broker(
			matches!(transaction_type, TransactionType::Write),
			kvs.live_query_broker(),
		);

		let exec_result = match kvs.transaction_timeout() {
			Some(timeout) => {
				match common::time::timeout(
					timeout,
					self.execute_plan_in_transaction(Arc::clone(&txn), start, stmt),
				)
				.await
				{
					Ok(res) => res,
					Err(_) => {
						let _ = txn.cancel().await;
						bail!(DatastoreError::TransactionTimedout(timeout.into()))
					}
				}
			}
			None => self.execute_plan_in_transaction(Arc::clone(&txn), start, stmt).await,
		};

		match exec_result {
			Ok(value) | Err(ControlFlow::Return(value)) => {
				// non-writable transactions might return an error on commit.
				// So cancel them instead. This is fine since a non-writable transaction
				// has nothing to commit anyway.
				if let TransactionType::Read = transaction_type {
					let _ = txn.cancel().await;
					return Ok(value);
				}

				// Stands in for a lost commit: the writes are discarded and the
				// conflict is returned unwrapped.
				#[cfg(test)]
				if live_or_kill
					&& let Err(e) = crate::kvs::testing::maybe_inject_retryable_conflict(
						crate::kvs::testing::RetryableConflictSite::BareLiveKillCommit,
						self.ctx.node_id(),
					) {
					let _ = txn.cancel().await;
					return Err(e);
				}

				if let Err(e) = txn.commit().await {
					// Propagate a transaction conflict unwrapped so its
					// structured kind (`TransactionConflict`, wire -32009)
					// survives the conversion to a result row and SDK
					// `.retry()` can recognise it. Wrapping it in
					// `QueryNotExecuted` (as non-conflict commit failures are)
					// would erase that signal.
					if crate::kvs::is_retryable_transaction_conflict(&e) {
						return Err(e);
					}
					// A commit whose outcome is unknown propagates unwrapped for
					// the same reason: `QueryNotExecuted` asserts the work did
					// not happen, and here it may have. Callers that read that
					// as "safe to replay" would double-apply a non-idempotent
					// statement.
					if crate::kvs::is_indeterminate_commit(&e) {
						return Err(e);
					}
					bail!(DatastoreError::QueryNotExecuted {
						message: e.to_string(),
					});
				}

				// Flush buffered notifications only after the write is durable. Failed commits and
				// cancelled transactions drop the receiver without delivery.
				if let Some(prepared) = receiver {
					Self::flush_live_query_notifications(prepared);
				}

				Ok(value)
			}
			Err(ControlFlow::Continue) | Err(ControlFlow::Break) => {
				let _ = txn.cancel().await;
				bail!(ExecError::InvalidControlFlow)
			}
			Err(ControlFlow::Err(e)) => {
				let _ = txn.cancel().await;
				Err(e)
			}
		}
	}

	/// Execute the begin statement and all statements after which are within a
	/// transaction block.
	async fn execute_begin_statement<S>(
		&mut self,
		kvs: &Datastore,
		stream: Pin<&mut S>,
		grouper: &mut RequestGrouper,
		emission: Emission,
	) -> Result<()>
	where
		S: Stream<Item = Result<TopLevelStatement>>,
	{
		debug_assert!(!self.holding);
		self.broker_owned_by_executor = false;
		self.holding = matches!(emission, Emission::Hold) && self.stream.is_some();
		let result = self.execute_begin_statement_impl(kvs, stream, grouper).await;
		if self.broker_owned_by_executor {
			self.clear_broker();
		}
		self.holding = false;
		// The transaction block has ended, committed or cancelled.
		if let Some(memory) = self.ctx.memory() {
			memory.transaction_ended();
		}
		result
	}

	async fn execute_begin_statement_impl<S>(
		&mut self,
		kvs: &Datastore,
		mut stream: Pin<&mut S>,
		grouper: &mut RequestGrouper,
	) -> Result<()>
	where
		S: Stream<Item = Result<TopLevelStatement>>,
	{
		let Ok(txn) = kvs.transaction(TransactionType::Write).await.map(|tx| {
			tx.with_tenant_identity(self.ctx.tenant_identity().cloned())
				.with_write_keys_limit(kvs.transaction_max_write_keys())
		}) else {
			// couldn't create a transaction.
			// Fast forward until we hit CANCEL or COMMIT
			while let Some(stmt) = stream.next().await {
				yield_now!();
				let stmt = stmt?;
				let place = place_statement(grouper, &stmt);
				if let StatementRole::Cancel | StatementRole::Commit = place.role {
					return Ok(());
				}

				let kind = stmt.statement_type();
				self.results.push(QueryResult {
					time: Duration::ZERO,
					result: Err(TypesError::query(
						"Tried to start a transaction while another transaction was open"
							.to_string(),
						Some(QueryError::NotExecuted),
					)),
					query_type: QueryType::Other,
				});
				self.emit_statement_event_unexecuted(
					kvs,
					kind,
					crate::observe::error_class::TXN_CREATE_FAILED,
				);
			}

			// Ran out of statements but still didn't hit a COMMIT or CANCEL
			// Just break as we can't do anything else since the query is already
			// effectively canceled.
			return Ok(());
		};
		let txn = Arc::new(txn);
		self.in_block = true;
		let ran = self.execute_begin_statement_timed(kvs, txn, stream, grouper).await;
		self.in_block = false;
		ran
	}

	/// [`Self::execute_begin_statement_impl`] once the block's transaction is
	/// open, bounded by the datastore's transaction timeout.
	async fn execute_begin_statement_timed<S>(
		&mut self,
		kvs: &Datastore,
		txn: Arc<Transaction>,
		stream: Pin<&mut S>,
		grouper: &mut RequestGrouper,
	) -> Result<()>
	where
		S: Stream<Item = Result<TopLevelStatement>>,
	{
		match kvs.transaction_timeout() {
			Some(timeout) => {
				let start_results = self.results.len();
				match common::time::timeout(
					timeout,
					self.execute_begin_statement_inner(kvs, Arc::clone(&txn), stream, grouper),
				)
				.await
				{
					Ok(result) => result,
					Err(_) => {
						let _ = txn.cancel().await;
						let timed_out_count = self.results.len().saturating_sub(start_results);
						for res in &mut self.results[start_results..] {
							res.query_type = QueryType::Other;
							res.result = Err(TypesError::query(
								format!(
									"The transaction timed out: {}",
									crate::val::Duration::from(timeout)
								),
								Some(QueryError::TimedOut {
									duration: timeout,
								}),
							));
						}
						// Emit one statement event per result that the
						// timeout retroactively turned into a failure.
						// We don't have the original statement kind any
						// more (the ast was consumed), so collapse to
						// `Other`; consumers care primarily about the
						// `error_class` and the `outcome` here.
						for _ in 0..timed_out_count {
							self.emit_statement_event_unexecuted(
								kvs,
								StatementType::Other,
								crate::observe::error_class::TXN_TIMEOUT,
							);
						}
						bail!(DatastoreError::TransactionTimedout(timeout.into()))
					}
				}
			}
			None => self.execute_begin_statement_inner(kvs, txn, stream, grouper).await,
		}
	}

	async fn execute_begin_statement_inner<S>(
		&mut self,
		kvs: &Datastore,
		txn: Arc<Transaction>,
		mut stream: Pin<&mut S>,
		grouper: &mut RequestGrouper,
	) -> Result<()>
	where
		S: Stream<Item = Result<TopLevelStatement>>,
	{
		// Create a sender for this transaction only if the context allows for
		// notifications.
		let receiver = self.prepare_broker(true, kvs.live_query_broker());
		let start_results = self.results.len();
		let mut skip_remaining = false;

		// loop over the statements until we hit a cancel or a commit statement.
		while let Some(stmt) = stream.next().await {
			// A block whose consumer has gone away is abandoned mid-way, which
			// falls through to the missing-COMMIT tail below and cancels the
			// transaction. Rolling back is the safe reading of "nobody is
			// waiting for this": the alternative is committing writes for a
			// query the caller stopped listening to.
			if self.consumer_gone() {
				break;
			}
			yield_now!();
			let stmt = match stmt {
				Ok(x) => x,
				Err(e) => {
					// make sure the transaction is properly canceled.
					let _ = txn.cancel().await;
					return Err(e);
				}
			};
			let place = place_statement(grouper, &stmt);

			// check for timeout and cancellation.
			//
			// `done` reports a timeout or cancellation as `Ok(Some(reason))` and a
			// crossed memory threshold as `Err`. Both must cancel the transaction
			// before unwinding: an abandoned transaction keeps its buffered writes
			// alive, so returning the memory error without cancelling holds on to
			// the very memory the threshold check exists to release.
			let done = match self.ctx.done(true) {
				Ok(done) => done,
				Err(e) => {
					let _ = txn.cancel().await;
					// The rollback discards the statements already recorded as
					// successful, so their results have to say so.
					for res in &mut self.results[start_results..] {
						res.query_type = QueryType::Other;
						res.result = Err(TypesError::query(
							"The query was not executed due to a cancelled transaction".to_string(),
							Some(QueryError::Cancelled),
						));
					}
					return Err(e);
				}
			};
			if let Some(done) = done {
				// A cancellation happened. Cancel the transaction, fast-forward the remaining
				// results and then return.
				let _ = txn.cancel().await;

				let cancelled_count = self.results.len().saturating_sub(start_results);
				for res in &mut self.results[start_results..] {
					res.query_type = QueryType::Other;
					res.result = Err(TypesError::query(
						"The query was not executed due to a cancelled transaction".to_string(),
						Some(QueryError::Cancelled),
					));
				}
				let cancel_class = match done {
					Reason::Timedout(_) => crate::observe::error_class::CTX_TIMEOUT,
					Reason::Canceled => crate::observe::error_class::CTX_CANCELLED,
				};
				for _ in 0..cancelled_count {
					self.emit_statement_event_unexecuted(kvs, StatementType::Other, cancel_class);
				}

				while let Some(stmt) = stream.next().await {
					yield_now!();
					let stmt = stmt?;
					let place = place_statement(grouper, &stmt);
					let kind = stmt.statement_type();
					// The statement that was pulled when `done` was observed may itself
					// have closed the block in the grouping, and it is dropped here. The
					// COMMIT or CANCEL that ends this fast-forward is then the next one
					// in the text, which the grouper sees outside a block, so both of
					// its roles stand for the end of the block.
					match place.role {
						StatementRole::Commit
						| StatementRole::Misplaced(MisplacedControl::CommitOutsideBlock) => {
							// After timeout/cancel the txn is already gone: COMMIT cannot succeed.
							// Still emit one `QueryResult` for this COMMIT statement so the batch
							// has one row per statement (mirrors successful COMMIT, which
							// pushes Ok(NONE) in the main `TopLevelExpr::Commit` branch
							// below) (#7207).
							self.results.push(QueryResult {
								time: Duration::ZERO,
								result: Err(match done {
									Reason::Timedout(d) => TypesError::query(
										format!("Cannot COMMIT: timed out ({d})"),
										Some(QueryError::TimedOut {
											duration: d.0,
										}),
									),
									Reason::Canceled => TypesError::query(
										"Cannot COMMIT: the transaction was cancelled".to_string(),
										Some(QueryError::Cancelled),
									),
								}),
								query_type: QueryType::Other,
							});
							self.emit_statement_event_unexecuted(kvs, kind, cancel_class);
							return Ok(());
						}
						role => {
							let result = Err(match done {
								Reason::Timedout(d) => TypesError::query(
									format!("Timed out: {d}"),
									Some(QueryError::TimedOut {
										duration: d.0,
									}),
								),
								Reason::Canceled => TypesError::query(
									"The query was not executed due to a cancelled transaction"
										.to_string(),
									Some(QueryError::Cancelled),
								),
							});
							self.results.push(QueryResult {
								time: Duration::ZERO,
								result,
								query_type: QueryType::Other,
							});
							self.emit_statement_event_unexecuted(kvs, kind, cancel_class);
							if matches!(
								role,
								StatementRole::Cancel
									| StatementRole::Misplaced(
										MisplacedControl::CancelOutsideBlock
									)
							) {
								return Ok(());
							}
						}
					}
				}

				// Missing CANCEL/COMMIT statement, statement already canceled so nothing todo.
				return Ok(());
			}

			// A statement skipped after a `RETURN` is skipped whatever the
			// grouping made of it, so a `BEGIN` the block never reaches does not
			// abort it; only the statement that ends the block is still run.
			if skip_remaining
				&& !matches!(place.role, StatementRole::Commit | StatementRole::Cancel)
			{
				continue;
			}

			trace!(target: TARGET, statement = %stmt.to_sql(), "Executing statement");

			let query_type = stmt.query_type();

			// Capture classification up-front so we can emit a
			// `StatementEvent` regardless of which control-flow branch the
			// match below takes. The statement itself is moved into the
			// match, so anything the observer needs must be derived here.
			let statement_type = stmt.statement_type();
			let statement_read_only = stmt.read_only();
			let sql_text = kvs.observer().needs_statement_text().then(|| stmt.to_sql());

			let before = Instant::now();
			// Row count populated by the `stmt =>` arm below before it
			// consumes the internal `Value`. Every other arm of the
			// match either returns early or contributes a non-DML
			// statement, so 0 is the right default.
			let mut stmt_result_rows: u64 = 0;
			let result = match place.role {
				// An open block holds no `BEGIN` that opens another: the grouping
				// marks it nested.
				StatementRole::Begin => {
					debug_assert!(false, "a BEGIN inside an open block is marked nested");
					let _ = txn.cancel().await;
					bail!(EngineError::unreachable(
						"a BEGIN inside an open block was not marked nested by request lowering"
					));
				}
				StatementRole::Misplaced(misplaced) => {
					debug_assert_eq!(misplaced, MisplacedControl::NestedBegin);
					let _ = txn.cancel().await;
					// tried to begin a transaction within a transaction.

					for res in &mut self.results[start_results..] {
						res.query_type = QueryType::Other;
						res.result = Err(TypesError::query(
							"The query was not executed due to a failed transaction".to_string(),
							Some(QueryError::NotExecuted),
						));
					}

					self.results.push(QueryResult {
						time: Duration::ZERO,
						result: Err(TypesError::internal(misplaced.message().to_string())),
						query_type: QueryType::Other,
					});

					self.emit_statement_event_cached(
						kvs,
						statement_type,
						statement_read_only,
						sql_text,
						&before,
						Outcome::Error,
						0,
						Some(crate::observe::error_class::INTERNAL),
					);

					while let Some(stmt) = stream.next().await {
						yield_now!();
						let stmt = stmt?;
						let place = place_statement(grouper, &stmt);
						match place.role {
							StatementRole::Commit => {
								self.results.push(QueryResult {
									time: Duration::ZERO,
									result: Err(TypesError::query(
										"Cannot COMMIT: the transaction was aborted due to a nested BEGIN"
											.to_string(),
										Some(QueryError::NotExecuted),
									)),
									query_type: QueryType::Other,
								});
								return Ok(());
							}
							role => {
								self.results.push(QueryResult {
									time: Duration::ZERO,
									result: Err(TypesError::query(
										"The query was not executed due to a failed transaction"
											.to_string(),
										Some(QueryError::NotExecuted),
									)),
									query_type: QueryType::Other,
								});
								if matches!(role, StatementRole::Cancel) {
									return Ok(());
								}
							}
						}
					}

					// Missing CANCEL/COMMIT statement, statement already canceled so nothing todo.
					return Ok(());
				}
				StatementRole::Cancel => {
					let _ = txn.cancel().await;

					// update the results indicating cancelation.
					for res in &mut self.results[start_results..] {
						res.query_type = QueryType::Other;
						res.result = Err(TypesError::query(
							"The query was not executed due to a cancelled transaction".to_string(),
							Some(QueryError::Cancelled),
						));
					}

					// CANCEL returns NONE
					self.results.push(QueryResult {
						time: before.elapsed(),
						result: Ok(convert_value_to_public_value(Value::None)?),
						query_type: QueryType::Other,
					});

					self.emit_statement_event_cached(
						kvs,
						statement_type,
						statement_read_only,
						sql_text,
						&before,
						Outcome::Success,
						0,
						None,
					);

					return Ok(());
				}
				StatementRole::Commit => {
					// Commit the transaction.
					// If error undo results.
					let e = if let Err(e) = txn.commit().await {
						e
					} else {
						// Successfully commited. everything is fine.

						// Flush buffered notifications only after COMMIT succeeds. Rollback and
						// failed COMMIT paths drop the receiver without delivery.
						if let Some(prepared) = receiver {
							Self::flush_live_query_notifications(prepared);
						}

						// COMMIT returns NONE
						self.results.push(QueryResult {
							time: before.elapsed(),
							result: Ok(convert_value_to_public_value(Value::None)?),
							query_type: QueryType::Other,
						});

						self.emit_statement_event_cached(
							kvs,
							statement_type,
							statement_read_only,
							sql_text,
							&before,
							Outcome::Success,
							0,
							None,
						);

						return Ok(());
					};

					// `txn.commit()` failed (e.g. constraint on commit, or txn already finished).
					// Surface the failure on a dedicated COMMIT result row; mark prior statement
					// slots as not executed so nothing implies a successful commit (#7207).
					//
					// Unless the commit's outcome is unknown, in which case "not
					// executed" is a false statement about statements that may
					// well have been applied.
					let indeterminate = crate::kvs::is_indeterminate_commit(&e);
					for res in &mut self.results[start_results..] {
						res.query_type = QueryType::Other;
						res.result = Err(if indeterminate {
							TypesError::query(
								"The transaction's commit outcome is unknown, so whether this \
								 query was applied is undetermined"
									.to_string(),
								None,
							)
						} else {
							TypesError::query(
								"The query was not executed due to a failed transaction"
									.to_string(),
								Some(QueryError::NotExecuted),
							)
						});
					}

					// A retryable write conflict keeps its structured
					// TransactionConflict kind (wire -32009) so SDK
					// `isRetryableConflict()`/`.retry()` fire. An unknown
					// outcome carries no kind, so neither the conflict-retry
					// path nor a not-executed reading applies. Anything else
					// (constraint failure, txn already finished) stays
					// NotExecuted.
					let commit_error_kind = if crate::kvs::is_retryable_transaction_conflict(&e) {
						Some(QueryError::TransactionConflict)
					} else if indeterminate {
						None
					} else {
						Some(QueryError::NotExecuted)
					};
					self.results.push(QueryResult {
						time: before.elapsed(),
						result: Err(TypesError::query(
							format!("Cannot COMMIT: {e}"),
							commit_error_kind,
						)),
						query_type: QueryType::Other,
					});

					// `Cannot COMMIT` surfaces as a caller-visible failure ("the
					// transaction your statements ran inside could not
					// commit"), not an internal fault. An unknown outcome still
					// reaches storage alerting through the `surrealdb.transaction.*`
					// family, which classifies the same error independently.
					self.emit_statement_event_cached(
						kvs,
						statement_type,
						statement_read_only,
						sql_text,
						&before,
						Outcome::Error,
						0,
						Some(crate::observe::error_class::CLIENT),
					);

					return Ok(());
				}
				StatementRole::Option => {
					let Dispatch::Option(option) = stmt.dispatch() else {
						let _ = txn.cancel().await;
						bail!(EngineError::unreachable(
							"a statement placed as OPTION did not dispatch as OPTION"
						));
					};
					match self.execute_option_statement(&option) {
						Ok(_) => {
							// OPTION returns NONE
							self.results.push(QueryResult {
								time: before.elapsed(),
								result: Ok(convert_value_to_public_value(Value::None)?),
								query_type: QueryType::Other,
							});
							self.emit_statement_event_cached(
								kvs,
								statement_type,
								statement_read_only,
								sql_text,
								&before,
								Outcome::Success,
								0,
								None,
							);
							continue;
						}
						Err(e) => Err(TypesError::internal(e.to_string())),
					}
				}
				StatementRole::Run => {
					// Install fresh per-statement counters so DML
					// iterators inside this BEGIN/COMMIT block can
					// surface affected-row counts independently of the
					// post-RETURN value shape.
					let counters = self.install_statement_counters();
					let statement_index = self.current_index();
					let r: Result<Value> = match self
						.execute_plan_in_transaction(Arc::clone(&txn), &before, stmt)
						.await
					{
						Ok(x) => Ok(x),
						Err(ControlFlow::Return(value)) => {
							skip_remaining = true;
							Ok(value)
						}
						// A `BREAK` or `CONTINUE` no loop of the statement caught is
						// the statement's error, which fails the block as any other
						// does.
						Err(
							flow @ (ControlFlow::Err(_)
							| ControlFlow::Break
							| ControlFlow::Continue),
						) => {
							let e = match flow {
								ControlFlow::Err(e) => e,
								_ => anyhow!(ExecError::InvalidControlFlow),
							};
							for res in &mut self.results[start_results..] {
								res.query_type = QueryType::Other;
								res.result = Err(TypesError::query(
									"The query was not executed due to a failed transaction"
										.to_string(),
									Some(QueryError::NotExecuted),
								));
							}

							// Convert the anyhow error before pushing so we can both
							// classify it for the metric attribute and store it on the
							// result row in a single move.
							let typed_err = types_error_from_anyhow(e);
							let error_class =
								Some(crate::observe::error_class::classify_types_error(&typed_err));

							// statement return an error. Consume all the other statement until
							// we hit a cancel or commit.
							self.results.push(QueryResult {
								time: before.elapsed(),
								result: Err(typed_err),
								query_type,
							});

							self.emit_statement_event_cached(
								kvs,
								statement_type,
								statement_read_only,
								sql_text,
								&before,
								Outcome::Error,
								0,
								error_class,
							);

							let _ = txn.cancel().await;

							while let Some(stmt) = stream.next().await {
								yield_now!();
								let stmt = stmt?;
								let place = place_statement(grouper, &stmt);
								match place.role {
									StatementRole::Commit => {
										// Aborted txn: COMMIT must error (same intent as
										// `txn.commit()` failure above — descriptive
										// `Cannot COMMIT:` prefix) (#7207).
										self.results.push(QueryResult {
												time: Duration::ZERO,
												result: Err(TypesError::query(
													"Cannot COMMIT: the transaction was aborted due to a prior error"
														.to_string(),
													Some(QueryError::NotExecuted),
												)),
												query_type: QueryType::Other,
											});
										return Ok(());
									}
									StatementRole::Cancel => {
										return Ok(());
									}
									_ => {
										self.results.push(QueryResult {
												time: Duration::ZERO,
												result: Err(TypesError::query(
													"The query was not executed due to a cancelled transaction"
														.to_string(),
													Some(QueryError::Cancelled),
												)),
												query_type: QueryType::Other,
											});
									}
								}
							}

							// ran out of statements before the transaction ended.
							// Just break as we have nothing else we can do.
							return Ok(());
						}
					};

					// Count rows from the internal Value before it's
					// consumed by the conversion to PublicValue. Non-DML
					// statement kinds collapse to 0 inside the helper;
					// DML statements consult the iterator-side counter
					// so RETURN NONE / fresh CREATE etc. report
					// accurately.
					// A streamed statement's rows never reach its value, so the
					// count comes from what the sink sent rather than from the
					// value shape, which would read as zero.
					let rows = match self.streamed_rows(statement_index) {
						Some(streamed) => streamed,
						None => Self::count_result_rows(
							statement_type,
							Some(counters.as_ref()),
							r.as_ref().ok(),
						),
					};
					stmt_result_rows = rows;

					match r {
						Ok(value) => match self.convert_statement_result(value) {
							Ok(value) => Ok(value),
							Err(err) => {
								// This returns from the function with the
								// transaction still open, so cancel it first:
								// an abandoned transaction is never rolled
								// back and holds its buffered writes.
								let _ = txn.cancel().await;
								return Err(err);
							}
						},
						Err(err) => Err(TypesError::internal(err.to_string())),
					}
				}
			};

			let outcome = Outcome::from(&result);
			let error_class =
				result.as_ref().err().map(crate::observe::error_class::classify_types_error);
			self.emit_statement_event_cached(
				kvs,
				statement_type,
				statement_read_only,
				sql_text,
				&before,
				outcome,
				stmt_result_rows,
				error_class,
			);

			self.results.push(QueryResult {
				time: before.elapsed(),
				result,
				query_type,
			});
		}

		// we ran out of query but we still have an open transaction.
		// Be conservative and treat this essentially as a CANCEL statement.
		let _ = txn.cancel().await;

		for res in &mut self.results[start_results..] {
			res.query_type = QueryType::Other;
			res.result = Err(TypesError::internal("Missing COMMIT statement".to_string()));
		}

		Ok(())
	}

	#[cfg_attr(
		feature = "observability",
		tracing::instrument(
			level = "debug",
			name = "executor",
			target = "surrealdb::core::dbs",
			skip_all
		)
	)]
	pub(crate) async fn execute_statements(
		kvs: &Datastore,
		ctx: FrozenContext,
		opt: Options,
		statements: Vec<TopLevelStatement>,
	) -> Result<Vec<QueryResult>> {
		let mut this = Executor::new(kvs, ctx, opt);
		this.prepare_request(&statements);
		let stream = this.counted(futures::stream::iter(statements.into_iter().map(Ok)));
		crate::exec::compiles::request(this.run_statement_stream(kvs, false, stream)).await?;
		Ok(this.results)
	}

	/// Execute a batch of statements with an existing transaction
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(
			level = "debug",
			name = "executor",
			target = "surrealdb::core::dbs",
			skip_all
		)
	)]
	pub(crate) async fn execute_statements_with_transaction(
		kvs: &Datastore,
		ctx: FrozenContext,
		opt: Options,
		statements: Vec<TopLevelStatement>,
	) -> Result<Vec<QueryResult>> {
		let mut executor = Self::new(kvs, ctx, opt);
		executor.prepare_request(&statements);
		crate::exec::compiles::request(executor.run_with_transaction(kvs, statements)).await?;
		Ok(executor.results)
	}

	/// Execute a logical plan with an existing transaction, streaming results
	/// into `items` as they are produced.
	///
	/// The transaction spans requests and only its owner can commit it, so a
	/// statement's result is final as soon as the statement ends — there is no
	/// block resolution to wait for, and whether the caller later commits is
	/// its own affair, exactly as on the buffered path.
	pub(crate) async fn execute_statements_streaming_with_transaction(
		kvs: &Datastore,
		ctx: FrozenContext,
		opt: Options,
		statements: Vec<TopLevelStatement>,
		items: Sender<QueryStreamItem>,
		row_format: Option<surrealdb_rpc::RowFormat>,
	) -> Result<Vec<QueryResult>> {
		let mut executor = Self::new_streaming(kvs, ctx, opt, items, row_format);
		executor.prepare_request(&statements);
		let outcome =
			crate::exec::compiles::request(executor.run_with_transaction(kvs, statements)).await;
		executor.finalise_results().await;
		outcome.map(|()| executor.results)
	}

	/// Shared body: run every statement against the transaction already on the
	/// context, accumulating results on `self` and streaming them when this
	/// executor streams.
	async fn run_with_transaction(
		&mut self,
		kvs: &Datastore,
		statements: Vec<TopLevelStatement>,
	) -> Result<()> {
		// The transaction is already set in the context
		// Execute each expression with the transaction
		let tx = self.ctx.tx();
		// The caller commits this transaction, so buffered notifications are flushed by a
		// commit action rather than when the batch ends. The broker stays installed for the
		// whole batch: every statement runs through `execute_plan_in_transaction`, which
		// never clears it, and the context dies with this executor.
		if let Some(prepared) = self.prepare_broker(tx.writeable(), kvs.live_query_broker()) {
			tx.on_commit(Box::new(FlushOnCommit(prepared))).await;
		}
		let batch_start = Instant::now();
		let results_start = self.results.len();
		// Every statement shares the transaction the caller holds, so each
		// transaction-control statement is marked misplaced.
		let mut grouper = RequestGrouper::new(true);

		// Every statement runs in the caller's transaction, as a block's do.
		self.in_block = true;
		for stmt in statements {
			// See `consumer_gone`: the rest of the batch is abandoned rather
			// than run. The transaction spans requests and its owner still
			// decides whether to commit.
			if self.consumer_gone() {
				break;
			}
			let place = place_statement(&mut grouper, &stmt);
			if let StatementRole::Misplaced(misplaced) = place.role {
				self.misplaced_control = Some(misplaced);
			}
			if let Some(request) = &self.request {
				request.taken.fetch_add(1, Ordering::Relaxed);
			}
			let start = Instant::now();
			// Capture classification before the statement is moved
			// into `execute_plan_in_transaction` so we can emit a
			// matching `StatementEvent` regardless of which control-flow
			// branch the result lands in (mirrors the cached helper
			// pattern used in `run_statement_stream`).
			let statement_type = stmt.statement_type();
			let statement_read_only = stmt.kind_reads_only();
			// A `LIVE SELECT` run inside a caller-owned transaction has to
			// report itself as one: the transports register the subscription
			// off this field, and one they never see is one they never tear
			// down when the session ends or its principal changes.
			let query_type = stmt.query_type();
			let counters = self.install_statement_counters();
			let result = self.execute_plan_in_transaction(Arc::clone(&tx), &start, stmt).await;
			self.misplaced_control = None;

			let time = start.elapsed();
			let query_result = match result {
				Ok(value) | Err(ControlFlow::Return(value)) => QueryResult {
					time,
					result: self
						.convert_statement_result(value)
						.map_err(|e| TypesError::internal(e.to_string())),
					query_type,
				},
				Err(ControlFlow::Err(e)) => QueryResult {
					time,
					result: Err(types_error_from_anyhow(e)),
					query_type: QueryType::Other,
				},
				Err(ControlFlow::Continue) | Err(ControlFlow::Break) => QueryResult {
					time,
					result: Err(TypesError::internal("Invalid control flow".to_string())),
					query_type: QueryType::Other,
				},
			};
			let outcome = Outcome::from(&query_result.result);
			let error_class = query_result
				.result
				.as_ref()
				.err()
				.map(crate::observe::error_class::classify_types_error);
			// Counters are accurate for DML (SELECT row counts come
			// from the value shape, which we no longer have post
			// conversion — passing `None` is fine since the helper
			// short-circuits to counter-driven counts on DML and
			// returns 0 on non-DML when value is `None`, matching the
			// previous behaviour for this entry point).
			let stmt_result_rows =
				Self::count_result_rows(statement_type, Some(counters.as_ref()), None);
			self.emit_statement_event_cached(
				kvs,
				statement_type,
				statement_read_only,
				None,
				&start,
				outcome,
				stmt_result_rows,
				error_class,
			);
			self.results.push(query_result);
			// The statement is over and this transaction spans requests, so
			// nothing later in this plan can rewrite it.
			self.finalise_results().await;
		}

		self.emit_query_event_for_results(kvs, batch_start, &self.results[results_start..]);
		Ok(())
	}

	#[cfg_attr(
		feature = "observability",
		tracing::instrument(
			level = "debug",
			name = "executor",
			target = "surrealdb::core::dbs",
			skip_all
		)
	)]
	pub(crate) async fn execute_stream<S>(
		kvs: &Datastore,
		ctx: FrozenContext,
		opt: Options,
		skip_success_results: bool,
		stream: S,
	) -> Result<Vec<QueryResult>>
	where
		S: Stream<Item = Result<sql::TopLevelExpr>>,
	{
		Self::execute_statement_stream(
			kvs,
			ctx,
			opt,
			skip_success_results,
			stream.map(|x| x.map(TopLevelStatement::Ast)),
		)
		.await
	}

	#[cfg_attr(feature = "observability", tracing::instrument(
		level = "debug",
		name = "executor",
		target = "surrealdb::core::dbs",
		skip_all,
		fields(
			// Pre-declared placeholder fields populated by the
			// enterprise OTEL enrichment observer. Tracing's
			// `Span::record(field, value)` only updates fields that
			// were declared at span-creation time; declaring them as
			// `tracing::field::Empty` here lets the observer fill in
			// tenant attributes without touching every executor
			// internal. Community builds pay zero cost — the
			// enrichment observer never attaches and the placeholders
			// stay empty.
			surrealdb.namespace = tracing::field::Empty,
			surrealdb.database = tracing::field::Empty,
			surrealdb.user = tracing::field::Empty,
			surrealdb.session_id = tracing::field::Empty,
			surrealdb.statement_type = tracing::field::Empty,
		),
	))]
	pub(crate) async fn execute_statement_stream<S>(
		kvs: &Datastore,
		ctx: FrozenContext,
		opt: Options,
		skip_success_results: bool,
		stream: S,
	) -> Result<Vec<QueryResult>>
	where
		S: Stream<Item = Result<TopLevelStatement>>,
	{
		let mut this = Executor::new(kvs, ctx, opt);
		this.run_statement_stream(kvs, skip_success_results, stream).await?;
		Ok(this.results)
	}

	/// Execute a batch of statements, sending results into `items` as they are
	/// produced rather than collecting them.
	///
	/// Returns the results as well, so an embedded caller that wants both the
	/// streamed items and the assembled answer pays for the query once.
	pub(crate) async fn execute_statements_streaming(
		kvs: &Datastore,
		ctx: FrozenContext,
		opt: Options,
		statements: Vec<TopLevelStatement>,
		items: Sender<QueryStreamItem>,
		row_format: Option<surrealdb_rpc::RowFormat>,
		held_transactions: bool,
	) -> Result<Vec<QueryResult>> {
		let mut this = Executor::new_streaming(kvs, ctx, opt, items, row_format);
		if held_transactions {
			this.explicit_emission = Emission::Hold;
		}
		this.prepare_request(&statements);
		let stream = this.counted(futures::stream::iter(statements.into_iter().map(Ok)));
		let outcome =
			crate::exec::compiles::request(this.run_statement_stream(kvs, false, stream)).await;
		// However the execution ended, every result it produced is now final.
		// A consumer is owed a terminal item for each one, including on the
		// paths that returned an error before the loop finalised them.
		this.finalise_results().await;
		outcome.map(|()| this.results)
	}

	/// Execute every statement in `stream`, accumulating results and, when this
	/// executor streams, sending them onward as they become final.
	///
	/// The results stay on `self` rather than being returned so that a caller
	/// can finalise whatever was produced even on the paths that fail — a
	/// streaming consumer is owed a terminal item for every statement that
	/// produced a result, however the execution ended.
	async fn run_statement_stream<S>(
		&mut self,
		kvs: &Datastore,
		skip_success_results: bool,
		stream: S,
	) -> Result<()>
	where
		S: Stream<Item = Result<TopLevelStatement>>,
	{
		// Capture batch boundaries up-front so the `QueryEvent` emitted
		// at every return path measures the full span of the batch even
		// when an early parse error short-circuits the loop. The slice
		// of `QueryResult`s the executor produced is the source of
		// truth for the per-batch counters.
		let batch_start = Instant::now();
		let batch_results_start = self.results.len();
		let mut stream = pin!(stream);
		// Groups the statements as they are pulled: the stream cannot be read
		// ahead of time, and import cannot be lowered ahead of time.
		let mut grouper = RequestGrouper::new(false).with_explicit_emission(self.explicit_emission);

		if skip_success_results {
			// The import path requires OPTION IMPORT as the first statement.
			// This sets opt.import which skips events, live queries, field
			// processing, table views, and result output for performance.
			match stream.as_mut().next().await.map(|stmt| {
				stmt.map(|stmt| {
					place_statement(&mut grouper, &stmt);
					stmt.dispatch()
				})
			}) {
				Some(Ok(Dispatch::Option(ref stmt)))
					if stmt.name.eq_ignore_ascii_case("IMPORT") && stmt.what =>
				{
					self.execute_option_statement(stmt)?;
				}
				Some(Err(e)) => {
					bail!(ExecError::InvalidStatement(e.to_string()));
				}
				_ => {
					bail!(ExecError::InvalidStatement(
						"Import requires `OPTION IMPORT;` as the first statement. \
						 This disables events, live queries, field processing, and result \
						 output for optimal import performance. To execute queries with \
						 full side effects, use the /sql endpoint instead."
							.to_string()
					));
				}
			}
		}

		while let Some(stmt) = stream.next().await {
			// Nothing is reading the results any more, so the statements after
			// this one are abandoned rather than run.
			if self.consumer_gone() {
				break;
			}
			let stmt = match stmt {
				Ok(x) => x,
				Err(e) => {
					self.results.push(QueryResult {
						time: Duration::ZERO,
						result: Err(TypesError::internal(e.to_string())),
						query_type: QueryType::Other,
					});

					self.emit_query_event_for_results(
						kvs,
						batch_start,
						&self.results[batch_results_start..],
					);
					return Ok(());
				}
			};

			// Capture classification up-front so we can emit a
			// `StatementEvent` from whichever branch of the match consumes
			// `stmt`. None of these helpers retain any data that could leak
			// customer values to an unauthenticated sink -- `StatementType`
			// is a bounded enum and `to_sql` is only invoked when the
			// installed observer explicitly opts in.
			let place = place_statement(&mut grouper, &stmt);
			let statement_type = stmt.statement_type();
			let statement_read_only = stmt.read_only();
			let sql_text = kvs.observer().needs_statement_text().then(|| stmt.to_sql());
			let start = Instant::now();

			match place.role {
				StatementRole::Option => {
					let Dispatch::Option(stmt) = stmt.dispatch() else {
						bail!(EngineError::unreachable(
							"a statement placed as OPTION did not dispatch as OPTION"
						));
					};
					if skip_success_results && stmt.name.eq_ignore_ascii_case("IMPORT") {
						bail!(ExecError::InvalidStatement(
							"Cannot change OPTION IMPORT during an import stream. \
						 Import mode is locked for the duration of the /import request."
								.to_string()
						));
					}
					let result = self.execute_option_statement(&stmt);
					let outcome = Outcome::from(&result);
					// `execute_option_statement` returns `anyhow::Result`; the
					// only failure today is a permission denial via the
					// capability gate (`is_allowed(..., Action::Edit,
					// ResourceKind::Option, ...)`), so the bounded error class
					// is `permission`, not the generic `client` bucket.
					let error_class =
						result.as_ref().err().map(|_| crate::observe::error_class::PERMISSION);
					self.emit_statement_event_cached(
						kvs,
						statement_type,
						statement_read_only,
						sql_text,
						&start,
						outcome,
						0,
						error_class,
					);
					result?;
					if !skip_success_results {
						self.results.push(QueryResult {
							time: Duration::ZERO,
							result: Ok(convert_value_to_public_value(Value::None)?),
							query_type: QueryType::Other,
						});
					}
				}
				StatementRole::Begin => {
					if !skip_success_results {
						self.results.push(QueryResult {
							time: Duration::ZERO,
							result: Ok(convert_value_to_public_value(Value::None)?),
							query_type: QueryType::Other,
						});
					}

					let begin_result = self
						.execute_begin_statement(kvs, stream.as_mut(), &mut grouper, place.emission)
						.await;
					let outcome = Outcome::from(&begin_result);
					// `execute_begin_statement` returns `anyhow::Result`; on
					// failure the result is wrapped via
					// `types_error_from_anyhow` below (see the `if let
					// Err(e)` arm). We can't classify before that conversion
					// because we'd consume the error twice, so reach for the
					// `txn_create_failed` constant: the only way
					// `execute_begin_statement` errors today is when the
					// transaction creation itself failed.
					let error_class = begin_result
						.as_ref()
						.err()
						.map(|_| crate::observe::error_class::TXN_CREATE_FAILED);
					self.emit_statement_event_cached(
						kvs,
						statement_type,
						statement_read_only,
						sql_text,
						&start,
						outcome,
						0,
						error_class,
					);

					if let Err(e) = begin_result {
						self.results.push(QueryResult {
							time: Duration::ZERO,
							result: Err(types_error_from_anyhow(e)),
							query_type: QueryType::Other,
						});

						self.emit_query_event_for_results(
							kvs,
							batch_start,
							&self.results[batch_results_start..],
						);
						return Ok(());
					}
				}
				role @ (StatementRole::Misplaced(_)
				| StatementRole::Commit
				| StatementRole::Cancel
				| StatementRole::Run) => {
					// With no block open, a `COMMIT` or `CANCEL` is marked
					// misplaced and reaches the engine with its mark.
					debug_assert!(!matches!(role, StatementRole::Commit | StatementRole::Cancel));
					if let StatementRole::Misplaced(misplaced) = role {
						self.misplaced_control = Some(misplaced);
					}
					let query_type: QueryType = stmt.query_type();

					// Install fresh per-statement counters so DML
					// iterators can record affected rows independently of
					// the post-RETURN value shape.
					let counters = self.install_statement_counters();
					let statement_index = self.current_index();
					let result = self.execute_bare_statement(kvs, &start, stmt).await;
					self.misplaced_control = None;
					let outcome = Outcome::from(&result);
					// A streamed statement's rows never reach its value, so the
					// count comes from what the sink sent rather than from the
					// value shape, which would read as zero.
					let result_rows = match self.streamed_rows(statement_index) {
						Some(streamed) => streamed,
						None => Self::count_result_rows(
							statement_type,
							Some(counters.as_ref()),
							result.as_ref().ok(),
						),
					};
					let error_class = result.as_ref().err().map(classify_anyhow_error);
					self.emit_statement_event_cached(
						kvs,
						statement_type,
						statement_read_only,
						sql_text,
						&start,
						outcome,
						result_rows,
						error_class,
					);

					if skip_success_results {
						if let Err(err) = result {
							self.results.push(QueryResult {
								time: start.elapsed(),
								result: Err(types_error_from_anyhow(err)),
								query_type,
							});
						}
					} else {
						let result = match result {
							// A conversion failure lands in this statement's own
							// result slot, so one bad result does not propagate out
							// and wipe the sibling statements' results too.
							Ok(value) => self
								.convert_statement_result(value)
								.map_err(types_error_from_anyhow),
							Err(err) => Err(types_error_from_anyhow(err)),
						};
						self.results.push(QueryResult {
							time: start.elapsed(),
							result,
							query_type,
						});
					}
				}
			}
			// This statement's outcome can no longer change, so it — and, when
			// the statement was a `BEGIN` that has now resolved, every statement
			// its block held — can be sent onward.
			self.finalise_results().await;
			yield_now!();
		}
		self.emit_query_event_for_results(kvs, batch_start, &self.results[batch_results_start..]);
		Ok(())
	}
}

impl Executor {
	/// Record the statements of a request that arrived whole, to be compiled
	/// together when the first of them is planned ([`RequestPlan`]).
	fn prepare_request(&mut self, statements: &[TopLevelStatement]) {
		if crate::exec::compiles::per_statement() {
			return;
		}
		let entries = statements.iter().map(|stmt| self.request_entry(stmt)).collect();
		self.request = Some(RequestPlan {
			entries,
			compiled: None,
			program: None,
			taken: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
		});
	}

	/// What compiling `stmt` with the rest of its request needs.
	fn request_entry(&self, stmt: &TopLevelStatement) -> RequestEntry {
		let read_only = stmt.read_only();
		match stmt {
			TopLevelStatement::Plan(op) => RequestEntry::Plan {
				op: Some(op.as_ref().clone()),
				read_only,
			},
			TopLevelStatement::Ast(stmt) => match stmt {
				sql::TopLevelExpr::Begin => RequestEntry::Begin,
				sql::TopLevelExpr::Commit => RequestEntry::Commit,
				sql::TopLevelExpr::Cancel => RequestEntry::Cancel,
				sql::TopLevelExpr::Option(option) => RequestEntry::Option(option.clone().into()),
				sql::TopLevelExpr::Use(stmt) => {
					let name = |expr: &sql::Expr| {
						crate::ddl::scalar::written_name(&crate::expr::Expr::from(expr.clone()))
					};
					RequestEntry::Use(match stmt {
						sql::statements::UseStatement::Ns(ns) => {
							name(ns).map(|ns| (Some(ns), None))
						}
						sql::statements::UseStatement::Db(db) => {
							name(db).map(|db| (None, Some(db)))
						}
						sql::statements::UseStatement::NsDb(ns, db) => {
							name(ns).zip(name(db)).map(|(ns, db)| (Some(ns), Some(db)))
						}
						sql::statements::UseStatement::Default => None,
					})
				}
				sql::TopLevelExpr::Live(_) | sql::TopLevelExpr::Access(_) => RequestEntry::Stores,
				sql::TopLevelExpr::Kill(_) | sql::TopLevelExpr::Show(_) => RequestEntry::Inert,
				sql::TopLevelExpr::Expr(sql::Expr::Let(stmt)) => RequestEntry::Let {
					name: stmt.name.to_string(),
					kind: stmt.kind.clone().map(Into::into),
					read_only,
					value: Some(lower(stmt.what.clone()).map_err(|reason| {
						ExecError::PlannerUnsupported {
							reason: reason.to_string(),
						}
						.into()
					})),
				},
				sql::TopLevelExpr::Expr(expr) => RequestEntry::Run {
					op: Some(self.lower_bare_expression(expr)),
					read_only,
				},
			},
		}
	}

	/// `stream`, counting each statement the driver takes from it, so the
	/// statement running is known by its position in the request.
	fn counted<S>(&self, stream: S) -> impl Stream<Item = Result<TopLevelStatement>> + use<S>
	where
		S: Stream<Item = Result<TopLevelStatement>>,
	{
		let taken = self.request.as_ref().map(|request| Arc::clone(&request.taken));
		stream.inspect(move |_| {
			if let Some(taken) = &taken {
				taken.fetch_add(1, Ordering::Relaxed);
			}
		})
	}

	/// The position in its request of the statement running.
	fn position(&self) -> Option<usize> {
		let request = self.request.as_ref()?;
		request.taken.load(Ordering::Relaxed).checked_sub(1)
	}

	/// The compiled form of the statement running, compiling the request in
	/// `txn` first when it has not been; `None` where the statement is
	/// planned as it is reached.
	async fn compiled_statement(&mut self, txn: &Arc<Transaction>) -> Option<CompiledStatement> {
		let position = self.position()?;
		if self.request.as_ref().is_some_and(|request| request.compiled.is_none()) {
			let (compiled, program) =
				crate::exec::compiles::compiling_request(self.compile_request(txn, position)).await;
			if let Some(request) = self.request.as_mut() {
				request.compiled = Some(compiled);
				request.program = program;
			}
		}
		let request = self.request.as_mut()?;
		let compiled = request.compiled.as_mut()?.get_mut(position)?.take()?;
		let plan = match compiled.entry {
			Planned::Compiled(Ok(crate::exec::vm::RequestEntry::Statement(index))) => {
				Ok(StatementPlan::Ready(request.program.as_ref()?.statement(index)?))
			}
			Planned::Compiled(Ok(crate::exec::vm::RequestEntry::Operator(operator))) => {
				Ok(StatementPlan::Ready(operator))
			}
			Planned::Compiled(Err(e)) => Err(e),
			Planned::Served {
				op,
				front_end,
			} => Ok(StatementPlan::Served {
				op,
				front_end,
			}),
			Planned::Reached {
				op,
				front_end,
			} => Ok(StatementPlan::Reached {
				op,
				front_end,
			}),
		};
		Some(CompiledStatement {
			plan,
			logged: compiled.logged,
		})
	}

	/// Compile every statement of the request from `first`, the statement
	/// about to be planned, in `txn`, the transaction it runs in, into one
	/// program: the request's steps, and the program of each statement
	/// compiled.
	///
	/// A statement is compiled here only where it runs in `txn`, no
	/// statement before it there may have changed the catalog — a schema
	/// statement, a write, which may define a table or run code that changes
	/// it, a `USE`, a `LIVE` or `ACCESS`, or anything whose effects are not
	/// known — and it reads no parameter an earlier `LET` binds to what only
	/// the run computes; past the first statement, only where binding folds no
	/// read of the clock. It is compiled under the session the statements
	/// before it leave: the names a `USE` writes, the options an `OPTION`
	/// sets, the constants a `LET` binds. Every other statement is served
	/// when it is reached, planned for the catalog its transaction reads then
	/// ([`StatementPlan::Served`]).
	async fn compile_request(
		&mut self,
		txn: &Arc<Transaction>,
		first: usize,
	) -> (Vec<Option<PlannedStatement>>, Option<crate::exec::vm::request::CompiledRequest>) {
		use crate::exec::vm::program::RequestStep;
		let Some(request) = self.request.as_mut() else {
			return (Vec::new(), None);
		};
		let mut entries = std::mem::take(&mut request.entries);
		let mut compiled: Vec<Option<PlannedStatement>> =
			std::iter::repeat_with(|| None).take(entries.len()).collect();
		let mut builder = crate::exec::vm::RequestBuilder::default();
		let mut steps: Vec<RequestStep> = Vec::with_capacity(entries.len());
		let mut opt = self.opt.clone();
		let mut session = self.ctx.value("session").cloned().unwrap_or(Value::None);
		let mut bound: Vec<(String, Value)> = Vec::new();
		let mut computed: std::collections::HashSet<String> = std::collections::HashSet::new();
		let keep_logged = self.ctx.slow_log().is_some();
		let mut same_txn = true;
		let mut changed = false;
		let mut in_block = self.in_block;
		for (at, entry) in entries.iter_mut().enumerate() {
			let planning = at >= first;
			let is_first = at == first;
			if planning && at > first && !in_block {
				same_txn &= matches!(
					entry,
					RequestEntry::Begin
						| RequestEntry::Commit
						| RequestEntry::Cancel
						| RequestEntry::Option(_)
				);
			}
			let inline = same_txn && !changed;
			let step = match entry {
				RequestEntry::Begin => {
					if planning && at > first && !in_block {
						same_txn = false;
					}
					in_block = true;
					RequestStep::Begin
				}
				RequestEntry::Commit => {
					in_block = false;
					same_txn &= !planning;
					RequestStep::Commit
				}
				RequestEntry::Cancel => {
					in_block = false;
					same_txn &= !planning;
					RequestStep::Cancel
				}
				RequestEntry::Option(option) => {
					if planning {
						if option.name.eq_ignore_ascii_case("IMPORT") {
							opt.set_import(option.what);
						} else if option.name.eq_ignore_ascii_case("FORCE") {
							opt.force = if option.what {
								Force::All
							} else {
								Force::None
							};
						}
					}
					RequestStep::Option {
						name: option.name.to_string(),
						what: option.what,
					}
				}
				RequestEntry::Use(names) => {
					if planning {
						changed = true;
						if let Some((ns, db)) = names {
							if let Some(ns) = ns {
								opt.set_ns(Some(ns.as_str().into()));
								session.put(NS.as_ref(), ns.clone().into());
							}
							if let Some(db) = db {
								opt.set_db(Some(db.as_str().into()));
								session.put(DB.as_ref(), db.clone().into());
							}
						}
					}
					match names {
						Some((ns, db)) => RequestStep::Use {
							ns: ns.clone(),
							db: db.clone(),
							computed: false,
						},
						None => RequestStep::Use {
							ns: None,
							db: None,
							computed: true,
						},
					}
				}
				RequestEntry::Stores => {
					changed |= planning;
					RequestStep::Surface
				}
				RequestEntry::Inert => RequestStep::Surface,
				RequestEntry::Let {
					name,
					kind,
					value,
					read_only,
				} => {
					// A value with no logical form fails the `LET` when it is
					// reached.
					let constant = match value.as_ref() {
						Some(Ok(op)) if planning => {
							compiled[at] = Some(
								self.compile_one(
									txn,
									op,
									&opt,
									&session,
									&bound,
									&computed,
									inline,
									is_first,
									keep_logged,
									&mut builder,
								)
								.await,
							);
							constant_value(op, kind.as_ref())
						}
						_ => None,
					};
					bound.retain(|(bound_name, _)| bound_name != name);
					match constant {
						Some(constant) => {
							computed.remove(name.as_str());
							bound.push((name.clone(), constant));
						}
						None => {
							computed.insert(name.clone());
						}
					}
					changed |= planning && !*read_only;
					RequestStep::Let {
						name: name.clone(),
						value: compiled[at].as_ref().and_then(PlannedStatement::index),
					}
				}
				RequestEntry::Run {
					op,
					read_only,
				} => {
					match op.as_ref() {
						Some(Ok(lowered)) if planning => {
							compiled[at] = Some(
								self.compile_one(
									txn,
									lowered,
									&opt,
									&session,
									&bound,
									&computed,
									inline,
									is_first,
									keep_logged,
									&mut builder,
								)
								.await,
							);
						}
						// A statement with no logical form fails with that,
						// whatever the catalog holds.
						Some(Err(_)) if planning => {
							let Some(Err(e)) = op.take() else {
								unreachable!("matched an error")
							};
							compiled[at] = Some(PlannedStatement {
								entry: Planned::Compiled(Err(e)),
								logged: None,
							});
						}
						_ => {}
					}
					changed |= planning && !*read_only;
					RequestStep::Run {
						statement: compiled[at].as_ref().and_then(PlannedStatement::index),
					}
				}
				RequestEntry::Plan {
					op,
					read_only,
				} => {
					if planning && let Some(plan) = op.take() {
						let logged = keep_logged.then(|| plan.clone());
						let entry = if inline {
							Planned::Compiled(
								crate::exec::planner::plan_logical_inner(
									plan,
									&self.ctx,
									&self.function_registry,
									Arc::clone(txn),
									Some(Arc::clone(&self.opt.auth)),
									0,
								)
								.await
								.map(crate::exec::vm::RequestEntry::Operator),
							)
						} else {
							served_for(plan, true)
						};
						compiled[at] = Some(PlannedStatement {
							entry,
							logged,
						});
					}
					changed |= planning && !*read_only;
					RequestStep::Run {
						statement: None,
					}
				}
			};
			steps.push(step);
		}
		let program = crate::exec::planner::finish_request(
			&self.ctx,
			&self.function_registry,
			Arc::clone(txn),
			builder,
			steps,
		);
		match program {
			Ok(program) => (compiled, Some(program)),
			// A request whose program cannot be put together plans each
			// statement when it is reached.
			Err(_) => (Vec::new(), None),
		}
	}

	/// Compile `op`, a statement of the request, in `txn` under the session
	/// the statements before it leave, into the request `builder` builds;
	/// or, where it is not `inline`, where it reads a parameter only the run
	/// computes, or where binding it past the first statement would fold a
	/// read of the clock, leave it to be served when it is reached.
	#[expect(
		clippy::too_many_arguments,
		reason = "the statement, the session the statements before it leave, and the request"
	)]
	async fn compile_one(
		&self,
		txn: &Arc<Transaction>,
		op: &LogicalOp,
		opt: &Options,
		session: &Value,
		bound: &[(String, Value)],
		computed: &std::collections::HashSet<String>,
		inline: bool,
		first: bool,
		keep_logged: bool,
		builder: &mut crate::exec::vm::RequestBuilder,
	) -> PlannedStatement {
		let logged = keep_logged.then(|| op.clone());
		let served = || PlannedStatement {
			entry: served_for(op.clone(), false),
			logged: keep_logged.then(|| op.clone()),
		};
		if !inline || (!computed.is_empty() && reads_any(op, computed)) {
			return served();
		}
		let mut ctx = Context::snapshot(&self.ctx);
		ctx.set_transaction(Arc::clone(txn));
		ctx.add_value("session", session.clone().into());
		for (name, value) in bound {
			ctx.add_value(name.clone(), value.clone().into());
		}
		let ctx = ctx.freeze();
		let planned = crate::exec::planner::plan_request_statement(
			op.clone(),
			&ctx,
			&self.function_registry,
			Arc::clone(txn),
			Some(Arc::clone(&opt.auth)),
			Some(Arc::new(opt.clone())),
			builder,
		)
		.await;
		if !first && planned.binds_the_clock {
			return served();
		}
		PlannedStatement {
			entry: Planned::Compiled(planned.entry),
			logged,
		}
	}
}

/// How `op`, a statement planned when it is reached, is planned then: served
/// from the compile cache, or planned for that run alone
/// ([`StatementPlan::Reached`]) — an `EXPLAIN`, whose operator renders what
/// that run measured, and a statement holding a schema statement, whose
/// planning models it with a speculative run in the transaction it runs in.
fn served_for(op: LogicalOp, front_end: bool) -> Planned {
	if op.any(|node| matches!(node, LogicalOp::Explain(_)))
		|| crate::exec::planner::fold::changes_the_catalog(&op)
	{
		Planned::Reached {
			op,
			front_end,
		}
	} else {
		Planned::Served {
			op,
			front_end,
		}
	}
}

/// The value `LET` binds for `value`, where lowering wrote it as a constant:
/// a value statement whose expression denotes a value without evaluation,
/// coerced to `kind` when it declares one. `None` where the run computes it,
/// or where the coercion fails, which the run raises.
fn constant_value(value: &LogicalOp, kind: Option<&crate::expr::Kind>) -> Option<Value> {
	let LogicalOp::Eval(eval) = value else {
		return None;
	};
	let constant = crate::exec::planner::static_value(&eval.expr)?;
	match kind {
		Some(kind) => constant.coerce_to_kind(kind).ok(),
		None => Some(constant),
	}
}

/// Whether `op` reads any of `names` as a parameter.
fn reads_any(op: &LogicalOp, names: &std::collections::HashSet<String>) -> bool {
	struct Reads<'a> {
		names: &'a std::collections::HashSet<String>,
	}
	impl Visitor for Reads<'_> {
		type Error = ();
		fn visit_param(&mut self, param: &crate::expr::Param) -> Result<(), ()> {
			match self.names.contains(param.as_str()) {
				true => Err(()),
				false => Ok(()),
			}
		}
	}
	visit_plan_params(
		op,
		&mut Reads {
			names,
		},
	)
	.is_err()
}

/// Place the next statement of a request in `grouper`.
///
/// Every statement a driver pulls from its stream passes through here exactly
/// once and in order, whichever loop pulls it, so the grouping stays in step
/// with the request.
fn place_statement(grouper: &mut RequestGrouper, stmt: &TopLevelStatement) -> StatementPlace {
	let place = grouper.place(stmt.request_kind());
	debug_assert!(
		role_agrees_with_dispatch(place.role, &stmt.dispatch()),
		"the grouping placed a {:?} statement as {:?}",
		stmt.dispatch(),
		place.role
	);
	place
}

/// Whether `role` is a role the grouping can give a statement that `dispatch`
/// classifies. Debug checks only.
fn role_agrees_with_dispatch(role: StatementRole, dispatch: &Dispatch) -> bool {
	match dispatch {
		Dispatch::Begin => matches!(
			role,
			StatementRole::Begin
				| StatementRole::Misplaced(
					MisplacedControl::NestedBegin | MisplacedControl::BeginInSession
				)
		),
		Dispatch::Commit => matches!(
			role,
			StatementRole::Commit | StatementRole::Misplaced(MisplacedControl::CommitOutsideBlock)
		),
		Dispatch::Cancel => matches!(
			role,
			StatementRole::Cancel | StatementRole::Misplaced(MisplacedControl::CancelOutsideBlock)
		),
		Dispatch::Option(_) => matches!(role, StatementRole::Option),
		Dispatch::Runnable => matches!(role, StatementRole::Run),
	}
}

/// The parameters a plan references, as the slow log reads them.
///
/// A plan's scalars are [`LogicalExpr`]s, which the expression-layer visitor
/// does not walk, so each `$param` is named to the visitor directly — through
/// every nested statement. A `FROM $param` source names its parameter too: a
/// source holds it as a [`SourceRef::Param`], not as a scalar. A GQL `MATCH`
/// plan's scalars are reached the same way, as scalars of its node.
struct PlanParams<'a>(&'a LogicalOp);

impl<'v> Visit<ParamVisitor<'v>> for PlanParams<'_> {
	fn visit(&self, visitor: &mut ParamVisitor<'v>) -> Result<(), ()> {
		visit_plan_params(self.0, visitor)
	}
}

/// Name every parameter `op` references to `visitor`, nested statements
/// included: each node's scalars and parameter sources, the node's own before
/// its inputs'.
fn visit_plan_params<V: Visitor<Error = ()>>(op: &LogicalOp, visitor: &mut V) -> Result<(), ()> {
	if let LogicalOp::Source(LogicalSource {
		source: SourceRef::Param(param),
		..
	}) = op
	{
		visitor.visit_param(param)?;
	}
	op.visit_scalars(&mut |scalar| visit_scalar_params(scalar, visitor))?;
	op.children().iter().try_for_each(|child| visit_plan_params(child, visitor))
}

/// Name every parameter `scalar` references to `visitor`, descending into the
/// statements it holds.
fn visit_scalar_params<V: Visitor<Error = ()>>(
	scalar: &LogicalExpr,
	visitor: &mut V,
) -> Result<(), ()> {
	use std::ops::ControlFlow as Walk;
	let walked = scalar.visit(&mut |expr| match expr {
		LogicalExpr::Param(param) => match visitor.visit_param(param) {
			Ok(()) => Walk::Continue(()),
			Err(()) => Walk::Break(()),
		},
		LogicalExpr::Subquery(op) => match visit_plan_params(op, visitor) {
			Ok(()) => Walk::Continue(()),
			Err(()) => Walk::Break(()),
		},
		_ => Walk::Continue(()),
	});
	match walked {
		Walk::Continue(()) => Ok(()),
		Walk::Break(()) => Err(()),
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_types::ToSql;

	use super::{shape_statement_value, visit_plan_params};
	use crate::dbs::Session;
	use crate::exec::OutputShape;
	use crate::expr::logical::lower;
	use crate::expr::visit::Visitor;
	use crate::iam::{Level, Role};
	use crate::kvs::Datastore;
	use crate::val::{Array, Value};

	/// The slow log renders a statement the engine ran through its
	/// lowered form, so a credential is replaced by its derivation wherever in
	/// the statement it sits.
	///
	/// `DEFINE USER … PASSWORD` is the shape that carries one, and a schema
	/// statement can sit anywhere an expression can. Rendering the surface
	/// text instead would put the password in the log for every position but
	/// the top level, which is the position a reader is least likely to check.
	#[test]
	fn the_slow_log_renders_no_plaintext_credential_wherever_the_statement_sits() {
		for src in [
			"DEFINE USER bob ON ROOT PASSWORD 'hunter2' ROLES OWNER",
			"{ DEFINE USER bob ON ROOT PASSWORD 'hunter2' ROLES OWNER }",
			"IF true { DEFINE USER bob ON ROOT PASSWORD 'hunter2' ROLES OWNER }",
			"LET $x = (DEFINE USER bob ON ROOT PASSWORD 'hunter2' ROLES OWNER)",
			"FOR $i IN [1] { DEFINE USER bob ON ROOT PASSWORD 'hunter2' ROLES OWNER }",
		] {
			let surface = crate::syn::expr(src).expect("statement should parse");
			let rendered = crate::expr::Expr::from(surface).to_sql();
			assert!(!rendered.contains("hunter2"), "`{src}` rendered its password: {rendered}");
			assert!(
				rendered.contains("PASSHASH"),
				"`{src}` should render the derivation: {rendered}"
			);
		}
	}

	/// Collects the name of every parameter a plan walk reports.
	struct Names(Vec<String>);

	impl Visitor for Names {
		type Error = ();

		fn visit_param(&mut self, param: &crate::expr::Param) -> Result<(), ()> {
			self.0.push(param.to_string());
			Ok(())
		}
	}

	/// The parameters the slow log lists for `src`, sorted by name: the log
	/// lists a set, so the walk order is not part of its contract.
	fn plan_params(src: &str) -> Vec<String> {
		let expr = crate::syn::expr(src).expect("statement should parse");
		let op = lower(expr).expect("statement should have a logical form");
		let mut names = Names(Vec::new());
		visit_plan_params(&op, &mut names).expect("the walk cannot fail");
		names.0.sort();
		names.0
	}

	/// A `FROM $param` source names its parameter to the slow log alongside the
	/// parameters in the statement's scalars, and so does one nested in a
	/// statement-shaped scalar.
	#[test]
	fn plan_params_include_a_parameter_source() {
		assert_eq!(plan_params("SELECT * FROM $tb WHERE a = $v"), ["tb", "v"]);
		assert_eq!(plan_params("RETURN (SELECT * FROM $tb)"), ["tb"]);
		assert_eq!(plan_params("SELECT (SELECT * FROM $inner) FROM $outer"), ["inner", "outer"]);
	}

	fn values(n: usize) -> Vec<Value> {
		(0..n).map(|i| Value::from(i as i64)).collect()
	}

	/// A `Rows` plan's result is an array of everything it emitted, however many
	/// that turns out to be -- including none.
	#[test]
	fn rows_are_always_an_array() {
		for n in [0, 1, 2, 7] {
			let shaped = shape_statement_value(OutputShape::Rows, values(n))
				.expect("rows are shapeable at any count");
			assert_eq!(shaped, Value::Array(Array::from(values(n))), "{n} values");
		}
	}

	/// A `Scalar` plan's result is the value it emitted, unwrapped -- not a
	/// one-element array.
	#[test]
	fn a_scalar_is_unwrapped() {
		let shaped = shape_statement_value(OutputShape::Scalar, vec![Value::from(42i64)])
			.expect("one value is what Scalar promises");
		assert_eq!(shaped, Value::from(42i64));
	}

	/// The declared shape is authoritative: a `Scalar` plan that emits the wrong
	/// number of values is reported rather than quietly reshaped into an array.
	/// Reshaping would hide the operator's bug behind a result that merely
	/// looks plausible, and a streaming consumer cannot reshape at all --
	/// by the time the count is known the values have already been sent.
	#[test]
	fn a_scalar_plan_that_emits_the_wrong_count_is_reported() {
		for n in [0, 2, 5] {
			let error = shape_statement_value(OutputShape::Scalar, values(n))
				.expect_err("a scalar plan emits exactly one value");
			assert!(
				error.to_string().contains(&format!("produced {n} values")),
				"the error should name the count that arrived, got: {error}"
			);
		}
	}

	/// A transaction supplied by the caller already is the block, so the
	/// transaction statements of the request itself have no block to open or
	/// close: each reports the place it ran instead of acting, and the
	/// statements around them are unaffected.
	#[tokio::test]
	async fn misplaced_transaction_statements_in_a_session_transaction_report_where_they_run() {
		use std::sync::Arc;

		use crate::catalog::providers::CatalogProvider;
		use crate::kvs::{QueryRequest, TransactionType};

		let ds = Datastore::new("memory").await.expect("a memory datastore");
		let setup = ds.transaction(TransactionType::Write).await.expect("a write transaction");
		setup.ensure_ns_db(None, "test", "test").await.expect("the test namespace and database");
		setup.commit().await.expect("committing the namespace");

		let ses = Session::owner().with_ns("test").with_db("test");
		let tx = Arc::new(ds.transaction(TransactionType::Write).await.expect("a transaction"));
		let results = ds
			.run(
				QueryRequest::new("BEGIN; COMMIT; CANCEL; RETURN 1", &ses)
					.with_transaction(Arc::clone(&tx)),
			)
			.await
			.expect("the request runs");
		tx.cancel().await.expect("cancelling the session transaction");

		assert_eq!(results.len(), 4, "one result per statement: {results:?}");
		for (index, expected) in [
			"Cannot BEGIN a transaction within a transaction",
			"Cannot COMMIT without starting a transaction",
			"Cannot CANCEL without starting a transaction",
		]
		.into_iter()
		.enumerate()
		{
			let error = results[index].result.as_ref().expect_err("a misplaced statement errors");
			assert!(
				error.to_string().contains(expected),
				"statement {index} should report `{expected}`, got: {error}"
			);
		}
		assert_eq!(
			results[3].result.as_ref().expect("the RETURN runs"),
			&surrealdb_types::Value::Number(surrealdb_types::Number::Int(1))
		);
	}

	/// One request holding every kind of misplaced or unterminated
	/// transaction statement answers, statement for statement, as the same
	/// pieces run as requests of their own: what the grouping decides depends
	/// on the request text only, never on what ran before it.
	#[tokio::test]
	async fn a_request_groups_each_piece_as_it_would_alone() {
		use crate::kvs::{QueryRequest, TransactionType};

		async fn run(sql: &str) -> Vec<String> {
			use crate::catalog::providers::CatalogProvider;

			let ds = Datastore::new("memory").await.expect("a memory datastore");
			let setup = ds.transaction(TransactionType::Write).await.expect("a transaction");
			setup.ensure_ns_db(None, "test", "test").await.expect("the namespace and database");
			setup.commit().await.expect("committing the namespace");
			let ses = Session::owner().with_ns("test").with_db("test");
			ds.run(QueryRequest::new(sql, &ses))
				.await
				.expect("the request runs")
				.into_iter()
				.map(|res| format!("{:?}", res.result))
				.collect()
		}

		let pieces = [
			"BEGIN; CREATE a:1; BEGIN; CREATE a:2; COMMIT; RETURN 'nested';",
			"BEGIN; RETURN 1; BEGIN; COMMIT; RETURN 2;",
			"COMMIT; CANCEL; RETURN 3;",
			"CREATE b:0; BEGIN; CREATE b:1; RETURN 4;",
		];
		let mut expected = Vec::new();
		for piece in pieces {
			expected.extend(run(piece).await);
		}
		let together = pieces.join("\n");
		assert_eq!(run(&together).await, expected);
	}

	/// A cancellation observed as a block's own `COMMIT` is pulled drops that
	/// `COMMIT`, and the fast-forward ends at the next `COMMIT` of the text,
	/// which the grouping sees outside a block. That one answers as a
	/// `COMMIT` of the cancelled block, as it does when the cancellation is
	/// observed at any other statement of the block.
	#[tokio::test]
	async fn a_cancellation_at_a_commit_ends_at_the_next_commit() {
		use futures::StreamExt;

		use crate::catalog::providers::CatalogProvider;
		use crate::kvs::TransactionType;
		use crate::sql::TopLevelExpr;

		let ds = Datastore::new("memory").await.expect("a memory datastore");
		let setup = ds.transaction(TransactionType::Write).await.expect("a transaction");
		setup.ensure_ns_db(None, "test", "test").await.expect("the namespace and database");
		setup.commit().await.expect("committing the namespace");

		let ses = Session::owner().with_ns("test").with_db("test");
		let opt = ds.setup_options(&ses);
		let mut ctx = ds.setup_ctx().expect("a context");
		let canceller = ctx.add_cancel();
		ctx.attach_session(&ses).expect("the session attaches");

		let ast = crate::syn::parse("BEGIN; CREATE a:1; COMMIT; CREATE b:1; COMMIT; RETURN 1;")
			.expect("the request parses");
		let mut commits = 0;
		let stream = futures::stream::iter(ast.expressions).map(move |expr| {
			if matches!(expr, TopLevelExpr::Commit) {
				commits += 1;
				if commits == 1 {
					canceller.cancel();
				}
			}
			Ok(expr)
		});
		let results = super::Executor::execute_stream(&ds, ctx.freeze(), opt, false, stream)
			.await
			.expect("the request runs");

		let messages: Vec<String> = results
			.iter()
			.map(|res| match &res.result {
				Ok(value) => format!("{value:?}"),
				Err(e) => e.to_string(),
			})
			.collect();
		assert_eq!(results.len(), 5, "one result per statement: {messages:?}");
		for (index, message) in messages[1..3].iter().enumerate() {
			assert!(
				message.contains("The query was not executed due to a cancelled transaction"),
				"statement {}: {message}",
				index + 1
			);
		}
		assert!(
			messages[3].contains("Cannot COMMIT: the transaction was cancelled"),
			"the second COMMIT: {}",
			messages[3]
		);
	}

	#[tokio::test]
	async fn check_execute_option_permissions() {
		let tests = vec![
			// Root level
			(
				Session::for_level(Level::Root, Role::Owner).with_ns("NS").with_db("DB"),
				true,
				"owner at root level should be able to set options",
			),
			(
				Session::for_level(Level::Root, Role::Editor).with_ns("NS").with_db("DB"),
				true,
				"editor at root level should be able to set options",
			),
			(
				Session::for_level(Level::Root, Role::Viewer).with_ns("NS").with_db("DB"),
				false,
				"viewer at root level should not be able to set options",
			),
			// Namespace level
			(
				Session::for_level(Level::Namespace("NS".to_string()), Role::Owner)
					.with_ns("NS")
					.with_db("DB"),
				true,
				"owner at namespace level should be able to set options on its namespace",
			),
			(
				Session::for_level(Level::Namespace("NS".to_string()), Role::Owner)
					.with_ns("OTHER_NS")
					.with_db("DB"),
				false,
				"owner at namespace level should not be able to set options on another namespace",
			),
			(
				Session::for_level(Level::Namespace("NS".to_string()), Role::Editor)
					.with_ns("NS")
					.with_db("DB"),
				true,
				"editor at namespace level should be able to set options on its namespace",
			),
			(
				Session::for_level(Level::Namespace("NS".to_string()), Role::Editor)
					.with_ns("OTHER_NS")
					.with_db("DB"),
				false,
				"editor at namespace level should not be able to set options on another namespace",
			),
			(
				Session::for_level(Level::Namespace("NS".to_string()), Role::Viewer)
					.with_ns("NS")
					.with_db("DB"),
				false,
				"viewer at namespace level should not be able to set options on its namespace",
			),
			// Database level
			(
				Session::for_level(
					Level::Database("NS".to_string(), "DB".to_string()),
					Role::Owner,
				)
				.with_ns("NS")
				.with_db("DB"),
				true,
				"owner at database level should be able to set options on its database",
			),
			(
				Session::for_level(
					Level::Database("NS".to_string(), "DB".to_string()),
					Role::Owner,
				)
				.with_ns("NS")
				.with_db("OTHER_DB"),
				false,
				"owner at database level should not be able to set options on another database",
			),
			(
				Session::for_level(
					Level::Database("NS".to_string(), "DB".to_string()),
					Role::Owner,
				)
				.with_ns("OTHER_NS")
				.with_db("DB"),
				false,
				"owner at database level should not be able to set options on another namespace even if the database name matches",
			),
			(
				Session::for_level(
					Level::Database("NS".to_string(), "DB".to_string()),
					Role::Editor,
				)
				.with_ns("NS")
				.with_db("DB"),
				true,
				"editor at database level should be able to set options on its database",
			),
			(
				Session::for_level(
					Level::Database("NS".to_string(), "DB".to_string()),
					Role::Editor,
				)
				.with_ns("NS")
				.with_db("OTHER_DB"),
				false,
				"editor at database level should not be able to set options on another database",
			),
			(
				Session::for_level(
					Level::Database("NS".to_string(), "DB".to_string()),
					Role::Editor,
				)
				.with_ns("OTHER_NS")
				.with_db("DB"),
				false,
				"editor at database level should not be able to set options on another namespace even if the database name matches",
			),
			(
				Session::for_level(
					Level::Database("NS".to_string(), "DB".to_string()),
					Role::Viewer,
				)
				.with_ns("NS")
				.with_db("DB"),
				false,
				"viewer at database level should not be able to set options on its database",
			),
		];
		let statement = "OPTION IMPORT = false";

		for test in tests.iter() {
			let (session, should_succeed, msg) = test;

			{
				let ds =
					Datastore::builder().with_auth(true).build_with_path("memory").await.unwrap();

				let res = ds.execute(statement, session, None).await;

				if *should_succeed {
					assert!(res.is_ok(), "{}: {:?}", msg, res);
				} else {
					let err = res.unwrap_err();
					assert!(err.is_not_allowed(), "{msg}: expected NotAllowed error, got {err}")
				}
			}
		}

		// Anonymous with auth enabled
		{
			let ds = Datastore::builder().with_auth(true).build_with_path("memory").await.unwrap();

			let res =
				ds.execute(statement, &Session::default().with_ns("NS").with_db("DB"), None).await;

			let err = res.unwrap_err();
			assert!(
				err.is_not_allowed(),
				"anonymous user should not be able to set options: {}",
				err
			)
		}

		// Anonymous with auth disabled
		{
			let ds = Datastore::builder().with_auth(false).build_with_path("memory").await.unwrap();

			let res =
				ds.execute(statement, &Session::default().with_ns("NS").with_db("DB"), None).await;

			assert!(
				res.is_ok(),
				"anonymous user should be able to set options when auth is disabled: {:?}",
				res
			)
		}
	}

	#[tokio::test]
	async fn check_execute_timeout() {
		// With small timeout
		{
			let ds = Datastore::new("memory").await.unwrap();
			let stmt = "UPDATE test TIMEOUT 2s";
			let res = ds.execute(stmt, &Session::default().with_ns("NS").with_db("DB"), None).await;
			assert!(res.is_ok(), "Failed to execute statement with small timeout: {:?}", res);
		}
		// With large timeout
		{
			let ds = Datastore::new("memory").await.unwrap();
			let stmt = "UPDATE test TIMEOUT 31540000s"; // 1 year
			let res = ds.execute(stmt, &Session::default().with_ns("NS").with_db("DB"), None).await;
			assert!(res.is_ok(), "Failed to execute statement with large timeout: {:?}", res);
		}
		// With very large timeout. The namespace and database are defined
		// first: the statement has to reach its own clause, and a plan resolves
		// the catalog before any operator runs, so an undefined namespace would
		// be reported ahead of the timeout rather than the timeout being the
		// answer.
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::default().with_ns("NS").with_db("DB");
			ds.execute("DEFINE NAMESPACE NS; DEFINE DATABASE DB", &sess, None).await.unwrap();
			let stmt = "UPDATE test TIMEOUT 9460800000000000000s"; // 300 billion years
			let res = ds.execute(stmt, &sess, None).await;
			assert!(res.is_ok(), "Failed to execute statement with very large timeout: {:?}", res);
			let results = res.unwrap();
			let err = results[0].result.as_ref().unwrap_err();
			assert!(
				err.is_validation()
					|| (err.is_internal() && err.message().contains("Invalid timeout")),
				"Expected to find invalid timeout error: {:?}",
				err
			);
		}
	}

	#[tokio::test]
	async fn import_stream_suppresses_results_but_persists_data() {
		use bytes::Bytes;

		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::default().with_ns("NS").with_db("DB");

		ds.execute("DEFINE NAMESPACE NS; USE NS NS; DEFINE DATABASE DB", &sess, None)
			.await
			.unwrap();

		let sql =
			"OPTION IMPORT; INSERT INTO person [{ name: 'a' }, { name: 'b' }, { name: 'c' }];";
		let body = futures::stream::once(async { Ok(Bytes::from(sql)) });
		let results = ds.import_stream(&sess, body).await.unwrap();

		assert!(
			results.is_empty(),
			"import_stream should suppress successful results, got {} results",
			results.len()
		);

		let verify = ds.execute("SELECT * FROM person ORDER BY name", &sess, None).await.unwrap();
		let rows = verify[0].result.as_ref().unwrap();
		assert!(rows.is_array(), "Expected an array of results");
		let arr = rows.as_array().unwrap();
		assert_eq!(arr.len(), 3, "Expected 3 inserted records, got {}", arr.len());
	}

	#[tokio::test]
	async fn import_stream_still_reports_errors() {
		use bytes::Bytes;

		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::default().with_ns("NS").with_db("DB");

		ds.execute("DEFINE NAMESPACE NS; USE NS NS; DEFINE DATABASE DB", &sess, None)
			.await
			.unwrap();

		let sql = "OPTION IMPORT; INSERT INTO person { name: 'ok' }; BREAK;";
		let body = futures::stream::once(async { Ok(Bytes::from(sql)) });
		let results = ds.import_stream(&sess, body).await.unwrap();

		assert!(!results.is_empty(), "import_stream should report errors");
		assert!(
			results.iter().any(|r| r.result.is_err()),
			"Expected at least one error result from invalid BREAK statement"
		);

		let verify = ds.execute("SELECT * FROM person", &sess, None).await.unwrap();
		let rows = verify[0].result.as_ref().unwrap();
		let arr = rows.as_array().unwrap();
		assert_eq!(arr.len(), 1, "The successful INSERT before the error should have persisted");
	}

	#[tokio::test]
	async fn import_stream_rejects_without_option_import() {
		use bytes::Bytes;

		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::default().with_ns("NS").with_db("DB");

		ds.execute("DEFINE NAMESPACE NS; USE NS NS; DEFINE DATABASE DB", &sess, None)
			.await
			.unwrap();

		let sql = "INSERT INTO person { name: 'a' };";
		let body = futures::stream::once(async { Ok(Bytes::from(sql)) });
		let result = ds.import_stream(&sess, body).await;

		assert!(result.is_err(), "import_stream should reject input without OPTION IMPORT");
		let err = result.unwrap_err().to_string();
		assert!(err.contains("OPTION IMPORT"), "Error should mention OPTION IMPORT, got: {err}");
	}

	#[tokio::test]
	async fn import_stream_rejects_option_import_change_midstream() {
		use bytes::Bytes;

		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::default().with_ns("NS").with_db("DB");

		ds.execute("DEFINE NAMESPACE NS; USE NS NS; DEFINE DATABASE DB", &sess, None)
			.await
			.unwrap();

		let sql = "OPTION IMPORT; OPTION IMPORT = false; INSERT INTO person { name: 'a' };";
		let body = futures::stream::once(async { Ok(Bytes::from(sql)) });
		let result = ds.import_stream(&sess, body).await;

		assert!(result.is_err(), "import_stream should reject OPTION IMPORT changes mid-stream");
		let err = result.unwrap_err().to_string();
		assert!(
			err.contains("Cannot change OPTION IMPORT"),
			"Error should explain that import mode is locked, got: {err}"
		);
	}

	mod result_rows_observer {
		//! Regression coverage for the affected-row contract on
		//! `StatementEventSafe.result_rows` and the per-batch
		//! `QueryEvent` dispatch. The iterator-side counter must report
		//! the actual number of records mutated for DML statements --
		//! including those that suppress the per-document value via
		//! `RETURN NONE` or that lack a "before" value (fresh `CREATE`).
		//! The executor must also emit one `QueryEvent` per query batch
		//! so authenticated `surrealdb_queries_total` /
		//! `surrealdb_query_errors_total` /
		//! `surrealdb_query_duration_seconds` keep advancing under load.

		use std::sync::{Arc, Mutex};

		use crate::dbs::Session;
		use crate::kvs::Datastore;
		use crate::observe::{
			ExecutionObserver, Outcome, QueryEvent, StatementEvent, StatementType,
		};

		#[derive(Default)]
		struct CapturingObserver {
			events: Mutex<Vec<(StatementType, u64)>>,
			queries: Mutex<Vec<(Outcome, u32, u32, u32)>>,
		}

		impl CapturingObserver {
			fn snapshot(&self) -> Vec<(StatementType, u64)> {
				self.events.lock().unwrap().clone()
			}

			fn queries(&self) -> Vec<(Outcome, u32, u32, u32)> {
				self.queries.lock().unwrap().clone()
			}
		}

		impl ExecutionObserver for CapturingObserver {
			fn on_statement_complete(&self, event: &StatementEvent) {
				self.events.lock().unwrap().push((event.safe.kind, event.safe.result_rows));
			}

			fn on_query_complete(&self, event: &QueryEvent) {
				let c = event.safe.counters;
				self.queries.lock().unwrap().push((event.safe.outcome, c.total, c.ok, c.err));
			}
		}

		async fn run(sql: &str) -> Vec<(StatementType, u64)> {
			let (events, _) = run_capturing(sql).await;
			events
		}

		#[allow(clippy::clone_on_ref_ptr)]
		async fn run_capturing(sql: &str) -> (Vec<(StatementType, u64)>, Arc<CapturingObserver>) {
			let observer = Arc::new(CapturingObserver::default());
			let obs: Arc<dyn ExecutionObserver> = observer.clone();
			let ds =
				Datastore::builder().with_observer(obs).build_with_path("memory").await.unwrap();
			let sess = Session::default().with_ns("NS").with_db("DB");
			// The capturing observer also receives the DEFINE bootstrap
			// events; drain them before running the test SQL so the
			// assertions only see the statements they care about.
			ds.execute("DEFINE NAMESPACE NS; USE NS NS; DEFINE DATABASE DB", &sess, None)
				.await
				.unwrap();
			observer.events.lock().unwrap().clear();
			observer.queries.lock().unwrap().clear();
			ds.execute(sql, &sess, None).await.unwrap();
			(observer.snapshot(), observer)
		}

		fn rows_for(events: &[(StatementType, u64)], kind: StatementType) -> Vec<u64> {
			events.iter().filter(|(k, _)| *k == kind).map(|(_, n)| *n).collect()
		}

		#[tokio::test]
		async fn update_return_none_reports_affected_count() {
			let events = run("\
				 CREATE foo:1; CREATE foo:2; CREATE foo:3;\
				 UPDATE foo SET x = 1 RETURN NONE;\
				 ")
			.await;
			let updates = rows_for(&events, StatementType::Update);
			assert_eq!(updates, vec![3], "UPDATE RETURN NONE should report 3 affected rows");
		}

		#[tokio::test]
		async fn create_return_before_reports_one() {
			let events = run("CREATE foo:bar RETURN BEFORE;").await;
			let creates = rows_for(&events, StatementType::Create);
			assert_eq!(creates, vec![1], "CREATE RETURN BEFORE should report 1 affected row");
		}

		#[tokio::test]
		async fn delete_return_none_reports_affected_count() {
			let events = run("\
				 CREATE foo:1; CREATE foo:2;\
				 DELETE foo RETURN NONE;\
				 ")
			.await;
			let deletes = rows_for(&events, StatementType::Delete);
			assert_eq!(deletes, vec![2], "DELETE RETURN NONE should report 2 affected rows");
		}

		#[tokio::test]
		async fn update_where_no_match_reports_zero() {
			// `WHERE` filter that no row satisfies must report zero
			// affected rows, even though the iterator visits every
			// scanned record. Pre-mutation `IgnoreError::Ignore` from
			// `check_where_condition` must not bump the counter.
			let events = run("\
				 CREATE foo:1 SET x = 1; CREATE foo:2 SET x = 2; CREATE foo:3 SET x = 3;\
				 UPDATE foo SET x = 99 WHERE x = 999;\
				 ")
			.await;
			let updates = rows_for(&events, StatementType::Update);
			assert_eq!(
				updates,
				vec![0],
				"UPDATE with WHERE matching no rows should report 0 affected rows"
			);
		}

		#[tokio::test]
		async fn update_nonexistent_record_reports_zero() {
			// Targeting a record id that does not exist makes
			// `check_record_exists` return `IgnoreError::Ignore`
			// before any KV write; the counter must stay at zero.
			let events = run("UPDATE foo:does_not_exist SET x = 1;").await;
			let updates = rows_for(&events, StatementType::Update);
			assert_eq!(
				updates,
				vec![0],
				"UPDATE on a non-existent record id should report 0 affected rows"
			);
		}

		#[tokio::test]
		async fn delete_where_no_match_reports_zero() {
			// DELETE with a WHERE clause that filters every row out
			// must report zero affected rows even when the iterator
			// visits each record.
			let events = run("\
				 CREATE foo:1 SET x = 1; CREATE foo:2 SET x = 2;\
				 DELETE foo WHERE x = 999;\
				 ")
			.await;
			let deletes = rows_for(&events, StatementType::Delete);
			assert_eq!(
				deletes,
				vec![0],
				"DELETE with WHERE matching no rows should report 0 affected rows"
			);
		}

		#[tokio::test]
		async fn update_unchanged_record_reports_zero() {
			// `set_record` is suppressed when `!self.changed()`, so
			// no KV write happens and the counter must stay at zero
			// even though the iterator returned `Ok` from
			// `Document::process`.
			let events = run("\
				 CREATE foo:1 SET x = 1;\
				 UPDATE foo:1 SET x = 1;\
				 ")
			.await;
			let updates = rows_for(&events, StatementType::Update);
			assert_eq!(
				updates,
				vec![0],
				"UPDATE that does not change a record should report 0 affected rows"
			);
		}

		#[tokio::test]
		async fn upsert_existing_record_where_no_match_reports_zero() {
			// Baseline coverage for the UPSERT-falls-back-to-update
			// path: `upsert_create` fails with `RecordExists` from
			// `store_record_data` (which returns Err before flipping
			// `doc.mutated`), the dispatch rolls back the empty
			// savepoint, and `upsert_update` returns `Ignore` from
			// `check_where_condition`. No KV write survives, so the
			// affected-row count must be zero.
			let events = run("\
				 CREATE foo:1 SET x = 1;\
				 UPSERT foo:1 SET x = 99 WHERE x = 999;\
				 ")
			.await;
			let upserts = rows_for(&events, StatementType::Upsert);
			assert_eq!(
				upserts,
				vec![0],
				"UPSERT on an existing record with an unmatched WHERE should report 0 affected rows"
			);
		}

		#[tokio::test]
		async fn upsert_index_collision_then_where_no_match_reports_zero() {
			// Direct regression for the rollback-time `mutated`
			// reset: UPSERT on a fresh id passes
			// `upsert_create.store_record_data` (which sets
			// `doc.mutated = true`) and then trips a unique-index
			// collision in `store_index_data` against an existing
			// record. The dispatch rolls back the savepoint and
			// retries as an update against the colliding row, where
			// a WHERE clause filters it out. Without resetting
			// `doc.mutated` after the rollback, the stale flag from
			// the rolled-back create would inflate the affected-row
			// count even though no net KV write survives.
			let events = run("\
				 DEFINE INDEX uniq_x ON foo FIELDS x UNIQUE;\
				 CREATE foo:a SET x = 1;\
				 UPSERT foo:b SET x = 1 WHERE x = 999;\
				 ")
			.await;
			let upserts = rows_for(&events, StatementType::Upsert);
			assert_eq!(
				upserts,
				vec![0],
				"UPSERT that rolls back a unique-index collision and finds no WHERE match must report 0 affected rows"
			);
		}

		#[tokio::test]
		async fn select_unaffected_by_counter_path() {
			let events = run("\
				 CREATE foo:1; CREATE foo:2; CREATE foo:3;\
				 SELECT * FROM foo;\
				 ")
			.await;
			let selects = rows_for(&events, StatementType::Select);
			assert_eq!(selects, vec![3], "SELECT should still report rows from value-shape");
		}

		#[tokio::test]
		async fn query_event_emitted_for_successful_batch() {
			let (_, observer) = run_capturing("CREATE foo:1; SELECT * FROM foo;").await;
			let queries = observer.queries();
			assert_eq!(queries.len(), 1, "expected one QueryEvent per batch");
			let (outcome, total, ok, err) = queries[0];
			assert_eq!(outcome, Outcome::Success);
			assert_eq!(total, 2);
			assert_eq!(ok, 2);
			assert_eq!(err, 0);
		}

		#[tokio::test]
		async fn query_event_records_errors() {
			// Use an invalid runtime statement (THROW) so the parse step
			// still succeeds and the executor reaches the dispatch site.
			let (_, observer) = run_capturing("CREATE foo:1; THROW 'boom';").await;
			let queries = observer.queries();
			assert_eq!(queries.len(), 1, "expected one QueryEvent per batch");
			let (outcome, total, ok, err) = queries[0];
			assert_eq!(outcome, Outcome::Error);
			assert_eq!(total, 2);
			assert_eq!(ok, 1);
			assert_eq!(err, 1);
		}
	}

	/// A bare `LIVE` or `KILL` that loses a retryable commit conflict runs
	/// again in a fresh transaction, bounded by `LIVE_KILL_CONFLICT_ATTEMPTS`,
	/// and a statement inside a transaction block never does.
	mod live_kill_conflict_retry {
		use std::sync::Arc;
		use std::time::Duration;

		use surrealdb_catalog::providers::CatalogProvider;
		use surrealdb_types::{Action, Notification, Value};

		use super::super::LIVE_KILL_CONFLICT_ATTEMPTS;
		use crate::dbs::Session;
		use crate::kvs::Datastore;
		use crate::kvs::testing::{
			RetryableConflictSite, inject_retryable_conflicts, retryable_conflict_count,
		};

		const SITE: RetryableConflictSite = RetryableConflictSite::BareLiveKillCommit;

		async fn new_ds() -> (Arc<Datastore>, async_channel::Receiver<Notification>, Session) {
			let (send, recv) = crate::channel::bounded(64);
			let ds = Datastore::builder()
				.with_notify(send)
				.build_with_path("memory")
				.await
				.expect("datastore should open");
			let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
			let txn = ds
				.transaction(crate::kvs::TransactionType::Write)
				.await
				.expect("transaction should open");
			txn.ensure_ns_db(None, "test", "test").await.expect("ns and db should be created");
			txn.commit().await.expect("commit should succeed");
			ds.execute("DEFINE TABLE person", &ses, None)
				.await
				.expect("DEFINE TABLE should run")
				.remove(0)
				.result
				.expect("DEFINE TABLE should succeed");
			(ds, recv, ses)
		}

		async fn live(ds: &Datastore, ses: &Session) -> Value {
			ds.execute("LIVE SELECT * FROM person", ses, None)
				.await
				.expect("LIVE should run")
				.remove(0)
				.result
				.expect("LIVE should succeed")
		}

		/// The ids `INFO FOR TABLE person` lists as live queries.
		async fn lives(ds: &Datastore, ses: &Session) -> Vec<String> {
			let info = ds
				.execute("INFO FOR TABLE person", ses, None)
				.await
				.expect("INFO should run")
				.remove(0)
				.result
				.expect("INFO should succeed");
			let Value::Object(info) = info else {
				panic!("INFO FOR TABLE should answer an object");
			};
			let Some(Value::Object(lives)) = info.get("lives") else {
				panic!("INFO FOR TABLE should list `lives`");
			};
			lives.keys().cloned().collect()
		}

		fn id_of(value: &Value) -> String {
			let Value::Uuid(id) = value else {
				panic!("LIVE should answer a uuid, got {value:?}");
			};
			id.to_string()
		}

		#[tokio::test]
		async fn a_killed_live_query_survives_conflicts_within_the_bound() {
			let (ds, recv, ses) = new_ds().await;
			let id = live(&ds, &ses).await;
			let _guard = inject_retryable_conflicts(
				SITE,
				ds.id(),
				(LIVE_KILL_CONFLICT_ATTEMPTS - 1) as usize,
			);
			let vars = surrealdb_types::vars! { id: id.clone() };
			let res = ds.execute("KILL $id", &ses, Some(vars)).await.expect("KILL should run");
			assert!(res[0].result.is_ok(), "KILL should succeed on its last attempt");
			assert_eq!(retryable_conflict_count(SITE, ds.id()), 0, "every conflict is consumed");
			assert!(lives(&ds, &ses).await.is_empty(), "the registration is gone");
			// The re-run must deliver through a broker of its own: a broker
			// left over from the lost attempt has no receiver.
			let killed = tokio::time::timeout(Duration::from_secs(1), recv.recv())
				.await
				.expect("a Killed notification should arrive")
				.expect("the channel should be open");
			assert_eq!(killed.action, Action::Killed);
			assert_eq!(Value::Uuid(killed.id), id);
		}

		#[tokio::test]
		async fn a_kill_that_keeps_conflicting_reports_the_conflict() {
			let (ds, _recv, ses) = new_ds().await;
			let id = live(&ds, &ses).await;
			let _guard =
				inject_retryable_conflicts(SITE, ds.id(), LIVE_KILL_CONFLICT_ATTEMPTS as usize);
			let vars = surrealdb_types::vars! { id: id.clone() };
			let res = ds.execute("KILL $id", &ses, Some(vars)).await.expect("KILL should run");
			let err = res[0].result.as_ref().expect_err("KILL should report the conflict");
			assert_eq!(
				err.query_details(),
				Some(&surrealdb_types::QueryError::TransactionConflict),
				"the conflict must arrive unwrapped: {err}"
			);
			assert_eq!(
				retryable_conflict_count(SITE, ds.id()),
				0,
				"exactly the bounded number of attempts is made"
			);
			assert_eq!(lives(&ds, &ses).await, vec![id_of(&id)]);
		}

		#[tokio::test]
		async fn a_live_query_registered_after_a_conflict_keeps_its_id() {
			let (ds, recv, ses) = new_ds().await;
			let _guard = inject_retryable_conflicts(SITE, ds.id(), 2);
			let id = live(&ds, &ses).await;
			assert_eq!(retryable_conflict_count(SITE, ds.id()), 0, "every conflict is consumed");
			assert_eq!(lives(&ds, &ses).await, vec![id_of(&id)]);
			ds.execute("CREATE person:1", &ses, None)
				.await
				.expect("CREATE should run")
				.remove(0)
				.result
				.expect("CREATE should succeed");
			let created = tokio::time::timeout(Duration::from_secs(1), recv.recv())
				.await
				.expect("a notification should arrive")
				.expect("the channel should be open");
			assert_eq!(created.action, Action::Create);
			assert_eq!(Value::Uuid(created.id), id);
		}

		#[tokio::test]
		async fn a_statement_in_a_transaction_block_is_not_retried() {
			let (ds, _recv, ses) = new_ds().await;
			let id = live(&ds, &ses).await;
			let guard = inject_retryable_conflicts(SITE, ds.id(), 1);
			let vars = surrealdb_types::vars! { id: id.clone() };
			let res = ds
				.execute("BEGIN; KILL $id; COMMIT;", &ses, Some(vars))
				.await
				.expect("the block should run");
			assert!(res.iter().all(|r| r.result.is_ok()), "the block should succeed: {res:?}");
			assert_eq!(
				retryable_conflict_count(SITE, ds.id()),
				1,
				"the bare-statement site is never consulted inside a block"
			);
			drop(guard);
			assert!(lives(&ds, &ses).await.is_empty());
		}
	}
}
