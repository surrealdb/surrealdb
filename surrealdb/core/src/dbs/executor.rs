use std::borrow::Cow;
use std::collections::HashMap;
use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use async_channel::Sender;
use futures::channel::oneshot;
use futures::{Stream, StreamExt};
use rand::Rng;
use surrealdb_datastore::close::{
	ActionScope, AfterCommitDeadline, AfterCommitWaits, CommitAction,
};
use surrealdb_expr::parser::{BeginOptions, IsolationLevel, Staleness, TopLevelExpr};
use surrealdb_strand::Strand;
use surrealdb_types::{Error as TypesError, QueryError, ToSql};
use web_time::Instant;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, RootProvider};
use crate::ctx::reason::Reason;
use crate::ctx::{Context, FrozenContext};
use crate::dbs::{
	DefaultBroker, Dispatch, Force, MessageBroker, Options, OrderedBroker, QueryResult,
	QueryStreamItem, QueryType, RoutedNotification, StatementCounters, TopLevelStatement,
	items_for_result,
};
use crate::err;
use crate::err::{EngineError, Error};
use crate::exec::context::shape_statement_value;
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::cache::deps::BindingOrigin;
use crate::exec::ir::iface::premise::{OpenRegionShape, RegionContext, TxnContext};
use crate::exec::ir::iface::region_kind::StatementCommit;
use crate::exec::ir::logical::constraints::request_rejection;
use crate::exec::ir::timers::{RequestTiming, compile_phase};
use crate::exec::planner::plan_statement_inner;
use crate::exec::{Error as ExecError, OutputShape};
use crate::expr::logical::{
	Emission, LogicalExpr, LogicalOp, LogicalSource, MisplacedControl, RequestGrouper, SourceRef,
	StatementPlace, StatementRole, lower,
};
use crate::expr::paths::{DB, NS};
use crate::expr::statements::{OptionStatement, UseStatement};
use crate::expr::visit::{Visit, Visitor};
use crate::expr::{ControlFlow, Expr, FlowResult};
use crate::kvs::slowlog::{ParamVisitor, SlowLogVisit};
use crate::kvs::{
	Datastore, DatastoreError, DocIdsReclaimPending, DocIdsReclaimStep, Isolation, ReadOnlyOptions,
	Transaction, TransactionType,
};
use crate::observe::{
	Outcome, QueryCounters, QueryEvent, QueryEventSafe, StatementEvent, StatementEventCtx,
	StatementEventSafe, StatementType,
};
use crate::rpc::types_error_from_anyhow;
use crate::val::{Value, convert_result_rows_to_public, convert_value_to_public_value};

const TARGET: &str = "surrealdb::core::dbs";

/// How many times a statement run in a transaction of its own runs, the first
/// attempt included, when it loses a retryable transaction conflict. A conflict
/// means the attempt's writes did not apply, so running the statement again is
/// safe unless the attempt did something outside the transaction
/// ([`Transaction::performed_external_effect`]). Another writer that commits
/// first, a client or one of the datastore's own background jobs, therefore
/// delays the statement rather than failing it. The count is bounded so a
/// statement that keeps losing the race still reports the conflict to its
/// caller rather than holding the connection indefinitely; it is also cut
/// short by the datastore's transaction timeout, whose one budget the
/// statement's attempts share (see `Executor::execute_rerunning_on_conflict`).
const STATEMENT_CONFLICT_ATTEMPTS: u32 = 10;

/// The wait before the second run of a statement that lost a conflict. It
/// doubles before each later run up to [`STATEMENT_CONFLICT_MAX_BACKOFF`], and
/// each wait is drawn from half to one and a half times that, so statements
/// that lost to one winner do not run again in step and collide again. The
/// wait also yields the connection's task, so a winner running on the same
/// task gets to commit.
const STATEMENT_CONFLICT_MIN_BACKOFF: Duration = Duration::from_millis(1);

/// The longest wait before a re-run of a statement that lost a conflict,
/// before jitter.
const STATEMENT_CONFLICT_MAX_BACKOFF: Duration = Duration::from_millis(32);

/// The keys one step of a refused index statement's doc-ID reclaim deletes
/// before the query's cancellation and timeout are checked again
/// (`Executor::finish_doc_ids_reclaim`): ten committed pages.
const DOC_IDS_RECLAIM_STEP_KEYS: u64 = crate::kvs::RECLAIM_BATCH_SIZE as u64 * 10;

/// How long a refused index statement waits between looks at a doc-ID reclaim
/// that another node's reaper is running.
const DOC_IDS_RECLAIM_POLL: Duration = Duration::from_millis(100);

/// What a statement that lost a conflict runs again from
/// (`Executor::execute_rerunning_on_conflict`).
enum Rerun {
	/// The source of a statement compiled to a plan, which its run consumed.
	Compile(surrealdb_expr::parser::StatementSource),
	/// A copy of a dedicated statement, taken before its run.
	Copy(TopLevelExpr),
}

impl Rerun {
	/// The statement to run again; `None` when its source no longer compiles.
	fn statement(self) -> Option<TopLevelStatement> {
		match self {
			Self::Compile(source) => {
				source.compile().map(|stmt| TopLevelStatement::Surreal(Box::new(stmt)))
			}
			Self::Copy(expr) => Some(TopLevelStatement::Ast(expr)),
		}
	}
}

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
/// It stays `pub` and is re-exported as `crate::observe::classify_anyhow_error`,
/// so an out-of-tree observer that fills the `error_class` attribute works from
/// an `anyhow::Error` alone. The kvs-is-retryable rule lives in this one function.
pub fn classify_anyhow_error(err: &anyhow::Error) -> &'static str {
	use crate::observe::error_class::{INTERNAL, STORAGE, TXN_CONFLICT, classify_types_error};
	// Both shapes reach here. A bare `kvs::Error` comes straight from the
	// store; the `err::Error::Kvs`-wrapped one comes from every `Transaction`
	// write, which goes through `map_err(Error::from)`. Both are matched, so a
	// write conflict is classified the same whichever shape arrives.
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

/// How an explicit transaction block ended: the terminal state of the block
/// and, for a failure, where the request met it.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum BlockEnd {
	/// `COMMIT` succeeded.
	Committed,
	/// `CANCEL` ended a block that had not failed.
	Cancelled,
	/// A statement failed (`FailCause::Error`) or a nested `BEGIN` aborted the
	/// block (`FailCause::NestedBegin`). `end` is what terminated the block
	/// afterwards.
	Failed {
		cause: FailCause,
		end: FailedEnd,
	},
	/// A block that had not failed ran out of statements before its `COMMIT` or
	/// `CANCEL`.
	Unterminated,
	/// `COMMIT` itself failed. `error` is the storage error's text,
	/// `indeterminate` is set when the commit's outcome is unknown, and
	/// `conflict` when the failure is a retryable write conflict.
	CommitFailed {
		error: String,
		indeterminate: bool,
		conflict: bool,
	},
	/// A timeout or cancellation was observed before a statement ran. `end` is
	/// what terminated the block afterwards.
	Interrupted {
		reason: Reason,
		end: InterruptEnd,
	},
	/// The memory threshold was crossed before a statement ran.
	MemoryThreshold,
	/// The block's transaction could not be opened, so none of its statements
	/// ran. `end` is what terminated the block afterwards.
	BeginFailed {
		end: FailedEnd,
	},
}

/// What made a block fail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FailCause {
	/// A statement of the block returned an error.
	Error,
	/// A `BEGIN` was met inside the open block.
	NestedBegin,
}

/// What terminated a failed block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FailedEnd {
	Commit,
	Cancel,
	/// The request ran out of statements first.
	RequestEnd,
}

/// What terminated an interrupted block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum InterruptEnd {
	Commit,
	Cancel,
	/// The request ran out of statements first.
	RequestEnd,
}

/// What one result slot answers.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum SlotValue {
	/// The statement's value is `NONE`.
	None,
	/// The statement is answered by a query error of `kind`.
	Error {
		text: Cow<'static, str>,
		kind: Option<QueryError>,
	},
	/// The statement is answered by an internal error.
	Internal {
		text: Cow<'static, str>,
	},
}

/// The slot rewrites one block end implies. Every field is `None` when that
/// group of slots keeps what it holds.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct SlotRewrite {
	/// What every slot recorded since the block's `BEGIN` becomes.
	pub recorded: Option<SlotValue>,
	/// What each statement met after the failure or interrupt point answers,
	/// up to the terminator.
	pub following: Option<SlotValue>,
	/// The block's terminating `COMMIT` or `CANCEL`. `None` means the
	/// terminator takes no slot: the block has no terminator
	/// (`RequestEnd`, `MemoryThreshold`) or the terminator is not answered.
	pub terminator: Option<SlotValue>,
	/// The statement observed at the moment an interrupt was reported, when it
	/// is not the block's terminator. `None` means the block end involves no
	/// interrupt.
	pub interrupted_statement: Option<SlotValue>,
}

impl SlotRewrite {
	/// Names why a block's transaction could not be opened, in place of the
	/// generic not-started text, when the cause is one the request controls.
	fn name_begin_failure(&mut self, reason: &str) {
		let named = |text: String| SlotValue::Error {
			text: Cow::Owned(text),
			kind: Some(QueryError::NotExecuted),
		};
		if self.following.is_some() {
			self.following = Some(named(reason.to_string()));
		}
		if matches!(self.terminator, Some(SlotValue::Error { .. })) {
			self.terminator = Some(named(format!("Cannot COMMIT: {reason}")));
		}
	}
}

/// A `BEGIN` option the request supplied that cannot be honoured.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct BeginOptionError(String);

/// The message of a failed transaction open that the request itself caused:
/// a bad option, a bound above the maximum, or a bound no snapshot meets that
/// the request or policy forbids serving fresh.
fn begin_failure_reason(failure: &anyhow::Error) -> Option<String> {
	if failure.downcast_ref::<BeginOptionError>().is_some() {
		return Some(failure.to_string());
	}
	match failure.downcast_ref::<DatastoreError>() {
		Some(
			DatastoreError::IsolationNotSupported {
				..
			}
			| DatastoreError::SerializableStaleRead
			| DatastoreError::StalenessAboveMaximum {
				..
			}
			| DatastoreError::ReadSnapshotTooStale {
				..
			},
		) => Some(failure.to_string()),
		_ => None,
	}
}

const CANCELLED_TXN: &str = "The query was not executed due to a cancelled transaction";
const FAILED_TXN: &str = "The query was not executed due to a failed transaction";

fn error_slot(text: &'static str, kind: QueryError) -> Option<SlotValue> {
	Some(SlotValue::Error {
		text: Cow::Borrowed(text),
		kind: Some(kind),
	})
}

/// The slot rewrites of a block end, as one pure function of the end.
///
/// Texts and kinds are the ones a client of the request sees. Slots recorded
/// before the failure point are rewritten whenever the block does not commit,
/// because the rollback discards what they report. The failing statement's own
/// slot is not part of the rewrite: it is that statement's own error.
pub(super) fn finalise_slots(end: &BlockEnd) -> SlotRewrite {
	let mut rewrite = SlotRewrite {
		recorded: None,
		following: None,
		terminator: None,
		interrupted_statement: None,
	};
	match end {
		BlockEnd::Committed => {
			rewrite.terminator = Some(SlotValue::None);
		}
		BlockEnd::Cancelled => {
			rewrite.recorded = error_slot(CANCELLED_TXN, QueryError::Cancelled);
			rewrite.terminator = Some(SlotValue::None);
		}
		BlockEnd::Failed {
			cause: FailCause::Error,
			end,
		} => {
			rewrite.recorded = error_slot(FAILED_TXN, QueryError::NotExecuted);
			rewrite.following = error_slot(CANCELLED_TXN, QueryError::Cancelled);
			rewrite.terminator = match end {
				FailedEnd::Commit => error_slot(
					"Cannot COMMIT: the transaction was aborted due to a prior error",
					QueryError::NotExecuted,
				),
				// CANCEL answers NONE however the block ended, as it does for a
				// healthy block.
				FailedEnd::Cancel => Some(SlotValue::None),
				FailedEnd::RequestEnd => None,
			};
		}
		BlockEnd::Failed {
			cause: FailCause::NestedBegin,
			end,
		} => {
			rewrite.recorded = error_slot(FAILED_TXN, QueryError::NotExecuted);
			rewrite.following = error_slot(FAILED_TXN, QueryError::NotExecuted);
			rewrite.terminator = match end {
				FailedEnd::Commit => error_slot(
					"Cannot COMMIT: the transaction was aborted due to a nested BEGIN",
					QueryError::NotExecuted,
				),
				FailedEnd::Cancel => rewrite.following.clone(),
				FailedEnd::RequestEnd => None,
			};
		}
		BlockEnd::Unterminated => {
			rewrite.recorded = Some(SlotValue::Internal {
				text: Cow::Borrowed("Missing COMMIT statement"),
			});
		}
		BlockEnd::CommitFailed {
			error,
			indeterminate,
			conflict,
		} => {
			rewrite.recorded = if *indeterminate {
				Some(SlotValue::Error {
					text: Cow::Borrowed(
						"The transaction's commit outcome is unknown, so whether this query \
						 was applied is undetermined",
					),
					kind: None,
				})
			} else {
				error_slot(FAILED_TXN, QueryError::NotExecuted)
			};
			// A retryable conflict keeps its structured kind. An unknown
			// outcome carries none, so neither a retry nor a not-executed
			// reading applies.
			let kind = if *conflict {
				Some(QueryError::TransactionConflict)
			} else if *indeterminate {
				None
			} else {
				Some(QueryError::NotExecuted)
			};
			rewrite.terminator = Some(SlotValue::Error {
				text: Cow::Owned(format!("Cannot COMMIT: {error}")),
				kind,
			});
		}
		BlockEnd::Interrupted {
			reason,
			end,
		} => {
			rewrite.recorded = error_slot(CANCELLED_TXN, QueryError::Cancelled);
			// The statement observed at the interrupt answers as every statement
			// after it does, unless it is the terminator, which has its own slot.
			rewrite.following = Some(match reason {
				Reason::Timedout(d) => SlotValue::Error {
					text: Cow::Owned(format!("Timed out: {d}")),
					kind: Some(QueryError::TimedOut {
						duration: d.0,
					}),
				},
				Reason::Canceled => SlotValue::Error {
					text: Cow::Borrowed(CANCELLED_TXN),
					kind: Some(QueryError::Cancelled),
				},
			});
			rewrite.terminator = match end {
				InterruptEnd::Commit => Some(match reason {
					Reason::Timedout(d) => SlotValue::Error {
						text: Cow::Owned(format!("Cannot COMMIT: timed out ({d})")),
						kind: Some(QueryError::TimedOut {
							duration: d.0,
						}),
					},
					Reason::Canceled => SlotValue::Error {
						text: Cow::Borrowed("Cannot COMMIT: the transaction was cancelled"),
						kind: Some(QueryError::Cancelled),
					},
				}),
				InterruptEnd::Cancel => rewrite.following.clone(),
				InterruptEnd::RequestEnd => None,
			};
			rewrite.interrupted_statement = rewrite.following.clone();
		}
		BlockEnd::MemoryThreshold => {
			rewrite.recorded = error_slot(CANCELLED_TXN, QueryError::Cancelled);
		}
		BlockEnd::BeginFailed {
			end,
		} => {
			// Nothing was recorded since the `BEGIN`: no statement of the block
			// ran, and each one met is answered as it is pulled.
			rewrite.following = error_slot(
				"Tried to start a transaction while another transaction was open",
				QueryError::NotExecuted,
			);
			rewrite.terminator = match end {
				FailedEnd::Commit => error_slot(
					"Cannot COMMIT: the transaction could not be started",
					QueryError::NotExecuted,
				),
				// CANCEL answers NONE however the block ended.
				FailedEnd::Cancel => Some(SlotValue::None),
				FailedEnd::RequestEnd => None,
			};
		}
	}
	rewrite
}

/// The result a slot answers. `time` is the elapsed time of the statement the
/// slot belongs to, zero for a statement that never ran.
fn slot_result(slot: &SlotValue, time: Duration) -> QueryResult {
	QueryResult {
		time,
		result: slot_outcome(slot),
		query_type: QueryType::Other,
	}
}

fn slot_outcome(slot: &SlotValue) -> std::result::Result<surrealdb_types::Value, TypesError> {
	match slot {
		SlotValue::None => Ok(surrealdb_types::Value::None),
		SlotValue::Error {
			text,
			kind,
		} => Err(TypesError::query(text.to_string(), kind.clone())),
		SlotValue::Internal {
			text,
		} => Err(TypesError::internal(text.to_string())),
	}
}

/// Where a streaming execution sends its output.
///
/// Only rows stream: a statement's `QueryResult` is one small entry either way,
/// so [`Executor::results`] still accumulates them, and this sends
/// [`QueryStreamItem`]s alongside. The transaction-block bookkeeping, which
/// rewrites already-produced results when a block fails, reads them the same way
/// whether or not they are streamed.
///
/// The ordering it maintains is the one the wire requires: a statement's rows
/// go out while it runs, and its `Finished` only once its outcome is final.
///
/// The rows of a held transaction never stream: its statements reach the sink
/// only through [`Executor::finalise_results`], once the transaction resolves.
struct StreamSink {
	items: Sender<QueryStreamItem>,
	/// Items produced by a write statement running in a transaction of its own,
	/// held back while [`Self::stage`] is [`Stage::Holding`] so that a
	/// retryable commit conflict can discard the failed attempt's rows before
	/// the statement runs again.
	staged: Vec<QueryStreamItem>,
	/// The estimated bytes of the rows in [`Self::staged`].
	staged_bytes: usize,
	/// The most estimated bytes [`Self::staged`] holds: past it the held items
	/// are sent and the attempt's later items go out as they are produced
	/// (`stream_stage_bytes`).
	stage_max: usize,
	/// Whether the attempt running holds its items back.
	stage: Stage,
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
	/// The trace kinds the consumer asked for. Empty records nothing.
	trace: surrealdb_rpc::TraceKinds,
	/// The trace of each statement not yet finalised that its rows have not
	/// carried: what a top-level `LET` bound, and what its reads recorded after
	/// its last batch of rows. Sent before the statement's `Finished` once its
	/// outcome is final, and dropped with a statement that failed or was
	/// rolled back.
	statement_traces: HashMap<usize, surrealdb_rpc::Trace>,
	/// The statement whose reads record into a trace ([`StatementTrace`]),
	/// when the consumer asked for a kind they record: its index and the
	/// trace. Created when the statement is first planned to run, and folded
	/// into [`Self::statement_traces`] once another statement runs or results
	/// are finalised.
	reads: Option<(usize, Arc<crate::exec::trace::StatementTrace>)>,
	/// How many of [`Executor::results`] have been finalised. Results at or
	/// past this index are still provisional — inside an unresolved
	/// `BEGIN … COMMIT` block, they may yet be rewritten into errors.
	finalised: usize,
}

/// Whether a [`StreamSink`] holds back the items of the attempt running.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
	/// Items go out as they are produced.
	Off,
	/// Items are held until the attempt's commit outcome is known.
	Holding,
	/// The attempt's items passed the staging bound and went out: the
	/// consumer has seen rows of this attempt, so it is not run again.
	Released,
}

impl StreamSink {
	/// Send one item, ignoring a consumer that has stopped reading.
	///
	/// A closed channel is not an error here: the caller has already been told
	/// to stop by [`Self::is_closed`], and an item produced in the meantime has
	/// nowhere to go.
	async fn send(&mut self, item: QueryStreamItem) {
		self.send_rows(item, 0).await;
	}

	/// Send one item of rows whose estimated size is `bytes`, holding it back
	/// while the attempt is staging. The item that takes the held bytes past
	/// the staging bound sends every held item, and the attempt is
	/// [`Stage::Released`].
	async fn send_rows(&mut self, item: QueryStreamItem, bytes: usize) {
		if self.stage != Stage::Holding {
			let _ = self.items.send(item).await;
			return;
		}
		self.staged.push(item);
		self.staged_bytes = self.staged_bytes.saturating_add(bytes);
		if self.staged_bytes > self.stage_max {
			self.stage = Stage::Released;
			self.send_staged().await;
		}
	}

	/// Stage without a bound: the consumer gathers the whole answer before any
	/// of it reaches a client, so an attempt's items never count as delivered
	/// and a write that loses a retryable conflict is always run again.
	fn collect_whole(&mut self) {
		self.stage_max = usize::MAX;
	}

	/// Whether items are being held back.
	fn holding(&self) -> bool {
		self.stage == Stage::Holding
	}

	/// Whether an item's size is wanted: items are held back under a bound.
	/// With no bound (`usize::MAX`) nothing is measured.
	fn measuring(&self) -> bool {
		self.holding() && self.stage_max != usize::MAX
	}

	/// Hold back items produced by a write statement running in a transaction
	/// of its own until its commit outcome is known or they pass the staging
	/// bound.
	fn begin_staging(&mut self) {
		self.stage = Stage::Holding;
		self.staged.clear();
		self.staged_bytes = 0;
	}

	/// Send every held item once the attempt has committed, and stop staging.
	async fn flush_staged(&mut self) {
		self.stage = Stage::Off;
		self.send_staged().await;
	}

	/// Send every held item, in the order it was produced.
	async fn send_staged(&mut self) {
		self.staged_bytes = 0;
		for item in std::mem::take(&mut self.staged) {
			let _ = self.items.send(item).await;
		}
	}

	/// Discard held items and the streamed row count for `index` after an
	/// attempt that failed, whether it is run again or its error is final,
	/// and stop staging. Answers whether any of the attempt's items had
	/// already reached the consumer, in which case it must not run again.
	fn discard_staged(&mut self, index: usize) -> bool {
		// What the attempt's reads recorded goes with its rows.
		if let Some((at, reads)) = &self.reads
			&& *at == index
		{
			let _ = reads.take();
		}
		let released = self.stage == Stage::Released;
		self.stage = Stage::Off;
		self.staged.clear();
		self.staged_bytes = 0;
		self.streamed.remove(&index);
		released
	}

	/// Whether the consumer has gone away.
	fn is_closed(&self) -> bool {
		self.items.is_closed()
	}

	/// The trace statement `index`'s reads record into, when the consumer
	/// asked for a kind they record. The trace of a statement before it is
	/// folded into [`Self::statement_traces`] first.
	fn reads_for(&mut self, index: usize) -> Option<Arc<crate::exec::trace::StatementTrace>> {
		if let Some((at, reads)) = &self.reads
			&& *at == index
		{
			return Some(Arc::clone(reads));
		}
		self.fold_reads();
		let reads = Arc::new(crate::exec::trace::StatementTrace::new(self.trace)?);
		self.reads = Some((index, Arc::clone(&reads)));
		Some(reads)
	}

	/// Move what the running statement's reads recorded and its rows did not
	/// carry into [`Self::statement_traces`], and stop recording it.
	fn fold_reads(&mut self) {
		if let Some((index, reads)) = self.reads.take() {
			let trace = reads.take();
			if !trace.is_empty() {
				self.statement_traces.entry(index).or_default().absorb(trace);
			}
		}
	}

	/// Send what statement `index`'s reads recorded so far, ahead of the batch
	/// of its rows about to be sent, holding it back with them while staging.
	async fn send_reads(&mut self, index: usize) {
		let Some((at, reads)) = &self.reads else {
			return;
		};
		if *at != index {
			return;
		}
		// Charged at what it holds, so a held attempt's traces count toward the
		// staging bound with its rows.
		let (trace, bytes) = reads.take_sized();
		if !trace.is_empty() {
			self.send_rows(
				QueryStreamItem::Trace {
					index,
					trace: Box::new(trace),
				},
				bytes,
			)
			.await;
		}
	}
}

pub(crate) struct PreparedBroker {
	receiver: async_channel::Receiver<RoutedNotification>,
	delivery: Arc<OrderedBroker>,
	/// What the transaction wrote, for the subscriptions its snapshot did not
	/// hold, and where to read them once it commits.
	recheck: Option<crate::dbs::live_recheck::RecheckHandle>,
}

/// Flush a batch's buffered live-query notifications once the caller-owned
/// transaction that produced them commits.
///
/// Only the owner commits such a transaction, so the executor has no commit of
/// its own to flush after. Queued on the transaction, the notifications go out
/// with the commit and are dropped with a cancel or a failed commit.
pub(crate) struct FlushOnCommit(PreparedBroker);

impl FlushOnCommit {
	/// Flush `prepared` once the transaction holding this action commits. The
	/// transaction holds the recheck, so its write contexts are detached as
	/// they are noted rather than held until the commit.
	pub(crate) fn new(prepared: PreparedBroker) -> Self {
		if let Some(recheck) = &prepared.recheck {
			recheck.detach_at_write();
		}
		Self(prepared)
	}
}

impl CommitAction for FlushOnCommit {
	fn run(self: Box<Self>) -> Pin<Box<dyn Future<Output = ()> + Send>> {
		crate::exec::vm::out_of_line_boxed(|| async move {
			flush_live_query_notifications(self.0);
		})
	}

	/// Flushes the whole batch's notifications once the transaction commits.
	fn scope(&self) -> ActionScope {
		ActionScope::Transaction
	}
}

/// What the top-level `LET`s of one stream may retain between statements.
///
/// A stream that is not held in memory as a whole (an import) binds every
/// top-level `LET` into the request context for the rest of the stream, so
/// without a bound the carried parameters grow with the input. Each limit is
/// checked as a `LET` binds, by [`CarryBound::admit`]: `names` caps the
/// distinct names bound, `bytes` caps an estimate of the values bound. A limit
/// of `None` is not checked and costs nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CarryBound {
	pub names: Option<usize>,
	pub bytes: Option<usize>,
}

impl CarryBound {
	/// No limit on either count.
	pub(crate) const UNBOUNDED: CarryBound = CarryBound {
		names: None,
		bytes: None,
	};

	/// Record the binding of `name` to `value` in `carried`, or refuse it.
	///
	/// A rebinding replaces the entry of its name, so it counts once. The
	/// estimate of a value is walked only when `bytes` is set. A refused
	/// binding leaves `carried` unchanged.
	pub(crate) fn admit(
		&self,
		carried: &mut Carried,
		name: &str,
		value: &Value,
	) -> Result<(), ExecError> {
		if self.names.is_none() && self.bytes.is_none() {
			return Ok(());
		}
		let previous = carried.held.get(name).copied();
		if let Some(limit) = self.names
			&& previous.is_none()
			&& carried.held.len() >= limit
		{
			return Err(ExecError::InvalidStatement(format!(
				"The stream defined more parameters with LET than its limit of {limit}"
			)));
		}
		let size = match self.bytes {
			Some(_) => name.len() + estimate_bytes(value),
			None => 0,
		};
		let total = carried.bytes - previous.unwrap_or(0) + size;
		if let Some(limit) = self.bytes
			&& total > limit
		{
			return Err(ExecError::InvalidStatement(format!(
				"The stream retains more than {limit} bytes of parameters"
			)));
		}
		carried.held.insert(name.to_owned(), size);
		carried.bytes = total;
		Ok(())
	}
}

/// The request `LET` bindings written since the open block's `BEGIN`, each with
/// what it overwrote, so a block that does not commit can put them back.
///
/// A block's mark is set by [`LetUndo::begin`] and cleared by exactly one of
/// [`LetUndo::commit`] or [`LetUndo::rollback`]; there is at most one open
/// block because blocks never nest. [`LetUndo::record`] is a no-op while no
/// mark is set, so top-level `LET`s outside a block and every statement of a
/// session transaction record nothing. `USE` and the session variables are
/// written elsewhere and are never recorded: a block's end does not undo them.
#[derive(Default)]
struct LetUndo {
	mark: Option<usize>,
	log: Vec<(Strand, Option<Arc<Value>>)>,
}

impl LetUndo {
	/// Whether a block is open, which is exactly when [`LetUndo::record`]
	/// keeps what it is given.
	fn is_open(&self) -> bool {
		self.mark.is_some()
	}

	/// Open a block: what is recorded from here on belongs to it.
	fn begin(&mut self) {
		self.mark = Some(self.log.len());
	}

	/// Record that `name` held `prev` before the binding about to overwrite
	/// it, `None` where the request context held no binding of its own. Kept
	/// only while a block is open.
	fn record(&mut self, name: &str, prev: Option<Arc<Value>>) {
		if self.mark.is_some() {
			self.log.push((Strand::from(name), prev));
		}
	}

	/// The block committed: keep its bindings and forget how to undo them.
	fn commit(&mut self) {
		if let Some(mark) = self.mark.take() {
			self.log.truncate(mark);
		}
	}

	/// The block did not commit: put its bindings back in reverse order of
	/// writing, so a name written twice ends at the value it held at `BEGIN`.
	/// A name that held nothing is restored as absent, so a `DEFINE PARAM` of
	/// the same name, which resolves only while the request binds none, is
	/// answered again rather than shadowed by `NONE`.
	fn rollback(&mut self, ctx: &mut Context) {
		if let Some(mark) = self.mark.take() {
			while self.log.len() > mark {
				if let Some((name, prev)) = self.log.pop() {
					ctx.restore_value(&name, prev);
				}
			}
		}
	}
}

/// The top-level `LET` bindings a stream holds, as [`CarryBound::admit`]
/// tracks them: each name with the bytes estimated for its current value, and
/// the sum of those estimates.
#[derive(Default)]
pub(crate) struct Carried {
	held: HashMap<String, usize>,
	bytes: usize,
}

/// An estimate of the memory `value` retains: the inline size of every node
/// plus what each holds on the heap — strings, byte buffers, names, a
/// geometry's coordinates and a closure's captures.
///
/// Iterative, so a deeply nested value cannot overflow the stack. The
/// estimate is a measure for the bound, not an allocator's account.
fn estimate_bytes(value: &Value) -> usize {
	use crate::val::RecordIdKey;

	let node = std::mem::size_of::<Value>();
	let mut total = 0;
	let mut stack = vec![value];
	while let Some(value) = stack.pop() {
		total += node;
		match value {
			Value::String(s) => total += s.len(),
			Value::Bytes(b) => total += b.len(),
			Value::Array(a) => stack.extend(a.iter()),
			Value::Set(s) => stack.extend(s.iter()),
			Value::Object(o) => {
				for (key, value) in o.0.iter() {
					total += key.len();
					stack.push(value);
				}
			}
			Value::RecordId(id) => {
				total += id.table.as_str().len();
				match &id.key {
					RecordIdKey::String(s) => total += s.len(),
					RecordIdKey::Array(a) => stack.extend(a.iter()),
					RecordIdKey::Object(o) => {
						for (key, value) in o.0.iter() {
							total += key.len();
							stack.push(value);
						}
					}
					RecordIdKey::Number(_) | RecordIdKey::Uuid(_) | RecordIdKey::Range(_) => {}
				}
			}
			Value::Range(r) => {
				for bound in [&r.start, &r.end] {
					if let std::ops::Bound::Included(v) | std::ops::Bound::Excluded(v) = bound {
						stack.push(v);
					}
				}
			}
			Value::Geometry(geometry) => total += geometry.heap_bytes(),
			Value::Table(table) => total += table.as_str().len(),
			Value::File(file) => total += file.bucket.len() + file.key.len(),
			Value::Regex(regex) => total += regex.0.as_str().len(),
			// What a closure captured is held with it.
			Value::Closure(_) => total += crate::exec::memory::size::value_bytes(value),
			Value::None
			| Value::Null
			| Value::Bool(_)
			| Value::Number(_)
			| Value::Duration(_)
			| Value::Datetime(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_)
			| Value::Uuid(_) => {}
		}
	}
	total
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
	/// The index in `results` of the `BEGIN` result being opened, which a read-only
	/// block overwrites with the description of its snapshot.
	begin_result_slot: Option<usize>,
	/// The bare `LIVE` statement converted as it arrived, with the unit
	/// compiled for it in a read transaction before its write transaction
	/// opened. [`Executor::run_live`] takes it, so the statement registers
	/// under the id and unit it arrived with; it is empty outside the one
	/// statement that set it.
	arrived_live:
		Option<(crate::expr::statements::LiveStatement, Option<crate::exec::vm::live::Arrived>)>,
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
	/// True while the statements of a transaction the caller holds across
	/// requests run, which is exactly the span of
	/// [`Executor::execute_statements_with_transaction`]. It is what
	/// [`Executor::region_context`] reads to tell a session transaction from
	/// an explicit block, since both run with `in_block` set.
	in_session_txn: bool,
	/// True while the executor drives a stream whose every statement is a
	/// fragment of its own, which is exactly the span of
	/// [`Executor::execute_fragments`]. A statement met while an explicit block
	/// is open then continues the region an earlier fragment opened.
	fragmented: bool,
	/// Where each name the request binds came from, as a history per name of
	/// `(from, origin)` pairs in ascending `from`: the origin holds for the
	/// statements at positions `from` and after, until the next pair. An input
	/// recorded by [`Executor::note_inputs`] holds from position 0.
	/// [`Executor::compile_request`] adds a pair for a top-level `LET` at
	/// position `p` from `p + 1`: `LetConst` where `constant_value` denotes its
	/// value and `LetComputed` where the run computes it. A block that ends adds
	/// a `LetComputed` pair from the statement after its terminator for every
	/// name a `LET` inside it bound, because the block's end may have put the
	/// bindings back. A name absent here has no recorded origin.
	binding_origins: HashMap<String, Vec<(usize, BindingOrigin)>>,
	/// What the top-level `LET`s of the stream this executor drives may retain.
	carry: CarryBound,
	/// The top-level `LET` bindings admitted so far, under `carry`.
	carried: Carried,
	/// Set when a `LET` was refused by `carry`. The stream ends at the
	/// statement that set it: a block it was inside is cancelled and nothing
	/// after it is pulled.
	carry_crossed: bool,
	/// The refusal of the region rejection ([`request_rejection`]) found
	/// while the request was compiled at its arrival, before any statement of
	/// it ran. It refuses the request as a whole: no statement runs, and the
	/// driver answers the request with it.
	request_rejection: Option<crate::exec::vm::payload::Refusal>,
	/// The bindings the open explicit block has written, for the undo its end
	/// may owe them.
	let_undo: LetUndo,
	/// Where the program of a top-level statement records the changes it makes
	/// to this session, which [`Executor::apply_session_effects`] applies.
	session_sink: Arc<crate::exec::context::SessionSink>,
	/// A statement whose slow-log check waits for the work its transaction
	/// leaves for after the commit, with the logical form that supplies its
	/// parameters (see [`Executor::run_on_streaming_engine`]). Taken by
	/// [`Executor::execute_plan_impl`] once that work has settled.
	slow_log_after_commit: Option<(TopLevelStatement, LogicalOp)>,
	/// The statements of the open explicit block that queued work for after
	/// its commit, in the order they ran, each with the event it holds until
	/// that work settles (see [`Executor::settle_block_waits`]).
	block_waits: Vec<BlockWait>,
	/// Whether the statement running binds its rows to a `LET`'s name: the statements after it
	/// read them in the order they were bound, so the plan runs them in the order of the serial
	/// run.
	binds_rows: bool,
	/// When the batch this executor drives began, which the slow-log entry for
	/// a batch its timeout or cancellation cuts short measures from (see
	/// [`Executor::slow_log_interrupted`]). `None` outside a batch.
	batch_start: Option<Instant>,
	/// Whether the slow log already accounts for the interruption of the
	/// batch, by an entry for the batch or for the statement the interruption
	/// cut short. Every statement after an interruption observes it, and this
	/// keeps the batch to one entry.
	interruption_slow_logged: bool,
}

/// A statement of an explicit block that queued work for after the block's
/// commit: the wait for the index it built to be materialised.
struct BlockWait {
	/// The statement's slot in [`Executor::results`].
	slot: usize,
	/// How many waits it queued, consecutive in the block's queue.
	waits: usize,
	/// Its `StatementEvent`, held until its waits settle, so that the event's
	/// outcome, error class and duration are the ones its slot ends with.
	event: HeldStatementEvent,
}

/// What a `StatementEvent` held back from a block statement is emitted with.
struct HeldStatementEvent {
	kind: StatementType,
	read_only: bool,
	sql: Option<String>,
	/// When the statement began, which both its slot's time and its event's
	/// duration are measured from.
	before: Instant,
	rows: u64,
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

/// The result of a statement once the creates it held are checked ([`Transaction::put_held`]):
/// `result` when every held key was absent, and otherwise the duplicate the earliest failing
/// create would have raised had its check been immediate, whatever the statement went on to do.
async fn settle_held_creates(txn: &Transaction, result: FlowResult<Value>) -> FlowResult<Value> {
	match txn.take_absence_violation().await {
		Ok(None) => result,
		Ok(Some(key)) => Err(ControlFlow::Err(held_create_exists(&key))),
		Err(e) => match result {
			Ok(_) => Err(ControlFlow::Err(e.into())),
			Err(prior) => Err(prior),
		},
	}
}

/// The error a create of the record at `key` raises when the record exists.
pub(crate) fn held_create_exists(key: &[u8]) -> anyhow::Error {
	use crate::key::KVKeyDecode as _;
	match crate::key::schema::RecordKey::decode_key(key) {
		Ok(key) => anyhow::Error::new(crate::doc::Error::RecordExists {
			record: crate::val::RecordId {
				table: key.tb.into_owned(),
				key: key.id.into_owned(),
			},
		}),
		Err(e) => e,
	}
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
/// The error a statement fails with when the query's timeout or cancellation
/// has ended it.
fn interruption_error(reason: Reason) -> anyhow::Error {
	match reason {
		Reason::Timedout(d) => EngineError::QueryTimedout(d.0).into(),
		Reason::Canceled => EngineError::QueryCancelled.into(),
	}
}

pub(crate) fn context_mut(ctx: &mut Arc<Context>) -> Result<&mut Context, anyhow::Error> {
	Arc::get_mut(ctx)
		.ok_or_else(|| {
			EngineError::unreachable("Tried to unfreeze a Context with multiple references")
		})
		.map_err(anyhow::Error::new)
}

/// Install a notification broker on `ctx`, which buffers the notifications of the statement that
/// runs next for `delivery`, and answer the handle that delivers them once its transaction has
/// committed. Answers `None` where a higher layer already provided a broker, or where `ctx` is
/// shared and cannot be changed.
///
/// Under the inline live-query engine the broker also collects what the transaction writes, so
/// that the subscriptions registered while it ran, which its snapshot does not hold, are told
/// once it commits, read through `source` ([`crate::dbs::live_recheck`]).
pub(crate) fn install_broker(
	ctx: &mut FrozenContext,
	delivery: Arc<OrderedBroker>,
	source: crate::dbs::live_recheck::RecheckSource,
) -> Option<PreparedBroker> {
	// If a broker is already provided by a higher layer, don't override it here.
	if ctx.broker().is_some() {
		return None;
	}
	let (send, recv) = async_channel::unbounded();
	let Some(ctx) = Arc::get_mut(ctx) else {
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
	let recheck = (ctx.config.datastore.live_query_engine == crate::kvs::LiveQueryEngine::Inline)
		.then(|| Arc::new(crate::dbs::live_recheck::LiveRecheck::default()));
	ctx.set_broker(Some(DefaultBroker::new(send, Arc::clone(&delivery) as Arc<dyn MessageBroker>)));
	ctx.set_live_recheck(recheck.clone());
	Some(PreparedBroker {
		receiver: recv,
		delivery,
		recheck: recheck
			.map(|recheck| crate::dbs::live_recheck::RecheckHandle::new(recheck, source)),
	})
}

/// Hand the notifications `prepared` buffered to the broker that delivers them. Called only once
/// the transaction that produced them has committed, and before the statement that committed it
/// answers.
///
/// What the transaction buffered is queued before this returns, so the notifications of commits a
/// client makes one after another are delivered in the order of those commits. A context that
/// outlives the commit may still hold a sender — a `KILL` queues its notification for after the
/// commit — and what it sends later is queued as it arrives, behind everything queued here.
pub(crate) fn flush_live_query_notifications(prepared: PreparedBroker) {
	let PreparedBroker {
		receiver,
		delivery,
		recheck,
	} = prepared;
	if let Some(recheck) = recheck {
		crate::dbs::live_recheck::spawn(recheck, Arc::clone(&delivery) as Arc<dyn MessageBroker>);
	}
	// Read before draining: once every sender is gone nothing more can arrive,
	// so the drain below takes everything there will ever be.
	let closed = receiver.is_closed();
	while let Ok(item) = receiver.try_recv() {
		delivery.enqueue(item);
	}
	if closed {
		return;
	}
	common::rt::spawn_detached(async move {
		while let Ok(item) = receiver.recv().await {
			delivery.enqueue(item);
		}
	});
}

/// Install a fresh [`StatementCounters`] and the statement clock on `ctx` for the next top-level
/// statement, and return a clone of the counters handle so the caller can read the counts after the
/// statement returns. The iterator (deep in the execution path) inherits the same handle through
/// the child contexts it derives from `ctx`.
///
/// The clock is read here, once per top-level statement, so every CLOCK read the statement makes
/// answers the instant it started, however long it runs. A nested query and an event body run in
/// contexts derived from `ctx` and read that instant. The statements of a block, of the body of a
/// `FOR` or an `IF`, and of a function body each take an instant of their own when they start, in a
/// context derived from this one, and the statements they hold read that.
///
/// Between statements the caller holds the only strong reference to `ctx`, so `Arc::get_mut`
/// succeeds. If it ever fails, because some child kept a strong reference past statement
/// completion, the iterator would record into the previous statement's counters (or `None`) while
/// `count_result_rows` reads zero from the new one, silently underreporting the affected rows of
/// DML statements that use `RETURN NONE`, which is what `StatementCounters` exists to count. That
/// is a programmer error: in debug builds it panics so tests fail loud, and in release builds it
/// logs a `warn` (deduped per process). A stale instant, never the wall clock, is the failure mode.
pub(crate) fn install_statement_state(ctx: &mut FrozenContext) -> Arc<StatementCounters> {
	let counters = StatementCounters::new();
	if let Some(ctx) = Arc::get_mut(ctx) {
		ctx.set_statement_counters(Some(Arc::clone(&counters)));
		ctx.set_statement_clock(crate::val::Datetime::now());
	} else {
		debug_assert!(
			false,
			"install_statement_state: ctx Arc was contended at statement boundary"
		);
		static WARNED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
		if !WARNED.swap(true, std::sync::atomic::Ordering::Relaxed) {
			tracing::warn!(
				target: "surrealdb::core::dbs",
				"install_statement_state: ctx Arc contended at statement boundary; \
				 result_rows on RETURN NONE DML may underreport. This is a bug — \
				 future occurrences will not log."
			);
		}
	}
	counters
}

/// Whether `op` is a write statement, bare or under its `TIMEOUT`: one the
/// cache of compiled statements may keep ([`crate::exec::vm::serve::kept_for_requests`]).
fn is_write_statement(op: &LogicalOp) -> bool {
	matches!(op, LogicalOp::Write(_))
		|| matches!(op, LogicalOp::Timeout(timeout) if matches!(timeout.input.as_ref(), LogicalOp::Write(_)))
}

/// Guard that stops the timeout task when dropped, so it does not
/// outlive the execution it was bounding.
///
/// The task has to be spawned rather than raced inside the query
/// future: the point is for it to fire while the executor is inside an
/// operator that is not otherwise yielding to check the deadline.
#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
pub(crate) struct TimeoutGuard(common::rt::JoinHandle<()>);
#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
impl Drop for TimeoutGuard {
	fn drop(&mut self) {
		self.0.abort();
	}
}

/// The browser executor returns no handle to abort, so there the task
/// is stopped by a token it waits on alongside the timer.
#[cfg(all(target_family = "wasm", target_os = "unknown"))]
pub(crate) struct TimeoutGuard(tokio_util::sync::CancellationToken);
#[cfg(all(target_family = "wasm", target_os = "unknown"))]
impl Drop for TimeoutGuard {
	fn drop(&mut self) {
		self.0.cancel();
	}
}

/// Cancels `token` once `timeout` elapses, unless the returned guard is
/// dropped first.
pub(crate) fn spawn_timeout(
	timeout: Duration,
	token: tokio_util::sync::CancellationToken,
) -> TimeoutGuard {
	#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
	{
		TimeoutGuard(common::rt::spawn(async move {
			common::time::sleep(timeout).await;
			token.cancel();
		}))
	}
	#[cfg(all(target_family = "wasm", target_os = "unknown"))]
	{
		let finished = tokio_util::sync::CancellationToken::new();
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

/// Commit the transaction of a statement that ran in a transaction of its own.
///
/// A transaction conflict propagates unwrapped so its structured kind (`TransactionConflict`, wire
/// -32009) survives the conversion to a result row and SDK `.retry()` can recognise it. Wrapping it
/// in `QueryNotExecuted` (as non-conflict commit failures are) would erase that signal. A commit
/// whose outcome is unknown propagates unwrapped for the same reason: `QueryNotExecuted` asserts
/// the work did not happen, and here it may have. Callers that read that as "safe to replay" would
/// double-apply a non-idempotent statement.
pub(crate) async fn commit_statement(txn: &Transaction) -> Result<()> {
	if let Err(e) = txn.commit().await {
		if crate::kvs::is_retryable_transaction_conflict(&e) {
			return Err(e);
		}
		if crate::kvs::is_indeterminate_commit(&e) {
			return Err(e);
		}
		bail!(DatastoreError::QueryNotExecuted {
			message: e.to_string(),
		});
	}
	Ok(())
}

/// Why a top-level `LET` binding was refused.
pub(crate) enum SessionLetRefusal {
	/// The name is protected or bound by the system.
	Name(ExecError),
	/// The binding crosses the stream's carry bound, which ends the stream.
	Carry(ExecError),
}

impl SessionLetRefusal {
	/// The error the refused statement fails with.
	pub(crate) fn into_error(self) -> ExecError {
		match self {
			SessionLetRefusal::Name(error) | SessionLetRefusal::Carry(error) => error,
		}
	}
}

/// Admit the binding of the top-level `LET` `name` to `value`, or refuse it.
///
/// SECURITY: a protected name (`$auth`, `$token`, `$session`, `$access`) is never bound by a
/// `LET`, whichever path delivers the binding. A top-level `LET` binds a variable of the request,
/// as a client's own `set` does, so a name the system binds in a frame (`$this`, `$value`, ...)
/// is admitted here and shadowed by the frame that binds it. A binding the carry bound refuses is
/// recorded nowhere. Every driver of top-level statements admits a binding here before it writes
/// it to the request context.
pub(crate) fn admit_session_let(
	carry: &CarryBound,
	carried: &mut Carried,
	name: &str,
	value: &Value,
) -> std::result::Result<(), SessionLetRefusal> {
	if surrealdb_cnf::PROTECTED_PARAM_NAMES.contains(&name) {
		return Err(SessionLetRefusal::Name(ExecError::InvalidParam {
			name: name.to_owned(),
		}));
	}
	carry.admit(carried, name, value).map_err(SessionLetRefusal::Carry)
}

/// Converts a finished statement's internal result value to a public value, honouring the `shape`
/// of the plan that produced it. A `Rows` result is an array-of-rows envelope, so each row is
/// converted as its own root at depth 1 - the same anchor the storage write guard and the
/// streaming read path use - rather than charging the envelope a nesting level. A `Scalar` result
/// is the single value and is converted whole.
pub(crate) fn statement_result(shape: OutputShape, value: Value) -> Result<surrealdb_types::Value> {
	match shape {
		OutputShape::Rows => convert_result_rows_to_public(value),
		OutputShape::Scalar => convert_value_to_public_value(value),
	}
}

/// The error a statement the engine did not plan fails with. A decline met in a session whose
/// namespace or database does not exist is that namespace or database not found, as the context
/// check of a statement that reaches it raises it; any other failure is its own.
pub(crate) async fn planning_failed(
	opt: &Options,
	txn: &Transaction,
	error: Error,
) -> anyhow::Error {
	if matches!(error, Error::Exec(ExecError::PlannerUnsupported { .. }))
		&& let (Ok(ns), Ok(db)) = (opt.ns(), opt.db())
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
/// No expression-layer value is built on this path. A shape with no logical form is reported as
/// `PlannerUnsupported`, the same error the planner raises. A `SELECT` is checked against the
/// `FOR UPDATE` rules before lowering, on the runtime syntax the parser produced. The expression
/// is consumed.
pub(crate) fn lower_statement(expr: Expr) -> Result<LogicalOp, Error> {
	if let Expr::Select(select) = &expr {
		crate::dbs::validate_for_update(select)?;
	}
	lower(expr).map_err(|reason| {
		ExecError::PlannerUnsupported {
			reason: reason.to_string(),
		}
		.into()
	})
}

impl Executor {
	/// Install a per-statement notification broker when `writable` is true and the session has
	/// notifications enabled. Read-only bare statements skip installation — they cannot emit
	/// LIVE/KILL notifications — avoiding allocation and stale-broker leaks on the hot path.
	#[inline]
	fn prepare_broker(
		&mut self,
		kvs: &Datastore,
		writable: bool,
		delivery: Option<Arc<OrderedBroker>>,
	) -> Option<PreparedBroker> {
		if !writable {
			return None;
		}
		let delivery = delivery?;
		let source = crate::dbs::live_recheck::RecheckSource {
			tf: kvs.transaction_factory().clone(),
			sequences: kvs.sequences().clone(),
		};
		let prepared = install_broker(&mut self.ctx, delivery, source)?;
		self.broker_owned_by_executor = true;
		Some(prepared)
	}

	/// Commit a statement's own transaction ([`commit_statement`]), then flush the
	/// live-query notifications it buffered. Notifications go out only once the
	/// write is durable; a failed commit drops them undelivered.
	async fn commit_and_flush(txn: &Transaction, receiver: Option<PreparedBroker>) -> Result<()> {
		commit_statement(txn).await?;
		if let Some(prepared) = receiver {
			flush_live_query_notifications(prepared);
		}
		Ok(())
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
			begin_result_slot: None,
			arrived_live: None,
			misplaced_control: None,
			explicit_emission: Emission::Stream,
			holding: false,
			in_session_txn: false,
			fragmented: false,
			binding_origins: HashMap::new(),
			carry: CarryBound::UNBOUNDED,
			carried: Carried::default(),
			carry_crossed: false,
			request_rejection: None,
			let_undo: LetUndo::default(),
			session_sink: Arc::default(),
			slow_log_after_commit: None,
			block_waits: Vec::new(),
			binds_rows: false,
			batch_start: None,
			interruption_slow_logged: false,
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
		trace: surrealdb_rpc::TraceKinds,
	) -> Self {
		let mut this = Self::new(kvs, ctx, opt);
		this.stream = Some(StreamSink {
			items,
			staged: Vec::new(),
			staged_bytes: 0,
			stage_max: kvs.config().exec.stream_stage_bytes,
			stage: Stage::Off,
			row_format,
			trace,
			statement_traces: HashMap::new(),
			reads: None,
			streamed: HashMap::new(),
			finalised: 0,
		});
		this
	}

	/// Hold every attempt's items until its commit outcome is known, however
	/// many there are, for a consumer that gathers the whole answer before any
	/// of it reaches a client: nothing it receives has been delivered, so a
	/// write that loses a retryable conflict is always run again.
	fn collect_whole_answer(&mut self) {
		if let Some(sink) = self.stream.as_mut() {
			sink.collect_whole();
		}
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

	/// The region context `place` is compiled in: how the statement commits,
	/// which transaction holds it, and the regions an earlier batch left open.
	///
	/// A session transaction holds every statement of its request. Otherwise
	/// `BEGIN`, `COMMIT`, `CANCEL` and a nested `BEGIN` belong to the explicit
	/// block they open, end or abort, and any other statement is in the block
	/// while one is open and commits on its own when none is.
	///
	/// The open-region shape is empty except in fragments mode, where each
	/// statement is a fragment of its own: a statement met while an explicit
	/// block is open, a `COMMIT` or `CANCEL` among them, continues the explicit
	/// region an earlier fragment opened. A session transaction is held by the
	/// caller across requests rather than left open by an earlier fragment, so
	/// it never continues.
	#[cfg_attr(
		not(test),
		expect(
			dead_code,
			reason = "the producer of the D10 section 6.7 premise, which the KV program cache consumes"
		)
	)]
	fn region_context(&self, place: &StatementPlace) -> RegionContext {
		let (commit, txn) = if self.in_session_txn {
			(StatementCommit::InTxn, TxnContext::Session)
		} else {
			match place.role {
				StatementRole::Begin
				| StatementRole::Commit
				| StatementRole::Cancel
				| StatementRole::Misplaced(MisplacedControl::NestedBegin) => {
					(StatementCommit::InTxn, TxnContext::Explicit)
				}
				StatementRole::Run
				| StatementRole::Option
				| StatementRole::Misplaced(
					MisplacedControl::BeginInSession
					| MisplacedControl::CommitOutsideBlock
					| MisplacedControl::CancelOutsideBlock,
				) if self.in_block => (StatementCommit::InTxn, TxnContext::Explicit),
				StatementRole::Run | StatementRole::Option | StatementRole::Misplaced(_) => {
					(StatementCommit::Auto, TxnContext::None)
				}
			}
		};
		let stack_shape = if self.fragmented && self.in_block && !self.in_session_txn {
			OpenRegionShape {
				open: TxnContext::Explicit,
				continues: true,
			}
		} else {
			OpenRegionShape {
				open: TxnContext::None,
				continues: false,
			}
		};
		RegionContext {
			commit,
			txn,
			stack_shape,
		}
	}

	/// Where the value `name` holds when the statement at position `at` runs
	/// came from.
	///
	/// A name the request binds with a top-level `LET` before `at` is
	/// `LetConst` where its value is a constant by `constant_value`, the test
	/// [`Executor::compile_request`] folds constants by, and `LetComputed`
	/// where the run computes it; a later `LET` of the name, before `at`,
	/// decides again, and a `LET` at or after `at` has no say. A name a block
	/// bound is `LetComputed` for every statement after the block's `COMMIT` or
	/// `CANCEL`, since a block that does not commit puts the name back to a
	/// value this walk does not name. A name recorded by
	/// [`Executor::note_inputs`] is `SessionVar` or `ClientParam`. A name with
	/// no recorded origin is `LetComputed`, so a caller that cannot place a name
	/// never treats its value as known.
	#[cfg_attr(
		not(test),
		expect(
			dead_code,
			reason = "the producer of the binding chain's origins, which the IR entry consumes"
		)
	)]
	fn binding_origin(&self, name: &str, at: usize) -> BindingOrigin {
		self.binding_origins
			.get(name)
			.and_then(|history| history.iter().rev().find(|(from, _)| *from <= at))
			.map_or(BindingOrigin::LetComputed, |(_, origin)| *origin)
	}

	/// Record the names the request's inputs bind: the session's variables
	/// first, then the request's own, which replace a session variable of the
	/// same name as they do in the context. Called before the request is
	/// compiled, so that a top-level `LET` of one of the names replaces its
	/// origin.
	#[cfg_attr(
		not(test),
		expect(
			dead_code,
			reason = "records the inputs `binding_origin` reports, which the driver names once the IR entry consumes them"
		)
	)]
	fn note_inputs<S, C>(&mut self, session: S, client: C)
	where
		S: IntoIterator,
		S::Item: Into<String>,
		C: IntoIterator,
		C::Item: Into<String>,
	{
		for name in session {
			self.binding_origins.insert(name.into(), vec![(0, BindingOrigin::SessionVar)]);
		}
		for name in client {
			self.binding_origins.insert(name.into(), vec![(0, BindingOrigin::ClientParam)]);
		}
	}

	/// Record that the origin of `name` is `origin` for the statements at
	/// position `from` and after.
	fn note_let_origin(&mut self, name: &str, from: usize, origin: BindingOrigin) {
		self.binding_origins.entry(name.to_owned()).or_default().push((from, origin));
	}

	/// The explicit block the walk has open ends at position `at`: every name
	/// its `LET`s bound is `LetComputed` for the statements after it, since a
	/// block that does not commit, and a `COMMIT` that fails, put the names
	/// back to what they held at `BEGIN`. A terminator with no open block is
	/// misplaced and changes nothing.
	fn end_block_origins(&mut self, explicit: &mut bool, block_names: &mut Vec<String>, at: usize) {
		if std::mem::take(explicit) {
			for name in std::mem::take(block_names) {
				self.note_let_origin(&name, at + 1, BindingOrigin::LetComputed);
			}
		}
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
		statement_result(self.result_shape, value)
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
		// A statement with a result has ended: what its reads recorded is final.
		if sink.reads.as_ref().is_some_and(|(index, _)| *index < self.results.len()) {
			sink.fold_reads();
		}
		while sink.finalised < self.results.len() {
			let index = sink.finalised;
			sink.finalised += 1;
			let result = &mut self.results[index];
			// A statement's trace stands only with its outcome: one that failed
			// or was rolled back retracts it with its rows.
			let mut trace = sink.statement_traces.remove(&index).unwrap_or_default();
			if sink.trace.contains(surrealdb_rpc::TraceKinds::SUBSCRIPTIONS)
				&& matches!(result.query_type, QueryType::Live)
				&& let Ok(surrealdb_types::Value::Uuid(id)) = &result.result
			{
				trace.subscriptions.push(*id);
			}
			if result.result.is_ok() && !trace.is_empty() {
				sink.send(QueryStreamItem::Trace {
					index,
					trace: Box::new(trace),
				})
				.await;
			}
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

	/// [`install_statement_state`] on the executor's context, for the next top-level statement.
	#[inline]
	fn install_statement_state(&mut self) -> Arc<StatementCounters> {
		install_statement_state(&mut self.ctx)
	}

	fn execute_option_statement(&mut self, stmt: &OptionStatement) -> Result<()> {
		// Allowed to run?
		crate::exec::session::option_allowed(&self.ctx, &self.opt)?;
		self.apply_option(&stmt.name, stmt.what);
		Ok(())
	}

	/// Set the option `name` to `what` on the session. Only `IMPORT` and `FORCE` have an effect.
	fn apply_option(&mut self, name: &str, what: bool) {
		if name.eq_ignore_ascii_case("IMPORT") {
			self.opt.set_import(what);
		} else if name.eq_ignore_ascii_case("FORCE") {
			let force = if what {
				Force::All
			} else {
				Force::None
			};
			self.opt.force = force;
		}
	}

	/// Select the namespace and database one level of a `USE` names on the session: the
	/// options and the `$session` value. The cached session information is the caller's to
	/// invalidate once the statement's levels are applied.
	fn apply_use(&mut self, ns: Option<Strand>, db: Option<Strand>) -> FlowResult<()> {
		let ctx = context_mut(&mut self.ctx)?;
		if let Some(ns) = ns {
			let mut session = ctx.value("session").unwrap_or(&Value::None).clone();
			self.opt.set_ns(Some(ns.as_str().into()));
			session.put(NS.as_ref(), Value::String(ns));
			ctx.add_value("session", session.into());
		}
		if let Some(db) = db {
			let mut session = ctx.value("session").unwrap_or(&Value::None).clone();
			self.opt.set_db(Some(db.as_str().into()));
			session.put(DB.as_ref(), Value::String(db));
			ctx.add_value("session", session.into());
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
	fn check_slow_log<T: ToSql, P: SlowLogVisit>(&mut self, start: &Instant, stm: &T, params: &P) {
		let Some(slow_log) = self.ctx.slow_log() else {
			return;
		};
		if slow_log.check_log(&self.ctx, start, stm, params) {
			// A statement the interruption cut short is accounted for by its
			// own entry.
			if !self.interruption_slow_logged && matches!(self.ctx.done(true), Ok(Some(_))) {
				self.interruption_slow_logged = true;
			}
		} else if !self.interruption_slow_logged
			&& let Ok(Some(reason)) = self.ctx.done(true)
		{
			// The statement ran under the threshold, but the batch it belongs
			// to may not have.
			self.slow_log_interrupted(reason, stm, Some(params));
		}
	}

	/// Log the batch as a slow query when its timeout or cancellation has
	/// stopped `stm` and the batch has run for at least the slow-log
	/// threshold.
	///
	/// `stm` is the statement the interruption cut short, with the `params`
	/// it ran with, or the statement it kept from starting, with none. The
	/// batch is logged at most once, and not when the statement the
	/// interruption cut short has an entry of its own.
	fn slow_log_interrupted<T: ToSql, P: SlowLogVisit>(
		&mut self,
		reason: Reason,
		stm: &T,
		params: Option<&P>,
	) {
		if self.interruption_slow_logged {
			return;
		}
		let (Some(slow_log), Some(batch_start)) = (self.ctx.slow_log(), self.batch_start) else {
			return;
		};
		self.interruption_slow_logged =
			slow_log.check_log_interrupted(&self.ctx, &batch_start, reason, stm, params);
	}

	/// [`Executor::slow_log_interrupted`] for `next`, a statement that is not
	/// run because the block it belongs to has already failed, when the reason
	/// the block failed is the batch's timeout or cancellation.
	fn slow_log_if_interrupted(&mut self, next: &TopLevelStatement) {
		if self.interruption_slow_logged || self.ctx.slow_log().is_none() {
			return;
		}
		if let Ok(Some(reason)) = self.ctx.done(true) {
			self.slow_log_interrupted(reason, next, None::<&PlanParams<'_>>);
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
		// The batch error is classified from the first errored statement's
		// structured `surrealdb_types::Error`, so operators see the dominant
		// error class for the batch on `surrealdb.query.*`. Per-statement classes
		// are reported on `surrealdb.statement.*`, which only the unexecuted-statement
		// paths populate.
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

		// Extract fields from the session Value. `Value::String` holds a `Strand`, so it is
		// moved into `SessionInfo` as it is, with no `Strand -> String -> Strand` round-trip.
		let ns = session_value.pick(NS.as_ref()).into_strand();

		let db = session_value.pick(DB.as_ref()).into_strand();

		let id = if let Value::Uuid(u) = session_value.pick(ID.as_ref()) {
			Some(*u.as_ref())
		} else {
			None
		};

		let ip = session_value.pick(IP.as_ref()).into_strand();

		let origin = session_value.pick(OR.as_ref()).into_strand();

		let ac = session_value.pick(AC.as_ref()).into_strand();

		let rd = {
			let v = session_value.pick(RD.as_ref());
			v.is_some().then_some(v)
		};

		let token = {
			let v = session_value.pick(TK.as_ref());
			v.is_some().then_some(v)
		};

		// Note: exp is not in the session object; it lives in the Session struct.
		// It is left as None here.
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
		let result = self.execute_planned(plan, Arc::clone(&txn), stream_rows).await;
		let result = settle_held_creates(&txn, result).await;
		// The drain runs whether the program returned or raised, before the statement's result is
		// recorded or sent and before the next step runs. An error it raises replaces the result.
		self.apply_session_effects(result.is_ok())?;
		result
	}

	/// Apply, in the order they were recorded, the changes the statement that just ran made to
	/// the session.
	///
	/// A program records an effect only after the part of the statement it stands for has
	/// succeeded, so draining after a failure leaves exactly what the statement had done by then
	/// (`USE NS a DB b` keeps the namespace when the database half fails). Nothing records on a
	/// statement the engine ran as written, so the drain is then empty. A refused effect fails
	/// the statement. `succeeded` says whether the program returned: the cached session
	/// information is dropped after a `USE` only then.
	fn apply_session_effects(&mut self, succeeded: bool) -> FlowResult<()> {
		use crate::exec::context::SessionEffect;
		let mut selected = false;
		for effect in self.session_sink.take() {
			match effect {
				SessionEffect::Option {
					name,
					what,
				} => self.apply_option(name.as_str(), what),
				SessionEffect::Use {
					ns,
					db,
				} => {
					self.apply_use(ns, db)?;
					selected = true;
				}
				SessionEffect::Let {
					name,
					value,
				} => self.bind_session_let(name.as_str(), value)?,
			}
		}
		// USE changes ns/db, which the cached session information holds. A statement that failed
		// part-way leaves the cache as it was, as the statement run as written does.
		if selected && succeeded {
			self.cached_session = None;
		}
		Ok(())
	}

	async fn execute_planned(
		&mut self,
		plan: StatementPlan,
		txn: Arc<Transaction>,
		stream_rows: bool,
	) -> FlowResult<Value> {
		use tokio_util::sync::CancellationToken;

		use crate::exec::context::ContextLevel;

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
					false => {
						crate::exec::vm::out_of_line(|| {
							crate::exec::vm::serve::request_statement(
								&probe,
								op,
								principal,
								self.binds_rows,
							)
						})
						.await
					}
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
						false if self.binds_rows => {
							self.plan_binding_expression(op, Arc::clone(&txn)).await
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
			.await?
			.with_session_sink(Arc::clone(&self.session_sink));
		let index = self.current_index();
		let exec_ctx = match self.stream.as_mut().and_then(|sink| sink.reads_for(index)) {
			Some(reads) => exec_ctx.with_query_trace(reads),
			None => exec_ctx,
		};

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

		// Collect all results
		let mut results = Vec::new();
		let mut streamed: u64 = 0;
		while let Some(batch_result) = stream.next().await {
			match batch_result {
				Ok(batch) => {
					if streaming {
						let sink = self.stream.as_ref().expect("streaming implies a sink");
						let row_format = sink.row_format;
						// Measured only while the sink holds rows back, against its bound.
						let measuring = sink.measuring();
						let mut batch = batch;
						let (item, bytes) = match row_format {
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
								let bytes = rows.bytes().len();
								(
									QueryStreamItem::Frame {
										index,
										rows,
									},
									bytes,
								)
							}
							None => {
								let bytes = if measuring {
									batch.values().iter().map(estimate_bytes).sum()
								} else {
									0
								};
								let values = batch
									.into_iter()
									.map(convert_value_to_public_value)
									.collect::<Result<Vec<_>>>()
									.map_err(ControlFlow::Err)?;
								streamed += values.len() as u64;
								(
									QueryStreamItem::Rows {
										index,
										values,
									},
									bytes,
								)
							}
						};
						// A row leaves only once the creates held before it are checked, so
						// no row follows a create that fails.
						txn.flush_held().await.map_err(|e| ControlFlow::Err(e.into()))?;
						let sink = self.stream.as_mut().expect("streaming implies a sink");
						// The batch's trace goes ahead of it, so a consumer has
						// the edges behind a row when the row arrives.
						sink.send_reads(index).await;
						sink.send_rows(item, bytes).await;
						// A consumer that stopped reading wants no more rows. The
						// statement fails rather than ends, so that its transaction
						// is cancelled and nothing it wrote commits for a consumer
						// that is no longer listening; the rows it held back, if
						// any, go nowhere.
						if sink.is_closed() {
							return Err(ControlFlow::Err(EngineError::QueryCancelled.into()));
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

	/// The context a statement runs in; see [`crate::exec::context::statement_context`]. The
	/// session sink is attached by the caller that runs the statement.
	async fn statement_context(
		&mut self,
		txn: &Arc<Transaction>,
		cancellation: &tokio_util::sync::CancellationToken,
		writes: bool,
		required_level: crate::exec::context::ContextLevel,
	) -> FlowResult<crate::exec::context::ExecutionContext> {
		let session = self.get_session_info();
		crate::exec::context::statement_context(
			&self.ctx,
			&self.opt,
			session,
			txn,
			cancellation,
			writes,
			required_level,
			None,
		)
		.await
	}

	/// Run a bare expression on the engine.
	///
	/// Lowering or planning can refuse a statement — a nested DML subquery or a
	/// shape no operator exists for — and that refusal is the statement's
	/// error: `PlannerUnsupported`, carrying the reason.
	async fn run_on_streaming_engine(
		&mut self,
		expr: Expr,
		txn: Arc<Transaction>,
		start: &Instant,
	) -> FlowResult<Value> {
		// Planning consumes the plan, and the slow log reads the statement's
		// parameters from it after the query has run; keep a copy of the plan
		// and of the statement only when a slow log is configured.
		let keep_for_log = self.ctx.slow_log().is_some();
		let (planned, logged, logged_expr) = match self.compiled_statement(&txn).await {
			Some(compiled) => {
				let logged_expr = compiled.logged.is_some().then_some(expr);
				(compiled.plan, compiled.logged, logged_expr)
			}
			None => {
				let logged_expr = keep_for_log.then(|| expr.clone());
				let lowered = self.lower_bare_expression(expr);
				let logged = match &lowered {
					Ok(op) if keep_for_log => Some(op.clone()),
					_ => None,
				};
				let planned = match lowered {
					Ok(op) => self.plan_bare_expression(op, Arc::clone(&txn)).await,
					Err(e) => Err(e),
				};
				(planned.map(StatementPlan::Ready), logged, logged_expr)
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
				// Statement rendering redacts credentials in every nested position;
				// the plan supplies the parameters referenced by the statement.
				// A statement outside a block whose transaction accepts work for
				// after its commit, a deferred build or a materialise wait, is
				// slow-logged once that work has settled, by `execute_plan_impl`:
				// its caller waits for that work too, so the time it takes is
				// part of the statement's. A block's waits settle only once the
				// whole block has committed, so its statements are logged here.
				if let (Some(op), Some(expr)) = (logged, logged_expr) {
					let statement = TopLevelStatement::Ast(TopLevelExpr::Expr(expr));
					if !self.in_block && txn.accepts_materialise_waits() {
						self.slow_log_after_commit = Some((statement, op));
					} else {
						self.check_slow_log(start, &statement, &PlanParams(&op));
					}
				}
				exec_result
			}
			Err(e) => Err(ControlFlow::Err(self.planning_failed(&txn, e).await)),
		}
	}

	/// The error a statement the engine did not plan fails with; see [`planning_failed`].
	async fn planning_failed(&self, txn: &Transaction, error: Error) -> anyhow::Error {
		planning_failed(&self.opt, txn, error).await
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
	/// on the runtime syntax the parser produced. The expression is consumed.
	fn lower_bare_expression(&self, expr: Expr) -> Result<LogicalOp, Error> {
		lower_statement(expr).map(|op| self.traced(op))
	}

	/// Whether the consumer asked for a trace kind a statement's reads record,
	/// so each statement's lookups are marked for it before it is planned.
	fn marks_lookups(&self) -> bool {
		self.stream
			.as_ref()
			.is_some_and(|sink| !sink.trace.intersection(crate::exec::trace::RECORDED).is_empty())
	}

	/// `op` with its lookups marked for a query trace, when the consumer asked
	/// for one ([`crate::expr::logical::trace`]).
	fn traced(&self, mut op: LogicalOp) -> LogicalOp {
		if self.marks_lookups() {
			crate::expr::logical::trace::mark(&mut op);
		}
		op
	}

	/// Mark the lookups of every statement of the request that arrived
	/// lowered; a statement lowered from syntax is marked where it is lowered.
	fn mark_statements(&self, statements: &mut [TopLevelStatement]) {
		if !self.marks_lookups() {
			return;
		}
		for statement in statements {
			match statement {
				TopLevelStatement::Plan(op) => crate::expr::logical::trace::mark(op),
				TopLevelStatement::Surreal(stmt) => {
					crate::expr::logical::trace::mark(&mut stmt.plan)
				}
				TopLevelStatement::Ast(_) => {}
			}
		}
	}

	/// Bind a lowered bare expression for `txn` and plan it as the operator
	/// that runs it.
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

	/// [`Self::plan_bare_expression`] for the value of a top-level `LET`, whose rows the
	/// statements after it read in the order they were bound.
	async fn plan_binding_expression(
		&self,
		op: LogicalOp,
		txn: Arc<Transaction>,
	) -> Result<Arc<dyn crate::exec::ExecOperator>, Error> {
		crate::exec::planner::plan_binding_value(
			op,
			&self.ctx,
			&self.function_registry,
			txn,
			Some(Arc::clone(&self.opt.auth)),
			Some(Arc::new(self.opt.clone())),
		)
		.await
	}

	/// Execute one statement using an open transaction.
	/// Credentials are prepared when execution reaches the statement, inside
	/// the transaction timeout. Rendering does not prepare credentials.
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
		// a plan (`execute_operator_plan`) overrides it. The shape selects how the
		// result is converted to a public value, so it is reset for each statement
		// here and never carried over from the one before.
		self.result_shape = OutputShape::Scalar;
		let res = match stmt {
			TopLevelStatement::Ast(stmt) => {
				self.run_surface_statement(stmt, Arc::clone(&txn), start).await
			}
			TopLevelStatement::Plan(op) => self.run_logical_plan(op, Arc::clone(&txn), start).await,
			TopLevelStatement::Surreal(stmt) => {
				self.run_flat_statement(*stmt, Arc::clone(&txn), start).await
			}
		};
		// Every create the statement held is checked before its result stands.
		let res = settle_held_creates(&txn, res).await;

		// Catch cancellation during running.
		self.bail_if_done()?;
		res
	}

	/// Run a parsed runtime statement, preparing identities and credentials
	/// only when execution reaches the corresponding payload.
	async fn run_surface_statement(
		&mut self,
		mut stmt: TopLevelExpr,
		txn: Arc<Transaction>,
		start: &Instant,
	) -> FlowResult<Value> {
		if !matches!(stmt, TopLevelExpr::Expr(_)) {
			stmt.prepare_credentials();
		}
		match stmt {
			TopLevelExpr::Use(stmt) => self.run_use(stmt, txn).await,
			TopLevelExpr::Option(_) => Self::option_reached_an_engine(),
			TopLevelExpr::Expr(Expr::Let(stmt)) => self.run_let(*stmt, txn, start).await,
			TopLevelExpr::Begin(_) | TopLevelExpr::Commit | TopLevelExpr::Cancel => {
				match self.misplaced_control.take() {
					Some(misplaced) => Self::transaction_control_error(misplaced.message()),
					None => Err(ControlFlow::Err(anyhow::Error::new(EngineError::unreachable(
						"a transaction-control statement reached an engine without a misplaced \
						 mark from request lowering",
					)))),
				}
			}
			TopLevelExpr::Kill(stmt) => self.run_kill(stmt, txn).await,
			TopLevelExpr::Live(stmt) => self.run_live(*stmt, txn).await,
			TopLevelExpr::Show(stmt) => self.run_show(stmt, txn).await,
			TopLevelExpr::Access(stmt) => self.run_access(*stmt, txn).await,
			TopLevelExpr::Expr(expr) => self.run_bare_expression(expr, txn, start).await,
		}
	}

	/// The error a transaction-control statement raises where no driver acted
	/// on it: `BEGIN` inside a transaction, `COMMIT` or `CANCEL` outside one.
	/// Request lowering marks each such statement with its message.
	fn transaction_control_error(message: &str) -> FlowResult<Value> {
		Err(ControlFlow::Err(anyhow::Error::new(ExecError::InvalidStatement(message.to_string()))))
	}

	/// Apply the `OPTION` statement `stmt` to the options every later statement
	/// of the request runs under, as a block does, answering `NONE`. A principal
	/// that may not edit options is refused, and nothing changes.
	fn run_option(&mut self, stmt: &TopLevelStatement) -> FlowResult<Value> {
		// The answer is a scalar, whatever the statement before it answered.
		self.result_shape = OutputShape::Scalar;
		let Dispatch::Option(option) = stmt.dispatch() else {
			return Err(ControlFlow::Err(anyhow::Error::new(EngineError::unreachable(
				"a statement placed as OPTION did not dispatch as OPTION",
			))));
		};
		self.execute_option_statement(&option).map_err(ControlFlow::Err)?;
		Ok(Value::None)
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
		mut expr: Expr,
		txn: Arc<Transaction>,
		start: &Instant,
	) -> FlowResult<Value> {
		let credentials_changed = expr.prepare_credentials_changed();
		if credentials_changed {
			self.refresh_reached_statement(&TopLevelStatement::Ast(TopLevelExpr::Expr(
				expr.clone(),
			)));
		}
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

	/// Execute the SurrealQL plan under the statement's request options and
	/// observer identity. A top-level binding lives in the request context.
	async fn run_flat_statement(
		&mut self,
		stmt: surrealdb_expr::parser::LogicalStatement,
		txn: Arc<Transaction>,
		start: &Instant,
	) -> FlowResult<Value> {
		if let LogicalOp::Let(binding) = stmt.plan {
			// Boxed, as every statement's run is: the binding's run is a large future.
			return Box::pin(self.run_binding(
				binding.name,
				binding.kind,
				&stmt.text,
				|| Ok(*binding.value),
				txn,
				start,
			))
			.await;
		}
		let (planned, logged) = match self.compiled_statement(&txn).await {
			Some(compiled) => (compiled.plan, compiled.logged),
			None => {
				let logged = self.ctx.slow_log().is_some().then(|| stmt.plan.clone());
				let planned = self.plan_bare_expression(stmt.plan, Arc::clone(&txn)).await;
				(planned.map(StatementPlan::Ready), logged)
			}
		};
		match planned {
			Ok(plan) => {
				context_mut(&mut self.ctx)?.set_transaction(Arc::clone(&txn));
				let stream_rows = self.streams_rows();
				let result = self.execute_operator_plan(plan, Arc::clone(&txn), stream_rows).await;
				if let Some(op) = logged {
					self.check_slow_log(start, &stmt.text, &PlanParams(&op));
				}
				result
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

		// The levels are applied after the transactional half returns, whether it succeeded or
		// failed part-way: a namespace the statement selected stays selected when its database
		// is refused.
		let mut levels = Vec::new();
		let selected = crate::exec::session::use_targets(
			&self.ctx,
			&self.opt,
			&txn,
			use_ns,
			use_db,
			|level| levels.push(level),
		)
		.await;
		for level in levels {
			let crate::exec::context::SessionEffect::Use {
				ns,
				db,
			} = level
			else {
				unreachable!("a USE records only levels");
			};
			self.apply_use(ns, db)?;
		}
		selected?;

		// Invalidate cached session info since USE changes ns/db
		self.cached_session = None;

		// Return the current namespace and database
		Ok(crate::exec::session::use_answer(self.opt.ns.as_deref(), self.opt.db.as_deref()))
	}

	/// Run a top-level `LET`, binding its result into the batch's scope.
	///
	/// The right-hand side is what runs, not the `LET` itself: the value is
	/// computed here and bound into `self.ctx`, which is the batch's scope. A
	/// side the planner refuses fails the statement with `PlannerUnsupported`.
	async fn run_let(
		&mut self,
		mut stm: crate::expr::statements::SetStatement,
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
		let credentials_changed = stm.what.prepare_credentials_changed();
		if credentials_changed {
			self.refresh_reached_statement(&TopLevelStatement::Ast(TopLevelExpr::Expr(Expr::Let(
				Box::new(stm.clone()),
			))));
		}
		let name = stm.name.to_string();
		let kind = stm.kind.clone();
		let statement = TopLevelStatement::Ast(TopLevelExpr::Expr(Expr::Let(Box::new(stm))));
		let marks = self.marks_lookups();
		self.run_binding(
			name,
			kind,
			&statement,
			|| {
				let TopLevelStatement::Ast(TopLevelExpr::Expr(Expr::Let(stm))) = &statement else {
					unreachable!("binding statement carries LET syntax");
				};
				lower(stm.what.clone())
					.map(|mut op| {
						if marks {
							crate::expr::logical::trace::mark(&mut op);
						}
						op
					})
					.map_err(|reason| {
						ControlFlow::Err(anyhow::Error::new(ExecError::PlannerUnsupported {
							reason: reason.to_string(),
						}))
					})
			},
			txn,
			start,
		)
		.await
	}

	/// Bind an evaluated right-hand side into request scope, enforcing
	/// protected names, coercion, and the bound value's depth before publication.
	///
	/// The value is no context of its own: a RETURN in it ends the statement
	/// with its value, and the name is not bound.
	async fn run_binding(
		&mut self,
		name: String,
		kind: Option<crate::expr::Kind>,
		statement: &impl ToSql,
		lower_value: impl FnOnce() -> FlowResult<LogicalOp>,
		txn: Arc<Transaction>,
		start: &Instant,
	) -> FlowResult<Value> {
		if surrealdb_cnf::PROTECTED_PARAM_NAMES.contains(&name.as_str()) {
			return Err(ControlFlow::from(anyhow::Error::new(ExecError::InvalidParam {
				name: name.clone(),
			})));
		}
		context_mut(&mut self.ctx)?.set_transaction(Arc::clone(&txn));

		let (planned, logged) = match self.compiled_statement(&txn).await {
			Some(compiled) => (compiled.plan, compiled.logged),
			None => {
				// The slow log reads the statement's parameters from the plan,
				// which planning consumes; a copy is kept only when a slow log
				// is configured.
				let op = lower_value()?;
				let logged = self.ctx.slow_log().is_some().then(|| op.clone());
				(
					self.plan_binding_expression(op, Arc::clone(&txn))
						.await
						.map(StatementPlan::Ready),
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
		self.binds_rows = true;
		let res = self.execute_operator_plan(plan, Arc::clone(&txn), false).await;
		self.binds_rows = false;

		let res = res?;
		let result = match &kind {
			Some(kind) => {
				let set_coerce = |e| {
					ControlFlow::Err(anyhow::Error::new(ExecError::SetCoerce {
						name: name.clone(),
						error: Box::new(e),
					}))
				};
				let res = res.coerce_to_kind(kind).map_err(set_coerce)?;
				if kind.has_named_type() {
					// The named types the kind holds are judged against the catalog of the
					// session's database, as a block-scoped `LET` judges them.
					let cancellation = match self.ctx.cancel_token() {
						Some(parent) => parent.child_token(),
						None => tokio_util::sync::CancellationToken::new(),
					};
					let exec_ctx = self
						.statement_context(
							&txn,
							&cancellation,
							false,
							crate::exec::context::ContextLevel::Root,
						)
						.await?;
					crate::exec::vm::schema_host::check_types_held(&exec_ctx, kind, res)
						.await
						.map_err(set_coerce)?
				} else {
					res
				}
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

		let result = Arc::new(result);
		self.trace_variable(&name, &result);
		self.bind_session_let(&name, result)?;

		// The runtime formatter redacts credentials, including nested user
		// definitions. The plan names every parameter the statement references.
		if let Some(op) = logged {
			self.check_slow_log(start, statement, &PlanParams(&op));
		}
		// Finalise transaction, returning nothing unless it couldn't commit
		Ok(Value::None)
	}

	/// Record what a top-level `LET` bound in the statement's trace, when the
	/// consumer asked for `variables`.
	///
	/// A value estimated past [`crate::exec::trace::CAP_BYTES`] is never copied:
	/// its name is reported with `NONE` and `variables` is truncated. A value that
	/// cannot be converted is left out rather than failing the statement: tracing
	/// never fails a query.
	fn trace_variable(&mut self, name: &str, value: &Value) {
		let index = self.current_index();
		let Some(sink) = self
			.stream
			.as_mut()
			.filter(|sink| sink.trace.contains(surrealdb_rpc::TraceKinds::VARIABLES))
		else {
			return;
		};
		let trace = sink.statement_traces.entry(index).or_default();
		trace.variables.retain(|(bound, _)| bound != name);
		// A value past the cap is not copied: its name is reported with `NONE`,
		// and `variables` is truncated.
		if estimate_bytes(value) > crate::exec::trace::CAP_BYTES {
			trace.variables.push((name.to_owned(), surrealdb_types::Value::None));
			trace.truncated = trace.truncated.union(surrealdb_rpc::TraceKinds::VARIABLES);
			return;
		}
		if let Ok(value) = convert_value_to_public_value(value.clone()) {
			trace.variables.push((name.to_owned(), value));
		}
	}

	/// Bind the value of a top-level `LET` into the request's scope for the statements after it.
	///
	/// The value is already coerced and its depth checked, and a protected name is refused here
	/// as it is before the value is computed. A binding the stream's carry bound
	/// refuses fails the statement and ends the stream. An open block remembers what the binding
	/// overwrites, the request context's own binding and never one a parent holds, so a block that
	/// does not commit restores exactly what was there, absence included.
	fn bind_session_let(&mut self, name: &str, value: Arc<Value>) -> FlowResult<()> {
		// SECURITY: a protected name (`$auth`, `$token`, `$session`, `$access`) or a name the
		// system binds (`$this`, `$value`, ...) is never bound by a `LET`, whichever path delivers
		// the binding.
		match admit_session_let(&self.carry, &mut self.carried, name, &value) {
			Ok(()) => {}
			Err(refusal @ SessionLetRefusal::Name(_)) => {
				return Err(ControlFlow::from(anyhow::Error::new(refusal.into_error())));
			}
			Err(refusal @ SessionLetRefusal::Carry(_)) => {
				self.carry_crossed = true;
				return Err(ControlFlow::from(anyhow::Error::new(refusal.into_error())));
			}
		}
		if self.let_undo.is_open() {
			let prev = self.ctx.own_value(name).cloned();
			self.let_undo.record(name, prev);
		}
		context_mut(&mut self.ctx)?.add_value(name.to_owned(), value);
		Ok(())
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
		stmt: crate::expr::statements::LiveStatement,
		txn: Arc<Transaction>,
	) -> FlowResult<Value> {
		let (s, arrived) = match self.arrived_live.take() {
			Some((s, arrived)) => (s, arrived),
			None => (stmt, None),
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
	) -> Option<crate::exec::vm::live::Arrived> {
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
			Some(reason) => {
				self.slow_log_interrupted(reason, &stmt, None::<&PlanParams<'_>>);
				return Err(interruption_error(reason));
			}
		}
		self.execute_plan_impl(kvs, start, stmt).await
	}

	/// Fail with the query's timeout or cancellation if it has ended.
	fn bail_if_done(&self) -> Result<()> {
		match self.ctx.done(true)? {
			None => Ok(()),
			Some(reason) => Err(interruption_error(reason)),
		}
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
		// Whichever way the statement ended, the work it left for after its
		// commit has settled by now.
		if let Some((statement, op)) = self.slow_log_after_commit.take() {
			self.check_slow_log(start, &statement, &PlanParams(&op));
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
		match stmt {
			TopLevelStatement::Ast(expr @ (TopLevelExpr::Live(_) | TopLevelExpr::Kill(_))) => {
				// Boxed: held inline, this rare run would set the size of the common one.
				Box::pin(self.execute_live_or_kill(kvs, start, expr, transaction_type)).await
			}
			stmt @ (TopLevelStatement::Ast(_)
			| TopLevelStatement::Plan(_)
			| TopLevelStatement::Surreal(_)) => {
				Box::pin(self.execute_rerunning_on_conflict(kvs, start, stmt, transaction_type))
					.await
			}
		}
	}

	/// Run `stmt` in a transaction of its own, and again in a fresh one each
	/// time it loses a retryable conflict having done nothing outside the
	/// transaction.
	///
	/// The statement runs at most `STATEMENT_CONFLICT_ATTEMPTS` times, its
	/// attempts sharing one transaction-timeout budget as a bare `LIVE` or
	/// `KILL`'s do (see [`Self::execute_live_or_kill`]). An attempt is not
	/// followed by another when it:
	/// - reached outside its transaction ([`Transaction::note_external_effect`]): a request sent or
	///   a bucket written is not undone by the lost commit, and a second attempt would repeat it;
	/// - committed and then left work to await (a blocking index build): the conflict is that
	///   work's, and the commit stands;
	/// - is a read: it has no commit to lose;
	/// - was supplied as a plan rather than as SurrealQL text, which keeps no source to run again
	///   from;
	/// - streamed rows past the staging bound (`stream_stage_bytes`): they have reached the
	///   consumer, which a second attempt's rows would be sent to as well.
	///
	/// When the attempts or the timeout run out, the last conflict is returned
	/// unchanged. Inside a transaction block or a caller-owned transaction a
	/// statement never reaches this point, and the conflict surfaces to the
	/// caller, who owns the transaction's other statements.
	async fn execute_rerunning_on_conflict(
		&mut self,
		kvs: &Datastore,
		start: &Instant,
		mut stmt: TopLevelStatement,
		transaction_type: TransactionType,
	) -> Result<Value> {
		let mut budget = kvs.transaction_timeout();
		let mut attempt = 1;
		let index = self.current_index();
		loop {
			// What a further attempt runs from, taken before this one consumes the
			// statement: the source text of a compiled statement, which is
			// shared and cheap to keep, or a copy of a dedicated one.
			let source = match (&stmt, transaction_type) {
				(_, TransactionType::Read) => None,
				(TopLevelStatement::Surreal(stmt), _) => Some(Rerun::Compile(stmt.source())),
				(TopLevelStatement::Ast(expr), _) => Some(Rerun::Copy(expr.clone())),
				(TopLevelStatement::Plan(_), _) => None,
			};
			let staging = source.is_some();
			if staging && let Some(sink) = self.stream.as_mut() {
				sink.begin_staging();
			}
			let mut rerunnable = false;
			let e = match self
				.execute_in_own_transaction(
					kvs,
					start,
					stmt,
					transaction_type,
					&mut budget,
					&mut rerunnable,
				)
				.await
			{
				Ok(value) => {
					if let Some(sink) = self.stream.as_mut() {
						sink.flush_staged().await;
					}
					return Ok(value);
				}
				Err(e) => e,
			};
			// A failed attempt's held rows never reach the consumer: its
			// statement finishes with an error, which retracts any rows it
			// produced, and a further attempt produces its own. An attempt
			// whose rows passed the staging bound has sent them, and the
			// consumer, which may already have acted on them, is not sent a
			// second attempt's.
			let released =
				staging && self.stream.as_mut().is_some_and(|sink| sink.discard_staged(index));
			// A statement refused because its table's shared doc-ID space is
			// part-way through a reclaim finishes the reclaim first, so the run
			// below, or a client's retry where there is none, can claim the space.
			if let Some(pending) = DocIdsReclaimPending::of(&e).cloned()
				&& !self.finish_doc_ids_reclaim(kvs, &pending, &mut budget).await?
			{
				return Err(e);
			}
			// A commit whose outcome is unknown is not a retryable conflict,
			// so a statement that may have applied is never run again.
			if released || !rerunnable || !crate::kvs::is_retryable_transaction_conflict(&e) {
				return Err(e);
			}
			if attempt == STATEMENT_CONFLICT_ATTEMPTS {
				return Err(e);
			}
			let Some(next) = source.and_then(Rerun::statement) else {
				return Err(e);
			};
			debug!(target: TARGET, attempt, "a statement lost a retryable transaction conflict");
			if !self.before_conflict_retry(attempt, &mut budget).await? {
				return Err(e);
			}
			stmt = next;
			attempt += 1;
		}
	}

	/// Run a bare `LIVE` or `KILL` in a transaction of its own, and again in a
	/// fresh one each time it loses a retryable conflict on its table's
	/// live-query stamp.
	///
	/// The statement runs at most `STATEMENT_CONFLICT_ATTEMPTS` times, and its
	/// attempts share one transaction-timeout budget, spent by their execution
	/// and by the waits between them: each attempt's execution must finish
	/// within what is left, and no attempt starts once the wait before it has
	/// spent it. As for every statement, the timeout bounds execution only:
	/// opening a transaction, compiling a `LIVE` arrival and the commit spend
	/// none of it, and the commit must not, since a commit abandoned part-way
	/// has an unknown outcome. A re-run `LIVE` registers
	/// under the subscription id of its first attempt. When the attempts or the
	/// timeout run out, the last conflict is returned unchanged. Inside a
	/// transaction block or a caller-owned transaction a statement never reaches
	/// this point, and the conflict surfaces to the caller.
	async fn execute_live_or_kill(
		&mut self,
		kvs: &Datastore,
		start: &Instant,
		mut expr: TopLevelExpr,
		transaction_type: TransactionType,
	) -> Result<Value> {
		// A `LIVE`'s subscription id is minted when its credentials are
		// prepared, so it is prepared once here and every attempt registers a
		// clone of the one prepared statement.
		if matches!(expr, TopLevelExpr::Live(_)) {
			expr.prepare_credentials();
		}
		let live: Option<crate::expr::statements::LiveStatement> =
			if let TopLevelExpr::Live(live) = &expr {
				Some((**live).clone())
			} else {
				None
			};
		let mut budget = kvs.transaction_timeout();
		let mut attempt = 1;
		loop {
			// Each attempt compiles its unit in a read transaction of its own,
			// since an arrival is pinned to the catalog version it compiled
			// against.
			if let Some(live) = &live {
				self.arrive_live(kvs, live.clone()).await;
			}
			let stmt = TopLevelStatement::Ast(expr.clone());
			// A `LIVE` or `KILL` reaches nothing outside its transaction, so
			// every attempt that loses a conflict may be followed by another.
			let e = match self
				.execute_in_own_transaction(
					kvs,
					start,
					stmt,
					transaction_type,
					&mut budget,
					&mut false,
				)
				.await
			{
				Ok(value) => return Ok(value),
				Err(e) => e,
			};
			// A commit whose outcome is unknown is not a retryable conflict,
			// so a statement that may have applied is never run again.
			if attempt == STATEMENT_CONFLICT_ATTEMPTS
				|| !crate::kvs::is_retryable_transaction_conflict(&e)
			{
				return Err(e);
			}
			debug!(target: TARGET, attempt, "LIVE or KILL lost a retryable transaction conflict");
			if !self.before_conflict_retry(attempt, &mut budget).await? {
				return Err(e);
			}
			attempt += 1;
		}
	}

	/// Return the executor to the state a statement starts from, then wait out
	/// the backoff, after attempt number `attempt` of a statement run in a
	/// transaction of its own lost a conflict. The wait, as long as it actually lasted, is taken
	/// from `budget`; `false` when it would spend or has spent what is left, so the
	/// statement is not run again.
	///
	/// The broker the previous attempt installed is cleared: a broker still on
	/// the context is reused by the next attempt, and its receiver is gone, so
	/// the `Killed` notification of a re-run `KILL` would go into a closed
	/// channel and its registration would never be dropped. The previous
	/// attempt's arrival is dropped with it. A query cancelled or timed out
	/// before or during the wait stops here instead of running again.
	async fn before_conflict_retry(
		&mut self,
		attempt: u32,
		budget: &mut Option<Duration>,
	) -> Result<bool> {
		if self.broker_owned_by_executor {
			self.clear_broker();
		}
		self.broker_owned_by_executor = false;
		self.arrived_live = None;
		// The lost attempt's transaction has ended, and with it what it held.
		if let Some(memory) = self.ctx.memory() {
			memory.transaction_ended();
		}
		if let Some(counters) = self.ctx.statement_counters() {
			counters.reset();
		}
		self.bail_if_done()?;
		// `attempt` is below `STATEMENT_CONFLICT_ATTEMPTS`, so the doubling
		// cannot overflow before the cap applies.
		let backoff = (STATEMENT_CONFLICT_MIN_BACKOFF * 2u32.pow(attempt - 1))
			.min(STATEMENT_CONFLICT_MAX_BACKOFF)
			.mul_f64(rand::rng().random_range(0.5..=1.5));
		if budget.is_some_and(|left| backoff >= left) {
			return Ok(false);
		}
		let waiting = Instant::now();
		common::time::sleep(backoff).await;
		// The query may have been cancelled or timed out during the wait.
		self.bail_if_done()?;
		// A wait that overran its backoff spends what it actually took.
		if let Some(left) = budget {
			*left = left.saturating_sub(waiting.elapsed());
			if left.is_zero() {
				return Ok(false);
			}
		}
		Ok(true)
	}

	/// Finish the reclaim of the shared doc-ID space `pending` names, which a
	/// `DEFINE INDEX` or `REBUILD INDEX` was refused for, so that a claim made
	/// afterwards finds nothing torn queued.
	///
	/// The reclaim is advanced in bounded steps, and between them the query's
	/// cancellation and timeout are checked and the time spent is taken from
	/// `budget`; `false` when `budget` runs out first. Where another node holds
	/// the reclaim lease this waits for that node's reaper to finish instead.
	async fn finish_doc_ids_reclaim(
		&self,
		kvs: &Datastore,
		pending: &DocIdsReclaimPending,
		budget: &mut Option<Duration>,
	) -> Result<bool> {
		let began = Instant::now();
		let out_of_time =
			|budget: &Option<Duration>| budget.is_some_and(|left| began.elapsed() >= left);
		let finished = loop {
			self.bail_if_done()?;
			if out_of_time(budget) {
				break false;
			}
			// A step can wait inside its lease check's retries, so it is held to
			// the query's cancellation and deadline from outside: polled while it
			// runs, and stopped through its canceller once either is reached.
			let canceller = tokio_util::sync::CancellationToken::new();
			let mut step = pin!(kvs.finish_tb_doc_ids_reclaim_step(
				pending.ns,
				pending.db,
				&pending.tb,
				DOC_IDS_RECLAIM_STEP_KEYS,
				&canceller,
			));
			let step = loop {
				tokio::select! {
					step = &mut step => break step,
					_ = common::time::sleep(DOC_IDS_RECLAIM_POLL) => {
						if self.ctx.done(true)?.is_some() || out_of_time(budget) {
							canceller.cancel();
						}
					}
				}
			};
			let step = match step {
				Ok(step) => step,
				// Stopped from here: the checks at the top of the loop report why.
				Err(_) if canceller.is_cancelled() => continue,
				Err(e) => return Err(e),
			};
			match step {
				DocIdsReclaimStep::Claimable => break true,
				DocIdsReclaimStep::Progressed => {
					yield_now!();
				}
				DocIdsReclaimStep::Waiting => common::time::sleep(DOC_IDS_RECLAIM_POLL).await,
			}
		};
		// Whether what is left still covers a rerun is the retry's own question
		// (`before_conflict_retry`), not whether the reclaim finished.
		if let Some(left) = budget {
			*left = left.saturating_sub(began.elapsed());
		}
		Ok(finished)
	}

	/// Run `stmt` in a transaction of `transaction_type` opened for it alone,
	/// committing a write transaction it completes and cancelling it otherwise.
	///
	/// `rerunnable` is set when the statement failed in a write transaction
	/// that reached nothing outside itself and left nothing to await after its
	/// commit, so that running the statement again from the start repeats
	/// nothing observable. It is left `false` otherwise.
	///
	/// Where `budget` holds a duration, the statement's execution must finish
	/// within it, and the time execution took is taken from it; a statement
	/// that runs out reports the datastore's configured transaction timeout.
	/// Opening the transaction and committing it spend none of it: a commit
	/// abandoned part-way has an unknown outcome.
	async fn execute_in_own_transaction(
		&mut self,
		kvs: &Datastore,
		start: &Instant,
		stmt: TopLevelStatement,
		transaction_type: TransactionType,
		budget: &mut Option<Duration>,
		rerunnable: &mut bool,
	) -> Result<Value> {
		*rerunnable = false;
		#[cfg(test)]
		let live_or_kill =
			matches!(&stmt, TopLevelStatement::Ast(TopLevelExpr::Live(_) | TopLevelExpr::Kill(_)));
		let txn = kvs
			.transaction(transaction_type)
			.await?
			.with_tenant_identity(self.ctx.tenant_identity().cloned())
			.with_write_keys_limit(kvs.transaction_max_write_keys());
		// A standalone blocking `DEFINE INDEX` or `REBUILD INDEX` commits its
		// catalog change first and its build is awaited below, once the commit
		// has applied: the build commits far more writes than a transaction can
		// stay open across on an optimistic storage engine. Every blocking index
		// statement a write transaction runs, wherever it sits in the statement,
		// queues the wait for its index to be materialised, which only a
		// committed index can be. A transaction that queues nothing commits as
		// any other does.
		let defers_builds = stmt.awaits_its_build_after_commit();
		let after_commit =
			matches!(transaction_type, TransactionType::Write).then(AfterCommitWaits::default);
		let txn = Arc::new(match &after_commit {
			Some(waits) if defers_builds => txn.with_after_commit_waits(waits),
			Some(waits) => txn.with_materialise_waits(waits),
			None => txn,
		});
		let receiver = self.prepare_broker(
			kvs,
			matches!(transaction_type, TransactionType::Write),
			kvs.live_query_broker(),
		);

		// The run is boxed so that this future, which also awaits the commit and the
		// cancel, does not hold it inline.
		let exec_result = match *budget {
			Some(left) => {
				let executing = Instant::now();
				let res = common::time::timeout(
					left,
					Box::pin(self.execute_plan_in_transaction(Arc::clone(&txn), start, stmt)),
				)
				.await;
				*budget = Some(left.saturating_sub(executing.elapsed()));
				match res {
					Ok(res) => res,
					Err(_) => {
						let _ = txn.cancel().await;
						let timeout = kvs.transaction_timeout().unwrap_or(left);
						bail!(DatastoreError::TransactionTimedout(timeout.into()))
					}
				}
			}
			None => Box::pin(self.execute_plan_in_transaction(Arc::clone(&txn), start, stmt)).await,
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
						crate::kvs::testing::RetryableConflictSite::LiveKillCommit,
						self.ctx.node_id(),
					) {
					let _ = txn.cancel().await;
					return Err(e);
				}
				// Stands in for a lost commit of a statement whose build has
				// started and is queued for after the commit.
				#[cfg(test)]
				if defers_builds
					&& let Err(e) = crate::kvs::testing::maybe_inject_retryable_conflict(
						crate::kvs::testing::RetryableConflictSite::AwaitedBuildCommit,
						self.ctx.node_id(),
					) {
					let _ = txn.cancel().await;
					return Err(e);
				}

				let Some(waits) = after_commit else {
					Self::commit_and_flush(&txn, receiver).await?;
					return Ok(value);
				};
				// Taken before the commit: a commit that definitely failed
				// discards them unrun, as it would discard the queue. A statement that queued
				// nothing — an `IF NOT EXISTS` that found its index — commits as any
				// other does.
				let waits = waits.take().await;
				if waits.is_empty() {
					// A commit that fails discards every write, and nothing is
					// left to await after it.
					*rerunnable = !txn.performed_external_effect();
					// Stands in for a lost commit of any other statement.
					#[cfg(test)]
					if !live_or_kill
						&& !defers_builds && let Err(e) =
						crate::kvs::testing::maybe_inject_retryable_conflict(
							crate::kvs::testing::RetryableConflictSite::StatementCommit,
							self.ctx.node_id(),
						) {
						let _ = txn.cancel().await;
						return Err(e);
					}
					Self::commit_and_flush(&txn, receiver).await?;
					return Ok(value);
				}
				// Past this point a commit followed by a wait that can only
				// compensate for it is answered, for a query that has already
				// timed out or been cancelled, by cancelling the transaction
				// instead: its build is then aborted by the rollback. A wait that
				// owes the commit nothing leaves the commit as it would be with
				// nothing queued.
				if waits.iter().any(|wait| wait.compensates())
					&& let Err(e) = self.bail_if_done()
				{
					// The timeout or cancellation is what the statement reports.
					// The transaction never commits on this path, so a cancel
					// that fails publishes none of its writes, and its own error
					// would only mask that one.
					let _ = txn.cancel().await;
					return Err(e);
				}
				// The statement's result is not settled until the work it left for
				// after the commit is: a blocking build that fails fails the
				// statement. What that work owes once the commit has applied — the
				// removal of a failed build's definition — must not depend on this
				// statement's caller still waiting, so the commit and everything
				// after it run on a task of their own, and each wait enforces its
				// own deadline (see `after_commit_bound`). A caller that stops
				// waiting, at any point, stops hearing the outcome and nothing else.
				let bound = self.after_commit_bound(kvs, *budget);
				let waiting = Instant::now();
				let (outcome, settled) = oneshot::channel();
				// A commit whose outcome is unknown may have applied, and with it
				// the builds the waits stand for, which go on as the definitions
				// they build committed. Its waits that can owe that commit
				// something still run: each settles against what is durably
				// recorded, and takes back nothing a commit that did not apply
				// left. The statement reports the commit's error at once, and
				// what the waits come to is only logged.
				common::rt::spawn_detached(async move {
					let deadline = |bound: Option<(Duration, Duration)>| {
						bound.map(|(after, timeout)| AfterCommitDeadline {
							timeout,
							at: Instant::now() + after,
						})
					};
					if let Err(err) = Self::commit_and_flush(&txn, receiver).await {
						let indeterminate = crate::kvs::is_indeterminate_commit(&err);
						// The caller may no longer be waiting for the outcome.
						let _ = outcome.send(Err(err));
						if indeterminate {
							let deadline = deadline(bound);
							for wait in waits.into_iter().filter(|wait| wait.compensates()) {
								if let Err(err) = wait.wait(deadline).await {
									warn!(
										error = %err,
										"a wait after a commit of unknown outcome ended with an error"
									);
								}
							}
						}
						return;
					}
					let result: Result<()> = async {
						let deadline = deadline(bound);
						for wait in waits {
							wait.wait(deadline).await?;
						}
						Ok(())
					}
					.await;
					// The caller may no longer be waiting for the outcome.
					let _ = outcome.send(result);
				});
				let result = settled.await;
				if let Some(left) = budget.as_mut() {
					*left = left.saturating_sub(waiting.elapsed());
				}
				result.map_err(|_| {
					EngineError::unreachable("the commit of a statement stopped without an outcome")
				})??;
				Ok(value)
			}
			Err(ControlFlow::Continue) | Err(ControlFlow::Break) => {
				let _ = txn.cancel().await;
				bail!(ExecError::InvalidControlFlow)
			}
			Err(ControlFlow::Err(e)) => {
				*rerunnable = matches!(transaction_type, TransactionType::Write)
					&& !txn.performed_external_effect();
				let _ = txn.cancel().await;
				Err(e)
			}
		}
	}

	/// Replaces the slots recorded from `start` on with the rewrite's
	/// `recorded` answer, when it has one.
	fn rewrite_recorded(&mut self, start: usize, rewrite: &SlotRewrite) {
		let Some(slot) = &rewrite.recorded else {
			return;
		};
		for res in &mut self.results[start..] {
			res.query_type = QueryType::Other;
			res.result = slot_outcome(slot);
		}
	}

	/// Pushes the slot of a statement met while a block runs on to its
	/// terminator: the terminator's slot when `terminates`, else the slot of a
	/// statement after the failure or interrupt point. Answers whether a slot
	/// was pushed.
	fn push_fast_forward_slot(&mut self, rewrite: &SlotRewrite, terminates: bool) -> bool {
		let slot = if terminates {
			&rewrite.terminator
		} else {
			&rewrite.following
		};
		let Some(slot) = slot else {
			return false;
		};
		self.results.push(slot_result(slot, Duration::ZERO));
		true
	}

	/// How long the work a statement left for after its commit may be waited
	/// for: no longer than what remains of `budget` and of the query's
	/// deadline, whichever is sooner, paired with the timeout to report should
	/// it run out — the configured transaction timeout for the request's
	/// budget, the query's own timeout for its deadline. Settled here rather
	/// than read back from the clock once the timer fires, which cannot tell
	/// two bounds apart that end close together. `None` when neither applies.
	///
	/// The commit spends none of it: the bound starts once the commit has
	/// applied. The query's cancellation ends the wait too, which the work
	/// notices through the statement's context.
	fn after_commit_bound(
		&self,
		kvs: &Datastore,
		budget: Option<Duration>,
	) -> Option<(Duration, Duration)> {
		let query = self.ctx.timeout().zip(self.ctx.deadline_timeout());
		let transaction = |left: Duration| (left, kvs.transaction_timeout().unwrap_or(left));
		match (budget, query) {
			(Some(left), Some((until, _))) if left <= until => Some(transaction(left)),
			(_, Some(query)) => Some(query),
			(Some(left), None) => Some(transaction(left)),
			(None, None) => None,
		}
	}

	/// Run, once a block has committed, the work its statements queued for
	/// after the commit, and resolve each statement's slot and held event.
	///
	/// Every queued wait runs, in the order it was queued, whatever an earlier
	/// one came to: a wait that stops on an error leaves the later indexes to
	/// materialise or report their own. The waits draw on one deadline, what is
	/// left of the transaction timeout since the block began alongside the
	/// query's own (see [`Self::after_commit_bound`]); a wait that reaches it
	/// returns success with its work left queued, and every later one returns
	/// at once on the same terms. A cancellation is seen by every remaining
	/// wait. A wait's error is its own statement's result, never `COMMIT`'s,
	/// which keeps its success, and a slot whose waits succeeded keeps its
	/// committed value.
	///
	/// The waits run on a task of their own, as a standalone statement's do,
	/// so a caller that stops listening does not abandon a pass part-way
	/// through its transaction.
	async fn settle_block_waits(
		&mut self,
		kvs: &Datastore,
		waits: &AfterCommitWaits,
		held: Vec<BlockWait>,
		block_started: Instant,
	) {
		let queued = waits.take().await;
		if held.is_empty() {
			return;
		}
		let budget = kvs
			.transaction_timeout()
			.map(|timeout| timeout.saturating_sub(block_started.elapsed()));
		let deadline =
			self.after_commit_bound(kvs, budget).map(|(after, timeout)| AfterCommitDeadline {
				timeout,
				at: Instant::now() + after,
			});
		let (outcome, settled) = oneshot::channel();
		common::rt::spawn_detached(async move {
			let mut outcomes = Vec::with_capacity(queued.len());
			for wait in queued {
				outcomes.push(wait.wait(deadline).await);
			}
			// The caller may no longer be waiting for the outcome.
			let _ = outcome.send(outcomes);
		});
		let mut outcomes = settled.await.unwrap_or_default().into_iter();
		for wait in held {
			let mut result = Ok(());
			for _ in 0..wait.waits {
				let outcome = outcomes.next().unwrap_or_else(|| {
					Err(EngineError::unreachable(
						"a wait after a block's commit ended without an outcome",
					)
					.into())
				});
				if let (Ok(()), Err(e)) = (&result, outcome) {
					result = Err(e);
				}
			}
			let slot = &mut self.results[wait.slot];
			slot.time = wait.event.before.elapsed();
			match result {
				Ok(()) => self.emit_held_statement_event(kvs, wait.event, Outcome::Success, None),
				Err(e) => {
					let error = types_error_from_anyhow(e);
					let class = crate::observe::error_class::classify_types_error(&error);
					slot.result = Err(error);
					self.emit_held_statement_event(kvs, wait.event, Outcome::Error, Some(class));
				}
			}
		}
	}

	/// Emit the `StatementEvent` a block statement held back, with the
	/// outcome its slot ends with.
	fn emit_held_statement_event(
		&self,
		kvs: &Datastore,
		event: HeldStatementEvent,
		outcome: Outcome,
		error_class: Option<&'static str>,
	) {
		self.emit_statement_event_cached(
			kvs,
			event.kind,
			event.read_only,
			event.sql,
			&event.before,
			outcome,
			event.rows,
			error_class,
		);
	}

	/// Commit the transaction of a block ended by `COMMIT`.
	async fn commit_block(&self, txn: &Transaction) -> Result<()> {
		// Stands in for a lost commit: the writes are discarded and the
		// conflict is reported as a failed commit is.
		#[cfg(test)]
		if let Err(e) = crate::kvs::testing::maybe_inject_retryable_conflict(
			crate::kvs::testing::RetryableConflictSite::LiveKillCommit,
			self.ctx.node_id(),
		) {
			let _ = txn.cancel().await;
			return Err(e);
		}
		// A read-only block has nothing to commit; `COMMIT` releases its
		// snapshot and cannot fail.
		if !txn.writeable() {
			return txn.cancel().await;
		}
		txn.commit().await
	}

	/// Execute the begin statement and all statements after which are within a
	/// transaction block.
	async fn execute_begin_statement<S>(
		&mut self,
		kvs: &Datastore,
		stream: Pin<&mut S>,
		grouper: &mut RequestGrouper,
		emission: Emission,
		options: &BeginOptions,
	) -> Result<()>
	where
		S: Stream<Item = Result<TopLevelStatement>>,
	{
		debug_assert!(!self.holding);
		self.broker_owned_by_executor = false;
		self.holding = matches!(emission, Emission::Hold) && self.stream.is_some();
		let result = self.execute_begin_statement_impl(kvs, stream, grouper, options).await;
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

	/// Opens the transaction a `BEGIN` block runs in: a write transaction for
	/// a plain `BEGIN`, a read transaction for `READ ONLY`, over a snapshot no
	/// older than `MAX STALENESS` when one is given.
	async fn open_begin_transaction(
		&mut self,
		kvs: &Datastore,
		options: &BeginOptions,
	) -> Result<Transaction> {
		let isolation = options.isolation.map(|level| match level {
			IsolationLevel::Snapshot => Isolation::Snapshot,
			IsolationLevel::Serializable => Isolation::Serializable,
		});
		let read_only = match &options.read_only {
			None => None,
			Some(read_only) => {
				let max_staleness = match &read_only.max_staleness {
					None => Duration::ZERO,
					Some(Staleness::Duration(bound)) => *bound,
					Some(Staleness::Param(name)) => match self.ctx.value(name) {
						Some(Value::Duration(bound)) => bound.0,
						Some(_) => {
							return Err(anyhow::Error::new(BeginOptionError(format!(
								"The parameter ${name} used for MAX STALENESS is not a duration"
							))));
						}
						None => {
							return Err(anyhow::Error::new(BeginOptionError(format!(
								"The parameter ${name} used for MAX STALENESS is not defined"
							))));
						}
					},
				};
				Some(ReadOnlyOptions {
					max_staleness,
					strict: read_only.strict,
				})
			}
		};
		let (txn, info) = kvs.begin_transaction(isolation, read_only).await?;
		let Some(info) = info else {
			return Ok(txn);
		};
		debug!(
			target: "surrealdb::core::dbs",
			age = ?info.age,
			stale = info.stale,
			"opened a read-only transaction"
		);
		if let Some(slot) = self.begin_result_slot.take()
			&& let Some(result) = self.results.get_mut(slot)
		{
			result.result = Ok(crate::kvs::snapshot_description(&info));
		}
		Ok(txn)
	}

	async fn execute_begin_statement_impl<S>(
		&mut self,
		kvs: &Datastore,
		mut stream: Pin<&mut S>,
		grouper: &mut RequestGrouper,
		options: &BeginOptions,
	) -> Result<()>
	where
		S: Stream<Item = Result<TopLevelStatement>>,
	{
		let opened = self.open_begin_transaction(kvs, options).await.map(|tx| {
			tx.with_tenant_identity(self.ctx.tenant_identity().cloned())
				.with_write_keys_limit(kvs.transaction_max_write_keys())
		});
		let txn = match opened {
			Ok(txn) => txn,
			Err(failure) => {
				let reason = begin_failure_reason(&failure);
				// couldn't create a transaction.
				// Fast forward until we hit CANCEL or COMMIT, which take their
				// terminator slot like every other statement of the block.
				while let Some(stmt) = stream.next().await {
					yield_now!();
					let stmt = stmt?;
					let place = place_statement(grouper, &stmt);
					let end = match place.role {
						StatementRole::Commit => FailedEnd::Commit,
						StatementRole::Cancel => FailedEnd::Cancel,
						StatementRole::Run
						| StatementRole::Option
						| StatementRole::Begin
						| StatementRole::Misplaced(_) => FailedEnd::RequestEnd,
					};
					let terminates = !matches!(end, FailedEnd::RequestEnd);
					let mut rewrite = finalise_slots(&BlockEnd::BeginFailed {
						end,
					});
					if let Some(reason) = &reason {
						rewrite.name_begin_failure(reason);
					}
					self.push_fast_forward_slot(&rewrite, terminates);
					self.emit_statement_event_unexecuted(
						kvs,
						stmt.statement_type(),
						crate::observe::error_class::TXN_CREATE_FAILED,
					);
					if terminates {
						return Ok(());
					}
				}

				// Ran out of statements but still didn't hit a COMMIT or CANCEL
				// Just break as we can't do anything else since the query is already
				// effectively canceled.
				return Ok(());
			}
		};
		// The block's blocking index statements build inside its transaction, so
		// a failed build rolls the whole block back, and each queues the wait
		// for its index to be materialised, which only a committed index can be.
		let waits = AfterCommitWaits::default();
		let txn = Arc::new(txn.with_materialise_waits(&waits));
		self.in_block = true;
		self.let_undo.begin();
		let block_started = Instant::now();
		let ran = self.execute_begin_statement_timed(kvs, txn, &waits, stream, grouper).await;
		self.in_block = false;
		// Outside the block's timer, so that work outlasting it can never turn
		// a committed block into one reported as timed out.
		let held = std::mem::take(&mut self.block_waits);
		if matches!(ran, Ok(BlockEnd::Committed)) {
			self.settle_block_waits(kvs, &waits, held, block_started).await;
		} else {
			// The block ended before its commit, which discarded what was
			// queued: each held event reports the statement as it ran.
			for wait in held {
				self.emit_held_statement_event(kvs, wait.event, Outcome::Success, None);
			}
		}
		// The block's single exit: its bindings stand only where it committed.
		// Every other end, and every error, puts them back.
		let undone = match &ran {
			Ok(BlockEnd::Committed) => {
				self.let_undo.commit();
				Ok(())
			}
			_ => match context_mut(&mut self.ctx) {
				Ok(ctx) => {
					self.let_undo.rollback(ctx);
					Ok(())
				}
				Err(e) => {
					self.let_undo.commit();
					Err(e)
				}
			},
		};
		let end = ran.map(|_end| ());
		end.and(undone)
	}

	/// [`Self::execute_begin_statement_impl`] once the block's transaction is
	/// open, bounded by the datastore's transaction timeout.
	async fn execute_begin_statement_timed<S>(
		&mut self,
		kvs: &Datastore,
		txn: Arc<Transaction>,
		waits: &AfterCommitWaits,
		mut stream: Pin<&mut S>,
		grouper: &mut RequestGrouper,
	) -> Result<BlockEnd>
	where
		S: Stream<Item = Result<TopLevelStatement>>,
	{
		let mut in_flight = None;
		match kvs.transaction_timeout() {
			Some(timeout) => {
				let start_results = self.results.len();
				match common::time::timeout(
					timeout,
					self.execute_begin_statement_inner(
						kvs,
						Arc::clone(&txn),
						waits,
						stream.as_mut(),
						grouper,
						&mut in_flight,
					),
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
						let reason = Reason::Timedout(timeout.into());
						let mut first = in_flight.is_some();
						loop {
							let (kind, role) = match in_flight.take() {
								Some(observed) => observed,
								None => {
									let Some(stmt) = stream.next().await else {
										break;
									};
									yield_now!();
									let stmt = stmt?;
									let place = place_statement(grouper, &stmt);
									(stmt.statement_type(), place.role)
								}
							};
							let end = match role {
								StatementRole::Commit
								| StatementRole::Misplaced(MisplacedControl::CommitOutsideBlock) => InterruptEnd::Commit,
								StatementRole::Cancel
								| StatementRole::Misplaced(MisplacedControl::CancelOutsideBlock) => InterruptEnd::Cancel,
								StatementRole::Run
								| StatementRole::Option
								| StatementRole::Begin
								| StatementRole::Misplaced(
									MisplacedControl::NestedBegin
									| MisplacedControl::BeginInSession,
								) => InterruptEnd::RequestEnd,
							};
							let interrupted = BlockEnd::Interrupted {
								reason,
								end,
							};
							let rewrite = finalise_slots(&interrupted);
							let terminates = !matches!(end, InterruptEnd::RequestEnd);
							let slot = if terminates {
								&rewrite.terminator
							} else if first {
								&rewrite.interrupted_statement
							} else {
								&rewrite.following
							};
							if let Some(slot) = slot {
								self.results.push(slot_result(slot, Duration::ZERO));
								self.emit_statement_event_unexecuted(
									kvs,
									kind,
									crate::observe::error_class::TXN_TIMEOUT,
								);
							}
							if terminates {
								return Ok(interrupted);
							}
							first = false;
						}
						Ok(BlockEnd::Interrupted {
							reason,
							end: InterruptEnd::RequestEnd,
						})
					}
				}
			}
			None => {
				self.execute_begin_statement_inner(kvs, txn, waits, stream, grouper, &mut in_flight)
					.await
			}
		}
	}

	async fn execute_begin_statement_inner<S>(
		&mut self,
		kvs: &Datastore,
		txn: Arc<Transaction>,
		waits: &AfterCommitWaits,
		mut stream: Pin<&mut S>,
		grouper: &mut RequestGrouper,
		in_flight: &mut Option<(StatementType, StatementRole)>,
	) -> Result<BlockEnd>
	where
		S: Stream<Item = Result<TopLevelStatement>>,
	{
		// Create a sender for this transaction only if the context allows for
		// notifications.
		let receiver = self.prepare_broker(kvs, true, kvs.live_query_broker());
		let start_results = self.results.len();

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
			let stmt = match stmt {
				Ok(x) => x,
				Err(e) => {
					// make sure the transaction is properly canceled.
					let _ = txn.cancel().await;
					return Err(e);
				}
			};
			// The statement is recorded as in flight before the next await: the
			// transaction timeout can drop this future at any await, and its
			// recovery answers `in_flight` first and then reads the stream on,
			// so a statement pulled but not recorded would get no result slot
			// and a COMMIT or CANCEL pulled that way would not end the block.
			let place = place_statement(grouper, &stmt);
			*in_flight = Some((stmt.statement_type(), place.role));
			yield_now!();

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
					let rewrite = finalise_slots(&BlockEnd::MemoryThreshold);
					self.rewrite_recorded(start_results, &rewrite);
					return Err(e);
				}
			};
			if let Some(done) = done {
				// A cancellation happened. Cancel the transaction, fast-forward the remaining
				// results and then return.
				self.slow_log_interrupted(done, &stmt, None::<&PlanParams<'_>>);
				let _ = txn.cancel().await;

				let cancelled_count = self.results.len().saturating_sub(start_results);
				let rewrite = finalise_slots(&BlockEnd::Interrupted {
					reason: done,
					end: InterruptEnd::RequestEnd,
				});
				self.rewrite_recorded(start_results, &rewrite);
				let cancel_class = match done {
					Reason::Timedout(_) => crate::observe::error_class::CTX_TIMEOUT,
					Reason::Canceled => crate::observe::error_class::CTX_CANCELLED,
				};
				for _ in 0..cancelled_count {
					self.emit_statement_event_unexecuted(kvs, StatementType::Other, cancel_class);
				}

				// The statement pulled when `done` was observed is the first one answered,
				// then the rest of the block's statements up to its terminator. Each takes
				// exactly one slot, so the batch has one `QueryResult` per statement.
				let mut observed = Some((stmt.statement_type(), place.role));
				let mut first = true;
				loop {
					let (kind, role) = match observed.take() {
						Some(first) => {
							*in_flight = None;
							first
						}
						None => {
							let Some(stmt) = stream.next().await else {
								break;
							};
							yield_now!();
							let stmt = stmt?;
							let place = place_statement(grouper, &stmt);
							(stmt.statement_type(), place.role)
						}
					};
					// A COMMIT or CANCEL the grouping places outside a block still stands
					// for the end of the block that was open.
					let end = match role {
						StatementRole::Commit
						| StatementRole::Misplaced(MisplacedControl::CommitOutsideBlock) => InterruptEnd::Commit,
						StatementRole::Cancel
						| StatementRole::Misplaced(MisplacedControl::CancelOutsideBlock) => InterruptEnd::Cancel,
						StatementRole::Run
						| StatementRole::Option
						| StatementRole::Begin
						| StatementRole::Misplaced(
							MisplacedControl::NestedBegin | MisplacedControl::BeginInSession,
						) => InterruptEnd::RequestEnd,
					};
					let interrupted = BlockEnd::Interrupted {
						reason: done,
						end,
					};
					let rewrite = finalise_slots(&interrupted);
					// After timeout/cancel the txn is already gone, so a COMMIT cannot
					// succeed. The terminator answers its own slot, the statement observed
					// at the interrupt and each one after it answer the interrupt's.
					let terminates = !matches!(end, InterruptEnd::RequestEnd);
					let slot = if terminates {
						&rewrite.terminator
					} else if first {
						&rewrite.interrupted_statement
					} else {
						&rewrite.following
					};
					if let Some(slot) = slot {
						self.results.push(slot_result(slot, Duration::ZERO));
						self.emit_statement_event_unexecuted(kvs, kind, cancel_class);
					}
					if terminates {
						return Ok(interrupted);
					}
					first = false;
				}

				// Missing CANCEL/COMMIT statement, statement already canceled so nothing todo.
				return Ok(BlockEnd::Interrupted {
					reason: done,
					end: InterruptEnd::RequestEnd,
				});
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
			// What the block had queued for after its commit before this
			// statement ran, so that what the statement queues is told apart.
			let queued_before = waits.len().await;
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

					let rewrite = finalise_slots(&BlockEnd::Failed {
						cause: FailCause::NestedBegin,
						end: FailedEnd::RequestEnd,
					});
					self.rewrite_recorded(start_results, &rewrite);

					*in_flight = None;
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
						let end = match place.role {
							StatementRole::Commit => FailedEnd::Commit,
							StatementRole::Cancel => FailedEnd::Cancel,
							StatementRole::Run
							| StatementRole::Option
							| StatementRole::Begin
							| StatementRole::Misplaced(_) => FailedEnd::RequestEnd,
						};
						let failed = BlockEnd::Failed {
							cause: FailCause::NestedBegin,
							end,
						};
						let rewrite = finalise_slots(&failed);
						let terminates = !matches!(end, FailedEnd::RequestEnd);
						self.push_fast_forward_slot(&rewrite, terminates);
						if terminates {
							return Ok(failed);
						}
					}

					// Missing CANCEL/COMMIT statement, statement already canceled so nothing todo.
					return Ok(BlockEnd::Failed {
						cause: FailCause::NestedBegin,
						end: FailedEnd::RequestEnd,
					});
				}
				StatementRole::Cancel => {
					let _ = txn.cancel().await;

					// update the results indicating cancelation.
					let rewrite = finalise_slots(&BlockEnd::Cancelled);
					self.rewrite_recorded(start_results, &rewrite);

					// CANCEL returns NONE
					*in_flight = None;
					if let Some(slot) = &rewrite.terminator {
						self.results.push(slot_result(slot, before.elapsed()));
					}

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

					return Ok(BlockEnd::Cancelled);
				}
				StatementRole::Commit => {
					// Commit the transaction.
					// If error undo results.
					let e = if let Err(e) = self.commit_block(&txn).await {
						e
					} else {
						// Successfully commited. everything is fine.

						// Flush buffered notifications only after COMMIT succeeds. Rollback and
						// failed COMMIT paths drop the receiver without delivery.
						if let Some(prepared) = receiver {
							flush_live_query_notifications(prepared);
						}

						// COMMIT returns NONE
						*in_flight = None;
						if let Some(slot) = &finalise_slots(&BlockEnd::Committed).terminator {
							self.results.push(slot_result(slot, before.elapsed()));
						}

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

						return Ok(BlockEnd::Committed);
					};

					// `txn.commit()` failed (e.g. constraint on commit, or txn already finished).
					// Surface the failure on a dedicated COMMIT result row; mark prior statement
					// slots as not executed so nothing implies a successful commit.
					//
					// Unless the commit's outcome is unknown, in which case "not
					// executed" is a false statement about statements that may
					// well have been applied.
					let commit_failed = BlockEnd::CommitFailed {
						error: e.to_string(),
						indeterminate: crate::kvs::is_indeterminate_commit(&e),
						conflict: crate::kvs::is_retryable_transaction_conflict(&e),
					};
					let rewrite = finalise_slots(&commit_failed);
					self.rewrite_recorded(start_results, &rewrite);
					*in_flight = None;
					if let Some(slot) = &rewrite.terminator {
						self.results.push(slot_result(slot, before.elapsed()));
					}

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

					return Ok(commit_failed);
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
							*in_flight = None;
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
					let counters = self.install_statement_state();
					let statement_index = self.current_index();
					let r: Result<Value> = match self
						.execute_plan_in_transaction(Arc::clone(&txn), &before, stmt)
						.await
					{
						Ok(x) => Ok(x),
						// A `RETURN` ends its own statement with its value, which is the catch
						// every path shares. The statements after it run.
						Err(ControlFlow::Return(value)) => Ok(value),
						// A `BREAK` or `CONTINUE` no loop of the statement caught is
						// the statement's error, which fails the block as any other
						// does.
						Err(
							flow @ (ControlFlow::Err(_)
							| ControlFlow::Break
							| ControlFlow::Continue),
						) => {
							let e = if let ControlFlow::Err(e) = flow {
								e
							} else {
								anyhow!(ExecError::InvalidControlFlow)
							};
							let rewrite = finalise_slots(&BlockEnd::Failed {
								cause: FailCause::Error,
								end: FailedEnd::RequestEnd,
							});
							self.rewrite_recorded(start_results, &rewrite);
							let pending = DocIdsReclaimPending::of(&e).cloned();

							// Convert the anyhow error before pushing so we can both
							// classify it for the metric attribute and store it on the
							// result row in a single move.
							let typed_err = types_error_from_anyhow(e);
							let error_class =
								Some(crate::observe::error_class::classify_types_error(&typed_err));

							// statement return an error. Consume all the other statement until
							// we hit a cancel or commit.
							*in_flight = None;
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

							// A block cannot be run again here, so its refused index
							// statement reaches the client as a retryable conflict.
							// Finishing the reclaim first means that retry can claim
							// the space; the block has failed whatever this reports.
							if let Some(pending) = pending {
								let mut budget = kvs.transaction_timeout();
								if let Err(e) =
									self.finish_doc_ids_reclaim(kvs, &pending, &mut budget).await
								{
									debug!(target: TARGET, "a doc-ID reclaim was left unfinished: {e}");
								}
							}

							// A refused `LET` ends the stream, so nothing after it is pulled.
							while !self.carry_crossed
								&& let Some(stmt) = stream.next().await
							{
								yield_now!();
								let stmt = stmt?;
								self.slow_log_if_interrupted(&stmt);
								let place = place_statement(grouper, &stmt);
								let end = match place.role {
									StatementRole::Commit => FailedEnd::Commit,
									StatementRole::Cancel => FailedEnd::Cancel,
									StatementRole::Run
									| StatementRole::Option
									| StatementRole::Begin
									| StatementRole::Misplaced(_) => FailedEnd::RequestEnd,
								};
								let failed = BlockEnd::Failed {
									cause: FailCause::Error,
									end,
								};
								let rewrite = finalise_slots(&failed);
								let terminates = !matches!(end, FailedEnd::RequestEnd);
								self.push_fast_forward_slot(&rewrite, terminates);
								if terminates {
									return Ok(failed);
								}
							}

							// ran out of statements before the transaction ended.
							// Just break as we have nothing else we can do.
							return Ok(BlockEnd::Failed {
								cause: FailCause::Error,
								end: FailedEnd::RequestEnd,
							});
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
			// A statement that queued work for after the commit holds its event
			// until that work settles, when its slot does.
			let queued = waits.len().await.saturating_sub(queued_before);
			if queued > 0 && result.is_ok() {
				self.block_waits.push(BlockWait {
					slot: self.results.len(),
					waits: queued,
					event: HeldStatementEvent {
						kind: statement_type,
						read_only: statement_read_only,
						sql: sql_text,
						before,
						rows: stmt_result_rows,
					},
				});
			} else {
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
			}

			*in_flight = None;
			self.results.push(QueryResult {
				time: before.elapsed(),
				result,
				query_type,
			});
		}

		// we ran out of query but we still have an open transaction.
		// Be conservative and treat this essentially as a CANCEL statement.
		let _ = txn.cancel().await;

		let rewrite = finalise_slots(&BlockEnd::Unterminated);
		self.rewrite_recorded(start_results, &rewrite);

		Ok(BlockEnd::Unterminated)
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
	#[expect(
		clippy::too_many_arguments,
		reason = "the request's statements, where they stream to and how"
	)]
	pub(crate) async fn execute_statements_streaming_with_transaction(
		kvs: &Datastore,
		ctx: FrozenContext,
		opt: Options,
		statements: Vec<TopLevelStatement>,
		items: Sender<QueryStreamItem>,
		row_format: Option<surrealdb_rpc::RowFormat>,
		trace: surrealdb_rpc::TraceKinds,
		collected: bool,
	) -> Result<Vec<QueryResult>> {
		let mut executor = Self::new_streaming(kvs, ctx, opt, items, row_format, trace);
		if collected {
			executor.collect_whole_answer();
		}
		let mut statements = statements;
		executor.mark_statements(&mut statements);
		executor.prepare_request(&statements);
		let outcome =
			crate::exec::compiles::request(executor.run_with_transaction(kvs, statements)).await;
		executor.finalise_results().await;
		outcome.map(|()| executor.results)
	}

	/// Shared body: run every statement against the transaction already on the
	/// context, accumulating results on `self` and streaming them when this
	/// executor streams.
	///
	/// The caller commits the transaction whatever the statements answered. An
	/// `OPTION` applies to the statements after it, as in a block, and a
	/// statement that fails after writing part of its change refuses that
	/// commit ([`crate::kvs::Transaction::poison_failed_statement`]).
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
		if let Some(prepared) = self.prepare_broker(kvs, tx.writeable(), kvs.live_query_broker()) {
			tx.on_commit(Box::new(FlushOnCommit::new(prepared))).await;
		}
		let batch_start = Instant::now();
		self.batch_start = Some(batch_start);
		self.interruption_slow_logged = false;
		let results_start = self.results.len();
		// Every statement shares the transaction the caller holds, so each
		// transaction-control statement is marked misplaced.
		let mut grouper = RequestGrouper::new(true);

		// Every statement runs in the caller's transaction, as a block's do.
		self.in_block = true;
		self.in_session_txn = true;
		for stmt in statements {
			// See `consumer_gone`: the rest of the batch is abandoned rather
			// than run. The transaction spans requests and its owner still
			// decides whether to commit.
			if self.consumer_gone() {
				break;
			}
			trace!(target: TARGET, statement = %stmt.to_sql(), "Executing statement");
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
			let counters = self.install_statement_state();
			let written = tx.bytes_written();
			let result = match place.role {
				StatementRole::Option => self.run_option(&stmt),
				role @ (StatementRole::Run
				| StatementRole::Misplaced(_)
				| StatementRole::Begin
				| StatementRole::Commit
				| StatementRole::Cancel) => {
					// A session grouper marks every transaction-control statement
					// misplaced, and it reaches the engine with its mark.
					debug_assert!(!matches!(
						role,
						StatementRole::Begin | StatementRole::Commit | StatementRole::Cancel
					));
					self.execute_plan_in_transaction(Arc::clone(&tx), &start, stmt).await
				}
			};
			self.misplaced_control = None;
			// The caller commits whatever the statement answered, so a statement that failed
			// after it wrote would leave that part of its change durable without the rest. Its
			// commit is refused instead. One that failed before writing leaves nothing behind.
			if matches!(
				result,
				Err(ControlFlow::Err(_) | ControlFlow::Continue | ControlFlow::Break)
			) && tx.bytes_written() > written
			{
				tx.poison_failed_statement();
			}

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
					result: Err(types_error_from_anyhow(anyhow!(ExecError::InvalidControlFlow))),
					query_type: QueryType::Other,
				},
			};
			let outcome = Outcome::from(&query_result.result);
			let error_class = query_result
				.result
				.as_ref()
				.err()
				.map(crate::observe::error_class::classify_types_error);
			// Counters are accurate for DML (SELECT row counts come from the value shape,
			// which does not exist after conversion, so passing `None` is fine: the helper
			// short-circuits to counter-driven counts on DML and returns 0 on non-DML when
			// the value is `None`.
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
	#[cfg_attr(
		not(test),
		expect(
			dead_code,
			reason = "the unbounded form of the stream entry point; the datastore passes its carry bound to the bounded form"
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
		S: Stream<Item = Result<TopLevelExpr>>,
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
	pub(crate) async fn execute_statement_stream_bounded<S>(
		kvs: &Datastore,
		ctx: FrozenContext,
		opt: Options,
		skip_success_results: bool,
		carry: CarryBound,
		stream: S,
	) -> Result<Vec<QueryResult>>
	where
		S: Stream<Item = Result<TopLevelStatement>>,
	{
		let mut this = Executor::new(kvs, ctx, opt);
		this.carry = carry;
		this.run_statement_stream(kvs, skip_success_results, stream).await?;
		Ok(this.results)
	}

	/// [`Executor::execute_statement_stream_bounded`] with no bound on what the
	/// stream's top-level `LET`s retain.
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
		Self::execute_statement_stream_bounded(
			kvs,
			ctx,
			opt,
			skip_success_results,
			CarryBound::UNBOUNDED,
			stream,
		)
		.await
	}

	/// Execute `statements` as the fragments of one stream.
	///
	/// Runs exactly what [`Executor::execute_statements`] runs, except that no
	/// request program is compiled up front: each statement is compiled when it
	/// arrives, as a fragment of its own. One executor spans every fragment, so
	/// the `LET`s of earlier fragments are visible to later ones. The fragments
	/// are a stream, so the `LET`s they retain are held to
	/// [`IMPORT_CARRY_BOUND`](crate::kvs::ds::IMPORT_CARRY_BOUND).
	#[cfg(feature = "internal-fragments")]
	pub(crate) async fn execute_fragments(
		kvs: &Datastore,
		ctx: FrozenContext,
		opt: Options,
		statements: Vec<TopLevelStatement>,
	) -> Result<Vec<QueryResult>> {
		let mut this = Executor::new(kvs, ctx, opt);
		this.fragmented = true;
		this.carry = crate::kvs::ds::IMPORT_CARRY_BOUND;
		let stream = futures::stream::iter(statements.into_iter().map(Ok));
		crate::exec::compiles::request(this.run_statement_stream(kvs, false, stream)).await?;
		Ok(this.results)
	}

	/// Execute a batch of statements, sending results into `items` as they are
	/// produced rather than collecting them.
	///
	/// Returns the results as well, so an embedded caller that wants both the
	/// streamed items and the assembled answer pays for the query once.
	#[expect(
		clippy::too_many_arguments,
		reason = "the request's statements, where they stream to and how"
	)]
	pub(crate) async fn execute_statements_streaming(
		kvs: &Datastore,
		ctx: FrozenContext,
		opt: Options,
		statements: Vec<TopLevelStatement>,
		items: Sender<QueryStreamItem>,
		row_format: Option<surrealdb_rpc::RowFormat>,
		trace: surrealdb_rpc::TraceKinds,
		held_transactions: bool,
		collected: bool,
	) -> Result<Vec<QueryResult>> {
		let mut this = Executor::new_streaming(kvs, ctx, opt, items, row_format, trace);
		if held_transactions {
			this.explicit_emission = Emission::Hold;
		}
		if collected {
			this.collect_whole_answer();
		}
		let mut statements = statements;
		this.mark_statements(&mut statements);
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
		self.batch_start = Some(batch_start);
		self.interruption_slow_logged = false;
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

			trace!(target: TARGET, statement = %stmt.to_sql(), "Executing statement");

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
					// only failure is a permission denial via the
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
					self.begin_result_slot = (!skip_success_results).then_some(self.results.len());
					if !skip_success_results {
						self.results.push(QueryResult {
							time: Duration::ZERO,
							result: Ok(convert_value_to_public_value(Value::None)?),
							query_type: QueryType::Other,
						});
					}

					let begin_options = if let Dispatch::Begin(options) = stmt.dispatch() {
						options
					} else {
						BeginOptions::default()
					};
					// Boxed, as the bare statement's run is below: held inline, the block's run
					// would make this future, and every future awaiting it, its size.
					let begin_result = Box::pin(self.execute_begin_statement(
						kvs,
						stream.as_mut(),
						&mut grouper,
						place.emission,
						&begin_options,
					))
					.await;
					let outcome = Outcome::from(&begin_result);
					// `execute_begin_statement` returns `anyhow::Result`; on
					// failure the result is wrapped via
					// `types_error_from_anyhow` below (see the `if let
					// Err(e)` arm). We can't classify before that conversion
					// because we'd consume the error twice, so reach for the
					// `txn_create_failed` constant, because the only error that
					// `execute_begin_statement` raises is a failed transaction creation.
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
					let counters = self.install_statement_state();
					let statement_index = self.current_index();
					// Boxed: a statement's run is a large future, and each await of a future
					// that holds it inline copies it whole.
					let result = Box::pin(self.execute_bare_statement(kvs, &start, stmt)).await;
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
			// A `LET` the carry bound refused was the last statement of the stream.
			if self.carry_crossed {
				break;
			}
			// A request the language rejects runs nothing further.
			if self.request_rejection.is_some() {
				break;
			}
			yield_now!();
		}
		self.emit_query_event_for_results(kvs, batch_start, &self.results[batch_results_start..]);
		if let Some(refusal) = self.request_rejection.take() {
			self.results.truncate(batch_results_start);
			return Err(anyhow::Error::new(refusal.error()));
		}
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
			TopLevelStatement::Surreal(stmt) => {
				if let LogicalOp::Let(binding) = &stmt.plan {
					RequestEntry::Let {
						name: binding.name.clone(),
						kind: binding.kind.clone(),
						read_only,
						value: Some(Ok(*binding.value.clone())),
					}
				} else {
					RequestEntry::Run {
						op: Some(Ok(stmt.plan.clone())),
						read_only,
					}
				}
			}
			TopLevelStatement::Ast(stmt) => match stmt {
				TopLevelExpr::Begin(_) => RequestEntry::Begin,
				TopLevelExpr::Commit => RequestEntry::Commit,
				TopLevelExpr::Cancel => RequestEntry::Cancel,
				TopLevelExpr::Option(option) => RequestEntry::Option(option.clone()),
				TopLevelExpr::Use(stmt) => {
					let name = |expr: &Expr| crate::ddl::scalar::written_name(expr);
					RequestEntry::Use(match stmt {
						UseStatement::Ns(ns) => name(ns).map(|ns| (Some(ns), None)),
						UseStatement::Db(db) => name(db).map(|db| (None, Some(db))),
						UseStatement::NsDb(ns, db) => {
							name(ns).zip(name(db)).map(|(ns, db)| (Some(ns), Some(db)))
						}
						UseStatement::Default => None,
					})
				}
				TopLevelExpr::Live(_) | TopLevelExpr::Access(_) => RequestEntry::Stores,
				TopLevelExpr::Kill(_) | TopLevelExpr::Show(_) => RequestEntry::Inert,
				TopLevelExpr::Expr(Expr::Let(stmt)) => RequestEntry::Let {
					name: stmt.name.to_string(),
					kind: stmt.kind.clone(),
					read_only,
					value: Some(lower(stmt.what.clone()).map(|op| self.traced(op)).map_err(
						|reason| {
							ExecError::PlannerUnsupported {
								reason: reason.to_string(),
							}
							.into()
						},
					)),
				},
				TopLevelExpr::Expr(expr) => RequestEntry::Run {
					op: Some(self.lower_bare_expression(expr.clone())),
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

	/// A reached statement's credentials belong to its execution payload. A
	/// request records inert syntax before execution, so its cached lowering
	/// must be replaced after preparation. Later statements stay inert.
	fn refresh_reached_statement(&mut self, statement: &TopLevelStatement) {
		let Some(position) = self.position() else {
			return;
		};
		let entry = self.request_entry(statement);
		if let Some(request) = self.request.as_mut() {
			if let Some(slot) = request.entries.get_mut(position) {
				*slot = entry;
			}
			if let Some(compiled) = request.compiled.as_mut()
				&& let Some(slot) = compiled.get_mut(position)
			{
				*slot = None;
			}
		}
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
		// A request the language rejects runs no statement: each statement
		// reached before the driver stops answers with the rejection.
		if let Some(refusal) = &self.request_rejection {
			return Some(CompiledStatement {
				plan: Err(refusal.error()),
				logged: None,
			});
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
	/// the run computes. It is compiled under the session the statements
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
		let _compiling = RequestTiming::begin();
		let mut entries = std::mem::take(&mut request.entries);
		let mut compiled: Vec<Option<PlannedStatement>> =
			std::iter::repeat_with(|| None).take(entries.len()).collect();
		let mut builder = crate::exec::vm::RequestBuilder::default();
		let mut steps: Vec<RequestStep> = Vec::with_capacity(entries.len());
		let mut plan_session = crate::exec::vm::PlanSession::new(
			self.opt.clone(),
			self.ctx.shared_value("session").cloned().unwrap_or_else(|| Arc::new(Value::None)),
		);
		let keep_logged = self.ctx.slow_log().is_some();
		let mut same_txn = true;
		let mut changed = false;
		let mut in_block = self.in_block;
		// The `LET`s of the walk start afresh; the inputs, from position 0, stay.
		for history in self.binding_origins.values_mut() {
			history.retain(|(from, _)| *from == 0);
		}
		// The names the open explicit block has bound, which its end may undo:
		// those an earlier fragment of the block bound, then those of the walk.
		// A session transaction's statements are never undone by a block end.
		let mut block_names: Vec<String> = Vec::new();
		let mut explicit = in_block && !self.in_session_txn && self.let_undo.is_open();
		// The first region-constraint rejection of a request compiled from its
		// first statement, with the statement's position.
		let mut judged: Option<(usize, crate::exec::vm::payload::Refusal)> = None;
		if explicit {
			block_names.extend(self.let_undo.log.iter().map(|(name, _)| name.to_string()));
		}
		for (at, entry) in entries.iter_mut().enumerate() {
			let planning = at >= first;
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
					if !in_block {
						explicit = true;
						block_names.clear();
					}
					in_block = true;
					RequestStep::Begin
				}
				RequestEntry::Commit => {
					self.end_block_origins(&mut explicit, &mut block_names, at);
					in_block = false;
					same_txn &= !planning;
					RequestStep::Commit
				}
				RequestEntry::Cancel => {
					self.end_block_origins(&mut explicit, &mut block_names, at);
					in_block = false;
					same_txn &= !planning;
					RequestStep::Cancel
				}
				RequestEntry::Option(option) => {
					if planning {
						plan_session.option(&option.name, option.what);
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
							plan_session.use_names(ns.as_deref(), db.as_deref());
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
							let planned = self
								.compile_one(
									txn,
									op,
									&mut plan_session,
									inline,
									false,
									keep_logged,
									true,
									&mut builder,
								)
								.await;
							if first == 0 && judged.is_none() {
								judged = self.judge(txn, op, &mut plan_session, at, &planned).await;
							}
							compiled[at] = Some(planned);
							constant_value(op, kind.as_ref())
						}
						_ => None,
					};
					let origin = match constant {
						Some(_) => BindingOrigin::LetConst,
						None => BindingOrigin::LetComputed,
					};
					plan_session.bind_let(name, constant);
					self.note_let_origin(name, at + 1, origin);
					if explicit {
						block_names.push(name.clone());
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
							let planned = self
								.compile_one(
									txn,
									lowered,
									&mut plan_session,
									inline,
									*read_only || is_write_statement(lowered),
									keep_logged,
									false,
									&mut builder,
								)
								.await;
							if first == 0 && judged.is_none() {
								judged =
									self.judge(txn, lowered, &mut plan_session, at, &planned).await;
							}
							compiled[at] = Some(planned);
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
		// Compiled from the first statement, nothing of the request has run:
		// a statement the language rejects refuses the request as a whole,
		// whether it is compiled now or served when it is reached. The first
		// rejection in statement order answers. A statement compiled after
		// others ran fails alone, as any statement that fails to plan does.
		if first == 0 {
			self.request_rejection = judged.map(|(_, refusal)| refusal);
		}
		let program =
			compile_phase!("finish", crate::exec::planner::finish_request(builder, steps));
		match program {
			Ok(program) => (compiled, Some(program)),
			// A request whose program cannot be put together plans each
			// statement when it is reached.
			Err(_) => (Vec::new(), None),
		}
	}

	/// The region-constraint rejection of `op`, the statement at `position`
	/// of the request, planned as `planned`. A statement compiled now was
	/// judged by its compile, and is bound again only to number a rejection
	/// by its position. A statement served or planned when it is reached is
	/// bound here when it may hold a rejected construct ([`may_be_rejected`]);
	/// a schema clause that has an effect is refused by the statement that
	/// defines it when it is reached, so it does not refuse the request.
	async fn judge(
		&self,
		txn: &Arc<Transaction>,
		op: &LogicalOp,
		planning: &mut crate::exec::vm::PlanSession,
		position: usize,
		planned: &PlannedStatement,
	) -> Option<(usize, crate::exec::vm::payload::Refusal)> {
		match &planned.entry {
			Planned::Compiled(Ok(_)) => None,
			Planned::Compiled(Err(error)) => {
				let refusal = request_rejection(error)?;
				Some(
					self.rejection_of(txn, op, planning, position)
						.await
						.unwrap_or((position, refusal)),
				)
			}
			Planned::Served {
				..
			}
			| Planned::Reached {
				..
			} => match may_be_rejected(op) {
				true => {
					self.rejection_of(txn, op, planning, position).await.filter(|(_, refusal)| {
						!matches!(
							refusal,
							crate::exec::vm::payload::Refusal::SchemaClauseEffect { .. }
						)
					})
				}
				false => None,
			},
		}
	}

	/// The region-constraint rejection of `op`, the statement at `position`
	/// of the request, bound in `txn` under the session the statements
	/// before it leave; `None` when every region admits what it holds.
	async fn rejection_of(
		&self,
		txn: &Arc<Transaction>,
		op: &LogicalOp,
		planning: &mut crate::exec::vm::PlanSession,
		position: usize,
	) -> Option<(usize, crate::exec::vm::payload::Refusal)> {
		let ctx = planning.bound_context(&self.ctx, txn);
		crate::exec::planner::request_statement_rejection(
			op.clone(),
			&ctx,
			&self.function_registry,
			Arc::clone(txn),
			Some(Arc::clone(&planning.opt.auth)),
			position,
		)
		.await
		.map(|refusal| (position, refusal))
	}

	/// Compile `op`, a statement of the request, in `txn` under the session
	/// the statements before it leave, into the request `builder` builds;
	/// or, where it is not `inline`, or where it reads a parameter only the run
	/// computes, leave it to be served when it is reached. A `cacheable`
	/// statement — one that only reads, or a write — that reads the request's
	/// variables only where one compilation serves every value of them
	/// ([`crate::exec::vm::serve::kept_for_requests`]), or whose literals in
	/// those places are freed as such variables
	/// ([`crate::exec::vm::serve::free_literals`]), is served here, as the
	/// request arrives, from the cache of compiled statements, when the node
	/// keeps one. `binds_rows` is whether the statement is the value of a
	/// top-level `LET`, whose rows are read in the order they were bound.
	#[expect(
		clippy::too_many_arguments,
		reason = "the request being built, the session its statements leave, what the statement may be and how its rows are read"
	)]
	async fn compile_one(
		&self,
		txn: &Arc<Transaction>,
		op: &LogicalOp,
		planning: &mut crate::exec::vm::PlanSession,
		inline: bool,
		cacheable: bool,
		keep_logged: bool,
		binds_rows: bool,
		builder: &mut crate::exec::vm::RequestBuilder,
	) -> PlannedStatement {
		let logged = keep_logged.then(|| op.clone());
		let served = || PlannedStatement {
			entry: served_for(op.clone(), false),
			logged: keep_logged.then(|| op.clone()),
		};
		if !inline || (!planning.computed.is_empty() && reads_any(op, &planning.computed)) {
			return served();
		}
		let ctx = compile_phase!("bind", planning.bound_context(&self.ctx, txn));
		let opt = &planning.opt;
		let caches = cacheable
			&& self.ctx.compiled_units().is_some_and(|units| units.caches_statements())
			&& !crate::exec::planner::fold::changes_the_catalog(op)
			&& !op.any(|node| matches!(node, LogicalOp::Explain(_)));
		let freed = caches.then(|| crate::exec::vm::serve::free_literals(op, &ctx)).flatten();
		let (kept, literals) = match &freed {
			Some((kept, literals)) => (kept, literals.as_slice()),
			None => (op, &[][..]),
		};
		if caches && crate::exec::vm::serve::kept_for_requests(kept, &ctx, literals) {
			// Served from the cache of compiled statements as the request
			// arrives, in a context of the session the statements before it
			// leave. One whose compilation fails is served when it is reached,
			// which raises its error as a statement planned then does.
			let probe = crate::exec::context::statement_context(
				&ctx,
				opt,
				self.extract_session_info(),
				txn,
				&tokio_util::sync::CancellationToken::new(),
				true,
				crate::exec::context::ContextLevel::Root,
				None,
			)
			.await;
			if let Ok(probe) = probe
				&& let Ok(plan) = crate::exec::vm::out_of_line(|| {
					crate::exec::vm::serve::kept_request_statement(
						&probe,
						kept.clone(),
						Some(Arc::clone(&opt.auth)),
						literals,
					)
				})
				.await
			{
				let plan: Arc<dyn crate::exec::ExecOperator> = match freed {
					Some((_, literals)) => {
						Arc::new(crate::exec::vm::serve::LiteralBound::new(plan, literals))
					}
					None => plan,
				};
				return PlannedStatement {
					entry: Planned::Compiled(Ok(crate::exec::vm::RequestEntry::Operator(plan))),
					logged,
				};
			}
			return served();
		}
		let planned = crate::exec::planner::plan_request_statement(
			op.clone(),
			&ctx,
			&self.function_registry,
			Arc::clone(txn),
			Some(Arc::clone(&opt.auth)),
			Some(Arc::new(opt.clone())),
			binds_rows,
			builder,
		)
		.await;
		PlannedStatement {
			entry: Planned::Compiled(planned),
			logged,
		}
	}
}

/// Whether `op` may hold a construct a region constraint rejects: a schema
/// statement, whose stored clauses are judged where it is written; a write,
/// schema statement, `LET` or `SLEEP` nested anywhere inside it; or a closure
/// literal, whose body the walk of the plan does not enter. Every row of the
/// constraint table names one of those, so a statement that holds none is
/// admitted without being bound.
fn may_be_rejected(op: &LogicalOp) -> bool {
	if matches!(op, LogicalOp::Ddl(_)) {
		return true;
	}
	// The walk is in pre-order, so the first node it visits is `op` itself.
	let mut visited = 0_usize;
	op.visit_ops_deep(&mut |node| {
		visited += 1;
		let nested = visited > 1;
		let held = nested
			&& matches!(
				node,
				LogicalOp::Write(_) | LogicalOp::Ddl(_) | LogicalOp::Let(_) | LogicalOp::Sleep(_)
			);
		if held
			|| node
				.visit_scalars(&mut |expr| {
					if holds_closure(expr) {
						Err(())
					} else {
						Ok(())
					}
				})
				.is_err()
		{
			std::ops::ControlFlow::Break(())
		} else {
			std::ops::ControlFlow::Continue(())
		}
	})
	.is_break()
}

/// Whether `expr` is or holds a closure literal.
fn holds_closure(expr: &crate::expr::logical::LogicalExpr) -> bool {
	if matches!(expr, crate::expr::logical::LogicalExpr::Closure(_)) {
		return true;
	}
	let mut found = false;
	expr.for_each_child(&mut |child| found = found || holds_closure(child));
	found
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
		Dispatch::Begin(_) => matches!(
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
	let walked = scalar.visit(&mut |expr| {
		let res = if let LogicalExpr::Param(param) = expr {
			visitor.visit_param(param)
		} else if let LogicalExpr::Subquery(op) = expr {
			visit_plan_params(op, visitor)
		} else {
			Ok(())
		};
		match res {
			Ok(()) => Walk::Continue(()),
			Err(()) => Walk::Break(()),
		}
	});
	match walked {
		Walk::Continue(()) => Ok(()),
		Walk::Break(()) => Err(()),
	}
}

#[cfg(test)]
mod tests {
	use std::borrow::Cow;
	use std::sync::Arc;

	use surrealdb_types::{QueryError, ToSql};

	use super::{
		BlockEnd, Carried, CarryBound, FailCause, FailedEnd, InterruptEnd, LetUndo, SlotRewrite,
		SlotValue, finalise_slots, shape_statement_value, visit_plan_params,
	};
	use crate::ctx::Context;
	use crate::ctx::reason::Reason;
	use crate::dbs::Session;
	use crate::exec::OutputShape;
	use crate::expr::logical::lower;
	use crate::expr::visit::Visitor;
	use crate::iam::{Level, Role};
	use crate::kvs::Datastore;
	use crate::val::{Array, Value};

	/// SECURITY: a traced `LET` is copied only under the cap, so the estimate
	/// counts what a geometry holds: a line past the cap is past it here too.
	#[test]
	fn a_large_geometry_is_estimated_past_the_trace_cap() {
		let points: Vec<(f64, f64)> = (0..100_000).map(|i| (i as f64, i as f64)).collect();
		let line = Value::Geometry(crate::val::Geometry::Line(geo_types::LineString::from(points)));
		assert!(super::estimate_bytes(&line) > crate::exec::trace::CAP_BYTES);
	}

	#[tokio::test]
	async fn reached_credentials_replace_inert_request_payloads() {
		for source in [
			"DEFINE USER fresh ON ROOT PASSWORD 'secret' ROLES OWNER",
			"RETURN 1; DEFINE USER fresh ON ROOT PASSWORD 'secret' ROLES OWNER",
			"LET $out = (DEFINE USER fresh ON ROOT PASSWORD 'secret' ROLES OWNER); RETURN $out",
			"RETURN 1; LET $out = (DEFINE USER fresh ON ROOT PASSWORD 'secret' ROLES OWNER)",
			"{ DEFINE USER fresh ON ROOT PASSWORD 'secret' ROLES OWNER }",
		] {
			let ds = Datastore::new("memory").await.expect("memory datastore");
			let results =
				ds.execute(source, &Session::owner(), None).await.expect("execute request");
			assert!(results.iter().all(|result| result.result.is_ok()), "statement failed");
			crate::iam::verify::verify_root_creds(&ds, "fresh", "secret")
				.await
				.expect("reached user has its prepared password hash");
			assert!(crate::iam::verify::verify_root_creds(&ds, "fresh", "wrong").await.is_err());
		}
	}

	/// Slow-log statement formatting redacts credentials in every nested position.
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
			assert!(surface.to_sql().contains("hunter2"), "fixture must contain a password");
			let rendered = crate::dbs::TopLevelStatement::Ast(
				surrealdb_expr::parser::TopLevelExpr::Expr(surface),
			)
			.to_sql();
			assert!(!rendered.contains("hunter2"), "`{src}` rendered its password: {rendered}");
			assert!(
				rendered.contains("[REDACTED]"),
				"`{src}` should redact credentials: {rendered}"
			);
			assert!(
				rendered.contains("PASSHASH"),
				"`{src}` should retain the credential clause: {rendered}"
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

	/// A cancellation observed as a block's own `COMMIT` is pulled answers that
	/// `COMMIT` as a `COMMIT` of the cancelled block, and the block ends there.
	#[tokio::test]
	async fn a_cancellation_at_a_commit_answers_that_commit() {
		use futures::StreamExt;
		use surrealdb_expr::parser::TopLevelExpr;

		use crate::catalog::providers::CatalogProvider;
		use crate::kvs::TransactionType;

		let ds = Datastore::new("memory").await.expect("a memory datastore");
		let setup = ds.transaction(TransactionType::Write).await.expect("a transaction");
		setup.ensure_ns_db(None, "test", "test").await.expect("the namespace and database");
		setup.commit().await.expect("committing the namespace");

		let ses = Session::owner().with_ns("test").with_db("test");
		let opt = ds.setup_options(&ses);
		let mut ctx = ds.setup_ctx().expect("a context");
		let canceller = ctx.add_cancel();
		ctx.attach_session(&ses).expect("the session attaches");

		let ast = crate::syn::parse("BEGIN; CREATE a:1; COMMIT;").expect("the request parses");
		let stream = futures::stream::iter(ast.expressions).map(move |expr| {
			if matches!(expr, TopLevelExpr::Commit) {
				canceller.cancel();
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
		assert_eq!(results.len(), 3, "one result per statement: {messages:?}");
		assert!(
			messages[1].contains("The query was not executed due to a cancelled transaction"),
			"the CREATE: {}",
			messages[1]
		);
		assert!(
			messages[2].contains("Cannot COMMIT: the transaction was cancelled"),
			"the COMMIT: {}",
			messages[2]
		);
	}

	/// An executor over a memory datastore whose namespace and database exist.
	async fn executor_in_memory() -> (Arc<Datastore>, super::Executor) {
		use crate::catalog::providers::CatalogProvider;
		use crate::kvs::TransactionType;

		let ds = Datastore::new("memory").await.expect("a memory datastore");
		let setup = ds.transaction(TransactionType::Write).await.expect("a transaction");
		setup.ensure_ns_db(None, "test", "test").await.expect("the namespace and database");
		setup.commit().await.expect("committing the namespace");
		let ses = Session::owner().with_ns("test").with_db("test");
		let opt = ds.setup_options(&ses);
		let mut ctx = ds.setup_ctx().expect("a context");
		ctx.attach_session(&ses).expect("the session attaches");
		let exec = super::Executor::new(&ds, ctx.freeze(), opt);
		(ds, exec)
	}

	/// The region context of each statement, by the state the driver holds it in.
	#[tokio::test]
	async fn region_context_follows_where_the_statement_runs() {
		use crate::exec::ir::iface::premise::{OpenRegionShape, RegionContext, TxnContext};
		use crate::exec::ir::iface::region_kind::StatementCommit;
		use crate::expr::logical::{RequestGrouper, RequestStatementKind as Kind};

		let none = OpenRegionShape {
			open: TxnContext::None,
			continues: false,
		};
		let auto = RegionContext {
			commit: StatementCommit::Auto,
			txn: TxnContext::None,
			stack_shape: none,
		};
		let explicit = RegionContext {
			commit: StatementCommit::InTxn,
			txn: TxnContext::Explicit,
			stack_shape: none,
		};
		let session = RegionContext {
			commit: StatementCommit::InTxn,
			txn: TxnContext::Session,
			stack_shape: none,
		};
		let continued = OpenRegionShape {
			open: TxnContext::Explicit,
			continues: true,
		};
		let (_ds, mut exec) = executor_in_memory().await;

		// Auto-commit statements, a transaction control statement with nothing to
		// act on among them.
		let mut grouper = RequestGrouper::new(false);
		for kind in [Kind::Run, Kind::Option, Kind::Commit, Kind::Cancel] {
			assert_eq!(exec.region_context(&grouper.place(kind)), auto, "{kind:?} outside a block");
		}

		// A block: `BEGIN` is placed before the driver marks the block open.
		let mut grouper = RequestGrouper::new(false);
		assert_eq!(exec.region_context(&grouper.place(Kind::Begin)), explicit, "BEGIN");
		exec.in_block = true;
		for kind in [Kind::Run, Kind::Option, Kind::Begin, Kind::Commit] {
			assert_eq!(exec.region_context(&grouper.place(kind)), explicit, "{kind:?} in a block");
		}
		exec.in_block = false;
		assert_eq!(exec.region_context(&grouper.place(Kind::Run)), auto, "after the block");

		// A session transaction holds every statement, the misplaced ones included.
		exec.in_block = true;
		exec.in_session_txn = true;
		let mut grouper = RequestGrouper::new(true);
		for kind in [Kind::Run, Kind::Option, Kind::Begin, Kind::Commit, Kind::Cancel] {
			assert_eq!(
				exec.region_context(&grouper.place(kind)),
				session,
				"{kind:?} in a session transaction"
			);
		}

		// Fragments mode: a statement met while a block is open continues the
		// region an earlier fragment opened, and the `BEGIN` that opens it does
		// not.
		exec.in_block = false;
		exec.in_session_txn = false;
		exec.fragmented = true;
		let mut grouper = RequestGrouper::new(false);
		assert_eq!(exec.region_context(&grouper.place(Kind::Begin)), explicit, "fragment BEGIN");
		exec.in_block = true;
		for kind in [Kind::Run, Kind::Option, Kind::Begin, Kind::Commit] {
			assert_eq!(
				exec.region_context(&grouper.place(kind)),
				RegionContext {
					stack_shape: continued,
					..explicit
				},
				"{kind:?} in a block of fragments"
			);
		}
		exec.in_block = false;
		assert_eq!(
			exec.region_context(&grouper.place(Kind::Run)),
			auto,
			"fragment after the block"
		);
		exec.in_block = true;
		exec.in_session_txn = true;
		let mut grouper = RequestGrouper::new(true);
		assert_eq!(
			exec.region_context(&grouper.place(Kind::Run)),
			session,
			"a session transaction is not continued"
		);
	}

	/// Run `sql` as one request through an executor that has had `inputs`
	/// noted, and hand the executor back for its origins to be read.
	async fn executor_after_request(
		sql: &str,
		inputs: Option<(&[&str], &[&str])>,
	) -> (usize, super::Executor) {
		let (ds, mut exec) = executor_in_memory().await;
		if let Some((session, client)) = inputs {
			exec.note_inputs(session.iter().copied(), client.iter().copied());
		}
		let statements: Vec<_> = crate::syn::parse(sql)
			.expect("the request parses")
			.expressions
			.into_iter()
			.map(crate::dbs::TopLevelStatement::Ast)
			.collect();
		let count = statements.len();
		exec.prepare_request(&statements);
		let stream = exec.counted(futures::stream::iter(statements.into_iter().map(Ok)));
		crate::exec::compiles::request(exec.run_statement_stream(&ds, false, stream))
			.await
			.expect("the request runs");
		(count, exec)
	}

	/// The origin of each bound name: inputs by their source, `LET`s by the
	/// constant test, the last binding of a name before the asking statement
	/// deciding.
	#[tokio::test]
	async fn binding_origin_reports_where_each_name_came_from() {
		use crate::exec::ir::cache::deps::BindingOrigin::{
			ClientParam, LetComputed, LetConst, SessionVar,
		};

		let (end, exec) = executor_after_request(
			"LET $constant = 1; LET $computed = time::now(); \
			LET $flips = 1; LET $flips = time::now(); \
			LET $rebound = 2; LET $rebound_by_let = time::now();",
			Some((
				&["from_session", "shadowed", "rebound", "rebound_by_let"],
				&["from_client", "shadowed", "rebound_by_let"],
			)),
		)
		.await;

		for (name, origin) in [
			("from_session", SessionVar),
			("from_client", ClientParam),
			("shadowed", ClientParam),
			("constant", LetConst),
			("computed", LetComputed),
			("flips", LetComputed),
			("rebound", LetConst),
			("rebound_by_let", LetComputed),
			("never_bound", LetComputed),
		] {
			assert_eq!(exec.binding_origin(name, end), origin, "${name}");
		}
	}

	/// A statement is told the origin its own position sees: a `LET` after it
	/// does not reach back, and the name's earlier binding still stands.
	#[tokio::test]
	async fn binding_origin_is_per_statement_position() {
		use crate::exec::ir::cache::deps::BindingOrigin::{LetComputed, LetConst};

		let (end, exec) =
			executor_after_request("LET $x = time::now(); RETURN $x; LET $x = 1;", None).await;
		assert_eq!(exec.binding_origin("x", 0), LetComputed, "before any binding");
		assert_eq!(exec.binding_origin("x", 1), LetComputed, "the first binding");
		assert_eq!(exec.binding_origin("x", 2), LetComputed, "the statement before the rebinding");
		assert_eq!(exec.binding_origin("x", 3), LetConst, "after the rebinding");
		assert_eq!(exec.binding_origin("x", end), LetConst, "at the end of the request");
	}

	/// A block that cancels puts its bindings back, so the name it rebound is
	/// not a constant after it; a name the block did not bind keeps its origin.
	#[tokio::test]
	async fn binding_origin_after_a_block_that_does_not_commit_is_computed() {
		use crate::exec::ir::cache::deps::BindingOrigin::{LetComputed, LetConst};

		let (_, exec) = executor_after_request(
			"LET $x = time::now(); LET $y = 1; BEGIN; LET $x = 1; LET $z = 2; CANCEL; \
			RETURN $x;",
			None,
		)
		.await;
		assert_eq!(exec.binding_origin("x", 4), LetConst, "inside the block");
		assert_eq!(exec.binding_origin("x", 6), LetComputed, "after CANCEL");
		assert_eq!(exec.binding_origin("z", 6), LetComputed, "a name only the block bound");
		assert_eq!(exec.binding_origin("y", 6), LetConst, "a name the block left alone");
	}

	/// A `COMMIT` can fail at run time and take its block back, so the names
	/// the block bound are not constants after it either.
	#[tokio::test]
	async fn binding_origin_after_a_committed_block_is_computed() {
		use crate::exec::ir::cache::deps::BindingOrigin::{LetComputed, LetConst};

		let (_, exec) = executor_after_request("BEGIN; LET $x = 1; COMMIT; RETURN $x;", None).await;
		assert_eq!(exec.binding_origin("x", 2), LetConst, "inside the block");
		assert_eq!(exec.binding_origin("x", 3), LetComputed, "after COMMIT");
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

	/// Bind each of `names` to a small value in turn under `bound`, reporting
	/// the outcome of each.
	fn admit_each(bound: CarryBound, names: &[&str]) -> Vec<bool> {
		let mut carried = Carried::default();
		names
			.iter()
			.map(|name| bound.admit(&mut carried, name, &Value::from(1i64)).is_ok())
			.collect()
	}

	/// Bindings under the name bound pass; the one that crosses it fails.
	#[test]
	fn carry_bound_admits_names_up_to_the_bound() {
		let bound = CarryBound {
			names: Some(2),
			bytes: None,
		};
		assert_eq!(admit_each(bound, &["a", "b", "c"]), [true, true, false]);
	}

	/// A rebinding replaces its entry, so it does not count twice.
	#[test]
	fn carry_bound_counts_a_rebinding_once() {
		let bound = CarryBound {
			names: Some(2),
			bytes: None,
		};
		assert_eq!(admit_each(bound, &["a", "a", "a", "b", "b"]), [true; 5]);
		assert_eq!(admit_each(bound, &["a", "b", "a", "c"]), [true, true, true, false]);
	}

	/// A refused binding is not recorded, so a later rebinding of a held name
	/// still passes.
	#[test]
	fn carry_bound_leaves_a_refused_binding_unrecorded() {
		let bound = CarryBound {
			names: Some(1),
			bytes: None,
		};
		let mut carried = Carried::default();
		let one = Value::from(1i64);
		assert!(bound.admit(&mut carried, "a", &one).is_ok());
		assert!(bound.admit(&mut carried, "b", &one).is_err());
		assert!(bound.admit(&mut carried, "a", &one).is_ok());
		assert_eq!(carried.held.len(), 1);
	}

	/// The byte bound is checked against the estimate of the values held, and
	/// a rebinding to a smaller value releases the bytes of the old one.
	#[test]
	fn carry_bound_checks_bytes_only_when_set() {
		let big = Value::from("x".repeat(1000));
		let small = Value::from(1i64);

		let unbounded = CarryBound::UNBOUNDED;
		let mut carried = Carried::default();
		assert!(unbounded.admit(&mut carried, "a", &big).is_ok());
		assert!(carried.held.is_empty(), "an unbounded stream tracks nothing");

		let bound = CarryBound {
			names: None,
			bytes: Some(1500),
		};
		let mut carried = Carried::default();
		assert!(bound.admit(&mut carried, "a", &big).is_ok());
		let err = bound.admit(&mut carried, "b", &big).expect_err("the second value crosses");
		assert!(err.to_string().contains("1500 bytes"), "got: {err}");
		assert!(bound.admit(&mut carried, "a", &small).is_ok());
		assert!(bound.admit(&mut carried, "b", &big).is_ok());
	}

	fn int(n: i64) -> Arc<Value> {
		Arc::new(Value::from(n))
	}

	/// What `name` holds in `ctx`, `None` where it holds nothing.
	fn held(ctx: &Context, name: &str) -> Option<Value> {
		ctx.value(name).cloned()
	}

	/// A commit keeps the block's bindings and forgets the log, so a later
	/// block's rollback cannot reach them.
	#[test]
	fn let_undo_commit_truncates_to_the_mark() {
		let mut ctx = Context::new_test();
		let mut undo = LetUndo::default();
		undo.begin();
		undo.record("a", None);
		ctx.add_value("a", int(1));
		undo.commit();
		assert!(!undo.is_open());
		assert!(undo.log.is_empty());

		undo.begin();
		undo.record("b", None);
		ctx.add_value("b", int(2));
		undo.rollback(&mut ctx);
		assert_eq!(held(&ctx, "a"), Some(Value::from(1i64)), "the committed binding stands");
		assert_eq!(held(&ctx, "b"), None, "a name the block introduced is absent again");
	}

	/// A rollback replays the log in reverse, so a rebinding returns to the
	/// value it held at `BEGIN` and a name that held nothing returns to absent.
	#[test]
	fn let_undo_rollback_restores_a_rebinding_and_an_absent_name() {
		let mut ctx = Context::new_test();
		ctx.add_value("x", int(0));
		let mut undo = LetUndo::default();
		undo.begin();
		for (name, value) in [("x", 1i64), ("x", 2), ("y", 3)] {
			undo.record(name, ctx.own_value(name).cloned());
			ctx.add_value(name, int(value));
		}
		assert_eq!(undo.log.len(), 3);
		undo.rollback(&mut ctx);
		assert!(!undo.is_open());
		assert!(undo.log.is_empty());
		assert_eq!(held(&ctx, "x"), Some(Value::from(0i64)));
		assert_eq!(held(&ctx, "y"), None);
	}

	/// Outside a block nothing is recorded, and a rollback with no block open
	/// changes nothing.
	#[test]
	fn let_undo_records_nothing_outside_a_block() {
		let mut ctx = Context::new_test();
		ctx.add_value("x", int(1));
		let mut undo = LetUndo::default();
		undo.record("x", None);
		assert!(undo.log.is_empty());
		undo.rollback(&mut ctx);
		assert_eq!(held(&ctx, "x"), Some(Value::from(1i64)));
	}

	/// A UTF-8 byte order mark at the start of an import is not part of its
	/// statements, however the stream is chunked.
	#[tokio::test]
	async fn import_stream_skips_a_utf8_byte_order_mark() {
		use bytes::Bytes;

		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::default().with_ns("NS").with_db("DB");
		ds.execute("DEFINE NAMESPACE NS; USE NS NS; DEFINE DATABASE DB", &sess, None)
			.await
			.unwrap();

		let sql = "\u{feff}OPTION IMPORT;\nCREATE person:1;\nCREATE person:2;\n";
		for chunk in [sql.len(), 1] {
			let chunks: Vec<_> =
				sql.as_bytes().chunks(chunk).map(|c| Ok(Bytes::copy_from_slice(c))).collect();
			let results =
				ds.import_stream(&sess, futures::stream::iter(chunks)).await.unwrap_or_else(|e| {
					panic!("chunks of {chunk}: {e}");
				});
			assert!(results.is_empty(), "chunks of {chunk}: {results:?}");
			ds.execute("DELETE person", &sess, None).await.unwrap();
		}
	}

	/// An import that is UTF-16 text is refused as such, rather than as a
	/// syntax error at its first byte.
	#[tokio::test]
	async fn import_stream_names_utf16_input() {
		use bytes::Bytes;

		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::default().with_ns("NS").with_db("DB");
		let sql = "OPTION IMPORT; CREATE person:1;";
		for bom in [[0xFF, 0xFE], [0xFE, 0xFF]] {
			let mut bytes = bom.to_vec();
			bytes.extend(sql.encode_utf16().flat_map(|u| u.to_le_bytes()));
			let body = futures::stream::once(async { Ok(Bytes::from(bytes)) });
			let err = ds.import_stream(&sess, body).await.unwrap_err();
			assert!(err.to_string().contains("UTF-16"), "got: {err}");
		}
	}

	/// An import whose carried names cross the bound fails the `LET` that
	/// crosses it and stops there.
	#[tokio::test]
	async fn import_stream_stops_at_the_let_that_crosses_the_carry_bound() {
		use bytes::Bytes;

		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::default().with_ns("NS").with_db("DB");

		ds.execute("DEFINE NAMESPACE NS; USE NS NS; DEFINE DATABASE DB", &sess, None)
			.await
			.unwrap();

		let sql = "OPTION IMPORT; CREATE person:1; LET $a = 1; LET $b = 2; LET $c = 3; \
		           CREATE person:2;";
		let body = futures::stream::once(async { Ok(Bytes::from(sql)) });
		let bound = CarryBound {
			names: Some(2),
			bytes: None,
		};
		let results = ds.execute_import_bounded(&sess, None, body, bound).await.unwrap();

		assert_eq!(results.len(), 1, "only the crossing LET reports");
		let err = results[0].result.as_ref().unwrap_err();
		assert!(err.message().contains("limit of 2"), "got: {err:?}");

		let verify = ds.execute("SELECT VALUE id FROM person", &sess, None).await.unwrap();
		let rows = verify[0].result.as_ref().unwrap().as_array().unwrap();
		assert_eq!(rows.len(), 1, "the statement before the crossing ran, the one after did not");
	}

	/// A bound crossed inside an open block cancels the block, and the stream
	/// ends there.
	#[tokio::test]
	async fn import_stream_cancels_the_open_block_at_the_carry_bound() {
		use bytes::Bytes;

		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::default().with_ns("NS").with_db("DB");

		ds.execute("DEFINE NAMESPACE NS; USE NS NS; DEFINE DATABASE DB", &sess, None)
			.await
			.unwrap();

		let sql = "OPTION IMPORT; BEGIN; CREATE person:1; LET $a = 1; LET $b = 2; COMMIT; \
		           CREATE person:2;";
		let body = futures::stream::once(async { Ok(Bytes::from(sql)) });
		let bound = CarryBound {
			names: Some(1),
			bytes: None,
		};
		let results = ds.execute_import_bounded(&sess, None, body, bound).await.unwrap();
		assert!(results.iter().any(|r| r.result.is_err()), "the crossing LET reports an error");

		// Neither the block nor the statement after it ran, so no statement
		// created the table.
		let verify = ds.execute("SELECT VALUE id FROM person", &sess, None).await.unwrap();
		let err = verify[0].result.as_ref().unwrap_err();
		assert!(err.message().contains("'person' does not exist"), "got: {err:?}");
	}

	/// A top-level `RETURN` ends its own statement; the statements after it run.
	#[tokio::test]
	async fn import_stream_continues_after_a_top_level_return() {
		use bytes::Bytes;

		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::default().with_ns("NS").with_db("DB");

		ds.execute("DEFINE NAMESPACE NS; USE NS NS; DEFINE DATABASE DB", &sess, None)
			.await
			.unwrap();

		let sql = "OPTION IMPORT; CREATE person:1; RETURN 1; CREATE person:2;";
		let body = futures::stream::once(async { Ok(Bytes::from(sql)) });
		let results = ds.import_stream(&sess, body).await.unwrap();
		assert!(results.is_empty(), "no statement failed, so nothing reports");

		let verify = ds.execute("SELECT VALUE id FROM person", &sess, None).await.unwrap();
		let rows = verify[0].result.as_ref().unwrap().as_array().unwrap();
		assert_eq!(rows.len(), 2, "the statement after the RETURN ran");
	}

	/// A failing statement after a `BEGIN` cancels the block, whichever
	/// statement of the import it arrives in.
	#[tokio::test]
	async fn import_stream_cancels_a_block_whose_later_statement_fails() {
		use bytes::Bytes;

		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::default().with_ns("NS").with_db("DB");

		ds.execute("DEFINE NAMESPACE NS; USE NS NS; DEFINE DATABASE DB", &sess, None)
			.await
			.unwrap();

		let sql = "OPTION IMPORT; BEGIN; CREATE person:1; THROW 'x'; COMMIT; CREATE person:2;";
		let body = futures::stream::once(async { Ok(Bytes::from(sql)) });
		let results = ds.import_stream(&sess, body).await.unwrap();
		assert!(results.iter().any(|r| r.result.is_err()), "the failing statement reports");

		let verify = ds.execute("SELECT VALUE id FROM person", &sess, None).await.unwrap();
		let rows = verify[0].result.as_ref().unwrap().as_array().unwrap();
		assert_eq!(rows.len(), 1, "the block was cancelled, the statement after it ran");
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

	/// A statement run in a transaction of its own that loses a retryable
	/// commit conflict runs again in a fresh one, bounded by
	/// `STATEMENT_CONFLICT_ATTEMPTS`, unless it reached outside its transaction.
	mod statement_conflict_retry {
		use std::sync::Arc;

		use surrealdb_types::Value;

		use super::super::STATEMENT_CONFLICT_ATTEMPTS;
		use crate::dbs::Session;
		use crate::kvs::Datastore;
		use crate::kvs::testing::{
			RetryableConflictSite, inject_retryable_conflicts, retryable_conflict_count,
		};

		const SITE: RetryableConflictSite = RetryableConflictSite::StatementCommit;

		async fn new_ds() -> (Arc<Datastore>, Session) {
			let ds = Datastore::builder()
				.without_maintenance_tasks()
				.with_capabilities(crate::dbs::Capabilities::all())
				.build_with_path("memory")
				.await
				.expect("datastore should open");
			let ses = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				"DEFINE NAMESPACE test; USE NS test; DEFINE DATABASE test",
				&Session::owner(),
				None,
			)
			.await
			.expect("the namespace and database should be defined");
			ds.execute("DEFINE TABLE t; CREATE t:counter SET n = 0", &ses, None)
				.await
				.expect("the table should be defined");
			(ds, ses)
		}

		async fn query(ds: &Datastore, ses: &Session, sql: &str) -> Result<Value, String> {
			let mut res = ds.execute(sql, ses, None).await.expect("the query should run");
			res.remove(0).result.map_err(|e| e.to_string())
		}

		async fn counter(ds: &Datastore, ses: &Session) -> Value {
			query(ds, ses, "SELECT VALUE n FROM ONLY t:counter").await.expect("the read")
		}

		#[tokio::test]
		async fn a_write_that_loses_its_commit_runs_again_and_applies_once() {
			let (ds, ses) = new_ds().await;
			let _guard = inject_retryable_conflicts(
				SITE,
				ds.id(),
				(STATEMENT_CONFLICT_ATTEMPTS - 1) as usize,
			);
			let res = query(&ds, &ses, "UPDATE t:counter SET n += 1 RETURN VALUE n").await;
			assert_eq!(
				res,
				Ok(Value::Array(vec![Value::Number(surrealdb_types::Number::Int(1))].into()))
			);
			assert_eq!(retryable_conflict_count(SITE, ds.id()), 0, "every conflict is consumed");
			assert_eq!(
				counter(&ds, &ses).await,
				Value::Number(surrealdb_types::Number::Int(1)),
				"the write applied once"
			);
		}

		#[tokio::test]
		async fn a_schema_statement_runs_again_too() {
			let (ds, ses) = new_ds().await;
			let _guard = inject_retryable_conflicts(SITE, ds.id(), 2);
			query(&ds, &ses, "REMOVE TABLE t").await.expect("REMOVE TABLE should succeed");
			assert_eq!(retryable_conflict_count(SITE, ds.id()), 0, "every conflict is consumed");
			let tables = query(&ds, &ses, "RETURN object::keys((INFO FOR DB).tables)").await;
			assert_eq!(tables, Ok(Value::Array(Vec::<Value>::new().into())), "the table is gone");
		}

		#[tokio::test]
		async fn a_write_that_keeps_losing_reports_the_conflict() {
			let (ds, ses) = new_ds().await;
			let _guard =
				inject_retryable_conflicts(SITE, ds.id(), STATEMENT_CONFLICT_ATTEMPTS as usize);
			let mut res = ds
				.execute("UPDATE t:counter SET n += 1", &ses, None)
				.await
				.expect("the query should run");
			let err = res.remove(0).result.expect_err("the write should report the conflict");
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
			assert_eq!(
				counter(&ds, &ses).await,
				Value::Number(surrealdb_types::Number::Int(0)),
				"nothing applied"
			);
		}

		/// A script may have reached outside the transaction, so a statement
		/// that ran one is not run again.
		#[cfg(feature = "scripting")]
		#[tokio::test]
		async fn a_write_that_ran_a_script_is_not_run_again() {
			let (ds, ses) = new_ds().await;
			let _guard = inject_retryable_conflicts(SITE, ds.id(), 1);
			let res = query(&ds, &ses, "UPDATE t:counter SET n += function() { return 1; }").await;
			let err = res.expect_err("the conflict is reported");
			assert!(err.contains("can be retried"), "the conflict is reported: {err}");
			assert_eq!(retryable_conflict_count(SITE, ds.id()), 0, "one attempt was made");
			assert_eq!(
				counter(&ds, &ses).await,
				Value::Number(surrealdb_types::Number::Int(0)),
				"nothing applied"
			);
		}

		#[tokio::test]
		async fn a_statement_in_a_block_is_not_run_again() {
			let (ds, ses) = new_ds().await;
			let _guard = inject_retryable_conflicts(SITE, ds.id(), 1);
			let res = ds
				.execute("BEGIN; UPDATE t:counter SET n += 1; COMMIT;", &ses, None)
				.await
				.expect("the query should run");
			assert!(res.iter().all(|r| r.result.is_ok()), "the block commits: {res:?}");
			assert_eq!(
				retryable_conflict_count(SITE, ds.id()),
				1,
				"a block's commit is not a statement's own"
			);
			assert_eq!(counter(&ds, &ses).await, Value::Number(surrealdb_types::Number::Int(1)));
		}
	}

	/// A streamed write statement running in a transaction of its own holds
	/// its rows back until its commit while they stay within
	/// `stream_stage_bytes`, so a lost commit runs it again without the
	/// consumer seeing the failed attempt's rows. Past the bound its rows reach
	/// the consumer as they are produced: the consumer can stop it before it
	/// commits, and it is not run again.
	mod streamed_write_staging {
		use std::collections::HashMap;
		use std::sync::Arc;
		use std::time::Duration;

		use surrealdb_cnf::ConfigMap;
		use surrealdb_rpc::QUERY_STREAM_BUFFER;
		use surrealdb_types::Value;

		use super::super::{Stage, StreamSink};
		use crate::dbs::{QueryStreamItem, Session};
		use crate::kvs::testing::{
			RetryableConflictSite, inject_retryable_conflicts, retryable_conflict_count,
		};
		use crate::kvs::{Datastore, QueryRequest};

		const SITE: RetryableConflictSite = RetryableConflictSite::StatementCommit;

		/// A statement whose returned rows are well past the default bound.
		const LARGE: &str = "CREATE |big:5000| SET pad = 'xxxxxxxxxxxxxxxx'";

		async fn new_ds(config: ConfigMap) -> (Arc<Datastore>, Session) {
			let ds = Datastore::builder()
				.without_maintenance_tasks()
				.with_config(config)
				.build_with_path("memory")
				.await
				.expect("datastore should open");
			let ses = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				"DEFINE NAMESPACE test; USE NS test; DEFINE DATABASE test",
				&Session::owner(),
				None,
			)
			.await
			.expect("the namespace and database should be defined");
			for res in ds
				.execute("DEFINE TABLE big; DEFINE TABLE t; CREATE t:counter SET n = 0", &ses, None)
				.await
				.expect("the seed should run")
			{
				res.result.expect("every seed statement succeeds");
			}
			(ds, ses)
		}

		/// Every item a consumer that reads to the end receives.
		async fn streamed_items(
			ds: &Arc<Datastore>,
			ses: &Session,
			sql: &str,
		) -> Vec<QueryStreamItem> {
			let (tx, rx) = async_channel::bounded(QUERY_STREAM_BUFFER);
			let job = ds.run_streaming(QueryRequest::new(sql, ses), tx).expect("the query parses");
			let drain = async {
				let mut items = Vec::new();
				while let Ok(item) = rx.recv().await {
					items.push(item);
				}
				items
			};
			let (outcome, items) = futures::future::join(job.run, drain).await;
			outcome.expect("the execution completes");
			items
		}

		/// The rows statement 0 sent, and its `Finished` error, if any.
		fn statement_zero(items: Vec<QueryStreamItem>) -> (Vec<Value>, Option<String>) {
			let mut rows = Vec::new();
			let mut error = None;
			for item in items {
				match item {
					QueryStreamItem::Rows {
						index: 0,
						values,
					} => rows.extend(values),
					QueryStreamItem::Frame {
						index: 0,
						rows: frame,
					} => rows.extend(frame.to_values().expect("rows decode")),
					QueryStreamItem::Finished {
						index: 0,
						error: e,
						..
					} => error = e.map(|e| e.to_string()),
					QueryStreamItem::Rows {
						..
					}
					| QueryStreamItem::Frame {
						..
					}
					| QueryStreamItem::Value {
						..
					}
					| QueryStreamItem::Finished {
						..
					} => {}
					QueryStreamItem::Trace {
						..
					} => {}
				}
			}
			(rows, error)
		}

		async fn count(ds: &Datastore, ses: &Session, table: &str) -> Value {
			let mut res = ds
				.execute(&format!("RETURN count(SELECT VALUE id FROM {table})"), ses, None)
				.await
				.expect("the count should run");
			res.remove(0).result.expect("the count")
		}

		fn int(n: i64) -> Value {
			Value::Number(surrealdb_types::Number::Int(n))
		}

		/// A small write that loses its commit runs again, and the consumer
		/// receives the rows of the attempt that committed alone.
		#[tokio::test]
		async fn a_small_write_that_loses_its_commit_sends_only_the_final_attempts_rows() {
			let (ds, ses) = new_ds(ConfigMap::empty()).await;
			let _guard = inject_retryable_conflicts(SITE, ds.id(), 2);
			let items =
				streamed_items(&ds, &ses, "UPDATE t:counter SET n += 1 RETURN VALUE n").await;
			assert_eq!(retryable_conflict_count(SITE, ds.id()), 0, "every conflict is consumed");
			let (rows, error) = statement_zero(items);
			assert_eq!(error, None, "the third attempt commits");
			assert_eq!(rows, vec![int(1)], "only the committed attempt's row is sent");
			assert_eq!(count(&ds, &ses, "t").await, int(1));
			let counter = ds
				.execute("SELECT VALUE n FROM ONLY t:counter", &ses, None)
				.await
				.expect("the read")
				.remove(0)
				.result;
			assert_eq!(counter, Ok(int(1)), "the write applied once");
		}

		/// A write whose rows pass the bound sends them before its commit: a
		/// lost commit then fails the statement, which is not run again.
		#[tokio::test]
		async fn a_large_write_sends_its_rows_before_its_commit_and_is_not_run_again() {
			let (ds, ses) =
				new_ds(ConfigMap::empty().with_key_value("stream_stage_bytes", "64KiB")).await;
			let _guard = inject_retryable_conflicts(SITE, ds.id(), 2);
			let items = streamed_items(&ds, &ses, LARGE).await;
			assert_eq!(retryable_conflict_count(SITE, ds.id()), 1, "one attempt was made");
			let (rows, error) = statement_zero(items);
			let error = error.expect("the lost commit fails the statement");
			assert!(error.contains("can be retried"), "the conflict is reported: {error}");
			assert!(!rows.is_empty(), "rows reached the consumer before the commit");
			assert!(rows.len() <= 5000, "no second attempt's rows were sent");
			assert_eq!(count(&ds, &ses, "big").await, int(0), "nothing committed");
		}

		/// With no bound, the default, the same statement is held back whole,
		/// and runs again after a lost commit.
		#[tokio::test]
		async fn by_default_a_large_write_is_held_back_and_runs_again() {
			let (ds, ses) = new_ds(ConfigMap::empty()).await;
			let _guard = inject_retryable_conflicts(SITE, ds.id(), 1);
			let (rows, error) = statement_zero(streamed_items(&ds, &ses, LARGE).await);
			assert_eq!(error, None, "the second attempt commits");
			assert_eq!(rows.len(), 5000, "only the committed attempt's rows are sent");
			assert_eq!(count(&ds, &ses, "big").await, int(5000));
		}

		/// A consumer that goes away while a large write is still sending its
		/// rows stops it before its commit, and nothing it wrote is kept.
		#[tokio::test]
		async fn a_consumer_that_stops_reading_stops_a_large_write_uncommitted() {
			let (ds, ses) =
				new_ds(ConfigMap::empty().with_key_value("stream_stage_bytes", "64KiB")).await;
			let (tx, rx) = async_channel::bounded(QUERY_STREAM_BUFFER);
			let sql = format!("{LARGE}; CREATE t:after");
			let job = ds
				.run_streaming(QueryRequest::new(sql.as_str(), &ses), tx)
				.expect("the query parses");
			let running = tokio::spawn(job.run);
			let first = tokio::time::timeout(Duration::from_secs(30), rx.recv())
				.await
				.expect("rows arrive before the statement commits")
				.expect("at least one item");
			assert!(
				matches!(
					first,
					QueryStreamItem::Rows {
						index: 0,
						..
					}
				),
				"the write's rows arrive first, got {first:?}"
			);
			assert_eq!(count(&ds, &ses, "big").await, int(0), "the write has not committed");
			drop(rx);
			running
				.await
				.expect("the execution task completes")
				.expect("abandoning the results is not itself an error");
			assert_eq!(count(&ds, &ses, "big").await, int(0), "nothing the write wrote is kept");
			assert_eq!(count(&ds, &ses, "t").await, int(1), "the statement after it never ran");
		}

		fn rows(index: usize) -> QueryStreamItem {
			QueryStreamItem::Rows {
				index,
				values: vec![Value::None],
			}
		}

		fn sink(items: async_channel::Sender<QueryStreamItem>, stage_max: usize) -> StreamSink {
			StreamSink {
				items,
				staged: Vec::new(),
				staged_bytes: 0,
				stage_max,
				stage: Stage::Off,
				streamed: HashMap::new(),
				row_format: None,
				trace: surrealdb_rpc::TraceKinds::NONE,
				statement_traces: HashMap::new(),
				reads: None,
				finalised: 0,
			}
		}

		/// An answer gathered whole holds an attempt's items past the bound:
		/// none of them has reached a client, so the attempt may run again.
		#[tokio::test]
		async fn a_collected_answer_holds_an_attempt_past_the_bound() {
			let (tx, rx) = async_channel::unbounded();
			let mut sink = sink(tx, 1000);
			sink.collect_whole();
			sink.begin_staging();
			for _ in 0..10 {
				sink.send_rows(rows(0), 300).await;
			}
			assert!(rx.is_empty(), "a collected answer sends nothing before the commit");
			assert_eq!(sink.stage, Stage::Holding, "so the attempt may still run again");
			assert!(!sink.discard_staged(0), "discarding it reports nothing delivered");
		}

		/// The held items never pass the bound by more than the item that
		/// crossed it: that item sends them all, and the rest go out as they
		/// come.
		#[tokio::test]
		async fn held_rows_stay_within_the_bound() {
			let (tx, rx) = async_channel::unbounded();
			let mut sink = sink(tx, 1000);
			sink.begin_staging();
			for sent in 1..=3 {
				sink.send_rows(rows(0), 300).await;
				assert_eq!(sink.staged.len(), sent);
				assert!(sink.holding());
			}
			assert!(rx.is_empty(), "nothing goes out under the bound");
			sink.send_rows(rows(0), 300).await;
			assert_eq!(sink.stage, Stage::Released);
			assert!(sink.staged.is_empty(), "crossing the bound sends what was held");
			assert_eq!(sink.staged_bytes, 0);
			assert_eq!(rx.len(), 4);
			for _ in 0..100 {
				sink.send_rows(rows(0), 300).await;
				assert!(sink.staged.is_empty(), "past the bound nothing is held");
			}
			assert_eq!(rx.len(), 104);
			assert!(sink.discard_staged(0), "the attempt's rows reached the consumer");
			assert_eq!(sink.stage, Stage::Off);
		}

		/// A bound of zero holds back no rows, and an attempt that sent none is
		/// still one that may run again.
		#[tokio::test]
		async fn a_zero_bound_holds_back_no_rows() {
			let (tx, rx) = async_channel::unbounded();
			let mut sink = sink(tx, 0);
			sink.begin_staging();
			assert!(!sink.discard_staged(0), "an attempt that sent nothing may run again");
			sink.begin_staging();
			sink.send_rows(rows(0), 1).await;
			assert!(sink.staged.is_empty());
			assert_eq!(rx.len(), 1);
			assert!(sink.discard_staged(0));
		}

		/// Rows held under the bound go out when the attempt commits, and are
		/// dropped when it fails.
		#[tokio::test]
		async fn held_rows_go_out_on_commit_and_are_dropped_on_failure() {
			let (tx, rx) = async_channel::unbounded();
			let mut sink = sink(tx, 1000);
			sink.begin_staging();
			sink.send_rows(rows(0), 10).await;
			assert!(!sink.discard_staged(0), "held rows never reached the consumer");
			assert!(rx.is_empty(), "a failed attempt's held rows are dropped");
			sink.begin_staging();
			sink.send_rows(rows(0), 10).await;
			sink.flush_staged().await;
			assert_eq!(sink.stage, Stage::Off);
			assert_eq!(rx.len(), 1, "a committed attempt's held rows go out");
		}
	}

	/// A bare `LIVE` or `KILL` that loses a retryable commit conflict runs
	/// again in a fresh transaction, bounded by `STATEMENT_CONFLICT_ATTEMPTS`,
	/// and a statement inside a transaction block never does.
	mod live_kill_conflict_retry {
		use std::sync::Arc;
		use std::time::Duration;

		use surrealdb_catalog::providers::CatalogProvider;
		use surrealdb_types::{Action, Notification, Value};

		use super::super::STATEMENT_CONFLICT_ATTEMPTS;
		use crate::dbs::Session;
		use crate::kvs::Datastore;
		use crate::kvs::testing::{
			RetryableConflictSite, inject_retryable_conflicts, retryable_conflict_count,
		};

		const SITE: RetryableConflictSite = RetryableConflictSite::LiveKillCommit;

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
				(STATEMENT_CONFLICT_ATTEMPTS - 1) as usize,
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
				inject_retryable_conflicts(SITE, ds.id(), STATEMENT_CONFLICT_ATTEMPTS as usize);
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
		async fn the_transaction_timeout_bounds_the_attempts_together() {
			let (ds, _recv, ses) = new_ds().await;
			// The waits before the second to seventh attempts come to at least
			// 31.5 ms, so a 20 ms budget stops the statement short of its
			// attempt bound.
			let ds = Arc::into_inner(ds)
				.expect("nothing else holds the datastore")
				.with_transaction_timeout(Some(Duration::from_millis(20)));
			let id = live(&ds, &ses).await;
			let _guard =
				inject_retryable_conflicts(SITE, ds.id(), STATEMENT_CONFLICT_ATTEMPTS as usize);
			let vars = surrealdb_types::vars! { id: id.clone() };
			let res = ds.execute("KILL $id", &ses, Some(vars)).await.expect("KILL should run");
			let err = res[0].result.as_ref().expect_err("KILL should report the conflict");
			assert_eq!(
				err.query_details(),
				Some(&surrealdb_types::QueryError::TransactionConflict),
				"the last conflict is returned when the budget runs out: {err}"
			);
			assert!(
				retryable_conflict_count(SITE, ds.id()) > 0,
				"the budget ended the attempts before their bound"
			);
			assert_eq!(lives(&ds, &ses).await, vec![id_of(&id)]);
		}

		#[tokio::test]
		async fn a_slow_arrival_spends_none_of_the_transaction_timeout() {
			let (ds, _recv, ses) = new_ds().await;
			let ds = Arc::into_inner(ds)
				.expect("nothing else holds the datastore")
				.with_transaction_timeout(Some(Duration::from_millis(20)));
			// Stalls between the arrival and the opening of the write
			// transaction for longer than the whole timeout.
			let hook: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
				Box::pin(tokio::time::sleep(Duration::from_millis(50)));
			super::super::AFTER_LIVE_ARRIVAL.with(|slot| *slot.borrow_mut() = Some(hook));
			let id = live(&ds, &ses).await;
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
			let _guard = inject_retryable_conflicts(SITE, ds.id(), 1);
			let vars = surrealdb_types::vars! { id: id.clone() };
			let res = ds
				.execute("BEGIN; KILL $id; COMMIT;", &ses, Some(vars))
				.await
				.expect("the block should run");
			assert_eq!(retryable_conflict_count(SITE, ds.id()), 0, "the block's commit lost");
			let [_begin, kill, commit] = res.as_slice() else {
				panic!("the block answers one row per statement: {res:?}");
			};
			assert!(kill.result.is_err(), "the KILL is reported not executed: {res:?}");
			assert_eq!(
				commit.result.as_ref().err().and_then(|e| e.query_details()),
				Some(&surrealdb_types::QueryError::TransactionConflict),
				"the lost commit is reported, not run again: {res:?}"
			);
			assert_eq!(lives(&ds, &ses).await, vec![id_of(&id)], "the KILL did not apply");
		}
	}

	fn err(text: &'static str, kind: Option<QueryError>) -> Option<SlotValue> {
		Some(SlotValue::Error {
			text: Cow::Borrowed(text),
			kind,
		})
	}

	fn owned_err(text: String, kind: Option<QueryError>) -> Option<SlotValue> {
		Some(SlotValue::Error {
			text: Cow::Owned(text),
			kind,
		})
	}

	const CANCELLED: &str = "The query was not executed due to a cancelled transaction";
	const FAILED: &str = "The query was not executed due to a failed transaction";

	/// Every row of the block-end table: what the recorded slots, the statements after the
	/// failure point and the terminator answer.
	#[test]
	fn finalise_slots_answers_every_block_end() {
		let timed_out =
			Reason::Timedout(crate::val::Duration::from(std::time::Duration::from_secs(3)));
		let timed_out_kind = Some(QueryError::TimedOut {
			duration: std::time::Duration::from_secs(3),
		});
		let timed_out_text =
			format!("Timed out: {}", crate::val::Duration::from(std::time::Duration::from_secs(3)));
		let timed_out_commit = format!(
			"Cannot COMMIT: timed out ({})",
			crate::val::Duration::from(std::time::Duration::from_secs(3))
		);
		let rows: Vec<(&str, BlockEnd, SlotRewrite)> = vec![
			(
				"committed",
				BlockEnd::Committed,
				SlotRewrite {
					recorded: None,
					following: None,
					terminator: Some(SlotValue::None),
					interrupted_statement: None,
				},
			),
			(
				"healthy cancel",
				BlockEnd::Cancelled,
				SlotRewrite {
					recorded: err(CANCELLED, Some(QueryError::Cancelled)),
					following: None,
					terminator: Some(SlotValue::None),
					interrupted_statement: None,
				},
			),
			(
				"failed statement, COMMIT",
				BlockEnd::Failed {
					cause: FailCause::Error,
					end: FailedEnd::Commit,
				},
				SlotRewrite {
					recorded: err(FAILED, Some(QueryError::NotExecuted)),
					following: err(CANCELLED, Some(QueryError::Cancelled)),
					terminator: err(
						"Cannot COMMIT: the transaction was aborted due to a prior error",
						Some(QueryError::NotExecuted),
					),
					interrupted_statement: None,
				},
			),
			(
				"failed statement, CANCEL",
				BlockEnd::Failed {
					cause: FailCause::Error,
					end: FailedEnd::Cancel,
				},
				SlotRewrite {
					recorded: err(FAILED, Some(QueryError::NotExecuted)),
					following: err(CANCELLED, Some(QueryError::Cancelled)),
					terminator: Some(SlotValue::None),
					interrupted_statement: None,
				},
			),
			(
				"failed statement, request end",
				BlockEnd::Failed {
					cause: FailCause::Error,
					end: FailedEnd::RequestEnd,
				},
				SlotRewrite {
					recorded: err(FAILED, Some(QueryError::NotExecuted)),
					following: err(CANCELLED, Some(QueryError::Cancelled)),
					terminator: None,
					interrupted_statement: None,
				},
			),
			(
				"nested BEGIN, COMMIT",
				BlockEnd::Failed {
					cause: FailCause::NestedBegin,
					end: FailedEnd::Commit,
				},
				SlotRewrite {
					recorded: err(FAILED, Some(QueryError::NotExecuted)),
					following: err(FAILED, Some(QueryError::NotExecuted)),
					terminator: err(
						"Cannot COMMIT: the transaction was aborted due to a nested BEGIN",
						Some(QueryError::NotExecuted),
					),
					interrupted_statement: None,
				},
			),
			(
				"nested BEGIN, CANCEL",
				BlockEnd::Failed {
					cause: FailCause::NestedBegin,
					end: FailedEnd::Cancel,
				},
				SlotRewrite {
					recorded: err(FAILED, Some(QueryError::NotExecuted)),
					following: err(FAILED, Some(QueryError::NotExecuted)),
					terminator: err(FAILED, Some(QueryError::NotExecuted)),
					interrupted_statement: None,
				},
			),
			(
				"nested BEGIN, request end",
				BlockEnd::Failed {
					cause: FailCause::NestedBegin,
					end: FailedEnd::RequestEnd,
				},
				SlotRewrite {
					recorded: err(FAILED, Some(QueryError::NotExecuted)),
					following: err(FAILED, Some(QueryError::NotExecuted)),
					terminator: None,
					interrupted_statement: None,
				},
			),
			(
				"unterminated",
				BlockEnd::Unterminated,
				SlotRewrite {
					recorded: Some(SlotValue::Internal {
						text: Cow::Borrowed("Missing COMMIT statement"),
					}),
					following: None,
					terminator: None,
					interrupted_statement: None,
				},
			),
			(
				"commit failed, determinate, not a conflict",
				BlockEnd::CommitFailed {
					error: "boom".to_string(),
					indeterminate: false,
					conflict: false,
				},
				SlotRewrite {
					recorded: err(FAILED, Some(QueryError::NotExecuted)),
					following: None,
					terminator: owned_err(
						"Cannot COMMIT: boom".to_string(),
						Some(QueryError::NotExecuted),
					),
					interrupted_statement: None,
				},
			),
			(
				"commit failed, determinate, conflict",
				BlockEnd::CommitFailed {
					error: "boom".to_string(),
					indeterminate: false,
					conflict: true,
				},
				SlotRewrite {
					recorded: err(FAILED, Some(QueryError::NotExecuted)),
					following: None,
					terminator: owned_err(
						"Cannot COMMIT: boom".to_string(),
						Some(QueryError::TransactionConflict),
					),
					interrupted_statement: None,
				},
			),
			(
				"commit failed, indeterminate",
				BlockEnd::CommitFailed {
					error: "boom".to_string(),
					indeterminate: true,
					conflict: false,
				},
				SlotRewrite {
					recorded: err(
						"The transaction's commit outcome is unknown, so whether this query was \
						 applied is undetermined",
						None,
					),
					following: None,
					terminator: owned_err("Cannot COMMIT: boom".to_string(), None),
					interrupted_statement: None,
				},
			),
			(
				"timed out, COMMIT",
				BlockEnd::Interrupted {
					reason: timed_out,
					end: InterruptEnd::Commit,
				},
				SlotRewrite {
					recorded: err(CANCELLED, Some(QueryError::Cancelled)),
					following: owned_err(timed_out_text.clone(), timed_out_kind.clone()),
					terminator: owned_err(timed_out_commit, timed_out_kind.clone()),
					interrupted_statement: owned_err(
						timed_out_text.clone(),
						timed_out_kind.clone(),
					),
				},
			),
			(
				"timed out, CANCEL",
				BlockEnd::Interrupted {
					reason: timed_out,
					end: InterruptEnd::Cancel,
				},
				SlotRewrite {
					recorded: err(CANCELLED, Some(QueryError::Cancelled)),
					following: owned_err(timed_out_text.clone(), timed_out_kind.clone()),
					terminator: owned_err(timed_out_text.clone(), timed_out_kind.clone()),
					interrupted_statement: owned_err(
						timed_out_text.clone(),
						timed_out_kind.clone(),
					),
				},
			),
			(
				"timed out, request end",
				BlockEnd::Interrupted {
					reason: timed_out,
					end: InterruptEnd::RequestEnd,
				},
				SlotRewrite {
					recorded: err(CANCELLED, Some(QueryError::Cancelled)),
					following: owned_err(timed_out_text.clone(), timed_out_kind.clone()),
					terminator: None,
					interrupted_statement: owned_err(timed_out_text, timed_out_kind),
				},
			),
			(
				"cancelled, COMMIT",
				BlockEnd::Interrupted {
					reason: Reason::Canceled,
					end: InterruptEnd::Commit,
				},
				SlotRewrite {
					recorded: err(CANCELLED, Some(QueryError::Cancelled)),
					following: err(CANCELLED, Some(QueryError::Cancelled)),
					terminator: err(
						"Cannot COMMIT: the transaction was cancelled",
						Some(QueryError::Cancelled),
					),
					interrupted_statement: err(CANCELLED, Some(QueryError::Cancelled)),
				},
			),
			(
				"cancelled, CANCEL",
				BlockEnd::Interrupted {
					reason: Reason::Canceled,
					end: InterruptEnd::Cancel,
				},
				SlotRewrite {
					recorded: err(CANCELLED, Some(QueryError::Cancelled)),
					following: err(CANCELLED, Some(QueryError::Cancelled)),
					terminator: err(CANCELLED, Some(QueryError::Cancelled)),
					interrupted_statement: err(CANCELLED, Some(QueryError::Cancelled)),
				},
			),
			(
				"cancelled, request end",
				BlockEnd::Interrupted {
					reason: Reason::Canceled,
					end: InterruptEnd::RequestEnd,
				},
				SlotRewrite {
					recorded: err(CANCELLED, Some(QueryError::Cancelled)),
					following: err(CANCELLED, Some(QueryError::Cancelled)),
					terminator: None,
					interrupted_statement: err(CANCELLED, Some(QueryError::Cancelled)),
				},
			),
			(
				"memory threshold",
				BlockEnd::MemoryThreshold,
				SlotRewrite {
					recorded: err(CANCELLED, Some(QueryError::Cancelled)),
					following: None,
					terminator: None,
					interrupted_statement: None,
				},
			),
			(
				"begin failed, commit",
				BlockEnd::BeginFailed {
					end: FailedEnd::Commit,
				},
				SlotRewrite {
					recorded: None,
					following: err(
						"Tried to start a transaction while another transaction was open",
						Some(QueryError::NotExecuted),
					),
					terminator: err(
						"Cannot COMMIT: the transaction could not be started",
						Some(QueryError::NotExecuted),
					),
					interrupted_statement: None,
				},
			),
			(
				"begin failed, cancel",
				BlockEnd::BeginFailed {
					end: FailedEnd::Cancel,
				},
				SlotRewrite {
					recorded: None,
					following: err(
						"Tried to start a transaction while another transaction was open",
						Some(QueryError::NotExecuted),
					),
					terminator: Some(SlotValue::None),
					interrupted_statement: None,
				},
			),
			(
				"begin failed, request end",
				BlockEnd::BeginFailed {
					end: FailedEnd::RequestEnd,
				},
				SlotRewrite {
					recorded: None,
					following: err(
						"Tried to start a transaction while another transaction was open",
						Some(QueryError::NotExecuted),
					),
					terminator: None,
					interrupted_statement: None,
				},
			),
		];
		for (name, end, expected) in rows {
			assert_eq!(finalise_slots(&end), expected, "block end: {name}");
		}
	}

	/// The effects a program records are applied in the order they were recorded, to the options,
	/// the `$session` value and the request's parameters, and the cached session is dropped.
	#[tokio::test]
	async fn the_drain_applies_session_effects_in_record_order() {
		use surrealdb_strand::Strand;

		use crate::exec::context::SessionEffect;

		let (_ds, mut exec) = executor_in_memory().await;
		exec.cached_session = exec.get_session_info();
		assert!(exec.cached_session.is_some());
		exec.session_sink.record(SessionEffect::Option {
			name: Strand::new("IMPORT"),
			what: true,
		});
		exec.session_sink.record(SessionEffect::Use {
			ns: Some(Strand::new("other")),
			db: None,
		});
		exec.session_sink.record(SessionEffect::Use {
			ns: None,
			db: Some(Strand::new("db2")),
		});
		exec.session_sink.record(SessionEffect::Let {
			name: Strand::new("x"),
			value: Arc::new(Value::from(7_i64)),
		});
		exec.apply_session_effects(true).expect("the effects apply");
		assert!(exec.opt.import);
		assert_eq!(exec.opt.ns.as_deref(), Some("other"));
		assert_eq!(exec.opt.db.as_deref(), Some("db2"));
		assert!(exec.cached_session.is_none());
		assert_eq!(exec.ctx.value("x"), Some(&Value::from(7_i64)));
		assert!(exec.session_sink.take().is_empty());
	}

	/// A `USE` that fails after its namespace level keeps that level, as the statement run as
	/// written does: the program records a level only once its catalog work has succeeded.
	#[tokio::test]
	async fn a_use_keeps_the_namespace_level_when_its_database_level_fails() {
		use surrealdb_strand::Strand;

		use crate::exec::context::SessionEffect;

		let (_ds, mut exec) = executor_in_memory().await;
		exec.cached_session = exec.get_session_info();
		exec.session_sink.record(SessionEffect::Use {
			ns: Some(Strand::new("kept")),
			db: None,
		});
		exec.apply_session_effects(false).expect("the namespace level applies");
		assert_eq!(exec.opt.ns.as_deref(), Some("kept"));
		assert_eq!(exec.opt.db.as_deref(), Some("test"));
		// The statement failed, so the cached session information stays as it was.
		assert!(exec.cached_session.is_some());
	}

	/// A session `LET` the drain binds is remembered by an open block, which a `CANCEL` undoes and
	/// a `COMMIT` keeps, and a binding the carry bound refuses ends the stream.
	#[tokio::test]
	async fn a_drained_let_follows_the_undo_and_carry_rules_of_the_executor() {
		use surrealdb_strand::Strand;

		use crate::exec::context::SessionEffect;

		let (_ds, mut exec) = executor_in_memory().await;
		exec.let_undo.begin();
		exec.session_sink.record(SessionEffect::Let {
			name: Strand::new("a"),
			value: Arc::new(Value::from(1_i64)),
		});
		exec.apply_session_effects(true).expect("the binding applies");
		assert_eq!(exec.ctx.value("a"), Some(&Value::from(1_i64)));
		assert_eq!(exec.let_undo.log.len(), 1);
		exec.let_undo.rollback(super::context_mut(&mut exec.ctx).expect("a unique context"));
		assert_eq!(exec.ctx.value("a"), None);

		exec.carry = CarryBound {
			names: Some(0),
			bytes: None,
		};
		exec.session_sink.record(SessionEffect::Let {
			name: Strand::new("b"),
			value: Arc::new(Value::from(2_i64)),
		});
		assert!(exec.apply_session_effects(true).is_err());
		assert!(exec.carry_crossed);
		assert_eq!(exec.ctx.value("b"), None);
	}
}
