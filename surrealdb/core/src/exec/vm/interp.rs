//! The interpreter: a pipeline's body, run synchronously for one row.
//!
//! [`run_row`] executes a body from an instruction until the row is done, the
//! pipeline needs no more rows, the body needs a step only the host can take
//! — a read of storage, a call, a nested statement — or the fuel is spent at
//! a backward jump ([`super::fuel`]), and says which with a [`Step`]. Calling
//! it again at the instruction the step names resumes the row.
//!
//! What persists across rows — paging counts, the rows a sort was handed, an
//! aggregate's groups, the output — is in [`State`].

use std::cmp::Ordering;

use anyhow::bail;

pub(super) use super::groups::Groups;
use super::ir_exec::IrFlow;
use super::program::{Instr, KeyOrder, Operand, Pipeline, Program, ROW, Reg};
use super::row::RowReader;
use super::scalar::Scalar;
use crate::exec::field_path::FieldPathPart;
use crate::exec::function::{AggregateFunction, FunctionRegistry};
use crate::exec::operators::{compare_values, first_value_needed};
use crate::val::{Object, Value};

/// The most top-level fields a projection writes straight into an output
/// frame; a wider one builds its object.
const FLAT_FIELDS: usize = 16;

/// How a row's run of a body ended.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Step {
	/// The row is done; the pipeline moves to its next.
	Next,
	/// The pipeline needs no more rows.
	Stop,
	/// Read the record the row names for the [`Instr::FetchRow`] at `at`.
	FetchRow {
		at: usize,
	},
	/// Run the [`Instr::Fetch`] at `at`, then resume the row after it.
	Fetch {
		at: usize,
	},
	/// Land the columns [`State::landing`] holds on the row, then resume the
	/// row at instruction `resume` with fresh registers.
	LandRow {
		resume: usize,
	},
	/// Replace the row with an object holding it under field `name` of the
	/// program, then resume the row at instruction `resume` with fresh
	/// registers.
	BindRow {
		name: u32,
		resume: usize,
	},
	/// Take projection-writing instruction `at`, then resume after it.
	WriteProjection {
		at: usize,
	},
	/// Take omit instruction `at`, then resume after it.
	Omit {
		at: usize,
	},
	/// Take access instruction `at` over a record id, array or geometry,
	/// then resume after it.
	Access {
		at: usize,
	},
	/// Run the IR instruction at `at` on the host, which answers where the row
	/// resumes.
	Ir {
		at: usize,
	},
	/// Take each instruction `at` over a record id or an array holding one,
	/// then resume after it.
	Each {
		at: usize,
	},
	/// Check builtin `call` is allowed, then resume at `resume`.
	CallAllowed {
		call: u32,
		frame: Option<u16>,
		resume: usize,
	},
	/// Take call instruction `at`, then resume after it.
	Call {
		at: usize,
	},
	/// Take method or method-closure instruction `at`, then resume after it.
	Method {
		at: usize,
	},
	/// Take subquery instruction `at`, then resume after it.
	Subquery {
		at: usize,
	},
	/// Take lookup instruction `at`, then resume after it.
	Lookup {
		at: usize,
	},
	/// Signal `BREAK` or `CONTINUE` from signal instruction `at`.
	Signal {
		at: usize,
	},
	/// Take recursion instruction `at`, then resume after it.
	Recurse {
		at: usize,
	},
	/// Take destructure instruction `at`, then resume after it.
	Destructure {
		at: usize,
	},
	/// Take judge instruction `at`, then resume after it.
	JudgeCopy {
		at: usize,
	},
	/// Take closure instruction `at` — making a closure value, or invoking
	/// one — then resume after it.
	Closure {
		at: usize,
	},
	/// Link a unit into the run for each closure value instruction `at`
	/// hands a callee and the state has not linked one for — faulting to the
	/// host for one no table holds — then take `at` again.
	NeedUnit {
		at: usize,
	},
	/// Take function-entry or function-call instruction `at`, then resume
	/// after it.
	Function {
		at: usize,
	},
	/// Read parameter `param` into register `dst`, then resume the row at
	/// instruction `resume`.
	Param {
		param: u32,
		dst: Reg,
		frame: Option<u16>,
		resume: usize,
	},
	/// Service store instruction `at` over the row's registers; the host
	/// decides where the row resumes.
	/// Take block instruction `at`, then resume where it says.
	Block {
		at: usize,
	},
	Store {
		at: usize,
	},
	/// Service write instruction `at`, which decides where the row resumes,
	/// with fresh registers.
	Write {
		at: usize,
	},
	/// Take index-function instruction `at`, then resume after it.
	IndexFn {
		at: usize,
	},
	/// Take `MATCHES` instruction `at`, then resume after it.
	Matches {
		at: usize,
	},
	/// Take external-call instruction `at` — entering a call of code the
	/// host runs, or making it — then resume after it.
	External {
		at: usize,
	},
	/// Take degree instruction `at`, then resume where it says.
	Degree {
		at: usize,
	},
	/// Evaluate the query vector of brute-force KNN ranking instruction `at`,
	/// computed when the statement runs, which no row of the run has been
	/// ranked against yet; then take `at` again.
	KnnQuery {
		at: usize,
	},
	/// The fuel is spent: reach a yield point
	/// ([`super::fuel::yield_point`]), then resume the row at instruction
	/// `resume` with the same registers. Every driver of a body matches it:
	/// a row abandoned here would lose the work done on it so far.
	Yield {
		resume: usize,
	},
}

/// What the body left for the host to do with the row once it is done.
#[derive(Debug)]
pub(super) enum Pending {
	/// Hand the row itself to the output.
	Emit,
	/// Hand the row to sort `sort` with these keys.
	Sort {
		sort: u16,
		keys: Vec<Value>,
	},
	/// Hand `value`, or the row itself when none, to split `split`; the host
	/// runs the rows it splits into (`host::run_value_row`), since each is a
	/// row of another pipeline.
	Split {
		split: u16,
		value: Option<Value>,
	},
	/// Hand the row itself to join `join` as a probe row, with its key; the
	/// host pairs it with the build rows and runs the pairings
	/// (`host::run_join`), since each is a row of another pipeline.
	Join {
		join: u16,
		keys: Vec<Value>,
	},
	/// Hand the row itself to join `join`, which holds its left row, as a row
	/// of its right input; the host pairs it with the held row and runs the
	/// pairing (`host::run_join_pair`), since it is a row of another pipeline.
	JoinPair {
		join: u16,
	},
}

#[cfg(test)]
thread_local! {
	/// How many verdicts this thread's runs have judged on the conjuncts
	/// filters judge once ([`Instr::VerdictSet`]).
	pub(super) static VERDICTS_JUDGED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// What a join that holds its left input's one row keeps while its right
/// input streams past it ([`super::program::JoinSpec::streams`]).
pub(super) struct HeldJoin {
	/// The left row.
	pub(super) probe: Value,
	/// Its key.
	pub(super) keys: Vec<Value>,
	/// Whether a right row has been paired with it.
	pub(super) matched: bool,
	/// Whether the join's pipeline took no more rows.
	pub(super) stopped: bool,
	/// The right rows whose key holds a regex that `=` matches the left key
	/// with, paired once the right input ends, as the build table pairs them
	/// after the rows the left key equals by ordering.
	pub(super) regex: Vec<Value>,
	/// Where the left key holds a regex, the right rows keyed, paired once the
	/// right input ends as the build table pairs them.
	pub(super) table: Option<crate::exec::operators::BuildTable>,
	/// The state the right rows' keys and the residual are evaluated on.
	pub(super) eval: Option<PooledState>,
}

/// A paging clause's counts across the rows of its pipeline.
#[derive(Debug, Clone, Copy)]
pub(super) struct Paging {
	/// Rows `START` still skips.
	pub(super) skip: usize,
	/// Rows `LIMIT` still admits; `None` without a limit.
	pub(super) remaining: Option<usize>,
}

/// A running program's state across rows and pipelines.
/// What a run has learned of a user-defined function it calls, so that a call
/// after the first neither hashes the definition nor the principal again.
#[derive(Default)]
pub(super) struct FunctionMemo {
	/// The definition an entry found to hash as the program expects.
	pub(super) definition: Option<std::sync::Arc<crate::catalog::FunctionDefinition>>,
	/// The principal an entry ran the body under for the caller's auth it
	/// was derived from, for the definition held above.
	pub(super) principal: Option<super::function::Narrowed>,
	/// The definition an entry found to hash otherwise than the program
	/// expects, which the call runs through the function's call unit.
	pub(super) redefined: Option<std::sync::Arc<crate::catalog::FunctionDefinition>>,
	/// The call unit the site's last call of a redefined function linked
	/// ([`super::call_unit::Linked`]).
	pub(super) linked: Option<super::call_unit::Linked>,
}

/// A region the row's body is in ([`State::tries`]).
#[derive(Clone, Copy, Debug)]
pub(super) enum Region {
	/// An [`Instr::TryNone`] region: where it continues when it catches an
	/// error, and the register it sets to `NONE`.
	Try {
		catch: usize,
		dst: Option<Reg>,
	},
	/// An [`Instr::MethodRegion`]: the method whose body runs in it.
	Method {
		method: u32,
	},
	/// An [`Instr::PermissionRegion`]: the check it names.
	Permission {
		check: super::program::PermissionCheck,
	},
}

/// What a nested statement run once keeps across its evaluations.
pub(super) enum Kept {
	/// The value the statement took.
	Value(Value),
	/// The rows the statement took, held for membership tests.
	Members(Box<super::membership::Members>),
}

pub(crate) struct State {
	pub(super) paging: Vec<Paging>,
	pub(super) sorts: Vec<Vec<(Vec<Value>, SortRow)>>,
	/// For each sort that keeps a bounded number of rows, how it keeps them:
	/// whether it prunes or merges, and a merging sort's rows and merge
	/// ([`Self::keep_sorted`]).
	pub(super) sort_arrivals: Vec<Arrivals>,
	/// Where a bulk instruction's copy of a collection stopped when the fuel
	/// ran out, for the row to resume it ([`copied`]).
	pub(super) bulk: Option<Bulk>,
	/// Key vectors and stored-record buffers of rows a bounded sort pruned,
	/// reused by the rows that reach a sort next.
	pub(super) spare_sort_keys: Vec<Vec<Value>>,
	pub(super) spare_sort_bytes: Vec<Vec<u8>>,
	/// For each sort, the stored bytes of the records it holds as
	/// [`SortRow::Slab`]s, back to back: an unbounded sort keeps every row
	/// it is handed, so its rows share one buffer rather than holding one
	/// each.
	pub(super) sort_slabs: Vec<Vec<u8>>,
	pub(super) topk_cells:
		Vec<Option<std::sync::Arc<crate::exec::topk_pushdown::TopKThresholdCell>>>,
	/// For each sort of a brute-force KNN whose query vector is computed when
	/// the statement runs, the cell its vector is evaluated into.
	pub(super) knn_queries: Vec<KnnQueryCell>,
	/// For each query vector a KNN search of the run computes
	/// ([`Source::Knn`](super::program::Source)), the cell it is evaluated
	/// into, found by the identity of the query the sources share.
	pub(super) knn_searches: Vec<(std::sync::Arc<super::program::KnnQuery>, KnnQueryCell)>,
	/// For each sort that may spill, the rows it holds before it writes them
	/// out as a run whatever its query's memory (a sort under `TEMPFILES`),
	/// and its order.
	pub(super) sort_spill: Vec<Option<super::sort::SortSpill>>,
	/// The runs each sort has spilled ([`super::spill`]).
	pub(super) sort_runs: Vec<Option<super::spill::Runs>>,
	/// What each sort holds, against its query's lease; unbounded for a
	/// brute-force KNN's sort, which never spills, and for a keep sort of a
	/// query with no temporary store, which prunes in memory.
	pub(super) sort_accounts: Vec<crate::exec::memory::Reservation>,
	/// The runs each aggregate's groups have spilled, as saved groups in key
	/// order ([`Groups::save_all`]).
	pub(super) group_runs: Vec<Option<super::spill::Runs>>,
	/// What each aggregate grouped by a key holds, against its query's lease.
	pub(super) group_accounts: Vec<crate::exec::memory::Reservation>,
	/// What the rows a statement gathers into its value hold, against its
	/// query's lease, while it gathers them.
	pub(super) gather_account: Option<crate::exec::memory::Reservation>,
	/// What the values the program keeps hold, against its query's lease
	/// ([`super::ledger`]); made when the program first keeps a value with a
	/// heap.
	pub(super) values: Option<super::ledger::Ledger>,
	/// Whether the statement that runs next hands its gathered rows on as the
	/// value of a `LET`, which keeps their account in [`Self::handed`].
	pub(super) hand_on: bool,
	/// The account of the rows a `LET`'s statement gathered, until the binding
	/// that takes their value absorbs it: the bytes stay reserved as the value
	/// changes hands.
	pub(super) handed: Option<crate::exec::memory::Reservation>,
	/// The query's memory, where its breakers are bounded.
	pub(super) memory: Option<std::sync::Arc<crate::exec::memory::QueryMemory>>,
	/// Whether a breaker's account has passed its lease since the last
	/// flush, so the flush spills it.
	pub(super) spill_due: bool,
	pub(super) groups: Vec<Option<Groups>>,
	/// The [`FunctionRegistry::id`] of the registry the aggregate functions
	/// and method descriptors this state holds were resolved from, `0` for a
	/// state that holds none: a state started against another registry lets
	/// them go.
	registry: u64,
	/// The function each aggregate call names, by aggregate, field and call,
	/// resolved from the registry when the state is started for a program
	/// whose aggregates name other functions than those it holds.
	pub(super) aggregates: std::sync::Arc<ResolvedAggregates>,
	/// The regions the row is in, innermost last.
	pub(super) tries: Vec<Region>,
	pub(super) output: Vec<Value>,
	pub(super) pending: Option<Pending>,
	/// Whether the statement's permissions are not checked, so a permission
	/// the bodies apply admits every row.
	pub(super) permissions_bypassed: bool,
	/// Whether an error leaving a table's permission region in the program's
	/// own pipelines is reported as a record read by id reports it: under the
	/// context "Permission check failed", through which a typed error's own
	/// message shows. Otherwise the error is prefixed with the check that
	/// failed, as a scan reports it; a field's permission is prefixed either
	/// way, and the block statements the program runs
	/// ([`super::host::run_group`]) prefix their own.
	pub(super) read_in_place_permission_errors: bool,
	/// The context each of the program's permission predicates is evaluated
	/// in, built once for the execution.
	pub(super) frames: Vec<crate::exec::ExecutionContext>,
	/// The call frames open, by [`CALL_FRAME`](super::program::CALL_FRAME)
	/// offset: the function each has entered, until it is called.
	pub(super) calls: Vec<Option<crate::exec::vm::calls::EnteredCall>>,
	/// For each of the program's [`FunctionSpec`](super::program::FunctionSpec)s,
	/// what an entry of the run learned of it.
	pub(super) functions: Vec<FunctionMemo>,
	/// The descriptors of the tables the run's graph lookups source from,
	/// served while the catalog is unchanged ([`super::descriptor::TableMemo`]).
	pub(super) tables: super::descriptor::TableMemo,
	/// For each open call frame, the digest of the principal its body runs
	/// under ([`super::program::UnitFrame::digest`]).
	pub(super) call_principals: Vec<u64>,
	/// The record the computed frames compute, as [`Instr::BeginComputing`]
	/// last set it for the row.
	pub(super) computing: Option<std::sync::Arc<crate::exec::eval_context::ComputingRecord>>,
	/// What `$input` names while a write's data clause that binds it is
	/// evaluated: the value the statement supplied for the record. Every host
	/// step's evaluation reads it as its write binding.
	pub(super) write_input: Option<Value>,
	/// The cuts and the sets of the projection layer [`Instr::LandRow`] is
	/// landing, between the instruction and the host landing them.
	pub(super) landing: Option<(crate::val::Object, crate::val::Object)>,
	/// Whether the host pipeline running judges a write permission, whose
	/// reads are not permission-checked for the whole of its evaluation.
	pub(super) unchecked_reads: bool,
	/// Whether the pipeline running has no row, so an expression it
	/// evaluates reads no document.
	pub(super) rowless: bool,
	/// Whether the pipeline running has a row that is no document: a closure
	/// body invoked with a current value and no document root runs over the
	/// value, and an expression it evaluates reads the value as its current
	/// value and no document, as [`Instr::DocumentRoot`] does.
	pub(super) undocumented: bool,
	/// The document root of a closure body invoked from an element filter's
	/// body, whose row is the element: an index or a method argument the
	/// body evaluates reads it ([`Instr::DocumentRoot`]). `None` in any other
	/// pipeline, whose document root is its row.
	pub(super) root: Option<Value>,
	/// For a write statement, the record in flight and what the statement
	/// has written so far.
	pub(super) write: Option<Box<super::write::WriteState>>,
	/// A block's current frame, once it has bound a parameter: the context
	/// its statements and expressions run in.
	pub(super) frame: Option<crate::exec::ExecutionContext>,
	/// The frames a block saved, innermost last.
	pub(super) saved: Vec<Option<crate::exec::ExecutionContext>>,
	/// A block's `FOR` iterators, by slot.
	pub(super) iters: Vec<Option<super::block::ForIter>>,
	/// The keys of the sorted row the pipeline is running, which
	/// [`Instr::SortKey`] reads.
	pub(super) sorted_keys: Vec<Value>,
	/// The program running, for a block that runs its statements' groups
	/// over this state.
	pub(super) program: Option<std::sync::Arc<Program>>,
	/// The write state of each block statement compiled into the program
	/// that writes, kept across its reaches.
	pub(super) group_writes: Vec<Option<Box<super::write::WriteState>>>,
	/// The values a block has bound its `LET` and `FOR` names to, by slot.
	pub(super) slots: Vec<Value>,
	/// What each nested statement run once has taken, by
	/// [`Program::nested`](super::program::Program::nested) index, until the
	/// group it is evaluated in starts again.
	pub(super) once: Vec<Option<Kept>>,
	/// For each nested statement read through an index nested loop with a
	/// probe to fall back on, the evaluations made since the group that owns
	/// it started ([`super::program::NestedSpec::hashed`]).
	pub(super) lookups: Vec<u32>,
	/// For each join reading its right input through an index, the left rows
	/// it has looked up since the group that owns it started
	/// ([`super::program::JoinSpec::lookup`]).
	pub(super) join_lookups: Vec<u32>,
	/// The key a join hands the index lookup its right input reads through,
	/// while that input runs for one left row ([`super::program::Source::Lookup`]).
	pub(super) lookup_key: Option<Value>,
	/// The rows each probe has kept, by
	/// [`Program::probes`](super::program::Program::probes) index, until its
	/// owner starts again.
	pub(super) probes: Vec<Option<std::sync::Arc<super::nested::Probe>>>,
	/// What each binding-table step of the program keeps across the rows of
	/// the run, made when the step first runs.
	pub(super) steps: Vec<Option<Box<super::binding::StepRun>>>,
	/// The rows each join's build side holds, keyed, by
	/// [`Program::joins`](super::program::Program::joins) index, once the
	/// first probe row has reached the join and until its owner starts again.
	pub(super) joins: Vec<Option<super::join_spill::JoinBuilt>>,
	/// The left row each join that holds its left input holds, by
	/// [`Program::joins`](super::program::Program::joins) index, from when
	/// the left input hands it over until the right input has streamed past
	/// it ([`super::program::JoinSpec::streams`]).
	pub(super) held: Vec<Option<Box<HeldJoin>>>,
	/// The verdict each filter holds on the conjuncts it judges once, by
	/// [`Program::verdicts`](super::program::Program::verdicts) index, from the
	/// first row that reaches it until the group that owns it starts again.
	pub(super) verdicts: Vec<Option<bool>>,
	/// The rows each distinct set has passed, by
	/// [`Program::distincts`](super::program::Program::distincts) index.
	pub(super) seen: Vec<crate::exec::operators::SeenSet>,
	/// How many rows a distinct set holds before it fails the statement.
	pub(super) build_budget: usize,
	/// Whether the statement in `FROM` running is bound to its reader's zero
	/// limit, so its own reads of storage read no record.
	pub(super) zeroed: bool,
	/// How many records the reader of the run's rows takes before its paging
	/// is spent, where it says: the first read of storage the run opens takes
	/// no more than these from its cursor before it hands its rows on, as a
	/// read of a table named in the text stops at its statement's `LIMIT`.
	/// Taken by that read.
	pub(super) scan_hint: Option<u32>,
	/// The windows the run's dereferences of collections of record links read through, which
	/// its sites take for the length of a loop and give back.
	pub(super) link_pool: crate::exec::operators::link_window::LinkPool,
	/// The records of the links the rows of the batch being run hold, which a pipeline whose plan
	/// states [`Pipeline::rows_ahead`](super::program::Pipeline::rows_ahead) read before the rows
	/// ran. Replaced by the next batch's, and emptied when the scan that filled it ends, however
	/// it ends: a `Fetch` of any pipeline of the state reads it by key, so it holds bytes only
	/// while the scan that filled it runs.
	pub(super) link_ahead: Option<crate::exec::operators::link_window::RowsWindow>,
	/// What the inline caches and adjacency keys of the sources of the hop a lookup is reading
	/// held when it filled its window, which the hop's reads consume. Emptied when the hop has
	/// read its sources.
	pub(super) frontier_hop: super::graph::Frontier,
	/// What the inline caches and adjacency keys of the origins of one group of an array a lookup
	/// answers held when it filled its window, which each origin's first hop consumes. Emptied
	/// when the group has been answered.
	pub(super) frontier_origins: super::graph::Frontier,
	/// What the inline caches and adjacency keys of the sources of the batch of rows being run
	/// held when the batch's window was filled, which every lookup of the batch consumes.
	/// Replaced by the next batch's, and emptied when the scan that filled it ends, however it
	/// ends: a lookup of any pipeline of the state reads it by key, so it holds bytes only while
	/// the scan that filled it runs.
	pub(super) frontier_rows: super::graph::Frontier,
	/// The records each level of a path reached while the batch's window was filled: the sources
	/// of the next level. Empty between fills; it holds the buffers of the levels.
	pub(super) frontier_levels: super::graph::frontier::Levels,
	/// The paging clause a morsel counts its rows against, which the morsel
	/// leaves to its merge: `None` for a state that runs no such morsel.
	pub(super) batch_paging: Option<u16>,
	/// Whether a row has executed the paging instruction of
	/// [`batch_paging`](Self::batch_paging) since the flag was last cleared.
	/// A statement nested in the row's own expressions pages the rows it reads
	/// by clauses of its own, which say nothing of whether the row is taken,
	/// so no instruction of another clause sets it.
	pub(super) reached_paging: bool,
	/// Whether the row that passes a distinct set leaves its value in
	/// [`Self::distinct_taken`], for a morsel whose merge keeps the first
	/// occurrence of each row among its morsels'.
	pub(super) capture_distinct: bool,
	/// The row a distinct set took since the field was last cleared, while
	/// [`Self::capture_distinct`] holds.
	pub(super) distinct_taken: Option<Value>,
	/// Which of [`Program::reservations`] a draw has taken: each is taken
	/// once, by the first run of the draw that reserved it.
	pub(super) reserved_taken: Vec<bool>,
	/// The root program's units, for a state that runs a unit, whose own
	/// program holds none: the table the ids it holds index.
	pub(super) units: Option<std::sync::Arc<[std::sync::Arc<Program>]>>,
	/// The descriptor each of the program's [`MethodSpec`](super::program::MethodSpec)s
	/// names, by index, resolved from the registry on the method's first call
	/// in the run and not again per row.
	pub(super) methods: Vec<Option<std::sync::Arc<crate::exec::function::MethodDescriptor>>>,
	/// The function and index context each of the program's index-function
	/// [`CallSpec`](super::program::CallSpec)s resolved to, by index, on the
	/// call's first use in the run.
	pub(super) index_runs: Vec<Option<IndexRun>>,
	/// The distances each statement's `KNN` scan has published, by
	/// [`Source::Knn::slot`](super::program::Source::Knn).
	pub(super) knn_contexts: Vec<Option<std::sync::Arc<crate::exec::function::KnnContext>>>,
	/// The decision tree each of the program's `MATCHES` operators runs, by
	/// [`Program::matches`](super::program::Program::matches) index, with the
	/// index resolutions it has cached.
	pub(super) matches_evals: Vec<Option<std::sync::Arc<crate::exec::vm::matches::MatchesEval>>>,
	/// The scope a schema statement's backfill of a view maintains the view
	/// table's rows through, opened by the backfill's first row so that the
	/// view table's write context is resolved once for the run.
	pub(super) backfill: Option<std::sync::Arc<crate::exec::operators::write::scope::WriteScope>>,
	/// The order of the view table a backfill fills, taken when the backfill
	/// met it: the table, and its order.
	pub(super) backfill_order: Option<(surrealdb_strand::TableName, std::sync::Arc<Program>)>,
	/// The format a backfill writes the rows of the view table it fills in: the table, and
	/// the slot that answers for it while the transaction's layout epoch
	/// ([`crate::kvs::Transaction::layout_epoch`]) stands.
	pub(super) backfill_format:
		Option<(surrealdb_strand::TableName, crate::exec::operators::write::ctx::RecordFormatSlot)>,
	/// The calls of code the host runs that [`Instr::ExternalEnter`] has
	/// entered and [`Instr::ExternalCall`] has yet to make, by
	/// [`Program::externals`](super::program::Program::externals) index.
	pub(super) externals: Vec<Option<super::external::Entered>>,
	/// For each external call of a user-defined function, by the same index,
	/// the call unit its last call linked ([`super::call_unit::Linked`]).
	pub(super) call_units: Vec<Option<super::call_unit::Linked>>,
	/// For a unit that is a `.@` recursion's body, the phase the recursion
	/// runs it in, which its `.@` markers answer in.
	pub(super) recursion: Option<crate::exec::eval_context::RecursionCtx>,
	/// While a lookup judges a table permission that reads `$parent`: the
	/// row the lookup is taken over, bound as `$parent`.
	pub(super) parent: Option<std::collections::HashMap<surrealdb_strand::Strand, Value>>,
	/// The degree fast path of each of the program's
	/// [`DegreeSpec`](super::program::DegreeSpec)s, by index, made on its
	/// first use in the run.
	pub(super) degrees: Vec<Option<std::sync::Arc<crate::exec::vm::degree::DegreeCounter>>>,
	/// The closures the state has linked a unit for, by the frame an
	/// instruction invokes them in and their digest, most recent last: an
	/// instruction handing a callee a closure not held here yields
	/// [`Step::NeedUnit`] before it is taken. Holds at most
	/// [`super::closure::LINKED`].
	pub(super) linked: Vec<(Option<u16>, super::program::ClosureDigest)>,
	/// For a statement whose rows leave encoded for the wire, the frame they
	/// are written into, in output order: a stored record emitted as it lies
	/// is transcoded from its bytes, and [`Self::output`] is drained into the
	/// frame ahead of it ([`Self::framed_output`]).
	pub(super) row_frame: Option<super::row_frame::RowFrame>,
	/// The statement's registration with the node's places, made by the first
	/// morsel set that wants a place and shared by every set of the run, so
	/// that the statement holds no more places than the policy's cap whatever
	/// the number of its scans. The state of a morsel, which runs a body over
	/// the rows it is handed and reads no source, starts with none and makes
	/// none.
	pub(super) places: Option<super::morsel::Statement>,
}

/// The query vector of a brute-force KNN sort, or of a KNN search, that is
/// computed when the statement runs: empty until the first row of the run is
/// ranked against it ([`Step::KnnQuery`]), or the first source searching with
/// it opens, which evaluates it once. A statement's morsels share its cell
/// ([`RunShare`]), so the morsel that ranks the first row evaluates it for
/// all of them, and a morsel whose rows all fail a condition beneath the
/// ranking evaluates nothing.
///
/// The cell holds the evaluation's outcome, a failure included, so a vector
/// whose evaluation fails is never evaluated again in the run: every reader
/// that waited on it, or comes after it, raises the same failure.
pub(super) type KnnQueryCell = std::sync::Arc<tokio::sync::OnceCell<KnnQueryOutcome>>;

/// What the one evaluation of a KNN query vector left in its [`KnnQueryCell`].
#[derive(Debug, Clone)]
pub(super) enum KnnQueryOutcome {
	/// The vector.
	Vector(std::sync::Arc<[crate::val::Number]>),
	/// The evaluation failed: the error as it reaches the caller of the query
	/// ([`crate::err::anyhow_to_types_error`]), which every reader raises.
	Failed(surrealdb_types::Error),
	/// The evaluation signalled `BREAK`, `CONTINUE` or `RETURN`.
	Break,
	Continue,
	Return(Value),
}

impl KnnQueryOutcome {
	/// The outcome `evaluated` leaves for every reader of the cell.
	pub(super) fn of(
		evaluated: crate::expr::FlowResult<std::sync::Arc<[crate::val::Number]>>,
	) -> Self {
		match evaluated {
			Ok(vector) => Self::Vector(vector),
			Err(crate::expr::ControlFlow::Err(error)) => {
				Self::Failed(crate::err::anyhow_to_types_error(error))
			}
			Err(crate::expr::ControlFlow::Break) => Self::Break,
			Err(crate::expr::ControlFlow::Continue) => Self::Continue,
			Err(crate::expr::ControlFlow::Return(value)) => Self::Return(value),
		}
	}

	/// The vector, where the evaluation computed one.
	pub(super) fn vector(&self) -> Option<&std::sync::Arc<[crate::val::Number]>> {
		match self {
			Self::Vector(vector) => Some(vector),
			Self::Failed(_) | Self::Break | Self::Continue | Self::Return(_) => None,
		}
	}

	/// The outcome as a reader raises it.
	pub(super) fn read(&self) -> crate::expr::FlowResult<std::sync::Arc<[crate::val::Number]>> {
		match self {
			Self::Vector(vector) => Ok(std::sync::Arc::clone(vector)),
			Self::Failed(error) => {
				Err(crate::expr::ControlFlow::Err(anyhow::Error::new(error.clone())))
			}
			Self::Break => Err(crate::expr::ControlFlow::Break),
			Self::Continue => Err(crate::expr::ControlFlow::Continue),
			Self::Return(value) => Err(crate::expr::ControlFlow::Return(value.clone())),
		}
	}
}

/// What the morsels of a statement share with the statement: its
/// aggregate functions, the distances its scan published, the query vectors
/// of its brute-force searches, and the resolutions of the index functions and `MATCHES` operators
/// it holds, so that a morsel resolves nothing against the registry.
#[derive(Clone)]
pub(super) struct RunShare {
	registry: u64,
	aggregates: std::sync::Arc<ResolvedAggregates>,
	knn_contexts: Vec<Option<std::sync::Arc<crate::exec::function::KnnContext>>>,
	knn_queries: Vec<KnnQueryCell>,
	index_runs: Vec<Option<IndexRun>>,
	matches_evals: Vec<Option<std::sync::Arc<crate::exec::vm::matches::MatchesEval>>>,
	/// The threshold cell of each sort the run's scan pushes a top-k down for: the sorts of the
	/// morsels publish into the statement's cell, so that every morsel's scan rejects by the
	/// tightest threshold any of them holds.
	topk_cells: Vec<Option<std::sync::Arc<crate::exec::topk_pushdown::TopKThresholdCell>>>,
}

/// The aggregate functions a program's aggregates name, with the names they
/// were resolved from: a state started against the registry it holds them
/// from ([`State::registry`]), for a program whose aggregates name the same
/// functions, takes them as they are.
#[derive(Default)]
pub(super) struct ResolvedAggregates {
	/// The id each call names, by aggregate, field and call.
	ids: Vec<Vec<Vec<crate::exec::operators::AggregateId>>>,
	pub(super) functions: AggFunctions,
}

impl ResolvedAggregates {
	/// Resolve the functions the aggregates of `program` name against
	/// `registry`.
	fn resolve(program: &Program, registry: &FunctionRegistry) -> anyhow::Result<Self> {
		#[cfg(test)]
		RESOLUTIONS.with(|resolved| resolved.set(resolved.get() + 1));
		Ok(Self {
			ids: program
				.aggregates
				.iter()
				.map(|sink| {
					sink.fields.iter().map(|field| field_ids(field).cloned().collect()).collect()
				})
				.collect(),
			functions: resolve_aggregates(program, registry)?,
		})
	}

	/// Whether these are the functions the aggregates of `program` name, in
	/// the registry they were resolved from.
	fn answers(&self, program: &Program) -> bool {
		self.ids.len() == program.aggregates.len()
			&& self.ids.iter().zip(&program.aggregates).all(|(ids, sink)| {
				ids.len() == sink.fields.len()
					&& ids
						.iter()
						.zip(&sink.fields)
						.all(|(ids, field)| ids.iter().eq(field_ids(field)))
			})
	}
}

#[cfg(test)]
thread_local! {
	/// How many times this thread has resolved a program's aggregates, for
	/// tests that must know a run resolves them once.
	pub(super) static RESOLUTIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The resolution of no aggregate, which a state holds until it resolves
/// some.
static NO_AGGREGATES: std::sync::LazyLock<std::sync::Arc<ResolvedAggregates>> =
	std::sync::LazyLock::new(std::sync::Arc::default);

/// The ids the calls of aggregate field `field` name, in order.
fn field_ids(
	field: &super::program::AggSpec,
) -> impl Iterator<Item = &crate::exec::operators::AggregateId> {
	field.aggregate.iter().flat_map(|aggregate| aggregate.calls.iter()).map(|call| &call.name)
}

/// How many spare states a thread keeps ([`SPARE`]).
const SPARE_STATES: usize = 16;

/// A spare state, holding nothing but the allocations an earlier run left
/// it.
struct SpareState(Box<State>);

thread_local! {
	/// The states this thread has ready for a run: a run takes one rather
	/// than building a state, and gives it back when it is done
	/// ([`PooledState`]).
	static SPARE: std::cell::RefCell<Vec<SpareState>> =
		const { std::cell::RefCell::new(Vec::new()) };
}

/// The most elements a collection of a state given back keeps room for; one
/// that grew past it is let go.
const KEPT_CAPACITY: usize = 256;

/// What a run holds and gives back to the thread's spare ones when it is
/// done ([`Pooled`]).
pub(super) trait Spare: Sized {
	/// One holding nothing, the thread's spare one when it has one.
	fn take() -> Box<Self>;
	/// Let go of everything `this` holds and keep it as a spare.
	fn give(this: Box<Self>);
}

impl Spare for State {
	fn take() -> Box<Self> {
		SPARE
			.with(|spare| spare.borrow_mut().pop())
			.map_or_else(|| Box::new(State::vacant()), |spare| spare.0)
	}

	fn give(mut this: Box<Self>) {
		this.clear();
		// A thread being torn down keeps nothing.
		let _ = SPARE.try_with(|spare| {
			let mut spare = spare.borrow_mut();
			if spare.len() < SPARE_STATES {
				spare.push(SpareState(this));
			}
		});
	}
}

/// A `T` a run holds, taken from the thread's spare ones and given back to
/// them, holding nothing of the run, when it is dropped.
pub(super) struct Pooled<T: Spare>(Option<Box<T>>);

/// A state a run holds ([`Pooled`]).
pub(super) type PooledState = Pooled<State>;

impl<T: Spare> Pooled<T> {
	fn take() -> Self {
		Self(Some(T::take()))
	}
}

impl<T: Spare> std::ops::Deref for Pooled<T> {
	type Target = T;
	fn deref(&self) -> &T {
		self.0.as_deref().expect("a pooled value is held until it is dropped")
	}
}

impl<T: Spare> std::ops::DerefMut for Pooled<T> {
	fn deref_mut(&mut self) -> &mut T {
		self.0.as_deref_mut().expect("a pooled value is held until it is dropped")
	}
}

impl<T: Spare> Drop for Pooled<T> {
	fn drop(&mut self) {
		if let Some(held) = self.0.take() {
			T::give(held);
		}
	}
}

/// Empty `items`, letting its allocation go when it grew past
/// [`KEPT_CAPACITY`].
fn emptied<T>(items: &mut Vec<T>) {
	if items.capacity() > KEPT_CAPACITY {
		*items = Vec::new();
	} else {
		items.clear();
	}
}

/// An index function resolved for a run, with what it reports against.
#[derive(Clone)]
pub(super) struct IndexRun {
	pub(super) function: std::sync::Arc<dyn crate::exec::function::IndexFunction>,
	pub(super) context: crate::exec::function::IndexContext,
	/// The query read when the call ran that the context was resolved for;
	/// `None` for a context resolved for its clause's planned query.
	pub(super) query: Option<String>,
}

/// The query index function call `spec` reads when it runs, taken from the
/// end of `args`, where the call's clause reads one
/// ([`super::program::IndexBinding::FullText::query_arg`]). A value that is no
/// string names no clause the call can report against and registers no query.
pub(super) fn runtime_query(
	spec: &super::program::CallSpec,
	args: &mut Vec<Value>,
) -> anyhow::Result<Option<String>> {
	let Some(super::program::IndexBinding::FullText {
		query_arg: true,
		..
	}) = &spec.index
	else {
		return Ok(None);
	};
	match args.pop() {
		Some(Value::String(query)) => Ok(Some(query.as_str().to_owned())),
		_ => Err(anyhow::Error::new(crate::exec::Error::Query {
			message: format!(
				"Index function '{}': no MATCHES clause found in WHERE condition",
				spec.name
			),
		})),
	}
}

/// The function each call of each field of each aggregate names.
pub(super) type AggFunctions = Vec<Vec<Vec<std::sync::Arc<dyn AggregateFunction>>>>;

/// Resolve the functions the aggregates of `program` name against `registry`:
/// the one place a run looks an aggregate up, so folding a row never does.
fn resolve_aggregates(
	program: &Program,
	registry: &FunctionRegistry,
) -> anyhow::Result<AggFunctions> {
	program
		.aggregates
		.iter()
		.map(|sink| {
			sink.fields
				.iter()
				.map(|field| {
					field
						.aggregate
						.iter()
						.flat_map(|aggregate| aggregate.calls.iter())
						.map(|call| {
							call.name.resolve(registry).ok_or_else(|| {
								anyhow::anyhow!(
									"the aggregate function {} is not registered",
									call.name.name()
								)
							})
						})
						.collect()
				})
				.collect()
		})
		.collect()
}

impl State {
	/// A state for a run of `program` with `paging`, taken from the thread's
	/// spare states. The functions its aggregates name are resolved from
	/// `registry` unless the spare state holds them already; the index
	/// functions and `MATCHES` operators it calls are resolved when first
	/// called.
	pub(super) fn new(
		program: &Program,
		paging: Vec<Paging>,
		registry: &FunctionRegistry,
	) -> anyhow::Result<PooledState> {
		let mut state = PooledState::take();
		state.resolving_from(registry.id());
		if !program.aggregates.is_empty() && !state.aggregates.answers(program) {
			state.aggregates = std::sync::Arc::new(ResolvedAggregates::resolve(program, registry)?);
		}
		state.start(program, paging);
		Ok(state)
	}

	/// A state for a batch of a run of `program` with `paging`, holding what
	/// the run shares with its batches ([`Self::shared`]): it resolves
	/// nothing against the registry.
	pub(super) fn sharing(program: &Program, paging: Vec<Paging>, shared: RunShare) -> PooledState {
		let mut state = PooledState::take();
		state.resolving_from(shared.registry);
		state.aggregates = shared.aggregates;
		state.knn_contexts = shared.knn_contexts;
		state.knn_queries = shared.knn_queries;
		state.index_runs = shared.index_runs;
		state.matches_evals = shared.matches_evals;
		state.topk_cells = shared.topk_cells;
		state.start(program, paging);
		state
	}

	/// Let go of the aggregate functions and method descriptors this state
	/// holds unless they were resolved from the registry whose
	/// [`FunctionRegistry::id`] is `registry`, from which it resolves them
	/// from now on.
	fn resolving_from(&mut self, registry: u64) {
		if self.registry != registry {
			self.registry = registry;
			self.aggregates = std::sync::Arc::clone(&NO_AGGREGATES);
			emptied(&mut self.methods);
		}
	}

	/// Make this state, which holds nothing of a run ([`Self::clear`]), the
	/// start of a run of `program` with `paging`, its aggregates' functions
	/// already resolved: a sort, an aggregate's groups, a block statement's
	/// write, a nested statement's value and a probe for each the program
	/// holds, none of them begun.
	fn start(&mut self, program: &Program, paging: Vec<Paging>) {
		let sorts = program.sorts.len();
		self.paging = paging;
		self.sorts.resize_with(sorts, Vec::new);
		self.sort_slabs.resize_with(sorts, Vec::new);
		self.sort_arrivals.resize(sorts, Arrivals::default());
		self.topk_cells.resize(sorts, None);
		self.knn_queries.resize_with(sorts, KnnQueryCell::default);
		self.sort_spill.resize(sorts, None);
		self.sort_runs.resize_with(sorts, || None);
		self.sort_accounts.resize_with(sorts, crate::exec::memory::Reservation::unbounded);
		self.group_runs.resize_with(program.aggregates.len(), || None);
		self.group_accounts
			.resize_with(program.aggregates.len(), crate::exec::memory::Reservation::unbounded);
		let functions = &self.aggregates.functions;
		self.groups.extend(
			program
				.aggregates
				.iter()
				.zip(functions)
				.map(|(sink, functions)| Some(fresh_groups(sink, functions))),
		);
		self.group_writes.resize_with(program.groups.len(), || None);
		self.once.resize_with(program.nested.len(), || None);
		self.lookups.resize(program.nested.len(), 0);
		self.probes.resize(program.probes.len(), None);
		self.steps.resize_with(program.splits.len(), || None);
		self.joins.resize_with(program.joins.len(), || None);
		self.join_lookups.resize(program.joins.len(), 0);
		self.held.resize_with(program.joins.len(), || None);
		self.verdicts.resize(program.verdicts.len(), None);
		self.seen.resize_with(program.distincts.len(), crate::exec::operators::SeenSet::new);
		self.reserved_taken.resize(program.reservations.len(), false);
	}

	/// Let go of everything a run left in this state, keeping the room its
	/// collections had, up to [`KEPT_CAPACITY`] each, and the aggregate
	/// functions and method descriptors it resolved, with the registry it
	/// resolved them from ([`Self::resolving_from`]); a descriptor is looked
	/// up again for a method of another name. Every field is named, so a
	/// field added to the state is cleared here too.
	fn clear(&mut self) {
		let State {
			paging,
			sorts,
			sort_arrivals,
			bulk,
			spare_sort_keys,
			spare_sort_bytes,
			sort_slabs,
			topk_cells,
			knn_queries,
			knn_searches,
			sort_spill,
			sort_runs,
			sort_accounts,
			group_runs,
			group_accounts,
			gather_account,
			values,
			hand_on,
			handed,
			memory,
			spill_due,
			groups,
			registry: _,
			aggregates: _,
			tries,
			output,
			pending,
			permissions_bypassed,
			read_in_place_permission_errors,
			frames,
			calls,
			functions,
			tables,
			call_principals,
			computing,
			write_input,
			landing,
			unchecked_reads,
			rowless,
			undocumented,
			root,
			write,
			frame,
			saved,
			iters,
			sorted_keys,
			program,
			group_writes,
			slots,
			once,
			lookups,
			join_lookups,
			lookup_key,
			probes,
			steps,
			joins,
			held,
			verdicts,
			seen,
			build_budget,
			zeroed,
			scan_hint,
			link_pool,
			link_ahead,
			frontier_hop,
			frontier_origins,
			frontier_rows,
			frontier_levels,
			batch_paging,
			reached_paging,
			capture_distinct,
			distinct_taken,
			reserved_taken,
			units,
			methods: _,
			index_runs,
			knn_contexts,
			matches_evals,
			backfill,
			backfill_order,
			backfill_format,
			externals,
			call_units,
			recursion,
			parent,
			degrees,
			linked,
			row_frame,
			places,
		} = self;
		emptied(paging);
		emptied(sorts);
		emptied(sort_arrivals);
		*bulk = None;
		emptied(spare_sort_keys);
		emptied(spare_sort_bytes);
		emptied(sort_slabs);
		emptied(topk_cells);
		emptied(knn_queries);
		emptied(knn_searches);
		emptied(sort_spill);
		emptied(sort_runs);
		emptied(sort_accounts);
		emptied(group_runs);
		emptied(group_accounts);
		*gather_account = None;
		*values = None;
		*hand_on = false;
		*handed = None;
		*memory = None;
		*spill_due = false;
		emptied(groups);
		emptied(tries);
		emptied(output);
		*pending = None;
		*permissions_bypassed = false;
		*read_in_place_permission_errors = false;
		emptied(frames);
		emptied(calls);
		emptied(functions);
		tables.clear();
		emptied(call_principals);
		*computing = None;
		*write_input = None;
		*landing = None;
		*unchecked_reads = false;
		*rowless = false;
		*undocumented = false;
		*root = None;
		*write = None;
		*frame = None;
		emptied(saved);
		emptied(iters);
		emptied(sorted_keys);
		*program = None;
		emptied(group_writes);
		emptied(slots);
		emptied(once);
		emptied(lookups);
		emptied(join_lookups);
		*lookup_key = None;
		emptied(probes);
		emptied(steps);
		emptied(joins);
		emptied(held);
		emptied(verdicts);
		emptied(seen);
		*build_budget = usize::MAX;
		*zeroed = false;
		*scan_hint = None;
		link_pool.clear();
		if let Some(ahead) = link_ahead {
			ahead.clear();
		}
		frontier_hop.clear();
		frontier_origins.clear();
		frontier_rows.clear();
		frontier_levels.clear();
		*batch_paging = None;
		*reached_paging = false;
		*capture_distinct = false;
		*distinct_taken = None;
		emptied(reserved_taken);
		*units = None;
		emptied(index_runs);
		emptied(knn_contexts);
		emptied(matches_evals);
		*backfill = None;
		*backfill_order = None;
		*backfill_format = None;
		emptied(externals);
		emptied(call_units);
		*recursion = None;
		*parent = None;
		emptied(degrees);
		emptied(linked);
		*row_frame = None;
		*places = None;
	}

	/// The descriptor of method `index`, `spec`'s, resolved from `registry` the
	/// first time it is asked for in the run. A slot holding another method's
	/// descriptor is resolved again, so a state shared by programs whose
	/// methods differ never answers for the wrong name.
	pub(super) fn method_descriptor(
		&mut self,
		index: u32,
		spec: &super::program::MethodSpec,
		registry: &crate::exec::function::FunctionRegistry,
	) -> Option<std::sync::Arc<crate::exec::function::MethodDescriptor>> {
		let index = index as usize;
		if self.methods.len() <= index {
			self.methods.resize(index + 1, None);
		}
		let slot = &mut self.methods[index];
		if slot.as_ref().is_none_or(|held| held.name != spec.name) {
			*slot = registry.get_method(spec.name.as_str()).cloned();
		}
		slot.clone()
	}

	/// What a morsel of this run shares with it: the index functions
	/// and `MATCHES` operators of `program`, this run's, are resolved first,
	/// so that every morsel reports against the same clauses and the same
	/// distances.
	pub(super) fn shared(
		&mut self,
		program: &Program,
		registry: &FunctionRegistry,
	) -> anyhow::Result<RunShare> {
		for (index, call) in program.calls.iter().enumerate() {
			if call.index.is_some() {
				self.index_run(index as u32, call, registry)?;
			}
		}
		for (index, spec) in program.matches.iter().enumerate() {
			self.matches_eval(index as u32, spec);
		}
		Ok(RunShare {
			registry: self.registry,
			aggregates: std::sync::Arc::clone(&self.aggregates),
			knn_contexts: self.knn_contexts.clone(),
			knn_queries: self.knn_queries.clone(),
			index_runs: self.index_runs.clone(),
			matches_evals: self.matches_evals.clone(),
			topk_cells: self.topk_cells.clone(),
		})
	}

	/// The decision tree of `MATCHES` operator `index`, `spec`'s, made the
	/// first time it is asked for in the run: it caches the index resolutions
	/// of the rows it has judged.
	pub(super) fn matches_eval(
		&mut self,
		index: u32,
		spec: &super::program::MatchesSpec,
	) -> std::sync::Arc<crate::exec::vm::matches::MatchesEval> {
		let at = index as usize;
		if self.matches_evals.len() <= at {
			self.matches_evals.resize(at + 1, None);
		}
		std::sync::Arc::clone(self.matches_evals[at].get_or_insert_with(|| {
			std::sync::Arc::new(crate::exec::vm::matches::MatchesEval::new(
				spec.operator.clone(),
				spec.probe.clone(),
				spec.registered,
				spec.executor_tables.clone(),
				spec.sql.clone(),
			))
		}))
	}

	/// The degree fast path of degree count `index`, `spec`'s, made the first
	/// time the run asks for it.
	pub(super) fn degree_counter(
		&mut self,
		index: u32,
		spec: &super::program::DegreeSpec,
	) -> anyhow::Result<std::sync::Arc<crate::exec::vm::degree::DegreeCounter>> {
		let at = index as usize;
		if self.degrees.len() <= at {
			self.degrees.resize(at + 1, None);
		}
		if let Some(counter) = &self.degrees[at] {
			return Ok(std::sync::Arc::clone(counter));
		}
		let counter = std::sync::Arc::new(crate::exec::vm::degree::DegreeCounter::new(
			spec.direction,
			&spec.tables,
		)?);
		self.degrees[at] = Some(std::sync::Arc::clone(&counter));
		Ok(counter)
	}

	/// The distance slot `slot` of the run: shared by the `KNN` scan that
	/// publishes into it and the index functions that read it.
	pub(super) fn knn_context(
		&mut self,
		slot: u16,
	) -> std::sync::Arc<crate::exec::function::KnnContext> {
		let slot = slot as usize;
		if self.knn_contexts.len() <= slot {
			self.knn_contexts.resize(slot + 1, None);
		}
		std::sync::Arc::clone(
			self.knn_contexts[slot].get_or_insert_with(|| {
				std::sync::Arc::new(crate::exec::function::KnnContext::new())
			}),
		)
	}

	/// The index function call `index` of the program names, resolved from
	/// the registry the first time the run calls it. The full-text context
	/// resolves its index lazily, on the first row it is asked about.
	pub(super) fn index_run(
		&mut self,
		index: u32,
		spec: &super::program::CallSpec,
		registry: &crate::exec::function::FunctionRegistry,
	) -> anyhow::Result<IndexRun> {
		self.index_run_for(index, spec, registry, None)
	}

	/// [`Self::index_run`] for a call whose clause's query is read when it
	/// runs ([`super::program::IndexBinding::FullText::query_arg`]): `query`,
	/// the value read. The resolution is made again when the query differs
	/// from the one it was made for.
	pub(super) fn index_run_for(
		&mut self,
		index: u32,
		spec: &super::program::CallSpec,
		registry: &crate::exec::function::FunctionRegistry,
		query: Option<&str>,
	) -> anyhow::Result<IndexRun> {
		use super::program::IndexBinding;
		let at = index as usize;
		if self.index_runs.len() <= at {
			self.index_runs.resize(at + 1, None);
		}
		if let Some(run) = &self.index_runs[at]
			&& run.query.as_deref() == query
		{
			return Ok(run.clone());
		}
		let function = registry.get_index_function(&spec.name).cloned().ok_or_else(|| {
			anyhow::anyhow!(
				"Unknown index function '{}' - not found in function registry",
				spec.name
			)
		})?;
		let context = match &spec.index {
			Some(IndexBinding::FullText {
				table,
				path,
				query: planned,
				..
			}) => crate::exec::function::IndexContext::FullText(std::sync::Arc::new(
				crate::exec::function::MatchContext::of_path(
					path.clone(),
					query.map_or_else(|| planned.clone(), str::to_owned),
					table.clone(),
				),
			)),
			Some(
				IndexBinding::Knn {
					slot,
				}
				| IndexBinding::KnnHolds {
					slot,
				},
			) => crate::exec::function::IndexContext::Knn(self.knn_context(*slot)),
			None => anyhow::bail!("`{}` is not bound to an index clause", spec.name),
		};
		let run = IndexRun {
			function,
			context,
			query: query.map(str::to_owned),
		};
		self.index_runs[at] = Some(run.clone());
		Ok(run)
	}

	/// A state holding nothing, which allocates nothing: what a state is
	/// left as while it is moved into a nested run.
	pub(super) fn vacant() -> Self {
		Self {
			paging: Vec::new(),
			sorts: Vec::new(),
			sort_arrivals: Vec::new(),
			bulk: None,
			spare_sort_keys: Vec::new(),
			spare_sort_bytes: Vec::new(),
			sort_slabs: Vec::new(),
			topk_cells: Vec::new(),
			knn_queries: Vec::new(),
			knn_searches: Vec::new(),
			sort_spill: Vec::new(),
			sort_runs: Vec::new(),
			sort_accounts: Vec::new(),
			group_runs: Vec::new(),
			group_accounts: Vec::new(),
			gather_account: None,
			values: None,
			hand_on: false,
			handed: None,
			memory: None,
			spill_due: false,
			groups: Vec::new(),
			registry: 0,
			aggregates: std::sync::Arc::clone(&NO_AGGREGATES),
			tries: Vec::new(),
			output: Vec::new(),
			pending: None,
			permissions_bypassed: false,
			read_in_place_permission_errors: false,
			frames: Vec::new(),
			calls: Vec::new(),
			functions: Vec::new(),
			tables: Default::default(),
			call_principals: Vec::new(),
			computing: None,
			unchecked_reads: false,
			write_input: None,
			landing: None,
			rowless: false,
			undocumented: false,
			root: None,
			write: None,
			frame: None,
			saved: Vec::new(),
			iters: Vec::new(),
			sorted_keys: Vec::new(),
			program: None,
			group_writes: Vec::new(),
			slots: Vec::new(),
			once: Vec::new(),
			lookups: Vec::new(),
			join_lookups: Vec::new(),
			lookup_key: None,
			probes: Vec::new(),
			steps: Vec::new(),
			joins: Vec::new(),
			held: Vec::new(),
			verdicts: Vec::new(),
			seen: Vec::new(),
			build_budget: usize::MAX,
			zeroed: false,
			scan_hint: None,
			link_pool: Default::default(),
			link_ahead: None,
			frontier_hop: Default::default(),
			frontier_origins: Default::default(),
			frontier_rows: Default::default(),
			frontier_levels: Default::default(),
			batch_paging: None,
			reached_paging: false,
			capture_distinct: false,
			distinct_taken: None,
			reserved_taken: Vec::new(),
			units: None,
			methods: Vec::new(),
			index_runs: Vec::new(),
			knn_contexts: Vec::new(),
			matches_evals: Vec::new(),
			backfill: None,
			backfill_order: None,
			backfill_format: None,
			externals: Vec::new(),
			call_units: Vec::new(),
			recursion: None,
			parent: None,
			degrees: Vec::new(),
			linked: Vec::new(),
			row_frame: None,
			places: None,
		}
	}

	/// Start aggregate `aggregate` of `program` afresh, as a program's start
	/// leaves it.
	pub(super) fn reset_aggregate(&mut self, program: &Program, aggregate: usize) {
		let sink = &program.aggregates[aggregate];
		self.groups[aggregate] = Some(fresh_groups(sink, &self.aggregates.functions[aggregate]));
		if let Some(runs) = self.group_runs.get_mut(aggregate) {
			*runs = None;
		}
		if let Some(account) = self.group_accounts.get_mut(aggregate) {
			account.clear();
		}
	}

	/// Whether the state has linked a unit for the closure `digest` names,
	/// invoked in frame `frame`.
	#[inline]
	pub(super) fn has_linked(
		&self,
		frame: Option<u16>,
		digest: super::program::ClosureDigest,
	) -> bool {
		self.linked.iter().rev().any(|held| *held == (frame, digest))
	}

	/// Note that the state has linked a unit for the closure `digest` names,
	/// invoked in frame `frame`, forgetting the least recent beyond
	/// [`super::closure::LINKED`].
	pub(super) fn note_linked(
		&mut self,
		frame: Option<u16>,
		digest: super::program::ClosureDigest,
	) {
		if self.has_linked(frame, digest) {
			return;
		}
		if self.linked.len() >= super::closure::LINKED {
			self.linked.remove(0);
		}
		self.linked.push((frame, digest));
	}

	/// Forget that the state linked a unit for the closure `digest` names,
	/// invoked in frame `frame`: the run's table holds no entry for it.
	pub(super) fn forget_linked(
		&mut self,
		frame: Option<u16>,
		digest: super::program::ClosureDigest,
	) {
		self.linked.retain(|held| *held != (frame, digest));
	}

	pub(super) fn take_output(&mut self) -> Vec<Value> {
		std::mem::take(&mut self.output)
	}

	/// Note that the row has executed the paging instruction of clause
	/// `limit`, when `limit` is the clause the batch this state runs counts
	/// its rows against.
	#[inline]
	fn reach_paging(&mut self, limit: u16) {
		if self.batch_paging == Some(limit) {
			self.reached_paging = true;
		}
	}

	/// Take the row just pushed onto sort `sort` into the rows it keeps, when
	/// it keeps a bounded number.
	///
	/// The work a row costs is bounded by the quantum whatever the limit
	/// ([`Arrivals::run`]):
	///
	/// - A sort whose limit, with a quarter more, fits a quantum prunes: once it holds a quarter
	///   more rows than it keeps it sorts them and keeps the first, a sort of no more than a
	///   quantum of rows per quarter of the kept rows. The sort is stable, so a row's place among
	///   equal keys is its arrival.
	/// - A sort with a larger limit merges ([`Self::merge_arrived`]): it holds its kept rows sorted
	///   at the front of its buffer and the rows that arrived since behind them, and takes those
	///   into the kept rows a run of no more than a quantum at a time, the run sorted in one go and
	///   merged a share per arrival. Each row carries its arrival after its keys, which orders a
	///   tie ([`super::sort::compare_rows`]), so the rows kept, and the order the sort hands them
	///   on in, are the stable ones wherever a row lies in the buffer.
	///
	/// Once a sort holds as many as it keeps, the last kept row's keys are its
	/// floor ([`Self::sort_floor`]): [`Instr::SortPush`] drops a row whose
	/// keys do not order strictly before them, as no later row with equal keys
	/// can take a kept place.
	pub(super) fn keep_sorted(&mut self, program: &Program, sort: u16) {
		let at = sort as usize;
		let spec = &program.sorts[at];
		if spec.random {
			return;
		}
		let Some(keep) = spec.keep else {
			return;
		};
		let arrivals = &mut self.sort_arrivals[at];
		if arrivals.run(keep) == 0 {
			self.prune(program, sort, keep);
			return;
		}
		let arrival = arrivals.arrived;
		arrivals.arrived += 1;
		if let Some((keys, _)) = self.sorts[at].last_mut() {
			keys.push(Value::from(arrival as i64));
		}
		self.merge_arrived(program, sort, keep);
	}

	/// [`Self::keep_sorted`] for rows a morsel kept for sort `sort`,
	/// in the order its own sort held them: they arrived after every row the
	/// sort holds, `arrived` of them reaching the morsel's sort. A merging
	/// sort's rows carry their arrival in the batch, which is rebased on the
	/// sort's own.
	pub(super) fn keep_absorbed(
		&mut self,
		program: &Program,
		sort: u16,
		rows: Vec<(Vec<Value>, SortRow)>,
		arrived: u64,
	) {
		let at = sort as usize;
		let spec = &program.sorts[at];
		let keep = match spec.keep {
			Some(keep) if !spec.random => keep,
			_ => {
				// A keep sort charges its buffer's length, so each row is
				// pushed before the next is charged.
				for row in rows {
					self.charge_sort(sort, &row);
					self.sorts[at].push(row);
				}
				return;
			}
		};
		if self.sort_arrivals[at].run(keep) == 0 {
			for row in rows {
				self.charge_sort(sort, &row);
				self.sorts[at].push(row);
				self.prune(program, sort, keep);
			}
			return;
		}
		let width = spec.keys.len();
		let base = self.sort_arrivals[at].arrived;
		self.sort_arrivals[at].arrived += arrived.max(rows.len() as u64);
		for (place, (mut keys, row)) in rows.into_iter().enumerate() {
			// A row the batch kept unstamped held its place among equal keys
			// by where it lay, which stamps it.
			match keys.get_mut(width) {
				Some(Value::Number(crate::val::Number::Int(arrival))) => *arrival += base as i64,
				_ => {
					keys.truncate(width);
					keys.push(Value::from((base + place as u64) as i64));
				}
			}
			let row = (keys, row);
			self.charge_sort(sort, &row);
			self.sorts[at].push(row);
			self.merge_arrived(program, sort, keep);
		}
	}

	/// Prune sort `sort` to the `keep` rows its order can still reach once it
	/// holds a quarter more, and take the last kept row as its floor. The
	/// rows a prune sorts, no more than a quantum, are charged whole.
	///
	/// A sort that cuts a key-range scan by the second of a datetime component
	/// ([`SortSpec::floors_at_keep`](super::program::SortSpec)) also prunes the
	/// first time it holds as many rows as it keeps, so the scan has a
	/// threshold from then on rather than a quarter more rows later: a region
	/// hands its sort no more than `keep` rows a batch, so a sort waiting for
	/// the quarter more would publish only several batches on.
	fn prune(&mut self, program: &Program, sort: u16, keep: usize) {
		let at = sort as usize;
		let first_floor =
			program.sorts[at].floors_at_keep && keep > 0 && !self.sort_arrivals[at].floored;
		let margin = if first_floor {
			0
		} else {
			(keep / 4).max(16)
		};
		let rows = &mut self.sorts[at];
		if rows.len() < keep.saturating_add(margin) {
			return;
		}
		super::fuel::charge(super::fuel::units(rows.len()));
		#[cfg(test)]
		LARGEST_PRUNE.with(|largest| largest.set(largest.get().max(rows.len())));
		rows.sort_by(|(a, _), (b, _)| compare_keys(a, b, &program.sorts[at].keys));
		// The rows pruned hand their buffers to the rows that reach the sort
		// next, so a bounded sort allocates for no more rows than one prune
		// drops.
		for row in rows.drain(keep.min(rows.len())..) {
			recycle_sort_row(&mut self.spare_sort_keys, &mut self.spare_sort_bytes, row);
		}
		if keep > 0 && rows.len() == keep {
			self.sort_arrivals[at].floored = true;
			if let Some(Some(cell)) = self.topk_cells.get(at)
				&& let Some(first_key) = rows[keep - 1].0.first()
			{
				publish_threshold(cell, first_key, &program.sorts[at].keys);
			}
		}
	}

	/// Advance a merging sort `sort` by the row that just arrived, stamped
	/// with its arrival: a step of the merge under way, or a run started once
	/// enough rows have arrived behind the kept ones.
	///
	/// The buffer holds the kept rows, sorted, at `0..kept` and the rows that
	/// arrived since behind them, in no order. A run is taken in one of two
	/// ways, so that a merge only ever swaps rows within the buffer and the
	/// buffer holds every row the sort may keep at every point between rows:
	///
	/// - While the sort holds fewer than it keeps, the run is the last rows of the buffer, no more
	///   than fill it, taken once as many again have arrived; nothing is dropped, and the rows the
	///   merge displaces from the front are arrived rows, which take the run's places.
	/// - Once it is full, the run is the rows just behind the kept ones. As many rows as the run
	///   holds are cut from the ends of the two, the greatest, and the merge writes the rest into
	///   `0..keep`, displacing the kept rows cut into the run's places, so that once it is done the
	///   cut rows are those at `keep..keep + run` and are removed.
	///
	/// A run's start sorts it (no more than a quantum of rows), finds the cut
	/// by a binary search and takes the merge's first step, which places the
	/// greatest row kept at `keep - 1`, where it stays as the sort's floor.
	/// Each later row owes a share of the merge, enough that it is done before
	/// the next run is due, and the shares are placed a stretch of no more
	/// than a quantum at a time.
	fn merge_arrived(&mut self, program: &Program, sort: u16, keep: usize) {
		let at = sort as usize;
		let keys = &program.sorts[at].keys;
		let rows = &mut self.sorts[at];
		let arrivals = &mut self.sort_arrivals[at];
		if keep == 0 {
			for row in rows.drain(..) {
				recycle_sort_row(&mut self.spare_sort_keys, &mut self.spare_sort_bytes, row);
			}
			return;
		}
		if let Some(merge) = &mut arrivals.merge {
			// An arrival owes its share of the merge, placed a stretch at a
			// time, so that a gallop's comparisons serve many rows.
			let stretch =
				MERGE_STRETCH.max(merge.pace).min(surrealdb_runtime::fuel::quantum() as usize);
			merge.owed += merge.pace;
			if merge.owed < stretch.min(merge.len) {
				return;
			}
			let placed = merge.owed.min(stretch);
			merge.owed -= placed;
			super::fuel::charge(super::fuel::units(placed));
			if merge.step(rows, keys, placed) {
				let merge = *merge;
				arrivals.merge = None;
				merge.finish(rows, arrivals, &mut self.spare_sort_keys, &mut self.spare_sort_bytes);
			}
			return;
		}
		let most = arrivals.run(keep);
		let kept = arrivals.kept;
		let arrived = rows.len() - kept;
		let full = kept == keep;
		let (run, start) = if full {
			if arrived < most {
				return;
			}
			(most, kept)
		} else {
			let run = most.min(keep - kept);
			if arrived < 2 * run {
				return;
			}
			(run, rows.len() - run)
		};
		super::fuel::charge(super::fuel::units(2 * run));
		#[cfg(test)]
		LARGEST_PRUNE.with(|largest| largest.set(largest.get().max(run)));
		// Stamped rows order totally, so an unstable sort is a stable one.
		rows[start..start + run]
			.sort_unstable_by(|(a, _), (b, _)| super::sort::compare_rows(a, b, keys));
		// Once full, the run's worth of the greatest rows of the two is cut:
		// `cut` from the kept rows, found by a binary search for the count
		// at which every row cut orders after every row kept.
		let cut = if full {
			let (mut low, mut high) = (0, run.min(kept));
			while low < high {
				let from_kept = low + (high - low) / 2;
				let from_run = run - from_kept;
				// Too few cut from the kept rows while the least row cut
				// from the run orders before the greatest kept row left.
				if super::sort::compare_rows(
					&rows[start + run - from_run].0,
					&rows[kept - from_kept - 1].0,
					keys,
				) == Ordering::Less
				{
					low = from_kept + 1;
				} else {
					high = from_kept;
				}
			}
			low
		} else {
			0
		};
		let (left, right) = if full {
			(kept - cut, cut)
		} else {
			(kept, run)
		};
		let len = left + right;
		let mut merge = Merge {
			kept: left,
			run: right,
			run_at: start,
			len,
			cut: if full {
				run
			} else {
				0
			},
			pace: (len.div_ceil(most) + 1).min(surrealdb_runtime::fuel::quantum() as usize),
			owed: 0,
		};
		let done = merge.step(rows, keys, 1);
		if len == keep {
			arrivals.floored = true;
			if let Some(Some(cell)) = self.topk_cells.get(at)
				&& let Some(first_key) = rows[keep - 1].0.first()
			{
				publish_threshold(cell, first_key, &program.sorts[at].keys);
			}
		}
		if done {
			merge.finish(rows, arrivals, &mut self.spare_sort_keys, &mut self.spare_sort_bytes);
		} else {
			arrivals.merge = Some(merge);
		}
	}

	/// The keys a row must order strictly before to reach a kept place of
	/// sort `sort`, once it keeps a bounded number of rows and holds that
	/// many ([`Self::keep_sorted`]): the last row a prune kept, or the row a
	/// merge's run placed last, or the floor of the runs the sort has spilled
	/// if that orders before. A later row whose keys equal them orders after
	/// that row, by arrival.
	///
	/// A row that does not order strictly before a spilled floor cannot reach
	/// a kept place: a run that holds `keep` records has `keep` rows, each
	/// ordering at or before its last record, and all of them arrived
	/// earlier, so they win every tie by source order in the merge.
	#[inline]
	pub(super) fn sort_floor<'s>(&'s self, program: &Program, sort: u16) -> Option<&'s [Value]> {
		let at = sort as usize;
		let spec = &program.sorts[at];
		let keep = match spec.keep {
			Some(keep) if keep > 0 && !spec.random => keep,
			_ => return None,
		};
		let memory_floor =
			self.sort_arrivals[at].floored.then(|| self.sorts[at][keep - 1].0.as_slice());
		let spilled_floor = self.sort_spill[at].as_ref().and_then(|spill| spill.floor.as_deref());
		match (memory_floor, spilled_floor) {
			(Some(a), Some(b)) => Some(if compare_keys(a, b, &spec.keys) == Ordering::Greater {
				b
			} else {
				a
			}),
			(a, b) => a.or(b),
		}
	}

	/// [`Self::settle`] for a record read in place, from its stored bytes: a
	/// sort takes the bytes, decoded only if the record keeps its place, and
	/// the output takes the record decoded.
	///
	/// The caller has admitted the record's generation against its table's
	/// layouts ([`crate::kvs::RowGate`]); the bytes a sort keeps are decoded
	/// later without a check. A record in a packed layout is never given: the
	/// host decodes it and settles it as a value ([`Self::settle`]), so a sort,
	/// a frame's late transcode and a spill run hold no packed bytes and read
	/// no layouts.
	pub(super) fn settle_stored(
		&mut self,
		program: &Program,
		key: &[u8],
		value: &[u8],
	) -> anyhow::Result<()> {
		match self.pending.take() {
			// A sort that keeps every row appends its records to its slab.
			Some(Pending::Sort {
				sort,
				keys,
			}) if program.sorts[sort as usize].keep.is_none() => {
				let slab = &mut self.sort_slabs[sort as usize];
				let start = slab.len();
				slab.extend_from_slice(key);
				slab.extend_from_slice(value);
				let row = (
					keys,
					SortRow::Slab {
						start,
						split: start + key.len(),
						end: slab.len(),
					},
				);
				self.charge_sort(sort, &row);
				self.sorts[sort as usize].push(row);
			}
			Some(Pending::Sort {
				sort,
				keys,
			}) => {
				let mut bytes = self.spare_sort_bytes.pop().unwrap_or_default();
				bytes.clear();
				bytes.reserve_exact(key.len() + value.len());
				bytes.extend_from_slice(key);
				bytes.extend_from_slice(value);
				let row = (
					keys,
					SortRow::Stored {
						bytes,
						split: key.len(),
					},
				);
				self.charge_sort(sort, &row);
				self.sorts[sort as usize].push(row);
				self.keep_sorted(program, sort);
			}
			// An emitted record of a statement whose rows leave encoded is
			// transcoded from its stored bytes, after the rows emitted before
			// it.
			Some(Pending::Emit) if self.row_frame.is_some() => {
				self.drain_output_into_frame()?;
				if let Some(frame) = &mut self.row_frame {
					frame.push_stored(key, value, None)?;
				}
			}
			pending => {
				self.pending = pending;
				let row = super::row::StoredRow::new(key, value).record()?;
				self.settle(program, row);
			}
		}
		Ok(())
	}

	/// Encode the rows [`Self::output`] holds into [`Self::frame`], in order.
	fn drain_output_into_frame(&mut self) -> anyhow::Result<()> {
		let Some(frame) = &mut self.row_frame else {
			return Ok(());
		};
		for row in self.output.drain(..) {
			frame.push_value(&row)?;
		}
		Ok(())
	}

	/// How many rows the statement holds for its output, encoded or not.
	pub(super) fn output_len(&self) -> usize {
		self.output.len() + self.row_frame.as_ref().map_or(0, super::row_frame::RowFrame::len)
	}

	/// For a statement whose rows leave encoded, every row it holds for its
	/// output, encoded and taken; `None` for one whose rows leave as values.
	pub(super) fn framed_output(&mut self) -> anyhow::Result<Option<surrealdb_rpc::EncodedRows>> {
		if self.row_frame.is_none() {
			return Ok(None);
		}
		self.drain_output_into_frame()?;
		Ok(self.row_frame.as_mut().map(super::row_frame::RowFrame::take_encoded))
	}

	/// Charge a row sort `sort` is about to take to its account, marking a
	/// spill due once its lease does not cover it.
	///
	/// A sort that keeps every row charges each row. A keep sort charges what
	/// its buffer holds, the row included, at the mean row it has taken, which
	/// bounds what a pruning or merging sort holds; within one chunk it takes
	/// nothing from its lease.
	#[inline]
	fn charge_sort(&mut self, sort: u16, row: &super::sort::Sorted) {
		let at = sort as usize;
		let Some(chunk) = self.sort_accounts[at].memory().map(|memory| memory.chunk()) else {
			return;
		};
		let bytes = super::sort::sorted_bytes(row);
		let account = &mut self.sort_accounts[at];
		let verdict = match self.sort_spill[at].as_mut() {
			Some(spill) if spill.keep.is_some() => {
				spill.taken_bytes += bytes as u64;
				spill.taken_rows += 1;
				let mean = spill.taken_bytes / spill.taken_rows;
				let held = usize::try_from((self.sorts[at].len() as u64 + 1).saturating_mul(mean))
					.unwrap_or(usize::MAX);
				if account.used() == 0 && held <= chunk {
					crate::exec::memory::Verdict::Hold
				} else {
					account.hold(held)
				}
			}
			_ => account.charge(bytes),
		};
		if verdict == crate::exec::memory::Verdict::Spill {
			self.spill_due = true;
		}
	}

	/// Hand `row`, keyed by `keys`, to sort `sort`, which keeps only the rows
	/// its order can still reach.
	fn sort_value(&mut self, program: &Program, sort: u16, keys: Vec<Value>, row: Value) {
		let row = (keys, SortRow::Value(row));
		self.charge_sort(sort, &row);
		self.sorts[sort as usize].push(row);
		self.keep_sorted(program, sort);
	}

	/// Hand a row the body left pending to where it goes: the output, or a
	/// sort, which keeps only the rows its order can still reach.
	pub(super) fn settle(&mut self, program: &Program, row: Value) {
		match self.pending.take() {
			Some(Pending::Emit) => self.output.push(row),
			Some(Pending::Sort {
				sort,
				keys,
			}) => self.sort_value(program, sort, keys, row),
			// A split's rows are run by the host, which takes the pending
			// split before a row is settled.
			Some(Pending::Split {
				..
			}) => debug_assert!(false, "a split's row was settled"),
			// A join's pairings are run by the host, which takes the pending
			// probe row before a row is settled.
			Some(
				Pending::Join {
					..
				}
				| Pending::JoinPair {
					..
				},
			) => debug_assert!(false, "a join's row was settled"),
			None => {}
		}
	}
}

/// Whether `operand`, an operand an instruction in frame `frame` hands a
/// callee, holds a closure value the state has not linked a unit for in that
/// frame. Read in place: nothing is cloned.
#[inline]
fn unlinked(
	program: &Program,
	row: &dyn RowReader,
	regs: &[Scalar<'_>],
	state: &State,
	frame: Option<u16>,
	operand: Operand,
) -> anyhow::Result<bool> {
	let digest = closure_digest(program, row, regs, operand, super::closure::digest_of)?;
	Ok(digest.is_some_and(|digest| !state.has_linked(frame, digest)))
}

/// What `find` answers of the value `operand` denotes, where it denotes a
/// value rather than a scalar of a register: read in place, nothing cloned.
#[inline]
fn closure_digest(
	program: &Program,
	row: &dyn RowReader,
	regs: &[Scalar<'_>],
	operand: Operand,
	find: impl FnOnce(&Value) -> Option<super::program::ClosureDigest>,
) -> anyhow::Result<Option<super::program::ClosureDigest>> {
	Ok(match operand {
		Operand::Reg(reg) => match &regs[reg as usize] {
			Scalar::Value(value) => find(value),
			_ => None,
		},
		Operand::Const(index) => find(&program.consts[index as usize]),
		Operand::Field(name) => match row.field(&program.fields[name as usize])? {
			Scalar::Value(value) => find(&value),
			_ => None,
		},
	})
}

/// The value `operand` denotes, for a host step that takes its operands as
/// values.
pub(super) fn read_operand(
	regs: &[Scalar<'_>],
	row: &dyn RowReader,
	program: &Program,
	operand: Operand,
) -> anyhow::Result<Value> {
	Ok(match operand {
		Operand::Reg(reg) => regs[reg as usize].to_value(),
		Operand::Const(index) => program.consts[index as usize].clone(),
		Operand::Field(name) => row.field(&program.fields[name as usize])?.to_value(),
	})
}

/// Run `pipeline`'s body for the row `row` reads, from instruction `pc`.
/// `regs` holds at least the pipeline's registers and is the row's own: what
/// it holds may borrow from the row.
pub(super) fn run_row<'r>(
	program: &'r Program,
	pipeline: &'r Pipeline,
	row: &'r dyn RowReader,
	regs: &mut [Scalar<'r>],
	state: &mut State,
	mut pc: usize,
) -> anyhow::Result<Step> {
	// The [`ROW`] slot is never handed out as a body register. When the row is borrowed whole,
	// the slot holds it, so code over row values reads the row from there.
	if let (Some(slot), Some(whole)) = (regs.get_mut(ROW as usize), row.borrowed()) {
		*slot = Scalar::Value(std::borrow::Cow::Borrowed(whole));
	}
	let read = |regs: &[Scalar<'r>], operand: Operand| -> anyhow::Result<Scalar<'r>> {
		Ok(match operand {
			Operand::Reg(reg) => regs[reg as usize].clone(),
			Operand::Const(index) => Scalar::of(&program.consts[index as usize]),
			Operand::Field(name) => row.field(&program.fields[name as usize])?,
		})
	};
	// The value `operand` denotes, owned: a composite a register holds is
	// cloned once, where `read` followed by `to_value` would clone it twice.
	let read_value = |regs: &[Scalar<'r>], operand: Operand| -> anyhow::Result<Value> {
		Ok(match operand {
			Operand::Reg(reg) => regs[reg as usize].to_value(),
			Operand::Const(index) => program.consts[index as usize].clone(),
			Operand::Field(name) => row.field(&program.fields[name as usize])?.to_value(),
		})
	};
	// A jump to `target`. A backward one is an iteration of a loop and spends
	// a unit of fuel; once the fuel is spent the row yields there, and its
	// driver resumes it at `target` with the same registers.
	macro_rules! jump {
		($target:expr) => {{
			let target: usize = $target;
			if target <= pc && super::fuel::charge(1) {
				return Ok(Step::Yield {
					resume: target,
				});
			}
			pc = target;
		}};
	}
	// The value `operand` names, borrowed where it is held whole — an array,
	// a set, an object and so on — with `field` holding a field's scalar;
	// `None` for a scalar held unboxed.
	macro_rules! peek {
		($operand:expr, $field:ident) => {
			match $operand {
				Operand::Reg(reg) => regs[reg as usize].as_whole(),
				Operand::Const(index) => Some(&program.consts[index as usize]),
				Operand::Field(name) => {
					$field = row.field(&program.fields[name as usize])?;
					$field.as_whole()
				}
			}
		};
	}
	loop {
		// A body that reaches past its last instruction, on entry, on resume after a
		// host step, or after a jump, means the program was lowered wrong. It raises an error.
		let Some(instr) = pipeline.code.get(pc) else {
			bail!("a pipeline body ran past its end");
		};
		match instr {
			Instr::CoerceField {
				reg,
				kind,
				field,
			} => {
				let value = std::mem::replace(&mut regs[*reg as usize], Scalar::None).to_value();
				let coerced = match value.coerce_to_kind(&program.kinds[*kind as usize]) {
					Ok(coerced) => coerced,
					Err(error) => {
						return Err(anyhow::Error::new(crate::doc::Error::FieldCoerce {
							record: crate::exec::operators::record_of(&row.record()?),
							field_name: program.fields[*field as usize].as_str().to_owned(),
							error: Box::new(error),
						}));
					}
				};
				regs[*reg as usize] = Scalar::Value(std::borrow::Cow::Owned(coerced));
				pc += 1;
			}
			Instr::BeginComputing {
				id,
				doc,
				names,
			} => {
				let names = &program.computed_names[*names as usize];
				state.computing = match row.field(&program.fields[*id as usize])? {
					Scalar::Value(id) => match id.as_ref() {
						Value::RecordId(rid) => {
							let stored = match doc {
								Some(ROW) => Some(row.record()?),
								Some(doc) => Some(regs[*doc as usize].to_value()),
								None if row.has_any_field(names)? => Some(row.record()?),
								None => None,
							};
							let stored = stored.map(|mut view| {
								if let Value::Object(obj) = &mut view {
									for name in names {
										obj.remove(name.as_str());
									}
								}
								view
							});
							Some(std::sync::Arc::new(crate::exec::eval_context::ComputingRecord {
								rid: rid.clone(),
								stored,
							}))
						}
						_ => None,
					},
					_ => None,
				};
				pc += 1;
			}
			Instr::LandRow {
				sets,
				cuts,
			} => {
				let take = |regs: &mut [Scalar<'r>], reg: Reg| match std::mem::replace(
					&mut regs[reg as usize],
					Scalar::None,
				)
				.to_value()
				{
					Value::Object(obj) => obj,
					_ => crate::val::Object::default(),
				};
				let cuts = take(regs, *cuts);
				let sets = take(regs, *sets);
				state.landing = Some((cuts, sets));
				return Ok(Step::LandRow {
					resume: pc + 1,
				});
			}
			Instr::BindRow {
				name,
			} => {
				return Ok(Step::BindRow {
					name: *name,
					resume: pc + 1,
				});
			}
			Instr::JumpIfPermissionsBypassed {
				target,
			} => {
				if state.permissions_bypassed {
					jump!(*target as usize);
				} else {
					pc += 1;
				}
			}
			Instr::WriteProjection {
				..
			} => {
				return Ok(Step::WriteProjection {
					at: pc,
				});
			}
			Instr::CompareFieldInt {
				field,
				op,
				lit,
				reversed,
				fallback,
				dst,
			} => {
				let name = &program.fields[*field as usize];
				let answer = match row.field_int(name)? {
					// `field op lit` is compiled with the field on the left.
					Some(value) => Scalar::Bool(op.apply(value, *lit)),
					None => {
						let value = row.field(name)?;
						let lit = Scalar::Number(crate::val::Number::Int(*lit));
						if *reversed {
							fallback.apply_scalar(&lit, &value)?
						} else {
							fallback.apply_scalar(&value, &lit)?
						}
					}
				};
				regs[*dst as usize] = answer;
				pc += 1;
			}
			Instr::JumpIfFieldInt {
				field,
				op,
				lit,
				reversed,
				fallback,
				truthy,
				target,
			} => {
				let name = &program.fields[*field as usize];
				let holds = match row.field_int(name)? {
					Some(value) => op.apply(value, *lit),
					None => {
						let value = row.field(name)?;
						let lit = Scalar::Number(crate::val::Number::Int(*lit));
						let answer = if *reversed {
							fallback.apply_scalar(&lit, &value)?
						} else {
							fallback.apply_scalar(&value, &lit)?
						};
						answer.is_truthy()
					}
				};
				if holds == *truthy {
					jump!(*target as usize);
				} else {
					pc += 1;
				}
			}
			Instr::FilterFieldInt {
				field,
				op,
				lit,
				reversed,
				fallback,
				reject,
			} => {
				let name = &program.fields[*field as usize];
				let holds = match row.field_int(name)? {
					Some(value) => op.apply(value, *lit),
					None => {
						let value = row.field(name)?;
						let lit = Scalar::Number(crate::val::Number::Int(*lit));
						let answer = if *reversed {
							fallback.apply_scalar(&lit, &value)?
						} else {
							fallback.apply_scalar(&value, &lit)?
						};
						answer.is_truthy()
					}
				};
				if holds == *reject {
					return Ok(Step::Next);
				}
				pc += 1;
			}
			Instr::Compare {
				op,
				left,
				right,
				dst,
			} => {
				let (left, right) = (read(regs, *left)?, read(regs, *right)?);
				let answer = op.apply_scalar(&left, &right)?;
				charge_walked(&left, &right);
				regs[*dst as usize] = answer;
				pc += 1;
			}
			Instr::JumpIfCompare {
				op,
				left,
				right,
				truthy,
				target,
			} => {
				let (left, right) = (read(regs, *left)?, read(regs, *right)?);
				let answer = op.apply_scalar(&left, &right)?;
				charge_walked(&left, &right);
				if answer.is_truthy() == *truthy {
					jump!(*target as usize);
				} else {
					pc += 1;
				}
			}
			Instr::FilterCompare {
				op,
				left,
				right,
				reject,
			} => {
				let (left, right) = (read(regs, *left)?, read(regs, *right)?);
				let answer = op.apply_scalar(&left, &right)?;
				charge_walked(&left, &right);
				if answer.is_truthy() == *reject {
					return Ok(Step::Next);
				}
				pc += 1;
			}
			Instr::Arith {
				op,
				left,
				right,
				dst,
			} => {
				let answer = op.apply(read_value(regs, *left)?, read_value(regs, *right)?)?;
				super::fuel::charge(super::fuel::weight(&answer));
				regs[*dst as usize] = Scalar::Value(std::borrow::Cow::Owned(answer));
				pc += 1;
			}
			Instr::Unary {
				op,
				src,
				dst,
			} => {
				let answer = op.apply(read_value(regs, *src)?, &program.kinds)?;
				regs[*dst as usize] = Scalar::Value(std::borrow::Cow::Owned(answer));
				pc += 1;
			}
			Instr::NewArray {
				dst,
			} => {
				regs[*dst as usize] =
					Scalar::Value(std::borrow::Cow::Owned(Value::Array(Default::default())));
				pc += 1;
			}
			Instr::ArrayPush {
				array,
				value,
			} => {
				let value = read_value(regs, *value)?;
				match &mut regs[*array as usize] {
					Scalar::Value(std::borrow::Cow::Owned(Value::Array(items))) => {
						items.push(value)
					}
					other => bail!("appending to {other:?}, which is not an array"),
				}
				pc += 1;
			}
			Instr::LoadSlot {
				slot,
				dst,
			} => {
				let value = state.slots.get(*slot as usize).cloned().unwrap_or(Value::None);
				regs[*dst as usize] = Scalar::Value(std::borrow::Cow::Owned(value));
				pc += 1;
			}
			Instr::LoadParam {
				param,
				dst,
				frame,
			} => {
				return Ok(Step::Param {
					param: *param,
					dst: *dst,
					frame: *frame,
					resume: pc + 1,
				});
			}
			Instr::Nav {
				op,
				src,
				index,
				dst,
			} => {
				// The value is read in place: a step takes a part of it, and
				// a copy of the whole would be a walk of its own.
				let (field, held);
				let value = match peek!(*src, field) {
					Some(value) => value,
					None => {
						held = read_value(regs, *src)?;
						&held
					}
				};
				let index = match index {
					Some(index) => Some(read_value(regs, *index)?),
					None => None,
				};
				let answer = op.apply(value, index.as_ref())?;
				// A flatten, or a slice by a range, walks what it answers in
				// one go: charged whole, the row yields at its next yield point.
				super::fuel::charge(super::fuel::weight(&answer));
				regs[*dst as usize] = Scalar::Value(std::borrow::Cow::Owned(answer));
				pc += 1;
			}
			Instr::Access {
				src,
				field,
				dst,
				..
			} => {
				let base = read(regs, *src)?;
				let name = program.fields[*field as usize].as_str();
				// A base the row or the program owns is read in place: the
				// answer borrows from it rather than copying the object it
				// sits in.
				let answer = match base {
					Scalar::Value(std::borrow::Cow::Borrowed(value)) => match value {
						Value::Object(object) => {
							Some(Scalar::of(object.get(name).unwrap_or(&Value::None)))
						}
						Value::RecordId(_)
						| Value::Array(_)
						| Value::Set(_)
						| Value::Geometry(_) => None,
						_ => Some(Scalar::None),
					},
					Scalar::Value(value) => match value.as_ref() {
						Value::Object(object) => Some(Scalar::Value(std::borrow::Cow::Owned(
							object.get(name).cloned().unwrap_or(Value::None),
						))),
						Value::RecordId(_)
						| Value::Array(_)
						| Value::Set(_)
						| Value::Geometry(_) => None,
						_ => Some(Scalar::None),
					},
					_ => Some(Scalar::None),
				};
				match answer {
					Some(answer) => {
						regs[*dst as usize] = answer;
						pc += 1;
					}
					None => {
						return Ok(Step::Access {
							at: pc,
						});
					}
				}
			}
			Instr::Each {
				src,
				dst,
				..
			} => {
				let field;
				let base = peek!(*src, field);
				let answer = match base {
					// The collection is copied a stretch at a time, a unit an
					// element, yielding mid-copy once the fuel is spent.
					Some(Value::Object(object)) => {
						let entries = object.0.as_slice();
						match copied(state, Bulk::at(pipeline, pc, entries), entries, |_| false) {
							Copied::Done(entries) => Some(Value::Object(crate::val::Object(
								surrealdb_collections::VecMap::from_sorted_vec_unchecked(entries),
							))),
							Copied::Spent => {
								return Ok(Step::Yield {
									resume: pc,
								});
							}
							Copied::Refused => None,
						}
					}
					Some(Value::Array(array)) => {
						match copied(state, Bulk::at(pipeline, pc, &array.0), &array.0, |item| {
							matches!(item, Value::RecordId(_))
						}) {
							Copied::Done(values) => Some(Value::Array(values.into())),
							Copied::Spent => {
								return Ok(Step::Yield {
									resume: pc,
								});
							}
							Copied::Refused => None,
						}
					}
					Some(Value::Set(set)) => {
						let elements = set.0.iter().as_slice();
						match copied(state, Bulk::at(pipeline, pc, elements), elements, |item| {
							matches!(item, Value::RecordId(_))
						}) {
							// No element is a record id, so the copy is the set
							// itself, in its order.
							Copied::Done(values) => Some(Value::Set(crate::val::Set::from(
								surrealdb_collections::VecSet::from_sorted_vec_unchecked(values),
							))),
							Copied::Spent => {
								return Ok(Step::Yield {
									resume: pc,
								});
							}
							Copied::Refused => None,
						}
					}
					Some(Value::RecordId(_)) => None,
					_ => Some(Value::None),
				};
				match answer {
					Some(answer) => {
						regs[*dst as usize] = Scalar::Value(std::borrow::Cow::Owned(answer));
						pc += 1;
					}
					None => {
						return Ok(Step::Each {
							at: pc,
						});
					}
				}
			}
			Instr::FilterBegin {
				src,
				items,
				dst,
				none,
			} => {
				// The elements the filter walks are copied a stretch at a
				// time, a unit an element, yielding mid-copy once the fuel is
				// spent; the loop it begins spends a unit an element again.
				let field;
				let base = peek!(*src, field);
				let (elements, empty) = match base {
					Some(Value::Array(array)) => {
						(&array.0[..], Value::Array(crate::val::Array::new()))
					}
					Some(Value::Set(set)) => {
						(set.0.iter().as_slice(), Value::Set(crate::val::Set::new()))
					}
					_ => {
						regs[*dst as usize] = Scalar::None;
						jump!(*none as usize);
						continue;
					}
				};
				match copied(state, Bulk::at(pipeline, pc, elements), elements, |_| false) {
					Copied::Done(elements) => {
						regs[*dst as usize] = Scalar::Value(std::borrow::Cow::Owned(empty));
						regs[*items as usize] =
							Scalar::Value(std::borrow::Cow::Owned(Value::Array(elements.into())));
						pc += 1;
					}
					Copied::Spent => {
						return Ok(Step::Yield {
							resume: pc,
						});
					}
					Copied::Refused => unreachable!("a copy that refuses nothing"),
				}
			}
			Instr::MapBegin {
				src,
				items,
				dst,
				other,
				sets,
			} => {
				// As `FilterBegin`: the elements are copied a stretch at a time.
				let field;
				let base = peek!(*src, field);
				let (elements, empty) = match base {
					Some(Value::Array(array)) => {
						(&array.0[..], Value::Array(crate::val::Array::new()))
					}
					Some(Value::Set(set)) if *sets => {
						(set.0.iter().as_slice(), Value::Set(crate::val::Set::new()))
					}
					_ => {
						jump!(*other as usize);
						continue;
					}
				};
				match copied(state, Bulk::at(pipeline, pc, elements), elements, |_| false) {
					Copied::Done(elements) => {
						regs[*dst as usize] = Scalar::Value(std::borrow::Cow::Owned(empty));
						regs[*items as usize] =
							Scalar::Value(std::borrow::Cow::Owned(Value::Array(elements.into())));
						pc += 1;
					}
					Copied::Spent => {
						return Ok(Step::Yield {
							resume: pc,
						});
					}
					Copied::Refused => unreachable!("a copy that refuses nothing"),
				}
			}
			Instr::CollectPush {
				collection,
				value,
			} => {
				let value = read_value(regs, *value)?;
				if let Scalar::Value(std::borrow::Cow::Owned(held)) =
					&mut regs[*collection as usize]
				{
					match held {
						Value::Array(array) => array.push(value),
						Value::Set(set) => {
							set.insert(value);
						}
						_ => {}
					}
				}
				pc += 1;
			}
			Instr::InvalidControlFlow => {
				return Err(anyhow::Error::new(crate::exec::Error::InvalidControlFlow));
			}
			Instr::Throw {
				src,
			} => {
				let value = read_value(regs, *src)?;
				return Err(anyhow::Error::new(crate::exec::Error::Thrown(value.to_raw_string())));
			}
			Instr::SortKey {
				key,
				dst,
			} => {
				let value = state.sorted_keys.get(*key as usize).cloned().ok_or_else(|| {
					anyhow::anyhow!("a sort key read by a row the sort did not hand over")
				})?;
				regs[*dst as usize] = Scalar::Value(std::borrow::Cow::Owned(value));
				pc += 1;
			}
			Instr::Fetch {
				..
			} => {
				return Ok(Step::Fetch {
					at: pc,
				});
			}
			Instr::CallAllowed {
				call,
				frame,
			} => {
				return Ok(Step::CallAllowed {
					call: *call,
					frame: *frame,
					resume: pc + 1,
				});
			}
			Instr::Call {
				call,
				args,
				frame,
				..
			} => {
				for &at in &program.calls[*call as usize].closure_args {
					if let Some(&arg) = args.get(at as usize)
						&& unlinked(program, row, regs, state, *frame, arg)?
					{
						return Ok(Step::NeedUnit {
							at: pc,
						});
					}
				}
				return Ok(Step::Call {
					at: pc,
				});
			}
			Instr::CallIndexFn {
				..
			} => {
				return Ok(Step::IndexFn {
					at: pc,
				});
			}
			Instr::Matches {
				..
			} => {
				return Ok(Step::Matches {
					at: pc,
				});
			}
			Instr::ExternalEnter {
				..
			}
			| Instr::ExternalCall {
				..
			} => {
				return Ok(Step::External {
					at: pc,
				});
			}
			Instr::Degree {
				..
			} => {
				return Ok(Step::Degree {
					at: pc,
				});
			}
			Instr::DocumentRoot {
				or,
				dst,
			} => {
				regs[*dst as usize] = if let Some(root) = &state.root {
					Scalar::Value(std::borrow::Cow::Owned(root.clone()))
				} else if state.rowless || state.undocumented {
					if let Some(refusal) = row.forbids() {
						return Err(refusal);
					}
					read(regs, *or)?
				} else {
					Scalar::Value(std::borrow::Cow::Owned(row.record()?))
				};
				pc += 1;
			}
			Instr::NewSet {
				dst,
			} => {
				regs[*dst as usize] =
					Scalar::Value(std::borrow::Cow::Owned(Value::Set(Default::default())));
				pc += 1;
			}
			Instr::Subquery {
				..
			} => {
				return Ok(Step::Subquery {
					at: pc,
				});
			}
			Instr::Lookup {
				..
			} => {
				return Ok(Step::Lookup {
					at: pc,
				});
			}
			Instr::Destructure {
				..
			} => {
				return Ok(Step::Destructure {
					at: pc,
				});
			}
			Instr::JudgeCopy {
				..
			} => {
				return Ok(Step::JudgeCopy {
					at: pc,
				});
			}
			Instr::Omit {
				..
			} => {
				return Ok(Step::Omit {
					at: pc,
				});
			}
			Instr::Signal {
				..
			} => {
				return Ok(Step::Signal {
					at: pc,
				});
			}
			Instr::Recurse {
				..
			}
			| Instr::RepeatRecurse {
				..
			} => {
				return Ok(Step::Recurse {
					at: pc,
				});
			}
			Instr::KnnRank {
				src,
				path,
				rank,
				dst,
				none,
			} => {
				let query: &[crate::val::Number] = match &rank.vector {
					super::program::KnnVector::Literal(vector) => vector,
					super::program::KnnVector::Query(sort) => {
						match state
							.knn_queries
							.get(*sort as usize)
							.and_then(|cell| cell.get())
							.and_then(KnnQueryOutcome::vector)
						{
							Some(query) => query.as_ref(),
							None => {
								return Ok(Step::KnnQuery {
									at: pc,
								});
							}
						}
					}
				};
				let value = read_value(regs, *src)?;
				// A row ranks only with a vector of the query's dimension:
				// `extract_vector` refuses a longer one, and a shorter one is
				// refused here, before a distance could only fail on it.
				let mut record = Vec::with_capacity(query.len());
				let ranked = (crate::exec::operators::extract_vector(
					&value,
					&program.paths[*path as usize],
					&mut record,
					query.len(),
				) && record.len() == query.len())
				.then(|| {
					crate::idx::kernels::vector::distance_compute(&rank.distance, &record, query)
						.ok()
				})
				.flatten();
				match ranked {
					Some(distance) => {
						regs[*dst as usize] =
							Scalar::Value(std::borrow::Cow::Owned(Value::Number(distance)));
						pc += 1;
					}
					None => jump!(*none as usize),
				}
			}
			Instr::Extract {
				src,
				path,
				dst,
			} => {
				let value = read_value(regs, *src)?;
				let key = program.paths[*path as usize].extract(&value).into_owned();
				regs[*dst as usize] = Scalar::Value(std::borrow::Cow::Owned(key));
				pc += 1;
			}
			Instr::Mock {
				mock,
				limit,
				dst,
			} => {
				let ids = mock.as_ref().clone().into_iter();
				if ids
					.size_hint()
					.1
					.is_none_or(|count| count.saturating_mul(std::mem::size_of::<Value>()) > *limit)
				{
					anyhow::bail!("Mock range exceeds allocation limit");
				}
				let ids: Vec<Value> = ids.map(Value::RecordId).collect();
				regs[*dst as usize] =
					Scalar::Value(std::borrow::Cow::Owned(Value::Array(ids.into())));
				pc += 1;
			}
			Instr::MakeClosure {
				..
			} => {
				return Ok(Step::Closure {
					at: pc,
				});
			}
			Instr::Invoke {
				target,
				frame,
				..
			} => {
				if unlinked(program, row, regs, state, *frame, *target)? {
					return Ok(Step::NeedUnit {
						at: pc,
					});
				}
				return Ok(Step::Closure {
					at: pc,
				});
			}
			Instr::ClosureField {
				name,
				recv,
				dst,
			} => {
				let recv = read_value(regs, *recv)?;
				let closure = match &recv {
					Value::Object(obj) => match obj.get(name.as_str()) {
						Some(closure @ Value::Closure(_)) => Some(closure.clone()),
						_ => None,
					},
					_ => None,
				};
				let Some(closure) = closure else {
					return Err(crate::exec::Error::InvalidFunction {
						name: name.to_string(),
						message: format!("no such method found for the {} type", recv.kind_of()),
					}
					.into());
				};
				regs[*dst as usize] = Scalar::Value(std::borrow::Cow::Owned(closure));
				pc += 1;
			}
			Instr::RequireClosure {
				src,
			} => {
				let target = read(regs, *src)?;
				if !matches!(&target, Scalar::Value(v) if matches!(v.as_ref(), Value::Closure(_))) {
					return Err(crate::exec::Error::InvalidFunction {
						name: "ANONYMOUS".to_string(),
						message: format!("'{}' is not a function", target.to_value().kind_of()),
					}
					.into());
				}
				pc += 1;
			}
			Instr::ArrayArg {
				src,
				dst,
				call,
				position,
			} => {
				// An array is copied a stretch at a time, as `FilterBegin`
				// copies one; anything else is converted whole.
				let field;
				if let Some(Value::Array(array)) = peek!(*src, field) {
					match copied(state, Bulk::at(pipeline, pc, &array.0), &array.0, |_| false) {
						Copied::Done(values) => {
							regs[*dst as usize] =
								Scalar::Value(std::borrow::Cow::Owned(Value::Array(values.into())));
							pc += 1;
							continue;
						}
						Copied::Spent => {
							return Ok(Step::Yield {
								resume: pc,
							});
						}
						Copied::Refused => unreachable!("a copy that refuses nothing"),
					}
				}
				let value = read_value(regs, *src)?;
				let array = value.coerce_to::<crate::val::Array>().map_err(|e| {
					crate::expr::Error::InvalidFunctionArguments {
						name: program.calls[*call as usize].name.clone(),
						message: format!("Argument {position} was the wrong type. {e}"),
					}
				})?;
				regs[*dst as usize] = Scalar::Value(std::borrow::Cow::Owned(Value::Array(array)));
				pc += 1;
			}
			Instr::ElemNext {
				array,
				pos,
				elem,
				index,
				done,
			} => {
				let at = match &regs[*pos as usize] {
					Scalar::Number(crate::val::Number::Int(at)) => *at as usize,
					_ => 0,
				};
				let next = match &regs[*array as usize] {
					Scalar::Value(value) => match value.as_ref() {
						Value::Array(items) => items.get(at).cloned(),
						_ => None,
					},
					_ => None,
				};
				match next {
					Some(item) => {
						regs[*elem as usize] = Scalar::Value(std::borrow::Cow::Owned(item));
						regs[*index as usize] = Scalar::Number(crate::val::Number::Int(at as i64));
						regs[*pos as usize] =
							Scalar::Number(crate::val::Number::Int(at as i64 + 1));
						pc += 1;
					}
					None => jump!(*done as usize),
				}
			}
			Instr::ClosureBind {
				closure,
				args,
				params,
			} => {
				let spec = &program.closures[*closure as usize];
				let args = args
					.iter()
					.map(|arg| read_value(regs, *arg))
					.collect::<anyhow::Result<Vec<_>>>()?;
				let bound = crate::exec::vm::calls::bind_closure_args(&spec.args, args)?;
				for (i, value) in bound.into_iter().enumerate() {
					regs[*params as usize + i] = Scalar::Value(std::borrow::Cow::Owned(value));
				}
				pc += 1;
			}
			Instr::ClosureReturn {
				closure,
				src,
				dst,
			} => {
				let spec = &program.closures[*closure as usize];
				let value = read_value(regs, *src)?;
				let value = crate::exec::vm::calls::helpers::validate_return(
					"ANONYMOUS",
					spec.returns.as_ref(),
					value,
				)?;
				regs[*dst as usize] = Scalar::Value(std::borrow::Cow::Owned(value));
				pc += 1;
			}
			Instr::Dive {
				..
			} => {
				return Ok(Step::Function {
					at: pc,
				});
			}
			Instr::FunctionEnter {
				..
			}
			| Instr::FunctionCall {
				..
			}
			| Instr::FunctionBind {
				..
			}
			| Instr::FunctionReturn {
				..
			} => {
				return Ok(Step::Function {
					at: pc,
				});
			}
			Instr::RecordId {
				record,
				dst,
			} => {
				let value = program.record_ids[*record as usize]
					.build(&mut |operand| read_value(regs, operand))?;
				regs[*dst as usize] = Scalar::Value(std::borrow::Cow::Owned(value));
				pc += 1;
			}
			Instr::MethodResolve {
				method,
				recv,
				closure,
			} => {
				let spec = &program.methods[*method as usize];
				// Only a heap value is borrowed; any other scalar is cheap to
				// make a value of.
				let scalar = read(regs, *recv)?;
				let owned;
				let value = match &scalar {
					Scalar::Value(value) => &**value,
					other => {
						owned = other.to_value();
						&owned
					}
				};
				if spec.resolves(value) {
					pc += 1;
				} else if let Some(closure) = closure
					&& let Value::Object(object) = value
					&& let Some(Value::Closure(_)) = object.get(spec.name.as_str())
				{
					jump!(*closure as usize);
				} else {
					return Err(anyhow::Error::new(crate::exec::Error::InvalidFunction {
						name: spec.name.clone(),
						message: format!("no such method found for the {} type", value.kind_of()),
					}));
				}
			}
			Instr::Method {
				method,
				args,
				frame,
				..
			} => {
				for &at in &program.methods[*method as usize].closure_args {
					if let Some(&arg) = args.get(at as usize)
						&& unlinked(program, row, regs, state, *frame, arg)?
					{
						return Ok(Step::NeedUnit {
							at: pc,
						});
					}
				}
				return Ok(Step::Method {
					at: pc,
				});
			}
			Instr::MethodClosure {
				method,
				recv,
				frame,
				..
			} => {
				let name = program.methods[*method as usize].name.as_str();
				let digest = closure_digest(program, row, regs, *recv, |value| match value {
					Value::Object(object) => object.get(name).and_then(super::closure::digest_of),
					_ => None,
				})?;
				if digest.is_some_and(|digest| !state.has_linked(*frame, digest)) {
					return Ok(Step::NeedUnit {
						at: pc,
					});
				}
				return Ok(Step::Method {
					at: pc,
				});
			}
			Instr::JumpIfNullish {
				cond,
				nullish,
				target,
			} => {
				if read_value(regs, *cond)?.is_nullish() == *nullish {
					jump!(*target as usize);
				} else {
					pc += 1;
				}
			}
			Instr::JumpIfNone {
				cond,
				none,
				target,
			} => {
				if read(regs, *cond)?.is_none() == *none {
					jump!(*target as usize);
				} else {
					pc += 1;
				}
			}
			Instr::JumpIfNotRecordId {
				cond,
				target,
			} => {
				let record = matches!(read(regs, *cond)?, Scalar::Value(value) if matches!(value.as_ref(), Value::RecordId(_)));
				if record {
					pc += 1;
				} else {
					jump!(*target as usize);
				}
			}
			Instr::JumpIfNotObject {
				cond,
				target,
			} => {
				let object = matches!(read(regs, *cond)?, Scalar::Value(value) if matches!(value.as_ref(), Value::Object(_)));
				if object {
					pc += 1;
				} else {
					jump!(*target as usize);
				}
			}
			Instr::JumpIf {
				cond,
				truthy,
				target,
			} => {
				if read(regs, *cond)?.is_truthy() == *truthy {
					jump!(*target as usize);
				} else {
					pc += 1;
				}
			}
			Instr::Filter {
				cond,
				reject,
			} => {
				let answer = read(regs, *cond)?.is_truthy();
				if answer == *reject {
					return Ok(Step::Next);
				}
				pc += 1;
			}
			Instr::Skip {
				limit,
				target,
			} => {
				state.reach_paging(*limit);
				let paging = &mut state.paging[*limit as usize];
				if paging.skip > 0 {
					paging.skip -= 1;
					jump!(*target as usize);
				} else {
					pc += 1;
				}
			}
			Instr::Take {
				limit,
			} => {
				state.reach_paging(*limit);
				if let Some(remaining) = &mut state.paging[*limit as usize].remaining {
					*remaining = remaining.saturating_sub(1);
				}
				pc += 1;
			}
			Instr::JumpIfLimitReached {
				limit,
				done,
			} => {
				if state.paging[*limit as usize].remaining == Some(0) {
					jump!(*done as usize);
				} else {
					pc += 1;
				}
			}
			Instr::SeedObject {
				dst,
			} => {
				let seed = match regs[*dst as usize].to_value() {
					Value::Object(object) => object,
					Value::Geometry(geometry) => geometry.as_object(),
					_ => Object::default(),
				};
				regs[*dst as usize] = Scalar::Value(std::borrow::Cow::Owned(Value::Object(seed)));
				pc += 1;
			}
			Instr::NewObject {
				dst,
			} => {
				regs[*dst as usize] =
					Scalar::Value(std::borrow::Cow::Owned(Value::Object(Object::default())));
				pc += 1;
			}
			Instr::LoadPath {
				dst,
				field,
				rest,
			} => {
				let base = row.field(&program.fields[*field as usize])?.to_value();
				let value = program.paths[*rest as usize].extract(&base).into_owned();
				regs[*dst as usize] = Scalar::Value(std::borrow::Cow::Owned(value));
				pc += 1;
			}
			Instr::LoadRow {
				dst,
			} => {
				regs[*dst as usize] = match row.borrowed() {
					Some(value) => Scalar::Value(std::borrow::Cow::Borrowed(value)),
					None => Scalar::Value(std::borrow::Cow::Owned(row.record()?)),
				};
				pc += 1;
			}
			Instr::FetchRow {
				dst,
				..
			} => {
				if row.is_record_id() {
					return Ok(Step::FetchRow {
						at: pc,
					});
				}
				regs[*dst as usize] = Scalar::Value(std::borrow::Cow::Owned(row.record()?));
				pc += 1;
			}
			Instr::RemoveField {
				object,
				name,
			} => {
				if let Scalar::Value(std::borrow::Cow::Borrowed(borrowed)) = &regs[*object as usize]
				{
					regs[*object as usize] =
						Scalar::Value(std::borrow::Cow::Owned((*borrowed).clone()));
				}
				if let Scalar::Value(std::borrow::Cow::Owned(Value::Object(object))) =
					&mut regs[*object as usize]
				{
					object.remove(program.fields[*name as usize].as_str());
				}
				pc += 1;
			}
			Instr::RemovePath {
				object,
				path,
			} => {
				if let Scalar::Value(std::borrow::Cow::Borrowed(borrowed)) = &regs[*object as usize]
				{
					regs[*object as usize] =
						Scalar::Value(std::borrow::Cow::Owned((*borrowed).clone()));
				}
				if let Scalar::Value(std::borrow::Cow::Owned(object)) = &mut regs[*object as usize]
				{
					remove_path(object, &program.paths[*path as usize].0);
				}
				pc += 1;
			}
			Instr::SetField {
				object,
				path,
				value,
			} => {
				let value = read_value(regs, *value)?;
				// An object loaded from a constant or the row is borrowed; it
				// becomes the register's own before it is changed.
				if let Scalar::Value(std::borrow::Cow::Borrowed(borrowed)) = &regs[*object as usize]
				{
					regs[*object as usize] =
						Scalar::Value(std::borrow::Cow::Owned((*borrowed).clone()));
				}
				if let Scalar::Value(std::borrow::Cow::Owned(object)) = &mut regs[*object as usize]
				{
					object.set_at_field_path_owned(&program.paths[*path as usize], value);
				}
				pc += 1;
			}
			Instr::Load {
				dst,
				src,
			} => {
				let value = read(regs, *src)?;
				regs[*dst as usize] = value;
				pc += 1;
			}
			Instr::BuildObject {
				dst,
				fields,
			} => {
				let mut object = Value::Object(Object::default());
				for (path, value) in fields.iter() {
					let value = read_value(regs, *value)?;
					object.set_at_field_path_owned(&program.paths[*path as usize], value);
				}
				regs[*dst as usize] = Scalar::Value(std::borrow::Cow::Owned(object));
				pc += 1;
			}
			Instr::BuildObjectEmit {
				fields,
			} => {
				// A statement whose rows leave encoded writes an object of
				// top-level fields straight into its frame, from the values
				// where they lie.
				if state.row_frame.is_some() && fields.len() <= FLAT_FIELDS {
					let mut flat: [(&str, Scalar<'_>); FLAT_FIELDS] =
						std::array::from_fn(|_| ("", Scalar::None));
					let mut written = true;
					for (at, (path, value)) in fields.iter().enumerate() {
						match program.paths[*path as usize].0.as_slice() {
							[FieldPathPart::Field(name)] => {
								flat[at] = (name.as_str(), read(regs, *value)?);
							}
							_ => {
								written = false;
								break;
							}
						}
					}
					let flat = &mut flat[..fields.len()];
					flat.sort_unstable_by(|a, b| a.0.cmp(b.0));
					// A key set twice keeps its last value, which the object
					// built below answers.
					if written && flat.windows(2).all(|w| w[0].0 != w[1].0) {
						state.drain_output_into_frame()?;
						if let Some(frame) = &mut state.row_frame {
							frame.push_fields(flat)?;
						}
						return Ok(Step::Next);
					}
				}
				let mut object = Value::Object(Object::default());
				for (path, value) in fields.iter() {
					let value = read_value(regs, *value)?;
					object.set_at_field_path_owned(&program.paths[*path as usize], value);
				}
				state.output.push(object);
				return Ok(Step::Next);
			}
			Instr::EmitEnd {
				src,
			} => {
				match src {
					Operand::Reg(ROW) => {
						state.pending = Some(Pending::Emit);
					}
					Operand::Reg(reg) => {
						let value = match std::mem::replace(&mut regs[*reg as usize], Scalar::None)
						{
							Scalar::Value(std::borrow::Cow::Owned(value)) => value,
							other => other.to_value(),
						};
						state.output.push(value);
					}
					other => {
						let value = read_value(regs, *other)?;
						state.output.push(value);
					}
				}
				return Ok(Step::Next);
			}
			Instr::Emit {
				src,
			} => {
				if *src == ROW {
					state.pending = Some(Pending::Emit);
				} else {
					let value = match std::mem::replace(&mut regs[*src as usize], Scalar::None) {
						Scalar::Value(std::borrow::Cow::Owned(value)) => value,
						other => other.to_value(),
					};
					state.output.push(value);
				}
				pc += 1;
			}
			Instr::SplitPush {
				split,
				src,
			} => {
				let value = (*src != ROW).then(|| {
					match std::mem::replace(&mut regs[*src as usize], Scalar::None) {
						Scalar::Value(std::borrow::Cow::Owned(value)) => value,
						other => other.to_value(),
					}
				});
				state.pending = Some(Pending::Split {
					split: *split,
					value,
				});
				pc += 1;
			}
			Instr::Distinct {
				distinct,
				src,
				target,
			} => {
				let value = if *src == ROW {
					row.record()?
				} else {
					regs[*src as usize].to_value()
				};
				let fresh = match state.seen[*distinct as usize].insert(&value, state.build_budget)
				{
					Ok(fresh) => fresh,
					Err(crate::expr::ControlFlow::Err(error)) => return Err(error),
					Err(other) => bail!("a distinct set ended its row with {other:?}"),
				};
				if fresh {
					if state.capture_distinct {
						state.distinct_taken = Some(value);
					}
					pc += 1;
				} else {
					jump!(*target as usize);
				}
			}
			Instr::JoinProbe {
				join,
				keys,
				count,
			} => {
				let keys = (*keys..*keys + *count)
					.map(|r| std::mem::replace(&mut regs[r as usize], Scalar::None).to_value())
					.collect();
				state.pending = Some(Pending::Join {
					join: *join,
					keys,
				});
				pc += 1;
			}
			Instr::VerdictJump {
				cell,
				admitted,
				refused,
			} => {
				pc = match state.verdicts[*cell as usize] {
					Some(true) => *admitted as usize,
					Some(false) => *refused as usize,
					None => pc + 1,
				};
			}
			Instr::VerdictSet {
				cell,
				admits,
			} => {
				#[cfg(test)]
				VERDICTS_JUDGED.with(|judged| judged.set(judged.get() + 1));
				state.verdicts[*cell as usize] = Some(*admits);
				pc += 1;
			}
			Instr::JoinPair {
				join,
			} => {
				state.pending = Some(Pending::JoinPair {
					join: *join,
				});
				pc += 1;
			}
			Instr::SortPush {
				sort,
				keys,
				count,
				src,
			} => {
				// A row that orders no earlier than the last row the sort keeps
				// is dropped before its keys are gathered or the row is taken.
				let reaches = match state.sort_floor(program, *sort) {
					Some(floor) => {
						let order = &program.sorts[*sort as usize].keys;
						let mut ordering = Ordering::Equal;
						for ((reg, bound), key) in (*keys..*keys + *count).zip(floor).zip(order) {
							ordering = compare_scalar_key(&regs[reg as usize], bound, *key);
							if ordering != Ordering::Equal {
								break;
							}
						}
						ordering == Ordering::Less
					}
					None => true,
				};
				if reaches {
					// A key vector a bounded sort pruned, with room for the arrival
					// a merging sort stamps after the keys.
					let mut gathered = state.spare_sort_keys.pop().unwrap_or_default();
					gathered.clear();
					gathered.reserve(*count as usize + 1);
					gathered.extend((*keys..*keys + *count).map(|r| {
						std::mem::replace(&mut regs[r as usize], Scalar::None).to_value()
					}));
					let keys = gathered;
					if *src == ROW {
						state.pending = Some(Pending::Sort {
							sort: *sort,
							keys,
						});
					} else {
						let value = match std::mem::replace(&mut regs[*src as usize], Scalar::None)
						{
							Scalar::Value(std::borrow::Cow::Owned(value)) => value,
							other => other.to_value(),
						};
						state.sort_value(program, *sort, keys, value);
					}
				}
				pc += 1;
			}
			Instr::GroupKey {
				aggregate,
				index,
				value,
			} => {
				let value = read_value(regs, *value)?;
				if let Some(groups) = &mut state.groups[*aggregate as usize] {
					groups.set_key(*index as usize, value);
				}
				pc += 1;
			}
			Instr::GroupSelect {
				aggregate,
			} => {
				let functions = &state.aggregates.functions[*aggregate as usize];
				if let Some(groups) = &mut state.groups[*aggregate as usize] {
					groups.select(functions);
				}
				pc += 1;
			}
			Instr::GroupFirstNeeded {
				aggregate,
				field,
				done,
			} => {
				let constant = program.aggregates[*aggregate as usize].fields[*field as usize]
					.fallback_is_constant;
				if state.groups[*aggregate as usize].as_ref().is_some_and(|groups| {
					first_value_needed(constant, groups.first(*field as usize))
				}) {
					pc += 1;
				} else {
					jump!(*done as usize);
				}
			}
			Instr::GroupFirstSet {
				aggregate,
				field,
				src,
			} => {
				let value = read_value(regs, *src)?;
				if let Some(groups) = &mut state.groups[*aggregate as usize] {
					groups.grew(crate::exec::memory::size::value_bytes(&value));
					*groups.first_mut(*field as usize) = Some(value);
				}
				pc += 1;
			}
			Instr::TryNone {
				catch,
				dst,
			} => {
				state.tries.push(Region::Try {
					catch: *catch as usize,
					dst: *dst,
				});
				pc += 1;
			}
			Instr::MethodRegion {
				method,
			} => {
				state.tries.push(Region::Method {
					method: *method,
				});
				pc += 1;
			}
			Instr::PermissionRegion {
				check,
			} => {
				state.tries.push(Region::Permission {
					check: *check,
				});
				pc += 1;
			}
			Instr::TryEnd => {
				state.tries.pop();
				pc += 1;
			}
			Instr::AccUpdate {
				aggregate,
				field,
				agg,
				value,
			} => {
				let value = read_value(regs, *value)?;
				if let Some(groups) = state.groups[*aggregate as usize].as_mut() {
					let mut grew = 0;
					if let Some(accumulator) = groups.accumulator(*field as usize, *agg as usize) {
						if accumulator.holds_inputs() {
							grew = crate::exec::memory::size::value_bytes(&value);
						}
						if let Err(e) = accumulator.update(value) {
							tracing::debug!(error = %e, "Accumulator update failed, skipping value");
						}
					}
					groups.grew(grew);
				}
				pc += 1;
			}
			Instr::Jump {
				target,
			} => {
				jump!(*target as usize);
			}
			Instr::Next {
				list,
				at,
				dst,
				done,
			} => {
				let index = match &regs[*at as usize] {
					Scalar::Number(crate::val::Number::Int(i)) => *i as usize,
					other => bail!("a list position that is not an integer: {other:?}"),
				};
				let element = match &regs[*list as usize] {
					Scalar::Value(value) => match value.as_ref() {
						Value::Array(array) => array.get(index).cloned(),
						other => bail!("stepping through {other:?}, which is not an array"),
					},
					other => bail!("stepping through {other:?}, which is not an array"),
				};
				match element {
					Some(element) => {
						regs[*dst as usize] = Scalar::Value(std::borrow::Cow::Owned(element));
						regs[*at as usize] =
							Scalar::Number(crate::val::Number::Int(index as i64 + 1));
						pc += 1;
					}
					None => jump!(*done as usize),
				}
			}
			Instr::Store(_) => {
				return Ok(Step::Store {
					at: pc,
				});
			}
			Instr::Block(_) => {
				return Ok(Step::Block {
					at: pc,
				});
			}
			Instr::Write(_) => {
				return Ok(Step::Write {
					at: pc,
				});
			}
			Instr::Ir(instr) => {
				match super::ir_exec::run(instr, program, pipeline, row, regs, state, pc)? {
					IrFlow::Fall => pc += 1,
					IrFlow::Goto(target) => jump!(target),
					IrFlow::EndRow => return Ok(Step::Next),
					IrFlow::StopPipeline => return Ok(Step::Stop),
					IrFlow::Host => {
						return Ok(Step::Ir {
							at: pc,
						});
					}
				}
			}
			Instr::EndRow => return Ok(Step::Next),
			Instr::StopPipeline => return Ok(Step::Stop),
		}
	}
}

/// Removes the location `path` names from `value`, as `Value::cut` does: a field leaves its object,
/// and an element leaves its array with the gap closed. A field step on an array applies to each
/// element, an index step on an object names the key it spells, and a step into anything
/// else removes nothing. A lookup names no location in a value, so a path
/// holding one removes nothing.
fn remove_path(value: &mut Value, path: &[FieldPathPart]) {
	let Some((part, rest)) = path.split_first() else {
		return;
	};
	match value {
		Value::Object(object) => match part {
			FieldPathPart::Field(name) => {
				if rest.is_empty() {
					object.remove(name.as_str());
				} else if let Some(nested) = object.get_mut(name.as_str()) {
					remove_path(nested, rest);
				}
			}
			FieldPathPart::Index(idx) => {
				let key = idx.to_string();
				if rest.is_empty() {
					object.remove(key.as_str());
				} else if let Some(nested) = object.get_mut(key.as_str()) {
					remove_path(nested, rest);
				}
			}
			FieldPathPart::First | FieldPathPart::Last | FieldPathPart::Lookup(_) => {}
		},
		Value::Array(array) => match part {
			FieldPathPart::Field(_) => {
				for element in array.iter_mut() {
					remove_path(element, path);
				}
			}
			FieldPathPart::Index(idx) => {
				if rest.is_empty() {
					if *idx < array.len() {
						array.remove(*idx);
					}
				} else if let Some(nested) = array.get_mut(*idx) {
					remove_path(nested, rest);
				}
			}
			FieldPathPart::First => {
				if rest.is_empty() {
					if !array.is_empty() {
						array.remove(0);
					}
				} else if let Some(nested) = array.first_mut() {
					remove_path(nested, rest);
				}
			}
			FieldPathPart::Last => {
				if rest.is_empty() {
					array.pop();
				} else if let Some(nested) = array.last_mut() {
					remove_path(nested, rest);
				}
			}
			FieldPathPart::Lookup(_) => {}
		},
		_ => {}
	}
}

/// Publish `first_key`, the first sort key of the last row a sort that keeps a bounded number of
/// rows holds once it holds that many, to the threshold cell of its execution. Sorts that share a
/// cell (the morsels of one scan, and the statement) each publish what they hold, so the cell keeps
/// the threshold that orders first: a threshold only tightens.
fn publish_threshold(
	cell: &crate::exec::topk_pushdown::TopKThresholdCell,
	first_key: &Value,
	keys: &[KeyOrder],
) {
	let Some(order) = keys.get(..1) else {
		return;
	};
	cell.tighten(first_key.clone(), |new, held| {
		compare_keys(std::slice::from_ref(new), std::slice::from_ref(held), order) == Ordering::Less
	});
}

/// An aggregate's groups before any row has been folded into them.
fn fresh_groups(
	sink: &super::program::AggregateSink,
	functions: &[Vec<std::sync::Arc<dyn AggregateFunction>>],
) -> Groups {
	Groups::new(sink.keys, functions)
}

/// A row a sort holds: the row, or a record read in place as its stored
/// bytes, decoded only once the sort hands it on. A record that does not
/// decode fails the statement only if it keeps its place: a record the sort
/// drops is never decoded.
#[derive(Debug)]
pub(crate) enum SortRow {
	Value(Value),
	/// The record's key and value bytes in one buffer, the key the first
	/// `split` bytes.
	Stored {
		bytes: Vec<u8>,
		split: usize,
	},
	/// The record's key and value bytes in its sort's slab
	/// ([`State::sort_slabs`]): the key at `start..split`, the value at
	/// `split..end`.
	Slab {
		start: usize,
		split: usize,
		end: usize,
	},
}

impl SortRow {
	/// The row, decoded when it is held as stored bytes. A row held in a
	/// slab is decoded with [`Self::into_value_in`].
	pub(super) fn into_value(self) -> anyhow::Result<Value> {
		self.into_value_in(&[])
	}

	/// The row, decoded when it is held as stored bytes, those of a slab row
	/// read from `slab`, its sort's slab.
	///
	/// Stored bytes entered the sort through a scan that admitted their
	/// generation in the same transaction, so the decode checks none: it reads
	/// the document or `revision` encoding and refuses any other envelope. A
	/// packed row never enters a sort as stored bytes, so the decode takes no
	/// layouts.
	pub(super) fn into_value_in(self, slab: &[u8]) -> anyhow::Result<Value> {
		match self {
			SortRow::Value(value) => Ok(value),
			SortRow::Stored {
				bytes,
				split,
			} => {
				let (key, value) = bytes.split_at(split);
				super::row::StoredRow::new(key, value).record()
			}
			SortRow::Slab {
				start,
				split,
				end,
			} => {
				let (Some(key), Some(value)) = (slab.get(start..split), slab.get(split..end))
				else {
					anyhow::bail!("a sorted row outside its sort's slab");
				};
				super::row::StoredRow::new(key, value).record()
			}
		}
	}

	/// The stored key and value bytes of a row held as stored bytes, those of
	/// a slab row in `slab`; `None` for a row held as a value.
	pub(super) fn stored<'a>(&'a self, slab: &'a [u8]) -> Option<(&'a [u8], &'a [u8])> {
		match self {
			SortRow::Value(_) => None,
			SortRow::Stored {
				bytes,
				split,
			} => Some(bytes.split_at(*split)),
			SortRow::Slab {
				start,
				split,
				end,
			} => Some((slab.get(*start..*split)?, slab.get(*split..*end)?)),
		}
	}
}

/// Charge the walk a comparison of two collections makes, whole: an
/// operator such as `CONTAINS` or `=` reads each operand in one go, and the
/// row yields at its next yield point.
#[inline]
fn charge_walked(left: &Scalar<'_>, right: &Scalar<'_>) {
	let walked = left
		.as_whole()
		.map_or(0, super::fuel::weight)
		.saturating_add(right.as_whole().map_or(0, super::fuel::weight));
	if walked > 0 {
		super::fuel::charge(walked);
	}
}

/// A bulk instruction's copy of a collection, stopped where the fuel ran out.
pub(super) struct Bulk {
	at: BulkAt,
	built: Built,
}

/// The instruction, and the collection, a [`Bulk`] copy belongs to: a row
/// resumes it only at the instruction that began it, over the same
/// collection, which its registers still hold.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct BulkAt {
	pipeline: usize,
	pc: usize,
	source: usize,
	len: usize,
	/// Whether the copy may stop for a yield: not inside the storage cursor,
	/// where a body never yields.
	yields: bool,
}

impl Bulk {
	fn at<T>(pipeline: &Pipeline, pc: usize, source: &[T]) -> BulkAt {
		BulkAt {
			pipeline: std::ptr::from_ref(pipeline) as usize,
			pc,
			source: source.as_ptr() as usize,
			len: source.len(),
			yields: !pipeline.in_place,
		}
	}
}

/// What a [`Bulk`] copy has built so far.
enum Built {
	Values(Vec<Value>),
	Entries(Vec<(surrealdb_strand::Strand, Value)>),
}

/// An element a bulk instruction copies.
trait Bulked: Clone + Sized {
	fn built(values: Vec<Self>) -> Built;
	fn resumed(built: Built) -> Option<Vec<Self>>;
}

impl Bulked for Value {
	fn built(values: Vec<Self>) -> Built {
		Built::Values(values)
	}

	fn resumed(built: Built) -> Option<Vec<Self>> {
		match built {
			Built::Values(values) => Some(values),
			Built::Entries(_) => None,
		}
	}
}

impl Bulked for (surrealdb_strand::Strand, Value) {
	fn built(entries: Vec<Self>) -> Built {
		Built::Entries(entries)
	}

	fn resumed(built: Built) -> Option<Vec<Self>> {
		match built {
			Built::Entries(entries) => Some(entries),
			Built::Values(_) => None,
		}
	}
}

/// How far a stretch of a bulk copy got.
enum Copied<T> {
	Done(T),
	/// The fuel ran out: the row yields and resumes the copy.
	Spent,
	/// An element the instruction does not take whole was met.
	Refused,
}

/// A copy of `source` for the bulk instruction `at`, taken a stretch at a
/// time, a unit an element, from where an earlier stretch stopped: once a
/// stretch spends the fuel the copy is kept in the state and the row yields,
/// resuming here. Refused, with nothing kept, at the first element `refuse`
/// holds for.
fn copied<T: Bulked>(
	state: &mut State,
	at: BulkAt,
	source: &[T],
	refuse: impl Fn(&T) -> bool,
) -> Copied<Vec<T>> {
	let mut built = match state.bulk.take() {
		Some(Bulk {
			at: held,
			built,
		}) if held == at => T::resumed(built).unwrap_or_else(|| Vec::with_capacity(source.len())),
		_ => Vec::with_capacity(source.len()),
	};
	while built.len() < source.len() {
		// A copy that finds the fuel already spent yields before it copies.
		if at.yields && super::fuel::spent() {
			state.bulk = Some(Bulk {
				at,
				built: T::built(built),
			});
			return Copied::Spent;
		}
		let from = built.len();
		let take = surrealdb_runtime::fuel::stretch().min(source.len() - from);
		let part = &source[from..from + take];
		if part.iter().any(&refuse) {
			return Copied::Refused;
		}
		built.extend_from_slice(part);
		if super::fuel::charge(super::fuel::units(take)) && at.yields && built.len() < source.len()
		{
			state.bulk = Some(Bulk {
				at,
				built: T::built(built),
			});
			return Copied::Spent;
		}
	}
	Copied::Done(built)
}

#[cfg(test)]
thread_local! {
	/// The most rows one prune, or one merge's run, of a bounded sort sorted
	/// on this thread.
	pub(super) static LARGEST_PRUNE: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The most rows a merging sort's arrivals place at once, held to the
/// quantum ([`State::merge_arrived`]).
const MERGE_STRETCH: usize = 256;

/// What a sort that keeps a bounded number of rows counts across its rows
/// ([`State::keep_sorted`]).
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct Arrivals {
	/// The most rows a run of a merging sort takes, or zero for a sort that
	/// prunes, decided by its first row: a sort whose limit, with a quarter
	/// more, exceeds the thread's quantum merges, so that no prune sorts more
	/// than a quantum of rows.
	pub(super) run: Option<usize>,
	/// Whether the sort holds as many rows as it keeps, so that the row at
	/// `keep - 1` is its floor.
	pub(super) floored: bool,
	/// The rows that have reached a merging sort, each stamped with its place
	/// among them.
	pub(super) arrived: u64,
	/// The rows a merging sort holds sorted at the front of its buffer.
	pub(super) kept: usize,
	/// A merging sort's merge under way.
	pub(super) merge: Option<Merge>,
}

impl Arrivals {
	/// The most rows a run of a sort keeping `keep` rows takes, or zero when
	/// it prunes.
	fn run(&mut self, keep: usize) -> usize {
		*self.run.get_or_insert_with(|| {
			let quantum = surrealdb_runtime::fuel::quantum() as usize;
			let slack = (keep / 4).max(16);
			if keep.saturating_add(slack) > quantum {
				slack.min(quantum).max(1)
			} else {
				0
			}
		})
	}
}

/// A merge of a sorted run into a merging sort's kept rows
/// ([`State::merge_arrived`]), from the greatest row down into `0..len`.
///
/// The places not yet written, `kept..kept + run`, hold neither kept nor run
/// rows, so the rows still to place move into them by swaps: the kept rows
/// that order after the run's greatest shift up as a block, or the run's
/// rows that order after the greatest kept row move across as one, each
/// block's length found by a gallop. The buffer holds the same rows after
/// every move.
#[derive(Debug, Clone, Copy)]
pub(super) struct Merge {
	/// The kept rows still to place, at `0..kept`.
	kept: usize,
	/// The run's rows still to place, at `run_at..run_at + run`.
	run: usize,
	run_at: usize,
	/// The rows the merge places, at `0..len`.
	len: usize,
	/// The rows cut, at `len..len + cut` once the merge is done.
	cut: usize,
	/// The rows an arriving row's share of the merge places.
	pace: usize,
	/// The rows the arrivals since the last moves have to place.
	owed: usize,
}

impl Merge {
	/// Place up to `most` rows, answering whether the merge is done.
	fn step(&mut self, rows: &mut [(Vec<Value>, SortRow)], keys: &[KeyOrder], most: usize) -> bool {
		let after = |rows: &[(Vec<Value>, SortRow)], a: usize, b: usize| {
			super::sort::compare_rows(&rows[a].0, &rows[b].0, keys) == Ordering::Greater
		};
		let mut left = most;
		while self.run > 0 && left > 0 {
			let top = self.run_at + self.run - 1;
			// The kept rows ordering after the run's greatest shift up by
			// the run's length, over places not yet written.
			let shift = gallop(self.kept.min(left), |n| after(rows, self.kept - n, top));
			if shift > 0 {
				let mut end = self.kept;
				let start = self.kept - shift;
				while end > start {
					let take = (end - start).min(self.run);
					let (low, high) = rows.split_at_mut(end);
					low[end - take..].swap_with_slice(&mut high[self.run - take..self.run]);
					end -= take;
				}
				self.kept -= shift;
				left -= shift;
				continue;
			}
			// The run's rows ordering after the greatest kept row move
			// across to the top of the places not yet written.
			let moved = match self.kept {
				0 => self.run.min(left),
				kept => gallop(self.run.min(left), |n| {
					after(rows, self.run_at + self.run - n, kept - 1)
				}),
			};
			let to = self.kept + self.run - moved;
			let from = self.run_at + self.run - moved;
			let (low, high) = rows.split_at_mut(from);
			low[to..to + moved].swap_with_slice(&mut high[..moved]);
			self.run -= moved;
			left -= moved;
		}
		self.run == 0
	}

	/// Take the merged rows as the sort's kept rows, and remove the rows cut:
	/// the rows that arrived since take their places, in no order, and the
	/// rows cut hand their buffers to the rows that reach the sort next.
	fn finish(
		self,
		rows: &mut Vec<(Vec<Value>, SortRow)>,
		arrivals: &mut Arrivals,
		spare_keys: &mut Vec<Vec<Value>>,
		spare_bytes: &mut Vec<Vec<u8>>,
	) {
		arrivals.kept = self.len;
		if self.cut == 0 {
			return;
		}
		let behind = rows.len() - (self.len + self.cut);
		let moved = behind.min(self.cut);
		let (low, high) = rows.split_at_mut(self.len + self.cut);
		low[self.len..self.len + moved].swap_with_slice(&mut high[behind - moved..]);
		let kept = rows.len() - self.cut;
		for row in rows.drain(kept..) {
			recycle_sort_row(spare_keys, spare_bytes, row);
		}
		super::fuel::charge(super::fuel::units(self.cut));
	}
}

/// The length of the prefix of `0..most` for which `holds(n + 1)` holds,
/// for a `holds` that holds of every length up to some and of none beyond:
/// found in a number of tests logarithmic in that length.
fn gallop(most: usize, holds: impl Fn(usize) -> bool) -> usize {
	// `low` lengths hold; `high` is past the last that may.
	let (mut low, mut high) = (0, most);
	let mut probe = 1;
	while probe <= most {
		if !holds(probe) {
			high = probe - 1;
			break;
		}
		low = probe;
		probe *= 2;
	}
	while low < high {
		let mid = low + (high - low).div_ceil(2);
		if holds(mid) {
			low = mid;
		} else {
			high = mid - 1;
		}
	}
	low
}

/// Hand a row a bounded sort dropped to the spare buffers the rows that
/// reach a sort next reuse: its key vector, and a stored record's bytes.
fn recycle_sort_row(
	spare_keys: &mut Vec<Vec<Value>>,
	spare_bytes: &mut Vec<Vec<u8>>,
	(keys, row): (Vec<Value>, SortRow),
) {
	spare_keys.push(keys);
	if let SortRow::Stored {
		bytes,
		..
	} = row
	{
		spare_bytes.push(bytes);
	}
}

pub(super) fn compare_keys(a: &[Value], b: &[Value], keys: &[KeyOrder]) -> Ordering {
	for ((a, b), key) in a.iter().zip(b).zip(keys) {
		let ordering = compare_key(a, b, *key);
		if ordering != Ordering::Equal {
			return ordering;
		}
	}
	Ordering::Equal
}

/// [`compare_key`] for a key held in a register: a number against a number
/// under a plain key is ordered as numbers, with nothing converted.
fn compare_scalar_key(a: &Scalar<'_>, b: &Value, key: KeyOrder) -> Ordering {
	if !key.collate
		&& !key.numeric
		&& let (Scalar::Number(a), Value::Number(b)) = (a, b)
	{
		let ordering = a.cmp(b);
		return if key.ascending {
			ordering
		} else {
			ordering.reverse()
		};
	}
	compare_key(&a.to_value(), b, key)
}

/// How `a` orders against `b` under one sort key.
fn compare_key(a: &Value, b: &Value, key: KeyOrder) -> Ordering {
	let ordering = compare_values(a, b, key.collate, key.numeric);
	if key.ascending {
		ordering
	} else {
		ordering.reverse()
	}
}

#[cfg(test)]
mod tests {
	use super::remove_path;
	use crate::exec::field_path::FieldPathPart;
	use crate::expr::Part;
	use crate::val::Value;
	use crate::val::convert_public::convert_public_value_to_internal;

	fn value(src: &str) -> Value {
		convert_public_value_to_internal(crate::syn::value(src).expect("a value literal"))
	}

	fn field(name: &str) -> FieldPathPart {
		FieldPathPart::Field(name.to_owned())
	}

	/// A path removes what `Value::cut` removes for the same parts, on
	/// objects, arrays and scalars alike.
	#[test]
	fn a_path_removes_what_value_cut_removes() {
		use FieldPathPart::{First, Index, Last};
		let values = [
			"{ a: { b: 1, c: 2 }, l: [{ x: 1, y: 2 }, { x: 3 }], '0': { b: 1 }, n: NULL }",
			"[{ a: { b: 1 } }, [1, 2], { a: 2 }, 4]",
			"[1, 2, 3]",
			"5",
		];
		let paths = [
			vec![field("a")],
			vec![field("a"), field("b")],
			vec![field("a"), field("b"), field("z")],
			vec![field("l"), field("x")],
			vec![field("l"), Index(1)],
			vec![field("l"), Index(0), field("x")],
			vec![field("l"), Index(9)],
			vec![Index(0)],
			vec![Index(0), field("b")],
			vec![Index(1), Index(0)],
			vec![First],
			vec![Last],
			vec![First, field("a")],
			vec![Last, field("a")],
			vec![field("l"), First],
			vec![field("l"), Last, field("x")],
			vec![field("n"), field("x")],
			vec![field("missing"), field("x")],
		];
		for src in values {
			for path in &paths {
				let parts: Vec<Part> = path
					.iter()
					.map(|part| match part {
						FieldPathPart::Field(name) => Part::Field(name.as_str().into()),
						Index(idx) => Part::index_int(*idx as i64),
						First => Part::First,
						Last => Part::Last,
						FieldPathPart::Lookup(_) => unreachable!("no lookup is listed"),
					})
					.collect();
				let mut removed = value(src);
				remove_path(&mut removed, path);
				let mut cut = value(src);
				cut.cut(&parts);
				assert_eq!(removed, cut, "{path:?} of {src}");
			}
		}
	}

	/// A lookup names no location in a value, so a path holding one removes
	/// nothing.
	#[test]
	fn a_path_through_a_lookup_removes_nothing() {
		let src = "{ a: { b: 1 }, l: [{ x: 1 }] }";
		for path in [
			vec![FieldPathPart::Lookup("->knows".to_owned())],
			vec![field("a"), FieldPathPart::Lookup("->knows".to_owned())],
			vec![field("l"), FieldPathPart::Lookup("->knows".to_owned()), field("x")],
		] {
			let mut removed = value(src);
			remove_path(&mut removed, &path);
			assert_eq!(removed, value(src), "{path:?}");
		}
	}
}
