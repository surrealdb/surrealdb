//! Pipelines and the parallel legality the logical plan states for them.
//!
//! A statement's operator tree is cut at its breakers into pipelines: a source,
//! a chain of streaming operators that handle one row at a time, and a sink that
//! collects the rows. For each pipeline the logical plan says what *may* be done
//! with it, from the operators' kinds, the effects of their expressions and
//! the footprints of their statements alone:
//!
//! - [`SourceSplit`]: whether the input can be cut into parts read independently;
//! - [`ChainLegality`]: whether every operator of the chain reads only its own row and what cannot
//!   change during the statement, and writes nothing;
//! - [`SinkCombine`]: how the partial results of the sink combine into the serial result, per
//!   aggregate function where the sink aggregates ([`AggregateCombine`]);
//! - [`Paging`]: how many `START` / `LIMIT` clauses the pipeline holds;
//! - [`ErrorOrder`]: which failure a split run raises;
//! - `may_raise`: whether evaluating the pipeline's expressions may fail for some row, which is
//!   whether a part may drop a row that a failure of an earlier row would have stopped the serial
//!   run before reaching.
//!
//! [`LegalityIndex`] answers these keyed by the operator that is each pipeline's source, and
//! records one [`Legality`] per breaker for the operators above it, up to the next breaker or the
//! edge of the statement (a segment). [`Legality::fuse`] joins the two when a physical rule makes
//! a breaker a chain operator of its pipeline or removes it. [`statement_legality`] states what
//! the statement as a whole refuses, from the effects of everything it evaluates.
//!
//! Nothing here reads a catalog, a statistic, an access path or bytecode. The
//! physical plan chooses whether and how to act on what is legal; it adds
//! refusals of its own and never legality.
//!
//! Beside the pipelines, two annotations say what a statement's siblings may do
//! at once: [`independent`] for sibling statements and [`foreach_legality`] for
//! the iterations and `IF` arms of a `FOR` body.

use super::effects::{SummaryOracle, permission_computed, plan_effects, scalar_effects};
use super::summaries::CatalogSummaries;
use crate::exec::function::FunctionRegistry;
use crate::expr::logical::{
	EffectSet, EvalFrame, LogicalAggregate, LogicalExpr, LogicalFetch, LogicalField, LogicalFields,
	LogicalOp, LogicalProject, LogicalSource, LogicalWriteSource, SourceRef,
};
use crate::expr::{BinaryOperator, Function, PrefixOperator};

/// Whether a pipeline's input can be cut into parts read independently.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SourceSplit {
	/// A key range the storage layer splits: a table scan, a record-id range, an index range
	/// whose access does not repeat a record.
	Range,
	/// A sequence the coordinator produces in a defined order and hands on in batches: a
	/// nearest-neighbour search, a full-text match, a bitmap, an index union, an index lookup,
	/// lightweight edges, an index range that may repeat a record, and a union of storage
	/// sources. A loop over a held value is the same shape: the elements of an array or a set, the
	/// integers of a range and the rows of a delivered subquery result are a sequence in a
	/// defined order, and the position of an element is its input position. The pipeline walker
	/// states a held value as `Whole`; the loop legality of the IR states it as `Ordinal`.
	Ordinal,
	/// Not partitionable: values, a statement in `FROM`, a sorted or grouped input, a split, a
	/// join, a count, a write's targets, a once-evaluated input.
	Whole,
}

/// Why a chain operator is not row-local.
///
/// Ordered as [`refusal_of`] names them: the first refusal a set of effects holds is the least.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ChainRefusal {
	/// An expression writes data or something outside the database, or the operator is a write.
	Writes,
	/// An expression advances state that outlives the statement, as a sequence does.
	AdvancesState,
	/// An expression draws entropy: its answers depend on how many were drawn before.
	Entropy,
	/// An expression blocks, as `SLEEP` does: never elided, never overlapped within a row.
	Blocks,
	/// An expression runs a body the plan cannot see: `eval`, text statements, stored endpoints.
	Opaque,
	/// An expression reads something outside the transaction's snapshot: a remote endpoint, a
	/// bucket, the API request being served. Its answers are not stable within a statement.
	External,
}

/// Every operator of the chain reads only its own row plus inputs that do not change during
/// the statement, and writes nothing.
///
/// A clock read is row-local, and so is a read of storage through the statement's transaction,
/// which is stable within a statement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ChainLegality {
	RowLocal,
	Refused(ChainRefusal),
}

impl ChainLegality {
	/// What a chain makes of the operators of two parts: row-local when both are, and otherwise
	/// the first refusal either names, in the order [`chain_legality`] documents.
	pub(crate) fn both(self, other: Self) -> Self {
		match (self, other) {
			(Self::Refused(a), Self::Refused(b)) => Self::Refused(a.min(b)),
			(Self::Refused(refusal), Self::RowLocal) | (Self::RowLocal, Self::Refused(refusal)) => {
				Self::Refused(refusal)
			}
			(Self::RowLocal, Self::RowLocal) => Self::RowLocal,
		}
	}
}

/// How the partial results of a pipeline's sink combine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SinkCombine {
	/// The statement's output: concatenation in input order.
	Concat,
	/// `ORDER BY`: a stable merge, ties broken by input position.
	StableMerge,
	/// `ORDER BY` under a `LIMIT`: a bounded heap per part and a shared threshold.
	TopK,
	/// One entry per aggregate function the sink folds.
	Aggregate(Vec<AggregateCombine>),
	/// The first occurrence in input order is kept.
	Distinct,
	/// A split, a join's probe or pairing, a write: the sink holds state a part cannot own.
	NotSplittable,
	/// A write loop's effects, applied in iteration order after the parts that produced them have
	/// run apart. Produced only for a loop whose apply is ordered and whose iterations are
	/// independent.
	#[allow(dead_code)]
	OrderedApply,
}

impl SinkCombine {
	/// Whether combining the parts as `self` is no stronger than combining them as `stated`.
	///
	/// The sinks that merge in an order weaken along `TopK`, `StableMerge`, `Concat` and
	/// `NotSplittable`, the serial run, which is the weakest of all. An aggregate, a distinct set
	/// and an ordered apply combine one way only, and weaken to `NotSplittable` alone.
	pub(crate) fn weaker_or_equal(&self, stated: &SinkCombine) -> bool {
		let order = |sink: &SinkCombine| match sink {
			SinkCombine::NotSplittable => Some(0),
			SinkCombine::Concat => Some(1),
			SinkCombine::StableMerge => Some(2),
			SinkCombine::TopK => Some(3),
			SinkCombine::Aggregate(_) | SinkCombine::Distinct | SinkCombine::OrderedApply => None,
		};
		match (order(self), order(stated)) {
			(Some(this), Some(stated)) => this <= stated,
			_ => {
				matches!(self, SinkCombine::NotSplittable)
					|| std::mem::discriminant(self) == std::mem::discriminant(stated)
			}
		}
	}
}

/// How an aggregate function's partial states combine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AggregateCombine {
	/// The partial states combine to exactly the serial fold.
	Exact,
	/// `array::group` and `array::collect`: exact when the partial states fold in input order.
	Appending,
	/// `math::sum`, `math::mean`, `math::stddev`, `math::variance` and `vector::sum`:
	/// decomposable, and a legal split. A float fold depends on how its terms are grouped, so the
	/// low bits of the answer may differ from the serial fold's.
	InexactFloat,
	/// The function has no combine over partial states.
	NotDecomposable,
}

/// The `START` / `LIMIT` clauses a pipeline holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Paging {
	None,
	One,
	Many,
}

/// Which failure a split run raises when several parts fail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ErrorOrder {
	/// The failure of the row earliest in the input wins, and it is the failure the serial run
	/// raises: every row before it succeeded, and no row after it is judged.
	FirstByInputPosition,
}

/// What the logical plan allows for one pipeline.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Legality {
	pub(crate) source: SourceSplit,
	pub(crate) chain: ChainLegality,
	pub(crate) sink: SinkCombine,
	pub(crate) paging: Paging,
	pub(crate) error_order: ErrorOrder,
	/// Whether evaluating the pipeline's expressions may raise an error for some row.
	///
	/// An allow-list: `false` only for a pipeline every expression of which is built from reads of
	/// stored fields, literals, `AND`, `OR`, `NOT` and the total comparisons
	/// (`=`, `==`, `!=`, `<`, `<=`, `>`, `>=`), in operators that are filters, plain projections,
	/// ordering keys or paging. Everything else may raise: every call, any other
	/// operator, a cast, an index, a record link followed, a `COMPUTED` field or other expression
	/// the catalog supplies, a layer and a fetch.
	pub(crate) may_raise: bool,
}

impl Legality {
	/// What the logical plan says about a physical pipeline it does not list: a source it
	/// cannot cut, so no split. A count strategy, a probe, a mock read as values, a `MATCH`
	/// stage and a breaker's output are such pipelines.
	pub(crate) const fn not_listed() -> Self {
		Self {
			source: SourceSplit::Whole,
			chain: ChainLegality::RowLocal,
			sink: SinkCombine::NotSplittable,
			paging: Paging::None,
			error_order: ErrorOrder::FirstByInputPosition,
			may_raise: true,
		}
	}

	/// Whether combining the parts as `picked` is no stronger than the sink this legality states.
	pub(crate) fn weaker_or_equal(&self, picked: &SinkCombine) -> bool {
		picked.weaker_or_equal(&self.sink)
	}

	/// Whether the pipeline's input may be cut into parts that run apart: the source is a range
	/// or an ordinal sequence, the chain is row-local, the sink is one whose partial results
	/// combine, and paging is at most one clause.
	///
	/// An aggregate sink combines unless one of its functions is
	/// [`AggregateCombine::NotDecomposable`], which is a refusal of the logical plan. An
	/// [`AggregateCombine::InexactFloat`] function is decomposable and does not refuse.
	pub(crate) fn may_split(&self) -> bool {
		matches!(self.source, SourceSplit::Range | SourceSplit::Ordinal)
			&& self.chain == ChainLegality::RowLocal
			&& match &self.sink {
				SinkCombine::NotSplittable => false,
				SinkCombine::Aggregate(combines) => {
					!combines.contains(&AggregateCombine::NotDecomposable)
				}
				SinkCombine::Concat
				| SinkCombine::StableMerge
				| SinkCombine::TopK
				| SinkCombine::Distinct
				| SinkCombine::OrderedApply => true,
			} && self.paging != Paging::Many
	}
}

/// How the partial states of the aggregate function `name` combine.
pub(crate) fn aggregate_combine(name: &str) -> AggregateCombine {
	match name {
		"count" | "math::min" | "math::max" | "math::median" | "time::min" | "time::max"
		| "array::join" | "array::distinct" | "array::min" | "array::max" => AggregateCombine::Exact,
		"array::group" | "array::collect" => AggregateCombine::Appending,
		"math::sum" | "math::mean" | "math::stddev" | "math::variance" | "vector::sum" => {
			AggregateCombine::InexactFloat
		}
		_ => AggregateCombine::NotDecomposable,
	}
}

/// Whether `chain` is row-local, from the effects `exprs` declare, in the order a refusal is
/// named: writes, state advances, entropy, blocking, an opaque body, a read outside the
/// transaction.
///
/// A stored function the chain calls is an unseen body, and no `COMPUTED` field is known
/// ([`CatalogSummaries::none`]); [`chain_legality_with`] folds in what a catalog states.
#[cfg_attr(
	not(test),
	allow(
		dead_code,
		reason = "the logical plan's own view, which tests compare the physical plan with"
	)
)]
pub(crate) fn chain_legality<'a>(
	exprs: impl IntoIterator<Item = &'a LogicalExpr>,
	registry: &FunctionRegistry,
) -> ChainLegality {
	chain_legality_with(exprs, registry, &CatalogSummaries::none())
}

/// [`chain_legality`] with the summaries of the stored functions and `COMPUTED` fields the
/// expressions reach.
#[cfg_attr(
	not(test),
	allow(
		dead_code,
		reason = "the logical plan's own view, which tests compare the physical plan with"
	)
)]
pub(crate) fn chain_legality_with<'a>(
	exprs: impl IntoIterator<Item = &'a LogicalExpr>,
	registry: &FunctionRegistry,
	summaries: &CatalogSummaries,
) -> ChainLegality {
	let mut fold = EffectsFold::default();
	for expr in exprs {
		fold.add_with(expr, registry, summaries);
	}
	fold.legality()
}

/// The effects of the expressions a chain evaluates, folded one expression at a time, for a
/// caller that cannot hand [`chain_legality`] an iterator of them.
#[derive(Clone, Copy, Debug)]
pub(crate) struct EffectsFold {
	set: EffectSet,
}

impl Default for EffectsFold {
	fn default() -> Self {
		Self {
			set: EffectSet::PURE,
		}
	}
}

impl EffectsFold {
	/// Fold in the effects of `expr`, and of every statement written inside it, as `summaries`
	/// state the definitions it reaches.
	///
	/// - A method call has the effects of every builtin it may resolve to
	///   ([`FunctionRegistry::method_effects`]).
	/// - A call of a stored function has the effects of its summary, and is an unseen body besides
	///   ([`SummaryOracle`]).
	/// - A read of a field some table computes has the effects of deriving it.
	pub(crate) fn add_with(
		&mut self,
		expr: &LogicalExpr,
		registry: &FunctionRegistry,
		summaries: &CatalogSummaries,
	) {
		let oracle = SummaryOracle::new(registry, summaries);
		self.set = self.set.union(scalar_effects(expr, &oracle));
	}

	/// Fold in `effects` that no expression of the chain declares: those of the `COMPUTED`
	/// fields a table permission derives for itself.
	fn add_effects(&mut self, effects: EffectSet) {
		self.set = self.set.union(effects);
	}

	/// What the effects folded so far make of the chain.
	pub(crate) fn legality(self) -> ChainLegality {
		refusal_of(self.set).map_or(ChainLegality::RowLocal, ChainLegality::Refused)
	}
}

/// The effects that refuse, each with the refusal it names, in the order [`chain_legality`]
/// documents. Together they are every effect that keeps a call from running for several rows at
/// once but the clock: a call that only reads the clock is row-local.
const REFUSALS: [(EffectSet, ChainRefusal); 6] = [
	(EffectSet::WRITES_DATA.union(EffectSet::WRITES_EXTERNAL), ChainRefusal::Writes),
	(EffectSet::ADVANCES_STATE, ChainRefusal::AdvancesState),
	(EffectSet::ENTROPY, ChainRefusal::Entropy),
	(EffectSet::BLOCKING, ChainRefusal::Blocks),
	(EffectSet::OPAQUE_BODY, ChainRefusal::Opaque),
	(EffectSet::READS_EXTERNAL, ChainRefusal::External),
];

/// The first refusal `set` names, in the order [`chain_legality`] documents.
fn refusal_of(set: EffectSet) -> Option<ChainRefusal> {
	REFUSALS.iter().find(|(effects, _)| set.intersects(*effects)).map(|(_, refusal)| *refusal)
}

/// One pipeline of a statement's logical plan: its id (its position in the list
/// [`pipelines_of`] answers), what the logical plan allows for it, and the pipeline that
/// consumes its result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LogicalPipeline {
	pub(crate) id: u32,
	pub(crate) legality: Legality,
	/// For a statement nested in an expression, the pipeline whose chain evaluates it.
	pub(crate) feeds: Option<u32>,
}

/// What a statement as a whole refuses, whatever pipeline it is in.
///
/// A split run evaluates a statement's expressions for several rows at once, and in a different
/// order from the serial run. An effect anywhere in the statement that depends on the order or
/// the number of its evaluations refuses every pipeline of the statement, since the pipelines
/// of a statement are run by one task and share its state. The effects are those
/// [`chain_legality`] refuses, and the clock is not one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StatementLegality {
	/// The first refusal the statement's effects name, in the order [`chain_legality`]
	/// documents.
	pub(crate) refusal: Option<ChainRefusal>,
}

impl StatementLegality {}

/// [`statement_legality`] as `summaries` state the definitions the statement reaches.
///
/// It folds every expression as [`EffectsFold::add_with`] does and adds the `COMPUTED` fields the
/// table permissions of the statement derive for themselves ([`plan_effects`]), so that a refusal
/// of any chain of the statement is a refusal of the statement.
pub(crate) fn statement_legality_with(
	op: &LogicalOp,
	registry: &FunctionRegistry,
	summaries: &CatalogSummaries,
) -> StatementLegality {
	let oracle = SummaryOracle::new(registry, summaries);
	StatementLegality {
		refusal: refusal_of(plan_effects(op, &oracle)),
	}
}

/// The logical pipelines of one statement, keyed by the operator that is each pipeline's source,
/// and the segments above its breakers, keyed by the breaker.
///
/// Both are keyed by the identity of an operator of the tree [`Self::of`] was built on: the
/// answer for a clone of an operator, or for an operator of another tree, is `None`.
pub(crate) struct LegalityIndex<'a> {
	entries: Vec<(&'a LogicalOp, LogicalPipeline)>,
	segments: Vec<(&'a LogicalOp, Legality)>,
}

impl<'a> LegalityIndex<'a> {
	/// [`Self::of`], with the summaries of the stored functions and `COMPUTED` fields the
	/// statement reaches.
	pub(crate) fn with_summaries(
		op: &'a LogicalOp,
		registry: &FunctionRegistry,
		summaries: &CatalogSummaries,
	) -> Self {
		let walked = walk(op, registry, summaries);
		Self {
			entries: walked.sources.into_iter().zip(walked.pipelines).collect(),
			segments: walked.segments,
		}
	}

	/// The legality of the pipeline whose source is `source`, by identity.
	pub(crate) fn of_source(&self, source: &LogicalOp) -> Option<&Legality> {
		self.entries
			.iter()
			.find(|(op, _)| std::ptr::eq(*op, source))
			.map(|(_, pipeline)| &pipeline.legality)
	}

	/// The legality of the segment above the breaker `breaker`, by identity.
	pub(crate) fn of_segment(&self, breaker: &LogicalOp) -> Option<&Legality> {
		self.segments
			.iter()
			.find(|(op, _)| std::ptr::eq(*op, breaker))
			.map(|(_, legality)| legality)
	}
}

/// [`pipelines_of`], with the summaries of the stored functions and `COMPUTED` fields the
/// statement reaches.
#[cfg_attr(
	not(test),
	allow(
		dead_code,
		reason = "the logical plan's own view, which tests compare the physical plan with"
	)
)]
pub(crate) fn pipelines_of_with(
	op: &LogicalOp,
	registry: &FunctionRegistry,
	summaries: &CatalogSummaries,
) -> Vec<LogicalPipeline> {
	walk(op, registry, summaries).pipelines
}

/// What [`walk`] found: the pipelines, the operator that is the source of each (in the same
/// order), and the segments above the breakers.
struct Walked<'a> {
	pipelines: Vec<LogicalPipeline>,
	sources: Vec<&'a LogicalOp>,
	segments: Vec<(&'a LogicalOp, Legality)>,
}

fn walk<'a>(
	op: &'a LogicalOp,
	registry: &FunctionRegistry,
	summaries: &CatalogSummaries,
) -> Walked<'a> {
	let mut walker = Walker {
		registry,
		summaries,
		out: Vec::new(),
		sources: Vec::new(),
		segments: Vec::new(),
	};
	walker.statement(op);
	Walked {
		pipelines: walker.out,
		sources: walker.sources,
		segments: walker.segments,
	}
}

/// A pipeline whose source has been met and whose breaker has not.
struct Live<'a> {
	source: SourceSplit,
	/// The operator the rows are read from.
	source_op: &'a LogicalOp,
	/// The expressions the chain and the breaker evaluate for each row.
	exprs: Vec<&'a LogicalExpr>,
	limits: usize,
	/// The effects of the `COMPUTED` fields the chain's table permissions derive for themselves,
	/// which no expression of the chain declares.
	derived: EffectSet,
	/// Whether an operator or an expression of the pipeline so far may raise.
	may_raise: bool,
}

/// The operators above a breaker, whose next breaker has not been met.
struct Above<'a> {
	breaker: &'a LogicalOp,
	exprs: Vec<&'a LogicalExpr>,
	limits: usize,
	derived: EffectSet,
	may_raise: bool,
}

impl<'a> Above<'a> {
	fn of(breaker: &'a LogicalOp) -> Self {
		Self {
			breaker,
			exprs: Vec::new(),
			limits: 0,
			derived: EffectSet::PURE,
			may_raise: false,
		}
	}
}

/// What an operator of the relational pipeline leaves open.
enum Open<'a> {
	Live(Live<'a>),
	/// The rows are a breaker's output: nothing the logical plan lists as a pipeline. The
	/// operators above the breaker are gathered when it is one the logical plan keys segments
	/// by.
	Spent(Option<Above<'a>>),
}

struct Walker<'r, 'a> {
	registry: &'r FunctionRegistry,
	summaries: &'r CatalogSummaries,
	out: Vec<LogicalPipeline>,
	/// The source operator of each pipeline of `out`.
	sources: Vec<&'a LogicalOp>,
	segments: Vec<(&'a LogicalOp, Legality)>,
}

impl<'a> Walker<'_, 'a> {
	/// Walk `op`, a statement or the root of a relational pipeline.
	fn statement(&mut self, op: &'a LogicalOp) {
		match op {
			LogicalOp::Source(_)
			| LogicalOp::Values(_)
			| LogicalOp::Match(_)
			| LogicalOp::Subquery(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Join(_)
			| LogicalOp::Filter(_)
			| LogicalOp::PermissionFilter(_)
			| LogicalOp::Split(_)
			| LogicalOp::Aggregate(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Sort(_)
			| LogicalOp::Limit(_)
			| LogicalOp::Project(_)
			| LogicalOp::Fetch(_)
			| LogicalOp::Timeout(_)
			| LogicalOp::VersionScope(_)
			| LogicalOp::Only(_)
			| LogicalOp::Explain(_) => {
				let open = self.stream(op);
				self.finish(open, SinkCombine::Concat, Vec::new(), false);
			}
			LogicalOp::Write(write) => {
				let own = op.scalars();
				match &write.source {
					LogicalWriteSource::Rows(input) => {
						let open = self.stream(input);
						self.finish(open, SinkCombine::NotSplittable, own, true);
					}
					LogicalWriteSource::Targets(_) | LogicalWriteSource::Inserts(_) => {
						self.nested(&own, None);
					}
				}
			}
			LogicalOp::Sequence(_)
			| LogicalOp::If(_)
			| LogicalOp::Let(_)
			| LogicalOp::Return(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::Eval(_)
			| LogicalOp::Throw(_)
			| LogicalOp::Sleep(_)
			| LogicalOp::Info(_)
			| LogicalOp::Ddl(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_) => {
				self.nested(&op.scalars(), None);
				for child in op.children() {
					self.statement(child);
				}
			}
		}
	}

	/// Walk the relational operator `op` and what it reads, answering what is left open.
	fn stream(&mut self, op: &'a LogicalOp) -> Open<'a> {
		match op {
			LogicalOp::Source(LogicalSource {
				source,
				..
			}) => {
				let exprs = op.scalars();
				let may_raise = self.may_raise_any(&exprs);
				Open::Live(Live {
					source: match source {
						SourceRef::Table(_) | SourceRef::RecordId(_) => SourceSplit::Range,
						SourceRef::Mock(_) | SourceRef::Param(_) | SourceRef::Dynamic(_) => {
							SourceSplit::Whole
						}
					},
					source_op: op,
					exprs,
					limits: 0,
					derived: EffectSet::PURE,
					may_raise,
				})
			}
			LogicalOp::Values(_) | LogicalOp::Match(_) => Open::Live(Live {
				source: SourceSplit::Whole,
				source_op: op,
				exprs: op.scalars(),
				limits: 0,
				derived: EffectSet::PURE,
				may_raise: true,
			}),
			LogicalOp::Subquery(subquery) => {
				self.statement(&subquery.input);
				Open::Live(Live {
					source: SourceSplit::Whole,
					source_op: op,
					exprs: Vec::new(),
					limits: 0,
					derived: EffectSet::PURE,
					may_raise: true,
				})
			}
			LogicalOp::UnionAll(union) => {
				let mut storage = true;
				for input in &union.inputs {
					let bare = matches!(
						input,
						LogicalOp::Source(LogicalSource {
							source: SourceRef::Table(_) | SourceRef::RecordId(_),
							..
						})
					);
					if !bare {
						storage = false;
						self.statement(input);
					}
				}
				Open::Live(Live {
					source: if storage {
						SourceSplit::Ordinal
					} else {
						SourceSplit::Whole
					},
					source_op: op,
					exprs: Vec::new(),
					limits: 0,
					derived: EffectSet::PURE,
					may_raise: !storage,
				})
			}
			LogicalOp::Filter(filter) => {
				let open = self.chained(&filter.input, op);
				// The search for the nearest neighbours of a vector keeps the `k` closest rows of
				// everything its input yields: a top-k sink, whatever the access path.
				if crate::exec::planner::util::has_knn_operator(&filter.predicate) {
					self.finish(open, SinkCombine::TopK, Vec::new(), false);
					return Open::Spent(Some(Above::of(op)));
				}
				open
			}
			LogicalOp::PermissionFilter(filter) => self.chained(&filter.input, op),
			LogicalOp::Project(project) => self.chained(&project.input, op),
			LogicalOp::Fetch(fetch) => self.chained(&fetch.input, op),
			LogicalOp::Limit(limit) => {
				// The rows the limit pages are the sort's, when only operators that keep every
				// row and its place stand between them.
				let mut keepers = Vec::new();
				let mut below = &*limit.input;
				while let LogicalOp::Project(LogicalProject {
					input,
					..
				})
				| LogicalOp::Fetch(LogicalFetch {
					input,
					..
				}) = below
				{
					keepers.push(below);
					below = input;
				}
				if let LogicalOp::Sort(sort) = below {
					let open = self.stream(&sort.input);
					let mut extra = sort_exprs(below);
					let raises = self.may_raise_any(&extra);
					extra.extend(op.scalars());
					self.finish(open, SinkCombine::TopK, extra, raises);
					let mut above = Above::of(below);
					for keeper in keepers.into_iter().rev() {
						let scalars = keeper.scalars();
						self.nested(&scalars, None);
						above.exprs.extend(scalars);
						let (raises, derived) = self.chain_op(keeper);
						above.may_raise |= raises;
						above.derived = above.derived.union(derived);
					}
					above.exprs.extend(op.scalars());
					above.limits = 1;
					return Open::Spent(Some(above));
				}
				let mut open = self.chained(&limit.input, op);
				match &mut open {
					Open::Live(live) => live.limits += 1,
					Open::Spent(Some(above)) => above.limits += 1,
					Open::Spent(None) => {}
				}
				open
			}
			LogicalOp::Timeout(timeout) => self.passed(&timeout.input, op),
			LogicalOp::VersionScope(scope) => self.passed(&scope.input, op),
			LogicalOp::Only(only) => self.passed(&only.input, op),
			LogicalOp::Explain(explain) => self.passed(&explain.input, op),
			LogicalOp::Sort(sort) => {
				let open = self.stream(&sort.input);
				let keys = op.scalars();
				let raises = self.may_raise_any(&keys);
				self.finish(open, SinkCombine::StableMerge, keys, raises);
				Open::Spent(Some(Above::of(op)))
			}
			LogicalOp::Distinct(distinct) => {
				let open = self.stream(&distinct.input);
				self.finish(open, SinkCombine::Distinct, Vec::new(), false);
				Open::Spent(Some(Above::of(op)))
			}
			LogicalOp::Aggregate(aggregate) => {
				let open = self.stream(&aggregate.input);
				let sink = self.aggregate_sink(aggregate);
				let exprs = op.scalars();
				let raises = self.may_raise_any(&exprs);
				self.finish(open, sink, exprs, raises);
				Open::Spent(Some(Above::of(op)))
			}
			LogicalOp::Split(split) => {
				let open = self.stream(&split.input);
				let exprs = op.scalars();
				let raises = self.may_raise_any(&exprs);
				self.finish(open, SinkCombine::NotSplittable, exprs, raises);
				Open::Spent(Some(Above::of(op)))
			}
			LogicalOp::Join(join) => {
				for input in join.inputs.iter() {
					let open = self.stream(input);
					self.finish(open, SinkCombine::NotSplittable, Vec::new(), true);
				}
				self.nested(&op.scalars(), None);
				Open::Spent(None)
			}
			LogicalOp::Write(_)
			| LogicalOp::Ddl(_)
			| LogicalOp::Eval(_)
			| LogicalOp::Sequence(_)
			| LogicalOp::Let(_)
			| LogicalOp::Return(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::If(_)
			| LogicalOp::Throw(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_)
			| LogicalOp::Info(_)
			| LogicalOp::Sleep(_) => {
				self.statement(op);
				Open::Spent(None)
			}
		}
	}

	/// `node`, a streaming operator over `input`: the pipeline of `input` with `node`'s
	/// expressions in its chain.
	fn chained(&mut self, input: &'a LogicalOp, node: &'a LogicalOp) -> Open<'a> {
		let mut open = self.stream(input);
		let scalars = node.scalars();
		let (raises, derived) = self.chain_op(node);
		match &mut open {
			Open::Live(live) => {
				live.exprs.extend(scalars);
				live.may_raise |= raises;
				live.derived = live.derived.union(derived);
			}
			Open::Spent(above) => {
				self.nested(&scalars, None);
				if let Some(above) = above {
					above.exprs.extend(scalars);
					above.may_raise |= raises;
					above.derived = above.derived.union(derived);
				}
			}
		}
		open
	}

	/// Whether the chain operator `node` may raise by what it is and what it evaluates, and the
	/// effects of the `COMPUTED` fields it derives for itself.
	///
	/// A filter, a plain projection and a paging clause may not, where their expressions are built
	/// of what [`answers_every_row`] admits; a table permission may not either, unless it derives
	/// a computed field, which is a body the catalog supplies. Every other operator may.
	fn chain_op(&self, node: &LogicalOp) -> (bool, EffectSet) {
		if let LogicalOp::Filter(filter) = node {
			(!answers_every_row(&filter.predicate, self.summaries), EffectSet::PURE)
		} else if let LogicalOp::Project(project) = node {
			(project_may_raise(project, self.summaries), EffectSet::PURE)
		} else if let LogicalOp::PermissionFilter(filter) = node {
			let derived = permission_computed(node, self.registry, self.summaries);
			(
				derived.is_some() || !answers_every_row(&filter.predicate, self.summaries),
				derived.unwrap_or(EffectSet::PURE),
			)
		} else if let LogicalOp::Limit(_) = node {
			// The paging clauses are evaluated once, by the host, and not for each row.
			(false, EffectSet::PURE)
		} else {
			(true, EffectSet::PURE)
		}
	}

	/// Whether some expression of `exprs` is not one [`answers_every_row`] admits.
	fn may_raise_any(&self, exprs: &[&LogicalExpr]) -> bool {
		exprs.iter().any(|expr| !answers_every_row(expr, self.summaries))
	}

	/// What the chain of the expressions `exprs` and the derived effects `derived` refuses.
	fn chain(&self, exprs: &[&LogicalExpr], derived: EffectSet) -> ChainLegality {
		let mut fold = EffectsFold::default();
		for expr in exprs {
			fold.add_with(expr, self.registry, self.summaries);
		}
		fold.add_effects(derived);
		fold.legality()
	}

	/// `node`, an operator over `input` that holds statement-level state and adds nothing to
	/// the chain.
	fn passed(&mut self, input: &'a LogicalOp, node: &'a LogicalOp) -> Open<'a> {
		let open = self.stream(input);
		self.nested(&node.scalars(), None);
		open
	}

	/// Close `open` with `sink`, `extra` being the expressions the breaker evaluates for each
	/// row and `raises` whether the breaker's evaluation may raise, and walk the statements nested
	/// in the pipeline's expressions.
	fn finish(
		&mut self,
		open: Open<'a>,
		sink: SinkCombine,
		extra: Vec<&'a LogicalExpr>,
		raises: bool,
	) {
		let mut live = match open {
			Open::Live(live) => live,
			Open::Spent(above) => {
				self.nested(&extra, None);
				if let Some(mut above) = above {
					above.exprs.extend(extra);
					let legality = Legality {
						source: SourceSplit::Whole,
						chain: self.chain(&above.exprs, above.derived),
						sink,
						paging: paging_of(above.limits),
						error_order: ErrorOrder::FirstByInputPosition,
						may_raise: above.may_raise || raises,
					};
					self.segments.push((above.breaker, legality));
				}
				return;
			}
		};
		live.exprs.extend(extra);
		let id = u32::try_from(self.out.len()).unwrap_or(u32::MAX);
		self.out.push(LogicalPipeline {
			id,
			legality: Legality {
				source: live.source,
				chain: self.chain(&live.exprs, live.derived),
				sink,
				paging: paging_of(live.limits),
				error_order: ErrorOrder::FirstByInputPosition,
				may_raise: live.may_raise || raises,
			},
			feeds: None,
		});
		self.sources.push(live.source_op);
		self.nested(&live.exprs, Some(id));
	}

	/// Walk the statements written inside `exprs`, which `host` evaluates.
	fn nested(&mut self, exprs: &[&'a LogicalExpr], host: Option<u32>) {
		let mut plans = Vec::new();
		for expr in exprs {
			subqueries(expr, &mut plans);
		}
		for plan in plans {
			let first = self.out.len();
			self.statement(plan);
			if let Some(host) = host {
				for pipeline in &mut self.out[first..] {
					pipeline.feeds.get_or_insert(host);
				}
			}
		}
	}

	/// How each aggregate function of `aggregate` combines.
	fn aggregate_sink(&self, aggregate: &LogicalAggregate) -> SinkCombine {
		let analysis = crate::exec::planner::analyse_aggregation(
			self.registry,
			&aggregate.fields,
			&aggregate.group,
		);
		SinkCombine::Aggregate(match analysis {
			Ok(analysis) => analysis
				.fields
				.iter()
				.filter_map(|field| field.info.as_ref())
				.flat_map(|info| info.calls.iter())
				.map(|call| aggregate_combine(call.id.name()))
				.collect(),
			Err(_) => vec![AggregateCombine::NotDecomposable],
		})
	}
}

/// Whether `expr` is built only of what answers for every row without raising: a read of a field
/// the catalog does not compute, a literal, `AND`, `OR`, `NOT` and the total comparisons `=`,
/// `==`, `!=`, `<`, `<=`, `>`, `>=`.
///
/// This is the allow-list of [`Legality::may_raise`]. A call, any other operator, a cast, an
/// index, a path that follows a record link and a nested statement are not in it, and neither is
/// a field some table computes. A parameter is not: a statement's own parameters are literals by
/// the time the plan is bound, and one that remains is read from a variable of the program or
/// from the row, which a body may fail to find.
pub(crate) fn answers_every_row(expr: &LogicalExpr, summaries: &CatalogSummaries) -> bool {
	if let LogicalExpr::Literal(_) = expr {
		return true;
	}
	if let LogicalExpr::Field(name) = expr {
		return !summaries.reads_computed(name.as_str());
	}
	if let LogicalExpr::Prefix {
		op: PrefixOperator::Not,
		expr,
	} = expr
	{
		return answers_every_row(expr, summaries);
	}
	// `NOT e` is also written as a call of the builtin of that name.
	if let LogicalExpr::Call(call) = expr
		&& matches!(&call.receiver, Function::Normal(name) if name == "not")
		&& let [operand] = call.args.as_slice()
	{
		return answers_every_row(operand, summaries);
	}
	if let LogicalExpr::Binary {
		left,
		op,
		right,
	} = expr
	{
		return matches!(
			op,
			BinaryOperator::And
				| BinaryOperator::Or
				| BinaryOperator::Equal
				| BinaryOperator::ExactEqual
				| BinaryOperator::NotEqual
				| BinaryOperator::LessThan
				| BinaryOperator::LessThanEqual
				| BinaryOperator::MoreThan
				| BinaryOperator::MoreThanEqual
		) && answers_every_row(left, summaries)
			&& answers_every_row(right, summaries);
	}
	false
}

/// Whether the projection `project` may raise: unless it is a plain projection of fields and `*`,
/// every column of which is a user's own expression that [`answers_every_row`] admits.
fn project_may_raise(project: &LogicalProject, summaries: &CatalogSummaries) -> bool {
	if project.is_layer() || !project.omit.is_empty() {
		return true;
	}
	match &project.fields {
		LogicalFields::Value {
			expr,
			..
		} => !answers_every_row(expr, summaries),
		LogicalFields::Select(fields) => fields.iter().any(|field| match field {
			LogicalField::All => false,
			LogicalField::Single {
				expr,
				kind,
				frame,
				..
			} => {
				kind.is_some()
					|| *frame != EvalFrame::caller()
					|| !answers_every_row(expr, summaries)
			}
		}),
	}
}

/// The paging of `limits` `START` / `LIMIT` clauses.
fn paging_of(limits: usize) -> Paging {
	match limits {
		0 => Paging::None,
		1 => Paging::One,
		_ => Paging::Many,
	}
}

/// The expressions of the sort `op`.
fn sort_exprs(op: &LogicalOp) -> Vec<&LogicalExpr> {
	op.scalars()
}

/// The statements written directly in `expr`: those not inside another statement.
fn subqueries<'a>(expr: &'a LogicalExpr, out: &mut Vec<&'a LogicalOp>) {
	if let LogicalExpr::Subquery(plan) = expr {
		out.push(plan);
		return;
	}
	expr.for_each_child(&mut |child| subqueries(child, out));
}

/// What a `FOR` body allows: whether its iterations are independent, and the legality of each
/// arm of the `IF` statements that are statements of the body.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code, reason = "read by the physical plan's placement of loops"))]
pub(crate) struct ForeachLegality {
	/// Every iteration reads only what does not change during the loop, writes nothing, and
	/// cannot end the loop (no `BREAK`, and no `RETURN`, in the body or its nested blocks, `IF`
	/// arms and loops), so the iterations may run at the same time or in any order.
	pub(crate) iterations_independent: bool,
	/// The legality of each arm of each `IF` among the body's statements, in order, with the
	/// conditions evaluated to reach it.
	pub(crate) arms: Vec<ChainLegality>,
}
