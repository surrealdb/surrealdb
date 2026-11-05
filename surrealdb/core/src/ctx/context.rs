use std::collections::HashMap;
use std::fmt::{self, Debug};
#[cfg(storage)]
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Result, bail};
use surrealdb_cnf::PROTECTED_PARAM_NAMES;
use surrealdb_cnf::dynamic::DynamicConfiguration;
use surrealdb_strand::Strand;
#[cfg(feature = "http")]
use url::Url;
use uuid::Uuid;
use web_time::Instant;

use crate::buc::Error as BucError;
use crate::buc::manager::BucketsManager;
use crate::buc::store::ObjectStore;
use crate::catalog::providers::{CatalogProvider, DatabaseProvider, NamespaceProvider};
use crate::catalog::{DatabaseId, Error as CatalogError, NamespaceId};
use crate::config::RuntimeConfig;
use crate::ctx::cancel::CancelHandle;
#[cfg(test)]
use crate::ctx::canceller::Canceller;
use crate::ctx::reason::Reason;
use crate::dbs::capabilities::Error as CapabilitiesError;
#[cfg(feature = "surrealism")]
use crate::dbs::capabilities::ExperimentalTarget;
#[cfg(feature = "http")]
use crate::dbs::capabilities::NetTarget;
use crate::dbs::{Capabilities, MessageBroker, Options, Session, StatementCounters, Variables};
use crate::err::{EngineError, Error};
use crate::exec::Error as ExecError;
use crate::exec::context::ForbiddenRow;
use crate::exec::function::FunctionRegistry;
use crate::exec::planner::placement::Policy;
use crate::exec::surface::Surface;
use crate::exec::vm::morsel::places;
use crate::expr::Base;
#[cfg(feature = "surrealism")]
use crate::extension::archive::{ArchiveSource, fetch_archive};
#[cfg(feature = "http")]
use crate::http::HttpClient;
use crate::iam::{Action, ResourceKind};
use crate::idx::trees::store::IndexStores;
use crate::kvs::cache::ds::DatastoreCache;
use crate::kvs::index::IndexBuilder;
use crate::kvs::sequences::Sequences;
use crate::kvs::slowlog::SlowLog;
use crate::kvs::{DatastoreError, Transaction};
use crate::mem::ALLOC;
#[cfg(feature = "surrealism")]
use crate::surrealism::cache::{SurrealismCache, SurrealismCacheLookup, SurrealismCachedModule};
use crate::types::PublicVariables;
use crate::val::convert_public::convert_public_value_to_internal;
use crate::val::{Datetime, Value};

pub type FrozenContext = Arc<Context>;

/// The names of the values a frame binds around the code it runs rather than
/// the query binding them: the row a statement reads (`$this`, `$self`,
/// `$parent`) and the names an event, a live notification or a field clause
/// binds. A function body called from such a frame reads them through the
/// whole context chain, as it reads the protected names; they are not a
/// caller's `LET` or `FOR` bindings.
const ROW_PARAM_NAMES: &[&str] =
	&["this", "self", "parent", "before", "after", "event", "value", "input"];

/// What a parameter name is bound to in a context's chain
/// ([`Context::binding`]).
#[derive(Debug, Clone, Copy)]
pub(crate) enum Binding<'a> {
	/// A value.
	Value(&'a Arc<Value>),
	/// A KNN query vector's row being scanned, which a statement or a body
	/// entered from the vector's evaluation binds as its `$parent` and
	/// `$this`: the parameter has no value, and a read of it fails with the
	/// row's refusal ([`ForbiddenRow::error`]).
	Forbidden(&'a Arc<ForbiddenRow>),
}

/// Ambient state for one query batch: datastore handles, cancellation, capabilities,
/// and **request-wide** configuration (node id, auth enabled, dynamic config, live/broker).
///
/// Stored behind [`FrozenContext`] (`Arc<Context>`) with optional parent links for scoped
/// deadlines and parameters. Per-statement toggles live on [`Options`], not here.
pub struct Context {
	// An optional parent context.
	parent: Option<FrozenContext>,
	// An optional deadline.
	deadline: Option<(Instant, Duration)>,
	// An optional slow log configuration used by the executor to log statements
	// that exceed a given duration threshold. This configuration is propagated
	// from the datastore into the context for the lifetime of a request.
	slow_log: Option<SlowLog>,
	// Whether or not this context is cancelled. Fast hot-path view checked
	// by [`Self::done`] at every executor yield.
	cancelled: Arc<AtomicBool>,
	// Awaitable view of the external cancellation signal, when one was
	// installed via [`Self::set_cancellation`]. Used by sites that bare-await
	// an external timer (e.g. `SLEEP`) so they can `select!` against the
	// cancel instead of running to completion. `None` for contexts without
	// an external cancel source (embedded callers, internal use).
	cancel_token: Option<tokio_util::sync::CancellationToken>,
	// A collection of read only values stored in this context. Shared with the
	// snapshots taken of it, and copied on the first write made while one is held.
	values: Arc<HashMap<Strand, Arc<Value>>>,
	// The parameter names this context binds to a KNN query vector's row being
	// scanned rather than to a value ([`Binding::Forbidden`]). A name is in
	// at most one of `values` and `forbidden` at one level.
	forbidden: Vec<(Strand, Arc<ForbiddenRow>)>,
	// Capabilities
	capabilities: Arc<Capabilities>,
	// An optional transaction
	transaction: Option<Arc<Transaction>>,
	// Does not read from parent `values`.
	isolated: bool,
	/// How many extension invocations enclose this context: zero for a query,
	/// one inside a host call an extension made, and so on. A child context
	/// carries its parent's.
	#[cfg(feature = "surrealism")]
	extension_depth: u8,
	// When true, EXPLAIN ANALYZE omits elapsed durations for deterministic test output
	redact_volatile_explain_attrs: bool,
	// Per-statement counters, shared with the executor so it can read the
	// number of rows affected by the running DML statement when emitting
	// the corresponding `StatementEvent`. Replaced by the executor before
	// each top-level statement; `None` outside an active statement.
	statement_counters: Option<Arc<StatementCounters>>,
	// The instant every CLOCK read of the running top-level statement answers.
	// Installed by the executor before each top-level statement and copied by
	// every context derived from this one, so a subquery, a function or closure
	// body, triggered code and a nested query (`eval::*`, a script's
	// `surrealdb.query()`) read their statement's instant. `None` where no
	// statement installed one: a read then takes the wall clock at the call.
	statement_clock: Option<Datetime>,
	// The parameters the request was made with: the client's bind parameters
	// and the session's variables, as attached by [`Self::attach_session`],
	// [`Self::attach_variables`] and [`Self::attach_public_variables`], which
	// a function body reads past its call boundary ([`Self::lexical_value`]).
	// A `LET` or `FOR` binding is never among them. A child context carries
	// its parent's; an isolated context holds none of its parent's, as it
	// holds none of its other values.
	request_params: Option<Arc<HashMap<Strand, Arc<Value>>>>,
	// Whether this node is the scope of a function body entered by a call
	// ([`Self::mark_call_boundary`]): the node [`Self::lexical_value`] stops
	// at before it answers from the request parameters.
	call_boundary: bool,
	// Pre-resolved tenant identity (namespace, database, user, session id,
	// client ip) derived from the active session at `attach_session` time.
	// Read by the executor and the transaction layer to populate the
	// `*Ctx` half of every emitted [`crate::observe`] event without
	// re-walking the session value tree on every emit.
	tenant_identity: Option<Arc<crate::observe::TenantIdentity>>,
	/// The statement families the entry that started the query serves. Set on
	/// the root context and copied by every derivation, so stored code run
	/// under a descendant is lowered and run under the same surface.
	surface: &'static Surface,
	/// The unit table of the function bundle whose body is executing around
	/// this context: the arena a nested program with none of its own finds the
	/// units its ids name in ([`crate::exec::vm::UnitTable`]). A child context
	/// carries its parent's.
	vm_units: Option<crate::exec::vm::UnitTable>,
	/// The per-table units the program executing around this context reads
	/// records by id through ([`crate::exec::vm::Deref`]). A child context
	/// carries its parent's.
	vm_deref: Option<Arc<crate::exec::vm::Deref>>,
	/// The late-bound statements the program executing around this context
	/// serves when a block reaches them, if any.
	vm_served: Option<crate::exec::vm::ServedStatements>,
	/// The metrics an `EXPLAIN ANALYZE` plan reports for the program running
	/// around this context, which the program's host steps record their
	/// counters into. A child context carries its parent's.
	vm_metrics: Option<Arc<crate::exec::metrics::OperatorMetrics>>,
	/// What the statement running around this context reports to a query
	/// trace (`crate::exec::trace`), when its request asked for one. A child
	/// context carries its parent's.
	query_trace: Option<Arc<crate::exec::trace::StatementTrace>>,
	/// The edge cascade a delete is running the stages of a record of an
	/// edge beneath: the bundle an edge cascade of those stages runs again
	/// (`crate::exec::vm::write_cascade`). A child context carries its
	/// parent's.
	vm_edge_cascade: Option<Arc<crate::exec::vm::program::Program>>,
	/// The closure-body units the programs executing around this context
	/// lowered ([`crate::exec::vm::closure::ClosureUnits`]), which an
	/// invocation of a closure value looks its unit up in. A child context
	/// carries its parent's.
	vm_closures: Option<Arc<crate::exec::vm::closure::ClosureUnits>>,
	/// Whether authentication is required for non-anonymous access (datastore boot flag).
	pub(crate) auth_enabled: bool,
	/// Whether this session may use realtime (`LIVE` / `KILL LIVE`).
	live: bool,
	/// Optional broker for cross-node live query notifications.
	broker: Option<Arc<dyn MessageBroker>>,
	/// Where the writes of the transaction the broker buffers for are noted,
	/// installed and cleared with it ([`crate::dbs::live_recheck`]).
	live_recheck: Option<Arc<crate::dbs::live_recheck::LiveRecheck>>,
	/// The host-provided `SHOW CHANGES` source, when the datastore has one.
	changefeed_source: Option<Arc<dyn crate::lq::external::ChangeFeedSource>>,
	/// How a running program gives up its thread once it has spent its
	/// quantum; the default scheduler when `None`. A child context carries
	/// its parent's.
	scheduler: Option<Arc<dyn crate::exec::scheduler::Scheduler>>,
	/// The host's places a morsel pipeline's morsels may run at besides the node's own; none
	/// when `None`. A child context carries its parent's.
	morsel_places: Option<Arc<dyn crate::exec::places::MorselPlaces>>,
	/// The node whose async event queue an event queued by a write in this context joins; `None`
	/// for this context's own node ([`Self::node_id`]).
	event_queue_node: Option<Uuid>,
	/// The query's memory: its lease from the node's governor and the store
	/// its breakers spill to. `None` outside a datastore query, where nothing
	/// is bounded. A child context carries its parent's.
	memory: Option<Arc<crate::exec::memory::QueryMemory>>,
	/// The ticket the index engines' parallel work takes its places through:
	/// a blocking statement's own registration, the background compactor's,
	/// or a build's. A child, isolated or snapshot context shares its parent's
	/// cell, since its work belongs to the same statement, so whichever of
	/// them asks first registers for all; a background or concurrent context
	/// starts with a cell of its own. With none set, the first request for the
	/// ticket registers with the node's pool, and every later one hands out
	/// that registration ([`crate::idx::env::IndexEnv::places`]). Setting a
	/// ticket replaces this context's cell alone, and what is derived from it
	/// afterwards.
	index_places: Arc<std::sync::OnceLock<crate::idx::places::Ticket>>,
	/// The per-layer configuration a running query reads. Crate-internal: these
	/// are operator knobs, not API.
	pub(crate) config: Arc<RuntimeConfig>,
	/// Client for making http requests.
	#[cfg(feature = "http")]
	http_client: Arc<HttpClient>,
	/// What every context of the datastore holds alike: what the datastore
	/// was built with. Held once and shared, so a context is derived without
	/// counting a reference to each, which every core would contend for.
	shared: Arc<Shared>,
}

/// The parts of a [`Context`] the datastore it was built from holds, which
/// every context derived from it shares.
#[derive(Clone)]
struct Shared {
	// An optional datastore cache
	cache: Option<Arc<DatastoreCache>>,
	// The index store
	index_stores: IndexStores,
	// The index concurrent builders. Held behind an `Arc` so that deriving a child context
	// is one reference-count bump rather than one per handle the builder carries.
	index_builder: Option<Arc<IndexBuilder>>,
	// The sequences, behind an `Arc` for the same reason.
	sequences: Option<Arc<Sequences>>,
	#[cfg(storage)]
	// The temporary directory
	temporary_directory: Option<Arc<PathBuf>>,
	// A map of bucket connections
	buckets: Option<BucketsManager>,
	// The surrealism cache
	#[cfg(feature = "surrealism")]
	surrealism_cache: Option<Arc<SurrealismCache>>,
	/// The datastore's capabilities when this context's root was created.
	/// Unlike [`Context::capabilities`], an extension's host call never narrows
	/// it, so it is what an extension's `CAPABILITIES` are intersected with.
	#[cfg(feature = "surrealism")]
	server_capabilities: Arc<Capabilities>,
	/// The datastore's function registry.
	///
	/// A function is resolved by name against one registry per datastore, so
	/// this is the datastore's and is carried rather than rebuilt. It reaches
	/// the context because compiling an expression needs it and not every
	/// compiler holds an [`crate::exec::ExecutionContext`]: a schema
	/// statement's clauses are compiled wherever the statement is reached,
	/// which includes the evaluator that owns the catalog writes.
	function_registry: Arc<FunctionRegistry>,
	/// The live-subscription units this node has compiled for the VM
	/// (`crate::exec::vm::live`), carried as the function registry is.
	live_units: Option<Arc<crate::exec::vm::live::LiveUnits>>,
	/// The function-body units this node has compiled (`crate::exec::vm::units`),
	/// carried as the live-subscription units are.
	compiled_units: Option<Arc<crate::exec::vm::units::CompiledUnits>>,
	/// Stable node id for this datastore instance (live routing, queue keys, etc.).
	node_id: Uuid,
	/// Runtime-adjustable configuration (e.g. global query timeout).
	dynamic_configuration: DynamicConfiguration,
}

impl Debug for Context {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.debug_struct("Context")
			.field("parent", &self.parent)
			.field("deadline", &self.deadline)
			.field("cancelled", &self.cancelled)
			.field("values", &self.values)
			.field("forbidden", &self.forbidden)
			.finish()
	}
}

impl Context {
	/// Creates a new empty background context. It carries the parent's
	/// statement clock: the work it runs, a live subscription's clauses for one
	/// write, belongs to the statement that built it. It holds none of the
	/// parent's values, so it carries none of its request parameters either.
	pub(crate) fn background(parent: &Context) -> Self {
		Self {
			values: Arc::default(),
			forbidden: Vec::new(),
			parent: None,
			deadline: None,
			slow_log: None,
			cancelled: Arc::new(AtomicBool::new(false)),
			cancel_token: None,
			capabilities: Arc::clone(&parent.capabilities),
			transaction: None,
			isolated: false,
			#[cfg(feature = "surrealism")]
			extension_depth: 0,
			redact_volatile_explain_attrs: false,
			statement_counters: None,
			statement_clock: parent.statement_clock,
			request_params: None,
			call_boundary: false,
			config: Arc::clone(&parent.config),
			surface: parent.surface,
			vm_units: parent.vm_units.clone(),
			vm_deref: parent.vm_deref.clone(),
			vm_served: parent.vm_served.clone(),
			vm_metrics: parent.vm_metrics.clone(),
			query_trace: parent.query_trace.clone(),
			vm_edge_cascade: parent.vm_edge_cascade.clone(),
			vm_closures: parent.vm_closures.clone(),
			tenant_identity: None,
			auth_enabled: parent.auth_enabled,
			live: parent.live,
			broker: parent.broker.clone(),
			live_recheck: parent.live_recheck.clone(),
			changefeed_source: parent.changefeed_source.clone(),
			scheduler: parent.scheduler.clone(),
			morsel_places: parent.morsel_places.clone(),
			event_queue_node: parent.event_queue_node,
			memory: parent.memory.clone(),
			index_places: Arc::new(std::sync::OnceLock::new()),
			#[cfg(feature = "http")]
			http_client: Arc::clone(&parent.http_client),
			shared: Arc::new(Shared {
				// Derived rather than new: this context is built per unit of work
				// (one LIVE query delivery), and a segmentation dictionary is far
				// too expensive to load and drop that often, so those are shared
				// with the parent while the other caches stay isolated.
				index_stores: parent.shared.index_stores.derive(
					parent.config.idx.hnsw_cache_size,
					parent.config.idx.diskann_cache_size,
					parent.config.idx.graph_resolve_cache_size,
					parent.config.idx.fulltext_cache_size,
				),
				cache: None,
				index_builder: None,
				sequences: None,
				#[cfg(storage)]
				temporary_directory: None,
				buckets: None,
				#[cfg(feature = "surrealism")]
				surrealism_cache: None,
				#[cfg(feature = "surrealism")]
				server_capabilities: Arc::clone(&parent.shared.server_capabilities),
				function_registry: Arc::clone(&parent.shared.function_registry),
				live_units: parent.shared.live_units.clone(),
				compiled_units: parent.shared.compiled_units.clone(),
				node_id: parent.shared.node_id,
				dynamic_configuration: parent.shared.dynamic_configuration.clone(),
			}),
		}
	}

	/// Creates a new child context from a frozen parent context.
	pub(crate) fn new_child(parent: &FrozenContext) -> Self {
		Self::new_child_with_capabilities(
			parent,
			Arc::clone(&parent.capabilities),
			#[cfg(feature = "http")]
			Arc::clone(&parent.http_client),
		)
	}

	/// Creates a new context from a frozen parent context with a given capabilities.
	///
	/// Make sure that the capabilities and http_client were created with the same capabilities.
	pub(crate) fn new_child_with_capabilities(
		parent: &FrozenContext,
		cap: Arc<Capabilities>,
		#[cfg(feature = "http")] http_client: Arc<HttpClient>,
	) -> Self {
		Context {
			values: Arc::default(),
			forbidden: Vec::new(),
			deadline: parent.deadline,
			slow_log: parent.slow_log.clone(),
			cancelled: Arc::new(AtomicBool::new(false)),
			cancel_token: None,
			capabilities: cap,
			transaction: parent.transaction.clone(),
			isolated: false,
			parent: Some(Arc::clone(parent)),
			#[cfg(feature = "surrealism")]
			extension_depth: parent.extension_depth,
			redact_volatile_explain_attrs: parent.redact_volatile_explain_attrs,
			statement_counters: parent.statement_counters.clone(),
			statement_clock: parent.statement_clock,
			request_params: parent.request_params.clone(),
			call_boundary: false,
			config: Arc::clone(&parent.config),
			#[cfg(feature = "http")]
			http_client,
			surface: parent.surface,
			vm_units: parent.vm_units.clone(),
			vm_deref: parent.vm_deref.clone(),
			vm_served: parent.vm_served.clone(),
			vm_metrics: parent.vm_metrics.clone(),
			query_trace: parent.query_trace.clone(),
			vm_edge_cascade: parent.vm_edge_cascade.clone(),
			vm_closures: parent.vm_closures.clone(),
			tenant_identity: parent.tenant_identity.clone(),
			auth_enabled: parent.auth_enabled,
			live: parent.live,
			broker: parent.broker.clone(),
			live_recheck: parent.live_recheck.clone(),
			changefeed_source: parent.changefeed_source.clone(),
			scheduler: parent.scheduler.clone(),
			morsel_places: parent.morsel_places.clone(),
			event_queue_node: parent.event_queue_node,
			memory: parent.memory.clone(),
			index_places: Arc::clone(&parent.index_places),
			shared: Arc::clone(&parent.shared),
		}
	}

	/// Create a new context from a frozen parent context.
	/// This context is isolated, and values specified on
	/// any parent contexts will not be accessible: the protected names
	/// (`$auth`, `$access`, `$token`, `$session`) aside, which are read through
	/// the parent chain. The parent's request parameters are values too, so
	/// the context holds none until it attaches its own.
	pub(crate) fn new_isolated(parent: &FrozenContext) -> Self {
		Self {
			values: Arc::default(),
			forbidden: Vec::new(),
			deadline: parent.deadline,
			slow_log: parent.slow_log.clone(),
			cancelled: Arc::new(AtomicBool::new(false)),
			cancel_token: None,
			capabilities: Arc::clone(&parent.capabilities),
			transaction: parent.transaction.clone(),
			isolated: true,
			parent: Some(Arc::clone(parent)),
			#[cfg(feature = "surrealism")]
			extension_depth: parent.extension_depth,
			redact_volatile_explain_attrs: parent.redact_volatile_explain_attrs,
			statement_counters: parent.statement_counters.clone(),
			statement_clock: parent.statement_clock,
			request_params: None,
			call_boundary: false,
			config: Arc::clone(&parent.config),
			surface: parent.surface,
			vm_units: parent.vm_units.clone(),
			vm_deref: parent.vm_deref.clone(),
			vm_served: parent.vm_served.clone(),
			vm_metrics: parent.vm_metrics.clone(),
			query_trace: parent.query_trace.clone(),
			vm_edge_cascade: parent.vm_edge_cascade.clone(),
			vm_closures: parent.vm_closures.clone(),
			tenant_identity: parent.tenant_identity.clone(),
			auth_enabled: parent.auth_enabled,
			live: parent.live,
			broker: parent.broker.clone(),
			live_recheck: parent.live_recheck.clone(),
			changefeed_source: parent.changefeed_source.clone(),
			scheduler: parent.scheduler.clone(),
			morsel_places: parent.morsel_places.clone(),
			event_queue_node: parent.event_queue_node,
			memory: parent.memory.clone(),
			index_places: Arc::clone(&parent.index_places),
			#[cfg(feature = "http")]
			http_client: Arc::clone(&parent.http_client),
			shared: Arc::clone(&parent.shared),
		}
	}

	/// Create an independent snapshot of a frozen context.
	///
	/// Flattens all values from the parent chain into the snapshot's own
	/// `values` map and sets `parent: None`, so the returned context does
	/// **not** hold an `Arc` reference to the original parent.
	///
	/// This is used by the engine to give the operator pipeline
	/// its own `Arc<Context>` that won't interfere with the executor's
	/// `Arc::get_mut` requirements between statements.
	///
	/// The cancellation flag and awaitable token are **cloned** from the
	/// source rather than allocated fresh. Without this, any operator that
	/// polls `Context::done` / `is_done` — e.g.
	/// the KNN operator calling into
	/// `idx::trees::hnsw::knn_search` / `idx::trees::diskann::knn_search`,
	/// whose inner loops poll `frozen_ctx.is_done(Some(count))` — would
	/// observe the snapshot's local (never-tripped) flag instead of the
	/// connection-level cancel handle installed on the source root, and
	/// would not abort on WebSocket disconnect.
	///
	/// A snapshot of a context inside a function body keeps the body's view:
	/// it holds the bindings [`Self::lexical_value`] answers there and is
	/// itself a call boundary, so what the body could not read the snapshot
	/// does not hold.
	pub(crate) fn snapshot(from: &FrozenContext) -> Self {
		let in_call_body = from.in_call_body();
		Self {
			// Flatten all values from the parent chain into this context
			values: match (in_call_body, &from.parent) {
				(true, _) => Arc::new(from.lexical_values()),
				// A context with no parent holds every value it answers: the
				// snapshot shares them rather than copying each.
				(false, None) => Arc::clone(&from.values),
				(false, Some(_)) => Arc::new(from.collect_values(HashMap::default())),
			},
			forbidden: from.collect_forbidden(),
			deadline: from.deadline,
			slow_log: from.slow_log.clone(),
			cancelled: Arc::clone(&from.cancelled),
			cancel_token: from.cancel_token.clone(),
			capabilities: Arc::clone(&from.capabilities),
			transaction: from.transaction.clone(),
			isolated: false,
			parent: None, // No parent reference — fully independent
			#[cfg(feature = "surrealism")]
			extension_depth: from.extension_depth,
			redact_volatile_explain_attrs: from.redact_volatile_explain_attrs,
			statement_counters: from.statement_counters.clone(),
			statement_clock: from.statement_clock,
			request_params: from.request_params.clone(),
			call_boundary: in_call_body,
			config: Arc::clone(&from.config),
			surface: from.surface,
			vm_units: from.vm_units.clone(),
			vm_deref: from.vm_deref.clone(),
			vm_served: from.vm_served.clone(),
			vm_metrics: from.vm_metrics.clone(),
			query_trace: from.query_trace.clone(),
			vm_edge_cascade: from.vm_edge_cascade.clone(),
			vm_closures: from.vm_closures.clone(),
			tenant_identity: from.tenant_identity.clone(),
			auth_enabled: from.auth_enabled,
			live: from.live,
			broker: from.broker.clone(),
			live_recheck: from.live_recheck.clone(),
			changefeed_source: from.changefeed_source.clone(),
			scheduler: from.scheduler.clone(),
			morsel_places: from.morsel_places.clone(),
			event_queue_node: from.event_queue_node,
			memory: from.memory.clone(),
			index_places: Arc::clone(&from.index_places),
			#[cfg(feature = "http")]
			http_client: Arc::clone(&from.http_client),
			shared: Arc::clone(&from.shared),
		}
	}

	/// Create a new context from a frozen parent context.
	/// This context is not linked to the parent context,
	/// and won't be cancelled if the parent is cancelled.
	///
	/// No statement clock and no statement counters are carried: the context
	/// runs a background task of its own, outside any top-level statement, and
	/// nothing written beneath it is a row the statement that started the task
	/// affected.
	///
	/// No query memory is carried either. A query's memory holds its lease from
	/// the node's governor, and the lease, with the minimum the governor
	/// guarantees it, stays reserved until the last context holding that memory
	/// drops. This context outlives the query `from` belongs to — the
	/// `Building` holding it lives for as long as the `IndexBuilder` remembers
	/// the build — so it would keep that lease from every other query. Work
	/// beneath it is bounded by a query memory installed in a context derived
	/// from it, as each build batch's context holds one of its own, whose lease
	/// closes when that context drops.
	///
	/// `index_builder` is intentionally cleared: the only caller is the
	/// background index `Building` task, which lives inside the
	/// `IndexBuilder`'s HashMap. Cloning the back-reference would form an
	/// `Arc<RwLock<HashMap<.., Arc<Building>>>>` cycle that pins the
	/// `Datastore` (and its storage handles, ~3 file descriptors per SurrealKV
	/// instance) until process exit.
	///
	/// `vm_closures` is cleared too. That table belongs to the run of the
	/// program `from` was made in, and a unit linked into it keeps a handle on
	/// the transaction it last checked its catalog reads in, so a context that
	/// outlives the run — the `Building` does, for as long as the
	/// `IndexBuilder` remembers it — would keep that transaction open with it.
	/// A closure invoked beneath this context links its unit into a table
	/// installed beneath it, as each build batch's context installs one, or
	/// into a table of the invocation's own.
	///
	/// `live_recheck` is not carried either: it notes the writes of the
	/// transaction `from` runs in, and holds what that transaction wrote until
	/// it has committed, while this context runs transactions of its own.
	pub(crate) fn new_concurrent(from: &FrozenContext) -> Self {
		Self {
			values: Arc::default(),
			forbidden: Vec::new(),
			deadline: None,
			slow_log: from.slow_log.clone(),
			cancelled: Arc::new(AtomicBool::new(false)),
			cancel_token: None,
			capabilities: Arc::clone(&from.capabilities),
			transaction: None,
			isolated: false,
			parent: None,
			#[cfg(feature = "surrealism")]
			extension_depth: from.extension_depth,
			redact_volatile_explain_attrs: from.redact_volatile_explain_attrs,
			statement_counters: None,
			statement_clock: None,
			request_params: None,
			call_boundary: false,
			config: Arc::clone(&from.config),
			surface: from.surface,
			vm_units: from.vm_units.clone(),
			vm_deref: from.vm_deref.clone(),
			vm_served: from.vm_served.clone(),
			vm_metrics: from.vm_metrics.clone(),
			query_trace: from.query_trace.clone(),
			vm_edge_cascade: from.vm_edge_cascade.clone(),
			vm_closures: None,
			tenant_identity: from.tenant_identity.clone(),
			auth_enabled: from.auth_enabled,
			live: from.live,
			broker: from.broker.clone(),
			live_recheck: None,
			changefeed_source: from.changefeed_source.clone(),
			scheduler: from.scheduler.clone(),
			morsel_places: from.morsel_places.clone(),
			event_queue_node: from.event_queue_node,
			memory: None,
			index_places: Arc::new(std::sync::OnceLock::new()),
			#[cfg(feature = "http")]
			http_client: Arc::clone(&from.http_client),
			// The index builder is not carried, as above.
			shared: Arc::new(Shared {
				index_builder: None,
				..Shared::clone(&from.shared)
			}),
		}
	}

	/// Creates a new context from a configured datastore.
	#[expect(clippy::too_many_arguments)]
	pub(crate) fn from_ds(
		node_id: Uuid,
		auth_enabled: bool,
		dynamic_configuration: DynamicConfiguration,
		time_out: Option<Duration>,
		slow_log: Option<SlowLog>,
		capabilities: Arc<Capabilities>,
		index_stores: IndexStores,
		index_builder: IndexBuilder,
		sequences: Sequences,
		cache: Arc<DatastoreCache>,
		#[cfg(feature = "http")] http_client: Arc<HttpClient>,
		function_registry: Arc<FunctionRegistry>,
		surface: &'static Surface,
		live_units: Arc<crate::exec::vm::live::LiveUnits>,
		compiled_units: Arc<crate::exec::vm::units::CompiledUnits>,
		#[cfg(storage)] temporary_directory: Option<Arc<PathBuf>>,
		buckets: BucketsManager,
		config: Arc<RuntimeConfig>,
		#[cfg(feature = "surrealism")] surrealism_cache: Arc<SurrealismCache>,
	) -> Result<Context> {
		#[cfg(feature = "surrealism")]
		let server_capabilities = Arc::clone(&capabilities);
		let mut ctx = Self {
			values: Arc::default(),
			forbidden: Vec::new(),
			parent: None,
			deadline: None,
			slow_log,
			cancelled: Arc::new(AtomicBool::new(false)),
			cancel_token: None,
			capabilities,
			transaction: None,
			isolated: false,
			#[cfg(feature = "surrealism")]
			extension_depth: 0,
			redact_volatile_explain_attrs: false,
			statement_counters: None,
			statement_clock: None,
			request_params: None,
			call_boundary: false,
			config,
			#[cfg(feature = "http")]
			http_client,
			surface,
			vm_units: None,
			vm_deref: None,
			vm_served: None,
			vm_metrics: None,
			query_trace: None,
			vm_edge_cascade: None,
			vm_closures: None,
			tenant_identity: None,
			auth_enabled,
			live: false,
			broker: None,
			live_recheck: None,
			changefeed_source: None,
			scheduler: None,
			morsel_places: None,
			event_queue_node: None,
			memory: None,
			index_places: Arc::new(std::sync::OnceLock::new()),
			shared: Arc::new(Shared {
				index_stores,
				cache: Some(cache),
				index_builder: Some(Arc::new(index_builder)),
				sequences: Some(Arc::new(sequences)),
				buckets: Some(buckets),
				#[cfg(feature = "surrealism")]
				surrealism_cache: Some(surrealism_cache),
				#[cfg(storage)]
				temporary_directory,
				#[cfg(feature = "surrealism")]
				server_capabilities,
				function_registry,
				live_units: Some(live_units),
				compiled_units: Some(compiled_units),
				node_id,
				dynamic_configuration,
			}),
		};
		if let Some(timeout) = time_out {
			ctx.add_timeout(timeout)?;
		}
		Ok(ctx)
	}

	/// Create a context for tests only
	#[cfg(test)]
	pub(crate) fn new_test() -> Context {
		Self {
			values: Arc::default(),
			forbidden: Vec::new(),
			parent: None,
			deadline: None,
			slow_log: None,
			cancelled: Arc::new(AtomicBool::new(false)),
			cancel_token: None,
			capabilities: Arc::new(Capabilities::default()),
			transaction: None,
			isolated: false,
			#[cfg(feature = "surrealism")]
			extension_depth: 0,
			redact_volatile_explain_attrs: false,
			statement_counters: None,
			statement_clock: None,
			request_params: None,
			call_boundary: false,
			config: Default::default(),
			surface: &crate::exec::surface::FULL,
			vm_units: None,
			vm_deref: None,
			vm_served: None,
			vm_metrics: None,
			query_trace: None,
			vm_edge_cascade: None,
			vm_closures: None,
			tenant_identity: None,
			auth_enabled: true,
			live: false,
			broker: None,
			live_recheck: None,
			changefeed_source: None,
			scheduler: None,
			morsel_places: None,
			event_queue_node: None,
			memory: None,
			index_places: Arc::new(std::sync::OnceLock::new()),
			#[cfg(feature = "http")]
			http_client: Arc::new(
				HttpClient::new(
					crate::dbs::capabilities::Targets::All,
					crate::dbs::capabilities::Targets::None,
					&Default::default(),
				)
				.expect("http client to be created"),
			),
			shared: Arc::new(Shared {
				index_stores: IndexStores::new(
					256 * 1024 * 1024,
					256 * 1024 * 1024,
					64 * 1024 * 1024,
					128 * 1024 * 1024,
					None,
				),
				cache: None,
				index_builder: None,
				sequences: None,
				#[cfg(storage)]
				temporary_directory: None,
				buckets: None,
				#[cfg(feature = "surrealism")]
				surrealism_cache: None,
				#[cfg(feature = "surrealism")]
				server_capabilities: Arc::new(Capabilities::default()),
				function_registry: Arc::new(FunctionRegistry::with_builtins()),
				live_units: None,
				compiled_units: None,
				node_id: Uuid::nil(),
				dynamic_configuration: DynamicConfiguration::default(),
			}),
		}
	}

	/// Freezes this context, allowing it to be used as a parent context.
	pub(crate) fn freeze(self) -> FrozenContext {
		Arc::new(self)
	}

	#[inline]
	/// The datastore's function registry, for a caller compiling an
	/// expression that may name a function.
	pub(crate) fn function_registry(&self) -> &Arc<FunctionRegistry> {
		&self.shared.function_registry
	}

	/// The statement families the entry that started the query serves.
	#[inline]
	pub(crate) fn surface(&self) -> &'static Surface {
		self.surface
	}

	/// The live-subscription units this node has compiled for the VM, or
	/// `None` for a context made without a datastore.
	pub(crate) fn live_units(&self) -> Option<Arc<crate::exec::vm::live::LiveUnits>> {
		self.shared.live_units.clone()
	}

	/// The unit table of the function bundle executing around this context,
	/// if any.
	pub(crate) fn vm_units(&self) -> Option<&crate::exec::vm::UnitTable> {
		self.vm_units.as_ref()
	}

	/// Make the units of `units` the ones the nested programs of this context
	/// find their ids in.
	pub(crate) fn set_vm_units(&mut self, units: crate::exec::vm::UnitTable) {
		self.vm_units = Some(units);
	}

	/// The late-bound statements the program executing around this context
	/// serves, if any.
	pub(crate) fn vm_served(&self) -> Option<&crate::exec::vm::ServedStatements> {
		self.vm_served.as_ref()
	}

	/// Make `served` the late-bound statements the program executing around
	/// this context serves.
	pub(crate) fn set_vm_served(&mut self, served: crate::exec::vm::ServedStatements) {
		self.vm_served = Some(served);
	}

	/// The per-table units the program executing around this context reads
	/// records by id through, if any.
	pub(crate) fn vm_deref(&self) -> Option<&Arc<crate::exec::vm::Deref>> {
		self.vm_deref.as_ref()
	}

	/// Make `deref` the per-table units the record reads of this context go
	/// through.
	pub(crate) fn set_vm_deref(&mut self, deref: Arc<crate::exec::vm::Deref>) {
		self.vm_deref = Some(deref);
	}

	/// The metrics the program running around this context records into, when
	/// its plan is analysed.
	pub(crate) fn vm_metrics(&self) -> Option<&Arc<crate::exec::metrics::OperatorMetrics>> {
		self.vm_metrics.as_ref()
	}

	/// Make `metrics` the ones the program running around this context
	/// records into.
	pub(crate) fn set_vm_metrics(&mut self, metrics: Arc<crate::exec::metrics::OperatorMetrics>) {
		self.vm_metrics = Some(metrics);
	}

	/// The query trace the statement running around this context reports to,
	/// when its request asked for one.
	pub(crate) fn query_trace(&self) -> Option<&Arc<crate::exec::trace::StatementTrace>> {
		self.query_trace.as_ref()
	}

	/// Make `trace` the query trace the statement running around this context
	/// reports to.
	pub(crate) fn set_query_trace(&mut self, trace: Arc<crate::exec::trace::StatementTrace>) {
		self.query_trace = Some(trace);
	}

	/// The edge cascade running around this context, if any.
	pub(crate) fn vm_edge_cascade(&self) -> Option<&Arc<crate::exec::vm::program::Program>> {
		self.vm_edge_cascade.as_ref()
	}

	/// Make `cascade` the edge cascade running around this context.
	pub(crate) fn set_vm_edge_cascade(&mut self, cascade: Arc<crate::exec::vm::program::Program>) {
		self.vm_edge_cascade = Some(cascade);
	}

	/// The closure-body units registered around this context, if any.
	pub(crate) fn vm_closures(&self) -> Option<&Arc<crate::exec::vm::closure::ClosureUnits>> {
		self.vm_closures.as_ref()
	}

	/// Make `closures` the registry the closure invocations of this context
	/// find their units in.
	pub(crate) fn set_vm_closures(
		&mut self,
		closures: Arc<crate::exec::vm::closure::ClosureUnits>,
	) {
		self.vm_closures = Some(closures);
	}

	/// The function-body units this node has compiled, or `None` for a
	/// context made without a datastore.
	pub(crate) fn compiled_units(&self) -> Option<&Arc<crate::exec::vm::units::CompiledUnits>> {
		self.shared.compiled_units.as_ref()
	}

	pub fn node_id(&self) -> Uuid {
		self.shared.node_id
	}

	#[inline]
	pub(crate) fn auth_enabled(&self) -> bool {
		self.auth_enabled
	}

	pub(crate) fn dynamic_configuration(&self) -> &DynamicConfiguration {
		&self.shared.dynamic_configuration
	}

	/// Whether this session may open `LIVE` queries or `KILL LIVE` (from [`Session::live`]
	/// applied in [`Context::attach_session`]).
	pub(crate) fn realtime(&self) -> Result<()> {
		if !self.live {
			bail!(DatastoreError::RealtimeDisabled);
		}
		Ok(())
	}

	pub(crate) fn broker(&self) -> Option<&Arc<dyn MessageBroker>> {
		self.broker.as_ref()
	}

	/// Install `broker`, or clear the broker with `None`. Either way the context
	/// no longer notes writes for a recheck ([`Self::set_live_recheck`]).
	pub(crate) fn set_broker(&mut self, broker: Option<Arc<dyn MessageBroker>>) {
		self.broker = broker;
		self.live_recheck = None;
	}

	/// Where a write notes what it wrote, for the subscriptions its snapshot
	/// did not hold to be told once it commits.
	pub(crate) fn live_recheck(&self) -> Option<&Arc<crate::dbs::live_recheck::LiveRecheck>> {
		self.live_recheck.as_ref()
	}

	/// Note this context's writes in `recheck`, until the broker is next set.
	pub(crate) fn set_live_recheck(
		&mut self,
		recheck: Option<Arc<crate::dbs::live_recheck::LiveRecheck>>,
	) {
		self.live_recheck = recheck;
	}

	/// The host-provided `SHOW CHANGES` source, if one was installed.
	pub(crate) fn changefeed_source(
		&self,
	) -> Option<&Arc<dyn crate::lq::external::ChangeFeedSource>> {
		self.changefeed_source.as_ref()
	}

	pub(crate) fn set_changefeed_source(
		&mut self,
		source: Option<Arc<dyn crate::lq::external::ChangeFeedSource>>,
	) {
		self.changefeed_source = source;
	}

	/// The scheduler a running program yields through, `None` for the
	/// default.
	pub(crate) fn scheduler(&self) -> Option<&Arc<dyn crate::exec::scheduler::Scheduler>> {
		self.scheduler.as_ref()
	}

	pub(crate) fn set_scheduler(
		&mut self,
		scheduler: Option<Arc<dyn crate::exec::scheduler::Scheduler>>,
	) {
		self.scheduler = scheduler;
	}

	/// The host's places a morsel pipeline's morsels may run at, `None` where the host gives
	/// none.
	pub(crate) fn morsel_places(&self) -> Option<&Arc<dyn crate::exec::places::MorselPlaces>> {
		self.morsel_places.as_ref()
	}

	/// The node whose async event queue an event queued by a write in this context joins: the
	/// node the datastore was told to queue under, else this context's own.
	pub(crate) fn event_queue_node(&self) -> Uuid {
		self.event_queue_node.unwrap_or(self.shared.node_id)
	}

	pub(crate) fn set_event_queue_node(&mut self, node: Option<Uuid>) {
		self.event_queue_node = node;
	}

	pub(crate) fn set_morsel_places(
		&mut self,
		places: Option<Arc<dyn crate::exec::places::MorselPlaces>>,
	) {
		self.morsel_places = places;
	}

	/// The ticket the index engines' parallel work takes its places through,
	/// when one is set on this context.
	pub(crate) fn index_places(&self) -> Option<&crate::idx::places::Ticket> {
		self.index_places.get()
	}

	/// Sets the ticket the index engines' parallel work takes its places
	/// through; see [`IndexPlaces`].
	pub(crate) fn set_index_places(&mut self, places: crate::idx::places::Ticket) {
		self.index_places = Arc::new(std::sync::OnceLock::from(places));
	}

	/// The query's memory, `None` where nothing is bounded.
	pub(crate) fn memory(&self) -> Option<&Arc<crate::exec::memory::QueryMemory>> {
		self.memory.as_ref()
	}

	pub(crate) fn set_memory(&mut self, memory: Option<Arc<crate::exec::memory::QueryMemory>>) {
		self.memory = memory;
	}

	/// IAM check using auth from [`Options`] and the datastore auth toggle on this context.
	pub fn is_allowed(
		&self,
		opt: &Options,
		action: Action,
		res: ResourceKind,
		base: Base,
	) -> Result<()> {
		let res = match base {
			Base::Root => res.on_root(),
			Base::Ns => res.on_ns(opt.ns()?),
			Base::Db => {
				let (ns, db) = opt.ns_db()?;
				res.on_db(ns, db)
			}
		};

		if !self.auth_enabled && opt.auth.is_anon() {
			return Ok(());
		}

		Ok(opt.auth.is_allowed(action, &res).map_err(crate::err::Error::from)?)
	}

	/// Whether a caller acting on a bucket at `level` is judged by the bucket
	/// rather than exempt by its role: [`Self::check_perms`] for a database's
	/// bucket, and for a bucket at the root or the session's namespace the role
	/// the caller holds at that level or above it.
	pub(crate) fn check_bucket_perms(
		&self,
		opt: &Options,
		action: Action,
		level: crate::catalog::BucketLevel,
	) -> Result<bool> {
		let ns = match level {
			crate::catalog::BucketLevel::Db(..) => return self.check_perms(opt, action),
			crate::catalog::BucketLevel::Ns(_) => Some(opt.ns()?),
			crate::catalog::BucketLevel::Root => None,
		};
		Ok(opt.perms
			&& crate::exec::permission::action_is_checked(
				self.auth_enabled,
				&opt.auth,
				action,
				ns,
				None,
			))
	}

	/// Table-level permission check, which the options' `perms` flag gates.
	pub fn check_perms(&self, opt: &Options, action: Action) -> Result<bool> {
		if !opt.perms {
			return Ok(false);
		}
		if !self.auth_enabled && opt.auth.is_anon() {
			return Ok(false);
		}
		match action {
			Action::Edit => {
				let allowed = opt.auth.has_editor_role();
				let (ns, db) = opt.ns_db()?;
				let db_in_actor_level =
					opt.auth.is_root() || opt.auth.is_ns_check(ns) || opt.auth.is_db_check(ns, db);
				Ok(!allowed || !db_in_actor_level)
			}
			Action::View => {
				let allowed = opt.auth.has_viewer_role();
				let (ns, db) = opt.ns_db()?;
				let db_in_actor_level =
					opt.auth.is_root() || opt.auth.is_ns_check(ns) || opt.auth.is_db_check(ns, db);
				Ok(!allowed || !db_in_actor_level)
			}
		}
	}

	/// Get the namespace id for the current context.
	/// If the namespace does not exist, it will be try to be created based on
	/// the `strict` option.
	pub(crate) async fn get_ns_id(&self, opt: &Options) -> Result<NamespaceId> {
		let ns = opt.ns()?;
		let tx = self.tx();
		// Creating a missing namespace to hold a definition needs the same
		// authority as an explicit DEFINE NAMESPACE (Edit on Namespace at root),
		// matching the USE path, so a database-level principal cannot recreate a
		// removed namespace through an ordinary DDL statement
		// (GHSA-3287-hpm5-3f8r). The existence read runs unsealed because this is
		// reached from sealed work (e.g. view maintenance); an authorization
		// check of the live catalog is orthogonal to that work's snapshot.
		let ns_present =
			crate::kvs::catalog_reads::sealed(false, tx.get_ns_by_name(ns, None)).await?.is_some();
		if !ns_present {
			self.is_allowed(opt, Action::Edit, ResourceKind::Namespace, Base::Root)?;
		}
		let ns_def = tx.get_or_add_ns(Some(self), ns).await?;
		Ok(ns_def.namespace_id)
	}

	/// Get the namespace id for the current context.
	/// If the namespace does not exist, it will return an error.
	pub(crate) async fn expect_ns_id(&self, opt: &Options) -> Result<NamespaceId> {
		let ns = opt.ns()?;
		let Some(ns_def) = self.tx().get_ns_by_name(ns, None).await? else {
			return Err(CatalogError::NsNotFound {
				name: ns.to_string(),
			}
			.into());
		};
		Ok(ns_def.namespace_id)
	}

	/// Get the namespace and database ids for the current context.
	/// If the namespace or database does not exist, it will be try to be
	/// created based on the `strict` option.
	pub(crate) async fn get_ns_db_ids(&self, opt: &Options) -> Result<(NamespaceId, DatabaseId)> {
		let (ns, db) = opt.ns_db()?;
		let txn = self.tx();
		// Creating a missing namespace or database to hold a definition needs
		// the same authority as creating it explicitly (Edit on Namespace at
		// root / Edit on Database at the namespace), matching the USE path, so a
		// database-level principal cannot recreate a removed namespace or
		// database through an ordinary DDL statement (GHSA-3287-hpm5-3f8r). The
		// existence reads run unsealed because this is reached from sealed work
		// (e.g. view maintenance); an authorization check of the live catalog is
		// orthogonal to that work's snapshot.
		let ns_present =
			crate::kvs::catalog_reads::sealed(false, txn.get_ns_by_name(ns, None)).await?.is_some();
		if !ns_present {
			self.is_allowed(opt, Action::Edit, ResourceKind::Namespace, Base::Root)?;
		} else {
			let db_present =
				crate::kvs::catalog_reads::sealed(false, txn.get_db_by_name(ns, db, None))
					.await?
					.is_some();
			if !db_present {
				self.is_allowed(opt, Action::Edit, ResourceKind::Database, Base::Ns)?;
			}
		}
		let db_def =
			crate::exec::vm::descriptor::object(txn.ensure_ns_db(Some(self), ns, db)).await?;
		Ok((db_def.namespace_id, db_def.database_id))
	}

	/// The level of the buckets the current context reaches: the session's
	/// database when one is selected, else its namespace, else the root. A
	/// selected namespace or database that does not exist is an error.
	pub(crate) async fn expect_bucket_level(
		&self,
		opt: &Options,
	) -> Result<crate::catalog::BucketLevel> {
		if opt.db.is_some() {
			let (ns, db) = self.expect_ns_db_ids(opt).await?;
			Ok(crate::catalog::BucketLevel::Db(ns, db))
		} else if opt.ns.is_some() {
			Ok(crate::catalog::BucketLevel::Ns(self.expect_ns_id(opt).await?))
		} else {
			Ok(crate::catalog::BucketLevel::Root)
		}
	}

	/// Get the namespace and database ids for the current context.
	/// If the namespace or database does not exist, it will return an error.
	pub(crate) async fn expect_ns_db_ids(
		&self,
		opt: &Options,
	) -> Result<(NamespaceId, DatabaseId)> {
		let (ns, db) = opt.ns_db()?;
		let txn = self.tx();
		let Some(db_def) =
			crate::exec::vm::descriptor::object(txn.get_db_by_name(ns, db, None)).await?
		else {
			return Err(CatalogError::DbNotFound {
				name: db.to_string(),
			}
			.into());
		};
		Ok((db_def.namespace_id, db_def.database_id))
	}

	/// Add a value to the context. It overwrites any previously set values
	/// with the same key.
	pub(crate) fn add_value<K>(&mut self, key: K, value: Arc<Value>)
	where
		K: Into<Strand>,
	{
		let key = key.into();
		if !self.forbidden.is_empty() {
			self.forbidden.retain(|(name, _)| *name != key);
		}
		Arc::make_mut(&mut self.values).insert(key, value);
	}

	/// Bind each of `names` to `row`, a KNN query vector's row being scanned,
	/// rather than to a value: [`Self::binding`] answers
	/// [`Binding::Forbidden`] for it, and a value bound at this level after it,
	/// or at a deeper one, shadows it.
	pub(crate) fn add_forbidden(&mut self, names: &[&str], row: &Arc<ForbiddenRow>) {
		for name in names {
			Arc::make_mut(&mut self.values).remove(*name);
			self.forbidden.retain(|(bound, _)| bound.as_str() != *name);
			self.forbidden.push((Strand::from(*name), Arc::clone(row)));
		}
	}

	/// Add a value to the context. It overwrites any previously set values
	/// with the same key.
	pub(crate) fn add_values<T, K, V>(&mut self, iter: T)
	where
		T: IntoIterator<Item = (K, V)>,
		K: Into<Strand>,
		V: Into<Arc<Value>>,
	{
		for (key, value) in iter {
			self.add_value(key, value.into());
		}
	}

	#[cfg(test)]
	/// Add cancellation to the context. The value that is returned will cancel
	/// the context and it's children once called.
	pub(crate) fn add_cancel(&mut self) -> Canceller {
		let cancelled = Arc::clone(&self.cancelled);
		Canceller::new(cancelled)
	}

	/// Install an externally-owned cancellation handle on this context.
	/// Child contexts allocate their own `cancelled` flag but walk up the
	/// parent chain in [`Self::done`], so installing the handle on the
	/// root is enough for the entire execution tree to observe it.
	///
	/// Stores both views of the handle:
	///
	/// * The `AtomicBool` flag is moved into `self.cancelled` so the executor's hot-path `done`
	///   walk fires `Reason::Canceled` at the next yield point.
	/// * The `CancellationToken` is retained on `self.cancel_token` so bare-await sites (e.g.
	///   `SLEEP`) can `select!` against it via [`Self::cancel_token`].
	///
	/// Used by the RPC layer so that a WebSocket disconnect cancels
	/// in-flight queries cleanly, including ones blocked inside an
	/// external timer.
	pub(crate) fn set_cancellation(&mut self, handle: &CancelHandle) {
		self.cancelled = handle.flag();
		self.cancel_token = Some(handle.token());
	}

	/// Awaitable cancellation token installed by [`Self::set_cancellation`].
	/// Walks up the parent chain so any child context can observe a
	/// connection-level cancel without needing its own copy.
	///
	/// Returns `None` for contexts without an external cancel source
	/// (embedded callers, internal use). Callers SHOULD treat `None` as
	/// "no awaitable cancel" rather than failing — the `AtomicBool` view
	/// is the primary cancel mechanism; this is the awaitable companion
	/// for `select!`-based races.
	pub(crate) fn cancel_token(&self) -> Option<tokio_util::sync::CancellationToken> {
		if let Some(token) = &self.cancel_token {
			return Some(token.clone());
		}
		self.parent.as_ref().and_then(|p| p.cancel_token())
	}

	/// Add a deadline to the context. If the current deadline is sooner than
	/// the provided deadline, this method does nothing.
	pub(crate) fn add_deadline(&mut self, deadline: Instant, duration: Duration) {
		match self.deadline {
			Some((current, _)) if current < deadline => (),
			_ => self.deadline = Some((deadline, duration)),
		}
	}

	/// Add a timeout to the context. If the current timeout is sooner than
	/// the provided timeout, this method does nothing. If the result of the
	/// addition causes an overflow, this method returns an error.
	pub(crate) fn add_timeout(&mut self, timeout: Duration) -> Result<(), DatastoreError> {
		match Instant::now().checked_add(timeout) {
			Some(deadline) => {
				self.add_deadline(deadline, timeout);
				Ok(())
			}
			None => Err(DatastoreError::InvalidTimeout(timeout.as_secs())),
		}
	}

	pub(crate) fn set_transaction(&mut self, txn: Arc<Transaction>) {
		self.transaction = Some(txn);
	}

	/// Install the per-statement counter set on this context. Called by the
	/// executor before each top-level statement so the iterator can record
	/// the actual number of records affected -- including for DML
	/// statements with `RETURN NONE`, where the post-RETURN value would
	/// otherwise be empty.
	pub(crate) fn set_statement_counters(&mut self, counters: Option<Arc<StatementCounters>>) {
		self.statement_counters = counters;
	}

	/// The per-statement counter set, if one is currently installed. Read
	/// from the iterator's record-result path.
	pub(crate) fn statement_counters(&self) -> Option<&Arc<StatementCounters>> {
		self.statement_counters.as_ref()
	}

	/// Install the instant every CLOCK read answers while the statement about
	/// to run executes. Called by the executor once per top-level statement
	/// and by a block for each of its statements when it starts, so a `sleep`
	/// inside the statement does not advance it. Contexts derived afterwards
	/// carry it.
	pub(crate) fn set_statement_clock(&mut self, clock: Datetime) {
		self.statement_clock = Some(clock);
	}

	/// The instant of the statement running around this context, or `None`
	/// where no statement installed one.
	pub(crate) fn statement_clock(&self) -> Option<&Datetime> {
		self.statement_clock.as_ref()
	}

	pub(crate) fn tx(&self) -> Arc<Transaction> {
		self.transaction
			.clone()
			.unwrap_or_else(|| unreachable!("The context was not associated with a transaction"))
	}

	/// Returns the transaction if one is associated with this context.
	pub(crate) fn try_tx(&self) -> Option<&Arc<Transaction>> {
		self.transaction.as_ref()
	}

	/// Get the timeout for this operation, if any. This is useful for
	/// checking if a long job should be started or not.
	pub(crate) fn timeout(&self) -> Option<Duration> {
		self.deadline.map(|(v, _)| v.saturating_duration_since(Instant::now()))
	}

	/// The timeout this context's deadline was set from, if it has one: what a
	/// caller reports once [`Self::timeout`] has run out.
	pub(crate) fn deadline_timeout(&self) -> Option<Duration> {
		self.deadline.map(|(_, timeout)| timeout)
	}

	/// Returns the slow log configuration, if any, attached to this context.
	/// The executor consults this to decide whether to emit slow-query log lines.
	pub(crate) fn slow_log(&self) -> Option<&SlowLog> {
		self.slow_log.as_ref()
	}

	/// Get the index_store for this context/ds
	pub(crate) fn get_index_stores(&self) -> &IndexStores {
		&self.shared.index_stores
	}

	/// Get the index_builder for this context/ds
	pub(crate) fn get_index_builder(&self) -> Option<&IndexBuilder> {
		self.shared.index_builder.as_deref()
	}

	/// Return the sequences manager
	pub(crate) fn get_sequences(&self) -> Option<&Sequences> {
		self.shared.sequences.as_deref()
	}

	pub(crate) fn try_get_sequences(&self) -> Result<&Sequences> {
		if let Some(sqs) = self.get_sequences() {
			Ok(sqs)
		} else {
			bail!(
				EngineError::Internal("Sequences are not supported in this context.".to_string(),)
			)
		}
	}

	// Get the current datastore cache
	pub(crate) fn get_cache(&self) -> Option<Arc<DatastoreCache>> {
		self.shared.cache.clone()
	}

	/// Check if the context is done. If it returns `None` the operation may
	/// proceed, otherwise the operation should be stopped.
	///
	/// # Check Priority Order
	/// The checks are performed in the following order, with earlier checks taking priority:
	/// 1. **Cancellation** (always checked): Fast atomic flag check via `self.cancelled`
	/// 2. **Memory threshold** (only if `deep_check = true`): Expensive check via
	///    `ALLOC.is_beyond_threshold()`
	/// 3. **Deadline** (only if `deep_check = true`): Moderately expensive check via
	///    `Instant::now()`
	///
	/// # Parameters
	/// - `deep_check`: When `true`, performs all checks (cancellation, memory, deadline). When
	///   `false`, only checks the cancellation flag (fast atomic operation).
	///
	/// # Performance Note
	/// - Checking an `AtomicBool` (cancellation): single-digit nanoseconds
	/// - Checking `Instant::now()` (deadline): tens to hundreds of nanoseconds
	/// - Checking `ALLOC.is_beyond_threshold()` (memory): hundreds of nanoseconds (requires lock +
	///   traversal)
	///
	/// Use `deep_check = false` in hot loops to minimize overhead while still allowing
	/// cancellation.
	pub(crate) fn done(&self, deep_check: bool) -> Result<Option<Reason>> {
		// Check cancellation FIRST (fast atomic operation)
		if self.cancelled.load(Ordering::Relaxed) {
			return Ok(Some(Reason::Canceled));
		}
		if deep_check {
			#[cfg(test)]
			crate::kvs::testing::maybe_inject_non_retryable_error(
				crate::kvs::testing::NonRetryableErrorSite::ContextDeepCheckMemoryThreshold,
				self.shared.node_id,
			)?;
			if ALLOC.is_beyond_threshold() {
				crate::mem::record_refusal(crate::mem::RefusalSite::Query);
				bail!(DatastoreError::QueryBeyondMemoryThreshold);
			}
			// The clock is read only by a context that holds a deadline.
			if let Some((deadline, timeout)) = self.deadline
				&& deadline <= Instant::now()
			{
				return Ok(Some(Reason::Timedout(timeout.into())));
			}
		}
		if let Some(ctx) = &self.parent {
			return ctx.done(deep_check);
		}
		Ok(None)
	}

	/// Check if there is some reason to stop processing the current query.
	///
	/// Returns `true` when the query should be stopped (cancelled, timed out, or exceeded memory
	/// threshold).
	///
	/// # Parameters
	/// - `count`: Optional iteration count for optimization. Pass:
	///   - `Some(count)` when called in a loop - enables adaptive checking to balance
	///     responsiveness with performance. The method will:
	///     - Yield every 32 iterations to allow other tasks to run
	///     - Perform deep checks (memory/deadline) at iterations 1, 2, 4, 8, 16, 32, then every 64
	///   - `None` when called outside a loop (e.g., single operations) - always performs a deep
	///     check for immediate cancellation/timeout detection
	///
	/// # Performance
	/// The adaptive checking strategy ("jitter-based back-off") minimizes overhead in hot loops
	/// while maintaining reasonable responsiveness to cancellation and timeout events.
	pub(crate) async fn is_done(&self, count: Option<usize>) -> Result<bool> {
		let deep_check = if let Some(count) = count {
			// We yield every 32 iterations
			if count % 32 == 0 {
				yield_now!();
			}
			// Adaptive back-off strategy for deep checks based on iteration number:
			// Check frequently early (powers of 2), then settle into every 64 iterations
			match count {
				1 | 2 | 4 | 8 | 16 | 32 => true,
				_ => count % 64 == 0,
			}
		} else {
			// No count provided - perform a deep check immediately (single operation context)
			true
		};
		Ok(self.done(deep_check)?.is_some())
	}

	/// Check if the context is not ok to continue, because it timed out.
	pub(crate) async fn is_timedout(&self) -> Result<Option<Duration>> {
		yield_now!();
		if let Some(Reason::Timedout(d)) = self.done(true)? {
			Ok(Some(d.0))
		} else {
			Ok(None)
		}
	}

	pub(crate) async fn expect_not_timedout(&self) -> Result<()> {
		if let Some(d) = self.is_timedout().await? {
			bail!(EngineError::QueryTimedout(d))
		} else {
			Ok(())
		}
	}

	#[cfg(storage)]
	/// Return the location of the temporary directory if any
	pub(crate) fn temporary_directory(&self) -> Option<&Arc<PathBuf>> {
		self.shared.temporary_directory.as_ref()
	}

	/// Get a value from the context. If no value is stored under the
	/// provided key, then this will return None.
	///
	/// A name bound to a KNN query vector's row being scanned holds no value
	/// and answers `None` here: an expression's read of a parameter resolves
	/// it through [`Self::binding`], which refuses it.
	pub(crate) fn value(&self, key: &str) -> Option<&Value> {
		self.shared_value(key).map(Arc::as_ref)
	}

	/// Whether `key` reads here as a request parameter: the request bound it
	/// and no binding made since, a `LET` or a `FOR`, has replaced it.
	pub(crate) fn is_request_param(&self, key: &str) -> bool {
		match (
			self.request_params.as_ref().and_then(|params| params.get(key)),
			self.shared_value(key),
		) {
			(Some(param), Some(visible)) => Arc::ptr_eq(param, visible),
			_ => false,
		}
	}

	/// This context's own binding of `key`, with no walk to the parent
	/// contexts. [`Self::value`] cannot tell an own binding from an inherited
	/// one, which an exact restore of a binding needs.
	pub(crate) fn own_value(&self, key: &str) -> Option<&Arc<Value>> {
		self.values.get(key)
	}

	/// The address of this context's value map, for a test asserting that a
	/// write was made in place rather than to a copy.
	#[cfg(test)]
	pub(crate) fn values_addr(&self) -> usize {
		Arc::as_ptr(&self.values) as *const () as usize
	}

	/// Puts the own binding of `key` back to `prev`: inserts the value, or
	/// removes the binding when `prev` is `None`. A removed binding leaves the
	/// name unbound here, so a parent's binding or a `DEFINE PARAM` of the same
	/// name is answered again instead of a shadowing `NONE`.
	pub(crate) fn restore_value(&mut self, key: &str, prev: Option<Arc<Value>>) {
		match prev {
			Some(value) => {
				Arc::make_mut(&mut self.values).insert(Strand::from(key), value);
			}
			None => {
				Arc::make_mut(&mut self.values).remove(key);
			}
		}
	}

	/// The allocation [`Self::value`] answers for `key`.
	pub(crate) fn shared_value(&self, key: &str) -> Option<&Arc<Value>> {
		match self.binding(key) {
			Some(Binding::Value(value)) => Some(value),
			Some(Binding::Forbidden(_)) | None => None,
		}
	}

	/// What `key` is bound to in this context's chain: the innermost binding,
	/// a value or a forbidden row, or `None` where no level binds it. An
	/// isolated context reads only the protected names (`$auth`, `$access`,
	/// `$token`, `$session`) from its parents.
	pub(crate) fn binding(&self, key: &str) -> Option<Binding<'_>> {
		if let Some(value) = self.values.get(key) {
			return Some(Binding::Value(value));
		}
		if let Some((_, row)) = self.forbidden.iter().find(|(name, _)| name.as_str() == key) {
			return Some(Binding::Forbidden(row));
		}
		match &self.parent {
			Some(p) if PROTECTED_PARAM_NAMES.contains(&key) || !self.isolated => p.binding(key),
			_ => None,
		}
	}

	/// The names [`Self::binding`] answers [`Binding::Forbidden`] for, with
	/// the row each is bound to, for a snapshot that flattens the chain.
	fn collect_forbidden(&self) -> Vec<(Strand, Arc<ForbiddenRow>)> {
		let mut names = Vec::new();
		let mut level = Some(self);
		while let Some(ctx) = level {
			for (name, _) in &ctx.forbidden {
				if !names.contains(name) {
					names.push(name.clone());
				}
			}
			level = match &ctx.parent {
				Some(p) if !ctx.isolated => Some(p.as_ref()),
				_ => None,
			};
		}
		names
			.into_iter()
			.filter_map(|name| match self.binding(name.as_str()) {
				Some(Binding::Forbidden(row)) => Some((name, Arc::clone(row))),
				Some(Binding::Value(_)) | None => None,
			})
			.collect()
	}

	/// Collect context values into the provided map: every value
	/// [`Self::value`] answers, so the parent chain's values, or of an isolated
	/// context's chain only the protected ones (`$auth`, `$access`, `$token`,
	/// `$session`), under this context's own.
	pub(crate) fn collect_values(
		&self,
		map: HashMap<Strand, Arc<Value>>,
	) -> HashMap<Strand, Arc<Value>> {
		let mut map = match &self.parent {
			Some(p) if !self.isolated => {
				let mut map = p.collect_values(map);
				// A name this level binds to a forbidden row shadows the
				// parents' value of it.
				for (name, _) in &self.forbidden {
					map.remove(name);
				}
				map
			}
			Some(p) => {
				let mut map = map;
				for name in PROTECTED_PARAM_NAMES {
					if let Some(value) = p.shared_value(name) {
						map.insert(Strand::from(*name), Arc::clone(value));
					}
				}
				map
			}
			None => map,
		};
		self.values.iter().for_each(|(k, v)| {
			map.insert(k.clone(), Arc::clone(v));
		});
		map
	}

	/// Marks this node as the scope of a function body entered by a call. The
	/// node holds the arguments, and [`Self::lexical_value`] answers from it
	/// and from what is bound below it, then from the request parameters,
	/// never from what the caller bound above it.
	pub(crate) fn mark_call_boundary(&mut self) {
		self.call_boundary = true;
	}

	/// Whether this context is, or is derived from, the scope of a function
	/// body ([`Self::mark_call_boundary`]).
	pub(crate) fn in_call_body(&self) -> bool {
		std::iter::successors(Some(self), |ctx| ctx.parent.as_deref()).any(|ctx| ctx.call_boundary)
	}

	/// The value `key` holds in a frame running in this context, as the code
	/// of a function body reads it.
	///
	/// With no call boundary on the chain it is [`Self::value`]. Otherwise
	/// the chain is walked as `value` walks it up to and including the
	/// nearest boundary ([`Self::mark_call_boundary`]), so the arguments of
	/// the call and every binding made inside the body are read, and past it
	/// the answer is:
	///
	/// - for the protected names (`$auth`, `$access`, `$token`, `$session`) and the row names
	///   (`$this`, `$self`, `$parent`, `$before`, `$after`, `$event`, `$value`, `$input`), the
	///   nearest value of the whole chain;
	/// - for every other name, the request parameter of that name, if any.
	///
	/// A caller's `LET` or `FOR` binding is therefore never read by the body,
	/// at any depth of calls, and a name no binding of the body or of the
	/// request holds is left to a `DEFINE PARAM` of the database.
	///
	/// A name bound to a KNN query vector's row being scanned holds no value
	/// and answers `None` here; [`Self::lexical_binding`] tells it apart.
	pub(crate) fn lexical_value(&self, key: &str) -> Option<&Arc<Value>> {
		match self.lexical_binding(key) {
			Some(Binding::Value(value)) => Some(value),
			Some(Binding::Forbidden(_)) | None => None,
		}
	}

	/// What `key` is bound to as the code of a function body reads it
	/// ([`Self::lexical_value`]): a value, a KNN query vector's row being
	/// scanned ([`Binding::Forbidden`]), or nothing.
	pub(crate) fn lexical_binding(&self, key: &str) -> Option<Binding<'_>> {
		let mut node = self;
		loop {
			if let Some(value) = node.values.get(key) {
				return Some(Binding::Value(value));
			}
			if let Some((_, row)) = node.forbidden.iter().find(|(name, _)| name.as_str() == key) {
				return Some(Binding::Forbidden(row));
			}
			if node.call_boundary {
				return node.beyond_boundary(key);
			}
			match &node.parent {
				Some(parent) if PROTECTED_PARAM_NAMES.contains(&key) || !node.isolated => {
					node = parent;
				}
				_ => return None,
			}
		}
	}

	/// What a read of `key` answers past this call boundary, once the node's own
	/// bindings do not hold it; see [`Self::lexical_value`].
	fn beyond_boundary(&self, key: &str) -> Option<Binding<'_>> {
		if PROTECTED_PARAM_NAMES.contains(&key) || ROW_PARAM_NAMES.contains(&key) {
			self.parent.as_ref()?.binding(key)
		} else {
			self.request_params.as_ref()?.get(key).map(Binding::Value)
		}
	}

	/// The names a read in this context may find a value under: those
	/// [`Self::value`] answers and the request parameters.
	fn visible_names(&self) -> Vec<Strand> {
		let mut names: Vec<Strand> = self.collect_values(HashMap::default()).into_keys().collect();
		if let Some(params) = &self.request_params {
			names.extend(params.keys().cloned());
		}
		names
	}

	/// Every binding [`Self::lexical_value`] answers in this context.
	fn lexical_values(&self) -> HashMap<Strand, Arc<Value>> {
		self.visible_names()
			.into_iter()
			.filter_map(|name| {
				let value = Arc::clone(self.lexical_value(&name)?);
				Some((name, value))
			})
			.collect()
	}

	/// The context a function body entered from `parent` is planned in: an
	/// isolated, call-boundary child of it that holds the request parameters
	/// and the row names of `parent` and reads the protected names through it.
	/// A binding the caller made with `LET` or `FOR` is not among them, so a
	/// plan made in it folds none, and a name no binding holds is looked up
	/// among the database's parameters.
	///
	/// The request parameters are shared with `parent`, never copied: a request
	/// variable is never a protected or a row name ([`bound_name_refused`]), so
	/// the scope holds them as they are, and the row names `parent` binds, when
	/// it binds any, are held by a child node of their own above them. Entering
	/// a body costs the row names, not the number of variables the request holds.
	pub(crate) fn new_call_scope(parent: &FrozenContext) -> Self {
		let mut scope = Self::new_isolated(parent);
		scope.request_params.clone_from(&parent.request_params);
		if let Some(params) = &parent.request_params {
			scope.values = Arc::clone(params);
		}
		// What the body reads under a row name is `parent`'s binding of it.
		let mut rows = Vec::new();
		let mut forbidden = Vec::new();
		for name in ROW_PARAM_NAMES {
			match parent.binding(name) {
				Some(Binding::Value(value)) => rows.push((Strand::from(*name), Arc::clone(value))),
				Some(Binding::Forbidden(row)) => {
					forbidden.push((Strand::from(*name), Arc::clone(row)))
				}
				None => {}
			}
		}
		if rows.is_empty() && forbidden.is_empty() {
			scope.call_boundary = true;
			return scope;
		}
		let mut held = Self::new_child(&scope.freeze());
		held.call_boundary = true;
		held.values = Arc::new(rows.into_iter().collect());
		held.forbidden = forbidden;
		held
	}

	/// Get a 'static view into the cancellation status.
	#[cfg(feature = "scripting")]
	pub(crate) fn cancellation(&self) -> crate::ctx::cancellation::Cancellation {
		crate::ctx::cancellation::Cancellation::new(
			self.deadline.map(|(deadline, _)| deadline),
			std::iter::successors(Some(self), |ctx| ctx.parent.as_ref().map(|c| c.as_ref()))
				.map(|ctx| Arc::clone(&ctx.cancelled))
				.collect(),
		)
	}

	/// Attach a session to the context and add any session variables to the
	/// context, as request parameters ([`Self::attach_variables`]).
	pub(crate) fn attach_session(&mut self, session: &Session) -> Result<(), Error> {
		self.live = session.live();
		let values = session.values();
		// Room for the session's values and variables at once, rather than a
		// table grown and rehashed as they are added one by one.
		Arc::make_mut(&mut self.values).reserve(values.len() + session.variables.len());
		self.add_values(values);
		// Propagate duration redaction flag from session.
		if session.redact_volatile_explain_attrs {
			self.redact_volatile_explain_attrs = true;
		}
		if !session.variables.is_empty() {
			self.attach_variables(session.variables.clone().into())?;
		}
		// Pre-resolve the tenant identity so emit sites do not need to
		// re-walk the session value tree on every event dispatch.
		self.tenant_identity = Some(Arc::new(crate::observe::TenantIdentity::from(session)));
		Ok(())
	}

	/// Pre-resolved tenant identity for the active session.
	pub(crate) fn tenant_identity(&self) -> Option<&Arc<crate::observe::TenantIdentity>> {
		self.tenant_identity.as_ref()
	}

	/// Attach variables to the context. They are the request's parameters: a
	/// function body reads them past its call boundary
	/// ([`Self::lexical_value`]), where it reads no `LET` or `FOR` binding.
	///
	/// A protected name is refused. A name the system binds in a frame
	/// (`$this`, `$value`, ...) is not: the frame binds it on a context of its
	/// own and [`Self::value`] prefers the nearer binding, so the frame's
	/// value is what its code reads, and outside such a frame the request's
	/// own binding is read. Refusing it here would refuse the ordinary
	/// parameters clients bind.
	pub(crate) fn attach_variables(&mut self, vars: Variables) -> Result<(), Error> {
		self.bind_variables(vars, true)
	}

	/// Binds the captures of a closure as values of this context, refusing a
	/// protected name as [`Self::attach_variables`] does. A capture is the
	/// closure's own binding, not a request parameter, so a function the
	/// closure's body calls does not read it. A capture may be named as a
	/// value the system binds (`$this`, `$value`, ...), which is the frame's
	/// binding that the closure captured.
	pub(crate) fn attach_captures(&mut self, vars: Variables) -> Result<(), Error> {
		self.bind_variables(vars, false)
	}

	fn bind_variables(&mut self, vars: Variables, request: bool) -> Result<(), Error> {
		Arc::make_mut(&mut self.values).reserve(vars.0.len());
		if request {
			Arc::make_mut(self.request_params.get_or_insert_with(Default::default))
				.reserve(vars.0.len());
		}
		for (name, val) in vars {
			if PROTECTED_PARAM_NAMES.contains(&name.as_str()) {
				return Err(ExecError::InvalidParam {
					name: name.into_string(),
				}
				.into());
			}
			match request {
				true => self.add_request_param(name, Arc::new(val)),
				false => self.add_value(name, Arc::new(val)),
			}
		}
		Ok(())
	}

	/// Carries the request parameters of `from` onto this context, which a
	/// frame built isolated from `from` does to keep the request's
	/// parameters readable to the function bodies it runs.
	pub(crate) fn inherit_request_params(&mut self, from: &Context) {
		self.request_params.clone_from(&from.request_params);
	}

	/// Adds a request parameter: a value of the context that a function body
	/// reads past its call boundary ([`Self::lexical_value`]).
	fn add_request_param(&mut self, name: Strand, value: Arc<Value>) {
		Arc::make_mut(self.request_params.get_or_insert_with(Default::default))
			.insert(name.clone(), Arc::clone(&value));
		Arc::make_mut(&mut self.values).insert(name, value);
	}

	/// Attach variables of the public type to the context, as
	/// [`Self::attach_variables`] attaches them.
	pub(crate) fn attach_public_variables(&mut self, vars: PublicVariables) -> Result<(), Error> {
		for (name, val) in vars {
			if PROTECTED_PARAM_NAMES.contains(&name.as_str()) {
				return Err(ExecError::InvalidParam {
					name,
				}
				.into());
			}
			self.add_request_param(name.into(), Arc::new(convert_public_value_to_internal(val)));
		}
		Ok(())
	}

	//
	// Capabilities
	//

	/// Get the capabilities for this context
	pub(crate) fn get_capabilities(&self) -> Arc<Capabilities> {
		Arc::clone(&self.capabilities)
	}

	/// Whether EXPLAIN ANALYZE should redact elapsed durations.
	pub(crate) fn redact_volatile_explain_attrs(&self) -> bool {
		self.redact_volatile_explain_attrs
	}

	/// Check if scripting is allowed
	#[cfg_attr(not(feature = "scripting"), expect(dead_code))]
	pub(crate) fn check_allowed_scripting(&self) -> Result<()> {
		if !self.capabilities.allows_scripting() {
			warn!("Capabilities denied scripting attempt");
			bail!(CapabilitiesError::ScriptingNotAllowed);
		}
		trace!("Capabilities allowed scripting");
		Ok(())
	}

	/// Checks if the provided URL's network target is allowed based on current
	/// capabilities.
	///
	/// This function performs a validation to ensure that the outgoing network
	/// connection specified by the provided `url` is permitted. It checks the
	/// resolved network targets associated with the URL and ensures that all
	/// targets adhere to the configured capabilities.
	///
	/// # Features
	/// The function is only available if the `http` feature is enabled.
	///
	/// # Parameters
	/// - `url`: A reference to a [`Url`] object representing the target endpoint to check.
	///
	/// # Returns
	/// This function returns a [`Result<()>`]:
	/// - On success, it returns `Ok(())` indicating the network target is allowed.
	/// - On failure, it returns an error wrapped in the [`Error`] type:
	///   - `NetTargetNotAllowed` if the target is not permitted.
	///   - `InvalidUrl` if the provided URL is invalid.
	///
	/// # Behavior
	/// 1. Extracts the host and port information from the URL.
	/// 2. Constructs a [`NetTarget`] object and checks if it is allowed by the current network
	///    capabilities.
	/// 3. If the network target resolves to multiple targets (e.g., DNS resolution), each target is
	///    validated individually.
	/// 4. Logs a warning and prevents the connection if the target is denied by the capabilities.
	///
	/// # Logging
	/// - Logs a warning message if the network target is denied.
	/// - Logs a trace message if the network target is permitted.
	///
	/// # Errors
	/// - `NetTargetNotAllowed`: Returned if any of the resolved targets are not allowed.
	/// - `InvalidUrl`: Returned if the URL does not have a valid host.
	#[cfg(feature = "http")]
	pub(crate) async fn check_allowed_net(&self, url: &Url) -> Result<()> {
		let match_any_deny_net = |t| {
			if self.capabilities.matches_any_deny_net(t) {
				warn!("Capabilities denied outgoing network connection attempt, target: '{t}'");
				bail!(CapabilitiesError::NetTargetNotAllowed(t.to_string()));
			}
			Ok(())
		};
		match url.host() {
			Some(host) => {
				let target =
					NetTarget::Host(NetTarget::canonical_host(host), url.port_or_known_default());
				// Check the domain name (if any) matches the allow list
				let host_allowed = self.capabilities.matches_any_allow_net(&target);
				if !host_allowed {
					warn!(
						"Capabilities denied outgoing network connection attempt, target: '{target}'"
					);
					bail!(CapabilitiesError::NetTargetNotAllowed(target.to_string()));
				}
				// Check against the deny list
				match_any_deny_net(&target)?;
				// Resolve the domain name to a vector of IP addresses
				#[cfg(not(target_family = "wasm"))]
				let targets = crate::net::resolve_net_target(&target).await?;
				#[cfg(target_family = "wasm")]
				let targets = crate::net::resolve_net_target(&target)?;
				for t in &targets {
					match_any_deny_net(t)?;
				}
				trace!("Capabilities allowed outgoing network connection, target: '{target}'");
				Ok(())
			}
			_ => bail!(Error::InvalidUrl(url.to_string())),
		}
	}

	pub(crate) fn get_buckets(&self) -> Option<&BucketsManager> {
		self.shared.buckets.as_ref()
	}

	/// Obtain the connection for a bucket
	pub(crate) async fn get_bucket_store(
		&self,
		level: crate::catalog::BucketLevel,
		bu: &str,
	) -> Result<Arc<dyn ObjectStore>> {
		// Do we have a buckets context?
		if let Some(buckets) = &self.shared.buckets {
			buckets.get_bucket_store(&self.tx(), level, bu).await
		} else {
			bail!(BucError::BucketUnavailable(bu.into()))
		}
	}

	#[cfg(feature = "surrealism")]
	pub(crate) fn get_surrealism_cache(&self) -> Option<Arc<SurrealismCache>> {
		self.shared.surrealism_cache.as_ref().map(Arc::clone)
	}

	/// How many extension invocations enclose this context.
	#[cfg(feature = "surrealism")]
	pub(crate) fn extension_depth(&self) -> u8 {
		self.extension_depth
	}

	/// Mark this context as running inside one more extension invocation.
	#[cfg(feature = "surrealism")]
	pub(crate) fn enter_extension(&mut self) {
		self.extension_depth = self.extension_depth.saturating_add(1);
	}

	/// The datastore's capabilities, which no extension's host call narrows.
	#[cfg(feature = "surrealism")]
	pub(crate) fn server_capabilities(&self) -> &Arc<Capabilities> {
		&self.shared.server_capabilities
	}

	/// The definition behind `lookup`, prepared, once its archive may run
	/// under `trust`, the definition being called.
	///
	/// The check is [`TrustStore::ensure`](crate::extension::trust::TrustStore::ensure):
	/// before an archive is prepared, and again on every call, since one
	/// prepared entry serves every definition whose archive has the same
	/// digest, in any namespace or database, and those definitions can name
	/// different signers.
	///
	/// A failure to load the definition is recorded per database, source,
	/// `DIGEST` and signer, and repeated without refetching for
	/// `surrealism_refetch_after_refusal`
	/// ([`REFETCH_AFTER_REFUSAL`](crate::extension::trust::REFETCH_AFTER_REFUSAL)),
	/// so a failing definition does not fetch, unpack and verify its archive on
	/// every call.
	#[cfg(feature = "surrealism")]
	pub(crate) async fn get_surrealism_module(
		&self,
		lookup: SurrealismCacheLookup<'_>,
		trust: crate::extension::trust::Trust<'_>,
	) -> Result<SurrealismCachedModule> {
		self.get_surrealism_module_from(lookup, trust, None).await
	}

	/// As [`Self::get_surrealism_module`], preparing `prefetched`, the
	/// archive's bytes, when they were just fetched and checked, rather than
	/// fetching them again.
	#[cfg(feature = "surrealism")]
	pub(crate) async fn get_surrealism_module_from(
		&self,
		lookup: SurrealismCacheLookup<'_>,
		trust: crate::extension::trust::Trust<'_>,
		prefetched: Option<bytes::Bytes>,
	) -> Result<SurrealismCachedModule> {
		if !self.get_capabilities().allows_experimental(&ExperimentalTarget::Surrealism) {
			bail!(
				"Failed to get surrealism runtime: Experimental capability `surrealism` is not enabled"
			);
		}
		let pinned = trust.pinned()?;

		let Some(cache) = self.get_surrealism_cache() else {
			bail!("Surrealism cache is not available");
		};

		let source = match lookup {
			SurrealismCacheLookup::File(_, _, bucket, key) => ArchiveSource::File {
				bucket,
				key,
			},
			SurrealismCacheLookup::Silo(_, _, organisation, package, major, minor, patch) => {
				ArchiveSource::Silo {
					organisation,
					package,
					major,
					minor,
					patch,
				}
			}
		};
		let (ns, db) = match lookup {
			SurrealismCacheLookup::File(ns, db, ..) | SurrealismCacheLookup::Silo(ns, db, ..) => {
				(*ns, *db)
			}
		};

		let refusal = crate::extension::trust::RefusalKey::new(ns, db, source, pinned, &trust);
		cache
			.trust()
			.check_refusal(&refusal, self.config.surrealism.surrealism_refetch_after_refusal)?;

		// Nothing is prepared until the archive is the pinned one and, with a
		// SIGNER, a signature proves it. Only MANIFEST and SIGNATURE are read
		// for that. The archive was fetched from the definition's source just
		// now, so its SIGNATURE is the current one and nothing is fetched
		// again. Decompressing is CPU work, so it runs off the async task.
		let prefetched = parking_lot::Mutex::new(prefetched);
		let fetch = async || {
			let prefetched = prefetched.lock().take();
			let archive = match prefetched {
				Some(archive) => archive,
				None => fetch_archive(self, ns, db, source).await?,
			};
			let max_fs_bytes = self.config.surrealism.surrealism_max_fs_bytes;
			let read = archive.clone();
			let files = tokio::task::spawn_blocking(move || {
				surrealism_runtime::package::trust_files_from_reader(read.as_ref(), max_fs_bytes)
			})
			.await
			.map_err(|e| anyhow::anyhow!("archive read task aborted: {e}"))??;
			let digest = files.manifest.digest().0;
			cache
				.trust()
				.ensure(self, ns, db, None, &digest, files.signature.as_deref(), trust)
				.await?;
			Ok(crate::surrealism::runner::VerifiedSource {
				digest: crate::expr::ManifestDigest::sha256(digest),
				archive,
				#[cfg(storage)]
				temp_base: self.temporary_directory().map(|p| p.as_path().to_path_buf()),
				#[cfg(not(storage))]
				temp_base: None,
				max_fs_bytes,
				signature: files.signature.map(Arc::from),
			})
		};
		let built =
			cache.get_or_load(&lookup, &crate::expr::ManifestDigest::sha256(pinned), fetch).await;
		if built.is_err() {
			cache.trust().record(refusal, &built);
		}
		let module = built?;
		let digest = crate::extension::trust::digest_bytes(&module.digest)?;
		// The module may have been prepared for another definition of the same
		// source, so its source's signature is checked against this one's
		// SIGNER. When the source has been re-signed since, the current
		// signature proves it and the source entry is dropped, so the next load
		// takes that signature with it.
		let resigned = cache
			.trust()
			.ensure(self, ns, db, Some(source), &digest, module.signature.as_deref(), trust)
			.await?;
		if resigned {
			cache.remove(&lookup, pinned);
		}
		Ok(module)
	}

	#[cfg(feature = "http")]
	pub(crate) fn http_client(&self) -> Arc<HttpClient> {
		Arc::clone(&self.http_client)
	}
}

impl crate::catalog::providers::CancellationProbe for Context {
	fn expect_not_timedout(&self) -> crate::catalog::providers::BoxProviderFut<'_, Result<()>> {
		Box::pin(Context::expect_not_timedout(self))
	}
}

/// The context is the index engines' environment.
///
/// A `&FrozenContext` — what every caller into the engines already holds —
/// coerces straight to `&dyn IndexEnv`: both `IndexEnv` and its supertrait
/// `CancellationProbe` declare a blanket `Arc` forward beside the trait itself.
impl crate::idx::env::IndexEnv for Context {
	fn tx(&self) -> Arc<Transaction> {
		Context::tx(self)
	}

	fn is_done(&self, count: Option<usize>) -> crate::idx::env::BoxEnvFut<'_> {
		Box::pin(Context::is_done(self, count))
	}

	fn index_stores(&self) -> &IndexStores {
		Context::get_index_stores(self)
	}

	fn node_id(&self) -> Uuid {
		Context::node_id(self)
	}

	fn sequences(&self) -> Result<&Sequences> {
		Context::try_get_sequences(self)
	}

	fn config(&self) -> &crate::idx::config::IdxConfig {
		&self.config.idx
	}

	/// The ticket set on this context, or else this context's own
	/// registration with the node's pool under the datastore's morsel policy,
	/// made on the first call and handed out by every later one.
	fn places(&self) -> crate::idx::places::Ticket {
		self.index_places
			.get_or_init(|| IndexPlaces::node(Policy::from_config(&self.config.exec)))
			.clone()
	}
}

/// A registration with the morsel place pool, as the index engines take places
/// through it ([`crate::idx::places::PlaceSource`]).
///
/// Places are granted, refused and owed exactly as they are to a statement's
/// morsel workers, under the same [`Policy`]: its OLTP reserve, its place cap
/// and its quantum. Three kinds of registration are wrapped:
///
/// * **foreground**: a blocking statement's own registration ([`Self::statement`]), so the workers
///   of its index work count as the statement's places;
/// * **background**: the compaction ticket the index compaction loop registers for its run
///   ([`Self::node`]);
/// * **build**: a ticket a build registers for itself ([`Self::node`]), or the registration of the
///   blocking statement waiting on it.
///
/// On targets without worker threads the registration never holds a place, so
/// the index engines' work runs on the calling task.
#[derive(Debug)]
pub(crate) struct IndexPlaces {
	statement: places::Statement,
	policy: Policy,
}

impl IndexPlaces {
	/// A ticket over an existing registration: the places its workers take are
	/// counted against `statement`.
	pub(crate) fn statement(
		statement: places::Statement,
		policy: Policy,
	) -> crate::idx::places::Ticket {
		crate::idx::places::Ticket::new(Self {
			statement,
			policy,
		})
	}

	/// A ticket over a new registration with the node's pool.
	pub(crate) fn node(policy: Policy) -> crate::idx::places::Ticket {
		Self::statement(places::register(), policy)
	}
}

impl crate::idx::places::PlaceSource for IndexPlaces {
	fn acquire(&self, keep_waiting: bool) -> Option<crate::idx::places::Place> {
		self.statement
			.acquire_keeping_waiting(self.policy, keep_waiting)
			.map(crate::idx::places::Place::new)
	}

	fn others_wait(&self) -> bool {
		self.statement.others_wait()
	}

	fn stop_waiting(&self) {
		self.statement.stop_waiting()
	}

	fn quantum(&self) -> u32 {
		self.policy.place_quantum()
	}
}

#[cfg(test)]
mod tests {
	#[cfg(feature = "http")]
	use std::str::FromStr;
	use std::sync::Arc;
	use std::time::Duration;

	#[cfg(all(feature = "allocation-tracking", feature = "allocator"))]
	use surrealdb_cnf::MEMORY_THRESHOLD;
	#[cfg(feature = "http")]
	use url::Url;

	use super::Binding;
	use crate::ctx::Context;
	use crate::ctx::reason::Reason;
	#[cfg(feature = "http")]
	use crate::dbs::Capabilities;
	use crate::dbs::Options;
	#[cfg(feature = "http")]
	use crate::dbs::capabilities::{NetTarget, Targets};
	use crate::exec::context::ForbiddenRow;
	use crate::expr::Base;
	use crate::iam::{Action, Auth, ResourceKind, Role};

	#[test]
	fn is_allowed_respects_context_auth_toggle_and_base() {
		let config = crate::exec::config::ExecConfig::default();

		// Auth disabled: anonymous allowed without IAM; still needs valid NS/DB for bases.
		{
			let mut ctx = Context::new_test();
			ctx.auth_enabled = false;

			let empty = Options::new(&config);
			ctx.is_allowed(&empty, Action::View, ResourceKind::Any, Base::Ns).unwrap_err();
			ctx.is_allowed(&empty, Action::View, ResourceKind::Any, Base::Db).unwrap_err();
			let db_only = Options::new(&config).with_db(Some("db".into()));
			ctx.is_allowed(&db_only, Action::View, ResourceKind::Any, Base::Db).unwrap_err();

			ctx.is_allowed(&empty, Action::View, ResourceKind::Any, Base::Root).unwrap();
			let ns = Options::new(&config).with_ns(Some("ns".into()));
			ctx.is_allowed(&ns, Action::View, ResourceKind::Any, Base::Ns).unwrap();
			let ns_db = Options::new(&config).with_ns(Some("ns".into())).with_db(Some("db".into()));
			ctx.is_allowed(&ns_db, Action::View, ResourceKind::Any, Base::Db).unwrap();
		}

		// Auth enabled: root owner still needs NS/DB set for NS/Db bases.
		{
			let mut ctx = Context::new_test();
			ctx.auth_enabled = true;

			let opts = Options::new(&config).with_auth(Auth::for_root(Role::Owner).into());
			ctx.is_allowed(&opts, Action::View, ResourceKind::Any, Base::Ns).unwrap_err();
			ctx.is_allowed(&opts, Action::View, ResourceKind::Any, Base::Db).unwrap_err();
			let db_only = opts.clone().with_db(Some("db".into()));
			ctx.is_allowed(&db_only, Action::View, ResourceKind::Any, Base::Db).unwrap_err();

			ctx.is_allowed(&opts, Action::View, ResourceKind::Any, Base::Root).unwrap();
			let ns = opts.with_ns(Some("ns".into()));
			ctx.is_allowed(&ns, Action::View, ResourceKind::Any, Base::Ns).unwrap();
			let ns_db = ns.with_db(Some("db".into()));
			ctx.is_allowed(&ns_db, Action::View, ResourceKind::Any, Base::Db).unwrap();
		}
	}

	#[cfg(feature = "http")]
	#[tokio::test]
	async fn test_context_check_allowed_net() {
		let cap = Capabilities::all().without_network_targets(Targets::Some(
			[NetTarget::from_str("127.0.0.1").unwrap()].into(),
		));
		let mut ctx = Context::new_test();
		ctx.capabilities = cap.into();
		let ctx = ctx.freeze();
		let r = ctx.check_allowed_net(&Url::parse("http://localhost").unwrap()).await;
		assert_eq!(
			r.err().unwrap().to_string(),
			"Access to network target '127.0.0.1/32' is not allowed"
		);
	}

	/// A wildcard rule is decided on the request host before any DNS lookup.
	#[cfg(feature = "http")]
	#[tokio::test]
	async fn test_context_check_allowed_net_wildcard() {
		let check = |cap: Capabilities, url: &'static str| async move {
			let mut ctx = Context::new_test();
			ctx.capabilities = cap.into();
			let ctx = ctx.freeze();
			ctx.check_allowed_net(&Url::parse(url).unwrap()).await
		};
		let not_allowed =
			|target: &str| format!("Access to network target '{target}' is not allowed");

		// A wildcard deny under a global allow refuses a subdomain
		let deny = Capabilities::all().without_network_targets(Targets::Some(
			[NetTarget::from_str("*.example.com").unwrap()].into(),
		));
		let r = check(deny, "http://api.example.com/").await;
		assert_eq!(r.unwrap_err().to_string(), not_allowed("api.example.com:80"));

		// A wildcard allow does not cover its apex or a lookalike domain
		let allow = Capabilities::default().with_network_targets(Targets::Some(
			[NetTarget::from_str("*.example.com").unwrap()].into(),
		));
		let r = check(allow.clone(), "https://example.com/").await;
		assert_eq!(r.unwrap_err().to_string(), not_allowed("example.com:443"));
		let r = check(allow, "https://evilexample.com/").await;
		assert_eq!(r.unwrap_err().to_string(), not_allowed("evilexample.com:443"));

		// A wildcard allow admits a subdomain past the capability check. The
		// `.invalid` name (RFC 6761) never resolves, so whatever is reported
		// next comes from DNS, not from the capabilities.
		let allow = Capabilities::default().with_network_targets(Targets::Some(
			[NetTarget::from_str("*.test.invalid").unwrap()].into(),
		));
		if let Err(e) = check(allow, "https://api.test.invalid/").await {
			assert!(
				!e.to_string().contains("Access to network target"),
				"the wildcard allow should have passed the capability check: {e}"
			);
		}
	}

	#[tokio::test]
	async fn test_context_cancellation_priority() {
		// Test that cancellation is detected even when a deadline is set and exceeded
		let mut ctx = Context::new_test();

		// Set a deadline in the past (already exceeded)
		ctx.add_timeout(Duration::from_nanos(1)).unwrap();
		// Give time for the deadline to pass
		tokio::time::sleep(Duration::from_millis(10)).await;

		// Cancel the context
		let canceller = ctx.add_cancel();
		canceller.cancel();

		let ctx = ctx.freeze();

		// Cancellation should be detected first, not timeout
		let result = ctx.done(true);
		assert!(result.is_ok());
		assert_eq!(result.unwrap(), Some(Reason::Canceled));
	}

	#[tokio::test]
	async fn test_context_deadline_detection() {
		// Test that deadline timeout is detected when context is not cancelled
		let mut ctx = Context::new_test();

		// Set a very short timeout
		ctx.add_timeout(Duration::from_nanos(1)).unwrap();
		// Give time for the deadline to pass
		tokio::time::sleep(Duration::from_millis(10)).await;

		let ctx = ctx.freeze();

		// Should detect timeout
		let result = ctx.done(true);
		assert!(result.is_ok());
		assert!(matches!(result.unwrap(), Some(Reason::Timedout(_))));
	}

	/// The index engines stop a loop through `expect_not_done`, which must name
	/// the reason this context gives, not just that it is done.
	#[tokio::test]
	async fn index_checkpoint_names_a_deadline_as_a_timeout() {
		let mut ctx = Context::new_test();
		ctx.add_timeout(Duration::from_nanos(1)).unwrap();
		tokio::time::sleep(Duration::from_millis(10)).await;
		let ctx = ctx.freeze();
		let err = crate::idx::env::expect_not_done(&ctx, None).await.unwrap_err();
		assert!(crate::err::is_query_timedout(&err), "expected a timeout, got {err:#}");
	}

	#[tokio::test]
	async fn index_checkpoint_names_a_cancellation_as_a_cancellation() {
		let mut ctx = Context::new_test();
		ctx.add_cancel().cancel();
		let ctx = ctx.freeze();
		let err = crate::idx::env::expect_not_done(&ctx, None).await.unwrap_err();
		assert!(crate::err::is_query_cancelled(&err), "expected a cancellation, got {err:#}");
	}

	/// A child context carries a statement's `TIMEOUT` while its parent carries
	/// none, so the reason comes from whichever context set the deadline.
	#[tokio::test]
	async fn index_checkpoint_names_a_statement_deadline_as_a_timeout() {
		let parent = Context::new_test().freeze();
		let mut ctx = Context::new_child(&parent);
		ctx.add_timeout(Duration::from_nanos(1)).unwrap();
		tokio::time::sleep(Duration::from_millis(10)).await;
		let ctx = ctx.freeze();
		let err = crate::idx::env::expect_not_done(&ctx, Some(64)).await.unwrap_err();
		assert!(crate::err::is_query_timedout(&err), "expected a timeout, got {err:#}");
		assert!(parent.done(true).unwrap().is_none(), "the parent is not done");
	}

	#[tokio::test]
	async fn index_checkpoint_passes_a_live_context() {
		let ctx = Context::new_test().freeze();
		crate::idx::env::expect_not_done(&ctx, None).await.unwrap();
	}

	#[tokio::test]
	async fn test_context_no_deadline() {
		// Test that a context without deadline or cancellation returns None
		let ctx = Context::new_test();
		let ctx = ctx.freeze();

		// Should return None (ok to continue)
		let result = ctx.done(true);
		assert!(result.is_ok());
		assert_eq!(result.unwrap(), None);
	}

	#[tokio::test]
	async fn test_context_is_done_adaptive_backoff() {
		// Test the adaptive back-off strategy in is_done()
		let ctx = Context::new_test();
		let ctx = ctx.freeze();

		// Test that early iterations trigger deep checks (1, 2, 4, 8, 16, 32)
		for count in [1, 2, 4, 8, 16, 32] {
			let result = ctx.is_done(Some(count)).await;
			assert!(result.is_ok());
			assert_eq!(result.unwrap(), false, "Count {} should not be done", count);
		}

		// Test that later iterations only check every 64
		// Count 33-63 should not trigger deep checks (except at 64)
		for count in 33..64 {
			let result = ctx.is_done(Some(count)).await;
			assert!(result.is_ok());
			assert_eq!(result.unwrap(), false, "Count {} should not be done", count);
		}

		// Count 64 should trigger a deep check (64 % 64 == 0)
		let result = ctx.is_done(Some(64)).await;
		assert!(result.is_ok());
		assert_eq!(result.unwrap(), false);
	}

	#[tokio::test]
	async fn test_context_is_done_with_none() {
		// Test that is_done(None) always performs a deep check
		let ctx = Context::new_test();
		let ctx = ctx.freeze();

		// Should perform deep check and return Ok(false) since no cancellation/timeout
		let result = ctx.is_done(None).await;
		assert!(result.is_ok());
		assert_eq!(result.unwrap(), false);
	}

	#[tokio::test]
	async fn test_context_is_done_detects_cancellation() {
		// Test that is_done detects cancellation
		let mut ctx = Context::new_test();
		let canceller = ctx.add_cancel();
		canceller.cancel();
		let ctx = ctx.freeze();

		// Should detect cancellation
		let result = ctx.is_done(None).await;
		assert!(result.is_ok());
		assert_eq!(result.unwrap(), true);

		// Should also detect with count
		let result = ctx.is_done(Some(1)).await;
		assert!(result.is_ok());
		assert_eq!(result.unwrap(), true);
	}

	/// `Context::snapshot` is called by the engine at
	/// `dbs::Executor::execute_operator_plan` and produces a ctx with
	/// `parent: None`. Operators that poll `Context::done` / `is_done`
	/// (HNSW/DiskANN KNN search inner loops, etc.) read the snapshot's own
	/// `cancelled` flag — with no parent to fall back on. If `snapshot`
	/// allocated a fresh `Arc<AtomicBool>` instead of sharing the source's,
	/// those checks would never observe a connection-level cancel installed
	/// via `set_cancellation`, and a WS disconnect during e.g. a KNN
	/// search would not abort the operation — the read loop's drain
	/// would block until the search finished.
	#[tokio::test]
	async fn test_snapshot_preserves_external_cancellation() {
		use crate::ctx::CancelHandle;

		let handle = CancelHandle::new();
		let mut ctx = Context::new_test();
		ctx.set_cancellation(&handle);
		let root = ctx.freeze();
		let snap = Context::snapshot(&root).freeze();
		assert_eq!(
			snap.is_done(Some(1)).await.unwrap(),
			false,
			"snapshot reports done before cancel was tripped"
		);

		handle.trip();
		assert_eq!(
			snap.is_done(Some(1)).await.unwrap(),
			true,
			"snapshot did not observe external cancel after trip — `is_done` on an \
			 execution snapshot would silently miss WebSocket disconnect"
		);
		assert!(
			snap.cancel_token().is_some(),
			"snapshot lost the awaitable cancel token — bare-await sites reached via \
			 the snapshot (`SLEEP`, etc.) could not `select!` against cancel"
		);
	}

	/// Test documenting the expected behavior when memory threshold is exceeded.
	///
	/// Note: This test documents the expected behavior but cannot easily test actual
	/// memory threshold violations without:
	/// 1. Setting MEMORY_THRESHOLD configuration (via cnf::MEMORY_THRESHOLD)
	/// 2. Actually allocating enough memory to exceed it
	/// 3. Having the "allocation-tracking" feature enabled
	///
	/// The key behavior tested elsewhere is that when is_beyond_threshold() returns true,
	/// it takes priority over deadline timeout, which prevents OOM crashes from being
	/// masked by timeout errors.
	#[tokio::test]
	async fn test_context_memory_threshold_priority_documentation() {
		// This test documents that the priority order in done() is:
		// 1. Cancellation (always checked, fast atomic operation)
		// 2. Memory threshold (checked when deep_check=true, if beyond threshold returns Error)
		// 3. Deadline (checked when deep_check=true, returns Reason::Timedout)

		// When ALLOC.is_beyond_threshold() returns true, done() will bail with
		// DatastoreError::QueryBeyondMemoryThreshold before checking the deadline.
		// This ensures memory violations are always detected before timeout errors.

		let ctx = Context::new_test();
		let ctx = ctx.freeze();

		// With no memory pressure, deadline not set, and no cancellation:
		let result = ctx.done(true);
		assert!(result.is_ok());
		assert_eq!(result.unwrap(), None);
	}

	/// A crossed memory threshold makes `done` bail with
	/// [`DatastoreError::QueryBeyondMemoryThreshold`].
	///
	/// `MEMORY_THRESHOLD` is a process-wide `LazyLock` read once on first use,
	/// and this binary's other tests assert the no-pressure path, so the
	/// threshold has to come from the environment rather than be installed
	/// here. Ignored by default so that it reports as skipped rather than
	/// passing without asserting anything:
	///
	/// ```text
	/// SURREAL_MEMORY_THRESHOLD=1mb cargo test -p surrealdb-core \
	///   --features allocator,allocation-tracking \
	///   test_context_memory_threshold_enforced
	/// ```
	#[tokio::test]
	#[cfg(all(feature = "allocation-tracking", feature = "allocator"))]
	#[ignore = "requires SURREAL_MEMORY_THRESHOLD in the process environment"]
	async fn test_context_memory_threshold_enforced() {
		use crate::kvs::DatastoreError;

		// The most this test will allocate to cross the threshold. Reaching a
		// production-shaped threshold would mean allocating that much for real,
		// so the environment is not allowed to ask for more than this: crossing
		// the guard at a small threshold proves the same code path.
		const BALLAST_LIMIT: usize = 256 * 1024 * 1024;
		const BALLAST_CHUNK: usize = 1024 * 1024;

		let threshold = *MEMORY_THRESHOLD;
		assert_ne!(
			threshold, 0,
			"SURREAL_MEMORY_THRESHOLD must be set in this process for the guard to be enforceable"
		);
		assert!(
			threshold <= BALLAST_LIMIT,
			"SURREAL_MEMORY_THRESHOLD is {threshold} bytes, above the {BALLAST_LIMIT}-byte ceiling \
			 this test will allocate to cross it; re-run with a smaller value"
		);

		// Cross the threshold in bounded steps. A single threshold-sized
		// request would route an allocation failure to `handle_alloc_error`,
		// aborting the binary with no failure message.
		let mut ballast: Vec<Vec<u8>> = Vec::new();
		while crate::mem::ALLOC.memory_allocated() <= threshold {
			ballast.push(vec![0u8; BALLAST_CHUNK]);
			assert!(
				ballast.len().saturating_mul(BALLAST_CHUNK) <= BALLAST_LIMIT,
				"allocated {BALLAST_LIMIT} bytes without the {threshold}-byte guard tripping"
			);
		}
		let ctx = Context::new_test().freeze();

		let err = ctx.done(true).expect_err("expected the memory threshold to be enforced");
		assert!(
			matches!(
				err.downcast_ref::<DatastoreError>(),
				Some(DatastoreError::QueryBeyondMemoryThreshold)
			),
			"expected QueryBeyondMemoryThreshold, got: {err:?}"
		);
	}

	/// A context derived from one that carries a statement clock reads the same
	/// instant, whichever way it is derived, and installs none of its own.
	#[test]
	fn a_derived_context_inherits_the_statement_clock() {
		use crate::val::Datetime;

		let mut root = Context::new_test();
		assert!(root.statement_clock().is_none());
		let now = Datetime::now();
		root.set_statement_clock(now);
		let root = root.freeze();
		assert_eq!(Context::new_child(&root).statement_clock(), Some(&now));
		assert_eq!(Context::new_isolated(&root).statement_clock(), Some(&now));
		assert_eq!(Context::snapshot(&root).statement_clock(), Some(&now));
	}

	/// A binding put back as absent leaves the name unbound in this context, so
	/// the binding of a parent shows through again instead of a shadowing `NONE`.
	#[test]
	fn restoring_an_absent_binding_removes_it() {
		use std::sync::Arc;

		use crate::val::Value;

		let mut parent = Context::new_test();
		parent.add_value("x", Arc::new(Value::from(1)));
		let parent = parent.freeze();
		let mut child = Context::new_child(&parent);
		assert!(child.own_value("x").is_none(), "an inherited binding is not the child's own");
		let before = child.own_value("x").cloned();
		child.add_value("x", Arc::new(Value::from(2)));
		assert_eq!(child.value("x"), Some(&Value::from(2)));
		child.restore_value("x", before);
		assert!(child.own_value("x").is_none());
		assert_eq!(child.value("x"), Some(&Value::from(1)), "the parent's binding answers again");
		let held = Arc::new(Value::from(3));
		child.restore_value("x", Some(Arc::clone(&held)));
		assert_eq!(child.own_value("x"), Some(&held));
	}
	fn int(n: i64) -> std::sync::Arc<crate::val::Value> {
		std::sync::Arc::new(crate::val::Value::from(n))
	}

	/// A request with the bind parameter `$q = 1`, a caller's `LET $q = 5` and
	/// `LET $l = 7`, and the body of a call with the argument `$a` over a row.
	fn a_call_body() -> std::sync::Arc<Context> {
		use crate::dbs::Variables;
		use crate::val::Value;

		let mut request = Context::new_test();
		let mut vars = Variables::new();
		vars.insert("q", Value::from(1));
		request.attach_variables(vars).expect("variables attach");
		request.add_value("q", int(5));
		request.add_value("l", int(7));
		request.add_value("session", int(9));
		let request = request.freeze();
		let mut caller = Context::new_child(&request);
		caller.add_value("f", int(8));
		let caller = caller.freeze();
		let mut body = Context::new_child(&caller);
		body.add_value("a", int(2));
		body.add_value("this", int(3));
		body.mark_call_boundary();
		let mut inside = Context::new_child(&body.freeze());
		inside.add_value("b", int(4));
		inside.freeze()
	}

	/// A body reads its arguments, what it binds, the request parameters, the
	/// protected names and the row names, and never a binding of its caller.
	#[test]
	fn a_body_reads_its_arguments_row_names_and_request_parameters_only() {
		let body = a_call_body();
		assert_eq!(body.lexical_value("a"), Some(&int(2)));
		assert_eq!(body.lexical_value("b"), Some(&int(4)));
		assert_eq!(body.lexical_value("this"), Some(&int(3)));
		assert_eq!(body.lexical_value("session"), Some(&int(9)));
		assert_eq!(body.lexical_value("q"), Some(&int(1)), "the request's q, not the caller's LET");
		assert_eq!(body.lexical_value("l"), None);
		assert_eq!(body.lexical_value("f"), None);
		// `value` is the plain chain walk.
		assert_eq!(body.value("l"), Some(&crate::val::Value::from(7)));
	}

	/// With no boundary on the chain a read is the chain walk.
	#[test]
	fn without_a_boundary_a_lexical_read_is_a_chain_read() {
		let mut root = Context::new_test();
		root.add_value("l", int(7));
		let root = root.freeze();
		let child = Context::new_child(&root).freeze();
		assert!(!child.in_call_body());
		assert_eq!(child.lexical_value("l"), Some(&int(7)));
		assert_eq!(child.lexical_value("none"), None);
	}

	/// An isolated context holds none of its parent's request parameters, so a
	/// body entered from a privileged frame reads none, and the ones it attaches
	/// are its own.
	#[test]
	fn an_isolated_context_holds_no_request_parameters_of_its_parent() {
		use crate::dbs::Variables;
		use crate::val::Value;

		let mut request = Context::new_test();
		let mut vars = Variables::new();
		vars.insert("q", Value::from(1));
		request.attach_variables(vars).expect("variables attach");
		let request = request.freeze();
		let mut frame = Context::new_isolated(&request);
		frame.mark_call_boundary();
		assert_eq!(frame.lexical_value("q"), None);
		let mut own = Variables::new();
		own.insert("p", Value::from(2));
		frame.attach_variables(own).expect("variables attach");
		assert_eq!(frame.lexical_value("p"), Some(&int(2)));
		assert_eq!(frame.lexical_value("q"), None);
	}

	/// A request's variables, and a closure's captures, refuse a protected name alone. A name
	/// the system binds in a frame is bound per frame and shadowed there, so a client may bind
	/// one as a parameter of its request: refusing it would refuse `$value`, `$input` and the
	/// rest as ordinary parameter names, which every SDK lets a caller bind.
	#[test]
	fn variables_refuse_a_protected_name_and_no_other() {
		use crate::dbs::Variables;
		use crate::val::Value;

		for name in surrealdb_cnf::PROTECTED_PARAM_NAMES {
			let mut vars = Variables::new();
			vars.insert(*name, Value::from(1));
			assert!(
				Context::new_test().attach_variables(vars.clone()).is_err(),
				"`{name}` bound as a request variable"
			);
			assert!(
				Context::new_test().attach_captures(vars).is_err(),
				"`{name}` bound as a capture"
			);
		}
		for name in surrealdb_cnf::SYSTEM_BOUND_PARAM_NAMES.iter().chain(["limit"].iter()) {
			let mut vars = Variables::new();
			vars.insert(*name, Value::from(1));
			Context::new_test()
				.attach_variables(vars.clone())
				.unwrap_or_else(|_| panic!("`{name}` attaches as a request variable"));
			Context::new_test()
				.attach_captures(vars)
				.unwrap_or_else(|_| panic!("`{name}` attaches as a capture"));
		}
	}

	/// A frame's own binding of a name the system binds is what its code reads, whatever the
	/// request bound under that name: the nearer binding wins.
	#[test]
	fn a_frame_binding_shadows_a_request_variable_of_the_same_name() {
		use std::sync::Arc;

		use crate::dbs::Variables;
		use crate::val::Value;

		let mut vars = Variables::new();
		vars.insert("value", Value::from(1));
		let mut request = Context::new_test();
		request.attach_variables(vars).expect("`value` attaches as a request variable");
		let request = request.freeze();
		assert_eq!(request.value("value"), Some(&Value::from(1)));

		let mut frame = Context::new_child(&request);
		frame.add_value("value", Arc::new(Value::from(2)));
		assert_eq!(frame.value("value"), Some(&Value::from(2)));
	}

	/// The scope a body is planned in holds the request parameters and the
	/// row names of the context it is entered from, and no binding of it.
	#[test]
	fn a_call_scope_holds_request_parameters_and_row_names_only() {
		let body = a_call_body();
		let scope = Context::new_call_scope(&body).freeze();
		assert_eq!(scope.value("q"), Some(&crate::val::Value::from(1)));
		assert_eq!(scope.value("this"), Some(&crate::val::Value::from(3)));
		assert_eq!(scope.value("session"), Some(&crate::val::Value::from(9)));
		for name in ["a", "b", "l", "f"] {
			assert_eq!(scope.value(name), None, "{name} reached a body's plan");
			assert_eq!(scope.lexical_value(name), None);
		}
	}

	/// A request of `n` bound variables, as a frozen context.
	fn a_request_of(n: i64) -> std::sync::Arc<Context> {
		use crate::dbs::Variables;
		use crate::val::Value;

		let mut request = Context::new_test();
		let mut vars = Variables::new();
		for i in 0..n {
			vars.insert(format!("v{i}"), Value::from(i));
		}
		request.attach_variables(vars).expect("variables attach");
		request.add_value("session", int(9));
		request.freeze()
	}

	/// A call scope entered from a frame that binds no row name is one node
	/// that shares the request's parameters: entering a body copies none of
	/// them, however many the request holds.
	#[test]
	fn a_call_scope_shares_the_request_parameters() {
		let request = a_request_of(1000);
		let mut caller = Context::new_child(&request);
		caller.add_value("l", int(7));
		let caller = caller.freeze();
		let scope = Context::new_call_scope(&caller);
		let params = request.request_params.as_ref().expect("the request holds parameters");
		assert!(Arc::ptr_eq(&scope.values, params), "the request's parameters were copied");
		assert_eq!(scope.lexical_value("v999"), Some(&int(999)));
		assert_eq!(scope.lexical_value("session"), Some(&int(9)));
		assert_eq!(scope.lexical_value("l"), None);
		assert_eq!(scope.value("l"), None);
	}

	/// A call scope entered from a frame that binds row names holds them in a
	/// node of their own, above the shared request parameters, and reads as
	/// the scope entered from a frame that binds none, with the row names.
	#[test]
	fn a_call_scope_holds_row_names_above_the_shared_parameters() {
		let request = a_request_of(1000);
		let mut row = Context::new_child(&request);
		row.add_value("this", int(3));
		row.add_value("l", int(7));
		let row = row.freeze();
		let scope = Context::new_call_scope(&row);
		let params = request.request_params.as_ref().expect("the request holds parameters");
		let below = scope.parent.as_ref().expect("the row names sit above the parameters");
		assert!(Arc::ptr_eq(&below.values, params), "the request's parameters were copied");
		assert_eq!(scope.values.len(), 1, "the row node holds only the row names");
		assert!(scope.in_call_body());
		assert_eq!(scope.lexical_value("this"), Some(&int(3)));
		assert_eq!(scope.value("this"), Some(&*int(3)));
		assert_eq!(scope.lexical_value("v0"), Some(&int(0)));
		assert_eq!(scope.lexical_value("session"), Some(&int(9)));
		assert_eq!(scope.lexical_value("parent"), None);
		assert_eq!(scope.lexical_value("l"), None);
		assert_eq!(scope.value("l"), None);
		let mut names: Vec<_> = scope.visible_names().into_iter().map(|n| n.to_string()).collect();
		names.sort();
		names.dedup();
		assert_eq!(names.len(), 1002, "the parameters, `session` and `this`: {names:?}");
	}

	/// A row name the frame binds to a KNN query vector's row being scanned
	/// is bound so in the scope: a read of it is refused, not answered by a
	/// value or by nothing.
	#[test]
	fn a_call_scope_keeps_a_forbidden_row_name_forbidden() {
		let request = a_request_of(3);
		let mut row = Context::new_child(&request);
		row.add_value("this", int(3));
		row.add_forbidden(&["parent"], &Arc::new(ForbiddenRow::for_test("$parent.v")));
		let row = row.freeze();
		let scope = Context::new_call_scope(&row);
		assert!(matches!(scope.lexical_binding("parent"), Some(Binding::Forbidden(_))));
		assert!(matches!(scope.binding("parent"), Some(Binding::Forbidden(_))));
		assert_eq!(scope.value("parent"), None);
		assert_eq!(scope.lexical_value("this"), Some(&int(3)));
		assert_eq!(scope.lexical_value("v2"), Some(&int(2)));
	}

	/// A snapshot of a context with no parent shares its values, and each
	/// write to either side after it is the writer's own: the other side reads
	/// what it read before.
	#[test]
	fn a_snapshot_shares_values_and_writes_after_it_stay_apart() {
		let mut root = Context::new_test();
		root.add_value("a", int(1));
		root.add_value("b", int(2));
		let root = root.freeze();
		let mut snapshot = Context::snapshot(&root);
		assert!(Arc::ptr_eq(&snapshot.values, &root.values), "the values were copied");

		snapshot.add_value("a", int(10));
		snapshot.restore_value("b", None);
		snapshot.add_forbidden(&["c"], &Arc::new(ForbiddenRow::for_test("$c")));
		assert_eq!(snapshot.value("a"), Some(&*int(10)));
		assert_eq!(snapshot.value("b"), None);
		assert_eq!(root.value("a"), Some(&*int(1)), "a write to the snapshot reached the root");
		assert_eq!(root.value("b"), Some(&*int(2)), "a removal from the snapshot reached the root");
		assert!(root.binding("c").is_none());

		let snapshot = snapshot.freeze();
		let mut again = Context::snapshot(&snapshot);
		again.add_value("a", int(20));
		assert_eq!(snapshot.value("a"), Some(&*int(10)));
		assert_eq!(again.value("a"), Some(&*int(20)));
	}

	/// A snapshot of a context inside a body is the body's view of it.
	#[test]
	fn a_snapshot_inside_a_body_keeps_the_lexical_view() {
		let body = a_call_body();
		let snapshot = Context::snapshot(&body);
		assert_eq!(snapshot.lexical_value("a"), Some(&int(2)));
		assert_eq!(snapshot.lexical_value("q"), Some(&int(1)));
		assert_eq!(snapshot.lexical_value("l"), None);
		assert_eq!(snapshot.value("l"), None);
	}

	/// The index engines' places, taken from a pool of their own so the books
	/// hold nothing but the test's registrations.
	#[cfg(not(target_family = "wasm"))]
	mod index_places {
		use crate::ctx::{Context, IndexPlaces};
		use crate::exec::config::ExecConfig;
		use crate::exec::planner::placement::Policy;
		use crate::exec::vm::morsel::places::Pool;
		use crate::idx::env::IndexEnv;

		/// A morsel policy with a place cap of `cap` and an OLTP reserve of
		/// `reserve`.
		fn policy(cap: usize, reserve: usize) -> Policy {
			Policy::from_config(&ExecConfig {
				morsel_places: cap,
				morsel_oltp_reserve: reserve,
				..ExecConfig::default()
			})
		}

		/// An up-front grant on a pool with `F` usable places takes exactly
		/// `min(max, F)`, at once. The books show the grant on the caller's
		/// registration while it is held and nothing once it is dropped, and
		/// the pool's reserve is never handed out.
		#[test]
		fn an_up_front_grant_takes_min_of_max_and_the_usable_places() {
			let pool = Pool::new(6);
			let statement = pool.register();
			let places = IndexPlaces::statement(statement.clone(), policy(usize::MAX, 2));
			let grant = places.acquire_places(3);
			assert_eq!(grant.len(), 3);
			assert_eq!(statement.held(), 3);
			drop(grant);
			assert_eq!(statement.held(), 0);
			let grant = places.acquire_places(10);
			assert_eq!(grant.len(), 4, "six places less a reserve of two");
			assert_eq!(statement.held(), 4);
			assert_eq!(pool.free(), 2, "the reserve stays free");
			assert!(!statement.waiting(), "a short grant leaves no waiting mark");
			drop(grant);
			assert_eq!(statement.held(), 0);
			assert_eq!(pool.free(), 6);
			let capped = IndexPlaces::statement(pool.register(), policy(1, 0));
			assert_eq!(capped.acquire_places(4).len(), 1, "the cap bounds a grant");
			let none = IndexPlaces::statement(pool.register(), policy(0, 0));
			assert!(none.acquire_places(4).is_empty(), "a grant of none is a grant");
		}

		/// Two places free, and a registration holding none asks for them
		/// through `take`, with or without an earlier registration waiting.
		/// Returns what it got and its registration.
		fn trimmed(
			waiter: bool,
			take: impl Fn(&crate::idx::places::Ticket) -> usize,
		) -> (usize, crate::exec::vm::morsel::places::Statement) {
			let pool = Pool::new(4);
			let waiting = pool.register();
			let asker = pool.register();
			let filler = pool.register();
			let unreserved = policy(usize::MAX, 0);
			let mut held: Vec<_> =
				(0..4).map(|_| filler.acquire(unreserved).expect("free")).collect();
			if waiter {
				assert!(waiting.acquire(unreserved).is_none());
				assert!(waiting.waiting());
			}
			held.truncate(2);
			let got = take(&IndexPlaces::statement(asker.clone(), unreserved));
			drop(held);
			(got, asker)
		}

		/// With an earlier registration waiting, a grant is trimmed by the
		/// pool's fairness exactly as a run of per-job asks is, and the grant
		/// leaves no waiting mark that would hold a freed place back from the
		/// waiting registration.
		#[test]
		fn a_grant_is_trimmed_by_fairness_as_a_per_job_ask_is() {
			let grant = |places: &crate::idx::places::Ticket| places.acquire_places(4).len();
			let per_job = |places: &crate::idx::places::Ticket| {
				let held: Vec<_> = std::iter::from_fn(|| places.acquire()).collect();
				places.stop_waiting();
				held.len()
			};
			assert_eq!(trimmed(false, grant).0, 2, "with nothing waiting, both free places");
			let (granted, asker) = trimmed(true, grant);
			assert_eq!(granted, 1, "one free place is owed to the waiting registration");
			assert!(!asker.waiting(), "the grant left no waiting mark behind");
			assert_eq!(trimmed(true, per_job).0, granted, "the same trim as per-job asks");
		}

		/// A grant to one user of a ticket while another still waits leaves the
		/// registration marked waiting under the grant's own lock, so a later
		/// registration holding as many places is refused the next free place:
		/// it is owed to the ticket, which registered first.
		#[test]
		fn a_grant_keeps_a_ticket_with_a_waiting_user_owed() {
			let pool = Pool::new(3);
			let unreserved = policy(usize::MAX, 0);
			let filler = pool.register();
			let statement = pool.register();
			let later = pool.register();
			let places = IndexPlaces::statement(statement.clone(), unreserved);
			let _later_held = later.acquire(unreserved).expect("free");
			let mut filled: Vec<_> =
				(0..2).map(|_| filler.acquire(unreserved).expect("free")).collect();
			let (waiting, other) = (places.asking(), places.asking());
			assert!(waiting.acquire().is_none(), "refused while the pool is full");
			drop(filled.pop());
			let _granted = other.acquire().expect("the freed place");
			assert!(statement.waiting(), "the grant left the registration waiting");
			drop(filled.pop());
			assert!(later.acquire(unreserved).is_none(), "the free place is owed to the ticket");
			drop(waiting);
			assert!(!statement.waiting(), "once no user waits, neither does the registration");
		}

		/// A ticket set on a statement's context counts the places taken
		/// through it against the statement's own registration, and every
		/// context deriving the statement's work hands the same ticket on; a
		/// background or concurrent context does not.
		#[test]
		fn a_statement_ticket_counts_against_its_registration() {
			let pool = Pool::new(4);
			let statement = pool.register();
			let lent = IndexPlaces::statement(statement.clone(), policy(usize::MAX, 0));
			let mut ctx = Context::new_test();
			ctx.set_index_places(lent.clone());
			let ctx = ctx.freeze();
			for derived in [
				Context::new_child(&ctx).freeze(),
				Context::new_isolated(&ctx).freeze(),
				Context::snapshot(&ctx).freeze(),
			] {
				assert!(IndexEnv::places(&*derived).same(&lent));
			}
			assert!(Context::background(&ctx).index_places().is_none());
			assert!(Context::new_concurrent(&ctx).index_places().is_none());
			let child = Context::new_child(&ctx).freeze();
			let grant = IndexEnv::places(&*child).acquire_places(3);
			assert_eq!(grant.len(), 3);
			assert_eq!(statement.held(), 3, "the places are the statement's");
			assert_eq!(pool.registrations(), 1, "no other registration was made");
		}
	}

	/// A context with no ticket set hands out one registration of its own,
	/// however often it is asked, rather than a new one per ask. A child,
	/// isolated or snapshot context hands out the same one, even when derived
	/// before the first ask; a ticket set on a derived context replaces it for
	/// that context alone.
	#[test]
	fn a_context_without_a_ticket_hands_out_one_registration() {
		use crate::idx::env::IndexEnv;

		let ctx = Context::new_test().freeze();
		let derived = [
			Context::new_child(&ctx).freeze(),
			Context::new_isolated(&ctx).freeze(),
			Context::snapshot(&ctx).freeze(),
		];
		let places = IndexEnv::places(&*ctx);
		assert!(IndexEnv::places(&*ctx).same(&places));
		for derived in &derived {
			assert!(IndexEnv::places(&**derived).same(&places), "derived before the first ask");
		}
		let mut lent = Context::new_child(&ctx);
		lent.set_index_places(crate::idx::places::Ticket::serial());
		let lent = lent.freeze();
		assert!(!IndexEnv::places(&*lent).same(&places), "the lent ticket is the child's");
		assert!(IndexEnv::places(&*ctx).same(&places), "the parent keeps its own");
		assert!(IndexEnv::places(&*derived[0]).same(&places), "and so do its other children");
	}
}
