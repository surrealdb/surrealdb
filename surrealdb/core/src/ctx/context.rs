use std::collections::HashMap;
use std::fmt::{self, Debug};
#[cfg(storage)]
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

#[cfg(feature = "surrealism")]
use anyhow::Context as _;
use anyhow::{Result, bail};
use surrealdb_cnf::PROTECTED_PARAM_NAMES;
use surrealdb_cnf::dynamic::DynamicConfiguration;
use surrealdb_strand::Strand;
#[cfg(feature = "surrealism")]
use surrealism_runtime::package::{SurrealismPackage, UnpackOptions};
#[cfg(feature = "surrealism")]
use surrealism_runtime::runtime::Runtime;
#[cfg(feature = "http")]
use url::Url;
use uuid::Uuid;
use web_time::Instant;

use crate::buc::Error as BucError;
use crate::buc::manager::BucketsManager;
#[cfg(feature = "surrealism")]
use crate::buc::store::ObjectKey;
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
#[cfg(all(feature = "http", feature = "surrealism"))]
use crate::dbs::capabilities::Targets;
use crate::dbs::{Capabilities, MessageBroker, Options, Session, StatementCounters, Variables};
use crate::err::{EngineError, Error};
use crate::exec::Error as ExecError;
use crate::exec::context::ForbiddenRow;
use crate::exec::function::FunctionRegistry;
use crate::expr::Base;
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
use crate::val::Value;
use crate::val::convert_public::convert_public_value_to_internal;

pub type FrozenContext = Arc<Context>;

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
	// A collection of read only values stored in this context.
	values: HashMap<Strand, Arc<Value>>,
	// The parameter names this context binds to a KNN query vector's row being
	// scanned rather than to a value ([`Binding::Forbidden`]). A name is in
	// at most one of `values` and `forbidden` at one level.
	forbidden: Vec<(Strand, Arc<ForbiddenRow>)>,
	// An optional datastore cache
	cache: Option<Arc<DatastoreCache>>,
	// The index store
	index_stores: IndexStores,
	// The index concurrent builders. Held behind an `Arc` so that deriving a child context
	// is one reference-count bump rather than one per handle the builder carries.
	index_builder: Option<Arc<IndexBuilder>>,
	// The sequences, behind an `Arc` for the same reason.
	sequences: Option<Arc<Sequences>>,
	// Capabilities
	capabilities: Arc<Capabilities>,
	#[cfg(storage)]
	// The temporary directory
	temporary_directory: Option<Arc<PathBuf>>,
	// An optional transaction
	transaction: Option<Arc<Transaction>>,
	// Does not read from parent `values`.
	isolated: bool,
	// A map of bucket connections
	buckets: Option<BucketsManager>,
	// The surrealism cache
	#[cfg(feature = "surrealism")]
	surrealism_cache: Option<Arc<SurrealismCache>>,
	// When true, EXPLAIN ANALYZE omits elapsed durations for deterministic test output
	redact_volatile_explain_attrs: bool,
	// Per-statement counters, shared with the executor so it can read the
	// number of rows affected by the running DML statement when emitting
	// the corresponding `StatementEvent`. Replaced by the executor before
	// each top-level statement; `None` outside an active statement.
	statement_counters: Option<Arc<StatementCounters>>,
	// Pre-resolved tenant identity (namespace, database, user, session id,
	// client ip) derived from the active session at `attach_session` time.
	// Read by the executor and the transaction layer to populate the
	// `*Ctx` half of every emitted [`crate::observe`] event without
	// re-walking the session value tree on every emit.
	tenant_identity: Option<Arc<crate::observe::TenantIdentity>>,
	/// Client for making http requests.
	#[cfg(feature = "http")]
	http_client: Arc<HttpClient>,
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
	/// Stable node id for this datastore instance (live routing, queue keys, etc.).
	node_id: Uuid,
	/// Whether authentication is required for non-anonymous access (datastore boot flag).
	pub(crate) auth_enabled: bool,
	/// Runtime-adjustable configuration (e.g. global query timeout).
	dynamic_configuration: DynamicConfiguration,
	/// Whether this session may use realtime (`LIVE` / `KILL LIVE`).
	live: bool,
	/// Optional broker for cross-node live query notifications.
	broker: Option<Arc<dyn MessageBroker>>,
	/// How a running program gives up its thread once it has spent its
	/// quantum; the default scheduler when `None`. A child context carries
	/// its parent's.
	scheduler: Option<Arc<dyn crate::exec::scheduler::Scheduler>>,
	/// The query's memory: its lease from the node's governor and the store
	/// its breakers spill to. `None` outside a datastore query, where nothing
	/// is bounded. A child context carries its parent's.
	memory: Option<Arc<crate::exec::memory::QueryMemory>>,
	/// The per-layer configuration a running query reads. Crate-internal: these
	/// are operator knobs, not API.
	pub(crate) config: Arc<RuntimeConfig>,
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
	/// Creates a new empty background context.
	pub(crate) fn background(parent: &Context) -> Self {
		Self {
			values: HashMap::default(),
			forbidden: Vec::new(),
			parent: None,
			deadline: None,
			slow_log: None,
			cancelled: Arc::new(AtomicBool::new(false)),
			cancel_token: None,
			capabilities: Arc::clone(&parent.capabilities),
			// Derived rather than new: this context is built per unit of work
			// (one LIVE query delivery), and a segmentation dictionary is far
			// too expensive to load and drop that often, so those are shared
			// with the parent while the other caches stay isolated.
			index_stores: parent.index_stores.derive(
				parent.config.idx.hnsw_cache_size,
				parent.config.idx.diskann_cache_size,
				parent.config.idx.graph_resolve_cache_size,
			),
			cache: None,
			index_builder: None,
			sequences: None,
			#[cfg(storage)]
			temporary_directory: None,
			transaction: None,
			isolated: false,
			buckets: None,
			#[cfg(feature = "surrealism")]
			surrealism_cache: None,
			redact_volatile_explain_attrs: false,
			statement_counters: None,
			config: Arc::clone(&parent.config),
			#[cfg(feature = "http")]
			http_client: Arc::clone(&parent.http_client),
			function_registry: Arc::clone(&parent.function_registry),
			live_units: parent.live_units.clone(),
			compiled_units: parent.compiled_units.clone(),
			vm_units: parent.vm_units.clone(),
			vm_deref: parent.vm_deref.clone(),
			vm_served: parent.vm_served.clone(),
			vm_metrics: parent.vm_metrics.clone(),
			vm_edge_cascade: parent.vm_edge_cascade.clone(),
			vm_closures: parent.vm_closures.clone(),
			tenant_identity: None,
			node_id: parent.node_id,
			auth_enabled: parent.auth_enabled,
			dynamic_configuration: parent.dynamic_configuration.clone(),
			live: parent.live,
			broker: parent.broker.clone(),
			scheduler: parent.scheduler.clone(),
			memory: parent.memory.clone(),
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
			values: HashMap::default(),
			forbidden: Vec::new(),
			deadline: parent.deadline,
			slow_log: parent.slow_log.clone(),
			cancelled: Arc::new(AtomicBool::new(false)),
			cancel_token: None,
			capabilities: cap,
			index_stores: parent.index_stores.clone(),
			cache: parent.cache.clone(),
			index_builder: parent.index_builder.clone(),
			sequences: parent.sequences.clone(),
			#[cfg(storage)]
			temporary_directory: parent.temporary_directory.clone(),
			transaction: parent.transaction.clone(),
			isolated: false,
			parent: Some(Arc::clone(parent)),
			buckets: parent.buckets.clone(),
			#[cfg(feature = "surrealism")]
			surrealism_cache: parent.surrealism_cache.clone(),
			redact_volatile_explain_attrs: parent.redact_volatile_explain_attrs,
			statement_counters: parent.statement_counters.clone(),
			config: Arc::clone(&parent.config),
			#[cfg(feature = "http")]
			http_client,
			function_registry: Arc::clone(&parent.function_registry),
			live_units: parent.live_units.clone(),
			compiled_units: parent.compiled_units.clone(),
			vm_units: parent.vm_units.clone(),
			vm_deref: parent.vm_deref.clone(),
			vm_served: parent.vm_served.clone(),
			vm_metrics: parent.vm_metrics.clone(),
			vm_edge_cascade: parent.vm_edge_cascade.clone(),
			vm_closures: parent.vm_closures.clone(),
			tenant_identity: parent.tenant_identity.clone(),
			node_id: parent.node_id,
			auth_enabled: parent.auth_enabled,
			dynamic_configuration: parent.dynamic_configuration.clone(),
			live: parent.live,
			broker: parent.broker.clone(),
			scheduler: parent.scheduler.clone(),
			memory: parent.memory.clone(),
		}
	}

	/// Create a new context from a frozen parent context.
	/// This context is isolated, and values specified on
	/// any parent contexts will not be accessible.
	pub(crate) fn new_isolated(parent: &FrozenContext) -> Self {
		Self {
			values: HashMap::default(),
			forbidden: Vec::new(),
			deadline: parent.deadline,
			slow_log: parent.slow_log.clone(),
			cancelled: Arc::new(AtomicBool::new(false)),
			cancel_token: None,
			capabilities: Arc::clone(&parent.capabilities),
			index_stores: parent.index_stores.clone(),
			cache: parent.cache.clone(),
			index_builder: parent.index_builder.clone(),
			sequences: parent.sequences.clone(),
			#[cfg(storage)]
			temporary_directory: parent.temporary_directory.clone(),
			transaction: parent.transaction.clone(),
			isolated: true,
			parent: Some(Arc::clone(parent)),
			buckets: parent.buckets.clone(),
			#[cfg(feature = "surrealism")]
			surrealism_cache: parent.surrealism_cache.clone(),
			redact_volatile_explain_attrs: parent.redact_volatile_explain_attrs,
			statement_counters: parent.statement_counters.clone(),
			config: Arc::clone(&parent.config),
			#[cfg(feature = "http")]
			http_client: Arc::clone(&parent.http_client),
			function_registry: Arc::clone(&parent.function_registry),
			live_units: parent.live_units.clone(),
			compiled_units: parent.compiled_units.clone(),
			vm_units: parent.vm_units.clone(),
			vm_deref: parent.vm_deref.clone(),
			vm_served: parent.vm_served.clone(),
			vm_metrics: parent.vm_metrics.clone(),
			vm_edge_cascade: parent.vm_edge_cascade.clone(),
			vm_closures: parent.vm_closures.clone(),
			tenant_identity: parent.tenant_identity.clone(),
			node_id: parent.node_id,
			auth_enabled: parent.auth_enabled,
			dynamic_configuration: parent.dynamic_configuration.clone(),
			live: parent.live,
			broker: parent.broker.clone(),
			scheduler: parent.scheduler.clone(),
			memory: parent.memory.clone(),
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
	pub(crate) fn snapshot(from: &FrozenContext) -> Self {
		Self {
			// Flatten all values from the parent chain into this context
			values: from.collect_values(HashMap::default()),
			forbidden: from.collect_forbidden(),
			deadline: from.deadline,
			slow_log: from.slow_log.clone(),
			cancelled: Arc::clone(&from.cancelled),
			cancel_token: from.cancel_token.clone(),
			capabilities: Arc::clone(&from.capabilities),
			index_stores: from.index_stores.clone(),
			cache: from.cache.clone(),
			index_builder: from.index_builder.clone(),
			sequences: from.sequences.clone(),
			#[cfg(storage)]
			temporary_directory: from.temporary_directory.clone(),
			transaction: from.transaction.clone(),
			isolated: false,
			parent: None, // No parent reference — fully independent
			buckets: from.buckets.clone(),
			#[cfg(feature = "surrealism")]
			surrealism_cache: from.surrealism_cache.clone(),
			redact_volatile_explain_attrs: from.redact_volatile_explain_attrs,
			statement_counters: from.statement_counters.clone(),
			config: Arc::clone(&from.config),
			#[cfg(feature = "http")]
			http_client: Arc::clone(&from.http_client),
			function_registry: Arc::clone(&from.function_registry),
			live_units: from.live_units.clone(),
			compiled_units: from.compiled_units.clone(),
			vm_units: from.vm_units.clone(),
			vm_deref: from.vm_deref.clone(),
			vm_served: from.vm_served.clone(),
			vm_metrics: from.vm_metrics.clone(),
			vm_edge_cascade: from.vm_edge_cascade.clone(),
			vm_closures: from.vm_closures.clone(),
			tenant_identity: from.tenant_identity.clone(),
			node_id: from.node_id,
			auth_enabled: from.auth_enabled,
			dynamic_configuration: from.dynamic_configuration.clone(),
			live: from.live,
			broker: from.broker.clone(),
			scheduler: from.scheduler.clone(),
			memory: from.memory.clone(),
		}
	}

	/// Create a new context from a frozen parent context.
	/// This context is not linked to the parent context,
	/// and won't be cancelled if the parent is cancelled.
	///
	/// `index_builder` is intentionally cleared: the only caller is the
	/// background index `Building` task, which lives inside the
	/// `IndexBuilder`'s HashMap. Cloning the back-reference would form an
	/// `Arc<RwLock<HashMap<.., Arc<Building>>>>` cycle that pins the
	/// `Datastore` (and its storage handles, ~7 file descriptors per RocksDB
	/// instance, ~3 per SurrealKV) until process exit — see issue
	/// surrealdb/surrealdb#7304.
	pub(crate) fn new_concurrent(from: &FrozenContext) -> Self {
		Self {
			values: HashMap::default(),
			forbidden: Vec::new(),
			deadline: None,
			slow_log: from.slow_log.clone(),
			cancelled: Arc::new(AtomicBool::new(false)),
			cancel_token: None,
			capabilities: Arc::clone(&from.capabilities),
			index_stores: from.index_stores.clone(),
			cache: from.cache.clone(),
			index_builder: None,
			sequences: from.sequences.clone(),
			#[cfg(storage)]
			temporary_directory: from.temporary_directory.clone(),
			transaction: None,
			isolated: false,
			parent: None,
			buckets: from.buckets.clone(),
			#[cfg(feature = "surrealism")]
			surrealism_cache: from.surrealism_cache.clone(),
			redact_volatile_explain_attrs: from.redact_volatile_explain_attrs,
			statement_counters: from.statement_counters.clone(),
			config: Arc::clone(&from.config),
			#[cfg(feature = "http")]
			http_client: Arc::clone(&from.http_client),
			function_registry: Arc::clone(&from.function_registry),
			live_units: from.live_units.clone(),
			compiled_units: from.compiled_units.clone(),
			vm_units: from.vm_units.clone(),
			vm_deref: from.vm_deref.clone(),
			vm_served: from.vm_served.clone(),
			vm_metrics: from.vm_metrics.clone(),
			vm_edge_cascade: from.vm_edge_cascade.clone(),
			vm_closures: from.vm_closures.clone(),
			tenant_identity: from.tenant_identity.clone(),
			node_id: from.node_id,
			auth_enabled: from.auth_enabled,
			dynamic_configuration: from.dynamic_configuration.clone(),
			live: from.live,
			broker: from.broker.clone(),
			scheduler: from.scheduler.clone(),
			memory: from.memory.clone(),
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
		live_units: Arc<crate::exec::vm::live::LiveUnits>,
		compiled_units: Arc<crate::exec::vm::units::CompiledUnits>,
		#[cfg(storage)] temporary_directory: Option<Arc<PathBuf>>,
		buckets: BucketsManager,
		config: Arc<RuntimeConfig>,
		#[cfg(feature = "surrealism")] surrealism_cache: Arc<SurrealismCache>,
	) -> Result<Context> {
		let mut ctx = Self {
			values: HashMap::default(),
			forbidden: Vec::new(),
			parent: None,
			deadline: None,
			slow_log,
			cancelled: Arc::new(AtomicBool::new(false)),
			cancel_token: None,
			capabilities,
			index_stores,
			cache: Some(cache),
			index_builder: Some(Arc::new(index_builder)),
			sequences: Some(Arc::new(sequences)),
			#[cfg(storage)]
			temporary_directory,
			transaction: None,
			isolated: false,
			buckets: Some(buckets),
			#[cfg(feature = "surrealism")]
			surrealism_cache: Some(surrealism_cache),
			redact_volatile_explain_attrs: false,
			statement_counters: None,
			config,
			#[cfg(feature = "http")]
			http_client,
			function_registry,
			live_units: Some(live_units),
			compiled_units: Some(compiled_units),
			vm_units: None,
			vm_deref: None,
			vm_served: None,
			vm_metrics: None,
			vm_edge_cascade: None,
			vm_closures: None,
			tenant_identity: None,
			node_id,
			auth_enabled,
			dynamic_configuration,
			live: false,
			broker: None,
			scheduler: None,
			memory: None,
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
			values: HashMap::default(),
			forbidden: Vec::new(),
			parent: None,
			deadline: None,
			slow_log: None,
			cancelled: Arc::new(AtomicBool::new(false)),
			cancel_token: None,
			capabilities: Arc::new(Capabilities::default()),
			index_stores: IndexStores::new(
				256 * 1024 * 1024,
				256 * 1024 * 1024,
				64 * 1024 * 1024,
				None,
			),
			cache: None,
			index_builder: None,
			sequences: None,
			#[cfg(storage)]
			temporary_directory: None,
			transaction: None,
			isolated: false,
			buckets: None,
			#[cfg(feature = "surrealism")]
			surrealism_cache: None,
			redact_volatile_explain_attrs: false,
			statement_counters: None,
			config: Default::default(),
			#[cfg(feature = "http")]
			http_client: Arc::new(
				HttpClient::new(
					crate::dbs::capabilities::Targets::All,
					crate::dbs::capabilities::Targets::None,
					&Default::default(),
				)
				.expect("http client to be created"),
			),
			function_registry: Arc::new(FunctionRegistry::with_builtins()),
			live_units: None,
			compiled_units: None,
			vm_units: None,
			vm_deref: None,
			vm_served: None,
			vm_metrics: None,
			vm_edge_cascade: None,
			vm_closures: None,
			tenant_identity: None,
			node_id: Uuid::nil(),
			auth_enabled: true,
			dynamic_configuration: DynamicConfiguration::default(),
			live: false,
			broker: None,
			scheduler: None,
			memory: None,
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
		&self.function_registry
	}

	/// The live-subscription units this node has compiled for the VM, or
	/// `None` for a context made without a datastore.
	pub(crate) fn live_units(&self) -> Option<Arc<crate::exec::vm::live::LiveUnits>> {
		self.live_units.clone()
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
		self.compiled_units.as_ref()
	}

	pub fn node_id(&self) -> Uuid {
		self.node_id
	}

	#[inline]
	pub(crate) fn auth_enabled(&self) -> bool {
		self.auth_enabled
	}

	pub(crate) fn dynamic_configuration(&self) -> &DynamicConfiguration {
		&self.dynamic_configuration
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

	pub(crate) fn set_broker(&mut self, broker: Option<Arc<dyn MessageBroker>>) {
		self.broker = broker;
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

	/// Table-level permission check frequency (mirrors former [`Options::check_perms`]).
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
		let db_def =
			crate::exec::vm::descriptor::object(txn.ensure_ns_db(Some(self), ns, db)).await?;
		Ok((db_def.namespace_id, db_def.database_id))
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
		self.values.insert(key, value);
	}

	/// Bind each of `names` to `row`, a KNN query vector's row being scanned,
	/// rather than to a value: [`Self::binding`] answers
	/// [`Binding::Forbidden`] for it, and a value bound at this level after it,
	/// or at a deeper one, shadows it.
	pub(crate) fn add_forbidden(&mut self, names: &[&str], row: &Arc<ForbiddenRow>) {
		for name in names {
			self.values.remove(*name);
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
		&self.index_stores
	}

	/// Get the index_builder for this context/ds
	pub(crate) fn get_index_builder(&self) -> Option<&IndexBuilder> {
		self.index_builder.as_deref()
	}

	/// Return the sequences manager
	pub(crate) fn get_sequences(&self) -> Option<&Sequences> {
		self.sequences.as_deref()
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
		self.cache.clone()
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
			if ALLOC.is_beyond_threshold() {
				bail!(DatastoreError::QueryBeyondMemoryThreshold);
			}
			let now = Instant::now();
			if let Some((deadline, timeout)) = self.deadline
				&& deadline <= now
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
		self.temporary_directory.as_ref()
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

	/// The allocation [`Self::value`] answers for `key`.
	fn shared_value(&self, key: &str) -> Option<&Arc<Value>> {
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
	/// context.
	pub(crate) fn attach_session(&mut self, session: &Session) -> Result<(), Error> {
		self.live = session.live();
		self.add_values(session.values());
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

	/// Attach variables to the context.
	pub(crate) fn attach_variables(&mut self, vars: Variables) -> Result<(), Error> {
		for (name, val) in vars {
			if PROTECTED_PARAM_NAMES.contains(&name.as_str()) {
				return Err(ExecError::InvalidParam {
					name: name.into_string(),
				}
				.into());
			}
			self.add_value(name, Arc::new(val));
		}
		Ok(())
	}

	pub(crate) fn attach_public_variables(&mut self, vars: PublicVariables) -> Result<(), Error> {
		for (name, val) in vars {
			if PROTECTED_PARAM_NAMES.contains(&name.as_str()) {
				return Err(ExecError::InvalidParam {
					name,
				}
				.into());
			}
			self.add_value(name, Arc::new(convert_public_value_to_internal(val)));
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
				let target = NetTarget::Host(host.to_owned(), url.port_or_known_default());
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
		self.buckets.as_ref()
	}

	/// Obtain the connection for a bucket
	pub(crate) async fn get_bucket_store(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		bu: &str,
	) -> Result<Arc<dyn ObjectStore>> {
		// Do we have a buckets context?
		if let Some(buckets) = &self.buckets {
			buckets.get_bucket_store(&self.tx(), ns, db, bu).await
		} else {
			bail!(BucError::BucketUnavailable(bu.into()))
		}
	}

	#[cfg(feature = "surrealism")]
	pub(crate) fn get_surrealism_cache(&self) -> Option<Arc<SurrealismCache>> {
		self.surrealism_cache.as_ref().map(Arc::clone)
	}

	#[cfg(feature = "surrealism")]
	pub(crate) async fn get_surrealism_module(
		&self,
		lookup: SurrealismCacheLookup<'_>,
	) -> Result<SurrealismCachedModule> {
		if !self.get_capabilities().allows_experimental(&ExperimentalTarget::Surrealism) {
			bail!(
				"Failed to get surrealism runtime: Experimental capability `surrealism` is not enabled"
			);
		}

		let Some(cache) = self.get_surrealism_cache() else {
			bail!("Surrealism cache is not available");
		};
		let max_pool_size = self.config.surrealism.surrealism_max_pool_size;
		let max_memory = self.config.surrealism.surrealism_max_memory;
		let max_execution_time =
			self.config.surrealism.surrealism_max_execution_time.map(Duration::from_millis);
		#[cfg(feature = "http")]
		let config = self.config.http.clone();

		cache
			.get_or_insert_with(&lookup, async || {
				// Both executable forms resolve to the bytes of one `.surli`
				// archive; only where those bytes come from differs.
				let (surli, temp_prefix) = match lookup {
					SurrealismCacheLookup::File(ns, db, bucket, key) => {
						let bucket = self.get_bucket_store(*ns, *db, bucket).await?;
						let key = ObjectKey::new(key);
						let surli = bucket
							.get(&key)
							.await
							.map_err(|e| anyhow::anyhow!("failed to get file: {}", e))?;

						let Some(surli) = surli else {
							bail!("file not found");
						};

						let safe_key = key.to_string().replace(['/', '\\'], "_");
						(surli, format!("SURREAL_MODFS_{ns}_{db}_{safe_key}_"))
					}
					SurrealismCacheLookup::Silo(ns, db, org, pkg, major, minor, patch) => {
						#[cfg(not(feature = "http"))]
						{
							let _ = (ns, db, org, pkg, major, minor, patch);
							bail!(
								"Failed to get surrealism runtime: silo module packages are fetched over HTTP, which this build does not support"
							);
						}
						#[cfg(feature = "http")]
						{
							let endpoint = &self.config.surrealism.surrealism_silo_endpoint;
							let url = crate::surrealism::silo::package_url(
								endpoint, org, pkg, major, minor, patch,
							)?;
							// A silo fetch is first-party package resolution
							// rather than user-directed traffic, so it needs no
							// `allow_net` grant. An operator's explicit deny is
							// still honoured.
							let allow = match url.host() {
								Some(host) => Targets::Some(
									[NetTarget::Host(
										host.to_owned(),
										url.port_or_known_default(),
									)]
									.into_iter()
									.collect(),
								),
								None => bail!("The silo module endpoint `{endpoint}` has no host"),
							};
							let client = HttpClient::new(
								allow,
								self.capabilities.denied_network_targets_ref().clone(),
								&config,
							)
							.context("Failed to create http client for silo module packages")?;

							let surli = crate::surrealism::silo::fetch_package(
								&client, &url, org, pkg, major, minor, patch,
							)
							.await?;

							(
								surli,
								format!(
									"SURREAL_MODFS_silo_{ns}_{db}_{org}_{pkg}_{major}_{minor}_{patch}_"
								),
							)
						}
					}
				};

				let unpack_opts = UnpackOptions {
					#[cfg(storage)]
					temp_base: self.temporary_directory().map(|p| p.as_path()),
					#[cfg(not(storage))]
					temp_base: None,
					temp_prefix: &temp_prefix,
					max_fs_bytes: self.config.surrealism.surrealism_max_fs_bytes,
				};
				let package =
					SurrealismPackage::from_reader(std::io::Cursor::new(surli), &unpack_opts)?;

				crate::surrealism::validate_surrealism_capabilities(
					&self.get_capabilities(),
					&package.config.capabilities,
				)?;

				let org = package.config.meta.organisation.clone();
				let name = package.config.meta.name.clone();

				#[cfg(feature = "http")]
				let module_net_targets =
					crate::surrealism::host::module_allow_net_targets(&package.config.capabilities);

				// The operator's deny rules travel with the runtime. The
				// module's declared `allow_net` narrows the SurrealQL path via
				// `module_scoped_capabilities`, but the guest's own WASI sockets
				// are checked by the policy inside the runtime, which has no
				// other way to learn what the server forbids.
				let deny_net =
					crate::surrealism::host::surrealism_deny_net(&self.get_capabilities());

				let runtime = tokio::task::spawn_blocking(move || {
					Runtime::new(package, max_pool_size, max_memory, max_execution_time, deny_net)
				})
				.await
				.context("WASM compile task aborted")??;
				let runtime = Arc::new(runtime);

				let module_display_name: Arc<str> = format!("{org}::{name}").into();

				#[cfg(feature = "http")]
				let client = if module_net_targets.is_empty() {
					Arc::new(
						HttpClient::new(Targets::None, Targets::All, &config)
							.context("Failed to create http client for WASM module")?,
					)
				} else {
					let allow = Targets::Some(module_net_targets);
					Arc::new(
						HttpClient::new(
							allow,
							self.capabilities.denied_network_targets_ref().clone(),
							&config,
						)
						.context("Failed to create http client for WASM module")?,
					)
				};

				Ok(SurrealismCachedModule {
					runtime,
					module_display_name,
					#[cfg(feature = "http")]
					client,
				})
			})
			.await
	}

	#[cfg(feature = "http")]
	pub(crate) fn http_client(&self) -> Arc<HttpClient> {
		Arc::clone(&self.http_client)
	}

	#[cfg(feature = "surrealism")]
	pub(crate) async fn get_surrealism_runtime(
		&self,
		lookup: SurrealismCacheLookup<'_>,
	) -> Result<Arc<Runtime>> {
		Ok(self.get_surrealism_module(lookup).await?.runtime)
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
}

#[cfg(test)]
mod tests {
	#[cfg(feature = "http")]
	use std::str::FromStr;
	use std::time::Duration;

	#[cfg(all(feature = "allocation-tracking", feature = "allocator"))]
	use surrealdb_cnf::MEMORY_THRESHOLD;
	#[cfg(feature = "http")]
	use url::Url;

	use crate::ctx::Context;
	use crate::ctx::reason::Reason;
	#[cfg(feature = "http")]
	use crate::dbs::Capabilities;
	use crate::dbs::Options;
	#[cfg(feature = "http")]
	use crate::dbs::capabilities::{NetTarget, Targets};
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
	/// `cancelled` flag — without parent walk to fall back to. If `snapshot`
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
}
