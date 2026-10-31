//! The unified metrics observer.
//!
//! [`MetricsObserver`] translates the structured events emitted by the core
//! into one labelled OpenTelemetry instrument family per signal domain,
//! registered under signal-domain meter scopes (`surrealdb.statement`,
//! `surrealdb.query`, `surrealdb.transaction`, `surrealdb.rpc`,
//! `surrealdb.session`, `surrealdb.auth`, `surrealdb.network`,
//! `surrealdb.http`, `surrealdb.live_query`). The shared `SdkMeterProvider`
//! built in [`crate::telemetry::metrics`] routes those instruments to two
//! readers: the Prometheus text exporter (rendered by `GET /metrics`) and
//! the OTLP push exporter (when configured). One recording site, two
//! exposition formats.
//!
//! # One labelled family per signal
//!
//! There is no aggregate-vs-dimensional duplication: every counter and
//! histogram carries the resolved tenant ctx
//! (`namespace`, `database`, `user`) plus the safe-half attributes
//! (`outcome`, `statement_type`, `protocol`, …). Anonymous and pre-auth
//! events collapse missing ctx fields to the sentinel `"-"` so a single
//! Prometheus query works in every deployment, without the operator
//! needing to know whether the build was community or enterprise.
//!
//! Edition (community vs enterprise) is conveyed once on the OTel
//! `Resource` via `service.edition` (set at process start through
//! [`crate::telemetry::set_service_edition`]), surfaced as
//! `target_info{service_edition=…}` in the Prometheus output and as a
//! resource attribute on every OTLP record. Operators filter or group by
//! it without observers having to per-instrument-stamp the value.
//!
//! # Naming
//!
//! Instruments use OpenTelemetry semantic-convention-style dotted names
//! (`surrealdb.statement.duration`); the Prometheus text exporter converts
//! them deterministically to `surrealdb_statement_duration_seconds` style
//! family names. Counters get a `_total` suffix; histograms / counters
//! with unit `s` produce `_seconds`, with unit `By` produce `_bytes`.
//!
//! Attribute keys with a dot in the name (`http.request.method`,
//! `http.route`, `http.response.status_code`, `rpc.method`) follow OTel
//! semantic conventions so generic OTel-aligned dashboards work without
//! relabel rules. The Prometheus exporter sanitises the dot to `_` for
//! Prom-form rendering.

use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, UpDownCounter};
use surrealdb_core::mem::{RefusalSite, refusal_count};
use surrealdb_core::observe::process_snapshot;
use surrealdb_observe::{
	AuthEvent, BucketOperationEvent, ExecutionObserver, HttpRequestEvent, HttpRequestStartEvent,
	IndexCompactionPassEvent, IndexCompactionPlanEvent, IndexCompactionRetryEvent,
	IndexHnswCacheDiscardEvent, IndexPendingReadEvent, NetworkBytesEvent, NetworkDirection,
	Outcome, QueryEvent, RpcEvent, SessionAction, SessionEvent, StatementEvent, TaskPassEvent,
	TransactionEvent, TransactionMetricsSnapshot,
};

use super::instruments::{NONE_LABEL, attrs, names, scope};
use super::provider::PipelineCounters;
use super::runtime::ObservabilityRuntime;

/// Bucket boundaries, in seconds, of the age of a background queue's oldest
/// entry. A queue age is no `.duration` and matches none of the provider's
/// views, so it carries its own: from the sub-second wait of a queue kept up
/// with to the day a stuck one reaches, where the latency buckets stop at
/// thirty seconds.
const TASK_QUEUE_AGE_BUCKETS: &[f64] = &[
	0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0,
	3600.0, 7200.0, 21600.0, 86400.0,
];

/// Bucket boundaries for `surrealdb.index.pending_keys_read`: counts of
/// uncompacted pending delta keys merged on a single query read, spanning a
/// single key through single-batch (<= 512) and multi-batch backlog. A read
/// that merges no pending key records no sample.
const INDEX_PENDING_KEYS_READ_BUCKETS: &[f64] =
	&[1.0, 4.0, 16.0, 64.0, 256.0, 512.0, 1024.0, 4096.0, 16384.0];

/// Resolve an `Option<&str>` ctx field into a stable attribute value.
///
/// Returns a [`Cow`] so the `NONE_LABEL` sentinel and any pre-interned
/// caller-supplied value skip the allocation; only present, non-empty
/// user-supplied strings are owned.
fn label_cow(opt: Option<&str>) -> Cow<'static, str> {
	match opt {
		Some(s) if !s.is_empty() => Cow::Owned(s.to_owned()),
		_ => Cow::Borrowed(NONE_LABEL),
	}
}

/// Stringify an HTTP status code as an attribute value. Falls back to
/// [`NONE_LABEL`] when the inner service did not produce a response.
fn http_status(code: Option<u16>) -> Cow<'static, str> {
	match code {
		Some(c) => Cow::Owned(c.to_string()),
		None => Cow::Borrowed(NONE_LABEL),
	}
}

/// Resolve an `Option<&'static str>` error classification into a stable
/// attribute value. Successful / cancelled outcomes carry `None` and collapse
/// to [`NONE_LABEL`]; error outcomes carry one of the bounded
/// [`super::instruments::error_class`] constants.
fn error_class_label(class: Option<&'static str>) -> &'static str {
	class.unwrap_or(NONE_LABEL)
}

/// Records [`surrealdb_core::observe`] events as a labelled OpenTelemetry
/// instrument family per signal domain.
///
/// The observer is the single recording site for every signal in the
/// unified surface. Enterprise composers contribute additional observers
/// for the cardinality-capped per-tenant rollups and SurrealDS cluster
/// internals, but the primary `surrealdb.*` family lives here.
pub struct MetricsObserver {
	/// The statement, query, transaction, RPC and network-byte families, which every request
	/// adds to, gathered before they reach their instruments ([`Gathered`]).
	gathered: Arc<Gathered>,
	/// The KV operation, key and byte counts of the transactions, reported by observable
	/// counters ([`TransactionTallies`]).
	transaction_tallies: Arc<TransactionTallies>,
	transaction_conflicts: Counter<u64>,
	// Auth (scope: surrealdb.auth)
	auth_total: Counter<u64>,
	// Session (scope: surrealdb.session)
	session_total: Counter<u64>,
	session_active: UpDownCounter<i64>,
	session_duration: Histogram<f64>,
	// Bucket object storage (scope: surrealdb.bucket)
	bucket_sent: Counter<u64>,
	bucket_received: Counter<u64>,
	bucket_operations: Counter<u64>,
	// HTTP (scope: surrealdb.http)
	http_request_total: Counter<u64>,
	http_request_duration: Histogram<f64>,
	http_request_size: Counter<u64>,
	http_response_size: Counter<u64>,
	http_active_requests: UpDownCounter<i64>,
	// Live query (scope: surrealdb.live_query)
	live_query_active: UpDownCounter<i64>,
	live_query_notifications: Counter<u64>,
	live_query_orphaned: Counter<u64>,
	// Slow query (scope: surrealdb.slow_query)
	slow_query_total: Counter<u64>,
	/// Cached threshold (in milliseconds) for `slow_query_total`. `0` disables
	/// the slow-query counter without affecting the duration histogram.
	slow_query_threshold_ms: u64,
	// GraphQL (scope: surrealdb.graphql)
	graphql_operation_total: Counter<u64>,
	graphql_operation_duration: Histogram<f64>,
	// MCP (scope: surrealdb.mcp)
	mcp_tool_invocation: Counter<u64>,
	mcp_tool_duration: Histogram<f64>,
	mcp_session_active: UpDownCounter<i64>,
	// Background tasks (scope: surrealdb.task)
	task_pass: Counter<u64>,
	task_pass_duration: Histogram<f64>,
	task_items: Counter<u64>,
	task_queue_age: Histogram<f64>,
	task_backlog: Counter<u64>,
	// Index compaction & backlog (scope: surrealdb.index)
	index_compaction_pass: Counter<u64>,
	index_compaction_pass_duration: Histogram<f64>,
	index_compaction_plans: Counter<u64>,
	index_compaction_plan_duration: Histogram<f64>,
	index_compaction_pending_keys_folded: Counter<u64>,
	index_compaction_retries: Counter<u64>,
	index_compaction_unqueued: Counter<u64>,
	index_hnsw_cache_discards: Counter<u64>,
	index_pending_keys_read: Histogram<f64>,
}

/// The KV operations a transaction made, by op, as the `kv_op` attribute names them.
const KV_OPS: [&str; 5] = ["get", "scan", "put", "set", "del"];

/// The outcomes a transaction ends with, in the order of [`TransactionTallies`]' counts.
const OUTCOMES: [Outcome; 3] = [Outcome::Success, Outcome::Error, Outcome::Cancelled];

/// The counts of the per-transaction KV metrics: the operations by op, and the keys and bytes read
/// and written by outcome. Every write transaction adds to up to nine of them, and an instrument
/// sorts and hashes its attributes and locks its aggregator on every add, which at a hundred
/// thousand transactions a second on many cores is a lock every core contends for. So they are
/// added to atomics, in shards that the cores mostly do not share, and the observable counters
/// that report them sum the shards when they are collected: the same cumulative sums, under the
/// same names, units and attributes.
struct TransactionTallies {
	shards: Box<[TallyShard]>,
}

/// How many shards [`TransactionTallies`] keeps.
const TALLY_SHARDS: usize = 16;

/// What a byte or key count of [`TransactionTallies`] counts.
#[derive(Clone, Copy)]
enum Tally {
	KeysRead,
	KeysWritten,
	KeyBytesRead,
	ValueBytesRead,
	KeyBytesWritten,
	ValueBytesWritten,
}

/// One shard of [`TransactionTallies`], on a cache line of its own.
#[derive(Default)]
#[repr(align(128))]
struct TallyShard {
	kv_ops: [AtomicU64; KV_OPS.len()],
	/// By [`Tally`], then by the index of the outcome in [`OUTCOMES`].
	counts: [[AtomicU64; OUTCOMES.len()]; 6],
}

impl TransactionTallies {
	/// The tallies, with an observable counter on `meter` for each of the seven metrics.
	///
	/// `gathered` is handed to its instruments in the callback of the first: a collection runs
	/// every callback before it reads any instrument, so it reads what was gathered until then.
	fn register(meter: &opentelemetry::metrics::Meter, gathered: Arc<Gathered>) -> Arc<Self> {
		let tallies = Arc::new(Self {
			shards: (0..TALLY_SHARDS).map(|_| TallyShard::default()).collect(),
		});
		let kv_ops = Arc::clone(&tallies);
		// `build` returns the instrument, not a result. The meter keeps the callback registered
		// once the handle is dropped, as for the process instruments in `register_process_metrics`.
		let _kv_ops = meter
			.u64_observable_counter(names::TRANSACTION_KV_OPS)
			.with_description("Per-transaction KV operation counts, broken out by op type")
			.with_callback(move |obs| {
				gathered.hand_over();
				for (i, op) in KV_OPS.iter().enumerate() {
					let total = kv_ops.sum(|shard| &shard.kv_ops[i]);
					obs.observe(total, &[KeyValue::new(attrs::KV_OP, *op)]);
				}
			})
			.build();
		let counts = [
			(
				Tally::KeysRead,
				names::TRANSACTION_KEYS_READ,
				"Total keys read across transactions",
				None,
			),
			(
				Tally::KeysWritten,
				names::TRANSACTION_KEYS_WRITTEN,
				"Total keys written across transactions",
				None,
			),
			(
				Tally::KeyBytesRead,
				names::TRANSACTION_KEY_BYTES_READ,
				"Total key bytes read across transactions",
				Some("By"),
			),
			(
				Tally::ValueBytesRead,
				names::TRANSACTION_VALUE_BYTES_READ,
				"Total value bytes read across transactions",
				Some("By"),
			),
			(
				Tally::KeyBytesWritten,
				names::TRANSACTION_KEY_BYTES_WRITTEN,
				"Total key bytes written across transactions",
				Some("By"),
			),
			(
				Tally::ValueBytesWritten,
				names::TRANSACTION_VALUE_BYTES_WRITTEN,
				"Total value bytes written across transactions",
				Some("By"),
			),
		];
		for (tally, name, description, unit) in counts {
			let observed = Arc::clone(&tallies);
			let builder = meter.u64_observable_counter(name).with_description(description);
			let builder = match unit {
				Some(unit) => builder.with_unit(unit),
				None => builder,
			};
			// The meter keeps the callback registered once the handle is dropped, as above.
			let _counter = builder
				.with_callback(move |obs| {
					for (i, outcome) in OUTCOMES.iter().enumerate() {
						let total = observed.sum(|shard| &shard.counts[tally as usize][i]);
						// A count never added to is not reported, as an `add` never made would not
						// have made its series.
						if total > 0 {
							obs.observe(
								total,
								&[KeyValue::new(attrs::OUTCOME, outcome.as_label())],
							);
						}
					}
				})
				.build();
		}
		tallies
	}

	/// The shard the calling thread adds to.
	fn shard(&self) -> &TallyShard {
		static NEXT: AtomicUsize = AtomicUsize::new(0);
		thread_local! {
			static SHARD: usize = NEXT.fetch_add(1, Ordering::Relaxed) % TALLY_SHARDS;
		}
		&self.shards[SHARD.with(|shard| *shard)]
	}

	fn sum(&self, count: impl Fn(&TallyShard) -> &AtomicU64) -> u64 {
		self.shards.iter().map(|shard| count(shard).load(Ordering::Relaxed)).sum()
	}

	/// Adds what a transaction that ended with `outcome` did.
	fn record(&self, outcome: Outcome, m: &TransactionMetricsSnapshot) {
		let shard = self.shard();
		let ops = [m.ops_get, m.ops_scan, m.ops_put, m.ops_set, m.ops_del];
		for (count, n) in shard.kv_ops.iter().zip(ops) {
			if n > 0 {
				count.fetch_add(u64::from(n), Ordering::Relaxed);
			}
		}
		let Some(at) = OUTCOMES.iter().position(|o| *o == outcome) else {
			return;
		};
		let counts = [
			(Tally::KeysRead, m.keys_read),
			(Tally::KeysWritten, m.keys_written),
			(Tally::KeyBytesRead, m.key_bytes_read),
			(Tally::ValueBytesRead, m.value_bytes_read),
			(Tally::KeyBytesWritten, m.key_bytes_written),
			(Tally::ValueBytesWritten, m.value_bytes_written),
		];
		for (tally, n) in counts {
			if n > 0 {
				shard.counts[tally as usize][at].fetch_add(n, Ordering::Relaxed);
			}
		}
	}
}

/// The statement, query, transaction, RPC and network-byte families, which every request adds to,
/// with what was added to them since they were last handed it.
///
/// An instrument sorts and hashes the attributes of every measurement, takes a read lock over its
/// series and, for a histogram, locks the series' buckets. A request adds to these families six
/// times over, every request of a tenant to the same series, so at a hundred thousand requests a
/// second on many cores those locks are what the cores contend for. So each thread adds to a shard
/// of its own, which keeps the measurements by series, and the shard hands them to the instruments
/// once it holds [`GATHER_HOLD`] of them, and whenever the metrics are collected: the
/// [`TransactionTallies`] callback hands every shard over before the collection reads the
/// instruments. The instruments receive every measurement, with the attributes it was made with, so
/// their sums, counts, buckets and bounds are those of measuring each as it is made.
struct Gathered {
	// Statement (scope: surrealdb.statement)
	statement_total: Counter<u64>,
	statement_duration: Histogram<f64>,
	statement_rows: Counter<u64>,
	// Query (scope: surrealdb.query)
	query_total: Counter<u64>,
	query_duration: Histogram<f64>,
	// Transaction (scope: surrealdb.transaction)
	transaction_total: Counter<u64>,
	transaction_duration: Histogram<f64>,
	// RPC (scope: surrealdb.rpc)
	rpc_total: Counter<u64>,
	rpc_duration: Histogram<f64>,
	// Network (scope: surrealdb.network)
	network_received: Counter<u64>,
	network_sent: Counter<u64>,
	shards: Box<[GatherShard]>,
}

/// How many shards [`Gathered`] keeps.
const GATHER_SHARDS: usize = 64;

/// How many measurements a shard of [`Gathered`] holds before it hands them over.
const GATHER_HOLD: usize = 256;

/// How many series a shard of [`Gathered`] keeps between hand-overs. A series is kept while it is
/// added to, so a shard that tenants come and go through does not grow without bound.
const GATHER_SERIES: usize = 64;

/// One shard of [`Gathered`], on a cache line of its own.
#[derive(Default)]
#[repr(align(128))]
struct GatherShard(std::sync::Mutex<GatherSeries>);

/// The series a shard of [`Gathered`] keeps, and how many measurements they hold.
#[derive(Default)]
struct GatherSeries {
	series: Vec<Series>,
	held: usize,
}

/// The family a measurement of [`Gathered`] is made in, with the attributes only that family has.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Family {
	/// A statement completed, of the type labelled.
	Statement(&'static str),
	/// A query batch completed.
	Query,
	/// A transaction completed; whether it could write.
	Transaction(bool),
	/// An RPC method of the name labelled completed.
	Rpc(&'static str),
	/// Bytes were received over the protocol labelled.
	Received(&'static str),
	/// Bytes were sent over the protocol labelled.
	Sent(&'static str),
}

/// The labels of one measurement of [`Gathered`]. `outcome` and `error_class` are not attributes
/// of the network-byte families.
#[derive(Clone, Copy)]
struct Labels<'a> {
	family: Family,
	outcome: &'static str,
	error_class: &'static str,
	namespace: Option<&'a str>,
	database: Option<&'a str>,
	user: Option<&'a str>,
}

/// The measurements one series of [`Gathered`] holds.
struct Series {
	family: Family,
	outcome: &'static str,
	error_class: &'static str,
	/// The tenant labels, `None` where [`label_cow`] reports [`NONE_LABEL`].
	namespace: Option<String>,
	database: Option<String>,
	user: Option<String>,
	/// How many measurements were made.
	count: u64,
	/// The rows of the statements, or the bytes received or sent.
	amount: u64,
	/// The duration of each measurement, in seconds, for the families that have one.
	durations: Vec<f64>,
}

/// A tenant label as [`Series`] keeps it.
fn tenant(label: Option<&str>) -> Option<&str> {
	label.filter(|label| !label.is_empty())
}

impl Series {
	fn new(labels: &Labels<'_>) -> Self {
		Self {
			family: labels.family,
			outcome: labels.outcome,
			error_class: labels.error_class,
			namespace: tenant(labels.namespace).map(str::to_owned),
			database: tenant(labels.database).map(str::to_owned),
			user: tenant(labels.user).map(str::to_owned),
			count: 0,
			amount: 0,
			durations: Vec::new(),
		}
	}

	/// Whether a measurement with `labels` belongs to this series.
	fn is(&self, labels: &Labels<'_>) -> bool {
		self.family == labels.family
			&& self.outcome == labels.outcome
			&& self.error_class == labels.error_class
			&& self.namespace.as_deref() == tenant(labels.namespace)
			&& self.database.as_deref() == tenant(labels.database)
			&& self.user.as_deref() == tenant(labels.user)
	}

	/// The tenant attributes, as each family reports them last.
	fn tenant_attributes(&self) -> [KeyValue; 3] {
		[
			KeyValue::new(attrs::NAMESPACE, label_cow(self.namespace.as_deref())),
			KeyValue::new(attrs::DATABASE, label_cow(self.database.as_deref())),
			KeyValue::new(attrs::USER, label_cow(self.user.as_deref())),
		]
	}

	/// The attributes of a family whose own attribute is `first`, followed by the outcome, the
	/// error class and the tenant.
	fn attributes(&self, first: KeyValue, tenant: [KeyValue; 3]) -> [KeyValue; 6] {
		let [namespace, database, user] = tenant;
		[
			first,
			KeyValue::new(attrs::OUTCOME, self.outcome),
			KeyValue::new(attrs::ERROR_CLASS, self.error_class),
			namespace,
			database,
			user,
		]
	}
}

impl Gathered {
	fn shards() -> Box<[GatherShard]> {
		(0..GATHER_SHARDS).map(|_| GatherShard::default()).collect()
	}

	/// The shard the calling thread adds to.
	fn shard(&self) -> &GatherShard {
		static NEXT: AtomicUsize = AtomicUsize::new(0);
		thread_local! {
			static SHARD: usize = NEXT.fetch_add(1, Ordering::Relaxed) % GATHER_SHARDS;
		}
		&self.shards[SHARD.with(|shard| *shard)]
	}

	/// Add a measurement with `labels`: its duration in seconds, for the families that have one,
	/// and the rows or bytes it counts.
	fn gather(&self, labels: Labels<'_>, duration: Option<f64>, amount: u64) {
		let mut held = self.shard().0.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
		let at = match held.series.iter().position(|series| series.is(&labels)) {
			Some(at) => at,
			None => {
				held.series.push(Series::new(&labels));
				held.series.len() - 1
			}
		};
		let series = &mut held.series[at];
		series.count += 1;
		series.amount += amount;
		if let Some(duration) = duration {
			series.durations.push(duration);
		}
		held.held += 1;
		if held.held >= GATHER_HOLD {
			self.hand_over_shard(&mut held);
		}
	}

	/// Hand what every shard holds to the instruments.
	fn hand_over(&self) {
		for shard in self.shards.iter() {
			self.hand_over_shard(
				&mut shard.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner),
			);
		}
	}

	/// Hand what `held` holds to the instruments, and let go of the series nothing was added to
	/// since the last hand-over.
	fn hand_over_shard(&self, held: &mut GatherSeries) {
		held.series.retain_mut(|series| {
			if series.count == 0 {
				return false;
			}
			self.hand(series);
			series.count = 0;
			series.amount = 0;
			series.durations.clear();
			true
		});
		if held.series.len() > GATHER_SERIES {
			held.series.clear();
		}
		held.held = 0;
	}

	/// Hand the measurements `series` holds to its family's instruments.
	fn hand(&self, series: &Series) {
		let tenant = series.tenant_attributes();
		match series.family {
			Family::Statement(kind) => {
				let attrs = series.attributes(KeyValue::new(attrs::STATEMENT_TYPE, kind), tenant);
				self.measure(&self.statement_total, &self.statement_duration, series, &attrs);
				if series.amount > 0 {
					self.statement_rows.add(series.amount, &attrs);
				}
			}
			Family::Query => {
				let [namespace, database, user] = tenant;
				let attrs = [
					KeyValue::new(attrs::OUTCOME, series.outcome),
					KeyValue::new(attrs::ERROR_CLASS, series.error_class),
					namespace,
					database,
					user,
				];
				self.measure(&self.query_total, &self.query_duration, series, &attrs);
			}
			Family::Transaction(write) => {
				let write = if write {
					"true"
				} else {
					"false"
				};
				let attrs = series.attributes(KeyValue::new(attrs::WRITE, write), tenant);
				self.measure(&self.transaction_total, &self.transaction_duration, series, &attrs);
			}
			Family::Rpc(method) => {
				let attrs = series.attributes(KeyValue::new(attrs::RPC_METHOD, method), tenant);
				self.measure(&self.rpc_total, &self.rpc_duration, series, &attrs);
			}
			Family::Received(protocol) => {
				let [namespace, database, user] = tenant;
				let attrs = [KeyValue::new(attrs::PROTOCOL, protocol), namespace, database, user];
				self.network_received.add(series.amount, &attrs);
			}
			Family::Sent(protocol) => {
				let [namespace, database, user] = tenant;
				let attrs = [KeyValue::new(attrs::PROTOCOL, protocol), namespace, database, user];
				self.network_sent.add(series.amount, &attrs);
			}
		}
	}

	/// Count the measurements of `series` in `total`, and record each one's duration in
	/// `duration`, with `attrs`.
	fn measure(
		&self,
		total: &Counter<u64>,
		duration: &Histogram<f64>,
		series: &Series,
		attrs: &[KeyValue],
	) {
		total.add(series.count, attrs);
		for value in &series.durations {
			duration.record(*value, attrs);
		}
	}
}

impl MetricsObserver {
	/// Construct the unified observer, lazily registering each
	/// instrument against the supplied
	/// [`ObservabilityRuntime`](super::ObservabilityRuntime)'s meter
	/// provider under its signal-domain scope.
	///
	/// When the runtime carries the no-op default
	/// [`opentelemetry_sdk::metrics::SdkMeterProvider`] (because telemetry
	/// is disabled), every recording becomes a no-op; the observer
	/// itself stays cheap because the OTel SDK short-circuits no-op
	/// meters internally.
	pub fn new(runtime: &ObservabilityRuntime) -> anyhow::Result<Self> {
		let stmt = runtime.meter(scope::STATEMENT);
		let qry = runtime.meter(scope::QUERY);
		let tx = runtime.meter(scope::TRANSACTION);
		let rpc = runtime.meter(scope::RPC);
		let auth = runtime.meter(scope::AUTH);
		let sess = runtime.meter(scope::SESSION);
		let net = runtime.meter(scope::NETWORK);
		let buc = runtime.meter(scope::BUCKET);
		let http = runtime.meter(scope::HTTP);
		let lq = runtime.meter(scope::LIVE_QUERY);
		let slow = runtime.meter(scope::SLOW_QUERY);
		let gql = runtime.meter(scope::GRAPHQL);
		let mcp = runtime.meter(scope::MCP);
		let task = runtime.meter(scope::TASK);
		let idx = runtime.meter(scope::INDEX);

		let gathered = Arc::new(Gathered {
			statement_total: stmt
				.u64_counter(names::STATEMENT_TOTAL)
				.with_description("Cumulative count of top-level statement completions")
				.build(),
			statement_duration: stmt
				.f64_histogram(names::STATEMENT_DURATION)
				.with_description("Distribution of top-level statement execution latency")
				.with_unit("s")
				.build(),
			statement_rows: stmt
				.u64_counter(names::STATEMENT_ROWS)
				.with_description(
					"Cumulative count of rows returned (SELECT) or affected \
					 (CREATE / UPDATE / UPSERT / DELETE / RELATE / INSERT) by \
					 completed statements",
				)
				.build(),
			query_total: qry
				.u64_counter(names::QUERY_TOTAL)
				.with_description("Cumulative count of query batch completions")
				.build(),
			query_duration: qry
				.f64_histogram(names::QUERY_DURATION)
				.with_description("Distribution of query batch latency")
				.with_unit("s")
				.build(),
			transaction_total: tx
				.u64_counter(names::TRANSACTION_TOTAL)
				.with_description("Cumulative count of transaction completions")
				.build(),
			transaction_duration: tx
				.f64_histogram(names::TRANSACTION_DURATION)
				.with_description("Distribution of transaction lifetime at commit / cancel")
				.with_unit("s")
				.build(),
			rpc_total: rpc
				.u64_counter(names::RPC_TOTAL)
				.with_description("Cumulative count of RPC method invocations")
				.build(),
			rpc_duration: rpc
				.f64_histogram(names::RPC_DURATION)
				.with_description("Distribution of RPC method latency")
				.with_unit("s")
				.build(),
			network_received: net
				.u64_counter(names::NETWORK_RECEIVED)
				.with_description(
					"Cumulative inbound bytes observed at the HTTP / WebSocket ingress",
				)
				.with_unit("By")
				.build(),
			network_sent: net
				.u64_counter(names::NETWORK_SENT)
				.with_description(
					"Cumulative outbound bytes written at the HTTP / WebSocket egress",
				)
				.with_unit("By")
				.build(),
			shards: Gathered::shards(),
		});
		Ok(Self {
			transaction_tallies: TransactionTallies::register(&tx, Arc::clone(&gathered)),
			gathered,
			transaction_conflicts: tx
				.u64_counter(names::TRANSACTION_CONFLICTS)
				.with_description(
					"Cumulative count of transactions that aborted due to a commit conflict",
				)
				.build(),
			auth_total: auth
				.u64_counter(names::AUTH_TOTAL)
				.with_description("Cumulative count of authentication attempts")
				.build(),
			session_total: sess
				.u64_counter(names::SESSION_TOTAL)
				.with_description("Cumulative count of session connect / disconnect events")
				.build(),
			session_active: sess
				.i64_up_down_counter(names::SESSION_ACTIVE)
				.with_description("Number of currently-connected sessions")
				.build(),
			session_duration: sess
				.f64_histogram(names::SESSION_DURATION)
				.with_description("Distribution of session lifetime at disconnect")
				.with_unit("s")
				.build(),
			bucket_sent: buc
				.u64_counter(names::BUCKET_SENT_BYTES)
				.with_description("Cumulative bytes uploaded to bucket object storage")
				.with_unit("By")
				.build(),
			bucket_received: buc
				.u64_counter(names::BUCKET_RECEIVED_BYTES)
				.with_description("Cumulative bytes downloaded from bucket object storage")
				.with_unit("By")
				.build(),
			bucket_operations: buc
				.u64_counter(names::BUCKET_OPERATIONS)
				.with_description("Cumulative count of bucket object-storage operations")
				.build(),
			http_request_total: http
				.u64_counter(names::HTTP_REQUEST_TOTAL)
				.with_description("Cumulative count of HTTP requests served by the tower stack")
				.build(),
			http_request_duration: http
				.f64_histogram(names::HTTP_REQUEST_DURATION)
				.with_description(
					"Distribution of HTTP request latency, measured at the outer tower layer",
				)
				.with_unit("s")
				.build(),
			http_request_size: http
				.u64_counter(names::HTTP_REQUEST_SIZE)
				.with_description("Cumulative inbound bytes observed on completed HTTP requests")
				.with_unit("By")
				.build(),
			http_response_size: http
				.u64_counter(names::HTTP_RESPONSE_SIZE)
				.with_description("Cumulative outbound bytes written on completed HTTP responses")
				.with_unit("By")
				.build(),
			// Up/down gauge attributes are intentionally restricted to
			// the safe-half of the request envelope (method / route).
			// HTTP request-start fires BEFORE auth runs and so carries
			// `HttpRequestEventCtx::default()`; the corresponding `+1`
			// and the `-1` from the populated completion event must hit
			// the same series, otherwise the gauge drifts forever.
			// Tenant attribution stays on the total / duration / size
			// families above.
			http_active_requests: http
				.i64_up_down_counter(names::HTTP_ACTIVE_REQUESTS)
				.with_description("HTTP requests currently in flight on the tower stack")
				.build(),
			live_query_active: lq
				.i64_up_down_counter(names::LIVE_QUERY_ACTIVE)
				.with_description("Number of currently-registered LIVE queries")
				.build(),
			live_query_notifications: lq
				.u64_counter(names::LIVE_QUERY_NOTIFICATIONS)
				.with_description("Cumulative count of LIVE query notifications dispatched")
				.build(),
			live_query_orphaned: lq
				.u64_counter(names::LIVE_QUERY_ORPHANED)
				.with_description(
					"Cumulative count of LIVE queries that were orphaned because their \
					 datastore-side registration could not be cleaned up after the \
					 WebSocket closed",
				)
				.build(),
			slow_query_total: slow
				.u64_counter(names::SLOW_QUERY_TOTAL)
				.with_description(
					"Cumulative count of statements whose execution exceeded \
					 SURREAL_SLOW_QUERY_METRIC_THRESHOLD_MS",
				)
				.build(),
			slow_query_threshold_ms: *crate::cnf::SLOW_QUERY_METRIC_THRESHOLD_MS,
			graphql_operation_total: gql
				.u64_counter(names::GRAPHQL_OPERATION_TOTAL)
				.with_description("Cumulative count of GraphQL operations executed")
				.build(),
			graphql_operation_duration: gql
				.f64_histogram(names::GRAPHQL_OPERATION_DURATION)
				.with_description("Distribution of GraphQL operation latency")
				.with_unit("s")
				.build(),
			mcp_tool_invocation: mcp
				.u64_counter(names::MCP_TOOL_INVOCATION)
				.with_description("Cumulative count of MCP tool invocations")
				.build(),
			mcp_tool_duration: mcp
				.f64_histogram(names::MCP_TOOL_DURATION)
				.with_description("Distribution of MCP tool invocation latency")
				.with_unit("s")
				.build(),
			mcp_session_active: mcp
				.i64_up_down_counter(names::MCP_SESSION_ACTIVE)
				.with_description("Number of currently-active MCP sessions")
				.build(),
			task_pass: task
				.u64_counter(names::TASK_PASS)
				.with_description(
					"Cumulative count of background maintenance job passes, by job, \
					 outcome and lease standing",
				)
				.build(),
			task_pass_duration: task
				.f64_histogram(names::TASK_PASS_DURATION)
				.with_description("Distribution of background maintenance pass duration")
				.with_unit("s")
				.build(),
			task_items: task
				.u64_counter(names::TASK_ITEMS)
				.with_description(
					"Cumulative count of queue entries or work units handled by background \
					 maintenance passes",
				)
				.build(),
			task_queue_age: task
				.f64_histogram(names::TASK_QUEUE_AGE)
				.with_description(
					"Distribution of the age of the oldest queued entry when a background \
					 pass started, for passes that found work queued",
				)
				.with_unit("s")
				.with_boundaries(TASK_QUEUE_AGE_BUCKETS.to_vec())
				.build(),
			task_backlog: task
				.u64_counter(names::TASK_BACKLOG)
				.with_description(
					"Cumulative count of background maintenance passes that stopped at their \
					 budget with work still pending",
				)
				.build(),
			index_compaction_pass: idx
				.u64_counter(names::INDEX_COMPACTION_PASS)
				.with_description(
					"Cumulative count of per-index compaction passes, by index kind, trigger, \
					 and outcome",
				)
				.build(),
			index_compaction_pass_duration: idx
				.f64_histogram(names::INDEX_COMPACTION_PASS_DURATION)
				.with_description("Distribution of per-index compaction pass duration")
				.with_unit("s")
				.build(),
			index_compaction_plans: idx
				.u64_counter(names::INDEX_COMPACTION_PLANS)
				.with_description(
					"Cumulative count of committed index compaction plans that folded at least \
					 one pending key, by index kind and plan mode",
				)
				.build(),
			index_compaction_plan_duration: idx
				.f64_histogram(names::INDEX_COMPACTION_PLAN_DURATION)
				.with_description(
					"Distribution of the duration of committed index compaction plans that \
					 folded at least one pending key",
				)
				.with_unit("s")
				.build(),
			index_compaction_pending_keys_folded: idx
				.u64_counter(names::INDEX_COMPACTION_PENDING_KEYS_FOLDED)
				.with_description(
					"Cumulative count of pending delta keys folded into compacted index \
					 structures, by index kind and plan mode",
				)
				.build(),
			index_compaction_retries: idx
				.u64_counter(names::INDEX_COMPACTION_RETRIES)
				.with_description(
					"Cumulative count of index compaction batch retries, by index kind and \
					 retry reason",
				)
				.build(),
			index_compaction_unqueued: idx
				.u64_counter(names::INDEX_COMPACTION_UNQUEUED)
				.with_description(
					"Cumulative count of compaction passes that owed their index a fresh \
					 compaction request and failed to queue it, by index kind and trigger",
				)
				.build(),
			index_hnsw_cache_discards: idx
				.u64_counter(names::INDEX_HNSW_CACHE_DISCARDS)
				.with_description(
					"Cumulative count of resident HNSW graph caches discarded after a failed \
					 compaction commit",
				)
				.build(),
			index_pending_keys_read: idx
				.f64_histogram(names::INDEX_PENDING_KEYS_READ)
				.with_description(
					"Distribution of uncompacted pending delta keys merged per query-time \
					 pending-delta read that merged any, by index kind",
				)
				.with_boundaries(INDEX_PENDING_KEYS_READ_BUCKETS.to_vec())
				.build(),
		})
	}

	/// Bump or drop the active LIVE query gauge under the supplied tenant
	/// ctx. Callers are expected to pass the namespace/database of the
	/// session that registered the LIVE statement; the corresponding
	/// decrement on KILL or session teardown MUST use the same ctx so the
	/// gauge series stays balanced.
	pub fn adjust_live_query_active(
		&self,
		delta: i64,
		namespace: Option<&str>,
		database: Option<&str>,
	) {
		if delta == 0 {
			return;
		}
		let attrs = [
			KeyValue::new(attrs::NAMESPACE, label_cow(namespace)),
			KeyValue::new(attrs::DATABASE, label_cow(database)),
		];
		self.live_query_active.add(delta, &attrs);
	}

	/// Record a single LIVE query notification dispatch under the supplied
	/// tenant ctx. The labels mirror those used on the active gauge so
	/// operators can join across the two families.
	pub fn record_live_query_notification(&self, namespace: Option<&str>, database: Option<&str>) {
		let attrs = [
			KeyValue::new(attrs::NAMESPACE, label_cow(namespace)),
			KeyValue::new(attrs::DATABASE, label_cow(database)),
		];
		self.live_query_notifications.add(1, &attrs);
	}

	/// Record a single orphaned LIVE query: the in-memory registration was
	/// lost and the datastore-side catalog row could not be removed, so the
	/// broker will keep producing notifications for it that are silently
	/// discarded for the lifetime of the datastore. The labels mirror those
	/// on the active gauge so operators can attribute the leak to a tenant.
	pub fn record_live_query_orphaned(&self, namespace: Option<&str>, database: Option<&str>) {
		let attrs = [
			KeyValue::new(attrs::NAMESPACE, label_cow(namespace)),
			KeyValue::new(attrs::DATABASE, label_cow(database)),
		];
		self.live_query_orphaned.add(1, &attrs);
	}

	/// Record a single GraphQL operation completion.
	///
	/// `operation_type` should be a stable lower-case identifier from the
	/// closed set `{"query", "mutation", "subscription", "unknown"}`.
	/// `error_class` is `Some` only when `outcome` is
	/// [`surrealdb_observe::Outcome::Error`].
	#[allow(clippy::too_many_arguments)]
	pub fn record_graphql_operation(
		&self,
		operation_type: &'static str,
		outcome: surrealdb_observe::Outcome,
		error_class: Option<&'static str>,
		duration: std::time::Duration,
		namespace: Option<&str>,
		database: Option<&str>,
		user: Option<&str>,
	) {
		let attrs = [
			KeyValue::new(attrs::GRAPHQL_OPERATION_TYPE, operation_type),
			KeyValue::new(attrs::OUTCOME, outcome.as_label()),
			KeyValue::new(attrs::ERROR_CLASS, error_class_label(error_class)),
			KeyValue::new(attrs::NAMESPACE, label_cow(namespace)),
			KeyValue::new(attrs::DATABASE, label_cow(database)),
			KeyValue::new(attrs::USER, label_cow(user)),
		];
		self.graphql_operation_total.add(1, &attrs);
		self.graphql_operation_duration.record(duration.as_secs_f64(), &attrs);
	}

	/// Record a single MCP tool invocation.
	///
	/// `tool` is the bounded tool identifier from the MCP service's static
	/// dispatch table; `transport` is one of `"stdio"` / `"http"`.
	pub fn record_mcp_tool(
		&self,
		tool: &'static str,
		transport: &'static str,
		outcome: surrealdb_observe::Outcome,
		error_class: Option<&'static str>,
		duration: std::time::Duration,
	) {
		let attrs = [
			KeyValue::new(attrs::MCP_TOOL, tool),
			KeyValue::new(attrs::MCP_TRANSPORT, transport),
			KeyValue::new(attrs::OUTCOME, outcome.as_label()),
			KeyValue::new(attrs::ERROR_CLASS, error_class_label(error_class)),
		];
		self.mcp_tool_invocation.add(1, &attrs);
		self.mcp_tool_duration.record(duration.as_secs_f64(), &attrs);
	}

	/// Bump or drop the active MCP session gauge. Connect / disconnect must
	/// pass the same `transport` so the gauge series stays balanced.
	pub fn adjust_mcp_session_active(&self, delta: i64, transport: &'static str) {
		if delta == 0 {
			return;
		}
		let attrs = [KeyValue::new(attrs::MCP_TRANSPORT, transport)];
		self.mcp_session_active.add(delta, &attrs);
	}

	/// Register pipeline self-metrics
	/// (`<scope_and_prefix>.records` / `.dropped` / `.queue_depth` /
	/// `.appended` / `.append_errors`) for an audit-style sink.
	///
	/// The same string is used both as the OTel meter scope and as the
	/// metric-name prefix, e.g. [`scope::AUDIT`] = `"surrealdb.audit"`
	/// produces gauges `surrealdb.audit.records`, `.dropped`, … under
	/// the `surrealdb.audit` instrumentation scope. Callers pass
	/// `&'static str` so the meter scope value lives for the lifetime
	/// of the global provider without per-call allocation.
	///
	/// Implemented as observable gauges so values are pulled at scrape /
	/// export time from the supplied [`PipelineCounters`]. The registered
	/// gauges live for the lifetime of the runtime's meter provider and
	/// need no explicit unregister.
	pub fn register_pipeline_self_metrics(
		runtime: &ObservabilityRuntime,
		scope_and_prefix: &'static str,
		kind: &str,
		counters: Arc<dyn PipelineCounters>,
	) -> anyhow::Result<()> {
		let meter = runtime.meter(scope_and_prefix);
		let records = Arc::clone(&counters);
		let _records_gauge = meter
			.u64_observable_gauge(format!("{scope_and_prefix}.records"))
			.with_description(format!("Cumulative count of {kind} records successfully enqueued",))
			.with_callback(move |obs| obs.observe(records.records_total(), &[]))
			.build();
		let dropped = Arc::clone(&counters);
		let _dropped_gauge = meter
			.u64_observable_gauge(format!("{scope_and_prefix}.dropped"))
			.with_description(format!(
				"Cumulative count of {kind} records dropped due to queue overflow",
			))
			.with_callback(move |obs| obs.observe(dropped.dropped_total(), &[]))
			.build();
		let depth = Arc::clone(&counters);
		let _depth_gauge = meter
			.i64_observable_gauge(format!("{scope_and_prefix}.queue_depth"))
			.with_description(format!(
				"Number of {kind} records currently buffered between observer and sink",
			))
			.with_callback(move |obs| obs.observe(depth.queue_depth(), &[]))
			.build();
		let appended = Arc::clone(&counters);
		let _appended_gauge = meter
			.u64_observable_gauge(format!("{scope_and_prefix}.appended"))
			.with_description(format!(
				"Cumulative count of {kind} records successfully written to the sink",
			))
			.with_callback(move |obs| obs.observe(appended.appended_total(), &[]))
			.build();
		let errors = counters;
		let _errors_gauge = meter
			.u64_observable_gauge(format!("{scope_and_prefix}.append_errors"))
			.with_description(format!(
				"Cumulative count of {kind} records that failed to write to the sink",
			))
			.with_callback(move |obs| obs.observe(errors.append_errors_total(), &[]))
			.build();
		Ok(())
	}
}

/// Register process-level observable gauges (build_info, uptime, memory,
/// cpu) against the supplied runtime's `surrealdb.process` meter scope.
///
/// Build info is a constant gauge with the `build_version` attribute. The
/// other three observe live values via the system snapshot helper on every
/// collection call, so scrape / export latency is bounded by `sysinfo`'s
/// refresh cost.
pub fn register_process_metrics(runtime: &ObservabilityRuntime) {
	let meter = runtime.meter(scope::PROCESS);
	let started_at = web_time::Instant::now();
	let version = crate::cnf::PKG_VERSION.as_str().to_owned();
	let _build_info = meter
		.u64_observable_gauge(names::BUILD_INFO)
		.with_description("SurrealDB build information (value is always 1)")
		.with_callback(move |obs| {
			obs.observe(1, &[KeyValue::new("build_version", version.clone())]);
		})
		.build();
	let _uptime = meter
		.i64_observable_gauge(names::PROCESS_UPTIME)
		.with_description("Seconds since the surreal process started")
		.with_unit("s")
		.with_callback(move |obs| {
			obs.observe(started_at.elapsed().as_secs() as i64, &[]);
		})
		.build();
	let _memory = meter
		.i64_observable_gauge(names::PROCESS_MEMORY)
		.with_description("Resident set size of the surreal process")
		.with_unit("By")
		.with_callback(|obs| {
			let snap = process_snapshot();
			obs.observe(snap.memory_bytes as i64, &[]);
		})
		.build();
	// The quantity the memory threshold is actually compared against. Paired
	// with `PROCESS_MEMORY` (resident set size) it shows how far tracked bytes
	// sit below the real footprint, and paired with `PROCESS_MEMORY_THRESHOLD`
	// it shows how close the instance is to refusing queries. Reports 0 in
	// builds without allocation tracking compiled in.
	let _memory_allocated = meter
		.i64_observable_gauge(names::PROCESS_MEMORY_ALLOCATED)
		.with_description(
			"Tracked memory of the surreal process: live Rust-heap allocations plus external \
			 memory reporters. This is the value compared against SURREAL_MEMORY_THRESHOLD",
		)
		.with_unit("By")
		.with_callback(|obs| {
			let snap = process_snapshot();
			obs.observe(snap.memory_allocated_bytes as i64, &[]);
		})
		.build();
	// The logical size of the process's data, before size-class rounding.
	// Unlike the reserved and allocated gauges it is comparable across builds
	// and architectures. Pair it with `PROCESS_MEMORY_RESERVED` for internal
	// fragmentation — not with `PROCESS_MEMORY_ALLOCATED`, which also carries
	// the external reporters. Reports 0 in builds without allocation tracking
	// compiled in.
	let _memory_requested = meter
		.i64_observable_gauge(names::PROCESS_MEMORY_REQUESTED)
		.with_description(
			"Bytes requested by the surreal process for live heap allocations, before the \
			 allocator rounded each request up to a size class",
		)
		.with_unit("By")
		.with_callback(|obs| {
			let snap = process_snapshot();
			obs.observe(snap.memory_requested_bytes as i64, &[]);
		})
		.build();
	// The heap half of `PROCESS_MEMORY_ALLOCATED`, excluding the external
	// reporters. This is the term that makes the fragmentation subtraction
	// meaningful, and the one to compare against a heap-only budget. Reports 0
	// in builds without allocation tracking compiled in.
	let _memory_reserved = meter
		.i64_observable_gauge(names::PROCESS_MEMORY_RESERVED)
		.with_description(
			"Reserved Rust-heap bytes of the surreal process: the size classes the allocator \
			 rounded live requests up into, excluding external memory reporters",
		)
		.with_unit("By")
		.with_callback(|obs| {
			let snap = process_snapshot();
			obs.observe(snap.memory_reserved_bytes as i64, &[]);
		})
		.build();
	// Emitted so a dashboard can plot the limit alongside the usage without the
	// operator having to restate it. Constant for the process lifetime, since
	// the threshold is read once from the environment.
	let _memory_threshold = meter
		.i64_observable_gauge(names::PROCESS_MEMORY_THRESHOLD)
		.with_description(
			"Configured SURREAL_MEMORY_THRESHOLD in bytes, or 0 when the memory guard is disabled",
		)
		.with_unit("By")
		.with_callback(|obs| {
			obs.observe(surrealdb_core::mem::memory_threshold() as i64, &[]);
		})
		.build();
	// Requests and queries the memory guard turned away, one series per
	// `RefusalSite`. A refusal short-circuits before dispatch, so this is the
	// only metric that moves when the guard sheds load. Monotonic for the
	// process lifetime.
	let _memory_refusals = meter
		.u64_observable_counter(names::PROCESS_MEMORY_REFUSALS)
		.with_description(
			"Requests and queries refused because tracked memory was beyond \
			 SURREAL_MEMORY_THRESHOLD, by the site that refused them",
		)
		.with_callback(|obs| {
			for site in RefusalSite::ALL {
				obs.observe(
					refusal_count(site),
					&[KeyValue::new(attrs::MEMORY_REFUSAL_SITE, site.as_label())],
				);
			}
		})
		.build();
	// How long the memory guard has been engaged without a break: 0 while it
	// is not. `/health` fails once this reaches
	// SURREAL_MEMORY_THRESHOLD_UNHEALTHY_AFTER. Reading it observes the guard,
	// as a `/health` probe does.
	let _memory_guard_engaged = meter
		.u64_observable_gauge(names::PROCESS_MEMORY_GUARD_ENGAGED)
		.with_description(
			"Seconds the memory guard has been refusing every request without a break, or 0 when \
			 tracked memory is below SURREAL_MEMORY_THRESHOLD",
		)
		.with_unit("s")
		.with_callback(|obs| {
			let engaged = crate::ntw::memory_guard::probe().unwrap_or_default();
			obs.observe(engaged.as_secs(), &[]);
		})
		.build();
	let _cpu = meter
		.f64_observable_gauge(names::PROCESS_CPU_PERCENT)
		.with_description(
			"Total CPU usage of the surreal process as a percentage (may exceed 100% on \
			 multi-core hosts)",
		)
		.with_callback(|obs| {
			let snap = process_snapshot();
			obs.observe(snap.cpu_percent as f64, &[]);
		})
		.build();
}

impl ExecutionObserver for MetricsObserver {
	fn on_statement_complete(&self, event: &StatementEvent) {
		self.gathered.gather(
			Labels {
				family: Family::Statement(event.safe.kind.as_label()),
				outcome: event.safe.outcome.as_label(),
				error_class: error_class_label(event.safe.error_class),
				namespace: event.ctx.namespace.as_deref(),
				database: event.ctx.database.as_deref(),
				user: event.ctx.user.as_deref(),
			},
			Some(event.safe.duration.as_secs_f64()),
			event.safe.result_rows,
		);
		if self.slow_query_threshold_ms > 0 {
			let duration_ms = event.safe.duration.as_millis() as u64;
			if duration_ms >= self.slow_query_threshold_ms {
				let attrs = [
					KeyValue::new(attrs::STATEMENT_TYPE, event.safe.kind.as_label()),
					KeyValue::new(attrs::OUTCOME, event.safe.outcome.as_label()),
					KeyValue::new(attrs::ERROR_CLASS, error_class_label(event.safe.error_class)),
					KeyValue::new(attrs::NAMESPACE, label_cow(event.ctx.namespace.as_deref())),
					KeyValue::new(attrs::DATABASE, label_cow(event.ctx.database.as_deref())),
					KeyValue::new(attrs::USER, label_cow(event.ctx.user.as_deref())),
				];
				// Slow-query counter intentionally re-uses the same labels
				// as `surrealdb.statement.*` so dashboards can join across
				// the two families on `statement_type` / `outcome` / tenant ctx.
				self.slow_query_total.add(1, &attrs);
			}
		}
	}

	fn on_query_complete(&self, event: &QueryEvent) {
		self.gathered.gather(
			Labels {
				family: Family::Query,
				outcome: event.safe.outcome.as_label(),
				error_class: error_class_label(event.safe.error_class),
				namespace: event.ctx.namespace.as_deref(),
				database: event.ctx.database.as_deref(),
				user: event.ctx.user.as_deref(),
			},
			Some(event.safe.duration.as_secs_f64()),
			0,
		);
	}

	fn on_transaction_complete(&self, event: &TransactionEvent) {
		self.gathered.gather(
			Labels {
				family: Family::Transaction(event.safe.write),
				outcome: event.safe.outcome.as_label(),
				error_class: error_class_label(event.safe.error_class),
				namespace: event.ctx.namespace.as_deref(),
				database: event.ctx.database.as_deref(),
				user: event.ctx.user.as_deref(),
			},
			Some(event.safe.duration.as_secs_f64()),
			0,
		);
		// Each commit-conflict failure is itself a retry trigger when
		// surrounded by an optimistic-retry loop, so the conflicts
		// counter doubles as the retry-pressure signal. Operators alert
		// on `rate(surrealdb_transaction_conflicts_total[5m])` rather
		// than a separate `retries` counter; per-transaction retry
		// counting does not fit the architecture (each optimistic
		// retry creates a fresh `Transaction` instance).
		if matches!(event.safe.error_class, Some(c) if c == super::instruments::error_class::TXN_CONFLICT)
		{
			// Drop the WRITE attr from conflicts so the series is keyed on
			// tenant ctx alone -- conflict rate is what operators alert on,
			// not whether the doomed txn was a write or read.
			let conflict_attrs = [
				KeyValue::new(attrs::NAMESPACE, label_cow(event.ctx.namespace.as_deref())),
				KeyValue::new(attrs::DATABASE, label_cow(event.ctx.database.as_deref())),
				KeyValue::new(attrs::USER, label_cow(event.ctx.user.as_deref())),
			];
			self.transaction_conflicts.add(1, &conflict_attrs);
		}
		self.transaction_tallies.record(event.safe.outcome, &event.safe.metrics);
	}

	fn on_rpc_complete(&self, event: &RpcEvent) {
		self.gathered.gather(
			Labels {
				family: Family::Rpc(event.safe.method.to_str()),
				outcome: event.safe.outcome.as_label(),
				error_class: error_class_label(event.safe.error_class),
				namespace: event.ctx.namespace.as_deref(),
				database: event.ctx.database.as_deref(),
				user: event.ctx.user.as_deref(),
			},
			Some(event.safe.duration.as_secs_f64()),
			0,
		);
	}

	fn on_auth_event(&self, event: &AuthEvent) {
		let attrs = [
			KeyValue::new(attrs::AUTH_ACTION, event.safe.action.as_label()),
			KeyValue::new(attrs::AUTH_SCOPE, event.safe.scope.as_label()),
			KeyValue::new(attrs::OUTCOME, event.safe.outcome.as_label()),
			KeyValue::new(attrs::ERROR_CLASS, error_class_label(event.safe.error_class)),
			KeyValue::new(attrs::NAMESPACE, label_cow(event.ctx.namespace.as_deref())),
			KeyValue::new(attrs::DATABASE, label_cow(event.ctx.database.as_deref())),
			KeyValue::new(attrs::USER, label_cow(event.ctx.user.as_deref())),
		];
		self.auth_total.add(1, &attrs);
	}

	fn on_session_event(&self, event: &SessionEvent) {
		let action = event.safe.action.as_label();
		let protocol = event.safe.protocol.as_label();
		let service = label_cow(event.ctx.service_name.as_deref());
		let total_attrs = [
			KeyValue::new(attrs::SESSION_ACTION, action),
			KeyValue::new(attrs::PROTOCOL, protocol),
			KeyValue::new(attrs::SERVICE, service.clone()),
		];
		self.session_total.add(1, &total_attrs);
		// Gauge attribute set is the protocol + service subset that is
		// stable across connect/disconnect so the `+1` and `-1` hit
		// the same series.
		let active_attrs = [
			KeyValue::new(attrs::PROTOCOL, protocol),
			KeyValue::new(attrs::SERVICE, service.clone()),
		];
		match event.safe.action {
			SessionAction::Connect => self.session_active.add(1, &active_attrs),
			SessionAction::Disconnect => {
				self.session_active.add(-1, &active_attrs);
				if let Some(d) = event.safe.duration {
					self.session_duration.record(d.as_secs_f64(), &active_attrs);
				}
			}
		}
	}

	fn on_network_bytes(&self, event: &NetworkBytesEvent) {
		if event.safe.bytes == 0 {
			return;
		}
		let protocol = event.safe.protocol.as_label();
		self.gathered.gather(
			Labels {
				family: match event.safe.direction {
					NetworkDirection::Received => Family::Received(protocol),
					NetworkDirection::Sent => Family::Sent(protocol),
				},
				outcome: NONE_LABEL,
				error_class: NONE_LABEL,
				namespace: event.ctx.namespace.as_deref(),
				database: event.ctx.database.as_deref(),
				user: event.ctx.user.as_deref(),
			},
			None,
			event.safe.bytes,
		);
	}

	fn on_bucket_operation(&self, event: &BucketOperationEvent) {
		let attrs = [
			KeyValue::new(attrs::BACKEND, event.safe.backend),
			KeyValue::new(attrs::BUCKET_OP, event.safe.op.as_label()),
			KeyValue::new(attrs::OUTCOME, event.safe.outcome.as_label()),
		];
		self.bucket_operations.add(1, &attrs);
		if event.safe.sent > 0 {
			self.bucket_sent.add(event.safe.sent, &attrs);
		}
		if event.safe.received > 0 {
			self.bucket_received.add(event.safe.received, &attrs);
		}
	}

	fn on_task_pass(&self, event: &TaskPassEvent) {
		let attrs = [
			KeyValue::new(attrs::JOB, event.safe.job),
			KeyValue::new(attrs::OUTCOME, event.safe.outcome.as_label()),
			KeyValue::new(attrs::LEASE, event.safe.lease.as_label()),
		];
		self.task_pass.add(1, &attrs);
		// Duration, throughput and lag are per job: the lease and outcome
		// labels would only split their distributions.
		let job = [KeyValue::new(attrs::JOB, event.safe.job)];
		self.task_pass_duration.record(event.safe.duration.as_secs_f64(), &job);
		if event.safe.items > 0 {
			self.task_items.add(event.safe.items, &job);
		}
		if let Some(age) = event.safe.oldest_age {
			self.task_queue_age.record(age.as_secs_f64(), &job);
		}
		if event.safe.backlog {
			self.task_backlog.add(1, &job);
		}
	}

	fn on_index_compaction_pass(&self, event: &IndexCompactionPassEvent) {
		let kind_label = event.safe.kind.map_or(NONE_LABEL, |k| k.as_label());
		let trigger_label = event.safe.trigger.as_label();
		let attrs = [
			KeyValue::new(attrs::INDEX_KIND, kind_label),
			KeyValue::new(attrs::COMPACTION_TRIGGER, trigger_label),
			KeyValue::new(attrs::OUTCOME, event.safe.outcome.as_label()),
		];
		self.index_compaction_pass.add(1, &attrs);
		self.index_compaction_pass_duration.record(event.safe.duration.as_secs_f64(), &attrs);
		if event.safe.unqueued {
			let unqueued_attrs = [
				KeyValue::new(attrs::INDEX_KIND, kind_label),
				KeyValue::new(attrs::COMPACTION_TRIGGER, trigger_label),
			];
			self.index_compaction_unqueued.add(1, &unqueued_attrs);
		}
	}

	fn on_index_compaction_plan(&self, event: &IndexCompactionPlanEvent) {
		let attrs = [
			KeyValue::new(attrs::INDEX_KIND, event.safe.kind.as_label()),
			KeyValue::new(attrs::COMPACTION_MODE, event.safe.mode.as_label()),
		];
		self.index_compaction_plans.add(1, &attrs);
		self.index_compaction_plan_duration.record(event.safe.duration.as_secs_f64(), &attrs);
		self.index_compaction_pending_keys_folded.add(event.safe.pending_entries, &attrs);
	}

	fn on_index_hnsw_cache_discard(&self, _event: &IndexHnswCacheDiscardEvent) {
		self.index_hnsw_cache_discards.add(1, &[]);
	}

	fn on_index_compaction_retry(&self, event: &IndexCompactionRetryEvent) {
		let attrs = [
			KeyValue::new(attrs::INDEX_KIND, event.safe.kind.as_label()),
			KeyValue::new(attrs::RETRY_REASON, event.safe.reason.as_label()),
		];
		self.index_compaction_retries.add(1, &attrs);
	}

	fn on_index_pending_read(&self, event: &IndexPendingReadEvent) {
		let attrs = [KeyValue::new(attrs::INDEX_KIND, event.safe.kind.as_label())];
		self.index_pending_keys_read.record(event.safe.keys as f64, &attrs);
	}

	fn on_http_request_started(&self, event: &HttpRequestStartEvent) {
		// Active-request gauge is attribute-stripped to method / route
		// so the `+1` here and the `-1` from completion stay balanced
		// across the gauge series. See the constructor for the rationale.
		let attrs = [
			KeyValue::new(attrs::HTTP_METHOD, event.safe.method.as_label()),
			KeyValue::new(attrs::HTTP_ROUTE, label_cow(event.safe.route)),
		];
		self.http_active_requests.add(1, &attrs);
	}

	fn on_http_request_complete(&self, event: &HttpRequestEvent) {
		let route = label_cow(event.safe.route);
		let active_attrs = [
			KeyValue::new(attrs::HTTP_METHOD, event.safe.method.as_label()),
			KeyValue::new(attrs::HTTP_ROUTE, route.clone()),
		];
		self.http_active_requests.add(-1, &active_attrs);

		let attrs = [
			KeyValue::new(attrs::HTTP_METHOD, event.safe.method.as_label()),
			KeyValue::new(attrs::HTTP_ROUTE, route),
			KeyValue::new(attrs::HTTP_STATUS_CODE, http_status(event.safe.status_code)),
			KeyValue::new(attrs::OUTCOME, event.safe.outcome.as_label()),
			KeyValue::new(attrs::ERROR_CLASS, error_class_label(event.safe.error_class)),
			KeyValue::new(attrs::NAMESPACE, label_cow(event.ctx.namespace.as_deref())),
			KeyValue::new(attrs::DATABASE, label_cow(event.ctx.database.as_deref())),
			KeyValue::new(attrs::USER, label_cow(event.ctx.user.as_deref())),
		];
		self.http_request_total.add(1, &attrs);
		self.http_request_duration.record(event.safe.duration.as_secs_f64(), &attrs);
		if let Some(bytes) = event.safe.request_size
			&& bytes > 0
		{
			self.http_request_size.add(bytes, &attrs);
		}
		if let Some(bytes) = event.safe.response_size
			&& bytes > 0
		{
			self.http_response_size.add(bytes, &attrs);
		}
	}

	fn needs_statement_text(&self) -> bool {
		false
	}
}

/// Construct a pre-built [`MetricsObserver`] wrapped in [`Arc`] suitable for
/// handing to the datastore builder.
pub fn into_arc_observer(observer: MetricsObserver) -> Arc<dyn ExecutionObserver> {
	Arc::new(observer)
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;
	use std::time::Duration;

	use opentelemetry_sdk::error::OTelSdkError;
	use opentelemetry_sdk::metrics::data::ResourceMetrics;
	use opentelemetry_sdk::metrics::reader::MetricReader;
	use opentelemetry_sdk::metrics::{InstrumentKind, ManualReader, SdkMeterProvider, Temporality};
	use surrealdb_observe::{
		AuthAction, AuthEvent, AuthEventCtx, AuthEventSafe, AuthScope, ExecutionObserver,
		HttpMethod, HttpRequestEvent, HttpRequestEventCtx, HttpRequestEventSafe,
		HttpRequestStartEvent, HttpRequestStartEventSafe, HttpVersion, NetworkBytesEvent,
		NetworkBytesEventCtx, NetworkBytesEventSafe, NetworkDirection, Outcome, QueryCounters,
		QueryEvent, QueryEventCtx, QueryEventSafe, RpcEvent, RpcEventCtx, RpcEventSafe,
		SessionAction, SessionEvent, SessionEventCtx, SessionEventSafe, SessionProtocol,
		StatementEvent, StatementEventCtx, StatementEventSafe, StatementType, TransactionEvent,
		TransactionEventCtx, TransactionEventSafe, TransactionMetricsSnapshot,
	};

	use super::*;

	#[derive(Clone, Debug)]
	struct TestReader {
		inner: Arc<ManualReader>,
	}

	impl MetricReader for TestReader {
		fn register_pipeline(
			&self,
			pipeline: std::sync::Weak<opentelemetry_sdk::metrics::Pipeline>,
		) {
			self.inner.register_pipeline(pipeline);
		}
		fn collect(&self, rm: &mut ResourceMetrics) -> Result<(), OTelSdkError> {
			self.inner.collect(rm)
		}
		fn force_flush(&self) -> Result<(), OTelSdkError> {
			self.inner.force_flush()
		}
		fn shutdown(&self) -> Result<(), OTelSdkError> {
			self.inner.shutdown()
		}
		fn shutdown_with_timeout(&self, timeout: Duration) -> Result<(), OTelSdkError> {
			self.inner.shutdown_with_timeout(timeout)
		}
		fn temporality(&self, kind: InstrumentKind) -> Temporality {
			self.inner.temporality(kind)
		}
	}

	/// Build a fresh runtime with an isolated [`ManualReader`] so each
	/// test sees only its own recordings without touching any global
	/// state. Returns the provider (so the test can call `shutdown()`),
	/// the reader (for collection), and the runtime (for the observer).
	fn fresh_runtime() -> (SdkMeterProvider, TestReader, ObservabilityRuntime) {
		let reader = TestReader {
			inner: Arc::new(ManualReader::builder().build()),
		};
		let provider = SdkMeterProvider::builder().with_reader(reader.clone()).build();
		let runtime = ObservabilityRuntime::builder(provider.clone()).build();
		(provider, reader, runtime)
	}

	/// Snapshot every metric name observed under any scope.
	fn collect_names(reader: &TestReader) -> Vec<(String, String)> {
		let mut rm = ResourceMetrics::default();
		reader.collect(&mut rm).expect("collect");
		rm.scope_metrics()
			.flat_map(|sm| {
				let scope = sm.scope().name().to_string();
				sm.metrics().map(move |m| (scope.clone(), m.name().to_string()))
			})
			.collect()
	}

	/// Sum every `i64::Sum` data-point on `metric_name` across every
	/// scope. Used to assert balanced gauges return to zero.
	fn sum_i64(reader: &TestReader, metric_name: &str) -> (i64, usize) {
		let mut rm = ResourceMetrics::default();
		reader.collect(&mut rm).expect("collect");
		let mut total: i64 = 0;
		let mut points = 0usize;
		for sm in rm.scope_metrics() {
			for metric in sm.metrics() {
				if metric.name() != metric_name {
					continue;
				}
				if let opentelemetry_sdk::metrics::data::AggregatedMetrics::I64(
					opentelemetry_sdk::metrics::data::MetricData::Sum(sum),
				) = metric.data()
				{
					for dp in sum.data_points() {
						total += dp.value();
						points += 1;
					}
				}
			}
		}
		(total, points)
	}

	/// The `site`-labelled data-points of the memory refusal counter.
	fn memory_refusals_by_site(reader: &TestReader) -> std::collections::BTreeMap<String, u64> {
		use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
		let mut rm = ResourceMetrics::default();
		reader.collect(&mut rm).expect("collect");
		let mut out = std::collections::BTreeMap::new();
		for sm in rm.scope_metrics() {
			for metric in sm.metrics() {
				if metric.name() != names::PROCESS_MEMORY_REFUSALS {
					continue;
				}
				let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() else {
					panic!("{} is not a u64 sum: {:?}", metric.name(), metric.data());
				};
				assert!(sum.is_monotonic(), "memory refusals must be a counter");
				for dp in sum.data_points() {
					let site = dp
						.attributes()
						.find(|kv| kv.key.as_str() == attrs::MEMORY_REFUSAL_SITE)
						.map(|kv| kv.value.as_str().into_owned())
						.expect("site attribute");
					out.insert(site, dp.value());
				}
			}
		}
		out
	}

	/// Every refusal recorded at a site shows up on that site's series of the
	/// counter, and every site is published even before it has refused anything.
	#[tokio::test]
	async fn memory_refusals_are_published_per_site() {
		use surrealdb_core::mem::record_refusal;

		let (provider, reader, runtime) = fresh_runtime();
		register_process_metrics(&runtime);
		let before = memory_refusals_by_site(&reader);
		assert_eq!(
			before.keys().map(String::as_str).collect::<Vec<_>>(),
			["http", "query", "websocket"],
		);
		record_refusal(RefusalSite::Websocket);
		record_refusal(RefusalSite::Http);
		record_refusal(RefusalSite::Http);
		let after = memory_refusals_by_site(&reader);
		// The counters are process-wide, so other tests may add to them too.
		assert!(after["websocket"] > before["websocket"]);
		assert!(after["http"] >= before["http"] + 2);
		provider.shutdown().expect("shutdown");
	}

	/// The engaged-for gauge is published, and reads 0 while the guard is not
	/// engaged.
	#[tokio::test]
	async fn memory_guard_engaged_gauge_is_published() {
		let (provider, reader, runtime) = fresh_runtime();
		register_process_metrics(&runtime);
		let names_seen = collect_names(&reader);
		assert!(
			names_seen.iter().any(|(_, n)| n == names::PROCESS_MEMORY_GUARD_ENGAGED),
			"{names_seen:?}"
		);
		provider.shutdown().expect("shutdown");
	}

	/// Walk every data-point on every metric in every scope looking for
	/// an attribute whose value matches `needle`. Used as a tenant-canary
	/// leak check.
	fn any_attr_value_contains(reader: &TestReader, needle: &str) -> bool {
		use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
		let mut rm = ResourceMetrics::default();
		reader.collect(&mut rm).expect("collect");
		for sm in rm.scope_metrics() {
			for metric in sm.metrics() {
				match metric.data() {
					AggregatedMetrics::U64(MetricData::Sum(s)) => {
						for dp in s.data_points() {
							if dp.attributes().any(|kv| kv.value.as_str().contains(needle)) {
								return true;
							}
						}
					}
					AggregatedMetrics::I64(MetricData::Sum(s)) => {
						for dp in s.data_points() {
							if dp.attributes().any(|kv| kv.value.as_str().contains(needle)) {
								return true;
							}
						}
					}
					AggregatedMetrics::F64(MetricData::Histogram(h)) => {
						for dp in h.data_points() {
							if dp.attributes().any(|kv| kv.value.as_str().contains(needle)) {
								return true;
							}
						}
					}
					AggregatedMetrics::F64(_)
					| AggregatedMetrics::U64(_)
					| AggregatedMetrics::I64(_) => {}
				}
			}
		}
		false
	}

	fn statement_event(
		kind: StatementType,
		ns: Option<&str>,
		user: Option<&str>,
		result_rows: u64,
		outcome: Outcome,
	) -> StatementEvent {
		StatementEvent {
			safe: StatementEventSafe {
				kind,
				outcome,
				duration: Duration::from_millis(3),
				read_only: true,
				result_rows,
				error_class: None,
			},
			ctx: StatementEventCtx {
				sql: None,
				namespace: ns.map(String::from),
				database: Some("main".into()),
				user: user.map(String::from),
				..Default::default()
			},
		}
	}

	#[tokio::test]
	async fn statement_records_under_signal_domain_scope() {
		let (provider, reader, runtime) = fresh_runtime();
		let obs = MetricsObserver::new(&runtime).expect("observer");
		obs.on_statement_complete(&statement_event(
			StatementType::Select,
			Some("acme"),
			Some("alice"),
			17,
			Outcome::Success,
		));
		let names = collect_names(&reader);
		// Statement instruments live on the `surrealdb.statement` scope.
		for (instrument, expected_scope) in [
			(names::STATEMENT_TOTAL, scope::STATEMENT),
			(names::STATEMENT_DURATION, scope::STATEMENT),
			(names::STATEMENT_ROWS, scope::STATEMENT),
		] {
			assert!(
				names.iter().any(|(s, n)| s == expected_scope && n == instrument),
				"missing `{instrument}` under `{expected_scope}`: {names:?}",
			);
		}
		provider.shutdown().expect("shutdown");
	}

	/// Measurements gathered past a shard's hand-over and those still held when
	/// the metrics are collected all reach the instruments, each once.
	#[tokio::test]
	async fn gathered_measurements_reach_the_instruments_once() {
		use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
		let (provider, reader, runtime) = fresh_runtime();
		let obs = MetricsObserver::new(&runtime).expect("observer");
		let made = 3 * GATHER_HOLD as u64 + 7;
		for _ in 0..made {
			obs.on_statement_complete(&statement_event(
				StatementType::Select,
				Some("acme"),
				Some("alice"),
				2,
				Outcome::Success,
			));
		}
		let mut rm = ResourceMetrics::default();
		reader.collect(&mut rm).expect("collect");
		let (mut total, mut rows, mut recorded) = (0, 0, 0);
		for sm in rm.scope_metrics() {
			for metric in sm.metrics() {
				let name = metric.name();
				if let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() {
					let sum = sum.data_points().map(|dp| dp.value()).sum::<u64>();
					if name == names::STATEMENT_TOTAL {
						total += sum;
					} else if name == names::STATEMENT_ROWS {
						rows += sum;
					}
				} else if let AggregatedMetrics::F64(MetricData::Histogram(histogram)) =
					metric.data() && name == names::STATEMENT_DURATION
				{
					recorded += histogram.data_points().map(|dp| dp.count()).sum::<u64>();
				}
			}
		}
		assert_eq!(total, made);
		assert_eq!(rows, 2 * made);
		assert_eq!(recorded, made);
		provider.shutdown().expect("shutdown");
	}

	#[tokio::test]
	async fn statement_rows_only_increment_when_non_zero() {
		use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
		let (provider, reader, runtime) = fresh_runtime();
		let obs = MetricsObserver::new(&runtime).expect("observer");
		obs.on_statement_complete(&statement_event(
			StatementType::Select,
			Some("acme"),
			Some("alice"),
			17,
			Outcome::Success,
		));
		// DEFINE statements emit zero rows and must not materialise a
		// data point on the rows family.
		obs.on_statement_complete(&statement_event(
			StatementType::Define,
			Some("acme"),
			Some("alice"),
			0,
			Outcome::Success,
		));
		let mut rm = ResourceMetrics::default();
		reader.collect(&mut rm).expect("collect");
		let mut found_rows_family = false;
		for sm in rm.scope_metrics() {
			for metric in sm.metrics() {
				if metric.name() != names::STATEMENT_ROWS {
					continue;
				}
				found_rows_family = true;
				if let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() {
					for dp in sum.data_points() {
						let kind = dp
							.attributes()
							.find(|kv| kv.key.as_str() == attrs::STATEMENT_TYPE)
							.map(|kv| kv.value.as_str().to_string());
						assert_ne!(
							kind.as_deref(),
							Some("define"),
							"non-DML statement leaked into rows family: {dp:?}",
						);
					}
				}
			}
		}
		assert!(found_rows_family, "rows family did not materialise");
		provider.shutdown().expect("shutdown");
	}

	#[tokio::test]
	async fn missing_context_collapses_to_sentinel() {
		use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
		let (provider, reader, runtime) = fresh_runtime();
		let obs = MetricsObserver::new(&runtime).expect("observer");
		obs.on_statement_complete(&statement_event(
			StatementType::Create,
			None,
			None,
			0,
			Outcome::Success,
		));
		let mut rm = ResourceMetrics::default();
		reader.collect(&mut rm).expect("collect");
		let mut saw_sentinel = false;
		for sm in rm.scope_metrics() {
			for metric in sm.metrics() {
				if metric.name() != names::STATEMENT_TOTAL {
					continue;
				}
				if let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() {
					for dp in sum.data_points() {
						let ns = dp
							.attributes()
							.find(|kv| kv.key.as_str() == attrs::NAMESPACE)
							.map(|kv| kv.value.as_str().to_string());
						let user = dp
							.attributes()
							.find(|kv| kv.key.as_str() == attrs::USER)
							.map(|kv| kv.value.as_str().to_string());
						if ns.as_deref() == Some(NONE_LABEL) && user.as_deref() == Some(NONE_LABEL)
						{
							saw_sentinel = true;
						}
					}
				}
			}
		}
		assert!(saw_sentinel, "sentinel attributes did not appear");
		provider.shutdown().expect("shutdown");
	}

	#[tokio::test]
	async fn transaction_records_under_signal_domain_scope() {
		let (provider, reader, runtime) = fresh_runtime();
		let obs = MetricsObserver::new(&runtime).expect("observer");
		obs.on_transaction_complete(&TransactionEvent {
			safe: TransactionEventSafe {
				outcome: Outcome::Success,
				write: true,
				duration: Duration::from_millis(7),
				metrics: TransactionMetricsSnapshot {
					keys_read: 12,
					keys_written: 5,
					key_bytes_read: 100,
					value_bytes_read: 1948,
					key_bytes_written: 50,
					value_bytes_written: 974,
					total_bytes_read: 2048,
					total_bytes_written: 1024,
					ops_get: 4,
					ops_scan: 1,
					ops_put: 0,
					ops_set: 5,
					ops_del: 0,
					ops_total: 10,
				},
				error_class: None,
			},
			ctx: TransactionEventCtx::default(),
		});
		let names_seen = collect_names(&reader);
		for expected in [
			names::TRANSACTION_TOTAL,
			names::TRANSACTION_KEYS_READ,
			names::TRANSACTION_KEY_BYTES_WRITTEN,
			names::TRANSACTION_VALUE_BYTES_WRITTEN,
			names::TRANSACTION_KV_OPS,
		] {
			assert!(
				names_seen.iter().any(|(s, n)| s == scope::TRANSACTION && n == expected),
				"missing `{expected}` under `{}`: {names_seen:?}",
				scope::TRANSACTION,
			);
		}
		provider.shutdown().expect("shutdown");
	}

	/// The `attr`-labelled data-points of the u64 counter `metric_name`, by the value of the
	/// attribute.
	fn u64_sums_by(
		reader: &TestReader,
		metric_name: &str,
		attr: &str,
	) -> std::collections::BTreeMap<String, u64> {
		use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
		let mut rm = ResourceMetrics::default();
		reader.collect(&mut rm).expect("collect");
		let mut out = std::collections::BTreeMap::new();
		for sm in rm.scope_metrics() {
			for metric in sm.metrics() {
				if metric.name() != metric_name {
					continue;
				}
				let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() else {
					panic!("{} is not a u64 sum: {:?}", metric.name(), metric.data());
				};
				assert!(sum.is_monotonic(), "{metric_name} is a counter");
				for dp in sum.data_points() {
					let label = dp
						.attributes()
						.find(|kv| kv.key.as_str() == attr)
						.map(|kv| kv.value.to_string())
						.unwrap_or_default();
					*out.entry(label).or_default() += dp.value();
				}
			}
		}
		out
	}

	/// The per-transaction KV counts are the sums of what every transaction reported, by op and
	/// by outcome, from whichever thread reported it.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn transaction_kv_counts_are_the_sums_of_every_transaction() {
		let (provider, reader, runtime) = fresh_runtime();
		let obs = Arc::new(MetricsObserver::new(&runtime).expect("observer"));
		let event = |outcome, keys_written, ops_set| TransactionEvent {
			safe: TransactionEventSafe {
				outcome,
				write: true,
				duration: Duration::from_millis(1),
				metrics: TransactionMetricsSnapshot {
					keys_read: 2,
					keys_written,
					key_bytes_written: 10 * keys_written,
					ops_get: 2,
					ops_set,
					..Default::default()
				},
				error_class: None,
			},
			ctx: TransactionEventCtx::default(),
		};
		let mut tasks = Vec::new();
		for i in 0..64u64 {
			let obs = Arc::clone(&obs);
			let outcome = if i % 4 == 0 {
				Outcome::Error
			} else {
				Outcome::Success
			};
			let event = event(outcome, 1, 3);
			tasks.push(tokio::spawn(async move { obs.on_transaction_complete(&event) }));
		}
		for task in tasks {
			task.await.unwrap();
		}
		let ops = u64_sums_by(&reader, names::TRANSACTION_KV_OPS, attrs::KV_OP);
		assert_eq!(ops.get("get"), Some(&128));
		assert_eq!(ops.get("set"), Some(&192));
		assert_eq!(ops.get("del"), Some(&0));
		let written = u64_sums_by(&reader, names::TRANSACTION_KEYS_WRITTEN, attrs::OUTCOME);
		assert_eq!(written.get("success"), Some(&48));
		assert_eq!(written.get("error"), Some(&16));
		assert_eq!(written.get("cancelled"), None, "a count never added to is not reported");
		let bytes = u64_sums_by(&reader, names::TRANSACTION_KEY_BYTES_WRITTEN, attrs::OUTCOME);
		assert_eq!(bytes.values().sum::<u64>(), 640);
		assert!(
			u64_sums_by(&reader, names::TRANSACTION_VALUE_BYTES_READ, attrs::OUTCOME).is_empty()
		);
		provider.shutdown().expect("shutdown");
	}

	#[tokio::test]
	async fn rpc_query_auth_record_under_their_scopes() {
		let (provider, reader, runtime) = fresh_runtime();
		let obs = MetricsObserver::new(&runtime).expect("observer");
		obs.on_query_complete(&QueryEvent {
			safe: QueryEventSafe {
				outcome: Outcome::Success,
				duration: Duration::from_millis(2),
				counters: QueryCounters {
					total: 3,
					ok: 3,
					err: 0,
				},
				error_class: None,
			},
			ctx: QueryEventCtx {
				namespace: Some("acme".into()),
				database: Some("prod".into()),
				user: Some("svc".into()),
				..Default::default()
			},
		});
		obs.on_rpc_complete(&RpcEvent {
			safe: RpcEventSafe {
				method: surrealdb_rpc::Method::Select,
				outcome: Outcome::Success,
				duration: Duration::from_millis(1),
				error_class: None,
			},
			ctx: RpcEventCtx {
				user: Some("svc".into()),
				..Default::default()
			},
		});
		obs.on_auth_event(&AuthEvent {
			safe: AuthEventSafe {
				action: AuthAction::Signin,
				scope: AuthScope::Root,
				outcome: Outcome::Error,
				error_class: None,
			},
			ctx: AuthEventCtx {
				user: Some("alice".into()),
				..Default::default()
			},
		});
		let names_seen = collect_names(&reader);
		for (instrument, expected_scope) in [
			(names::QUERY_TOTAL, scope::QUERY),
			(names::RPC_TOTAL, scope::RPC),
			(names::AUTH_TOTAL, scope::AUTH),
		] {
			assert!(
				names_seen.iter().any(|(s, n)| s == expected_scope && n == instrument),
				"missing `{instrument}` under `{expected_scope}`: {names_seen:?}",
			);
		}
		provider.shutdown().expect("shutdown");
	}

	#[tokio::test]
	async fn session_active_gauge_balances_on_connect_disconnect() {
		let (provider, reader, runtime) = fresh_runtime();
		let obs = MetricsObserver::new(&runtime).expect("observer");
		obs.on_session_event(&SessionEvent {
			safe: SessionEventSafe {
				action: SessionAction::Connect,
				protocol: SessionProtocol::WebSocket,
				duration: None,
			},
			ctx: SessionEventCtx {
				service_name: Some("orders".into()),
				..Default::default()
			},
		});
		obs.on_session_event(&SessionEvent {
			safe: SessionEventSafe {
				action: SessionAction::Disconnect,
				protocol: SessionProtocol::WebSocket,
				duration: Some(Duration::from_secs(30)),
			},
			ctx: SessionEventCtx {
				service_name: Some("orders".into()),
				..Default::default()
			},
		});
		let (sum, points) = sum_i64(&reader, names::SESSION_ACTIVE);
		assert_eq!(
			sum, 0,
			"session.active drifted to {sum} after balanced connect/disconnect over {points} \
			 data point(s)",
		);
		provider.shutdown().expect("shutdown");
	}

	#[tokio::test]
	async fn http_active_requests_balances_with_default_start_ctx() {
		// The dispatcher fires the start event with `HttpRequestEventCtx::default()`
		// (auth has not run yet). The complete event then carries the
		// populated tenant ctx. The gauge MUST be attribute-stripped to
		// the safe-half subset (method / route) so `+1` and `-1` hit
		// the same series.
		let (provider, reader, runtime) = fresh_runtime();
		let obs = MetricsObserver::new(&runtime).expect("observer");
		obs.on_http_request_started(&HttpRequestStartEvent {
			safe: HttpRequestStartEventSafe {
				method: HttpMethod::Get,
				route: Some("/sql"),
				version: HttpVersion::Http11,
			},
			ctx: HttpRequestEventCtx::default(),
		});
		obs.on_http_request_complete(&HttpRequestEvent {
			safe: HttpRequestEventSafe {
				method: HttpMethod::Get,
				route: Some("/sql"),
				status_code: Some(200),
				version: HttpVersion::Http11,
				outcome: Outcome::Success,
				duration: Duration::from_millis(2),
				request_size: Some(64),
				response_size: Some(128),
				error_class: None,
			},
			ctx: HttpRequestEventCtx {
				namespace: Some("acme".into()),
				database: Some("prod".into()),
				user: Some("alice".into()),
				..Default::default()
			},
		});
		let (sum, points) = sum_i64(&reader, names::HTTP_ACTIVE_REQUESTS);
		assert_eq!(
			sum, 0,
			"http.active_requests drifted to {sum} (attribute imbalance) across {points} data \
			 point(s)",
		);
		// Gauge must collapse to a single time series — anything else
		// signals tenant attrs leaked onto the +1 / -1 sides.
		assert_eq!(
			points, 1,
			"http.active_requests must collect to a single time series, got {points}",
		);
		provider.shutdown().expect("shutdown");
	}

	#[tokio::test]
	async fn network_bytes_record_with_tenant_dimensions() {
		let (provider, reader, runtime) = fresh_runtime();
		let obs = MetricsObserver::new(&runtime).expect("observer");
		obs.on_network_bytes(&NetworkBytesEvent {
			safe: NetworkBytesEventSafe {
				direction: NetworkDirection::Received,
				protocol: SessionProtocol::Http,
				bytes: 1_024,
			},
			ctx: NetworkBytesEventCtx {
				namespace: Some("acme".into()),
				database: Some("prod".into()),
				user: Some("svc".into()),
			},
		});
		let names_seen = collect_names(&reader);
		assert!(
			names_seen.iter().any(|(s, n)| s == scope::NETWORK && n == names::NETWORK_RECEIVED),
			"missing network.received under `{}`: {names_seen:?}",
			scope::NETWORK,
		);
		assert!(any_attr_value_contains(&reader, "acme"));
		provider.shutdown().expect("shutdown");
	}

	#[tokio::test]
	async fn zero_byte_network_event_is_ignored() {
		let (provider, reader, runtime) = fresh_runtime();
		let obs = MetricsObserver::new(&runtime).expect("observer");
		obs.on_network_bytes(&NetworkBytesEvent {
			safe: NetworkBytesEventSafe {
				direction: NetworkDirection::Received,
				protocol: SessionProtocol::Http,
				bytes: 0,
			},
			ctx: NetworkBytesEventCtx::default(),
		});
		let names_seen = collect_names(&reader);
		assert!(
			!names_seen.iter().any(|(_, n)| n == names::NETWORK_RECEIVED),
			"zero-byte event materialised an instrument: {names_seen:?}",
		);
		provider.shutdown().expect("shutdown");
	}

	/// Tenant canary leak guard: drive every event variant with a
	/// canary identifier in every `*Ctx` field, and assert the canary
	/// only appears on labelled instrument families. The HTTP active
	/// request gauge is attribute-stripped to method / route only and
	/// must NEVER carry the canary.
	#[tokio::test]
	async fn tenant_canary_does_not_leak_into_http_active_requests() {
		const CANARY: &str = "xCANARYx-tenant-secret-xyz";
		let (provider, reader, runtime) = fresh_runtime();
		let obs = MetricsObserver::new(&runtime).expect("observer");
		obs.on_statement_complete(&StatementEvent {
			safe: StatementEventSafe {
				kind: StatementType::Select,
				outcome: Outcome::Success,
				duration: Duration::from_millis(2),
				read_only: true,
				result_rows: 1,
				error_class: None,
			},
			ctx: StatementEventCtx {
				sql: Some(format!("SELECT * FROM {CANARY}")),
				namespace: Some(CANARY.into()),
				database: Some(CANARY.into()),
				user: Some(CANARY.into()),
				..Default::default()
			},
		});
		obs.on_http_request_started(&HttpRequestStartEvent {
			safe: HttpRequestStartEventSafe {
				method: HttpMethod::Get,
				route: Some("/sql"),
				version: HttpVersion::Http11,
			},
			ctx: HttpRequestEventCtx {
				namespace: Some(CANARY.into()),
				..Default::default()
			},
		});
		obs.on_http_request_complete(&HttpRequestEvent {
			safe: HttpRequestEventSafe {
				method: HttpMethod::Get,
				route: Some("/sql"),
				status_code: Some(200),
				version: HttpVersion::Http11,
				outcome: Outcome::Success,
				duration: Duration::from_millis(2),
				request_size: Some(64),
				response_size: Some(128),
				error_class: None,
			},
			ctx: HttpRequestEventCtx {
				namespace: Some(CANARY.into()),
				database: Some(CANARY.into()),
				user: Some(CANARY.into()),
				..Default::default()
			},
		});

		// The canary lands on labelled families like `surrealdb.statement`
		// by design (those families require root auth at `/metrics`).
		assert!(
			any_attr_value_contains(&reader, CANARY),
			"tenant canary should appear on labelled families",
		);

		// But it must NEVER appear on the active-request gauge, which
		// is attribute-stripped to method / route only.
		use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
		let mut rm = ResourceMetrics::default();
		reader.collect(&mut rm).expect("collect");
		for sm in rm.scope_metrics() {
			for metric in sm.metrics() {
				if metric.name() != names::HTTP_ACTIVE_REQUESTS {
					continue;
				}
				if let AggregatedMetrics::I64(MetricData::Sum(sum)) = metric.data() {
					for dp in sum.data_points() {
						let leaked = dp.attributes().any(|kv| kv.value.as_str().contains(CANARY));
						assert!(!leaked, "tenant canary leaked onto http.active_requests: {dp:?}",);
					}
				}
			}
		}
		provider.shutdown().expect("shutdown");
	}

	/// A queue age is recorded against boundaries of its own: the default
	/// ones would put every age under five seconds in a single bucket.
	#[tokio::test]
	async fn task_queue_age_records_against_its_own_buckets() {
		use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
		use surrealdb_observe::{
			TaskLeaseState, TaskPassEvent, TaskPassEventCtx, TaskPassEventSafe,
		};
		let (provider, reader, runtime) = fresh_runtime();
		let obs = MetricsObserver::new(&runtime).expect("observer");
		obs.on_task_pass(&TaskPassEvent {
			safe: TaskPassEventSafe {
				job: "event_processing",
				outcome: Outcome::Success,
				duration: Duration::from_millis(3),
				items: 1,
				oldest_age: Some(Duration::from_millis(400)),
				lease: TaskLeaseState::Held,
				backlog: false,
			},
			ctx: TaskPassEventCtx {},
		});
		let mut rm = ResourceMetrics::default();
		reader.collect(&mut rm).expect("collect");
		let bounds: Vec<f64> = rm
			.scope_metrics()
			.flat_map(|sm| sm.metrics())
			.filter(|m| m.name() == names::TASK_QUEUE_AGE)
			.find_map(|m| match m.data() {
				AggregatedMetrics::F64(MetricData::Histogram(h)) => {
					h.data_points().next().map(|dp| dp.bounds().collect())
				}
				AggregatedMetrics::F64(_)
				| AggregatedMetrics::U64(_)
				| AggregatedMetrics::I64(_) => None,
			})
			.expect("the queue age is recorded");
		assert_eq!(bounds, TASK_QUEUE_AGE_BUCKETS);
		provider.shutdown().expect("shutdown");
	}

	#[tokio::test]
	async fn index_compaction_and_pending_read_record_under_index_scope() {
		use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
		use surrealdb_observe::{
			IndexCompactionKind, IndexCompactionPassEvent, IndexCompactionPassEventCtx,
			IndexCompactionPassEventSafe, IndexCompactionPassOutcome, IndexCompactionPlanEvent,
			IndexCompactionPlanEventCtx, IndexCompactionPlanEventSafe, IndexCompactionPlanMode,
			IndexCompactionRetryEvent, IndexCompactionRetryEventCtx, IndexCompactionRetryEventSafe,
			IndexCompactionRetryReason, IndexCompactionTrigger, IndexHnswCacheDiscardEvent,
			IndexPendingReadEvent, IndexPendingReadEventCtx, IndexPendingReadEventSafe,
		};
		let (provider, reader, runtime) = fresh_runtime();
		let obs = MetricsObserver::new(&runtime).expect("observer");
		obs.on_index_compaction_pass(&IndexCompactionPassEvent {
			safe: IndexCompactionPassEventSafe {
				kind: Some(IndexCompactionKind::Hnsw),
				trigger: IndexCompactionTrigger::Queue,
				outcome: IndexCompactionPassOutcome::Short,
				duration: Duration::from_millis(12),
				unqueued: true,
			},
			ctx: IndexCompactionPassEventCtx::default(),
		});
		obs.on_index_compaction_plan(&IndexCompactionPlanEvent {
			safe: IndexCompactionPlanEventSafe {
				kind: IndexCompactionKind::Hnsw,
				mode: IndexCompactionPlanMode::Staged,
				duration: Duration::from_millis(8),
				pending_entries: 42,
			},
			ctx: IndexCompactionPlanEventCtx::default(),
		});
		obs.on_index_hnsw_cache_discard(&IndexHnswCacheDiscardEvent::default());
		obs.on_index_compaction_retry(&IndexCompactionRetryEvent {
			safe: IndexCompactionRetryEventSafe {
				kind: IndexCompactionKind::Hnsw,
				reason: IndexCompactionRetryReason::TxnConflict,
			},
			ctx: IndexCompactionRetryEventCtx::default(),
		});
		obs.on_index_pending_read(&IndexPendingReadEvent {
			safe: IndexPendingReadEventSafe {
				kind: IndexCompactionKind::Hnsw,
				keys: 16,
			},
			ctx: IndexPendingReadEventCtx::default(),
		});

		let names_seen = collect_names(&reader);
		for expected in [
			names::INDEX_COMPACTION_PASS,
			names::INDEX_COMPACTION_PASS_DURATION,
			names::INDEX_COMPACTION_PLANS,
			names::INDEX_COMPACTION_PLAN_DURATION,
			names::INDEX_COMPACTION_PENDING_KEYS_FOLDED,
			names::INDEX_COMPACTION_RETRIES,
			names::INDEX_COMPACTION_UNQUEUED,
			names::INDEX_HNSW_CACHE_DISCARDS,
			names::INDEX_PENDING_KEYS_READ,
		] {
			assert!(
				names_seen.iter().any(|(s, n)| s == scope::INDEX && n == expected),
				"missing `{expected}` under `{}`: {names_seen:?}",
				scope::INDEX,
			);
		}

		let mut rm = ResourceMetrics::default();
		reader.collect(&mut rm).expect("collect");
		let counter_sum = |name: &str| -> u64 {
			rm.scope_metrics()
				.flat_map(|sm| sm.metrics())
				.filter(|m| m.name() == name)
				.find_map(|m| match m.data() {
					AggregatedMetrics::U64(MetricData::Sum(s)) => {
						Some(s.data_points().map(|dp| dp.value()).sum())
					}
					AggregatedMetrics::F64(_)
					| AggregatedMetrics::U64(_)
					| AggregatedMetrics::I64(_) => None,
				})
				.expect("counter is recorded")
		};
		assert_eq!(counter_sum(names::INDEX_COMPACTION_PLANS), 1);
		assert_eq!(counter_sum(names::INDEX_HNSW_CACHE_DISCARDS), 1);
		assert_eq!(counter_sum(names::INDEX_COMPACTION_PENDING_KEYS_FOLDED), 42);
		let bounds: Vec<f64> = rm
			.scope_metrics()
			.flat_map(|sm| sm.metrics())
			.filter(|m| m.name() == names::INDEX_PENDING_KEYS_READ)
			.find_map(|m| match m.data() {
				AggregatedMetrics::F64(MetricData::Histogram(h)) => {
					h.data_points().next().map(|dp| dp.bounds().collect())
				}
				AggregatedMetrics::F64(_)
				| AggregatedMetrics::U64(_)
				| AggregatedMetrics::I64(_) => None,
			})
			.expect("pending_keys_read histogram is recorded");
		assert_eq!(bounds, INDEX_PENDING_KEYS_READ_BUCKETS);
		provider.shutdown().expect("shutdown");
	}
}
