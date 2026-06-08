//! Server-side composer extension for the unified observability pipeline.
//!
//! Core deliberately has no `prometheus` / `opentelemetry` dependencies,
//! so server-only hooks cannot ride on
//! [`surrealdb_core::observe::ObservabilityProvider`]. The server crate
//! therefore exposes its own [`ObservabilityProvider`] trait that
//! supertraits the core one and adds optional accessors used by the unified
//! `SdkMeterProvider` setup at startup. Every method defaults to [`None`]
//! so community composers (and any external composer pre-dating these
//! methods) keep working unchanged.

use std::sync::Arc;

use surrealdb_core::CommunityComposer;
use surrealdb_core::observe::ExecutionObserver;

use super::runtime::ObservabilityRuntime;

/// Read-only view of a record-pipeline's queue counters, exposed through
/// the unified [`opentelemetry_sdk::metrics::SdkMeterProvider`] as
/// aggregate self-metrics.
///
/// Generic over the pipeline kind: the audit log and the slow-query log
/// both implement this so the same registration helper can be used for
/// both, parameterised by a metric-name prefix. Composers that don't run
/// any pipeline return `None` from
/// [`ObservabilityProvider::audit_counters`] /
/// [`ObservabilityProvider::slow_query_counters`] and the community
/// surface stays unchanged.
pub trait PipelineCounters: Send + Sync + 'static {
	/// Total records successfully enqueued (before sink writes).
	fn records_total(&self) -> u64;
	/// Total records dropped on overflow.
	fn dropped_total(&self) -> u64;
	/// Current queue depth in records.
	fn queue_depth(&self) -> i64;
	/// Total records the worker successfully wrote to the sink. The gap
	/// between `records_total` and `appended_total` is either in-flight
	/// records still in the queue or records that the worker failed to
	/// append (see [`Self::append_errors_total`]).
	fn appended_total(&self) -> u64 {
		0
	}
	/// Total append failures observed by the worker. Compliance operators
	/// should alert on any non-zero value here — it indicates a record was
	/// lost between the queue and disk.
	fn append_errors_total(&self) -> u64 {
		0
	}
}

/// Server-side composer extension supertraiting the core
/// [`surrealdb_core::observe::ObservabilityProvider`].
///
/// Composers contribute optional pipeline-counter handles that the unified
/// meter provider registers as observable gauges. They also opt into the
/// runtime-aware observer hook
/// [`Self::create_observer_with_runtime`] when their observers need to
/// register instruments or claim audit-log scopes; the default
/// implementation delegates to the core
/// [`surrealdb_core::observe::ObservabilityProvider::create_observer`]
/// for community composers that have no enterprise instruments to
/// register.
pub trait ObservabilityProvider: surrealdb_core::observe::ObservabilityProvider {
	/// Construct the composer's [`ExecutionObserver`] with access to the
	/// process-local
	/// [`ObservabilityRuntime`](super::runtime::ObservabilityRuntime).
	///
	/// Composers that wire enterprise observers (per-tenant rollups,
	/// SurrealDS cluster internals, audit / slow-query pipelines)
	/// override this to register instruments against `runtime.meter(...)`
	/// and to claim `runtime.audit_logger(...)` for log emission. The
	/// default delegates to
	/// [`surrealdb_core::observe::ObservabilityProvider::create_observer`]
	/// so existing community composers keep working unchanged.
	fn create_observer_with_runtime(
		&self,
		_runtime: &ObservabilityRuntime,
	) -> Arc<dyn ExecutionObserver> {
		<Self as surrealdb_core::observe::ObservabilityProvider>::create_observer(self)
	}

	/// Optional read-only view of an audit pipeline's queue counters.
	/// When `Some`, the server registers `surrealdb.audit.*` observable
	/// gauges that read from the returned handle on every collection.
	fn audit_counters(&self) -> Option<Arc<dyn PipelineCounters>> {
		None
	}

	/// Optional read-only view of a slow-query log's queue counters.
	/// When `Some`, the server registers `surrealdb.slow_query.*`
	/// observable gauges the same way the audit pipeline does.
	fn slow_query_counters(&self) -> Option<Arc<dyn PipelineCounters>> {
		None
	}
}

impl ObservabilityProvider for CommunityComposer {}
