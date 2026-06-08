//! Process-local observability handle.
//!
//! [`ObservabilityRuntime`] owns every server-side telemetry provider so
//! observers can register instruments without consulting any process-wide
//! `OnceLock` or `opentelemetry::global` provider. Embedders that drive
//! the server through [`crate::init`] receive a runtime built by the
//! telemetry [`Builder`](crate::telemetry::Builder); embedders that wire
//! the server directly into their own Axum app can build a runtime by
//! hand via [`ObservabilityRuntime::builder`].
//!
//! Cloning the runtime is cheap: every field is internally `Arc`-shared
//! by the OTel SDK, and the outer [`Arc`] keeps the whole bundle to a
//! single pointer copy.

use std::sync::Arc;

use opentelemetry::InstrumentationScope;
use opentelemetry::logs::LoggerProvider as _;
use opentelemetry::metrics::{Meter, MeterProvider as _};
use opentelemetry_prometheus_text_exporter::PrometheusExporter;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::{SdkLogger, SdkLoggerProvider};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;

/// Process-local observability handle.
///
/// Carries every telemetry provider plus the optional Prometheus text
/// exporter the server crate uses to render the `/metrics` endpoint.
/// Cheap to clone (single [`Arc`] bump).
#[derive(Clone)]
pub struct ObservabilityRuntime {
	inner: Arc<RuntimeInner>,
}

/// Inner state of [`ObservabilityRuntime`].
///
/// Held inside an [`Arc`] so cloning the runtime is a single pointer
/// copy. Each provider field already has its own internal `Arc`, but the
/// outer wrapper keeps the API a single value rather than five.
struct RuntimeInner {
	resource: Resource,
	meter_provider: SdkMeterProvider,
	prometheus_exporter: Option<PrometheusExporter>,
	audit_logger_provider: Option<SdkLoggerProvider>,
	tracer_provider: Option<SdkTracerProvider>,
}

impl ObservabilityRuntime {
	/// Begin building a runtime. The minimum required state is the
	/// [`SdkMeterProvider`]; everything else is optional and defaults to
	/// `None`.
	pub fn builder(meter_provider: SdkMeterProvider) -> ObservabilityRuntimeBuilder {
		ObservabilityRuntimeBuilder {
			resource: None,
			meter_provider,
			prometheus_exporter: None,
			audit_logger_provider: None,
			tracer_provider: None,
		}
	}

	/// Construct a runtime backed by a no-op [`SdkMeterProvider`].
	///
	/// Use when telemetry is fully disabled (or in unit tests that only
	/// need a non-`None` runtime to satisfy a function signature).
	/// Recordings against the returned runtime have no effect.
	pub fn noop() -> Self {
		Self::builder(SdkMeterProvider::default()).build()
	}

	/// Build a [`Meter`] under the given instrumentation scope.
	///
	/// Replaces the historical `opentelemetry::global::meter_with_scope`
	/// call: each observer takes a runtime and asks it for the scoped
	/// meter, so no module needs to look at the OTel global provider.
	pub fn meter(&self, scope_name: &'static str) -> Meter {
		self.inner
			.meter_provider
			.meter_with_scope(InstrumentationScope::builder(scope_name).build())
	}

	/// Build a logger under the supplied scope from the audit / slow-query
	/// logger provider, when one is configured. Returns `None` outside the
	/// server runtime (e.g. in unit tests with no OTLP pipeline) so emit
	/// sites collapse to a no-op.
	pub fn audit_logger(&self, scope_name: &'static str) -> Option<SdkLogger> {
		let provider = self.inner.audit_logger_provider.as_ref()?;
		let scope = InstrumentationScope::builder(scope_name).build();
		Some(provider.logger_with_scope(scope))
	}

	/// Returns a clone of the Prometheus text exporter when one is
	/// installed. The [`MetricsState`](super::handler::MetricsState)
	/// carries this clone into the Axum extension layer.
	pub fn prometheus_exporter(&self) -> Option<PrometheusExporter> {
		self.inner.prometheus_exporter.clone()
	}

	/// Returns a borrow of the underlying meter provider. Useful when an
	/// observer needs to clone the provider into its own structure (e.g.
	/// a long-lived background task that owns its meters).
	pub fn meter_provider(&self) -> &SdkMeterProvider {
		&self.inner.meter_provider
	}

	/// Returns a borrow of the OTel resource bundle.
	pub fn resource(&self) -> &Resource {
		&self.inner.resource
	}

	/// Returns the audit / slow-query [`SdkLoggerProvider`] when
	/// configured.
	pub fn audit_logger_provider(&self) -> Option<&SdkLoggerProvider> {
		self.inner.audit_logger_provider.as_ref()
	}

	/// Returns the OTLP tracer provider when one is configured.
	pub fn tracer_provider(&self) -> Option<&SdkTracerProvider> {
		self.inner.tracer_provider.as_ref()
	}

	/// Flush every provider's batch processor and shut it down. Calls
	/// [`SdkMeterProvider::shutdown`] / [`SdkLoggerProvider::shutdown`] /
	/// [`SdkTracerProvider::shutdown`] in turn so the metrics, audit
	/// logs, and traces buffered in their batch processors all reach
	/// the configured exporters before the runtime is dropped.
	pub fn shutdown(&self) {
		if let Err(err) = self.inner.meter_provider.shutdown() {
			tracing::warn!(target: "surrealdb::observe", "meter provider shutdown failed: {err}");
		}
		if let Some(provider) = self.inner.audit_logger_provider.as_ref()
			&& let Err(err) = provider.shutdown()
		{
			tracing::warn!(target: "surrealdb::observe", "audit log provider shutdown failed: {err}");
		}
		if let Some(provider) = self.inner.tracer_provider.as_ref()
			&& let Err(err) = provider.shutdown()
		{
			tracing::warn!(target: "surrealdb::observe", "tracer provider shutdown failed: {err}");
		}
	}
}

/// Builder for an [`ObservabilityRuntime`].
///
/// Constructed via [`ObservabilityRuntime::builder`]. Every optional
/// provider defaults to `None`; pass each one in turn from the
/// telemetry init helpers.
#[must_use]
pub struct ObservabilityRuntimeBuilder {
	resource: Option<Resource>,
	meter_provider: SdkMeterProvider,
	prometheus_exporter: Option<PrometheusExporter>,
	audit_logger_provider: Option<SdkLoggerProvider>,
	tracer_provider: Option<SdkTracerProvider>,
}

impl ObservabilityRuntimeBuilder {
	/// Override the OTel `Resource` carried on the runtime. Defaults to
	/// the unit resource when unset; the production builder always sets
	/// this from [`crate::telemetry::OTEL_DEFAULT_RESOURCE`].
	pub fn with_resource(mut self, resource: Resource) -> Self {
		self.resource = Some(resource);
		self
	}

	/// Attach the Prometheus text exporter that renders `/metrics`.
	pub fn with_prometheus_exporter(mut self, exporter: PrometheusExporter) -> Self {
		self.prometheus_exporter = Some(exporter);
		self
	}

	/// Attach the audit / slow-query logger provider.
	pub fn with_audit_logger_provider(mut self, provider: SdkLoggerProvider) -> Self {
		self.audit_logger_provider = Some(provider);
		self
	}

	/// Attach the OTLP tracer provider.
	pub fn with_tracer_provider(mut self, provider: SdkTracerProvider) -> Self {
		self.tracer_provider = Some(provider);
		self
	}

	/// Finalise the runtime.
	pub fn build(self) -> ObservabilityRuntime {
		ObservabilityRuntime {
			inner: Arc::new(RuntimeInner {
				resource: self.resource.unwrap_or_else(|| Resource::builder_empty().build()),
				meter_provider: self.meter_provider,
				prometheus_exporter: self.prometheus_exporter,
				audit_logger_provider: self.audit_logger_provider,
				tracer_provider: self.tracer_provider,
			}),
		}
	}
}
