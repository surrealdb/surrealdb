//! The instrument-name allowlist used to filter the rendered Prometheus
//! output for unauthenticated `/metrics` consumers.
//!
//! # Threat model
//!
//! Every labelled `surrealdb.*` family registered by the unified
//! [`super::MetricsObserver`] carries the resolved tenant ctx
//! (`namespace`, `database`, `user`). The unified provider does not
//! distinguish between "safe" and "unsafe" labels at the instrument
//! level; the security boundary is the metric **name**, applied at
//! render time. Only metric families whose name appears in this list
//! are rendered for unauthenticated requests; everything else requires
//! root-level authentication.
//!
//! This is the only allowlist remaining in the observability path. The
//! older `SAFE_ATTRIBUTES` list (which constrained label keys on a
//! community-only meter scope) has been removed: with one labelled
//! family per signal, the right granularity for the boundary is the
//! family name, not the label key.
//!
//! # Adding a new public metric
//!
//! 1. Confirm the metric carries no per-tenant labels and no customer-identifying values. The
//!    family must be safe to expose to anonymous scrapers.
//! 2. Confirm that exposing the metric to unauthenticated consumers does not enable **aggregate**
//!    inference in multi-tenant deployments (Spectron). If in doubt, keep the metric
//!    authenticated-only by leaving it off this list.
//! 3. Get a security review (see CODEOWNERS) before merging.
//!
//! Operators running multi-tenant deployments SHOULD disable
//! `SURREAL_METRICS_ENABLED` or restrict the endpoint via their reverse proxy
//! even with this allowlist in place: aggregate workload signals may still
//! reveal tenant activity patterns.

/// Instrument names that may appear in the unauthenticated `/metrics` output.
///
/// Names follow the post-conversion form produced by the Prometheus text
/// exporter: dotted OTel names become underscore-separated, units add a
/// suffix (`_seconds`, `_bytes`), and the OTel SDK appends `target_info`
/// and `otel_scope_info` at the meter-provider level (both kept on the
/// public list because they are aggregate process metadata, not tenant
/// signal).
///
/// ORDERED ALPHABETICALLY. Additions require security review.
pub static PUBLIC_METRICS: &[&str] = &[
	// `otel_scope_info` is emitted by the OTel SDK to carry the
	// instrumentation-scope metadata. Aggregate, no tenant signal.
	"otel_scope_info",
	// Static build metadata. Emitted as a constant gauge with `build_version`.
	"surrealdb_build_info",
	// Aggregate process CPU percentage. Multi-tenant risk is low: the signal
	// is host-wide, carries no tenant labels, and operators running
	// co-tenanted deployments can hide it behind the reverse proxy.
	"surrealdb_process_cpu_percent",
	// Aggregate process resident set size in bytes. Same reasoning as CPU.
	"surrealdb_process_memory_bytes",
	// Process uptime in seconds. Aggregate, non-attributable.
	"surrealdb_process_uptime_seconds",
	// Resource metadata emitted by the OTel SDK from the process Resource.
	// Aggregate, host-wide, no tenant signal.
	"target_info",
];

/// Returns `true` if the given Prometheus metric family name is allowed in the
/// unauthenticated `/metrics` output.
pub fn is_public_metric(name: &str) -> bool {
	PUBLIC_METRICS.contains(&name)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn metrics_are_sorted_alphabetically() {
		let mut sorted = PUBLIC_METRICS.to_vec();
		sorted.sort_unstable();
		assert_eq!(PUBLIC_METRICS, sorted, "PUBLIC_METRICS must be kept sorted alphabetically",);
	}

	#[test]
	fn has_no_rpc_query_or_session_names() {
		// Anything identifying per-call, per-session, or per-statement
		// activity MUST require authentication. Aggregate process-level
		// signals only.
		const FORBIDDEN_PREFIXES: &[&str] = &[
			"surrealdb_queries_",
			"surrealdb_query_",
			"surrealdb_statements_",
			"surrealdb_statement_",
			"surrealdb_rpcs_",
			"surrealdb_rpc_",
			"surrealdb_sessions_",
			"surrealdb_session_",
			"surrealdb_auth_",
			"surrealdb_transactions_",
			"surrealdb_transaction_",
			"surrealdb_http_",
			"surrealdb_ws_",
			"surrealdb_network_",
			"surrealdb_live_query_",
			"surrealdb_audit_",
			"surrealdb_slow_query_",
			"surrealdb_storage_",
			"surrealdb_tenant_",
			// GraphQL operation metrics carry per-tenant labels
			// (operation_type + NS/DB/user); MCP tool metrics carry
			// per-tenant labels (tool + transport + outcome). Neither
			// must ever appear on the unauthenticated allowlist, so
			// list both prefixes here as a defence-in-depth backstop.
			"surrealdb_graphql_",
			"surrealdb_mcp_",
		];
		for m in PUBLIC_METRICS {
			for p in FORBIDDEN_PREFIXES {
				assert!(
					!m.starts_with(p),
					"`{m}` starts with forbidden prefix `{p}`: keep per-call metrics authenticated-only",
				);
			}
		}
	}

	#[test]
	fn is_public_metric_matches_list() {
		for m in PUBLIC_METRICS {
			assert!(is_public_metric(m));
		}
		assert!(!is_public_metric("surrealdb_query_duration_seconds"));
		assert!(!is_public_metric("surrealdb_rpcs_completed_total"));
		assert!(!is_public_metric("surrealdb_statement_total"));
	}
}
