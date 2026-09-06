#![allow(clippy::unwrap_used)]
#![recursion_limit = "256"]

mod common;

use common::{block_on, setup_datastore_with_query};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

// ============================================================================
// Benchmark: RATELIMIT admission overhead
// ============================================================================

fn bench_ratelimit_admission(c: &mut Criterion) {
	let mut group = c.benchmark_group("ratelimit_admission");
	let runtime = common::create_runtime();

	let (baseline_dbs, baseline_ses) =
		block_on(setup_datastore_with_query("CREATE item:test SET name = 'baseline', age = 30;"));
	let (limited_dbs, mut limited_ses) = block_on(setup_datastore_with_query(
		"DEFINE TABLE item RATELIMIT FOR SELECT BY $session.id LIMIT 100000000 PER 1h; \
		 CREATE item:test SET name = 'limited', age = 30;",
	));
	// Rate limits fail closed on a NONE key: without a session id this
	// benchmark would measure the (cheap) denial path, not admission.
	limited_ses.id = Some(uuid::Uuid::new_v4());
	let (where_false_dbs, where_false_ses) = block_on(setup_datastore_with_query(
		"DEFINE TABLE item RATELIMIT FOR SELECT WHERE false BY $session.id LIMIT 1 PER 1h; \
		 CREATE item:test SET name = 'where_false', age = 30;",
	));
	let (ip_bucket_dbs, mut ip_bucket_ses) = block_on(setup_datastore_with_query(
		"DEFINE TABLE item RATELIMIT FOR SELECT BY $session.ip LIMIT 100000000 PER 1h; \
		 CREATE item:test SET name = 'ip_bucket', age = 30;",
	));
	ip_bucket_ses.ip = Some("127.0.0.1".to_string());

	group.throughput(Throughput::Elements(1));
	group.bench_function("select_by_id_baseline", |b| {
		b.to_async(&runtime)
			.iter(|| async { query!(&baseline_dbs, &baseline_ses, "SELECT * FROM item:test;") });
	});
	group.bench_function("select_by_id_with_session_id_limit", |b| {
		b.to_async(&runtime)
			.iter(|| async { query!(&limited_dbs, &limited_ses, "SELECT * FROM item:test;") });
	});
	group.bench_function("select_by_id_with_where_false_policy", |b| {
		b.to_async(&runtime).iter(|| async {
			query!(&where_false_dbs, &where_false_ses, "SELECT * FROM item:test;")
		});
	});
	group.bench_function("select_by_id_with_session_ip_limit", |b| {
		b.to_async(&runtime)
			.iter(|| async { query!(&ip_bucket_dbs, &ip_bucket_ses, "SELECT * FROM item:test;") });
	});

	group.finish();
}

/// Denial-path costs. Under sustained abuse, denial is the steady state:
/// its cost is the amplification an abuser gets per rejected request.
/// Delivered-records limits withhold the data of an over-budget statement;
/// bounding the computation it performed is the cost-budget concern
/// governed separately.
fn bench_ratelimit_denial(c: &mut Criterion) {
	let mut group = c.benchmark_group("ratelimit_denial");
	let runtime = common::create_runtime();

	// Exhausted bucket: the setup query drains the single token; refill is
	// 1/hour, so every benchmarked request is denied at admission.
	let (drained_dbs, mut drained_ses) = block_on(setup_datastore_with_query(
		"DEFINE TABLE item RATELIMIT FOR SELECT BY $session.id LIMIT 1 PER 1h; \
		 CREATE item:test SET name = 'drained', age = 30;",
	));
	drained_ses.id = Some(uuid::Uuid::new_v4());

	// Fail-closed key: no session id, so the BY key evaluates to NONE and
	// the statement is denied before any bucket access.
	let (none_key_dbs, none_key_ses) = block_on(setup_datastore_with_query(
		"DEFINE TABLE item RATELIMIT FOR SELECT BY $session.id LIMIT 1000000 PER 1h; \
		 CREATE item:test SET name = 'none_key', age = 30;",
	));

	// Over-budget delivery: a SELECT that would deliver 256 rows against a
	// capacity-2 bucket. The statement executes, then the atomic settle
	// denies it before any data is returned — responses are never partial.
	let mut scan_setup =
		String::from("DEFINE TABLE item RATELIMIT FOR SELECT BY $session.id LIMIT 2 PER 1h;");
	for i in 0..256 {
		scan_setup.push_str(&format!(" CREATE item:{i} SET name = 'row', age = {i};"));
	}
	let (scan_dbs, mut scan_ses) = block_on(setup_datastore_with_query(&scan_setup));
	scan_ses.id = Some(uuid::Uuid::new_v4());

	group.throughput(Throughput::Elements(1));
	group.bench_function("denied_admission_exhausted_bucket", |b| {
		b.to_async(&runtime).iter(|| async {
			// Drain once in setup; every iteration here is a denial.
			query!(&drained_dbs, &drained_ses, "SELECT * FROM item:test;")
		});
	});
	group.bench_function("denied_fail_closed_none_key", |b| {
		b.to_async(&runtime)
			.iter(|| async { query!(&none_key_dbs, &none_key_ses, "SELECT * FROM item:test;") });
	});
	group.bench_function("denied_over_budget_delivery", |b| {
		b.to_async(&runtime).iter(|| async { query!(&scan_dbs, &scan_ses, "SELECT * FROM item;") });
	});

	group.finish();
}

criterion_group! {
	name = benches;
	config = Criterion::default();
	targets = bench_ratelimit_admission, bench_ratelimit_denial,
}
criterion_main!(benches);
