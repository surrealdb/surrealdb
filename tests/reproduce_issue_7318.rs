//! Regression for <https://github.com/surrealdb/surrealdb/issues/7318>:
//! DiskANN KNN search non-deterministically fails on indexes built CONCURRENTLY
//! over a few hundred vectors. The user reports ~95–100% of KNN queries return
//! `DiskANN KNN search failed`; the server log shows compaction failing with
//! `DiskANN element N is missing -- provider.rs:594`.
//!
//! Running against the embedded `mem://` SDK target spins up the same
//! `Datastore::index_compaction` periodic task as the released server binary
//! (see `surrealdb/src/engine/tasks.rs::spawn_task_index_compaction`), which
//! is the other compaction path that races with the index builder's
//! `compact_diskann_pendings`. The race manifests when:
//!   1. the builder commits its compaction (cache populated with 0..N-1),
//!   2. the periodic compactor — whose plan captured the same !dr keys — wins its `graph.write()`
//!      after the builder, applies, and then fails to commit (write conflict on !dg or the
//!      captured-key delc),
//!   3. its in-flight cache mutations leak adjacency entries for the not-yet-committed N..2N-1
//!      element ids,
//!   4. the post-commit-failure cache eviction is a tracker-driven scan that misses entries whose
//!      membership-tracker bookkeeping has fallen out of sync with quick_cache, so those polluted
//!      adjacency entries survive,
//!   5. subsequent KNN searches cache-hit the polluted entries and follow the neighbor pointers to
//!      ids that don't exist in KV — boom, "element N is missing".
//!
//! The fix removes step (4): cache cleanup now uses `quick_cache::retain` with
//! the index identity (rather than the lagging trackers), and the commit /
//! cleanup both happen inside the `graph.write()` critical section so KNN
//! searches cannot observe partially-cleared state.

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use surrealdb::Surreal;
	use surrealdb::engine::any;
	use surrealdb::types::SurrealValue;
	use tokio::time::{Instant, sleep};

	/// The bug-report parameters are (DIM=2560, N=1000, DEGREE=64, L_BUILD=100)
	/// which take ~13 minutes wall-clock even on a fast machine because of the
	/// raw graph-build cost. We use a smaller scale here that still triggers
	/// the underlying race (1 successful builder compaction + 1 periodic
	/// compaction whose commit conflicts; the failed compaction's writes leak
	/// into the shared DiskAnnCache and pollute KNN searches) — this completes
	/// in roughly one minute and is reliable.
	const DIM: usize = 1024;
	const N: usize = 500;
	const DEGREE: usize = 32;
	const L_BUILD: usize = 64;
	/// Number of KNN queries to run after `INFO FOR INDEX` reports `ready`.
	/// At the chosen scale 4–5 of 5 fail without the fix.
	const NUM_QUERIES: usize = 5;

	#[derive(Debug, SurrealValue)]
	struct InfoForIndex {
		building: Option<Building>,
	}

	#[derive(Debug, SurrealValue)]
	struct Building {
		status: String,
	}

	/// Cheap, deterministic PRNG so the test is reproducible without an
	/// extra `rand` dep at the workspace test level.
	struct SplitMix64(u64);

	impl SplitMix64 {
		fn new(seed: u64) -> Self {
			Self(seed)
		}
		fn next_u64(&mut self) -> u64 {
			self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
			let mut z = self.0;
			z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
			z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
			z ^ (z >> 31)
		}
		/// Box-Muller transform on two uniforms to produce a standard normal.
		fn next_gauss(&mut self) -> f64 {
			let u1 = ((self.next_u64() >> 11) as f64) / ((1u64 << 53) as f64);
			let u2 = ((self.next_u64() >> 11) as f64) / ((1u64 << 53) as f64);
			let r = (-2.0 * u1.max(1e-300).ln()).sqrt();
			let theta = 2.0 * std::f64::consts::PI * u2;
			r * theta.cos()
		}
	}

	fn unit_vector(rng: &mut SplitMix64, dim: usize) -> Vec<f64> {
		let mut v = Vec::with_capacity(dim);
		for _ in 0..dim {
			v.push(rng.next_gauss());
		}
		let norm = v.iter().map(|x| x * x).sum::<f64>().sqrt().max(1e-12);
		for x in v.iter_mut() {
			*x /= norm;
		}
		v
	}

	fn format_vector(v: &[f64]) -> String {
		let mut s = String::with_capacity(v.len() * 18);
		s.push('[');
		for (i, x) in v.iter().enumerate() {
			if i > 0 {
				s.push(',');
			}
			// 17 significant digits round-trip f64 losslessly.
			s.push_str(&format!("{:.17e}", x));
		}
		s.push(']');
		s
	}

	async fn wait_for_index_ready(
		client: &Surreal<any::Any>,
		table: &str,
		index: &str,
		timeout: Duration,
	) -> Result<(), Box<dyn std::error::Error>> {
		let started = Instant::now();
		loop {
			let mut q = client.query(format!("INFO FOR INDEX {index} ON {table};")).await?;
			let info: Vec<InfoForIndex> = q.take(0).unwrap_or_default();
			let status = info
				.first()
				.and_then(|i| i.building.as_ref())
				.map(|b| b.status.as_str())
				.unwrap_or("?")
				.to_string();
			if status == "ready" {
				return Ok(());
			}
			if started.elapsed() > timeout {
				return Err(
					format!("index `{index}` never became ready (last status: {status})").into()
				);
			}
			sleep(Duration::from_millis(250)).await;
		}
	}

	// Marked `#[ignore]` because the test still takes about a minute on a
	// fast machine (24 s of inserts + a CONCURRENTLY-built DiskANN graph +
	// KNN searches at the chosen scale). It is the regression for #7318:
	// without the cache-clear fix the test reliably fails with
	// `DiskANN KNN search failed`; with the fix it passes. Run with
	// `cargo test -- --ignored` or via a dedicated regression CI job.
	#[ignore = "slow (~1 min) DiskANN race regression — run with --ignored"]
	#[tokio::test]
	async fn diskann_knn_after_concurrent_build_should_not_fail()
	-> Result<(), Box<dyn std::error::Error>> {
		println!("--- issue #7318 regression: DiskANN KNN after CONCURRENTLY build ---");
		println!("DIM={DIM} N={N} DEGREE={DEGREE} L_BUILD={L_BUILD}");

		let client: Surreal<_> = any::connect("mem://").await?;
		client.use_ns("t").use_db("t").await?;
		client.query("DEFINE TABLE vec SCHEMALESS;").await?;

		// Insert N random unit vectors before defining the index, so the
		// build path that races with the periodic compactor is exercised.
		let mut rng = SplitMix64::new(42);
		let mut inserted = 0usize;
		// Each vector is ~24 KB of JSON for dim=2560. Bigger batches make
		// the build path slower but also amortise the SDK round-trip.
		let batch_size = 32usize;
		let t0 = Instant::now();
		while inserted < N {
			let upper = (inserted + batch_size).min(N);
			let mut sql = String::new();
			for i in inserted..upper {
				let v = unit_vector(&mut rng, DIM);
				sql.push_str(&format!(
					"CREATE vec SET idx={}, embedding={};",
					i,
					format_vector(&v)
				));
			}
			client.query(sql).await?;
			inserted = upper;
		}
		println!("inserted {N} vectors in {:.2?}", t0.elapsed());

		// Build the DiskANN index CONCURRENTLY. The user-visible status
		// flips to "ready" before the DiskANN pending-record compaction
		// finishes, so subsequent KNN queries race with the in-flight
		// compaction transactions.
		let define_index = format!(
			"DEFINE INDEX vidx ON vec FIELDS embedding DISKANN \
			 DIMENSION {DIM} DIST COSINE TYPE F32 \
			 DEGREE {DEGREE} L_BUILD {L_BUILD} ALPHA 1.2 CONCURRENTLY;"
		);
		client.query(define_index).await?;
		wait_for_index_ready(&client, "vec", "vidx", Duration::from_secs(60)).await?;
		println!("index reports `ready`");

		// Build a fixed query vector (different RNG stream from the data
		// vectors) and run a batch of KNN queries.
		let mut q_rng = SplitMix64::new(99);
		let q = unit_vector(&mut q_rng, DIM);
		let q_json = format_vector(&q);
		let knn_sql = format!("SELECT idx FROM vec WHERE embedding <|10,{L_BUILD}|> {q_json};");

		let mut failures = Vec::new();
		for i in 0..NUM_QUERIES {
			match client.query(knn_sql.as_str()).await {
				Ok(mut response) => match response.take::<surrealdb::types::Value>(0) {
					Ok(_) => {}
					Err(e) => failures.push(format!("query {i}: {e}")),
				},
				Err(e) => failures.push(format!("query {i}: {e}")),
			}
		}

		// The bug surfaces as `DiskANN KNN search failed` errors returned
		// from the SDK call (the underlying provider error is
		// `DiskANN element N is missing -- provider.rs:594/595`).
		if !failures.is_empty() {
			println!("BUG #7318 reproduced: {}/{NUM_QUERIES} KNN queries failed", failures.len());
			for f in failures.iter().take(3) {
				println!("  - {f}");
			}
		}
		assert!(
			failures.is_empty(),
			"DiskANN KNN search returned errors on a freshly built CONCURRENTLY index ({}/{} queries failed); first error: {}",
			failures.len(),
			NUM_QUERIES,
			failures.first().cloned().unwrap_or_default(),
		);
		Ok(())
	}
}
