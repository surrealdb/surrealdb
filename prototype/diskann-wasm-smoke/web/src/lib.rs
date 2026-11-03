//! Smoke test: a DiskANN index inside a wasm32-unknown-unknown build of the engine, run from
//! JavaScript with no tokio runtime.
//!
//! Builds an index over deterministic vectors, answers a KNN query from the
//! pending set, compacts it into the graph (`multi_insert`, which spawns tokio
//! tasks), answers again from the graph, deletes the nearest records, compacts
//! again, and compares every answer against the brute-force `<|k,EUCLIDEAN|>`.

use std::cell::RefCell;

thread_local! {
	static LOG: RefCell<String> = const { RefCell::new(String::new()) };
}

macro_rules! log {
	($($t:tt)*) => {
		LOG.with(|l| { let mut l = l.borrow_mut(); l.push_str(&format!($($t)*)); l.push('\n'); })
	};
}

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;

const N: usize = 2000;
const DIM: usize = 8;
const K: usize = 10;

fn vectors() -> Vec<Vec<f32>> {
	let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
	let mut next = || {
		state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
		((state >> 33) as f64 / (1u64 << 31) as f64) as f32
	};
	(0..N).map(|_| (0..DIM).map(|_| next()).collect()).collect()
}

fn lit(v: &[f32]) -> String {
	let parts: Vec<String> = v.iter().map(|x| format!("{x}f")).collect();
	format!("[{}]", parts.join(","))
}

async fn run(ds: &Datastore, ses: &Session, sql: &str) -> Result<Vec<String>> {
	let mut out = Vec::new();
	for r in ds.execute(sql, ses, None).await? {
		match r.result {
			Ok(v) => out.push(format!("{v:?}")),
			Err(e) => bail!("query failed: {e}\n  in: {sql}"),
		}
	}
	Ok(out)
}

async fn compare(ds: &Datastore, ses: &Session, label: &str, q: &str) -> Result<usize> {
	let sql = format!(
		"LET $ann = (SELECT VALUE record::id(id) FROM pts WHERE point <|{K},64|> {q});
		 LET $exact = (SELECT VALUE record::id(id) FROM pts WHERE point <|{K},EUCLIDEAN|> {q});
		 RETURN array::len(array::intersect($ann, $exact));
		 RETURN array::len($ann);"
	);
	let out = run(ds, ses, &sql).await?;
	let hits: usize = out[2].trim_start_matches("Number(Int(").trim_end_matches("))").parse()?;
	let len: usize = out[3].trim_start_matches("Number(Int(").trim_end_matches("))").parse()?;
	log!("{label}: ANN returned {len}, {hits}/{K} match brute force");
	if len != K {
		bail!("{label}: ANN returned {len} rows, expected {K}");
	}
	Ok(hits)
}

async fn compact(ds: &Arc<Datastore>) -> Result<()> {
	let (batches, errors) = Datastore::index_compaction(
		Arc::clone(ds),
		Duration::from_secs(1),
		tokio_util::sync::CancellationToken::new(),
	)
	.await?;
	log!("compaction: {batches} batches, {errors} errors");
	if errors > 0 {
		bail!("compaction reported {errors} failed indexes");
	}
	if batches == 0 {
		bail!("compaction processed no batches, so the graph was not built");
	}
	Ok(())
}

async fn smoke() -> Result<()> {
	log!(
		"target: {} / {}-bit pointers",
		if cfg!(all(target_family = "wasm", target_os = "unknown")) { "wasm32-unknown-unknown" } else { std::env::consts::OS },
		usize::BITS
	);
	let ds = Arc::new(Datastore::new("memory").await?);
	let ses = Session::owner().with_ns("t").with_db("t");
	run(&ds, &ses, "DEFINE NS t; DEFINE DB t;").await?;
	run(
		&ds,
		&ses,
		&format!(
			"DEFINE INDEX ix ON pts FIELDS point DISKANN DIMENSION {DIM} DIST EUCLIDEAN TYPE F32 DEGREE 16 L_BUILD 64;"
		),
	)
	.await?;

	let vs = vectors();
	for (chunk_no, chunk) in vs.chunks(250).enumerate() {
		let rows: Vec<String> = chunk
			.iter()
			.enumerate()
			.map(|(i, v)| format!("{{ id: {}, point: {} }}", chunk_no * 250 + i, lit(v)))
			.collect();
		run(&ds, &ses, &format!("INSERT INTO pts [{}] RETURN NONE;", rows.join(","))).await?;
	}
	log!("inserted {N} vectors of {DIM} dims");

	let queries: Vec<String> = [3usize, 777, 1500].iter().map(|&i| lit(&vs[i])).collect();
	let explain = run(
		&ds,
		&ses,
		&format!("SELECT id FROM pts WHERE point <|{K},64|> {} EXPLAIN;", queries[0]),
	)
	.await?;
	log!("plan: {}", explain[0].chars().take(300).collect::<String>());

	let mut worst = K;
	for q in &queries {
		worst = worst.min(compare(&ds, &ses, "pending ", q).await?);
	}
	compact(&ds).await?;
	for q in &queries {
		worst = worst.min(compare(&ds, &ses, "graph   ", q).await?);
	}

	// Delete the exact neighbours of the first query, keeping their ids in `gone:q0`, compact,
	// and check that the table lost exactly those rows and that no search returns one of them.
	run(
		&ds,
		&ses,
		&format!(
			"UPSERT gone:q0 SET ids = (SELECT VALUE id FROM pts WHERE point <|{K},EUCLIDEAN|> {});
			 FOR $r IN gone:q0.ids {{ DELETE $r; }};",
			queries[0]
		),
	)
	.await?;
	compact(&ds).await?;
	let back = run(
		&ds,
		&ses,
		&format!(
			"RETURN array::len((SELECT VALUE id FROM pts WHERE point <|{K},64|> {q}));
			 RETURN count(SELECT id FROM pts);
			 RETURN array::len(array::intersect(
				(SELECT VALUE id FROM pts WHERE point <|{K},64|> {q}),
				gone:q0.ids
			 ));",
			q = queries[0]
		),
	)
	.await?;
	let int = |s: &str| -> Result<usize> {
		Ok(s.trim_start_matches("Number(Int(").trim_end_matches("))").parse()?)
	};
	let (returned, remaining, stale) = (int(&back[0])?, int(&back[1])?, int(&back[2])?);
	log!("after deleting {K}: ANN returned {returned}, table has {remaining}, deleted ids returned: {stale}");
	if remaining != N - K {
		bail!("the table has {remaining} rows after deleting {K}, expected {}", N - K);
	}
	if returned != K {
		bail!("ANN returned {returned} rows after the deletes, expected {K}");
	}
	if stale != 0 {
		bail!("ANN returned {stale} deleted records after compaction");
	}
	for q in &queries {
		worst = worst.min(compare(&ds, &ses, "deleted ", q).await?);
	}

	if worst < K * 8 / 10 {
		bail!("recall fell to {worst}/{K}");
	}
	log!("OK (worst recall {worst}/{K})");
	Ok(())
}

#[wasm_bindgen::prelude::wasm_bindgen]
pub async fn run_smoke() -> Result<String, wasm_bindgen::JsValue> {
	let result = smoke().await;
	let log = LOG.with(|l| std::mem::take(&mut *l.borrow_mut()));
	match result {
		Ok(()) => Ok(log),
		Err(e) => Err(wasm_bindgen::JsValue::from_str(&format!("{log}\nERROR: {e:#}"))),
	}
}
