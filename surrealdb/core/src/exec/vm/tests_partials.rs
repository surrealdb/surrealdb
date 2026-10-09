//! Partials: what a producer hands a coordinator for one breaker, merged
//! into what the serial run answers. A producer here is a contiguous morsel
//! of the source run on a state of its own, as a fan or a spilling region
//! batch runs it, each on a context of its own over one temporary store.

use std::str::FromStr;
use std::sync::Arc;

use super::binding::{HostJudge, JoinStep, pairings};
use super::distinct_spill::DistinctProducer;
use super::groups::SavedFold;
use super::host::{finish_groups, statement_state};
use super::join_spill::{BuildSink, Pairing};
use super::partial::{Merged, Partial, PartialMerge, merge_partials};
use super::program::{Program, Source};
use super::region::{
	LeaseFault, batch_partials, body_breakers, lease_faults, merge_batch, run_morsel,
};
use super::spill::{Merge, write_run_as};
use super::tests_memory::{answer, bounded_on};
use super::tests_sources::program_of;
use crate::exec::memory::runs::{BreakerKind, Record, RunOrder, RunSpec};
use crate::exec::memory::{
	FairGovernor, MemTempStore, MemoryHost, MemorySettings, QueryMemory, RunRead, RunRef, RunWrite,
	SessionName, Sharing, TempStore, Unlimited,
};
use crate::exec::operators::distinct::SeenSet;
use crate::exec::operators::join::hash_join::{BuildTable, JoinKey, JoinType};
use crate::exec::operators::test_util::TestDb;
use crate::exec::{BoxFut, ExecutionContext};
use crate::expr::ControlFlow;
use crate::val::{Number, Object, Regex, Value};

/// Rows of `t` with a key of mixed numeric kinds that compare equal, a time,
/// a vector and a payload, so a morsel of a few hundred rows outgrows a small
/// budget.
const SETUP: &str = "
	DEFINE TABLE t;
	FOR $i IN 0..2000 {
		CREATE type::record('t', $i) SET
			n = $i, k = $i % 7, g = <string> ($i % 50), s = <string> ($i % 13),
			x = [1, 1.0f, 1dec][$i % 3],
			d = time::from_unix($i % 97),
			v = [$i % 5, $i % 3],
			pad = string::repeat('x', 64 + $i % 17)
	};
";

/// The producers a test splits its source over.
const NS: [usize; 3] = [1, 3, 8];

/// A configuration for a small budget: parts of 1 KiB, a merge of `fan_in`
/// runs at once, and a floor and chunk of a few KiB.
fn small(fan_in: usize) -> surrealdb_cnf::ConfigMap {
	surrealdb_cnf::ConfigMap::empty()
		.with_key_value("spill_part_size", "1024")
		.with_key_value("spill_merge_fan_in", fan_in.to_string())
		.with_key_value("query_memory_minimum", "4096")
		.with_key_value("memory_reserve_chunk", "4096")
		.with_key_value("region_row_threshold", usize::MAX.to_string())
}

/// A database of [`SETUP`] under a small budget, and the store it spills to.
async fn bounded() -> (TestDb, Arc<MemTempStore>) {
	let store = Arc::new(MemTempStore::new(1024, None));
	let governor = Arc::new(FairGovernor::new(16 << 10));
	let config = small(4).with_key_value("memory_reserve_chunk", "1024");
	let db = bounded_on(governor, Arc::clone(&store), SETUP, config).await;
	(db, store)
}

/// A database of [`SETUP`] with memory to spare.
async fn free() -> TestDb {
	TestDb::new_with_config(SETUP, small(4)).await
}

/// The rows of `t` in scan order.
async fn scan_rows(free: &TestDb) -> Vec<Value> {
	let rows = answer(free, "SELECT * FROM t", None).await.expect("the table reads");
	assert_eq!(rows.len(), 2000);
	rows
}

/// The pipeline of `program` that scans.
fn scan_pipeline(program: &Program) -> usize {
	program
		.pipelines
		.iter()
		.position(|pipeline| matches!(pipeline.source, Source::Scan { .. }))
		.expect("a scanning pipeline")
}

/// `rows` in `n` contiguous parts, in order.
fn morsels<T: Clone>(rows: &[T], n: usize) -> Vec<Vec<T>> {
	rows.chunks(rows.len().div_ceil(n)).map(<[T]>::to_vec).collect()
}

/// The memory of `ctx`'s query.
fn memory_of(ctx: &ExecutionContext) -> Arc<QueryMemory> {
	ctx.ctx().memory().cloned().expect("a query with memory")
}

/// Every record `merge` answers.
async fn drained(mut merge: Merge, ctx: &ExecutionContext) -> Vec<Record> {
	let mut all = Vec::new();
	loop {
		let chunk = merge.next_chunk(1000, ctx).await.expect("a chunk");
		if chunk.is_empty() {
			return all;
		}
		all.extend(chunk);
	}
}

fn records_of(merged: Merged) -> Merge {
	match merged {
		Merged::Records(merge) => merge,
		Merged::Join(_) => panic!("a join where records were merged"),
	}
}

fn text(flow: ControlFlow) -> String {
	match flow {
		ControlFlow::Err(error) => error.to_string(),
		_ => panic!("a control flow that is no error"),
	}
}

/// Every end of a test leaves no run behind and reads no more runs at once
/// than the fan-in.
fn assert_tidy(store: &MemTempStore) {
	assert_eq!(store.runs(), 0, "a spilled run outlived its query");
	assert!(store.most_read_at_once() <= 4, "read {} runs at once", store.most_read_at_once());
}

const SORTS: &[&str] = &[
	"SELECT * FROM t ORDER BY k",
	"SELECT n, k, pad FROM t ORDER BY k DESC, s",
	"SELECT n, pad FROM t ORDER BY s COLLATE, k",
	"SELECT n, pad FROM t ORDER BY pad, n DESC",
	"SELECT n, pad FROM t ORDER BY x",
	"SELECT n, pad FROM t ORDER BY k LIMIT 5 START 1500",
	"SELECT n, pad FROM t ORDER BY k DESC, n LIMIT 1200",
];

/// Sorts whose morsels are merged from their partials answer the records the
/// serial sort holds, ties and keeps included.
#[tokio::test]
async fn b_sorts_merged_from_partials_answer_the_serial_order() {
	let free = free().await;
	let (db, store) = bounded().await;
	let free_ctx = free.exec_ctx().await;
	let rows = scan_rows(&free).await;
	for sql in SORTS {
		let program = Arc::new(program_of(&free_ctx, sql).await);
		let at = scan_pipeline(&program);
		let sorts = body_breakers(&program.pipelines[at].code).sorts;
		assert_eq!(sorts.len(), 1, "`{sql}`");
		let sort = sorts[0];
		let how = PartialMerge::SortMerge {
			keys: program.sorts[sort as usize].keys.clone(),
			keep: program.sorts[sort as usize].keep,
		};
		let mut state = run_morsel(&program, at, rows.clone(), &free_ctx).await.expect("a morsel");
		let reference =
			Partial::of_sort(&mut state, &program, sort, &free_ctx).await.expect("a partial");
		assert!(reference.runs.is_empty(), "`{sql}` spilled with memory to spare");
		let want = reference.held;
		drop(state);
		for n in NS {
			let before = store.runs_written();
			let mut partials = Vec::new();
			for morsel in morsels(&rows, n) {
				let ctx = db.exec_ctx().await;
				let mut state = run_morsel(&program, at, morsel, &ctx).await.expect("a morsel");
				partials.push(
					Partial::of_sort(&mut state, &program, sort, &ctx).await.expect("a partial"),
				);
			}
			let carried = partials.iter().any(|p| !p.runs.is_empty());
			let wrote = store.runs_written() > before;
			let coordinator = db.exec_ctx().await;
			let merge = records_of(
				merge_partials(&memory_of(&coordinator), &how, partials, &coordinator)
					.await
					.expect("a merge"),
			);
			let got = drained(merge, &coordinator).await;
			assert_eq!(got, want, "`{sql}` merged from {n} producers");
			if !(sql.contains("START 1500") && n == 8) {
				assert!(wrote, "`{sql}` wrote no run for {n} producers");
				assert!(carried, "`{sql}` carried no run for {n} producers");
			}
		}
	}
	assert_tidy(&store);
}

const GROUPS: &[(&str, Spills)] = &[
	(
		"SELECT g, count() AS c, count(k > 3) AS ck, math::sum(n) AS s, math::mean(n) AS m, \
		 math::min(n) AS lo, math::max(n) AS hi FROM t GROUP BY g",
		Spills::Never,
	),
	(
		"SELECT n % 300 AS b, math::stddev(n) AS sd, math::variance(n) AS va, \
		 math::median(n) AS md FROM t GROUP BY b",
		Spills::Always,
	),
	(
		"SELECT n % 200 AS b, time::min(d) AS t0, time::max(d) AS t1, vector::sum(v) AS vs \
		 FROM t GROUP BY b",
		Spills::Always,
	),
	(
		"SELECT n % 400 AS b, array::group(k) AS ks, array::join(g, ',') AS j, \
		 array::distinct(s) AS ds, array::min(pad) AS p0, array::max(pad) AS p1 FROM t GROUP BY b",
		Spills::FewProducers,
	),
	("SELECT k, s, pad FROM t GROUP BY k, s", Spills::Never),
	("SELECT x, count() AS c, array::group(n) AS ns FROM t GROUP BY x", Spills::Never),
];

/// Whether a statement's groups outgrow a producer's budget.
#[derive(Clone, Copy)]
enum Spills {
	/// Too few groups to hold more than a chunk.
	Never,
	/// More groups than a morsel's own rows, whatever the producers.
	Always,
	/// Enough groups for one or three producers.
	FewProducers,
}

/// Aggregates folded from the partials of morsels answer the serial groups,
/// every accumulator and first value included.
#[tokio::test]
async fn b_groups_folded_from_partials_answer_the_serial_groups() {
	let free = free().await;
	let (db, store) = bounded().await;
	let free_ctx = free.exec_ctx().await;
	let rows = scan_rows(&free).await;
	let mut set_level = [(false, false); 3];
	for (sql, spills) in GROUPS {
		let program = Arc::new(program_of(&free_ctx, sql).await);
		let at = scan_pipeline(&program);
		let aggregate = body_breakers(&program.pipelines[at].code).aggregates[0];
		let mut state = run_morsel(&program, at, rows.clone(), &free_ctx).await.expect("a morsel");
		let groups = state.groups[aggregate as usize].take();
		let want = finish_groups(&program, aggregate, groups, &free_ctx).await.expect("finishes");
		drop(state);
		for (place, n) in NS.into_iter().enumerate() {
			let before = store.runs_written();
			let mut partials = Vec::new();
			for morsel in morsels(&rows, n) {
				let ctx = db.exec_ctx().await;
				let mut state = run_morsel(&program, at, morsel, &ctx).await.expect("a morsel");
				partials.push(Partial::of_groups(&mut state, &program, aggregate).expect("saved"));
			}
			let carried = partials.iter().any(|p| !p.runs.is_empty());
			let wrote = store.runs_written() > before;
			let got = folded(&db, &program, aggregate, partials).await;
			assert_eq!(got, want, "`{sql}` folded from {n} producers");
			let expected = match spills {
				Spills::Never => false,
				Spills::Always => true,
				Spills::FewProducers => n <= 3,
			};
			if expected {
				assert!(wrote && carried, "`{sql}` did not spill for {n} producers");
			}
			set_level[place].0 |= wrote;
			set_level[place].1 |= carried;
		}
	}
	for (place, (wrote, carried)) in set_level.into_iter().enumerate() {
		assert!(wrote && carried, "no aggregate spilled for {} producers", NS[place]);
	}
	assert_eq!(store.runs(), 0, "a spilled run outlived its query");
}

/// The rows aggregate `aggregate` of `program` finishes into, folded from
/// `partials` on a coordinator of `db`.
async fn folded(
	db: &TestDb,
	program: &Arc<Program>,
	aggregate: u16,
	partials: Vec<Partial>,
) -> Vec<Value> {
	let coordinator = db.exec_ctx().await;
	let memory = memory_of(&coordinator);
	let merge = records_of(
		merge_partials(&memory, &PartialMerge::KeyedFold, partials, &coordinator)
			.await
			.expect("a merge"),
	);
	let functions = statement_state(program, &coordinator).expect("a state").aggregates.functions
		[aggregate as usize]
		.clone();
	let batch = coordinator.root().ctx.config.exec.scan_batch_size.max(1);
	let mut fold = SavedFold::new(
		merge,
		program.aggregates[aggregate as usize].keys,
		functions,
		batch,
		memory.working() / 2,
	);
	let mut rows = Vec::new();
	while let Some(chunk) = fold.next_chunk(&coordinator).await.expect("a chunk") {
		rows.extend(
			finish_groups(program, aggregate, Some(chunk), &coordinator).await.expect("finishes"),
		);
	}
	rows
}

/// `GROUP ALL` folds from the one group each producer saves, under an empty
/// key.
#[tokio::test]
async fn b_group_all_folded_from_partials_answers_the_serial_group() {
	let sql = "SELECT count() AS c, array::group(k) AS ks, math::sum(n) AS s, \
		 array::min(pad) AS p FROM t GROUP ALL";
	let free = free().await;
	let (db, store) = bounded().await;
	let free_ctx = free.exec_ctx().await;
	let rows = scan_rows(&free).await;
	let program = Arc::new(program_of(&free_ctx, sql).await);
	let at = scan_pipeline(&program);
	let aggregate = body_breakers(&program.pipelines[at].code).aggregates[0];
	let mut state = run_morsel(&program, at, rows.clone(), &free_ctx).await.expect("a morsel");
	let groups = state.groups[aggregate as usize].take();
	let want = finish_groups(&program, aggregate, groups, &free_ctx).await.expect("finishes");
	assert_eq!(want.len(), 1);
	drop(state);
	for n in NS {
		let mut partials = Vec::new();
		for morsel in morsels(&rows, n) {
			let ctx = db.exec_ctx().await;
			let mut state = run_morsel(&program, at, morsel, &ctx).await.expect("a morsel");
			partials.push(Partial::of_groups(&mut state, &program, aggregate).expect("saved"));
		}
		assert_eq!(partials.len(), n);
		for partial in &partials {
			assert_eq!(partial.held.len(), 1);
			assert!(partial.held[0].0.is_empty(), "GROUP ALL's group has a key");
		}
		assert_eq!(folded(&db, &program, aggregate, partials).await, want, "{n} producers");
	}
	assert_tidy(&store);
}

/// A `GROUP ALL` over aggregates that answer differently on no rows, on one
/// row and on many: counts, sums, a mean, extremes and array aggregates.
const GROUP_ALL_AGGREGATES: &str = "SELECT count() AS c, count(k > 3) AS ck, math::sum(n) AS s, \
	 math::mean(n) AS m, math::min(n) AS lo, math::max(n) AS hi, array::group(k) AS ks, \
	 array::min(pad) AS p FROM t GROUP ALL";

/// A `GROUP ALL` no producer saw a row of still answers its one group, as the
/// serial run does.
#[tokio::test]
async fn b_group_all_folded_from_empty_producers_answers_the_serial_group() {
	let sql = GROUP_ALL_AGGREGATES;
	let free = free().await;
	let (db, store) = bounded().await;
	let free_ctx = free.exec_ctx().await;
	let program = Arc::new(program_of(&free_ctx, sql).await);
	let at = scan_pipeline(&program);
	let aggregate = body_breakers(&program.pipelines[at].code).aggregates[0];
	let mut state = run_morsel(&program, at, Vec::new(), &free_ctx).await.expect("a morsel");
	let groups = state.groups[aggregate as usize].take();
	let want = finish_groups(&program, aggregate, groups, &free_ctx).await.expect("finishes");
	assert_eq!(want.len(), 1);
	drop(state);
	for n in NS {
		let mut partials = Vec::new();
		for _ in 0..n {
			let ctx = db.exec_ctx().await;
			let mut state = run_morsel(&program, at, Vec::new(), &ctx).await.expect("a morsel");
			partials.push(Partial::of_groups(&mut state, &program, aggregate).expect("saved"));
		}
		for partial in &partials {
			assert_eq!(partial.held.len(), 1);
		}
		assert_eq!(folded(&db, &program, aggregate, partials).await, want, "{n} producers");
	}
	assert_tidy(&store);
}

/// `rows` over `n` producers in each arrangement that leaves some of them
/// with no row, by name: the empty ones first, last, alternating with the
/// rest, and all but one.
fn arrangements(rows: &[Value], n: usize) -> Vec<(String, Vec<Vec<Value>>)> {
	if n == 1 {
		return vec![("all rows".to_owned(), vec![rows.to_vec()])];
	}
	let mut all = Vec::new();
	let mut first = vec![Vec::new()];
	first.extend(morsels(rows, n - 1));
	all.push(("empty first".to_owned(), first));
	let mut last = morsels(rows, n - 1);
	last.push(Vec::new());
	all.push(("empty last".to_owned(), last));
	let mut alternating = Vec::new();
	for part in morsels(rows, n.div_ceil(2)) {
		alternating.push(part);
		alternating.push(Vec::new());
	}
	alternating.truncate(n);
	all.push(("alternating".to_owned(), alternating));
	for only in [0, n / 2, n - 1] {
		let mut producers = vec![Vec::new(); n];
		producers[only] = rows.to_vec();
		all.push((format!("only producer {only}"), producers));
	}
	all
}

/// A `GROUP ALL` some producers saw rows of and some did not folds to the
/// group the serial run answers, wherever the empty producers sit.
#[tokio::test]
async fn b_group_all_folded_from_mixed_producers_answers_the_serial_group() {
	let sql = GROUP_ALL_AGGREGATES;
	let free = free().await;
	let (db, store) = bounded().await;
	let free_ctx = free.exec_ctx().await;
	let rows = scan_rows(&free).await;
	let program = Arc::new(program_of(&free_ctx, sql).await);
	let at = scan_pipeline(&program);
	let aggregate = body_breakers(&program.pipelines[at].code).aggregates[0];
	let mut state = run_morsel(&program, at, rows.clone(), &free_ctx).await.expect("a morsel");
	let groups = state.groups[aggregate as usize].take();
	let want = finish_groups(&program, aggregate, groups, &free_ctx).await.expect("finishes");
	assert_eq!(want.len(), 1);
	drop(state);
	for n in NS {
		for (name, producers) in arrangements(&rows, n) {
			let count = producers.len();
			let mut partials = Vec::new();
			for morsel in producers {
				let ctx = db.exec_ctx().await;
				let mut state = run_morsel(&program, at, morsel, &ctx).await.expect("a morsel");
				partials.push(Partial::of_groups(&mut state, &program, aggregate).expect("saved"));
			}
			for partial in &partials {
				assert_eq!(partial.held.len(), 1);
			}
			assert_eq!(
				folded(&db, &program, aggregate, partials).await,
				want,
				"{name} over {count} producers"
			);
		}
	}
	assert_tidy(&store);
}

/// Batches that outgrow a lease of a few KiB hand the merge partials, and
/// neither the batch nor the merge lets the lease count less than the query
/// holds: a batch keeps what its buffers reserved until its partial's records
/// are charged in their place, and the statement charges the sorts and groups
/// it takes, and spills what the lease refuses, before the batch's reservation
/// is let go.
async fn lease_stays_counted(sql: &str, kind: BreakerKind) {
	let free = free().await;
	let (db, store) = bounded().await;
	let free_ctx = free.exec_ctx().await;
	let rows = scan_rows(&free).await;
	let program = Arc::new(program_of(&free_ctx, sql).await);
	let at = scan_pipeline(&program);
	let ctx = db.exec_ctx().await;
	let mut statement = statement_state(&program, &ctx).expect("a state");
	for batch in morsels(&rows, 4) {
		merge_batch(&program, at, batch, &mut statement, &ctx).await.expect("a merged batch");
	}
	let session = store.session();
	assert!(batch_partials(session, kind) >= 4, "`{sql}`: its batches did not all spill");
	assert_eq!(
		lease_faults(session, LeaseFault::Released),
		0,
		"`{sql}`: a batch released its lease"
	);
	assert_eq!(
		lease_faults(session, LeaseFault::Uncounted),
		0,
		"`{sql}`: the merge let the batch go before charging the statement"
	);
	drop(statement);
	assert_eq!(store.runs(), 0, "a spilled run outlived its query");
}

#[tokio::test]
async fn b_a_sorting_batch_keeps_its_lease_counted_through_the_merge() {
	lease_stays_counted("SELECT n, pad FROM t ORDER BY pad, n DESC", BreakerKind::Sort).await;
}

#[tokio::test]
async fn b_a_grouping_batch_keeps_its_lease_counted_through_the_merge() {
	lease_stays_counted(
		"SELECT n % 300 AS b, math::stddev(n) AS sd, math::variance(n) AS va, \
		 math::median(n) AS md FROM t GROUP BY b",
		BreakerKind::Groups,
	)
	.await;
}

/// The rows a distinct test feeds: many duplicates across morsels, numerics
/// of three kinds that compare equal, and strings.
fn distinct_rows() -> Vec<Value> {
	(0..3000usize)
		.map(|i| match i % 4 {
			0 => Value::from((i % 300) as i64),
			1 => Value::Number(Number::Float((i % 300) as f64)),
			2 => Value::Number(Number::Decimal((i % 300).to_string().parse().expect("a decimal"))),
			_ => Value::from(format!("s{}-{}", i % 41, "y".repeat(40))),
		})
		.collect()
}

/// The rows a serial distinct keeps, or the error it raises past `max_rows`.
fn serial_distinct(rows: &[Value], max_rows: usize) -> Result<Vec<Value>, String> {
	let mut seen = SeenSet::new();
	let mut kept = Vec::new();
	for row in rows {
		if seen.insert(row, max_rows).map_err(text)? {
			kept.push(row.clone());
		}
	}
	Ok(kept)
}

/// The rows a distinct merged from `n` producers' partials keeps, and
/// whether a partial carried a run.
async fn distinct_merged(
	db: &TestDb,
	rows: &[Value],
	n: usize,
	max_rows: usize,
) -> (Result<Vec<Value>, String>, bool) {
	let mut partials = Vec::new();
	for (producer, morsel) in morsels(rows, n).into_iter().enumerate() {
		let ctx = db.exec_ctx().await;
		let mut producer = DistinctProducer::new(producer as u32, &ctx);
		for row in morsel {
			producer.take(row, &ctx).await.expect("a row");
		}
		partials.push(producer.into_partial());
	}
	let carried = partials.iter().any(|p| !p.runs.is_empty());
	let coordinator = db.exec_ctx().await;
	let how = PartialMerge::DistinctFirst {
		max_rows,
	};
	let merged = match merge_partials(&memory_of(&coordinator), &how, partials, &coordinator).await
	{
		Ok(merged) => merged,
		Err(flow) => return (Err(text(flow)), carried),
	};
	let mut merge = records_of(merged);
	let mut kept = Vec::new();
	loop {
		match merge.next_chunk(1000, &coordinator).await {
			Ok(chunk) if chunk.is_empty() => return (Ok(kept), carried),
			Ok(chunk) => kept.extend(chunk.into_iter().map(|(_, row)| row)),
			Err(flow) => return (Err(text(flow)), carried),
		}
	}
}

#[tokio::test]
async fn b_distinct_from_partials_keeps_the_serial_first_rows() {
	let (db, store) = bounded().await;
	let rows = distinct_rows();
	let want = serial_distinct(&rows, usize::MAX).expect("no cap");
	for n in NS {
		let before = store.runs_written();
		let (got, carried) = distinct_merged(&db, &rows, n, usize::MAX).await;
		assert_eq!(got.expect("a merge"), want, "{n} producers");
		assert!(store.runs_written() > before, "{n} producers wrote no run");
		assert!(carried, "{n} producers carried no run");
	}
	assert_tidy(&store);
}

#[tokio::test]
async fn b_distinct_from_partials_fails_past_the_cap_as_serial() {
	let (db, store) = bounded().await;
	let rows = distinct_rows();
	let distinct = serial_distinct(&rows, usize::MAX).expect("no cap").len();
	let cap = distinct - 1;
	let error = serial_distinct(&rows, cap).expect_err("a cap below the distinct rows");
	for n in NS {
		let (got, _) = distinct_merged(&db, &rows, n, cap).await;
		assert_eq!(got.expect_err("a merge past the cap"), error, "{n} producers");
		let (got, _) = distinct_merged(&db, &rows, n, distinct).await;
		assert_eq!(got.expect("a merge at the cap").len(), distinct, "{n} producers");
	}
	assert_tidy(&store);
}

/// The value a join key column takes for row `i`: a number of one of three
/// kinds that compare equal, or a string; every fiftieth fifth row's key is a
/// regex.
fn key_of(i: usize, numbers: usize) -> Value {
	let string = || Value::from(format!("k{}", i % 17));
	match i % 5 {
		0 => Value::from((i % numbers) as i64),
		1 => Value::Number(Number::Float((i % numbers) as f64)),
		2 => Value::Number(Number::Decimal((i % numbers).to_string().parse().expect("a decimal"))),
		3 => string(),
		_ if i % 50 == 4 => Value::Regex(Regex::from_str("^k1").expect("a regex")),
		_ => string(),
	}
}

fn object_of(field: &str, i: usize, pad: bool) -> Value {
	let mut object = Object::default();
	object.insert(field.to_owned(), Value::from(i as i64));
	if pad {
		object.insert("pad".to_owned(), Value::from("z".repeat(48)));
	}
	Value::Object(object)
}

fn build_rows() -> Vec<(JoinKey, Value)> {
	(0..1500).map(|i| (vec![key_of(i, 60)], object_of("bb", i, true))).collect()
}

fn probe_rows() -> Vec<(Option<JoinKey>, Value)> {
	(0..700)
		.map(|j| {
			let key = if j % 37 == 0 {
				None
			} else if j % 41 == 0 {
				Some(vec![Value::Regex(Regex::from_str("^k2").expect("a regex"))])
			} else {
				Some(vec![key_of(j, 70)])
			};
			(key, object_of("pp", j, false))
		})
		.collect()
}

fn join_step(join_type: JoinType) -> JoinStep<u16, super::program::HostScalar> {
	JoinStep {
		build: 0u16,
		keys: vec!["k".into()],
		join_type,
		null_template: vec!["bb".into()],
		residual: None,
	}
}

/// Spilled joins built from the partials of morsels of the build side pair
/// every probe row as the in-memory join does.
#[tokio::test]
async fn b_join_from_partials_pairs_as_the_in_memory_join() {
	let free = free().await;
	let (db, store) = bounded().await;
	let free_ctx = free.exec_ctx().await;
	let any_program = program_of(&free_ctx, "SELECT * FROM t").await;
	let builds = build_rows();
	let probes = probe_rows();
	for join_type in [JoinType::Inner, JoinType::Left, JoinType::Semi, JoinType::Anti] {
		let step = join_step(join_type);
		let mut table = BuildTable::new();
		for (key, row) in &builds {
			table.insert(key.clone(), row.clone(), usize::MAX).expect("a build row");
		}
		let mut want = Vec::new();
		let mut emitted = 0;
		let mut state = None;
		let mut judge = HostJudge {
			program: &any_program,
			scalar: None,
			ctx: &free_ctx,
			state: &mut state,
		};
		for (key, row) in &probes {
			let matches: Vec<&Value> = key.as_ref().map_or(vec![], |k| table.matches(k).collect());
			want.extend(
				pairings(&step, &matches, row.clone(), &mut judge, &mut emitted, usize::MAX)
					.await
					.expect("pairs"),
			);
		}
		assert!(!want.is_empty(), "{join_type:?} pairs nothing");
		for n in NS {
			let before = store.runs_written();
			let mut partials = Vec::new();
			for (producer, morsel) in morsels(&builds, n).into_iter().enumerate() {
				let ctx = db.exec_ctx().await;
				let mut sink =
					BuildSink::with_width(1, usize::MAX, 0, &ctx).producing(producer as u32);
				for (key, row) in morsel {
					sink.push(key, row, &ctx).await.expect("a build row");
				}
				partials.push(sink.into_partial().expect("a partial"));
			}
			let carried = partials.iter().any(|p| !p.runs.is_empty());
			let coordinator = db.exec_ctx().await;
			let how = PartialMerge::JoinBuild {
				max_rows: usize::MAX,
			};
			let Merged::Join(mut spilled) =
				merge_partials(&memory_of(&coordinator), &how, partials, &coordinator)
					.await
					.expect("a merge")
			else {
				panic!("records where a join was merged");
			};
			let mut emitted = 0;
			let mut state = None;
			let mut pairing = Pairing {
				step: &step,
				residual: HostJudge {
					program: &any_program,
					scalar: None,
					ctx: &coordinator,
					state: &mut state,
				},
				emitted: &mut emitted,
				max_output_rows: usize::MAX,
			};
			for (key, row) in &probes {
				spilled.defer(key.clone(), row.clone(), &mut pairing).await.expect("defers");
			}
			let merge = spilled.finish(&mut pairing).await.expect("finishes");
			let got: Vec<Value> =
				drained(merge, &coordinator).await.into_iter().map(|(_, row)| row).collect();
			assert_eq!(got, want, "{join_type:?} from {n} producers");
			assert!(store.runs_written() > before, "{join_type:?}: {n} producers wrote no run");
			assert!(carried, "{join_type:?}: {n} producers carried no run");
		}
	}
	assert_eq!(store.runs(), 0, "a spilled run outlived its query");
}

/// A build side past its cap fails as the in-memory table does, before any
/// probe row is deferred.
#[tokio::test]
async fn b_join_from_partials_fails_past_the_build_cap_as_serial() {
	let (db, store) = bounded().await;
	let builds = build_rows();
	let cap = builds.len() - 1;
	let mut table = BuildTable::new();
	let mut error = None;
	for (key, row) in &builds {
		if let Err(flow) = table.insert(key.clone(), row.clone(), cap) {
			error = Some(text(flow));
			break;
		}
	}
	let error = error.expect("a table past its cap fails");
	let mut partials = Vec::new();
	for (producer, morsel) in morsels(&builds, 3).into_iter().enumerate() {
		let ctx = db.exec_ctx().await;
		let mut sink = BuildSink::with_width(1, cap, 0, &ctx).producing(producer as u32);
		for (key, row) in morsel {
			sink.push(key, row, &ctx).await.expect("a producer under the cap");
		}
		partials.push(sink.into_partial().expect("a partial"));
	}
	let coordinator = db.exec_ctx().await;
	let how = PartialMerge::JoinBuild {
		max_rows: cap,
	};
	match merge_partials(&memory_of(&coordinator), &how, partials, &coordinator).await {
		Err(flow) => assert_eq!(text(flow), error),
		Ok(_) => panic!("a build side past its cap merged"),
	}
	assert_eq!(store.runs(), 0, "a spilled run outlived its query");
}

/// Partials of two breakers, or merged as another breaker, are refused.
#[tokio::test]
async fn b_partials_of_two_breakers_do_not_merge() {
	let free = free().await;
	let (db, store) = bounded().await;
	let free_ctx = free.exec_ctx().await;
	let rows = scan_rows(&free).await;
	let sorted = Arc::new(program_of(&free_ctx, SORTS[0]).await);
	let grouped = Arc::new(program_of(&free_ctx, GROUPS[0].0).await);
	let (at, sort) = (
		scan_pipeline(&sorted),
		body_breakers(&sorted.pipelines[scan_pipeline(&sorted)].code).sorts[0],
	);
	let mut state = run_morsel(&sorted, at, rows.clone(), &free_ctx).await.expect("a morsel");
	let sort_partial =
		Partial::of_sort(&mut state, &sorted, sort, &free_ctx).await.expect("a partial");
	let at = scan_pipeline(&grouped);
	let aggregate = body_breakers(&grouped.pipelines[at].code).aggregates[0];
	let mut state = run_morsel(&grouped, at, rows.clone(), &free_ctx).await.expect("a morsel");
	let group_partial = Partial::of_groups(&mut state, &grouped, aggregate).expect("saved");
	let coordinator = db.exec_ctx().await;
	let memory = memory_of(&coordinator);
	let sorted_by = PartialMerge::SortMerge {
		keys: sorted.sorts[sort as usize].keys.clone(),
		keep: None,
	};
	let Err(flow) =
		merge_partials(&memory, &sorted_by, vec![sort_partial, group_partial], &coordinator).await
	else {
		panic!("partials of two breakers merged");
	};
	assert!(text(flow).contains("Internal error"));
	let mut state =
		run_morsel(&sorted, scan_pipeline(&sorted), rows, &free_ctx).await.expect("a morsel");
	let sort_partial =
		Partial::of_sort(&mut state, &sorted, sort, &free_ctx).await.expect("a partial");
	let Err(flow) =
		merge_partials(&memory, &PartialMerge::KeyedFold, vec![sort_partial], &coordinator).await
	else {
		panic!("a sort merged as groups");
	};
	assert!(text(flow).contains("Internal error"));
	assert_eq!(store.runs(), 0);
}

/// Two query memories over handles on one backend.
fn two_handles() -> (Arc<MemTempStore>, Arc<QueryMemory>, Arc<QueryMemory>) {
	let (a, memory_a, b) = two_handles_of_one_backend();
	(a, memory_a, memory_over(b))
}

/// A query memory over `store`, with no quota of its own.
fn memory_over(store: Arc<dyn TempStore>) -> Arc<QueryMemory> {
	MemoryHost {
		governor: Arc::new(Unlimited),
		store: Some(store),
		settings: MemorySettings {
			floor: 1 << 20,
			fan_in: 4,
			chunk: 4096,
			wait: std::time::Duration::ZERO,
			max: usize::MAX,
		},
	}
	.query()
}

/// The first handle, a query memory over it, and the second handle.
fn two_handles_of_one_backend() -> (Arc<MemTempStore>, Arc<QueryMemory>, Arc<dyn TempStore>) {
	let (a, b) = MemTempStore::shared(1024, None);
	let a = Arc::new(a);
	let memory_a = memory_over(Arc::clone(&a) as Arc<dyn TempStore>);
	(a, memory_a, Arc::new(b))
}

fn sort_spec() -> RunSpec {
	RunSpec::new(BreakerKind::Sort, 0, 1, RunOrder::Keys(vec![1]))
}

/// Records sorted by key, stable, `count` of them, telling `source` apart.
fn records(source: i64, count: i64) -> Vec<Record> {
	let mut records: Vec<Record> =
		(0..count).map(|i| (vec![Value::from(i % 9)], Value::from(source * 1000 + i))).collect();
	records.sort_by(|(a, _), (b, _)| a.cmp(b));
	records
}

/// A partial of two shared runs and some held records, written through
/// `memory`.
async fn partial_of(
	memory: &Arc<QueryMemory>,
	ctx: &ExecutionContext,
	sharing: Sharing,
) -> Partial {
	let mut runs = super::spill::Runs::default();
	for source in 0..2 {
		let run = write_run_as(memory, sort_spec(), records(source, 300), sharing, ctx)
			.await
			.expect("a run");
		runs.push(run).expect("one breaker");
	}
	Partial {
		spec: sort_spec(),
		runs,
		held: records(2, 50),
		side: Vec::new(),
		counts: Default::default(),
	}
}

/// A partial sealed on one handle and sent by its bytes merges on another as
/// it merges where it was written.
#[tokio::test]
async fn b_a_partial_travels_by_wire_between_two_handles() {
	let db = TestDb::new_with_config("", small(4)).await;
	let ctx = db.exec_ctx().await;
	let (a, memory_a, memory_b) = two_handles();
	let ascending = super::program::KeyOrder {
		ascending: true,
		collate: false,
		numeric: false,
	};
	let how = PartialMerge::SortMerge {
		keys: vec![ascending],
		keep: None,
	};
	let sent = partial_of(&memory_a, &ctx, Sharing::Shared)
		.await
		.seal(&memory_a, Sharing::Shared, &ctx)
		.await
		.expect("sealed");
	assert_eq!(sent.runs.len(), 3);
	let bytes = sent.to_wire().expect("a partial that travels");
	let received = Partial::from_wire(&bytes, &memory_b).expect("a wire partial");
	assert!(received.held.is_empty());
	assert_eq!(received.runs.len(), 3);
	let there =
		records_of(merge_partials(&memory_b, &how, vec![received], &ctx).await.expect("a merge"));
	let there = drained(there, &ctx).await;
	let here = partial_of(&memory_a, &ctx, Sharing::Private).await;
	let here =
		records_of(merge_partials(&memory_a, &how, vec![here], &ctx).await.expect("a merge"));
	let here = drained(here, &ctx).await;
	assert_eq!(there.len(), 650);
	assert_eq!(there, here);
	assert_eq!(a.runs(), 0, "a run outlived its partial");
}

/// A store handle that answers a quota of its own over another handle's runs.
struct Capped {
	store: Arc<dyn TempStore>,
	quota: u64,
}

impl TempStore for Capped {
	fn part_size(&self) -> usize {
		self.store.part_size()
	}

	fn quota(&self) -> Option<u64> {
		Some(self.quota)
	}

	fn session(&self) -> &SessionName {
		self.store.session()
	}

	fn shares(&self) -> bool {
		self.store.shares()
	}

	fn write_run(&self, sharing: Sharing) -> BoxFut<'_, anyhow::Result<Box<dyn RunWrite>>> {
		self.store.write_run(sharing)
	}

	fn read_run(&self, run: &RunRef) -> BoxFut<'_, anyhow::Result<Box<dyn RunRead>>> {
		self.store.read_run(run)
	}

	fn delete_run(&self, run: &RunRef) {
		self.store.delete_run(run)
	}
}

/// A receiver that cannot take every run of a wire partial adopts none of them
/// and leaves none behind in the shared store.
#[tokio::test]
async fn b_a_wire_partial_past_the_receivers_quota_leaves_no_runs() {
	let db = TestDb::new_with_config("", small(4)).await;
	let ctx = db.exec_ctx().await;
	let (a, memory_a, handle) = two_handles_of_one_backend();
	let sent = partial_of(&memory_a, &ctx, Sharing::Shared)
		.await
		.seal(&memory_a, Sharing::Shared, &ctx)
		.await
		.expect("sealed");
	assert_eq!(sent.runs.len(), 3);
	let first = sent.runs.runs()[0].run_ref().bytes;
	let bytes = sent.to_wire().expect("a partial that travels");
	assert_eq!(a.runs(), 3);
	let receiver = MemoryHost {
		governor: Arc::new(Unlimited),
		store: Some(Arc::new(Capped {
			store: handle,
			quota: first + 1,
		}) as Arc<dyn TempStore>),
		settings: MemorySettings {
			floor: 1 << 20,
			fan_in: 4,
			chunk: 4096,
			wait: std::time::Duration::ZERO,
			max: usize::MAX,
		},
	}
	.query();
	let error =
		Partial::from_wire(&bytes, &receiver).expect_err("a run past the quota").to_string();
	assert!(error.contains("quota"), "{error}");
	assert_eq!(a.runs(), 0, "a run outlived the refused partial");
	assert_eq!(receiver.spilled_bytes(), 0, "a refused run stayed counted");
}

/// A partial whose runs only its writer reads, or that holds records in
/// memory, is refused, and its runs are still deleted.
#[tokio::test]
async fn b_a_partial_of_local_runs_does_not_travel() {
	let db = TestDb::new_with_config("", small(4)).await;
	let ctx = db.exec_ctx().await;
	let store = Arc::new(MemTempStore::new(1024, None));
	let memory = MemoryHost {
		governor: Arc::new(Unlimited),
		store: Some(Arc::clone(&store) as Arc<dyn TempStore>),
		settings: MemorySettings {
			floor: 1 << 20,
			fan_in: 4,
			chunk: 4096,
			wait: std::time::Duration::ZERO,
			max: usize::MAX,
		},
	}
	.query();
	let mut partial = partial_of(&memory, &ctx, Sharing::Private).await;
	let held = std::mem::take(&mut partial.held);
	let error = partial.to_wire().expect_err("a local run travelled").to_string();
	assert!(error.contains("only this process can read"), "{error}");
	assert_eq!(store.runs(), 0, "a refused partial's runs outlived it");
	let mut partial = partial_of(&memory, &ctx, Sharing::Private).await;
	partial.held = held;
	let error = partial.to_wire().expect_err("held records travelled").to_string();
	assert!(error.contains("seal it first"), "{error}");
	assert_eq!(store.runs(), 0, "a refused partial's runs outlived it");
}
