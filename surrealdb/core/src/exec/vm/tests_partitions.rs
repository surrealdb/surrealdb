//! Range partitions of partials: what a producer hands a coordinator for one
//! breaker, divided by sampled splitters, finished a partition at a time and
//! put together as the unpartitioned finish answers it. Keys here take many
//! representations of one value, so a route that parted equal keys would
//! change an answer.

use std::str::FromStr;
use std::sync::Arc;

use rust_decimal::Decimal;

use super::binding::{HostJudge, JoinStep, pairings};
use super::distinct_spill::{DistinctProducer, by_keys};
use super::host::{finish_groups, statement_state};
use super::join_spill::{BuildSink, Pairing};
use super::partial::Partial;
use super::partition::{
	Partitioned, PartitionedJoin, Splitters, finish_distinct_partition, merge_distinct_outputs,
};
use super::program::{HostScalar, Program};
use super::region::{body_breakers, run_morsel};
use super::tests_memory::{answer, bounded_on};
use super::tests_partials::{
	assert_tidy, drained, join_step, memory_of, morsels, object_of, scan_pipeline, serial_distinct,
	small, text,
};
use super::tests_sources::program_of;
use crate::exec::ExecutionContext;
use crate::exec::memory::{FairGovernor, MemTempStore, Sharing};
use crate::exec::operators::join::hash_join::{BuildTable, JoinKey, JoinType};
use crate::exec::operators::test_util::TestDb;
use crate::val::{Array, Number, Object, RecordId, RecordIdKey, Regex, TableName, Value};

/// Rows of `u` in three blocks of 500 (`j` is the block). In every block, key
/// `k` takes each of eleven representations of each of thirty classes: one
/// number as int, float and decimal; one array and one object in two
/// representations each; NONE; NULL; and a string in lower and upper case.
/// `pad` makes groups outgrow a small budget.
const SETUP_C: &str = "
	DEFINE TABLE u;
	FOR $b IN 0..3 {
		FOR $x IN 0..500 {
			LET $i = $b * 500 + $x;
			LET $c = $i % 30;
			CREATE type::record('u', $i) SET
				n = $i,
				k = [$c, <float> $c, <decimal> $c, [$c, <float> $c], [<decimal> $c, $c],
					{ a: $c }, { a: <float> $c }, NONE, NULL,
					'key' + <string> $c, 'KEY' + <string> $c][$i % 11],
				j = $b,
				pad = string::repeat('x', 200 + $i % 13)
		}
	};
";

const PRODUCERS: usize = 3;
const PARTITIONS: [usize; 3] = [1, 2, 7];

/// A database of [`SETUP_C`] under a small budget, and the store it spills to.
async fn bounded_c() -> (TestDb, Arc<MemTempStore>) {
	let store = Arc::new(MemTempStore::new(1024, None));
	let governor = Arc::new(FairGovernor::new(16 << 10));
	let config = small(4).with_key_value("memory_reserve_chunk", "1024");
	let db = bounded_on(governor, Arc::clone(&store), SETUP_C, config).await;
	(db, store)
}

/// A database of [`SETUP_C`] with memory to spare.
async fn free_c() -> TestDb {
	TestDb::new_with_config(SETUP_C, small(4)).await
}

/// The rows of `u` in scan order.
async fn scan_rows_c(free: &TestDb) -> Vec<Value> {
	let rows = answer(free, "SELECT * FROM u", None).await.expect("the table reads");
	assert_eq!(rows.len(), 1500);
	rows
}

/// The records `part`'s producers hold, in runs and in memory.
fn records_in(part: &[Partial]) -> u64 {
	part.iter().map(|p| p.runs.total_records() + p.held.len() as u64).sum()
}

/// Splitters of at most `n` partitions drawn from every other route key of
/// `partials`.
async fn splitters_of(partials: &[Partial], n: usize, ctx: &ExecutionContext) -> Splitters {
	let mut samples = Vec::new();
	for partial in partials {
		samples.push(partial.sample(2, ctx).await.expect("a sample"));
	}
	Splitters::from_samples(samples, n)
}

fn float(f: f64) -> Value {
	Value::Number(Number::Float(f))
}

fn dec(s: &str) -> Value {
	Value::Number(Number::Decimal(Decimal::from_str(s).expect("a decimal")))
}

fn arr(values: Vec<Value>) -> Value {
	Value::Array(Array(values))
}

fn rid(values: Vec<Value>) -> Value {
	Value::RecordId(RecordId {
		table: TableName::new("r"),
		key: RecordIdKey::Array(Array(values)),
	})
}

/// An object `{a: value}`.
fn object_a(value: Value) -> Value {
	let mut object = Object::default();
	object.insert("a".to_owned(), value);
	Value::Object(object)
}

fn pad() -> Value {
	Value::from("p".repeat(40))
}

/// Class `c` in representation `v` (taken modulo twelve): numbers of three
/// kinds, arrays, objects and record ids holding them, and strings in two
/// cases, each padded so a few hundred outgrow a small budget.
fn class_value(c: usize, v: usize) -> Value {
	let n = c as i64;
	let object = |a: Value, b: Value| {
		let mut object = Object::default();
		object.insert("a".to_owned(), a);
		object.insert("b".to_owned(), b);
		object.insert("p".to_owned(), pad());
		Value::Object(object)
	};
	match v % 12 {
		0 => Value::from(n),
		1 if c == 0 => float(-0.0),
		1 => float(c as f64),
		2 => dec(&c.to_string()),
		3 => dec(&format!("{c}.00")),
		4 => arr(vec![Value::from(n), arr(vec![float(c as f64)]), pad()]),
		5 => arr(vec![dec(&c.to_string()), arr(vec![Value::from(n)]), pad()]),
		6 => object(Value::from(n), arr(vec![float(c as f64)])),
		7 => object(dec(&c.to_string()), arr(vec![Value::from(n)])),
		8 => rid(vec![Value::from(n)]),
		9 => rid(vec![float(c as f64)]),
		10 => Value::from(format!("key{c}-{}", "p".repeat(40))),
		_ => Value::from(format!("KEY{c}-{}", "p".repeat(40))),
	}
}

/// 3000 rows with many repeats of a value in different representations, and
/// a few NONE, NULL and 0.1 in two kinds.
fn distinct_rows_c() -> Vec<Value> {
	(0..3000usize)
		.map(|i| {
			if i % 97 == 0 {
				Value::None
			} else if i % 89 == 0 {
				Value::Null
			} else if i % 83 == 0 {
				float(0.1)
			} else if i % 79 == 0 {
				dec("0.1")
			} else {
				class_value(i % 60, (i / 7) % 12)
			}
		})
		.collect()
}

/// The partials of the aggregate of `program`, one per contiguous morsel of
/// `rows`, each run on a context of `db`.
async fn groups_partials(
	db: &TestDb,
	program: &Arc<Program>,
	rows: &[Value],
) -> (u16, Vec<Partial>) {
	let at = scan_pipeline(program);
	let aggregate = body_breakers(&program.pipelines[at].code).aggregates[0];
	let mut partials = Vec::new();
	for morsel in morsels(rows, PRODUCERS) {
		let ctx = db.exec_ctx().await;
		let mut state = run_morsel(program, at, morsel, &ctx).await.expect("a morsel");
		partials.push(Partial::of_groups(&mut state, program, aggregate).expect("saved"));
	}
	(aggregate, partials)
}

/// The rows the aggregate of `program` answers over every row of `rows` on
/// one state.
async fn serial_groups(free: &TestDb, program: &Arc<Program>, rows: &[Value]) -> Vec<Value> {
	let ctx = free.exec_ctx().await;
	let at = scan_pipeline(program);
	let aggregate = body_breakers(&program.pipelines[at].code).aggregates[0];
	let mut state = run_morsel(program, at, rows.to_vec(), &ctx).await.expect("a morsel");
	let groups = state.groups[aggregate as usize].take();
	finish_groups(program, aggregate, groups, &ctx).await.expect("finishes")
}

/// The rows aggregate `aggregate` finishes into, folded from `partitioned`
/// partition by partition.
async fn folded_in_order(
	program: &Arc<Program>,
	aggregate: u16,
	partitioned: Partitioned,
	coordinator: &ExecutionContext,
) -> Vec<Value> {
	let memory = memory_of(coordinator);
	let functions = statement_state(program, coordinator).expect("a state").aggregates.functions
		[aggregate as usize]
		.clone();
	let batch = coordinator.root().ctx.config.exec.scan_batch_size.max(1);
	let mut fold = partitioned.finish_in_order(
		&memory,
		program.aggregates[aggregate as usize].keys,
		functions,
		batch,
		memory.working() / 2,
	);
	let mut rows = Vec::new();
	while let Some(chunk) = fold.next_chunk(coordinator).await.expect("a chunk") {
		rows.extend(
			finish_groups(program, aggregate, Some(chunk), coordinator).await.expect("finishes"),
		);
	}
	rows
}

const G1: &str = "SELECT k, count() AS c, math::sum(n) AS s, math::min(n) AS lo, \
	 array::group(n) AS ns, pad FROM u GROUP BY k";
const G2: &str = "SELECT k, j, pad, count() AS c, math::sum(n) AS s FROM u GROUP BY k, j";

/// A producer's partial, once split, holds only the records that route to
/// each partition, and every record exactly once.
#[tokio::test]
async fn c_a_split_partial_holds_only_its_partitions_records() {
	let free = free_c().await;
	let (db, store) = bounded_c().await;
	let free_ctx = free.exec_ctx().await;
	let rows = scan_rows_c(&free).await;
	let program = Arc::new(program_of(&free_ctx, G2).await);
	let (_, partials) = groups_partials(&db, &program, &rows).await;
	assert!(partials.iter().any(|p| !p.runs.is_empty()), "no producer spilled");
	let splitters = Splitters::new(vec![
		vec![float(5.0), Value::from(1)],
		vec![arr(vec![dec("3"), Value::from(3)]), Value::from(0)],
		vec![object_a(float(7.0)), Value::from(2)],
		vec![Value::None, Value::from(1)],
		vec![Value::from("KEY3"), Value::from(0)],
	]);
	assert_eq!(splitters.partitions(), 6);
	let coordinator = db.exec_ctx().await;
	let memory = memory_of(&coordinator);
	for partial in partials {
		let records = partial.runs.total_records() + partial.held.len() as u64;
		let parts = partial
			.split(&memory, &splitters, Sharing::Private, &coordinator)
			.await
			.expect("a split");
		assert_eq!(parts.len(), 6);
		assert_eq!(parts.iter().map(|part| part.counts.taken).sum::<u64>(), records);
		for (at, part) in parts.into_iter().enumerate() {
			let merge = part
				.runs
				.merge(&memory, part.held, by_keys(), None, &coordinator)
				.await
				.expect("a merge");
			for (keys, _) in drained(merge, &coordinator).await {
				assert_eq!(splitters.route(&keys[..2]), at, "{keys:?}");
			}
		}
	}

	let ctx = db.exec_ctx().await;
	let mut sink = BuildSink::with_width(1, usize::MAX, 0, &ctx).producing(0);
	for (key, row) in build_rows_c() {
		sink.push(key, row, &ctx).await.expect("a build row");
	}
	let partial = sink.into_partial().expect("a partial");
	let (taken, side) = (partial.counts.taken, partial.side.len());
	assert!(side > 0, "no regex build row");
	let splitters = Splitters::new(vec![
		vec![float(5.0)],
		vec![Value::None],
		vec![Value::from(format!("KEY3-{}", "p".repeat(40)))],
	]);
	assert_eq!(splitters.partitions(), 4);
	let parts =
		partial.split(&memory, &splitters, Sharing::Private, &coordinator).await.expect("a split");
	assert_eq!(parts.len(), 4);
	assert!(parts.iter().all(|part| part.side.len() == side));
	assert_eq!(parts.iter().map(|part| part.counts.taken).sum::<u64>(), taken);
	for (at, part) in parts.into_iter().enumerate() {
		let merge = part
			.runs
			.merge(&memory, part.held, by_keys(), None, &coordinator)
			.await
			.expect("a merge");
		for (keys, _) in drained(merge, &coordinator).await {
			assert_eq!(splitters.route(&keys[..1]), at, "{keys:?}");
		}
	}
	assert_tidy(&store);
}

/// Aggregates folded partition by partition from range-split partials answer
/// the serial groups, whatever representation each key takes.
#[tokio::test]
async fn c_groups_from_partitions_answer_the_serial_groups() {
	let free = free_c().await;
	let (db, store) = bounded_c().await;
	let free_ctx = free.exec_ctx().await;
	let rows = scan_rows_c(&free).await;
	for (sql, groups) in [(G1, 152), (G2, 456)] {
		let program = Arc::new(program_of(&free_ctx, sql).await);
		let want = serial_groups(&free, &program, &rows).await;
		assert_eq!(want.len(), groups, "{sql}");
		for n in PARTITIONS {
			let (aggregate, partials) = groups_partials(&db, &program, &rows).await;
			if sql == G2 || n == 1 {
				assert!(partials.iter().any(|p| !p.runs.is_empty()), "{sql} carried no run");
			}
			let coordinator = db.exec_ctx().await;
			let splitters = splitters_of(&partials, n, &coordinator).await;
			assert_eq!(splitters.partitions(), n);
			let split = Partitioned::split(
				&memory_of(&coordinator),
				splitters,
				partials,
				Sharing::Private,
				&coordinator,
			)
			.await
			.expect("a split");
			assert_eq!(split.partitions(), n);
			assert_eq!(split.splitters().partitions(), n);
			let probe = [Value::None];
			assert_eq!(split.route(&probe), split.splitters().route(&probe));
			if n >= 2 {
				let held = split.parts().iter().filter(|part| records_in(part) > 0).count();
				assert!(held >= 2, "{sql}: {held} partitions hold records");
			}
			let got = folded_in_order(&program, aggregate, split, &coordinator).await;
			assert_eq!(got, want, "{sql} in {n} partitions");
		}
	}

	// Splitters that are class members in representations other than most
	// rows' own.
	let program = Arc::new(program_of(&free_ctx, G1).await);
	let want = serial_groups(&free, &program, &rows).await;
	let (aggregate, partials) = groups_partials(&db, &program, &rows).await;
	let splitters = Splitters::new(vec![
		vec![float(5.0)],
		vec![dec("10")],
		vec![arr(vec![dec("3"), Value::from(3)])],
		vec![object_a(float(7.0))],
		vec![Value::None],
		vec![Value::Null],
		vec![Value::from("KEY3")],
		vec![Value::from("key3")],
	]);
	assert_eq!(splitters.partitions(), 9);
	let coordinator = db.exec_ctx().await;
	let split = Partitioned::split(
		&memory_of(&coordinator),
		splitters,
		partials,
		Sharing::Private,
		&coordinator,
	)
	.await
	.expect("a split");
	let got = folded_in_order(&program, aggregate, split, &coordinator).await;
	assert_eq!(got, want, "G1 in 9 partitions");
	assert_tidy(&store);
}

/// `GROUP ALL` has the empty key, which routes to partition 0 whatever the
/// splitters.
#[tokio::test]
async fn c_group_all_lands_in_partition_zero_and_answers_the_serial_group() {
	let sql = "SELECT count() AS c, math::sum(n) AS s, array::group(j) AS js FROM u GROUP ALL";
	let free = free_c().await;
	let (db, store) = bounded_c().await;
	let free_ctx = free.exec_ctx().await;
	let rows = scan_rows_c(&free).await;
	let program = Arc::new(program_of(&free_ctx, sql).await);
	let want = serial_groups(&free, &program, &rows).await;
	assert_eq!(want.len(), 1);
	for n in PARTITIONS {
		for explicit in [false, true] {
			let (aggregate, partials) = groups_partials(&db, &program, &rows).await;
			let coordinator = db.exec_ctx().await;
			let splitters = if explicit {
				Splitters::new(vec![vec![Value::from(0)], vec![Value::from(5)]])
			} else {
				splitters_of(&partials, n, &coordinator).await
			};
			assert!(
				splitters.partitions()
					<= if explicit {
						3
					} else {
						n
					}
			);
			let split = Partitioned::split(
				&memory_of(&coordinator),
				splitters,
				partials,
				Sharing::Private,
				&coordinator,
			)
			.await
			.expect("a split");
			for (at, part) in split.parts().iter().enumerate() {
				assert_eq!(
					records_in(part),
					if at == 0 {
						3
					} else {
						0
					},
					"partition {at}"
				);
			}
			let got = folded_in_order(&program, aggregate, split, &coordinator).await;
			assert_eq!(got, want, "{n} partitions, explicit {explicit}");
		}
	}
	assert_tidy(&store);
}

/// The rows a distinct merged from range partitions of three producers'
/// partials keeps, whether a partial carried a run, and how many partitions
/// kept a row. `splitters` is a partition count to sample, or splitters.
async fn distinct_partitioned(
	db: &TestDb,
	rows: &[Value],
	splitters: Result<usize, Splitters>,
	max_rows: usize,
) -> (Result<Vec<Value>, String>, bool, usize) {
	let mut partials = Vec::new();
	for (producer, morsel) in morsels(rows, PRODUCERS).into_iter().enumerate() {
		let ctx = db.exec_ctx().await;
		let mut producer = DistinctProducer::new(producer as u32, &ctx);
		for row in morsel {
			producer.take(row, &ctx).await.expect("a row");
		}
		partials.push(producer.into_partial());
	}
	let carried = partials.iter().any(|p| !p.runs.is_empty());
	let coordinator = db.exec_ctx().await;
	let memory = memory_of(&coordinator);
	let splitters = match splitters {
		Ok(n) => {
			let splitters = splitters_of(&partials, n, &coordinator).await;
			assert_eq!(splitters.partitions(), n);
			splitters
		}
		Err(splitters) => splitters,
	};
	let split = Partitioned::split(&memory, splitters, partials, Sharing::Private, &coordinator)
		.await
		.expect("a split");
	let mut outputs = Vec::new();
	for part in split.into_parts() {
		match finish_distinct_partition(&memory, part, max_rows, Sharing::Private, &coordinator)
			.await
		{
			Ok(output) => outputs.push(output),
			Err(flow) => return (Err(text(flow)), carried, 0),
		}
	}
	let kept = outputs.iter().filter(|output| output.counts.taken > 0).count();
	let mut merge = match merge_distinct_outputs(&memory, outputs, max_rows, &coordinator).await {
		Ok(merge) => merge,
		Err(flow) => return (Err(text(flow)), carried, kept),
	};
	let mut kept_rows = Vec::new();
	loop {
		match merge.next_chunk(1000, &coordinator).await {
			Ok(chunk) if chunk.is_empty() => return (Ok(kept_rows), carried, kept),
			Ok(chunk) => kept_rows.extend(chunk.into_iter().map(|(_, row)| row)),
			Err(flow) => return (Err(text(flow)), carried, kept),
		}
	}
}

#[tokio::test]
async fn c_distinct_from_partitions_keeps_the_serial_first_rows() {
	let (db, store) = bounded_c().await;
	let rows = distinct_rows_c();
	let want = serial_distinct(&rows, usize::MAX).expect("no cap");
	let explicit = Splitters::new(vec![
		vec![dec("0.1")],
		vec![float(1.0)],
		vec![arr(vec![dec("5"), arr(vec![Value::from(5)]), pad()])],
		vec![rid(vec![float(7.0)])],
		vec![Value::None],
		vec![Value::Null],
		vec![Value::from(format!("KEY9-{}", "p".repeat(40)))],
	]);
	assert_eq!(explicit.partitions(), 8);
	let mut choices: Vec<Result<usize, Splitters>> = PARTITIONS.map(Ok).to_vec();
	choices.push(Err(explicit));
	for choice in choices {
		let n = choice.as_ref().map_or(8, |n| *n);
		let (got, carried, kept) = distinct_partitioned(&db, &rows, choice, usize::MAX).await;
		assert_eq!(got.expect("a merge"), want, "{n} partitions");
		assert!(carried, "{n} partitions: no producer carried a run");
		if n >= 2 {
			assert!(kept >= 2, "{n} partitions: {kept} kept a row");
		}
	}
	assert_tidy(&store);
}

#[tokio::test]
async fn c_distinct_from_partitions_fails_past_the_cap_as_serial() {
	let (db, store) = bounded_c().await;
	let rows = distinct_rows_c();
	let distinct = serial_distinct(&rows, usize::MAX).expect("no cap").len();
	let cap = distinct - 1;
	let error = serial_distinct(&rows, cap).expect_err("a cap below the distinct rows");
	for n in [2, 7] {
		let (got, _, _) = distinct_partitioned(&db, &rows, Ok(n), cap).await;
		assert_eq!(got.expect_err("a merge past the cap"), error, "{n} partitions");
		let (got, _, _) = distinct_partitioned(&db, &rows, Ok(n), distinct).await;
		assert_eq!(got.expect("a merge at the cap").len(), distinct, "{n} partitions");
	}
	assert_tidy(&store);
}

/// 1500 build rows keyed by a class in one of twelve representations; every
/// fiftieth, a regex.
fn build_rows_c() -> Vec<(JoinKey, Value)> {
	(0..1500)
		.map(|i| {
			let key = if i % 50 == 4 {
				Value::Regex(Regex::from_str("^key1").expect("a regex"))
			} else {
				class_value(i % 40, (i / 7) % 12)
			};
			(vec![key], object_of("bb", i, true))
		})
		.collect()
}

/// 700 probe rows, every thirty-seventh with no key.
fn probe_rows_plain() -> Vec<(Option<JoinKey>, Value)> {
	(0..700)
		.map(|j| {
			let key = (j % 37 != 0).then(|| vec![class_value(j % 50, (j / 5) % 12)]);
			(key, object_of("pp", j, false))
		})
		.collect()
}

/// The rows the in-memory join of `builds` yields for `probes`.
async fn in_memory_join(
	builds: &[(JoinKey, Value)],
	probes: &[(Option<JoinKey>, Value)],
	step: &JoinStep<u16, HostScalar>,
	program: &Program,
	ctx: &ExecutionContext,
) -> Vec<Value> {
	let mut table = BuildTable::new();
	for (key, row) in builds {
		table.insert(key.clone(), row.clone(), usize::MAX).expect("a build row");
	}
	let mut want = Vec::new();
	let mut emitted = 0;
	let mut state = None;
	let mut judge = HostJudge {
		program,
		scalar: None,
		ctx,
		state: &mut state,
	};
	for (key, row) in probes {
		let matches: Vec<&Value> = key.as_ref().map_or(vec![], |k| table.matches(k).collect());
		want.extend(
			pairings(step, &matches, row.clone(), &mut judge, &mut emitted, usize::MAX)
				.await
				.expect("pairs"),
		);
	}
	want
}

/// What a join partitioned into ranges answered.
struct Joined {
	rows: Vec<Value>,
	needs_global: bool,
	build_counts: Vec<u64>,
	carried: bool,
}

/// `probes` deferred against `builds` read by three producers and divided
/// into `n` partitions.
async fn partitioned_join(
	db: &TestDb,
	builds: &[(JoinKey, Value)],
	probes: &[(Option<JoinKey>, Value)],
	step: &JoinStep<u16, HostScalar>,
	program: &Program,
	n: usize,
) -> Joined {
	let mut partials = Vec::new();
	for (producer, morsel) in morsels(builds, PRODUCERS).into_iter().enumerate() {
		let ctx = db.exec_ctx().await;
		let mut sink = BuildSink::with_width(1, usize::MAX, 0, &ctx).producing(producer as u32);
		for (key, row) in morsel {
			sink.push(key, row, &ctx).await.expect("a build row");
		}
		partials.push(sink.into_partial().expect("a partial"));
	}
	let carried = partials.iter().any(|p| !p.runs.is_empty());
	let coordinator = db.exec_ctx().await;
	let splitters = splitters_of(&partials, n, &coordinator).await;
	assert_eq!(splitters.partitions(), n);
	let opened = PartitionedJoin::open(
		&memory_of(&coordinator),
		splitters,
		partials,
		usize::MAX,
		Sharing::Private,
		&coordinator,
	)
	.await;
	let Ok(mut join) = opened else {
		panic!("a join of partitions did not open");
	};
	assert_eq!(join.partitions(), n);
	let build_counts = join.build_counts().to_vec();
	let mut emitted = 0;
	let mut state = None;
	let mut pairing = Pairing {
		step,
		residual: HostJudge {
			program,
			scalar: None,
			ctx: &coordinator,
			state: &mut state,
		},
		emitted: &mut emitted,
		max_output_rows: usize::MAX,
	};
	for (key, row) in probes {
		join.defer(key.clone(), row.clone(), &mut pairing).await.expect("defers");
	}
	let needs_global = join.needs_global();
	let merge = join.finish(&mut pairing).await.expect("finishes");
	let rows = drained(merge, &coordinator).await.into_iter().map(|(_, row)| row).collect();
	Joined {
		rows,
		needs_global,
		build_counts,
		carried,
	}
}

/// Partitioned joins pair every probe row as the in-memory join does.
#[tokio::test]
async fn c_join_from_partitions_pairs_as_the_in_memory_join() {
	let free = free_c().await;
	let (db, store) = bounded_c().await;
	let free_ctx = free.exec_ctx().await;
	let any_program = program_of(&free_ctx, "SELECT * FROM u").await;
	let builds = build_rows_c();
	let probes = probe_rows_plain();
	for join_type in [JoinType::Inner, JoinType::Left] {
		let step = join_step(join_type);
		let want = in_memory_join(&builds, &probes, &step, &any_program, &free_ctx).await;
		assert!(!want.is_empty(), "{join_type:?} pairs nothing");
		for n in PARTITIONS {
			let joined = partitioned_join(&db, &builds, &probes, &step, &any_program, n).await;
			assert_eq!(joined.rows, want, "{join_type:?} in {n} partitions");
			assert!(joined.carried, "{join_type:?}: {n} partitions carried no run");
			assert!(!joined.needs_global);
			if n >= 2 {
				let held = joined.build_counts.iter().filter(|count| **count > 0).count();
				assert!(held >= 2, "{join_type:?}: {held} of {n} partitions hold build rows");
			}
		}
	}
	assert_eq!(store.runs(), 0);
	assert_tidy(&store);
}

/// A probe row whose key holds a regex matches build rows of every range, so
/// the join finishes as one.
#[tokio::test]
async fn c_join_with_a_regex_probe_falls_back_to_the_global_finish() {
	let free = free_c().await;
	let (db, store) = bounded_c().await;
	let free_ctx = free.exec_ctx().await;
	let any_program = program_of(&free_ctx, "SELECT * FROM u").await;
	let builds = build_rows_c();
	let probes: Vec<(Option<JoinKey>, Value)> = probe_rows_plain()
		.into_iter()
		.enumerate()
		.map(|(j, (key, row))| match key {
			Some(_) if j % 41 == 0 => {
				(Some(vec![Value::Regex(Regex::from_str("^key2").expect("a regex"))]), row)
			}
			key => (key, row),
		})
		.collect();
	for join_type in [JoinType::Inner, JoinType::Left] {
		let step = join_step(join_type);
		let want = in_memory_join(&builds, &probes, &step, &any_program, &free_ctx).await;
		let joined = partitioned_join(&db, &builds, &probes, &step, &any_program, 7).await;
		assert!(joined.needs_global);
		assert_eq!(joined.rows, want, "{join_type:?}");
	}
	assert_tidy(&store);
}

/// A build side past its cap fails as the in-memory table does, before any
/// build row is split.
#[tokio::test]
async fn c_join_from_partitions_fails_past_the_build_cap_before_splitting() {
	let (db, store) = bounded_c().await;
	let builds = build_rows_c();
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
	for (producer, morsel) in morsels(&builds, PRODUCERS).into_iter().enumerate() {
		let ctx = db.exec_ctx().await;
		let mut sink = BuildSink::with_width(1, cap, 0, &ctx).producing(producer as u32);
		for (key, row) in morsel {
			sink.push(key, row, &ctx).await.expect("a producer under the cap");
		}
		partials.push(sink.into_partial().expect("a partial"));
	}
	let before = store.runs_written();
	let coordinator = db.exec_ctx().await;
	let splitters = Splitters::new(vec![
		vec![float(5.0)],
		vec![Value::None],
		vec![Value::from(format!("KEY3-{}", "p".repeat(40)))],
	]);
	match PartitionedJoin::open(
		&memory_of(&coordinator),
		splitters,
		partials,
		cap,
		Sharing::Private,
		&coordinator,
	)
	.await
	{
		Err(flow) => assert_eq!(text(flow), error),
		Ok(_) => panic!("a build side past its cap opened"),
	}
	assert_eq!(store.runs_written(), before, "a build side was split past its cap");
	assert_eq!(store.runs(), 0, "a spilled run outlived its query");
}

/// A sort is not partitioned, alone or beside another breaker's partials.
#[tokio::test]
async fn c_partials_of_a_sort_are_not_split() {
	let free = free_c().await;
	let (db, store) = bounded_c().await;
	let free_ctx = free.exec_ctx().await;
	let rows = scan_rows_c(&free).await;
	let sorted = Arc::new(program_of(&free_ctx, "SELECT n FROM u ORDER BY n").await);
	let at = scan_pipeline(&sorted);
	let sort = body_breakers(&sorted.pipelines[at].code).sorts[0];
	let sort_partial = || async {
		let mut state = run_morsel(&sorted, at, rows.clone(), &free_ctx).await.expect("a morsel");
		Partial::of_sort(&mut state, &sorted, sort, &free_ctx).await.expect("a partial")
	};
	let coordinator = db.exec_ctx().await;
	let memory = memory_of(&coordinator);
	let Err(flow) = sort_partial()
		.await
		.split(&memory, &Splitters::single(), Sharing::Private, &coordinator)
		.await
	else {
		panic!("a sort was split");
	};
	assert!(text(flow).contains("Internal error"));
	let grouped = Arc::new(program_of(&free_ctx, G1).await);
	let (_, mut partials) = groups_partials(&db, &grouped, &rows).await;
	partials.push(sort_partial().await);
	match Partitioned::split(&memory, Splitters::single(), partials, Sharing::Private, &coordinator)
		.await
	{
		Err(flow) => assert!(text(flow).contains("Internal error")),
		Ok(_) => panic!("partials of two breakers were split"),
	}
	assert_eq!(store.runs(), 0);
}
