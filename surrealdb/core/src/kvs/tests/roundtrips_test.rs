//! Serial storage round trips per statement.
//!
//! Each corpus statement runs alone against a datastore whose transactions are
//! wrapped by [`latency`](super::latency), once per capability profile with
//! 1 ms injected per round trip, and the serial round-trip count of its
//! critical path is printed with the per-class and per-category breakdown.
//! The same statements run in lockstep on a datastore without the wrapper, and
//! every result must be identical, so the wrapper cannot hide a behaviour
//! change. [`BUDGET`] bounds the in-memory backend's count per statement.
//!
//! Run with `-- --nocapture`; set `SURREAL_RT_TRACE=1` to print every counted
//! operation of each statement.
#![cfg(any(feature = "kv-mem", feature = "kv-surrealkv"))]
#![allow(clippy::unwrap_used)]

use std::fmt::Write as _;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use regex::Regex;
use surrealdb_types::{ToSql, Variables};
use web_time::Instant;

use super::latency::{
	CATEGORIES, CLASSES, Category, Class, LatencyComposer, LatencyStats, Profile, RoundTrips,
};
use crate::dbs::Session;
use crate::kvs::Datastore;

/// Maximum serial round trips per corpus statement on the in-memory backend,
/// forward profile.
const BUDGET: &[(&str, u64)] = &[
	("point_select", 1),
	("point_only", 1),
	("id_list", 3),
	("idx_eq", 2),
	("idx_eq_limit", 2),
	("uniq_eq", 2),
	("range_limit", 1),
	("filter_limit", 1),
	("id_range", 1),
	("order_limit", 2),
	// A whole-table scan sizes its pages from the stored size of its range,
	// which may cut its first page short of a full batch.
	("count_all", 3),
	("fetch_one", 2),
	("fetch_many", 2),
	("link_field", 4),
	("graph_1hop", 5),
	("graph_2hop", 7),
	("graph_in", 5),
	("create_one", 4),
	("create_rand", 3),
	("create_batch", 31),
	("insert_batch", 31),
	("update_one", 4),
	("update_where_idx", 9),
	("upsert_new", 4),
	("upsert_existing", 2),
	("merge_one", 2),
	("delete_one", 7),
	("delete_where", 9),
	("relate_one", 4),
	("relate_many", 6),
	("create_event", 3),
	("create_ref", 2),
	("txn_two_stmts", 7),
];

/// Maximum serial round trips per read statement on SurrealKV, forward profile, for the
/// statements whose count depends on the placement its read views allow (a paged range may run as
/// morsels) and not only on what the store answers.
#[cfg(feature = "kv-surrealkv")]
const SURREALKV_BUDGET: &[(&str, u64)] = &[("fetch_many", 2), ("link_field", 4)];

/// One corpus statement. `$n` and `$sn` are bound to the run number as an
/// integer and as text, so a write names a fresh record on every run while the
/// statement text, and so its cache entry, stays the same.
struct Case {
	name: &'static str,
	/// Runs before each execution of `sql`, outside the measurement window.
	setup: Option<String>,
	sql: String,
	/// Compare after replacing generated record ids and datetimes.
	normalise: bool,
}

fn case(name: &'static str, sql: &str) -> Case {
	Case {
		name,
		setup: None,
		sql: sql.to_owned(),
		normalise: false,
	}
}

fn generated(name: &'static str, sql: &str) -> Case {
	Case {
		normalise: true,
		..case(name, sql)
	}
}

fn with_setup(name: &'static str, setup: &str, sql: &str) -> Case {
	Case {
		setup: Some(setup.to_owned()),
		..case(name, sql)
	}
}

fn corpus() -> Vec<Case> {
	let batch_random = (0..10)
		.map(|i| {
			format!("{{ name: 'x', age: {}, email: string::concat('b', $sn, '_{i}@x.io') }}", i + 3)
		})
		.collect::<Vec<_>>()
		.join(", ");
	let batch_ids = (0..10)
		.map(|i| {
			format!(
				"{{ id: type::record('person', string::concat('ib', $sn, '_{i}')), age: 4, email: string::concat('i', $sn, '_{i}@x.io') }}"
			)
		})
		.collect::<Vec<_>>()
		.join(", ");
	vec![
		case("point_select", "SELECT * FROM person:42"),
		case("point_only", "SELECT * FROM ONLY person:42"),
		case("id_list", "SELECT * FROM [person:1, person:2, person:3, person:4, person:5]"),
		case("idx_eq", "SELECT * FROM person WHERE age = 30"),
		case("idx_eq_limit", "SELECT * FROM person WHERE age = 30 LIMIT 5"),
		case("uniq_eq", "SELECT * FROM person WHERE email = 'p42@x.io'"),
		case("range_limit", "SELECT * FROM person LIMIT 10"),
		case("filter_limit", "SELECT * FROM person WHERE city = 'c3' LIMIT 10"),
		case("id_range", "SELECT * FROM person:100..120"),
		case("order_limit", "SELECT * FROM person ORDER BY age LIMIT 10"),
		case("count_all", "SELECT count() FROM person GROUP ALL"),
		case("fetch_one", "SELECT * FROM post:7 FETCH author"),
		case("fetch_many", "SELECT * FROM post LIMIT 20 FETCH author"),
		case("link_field", "SELECT author.name FROM post LIMIT 20"),
		case("graph_1hop", "SELECT ->knows->person FROM person:42"),
		case("graph_2hop", "SELECT ->knows->person->knows->person FROM person:42"),
		case("graph_in", "SELECT <-knows<-person FROM person:42"),
		case(
			"create_one",
			"CREATE type::record('person', string::concat('new_', $sn)) \
			 SET name = 'x', age = 1, email = string::concat('n', $sn, '@x.io')",
		),
		generated("create_rand", "CREATE person SET name = 'x', age = 2"),
		generated("create_batch", &format!("INSERT INTO person [ {batch_random} ]")),
		case("insert_batch", &format!("INSERT INTO person [ {batch_ids} ]")),
		case("update_one", "UPDATE person:42 SET age += 1"),
		with_setup(
			"update_where_idx",
			"UPDATE person:50, person:51 SET age = 31",
			"UPDATE person SET age += 1 WHERE age = 31",
		),
		case(
			"upsert_new",
			"UPSERT type::record('person', string::concat('ups_', $sn)) SET age = 3",
		),
		case("upsert_existing", "UPSERT person:43 SET age = 4"),
		case("merge_one", "UPDATE person:44 MERGE { city: 'c9' }"),
		with_setup(
			"delete_one",
			"CREATE type::record('person', string::concat('del_', $sn)) \
			 SET age = 1, email = string::concat('d', $sn, '@x.io')",
			"DELETE type::record('person', string::concat('del_', $sn))",
		),
		with_setup(
			"delete_where",
			"CREATE type::record('person', string::concat('dw_', $sn)) \
			 SET age = 77, email = string::concat('w', $sn, '@x.io')",
			"DELETE person WHERE age = 77",
		),
		generated("relate_one", "RELATE person:1->knows->person:2"),
		generated("relate_many", "RELATE person:1->knows->[person:3, person:4, person:5]"),
		generated("create_event", "CREATE audited SET v = 1"),
		generated("create_ref", "CREATE refd SET target = person:5"),
		case(
			"txn_two_stmts",
			"BEGIN; \
			 CREATE type::record('person', string::concat('t_', $sn)) \
			 SET age = 5, email = string::concat('t', $sn, '@x.io'); \
			 UPDATE person:42 SET age += 1; \
			 COMMIT;",
		),
	]
}

const SETUP: &str = "
	DEFINE TABLE person SCHEMALESS;
	DEFINE FIELD friend ON person TYPE option<record<person>>;
	DEFINE INDEX age_idx ON person FIELDS age;
	DEFINE INDEX email_uniq ON person FIELDS email UNIQUE;
	DEFINE TABLE post SCHEMALESS;
	DEFINE FIELD author ON post TYPE record<person>;
	DEFINE TABLE knows TYPE RELATION IN person OUT person;
	DEFINE TABLE audit_log SCHEMALESS;
	DEFINE TABLE audited SCHEMALESS;
	DEFINE EVENT audit ON audited WHEN $event = 'CREATE' THEN (CREATE audit_log SET v = $after.v);
	DEFINE TABLE refd SCHEMALESS;
	DEFINE FIELD target ON refd TYPE record<person> REFERENCE;
	FOR $i IN 0..1000 {
		CREATE type::record('person', $i) SET
			name = string::concat('n', $i),
			age = $i % 60,
			city = string::concat('c', $i % 10),
			email = string::concat('p', $i, '@x.io'),
			friend = type::record('person', ($i + 1) % 1000);
	};
	FOR $i IN 0..2000 {
		CREATE type::record('post', $i) SET
			title = string::concat('t', $i),
			author = type::record('person', $i % 1000);
	};
	FOR $i IN 0..1000 {
		FOR $k IN [1, 2, 3] {
			RELATE (type::record('person', $i))
				-> (type::record('knows', [$i, $k]))
				-> (type::record('person', ($i + $k * 7) % 1000));
		};
	};
";

fn variables(n: u64) -> Variables {
	let mut vars = Variables::new();
	vars.insert("n", n as i64);
	vars.insert("sn", n.to_string());
	vars
}

/// Execute `sql` with the run number bound, failing on any error, and return
/// every statement's result rendered as SurrealQL.
async fn run(ds: &Datastore, ses: &Session, sql: &str, n: u64) -> String {
	let results = ds.execute(sql, ses, Some(variables(n))).await.unwrap();
	let mut out = String::new();
	for r in results {
		match r.result {
			Ok(v) => writeln!(out, "{}", v.to_sql()).unwrap(),
			Err(e) => panic!("{sql}: {e:?}"),
		}
	}
	out
}

static GENERATED_ID: LazyLock<Regex> = LazyLock::new(|| {
	Regex::new(r"\b(person|knows|audited|audit_log|refd):[a-z0-9]{20}\b").unwrap()
});
static DATETIME: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"d'[^']*'").unwrap());

/// The output with generated record ids and datetimes replaced by placeholders.
fn normalised(out: &str) -> String {
	let out = GENERATED_ID.replace_all(out, "$1:<id>");
	DATETIME.replace_all(&out, "d'<datetime>'").into_owned()
}

/// A datastore on `path`, set up with the corpus dataset. With `stats` its
/// transactions are wrapped; the window is closed during setup.
async fn open(path: &str, stats: Option<&Arc<LatencyStats>>) -> (Arc<Datastore>, Session) {
	let builder = Datastore::builder().without_maintenance_tasks();
	let ds = match stats {
		Some(stats) => {
			builder.build_with_factory_path(path, LatencyComposer(Arc::clone(stats))).await
		}
		None => builder.build_with_path(path).await,
	}
	.unwrap();
	let ses = Session::owner().with_ns("t").with_db("t");
	ds.execute("DEFINE NAMESPACE t; DEFINE DATABASE t;", &ses, None).await.unwrap();
	run(&ds, &ses, SETUP, 0).await;
	(ds, ses)
}

/// One statement's measurements.
struct Measured {
	name: &'static str,
	fwd: RoundTrips,
	lsm: RoundTrips,
	wall_1ms: Duration,
	wall_5ms: Duration,
	trace: String,
}

struct Rig {
	wrapped: Arc<Datastore>,
	wrapped_ses: Session,
	plain: Arc<Datastore>,
	plain_ses: Session,
	stats: Arc<LatencyStats>,
	/// The next run number; every run of a statement gets a fresh one.
	next: u64,
}

impl Rig {
	/// Run `case` once on both datastores, the wrapped one inside the
	/// measurement window, and assert the results agree. Returns the wrapped
	/// run's wall time.
	async fn once(&mut self, case: &Case, window: bool) -> Duration {
		self.next += 1;
		let n = self.next;
		if let Some(setup) = &case.setup {
			run(&self.wrapped, &self.wrapped_ses, setup, n).await;
			run(&self.plain, &self.plain_ses, setup, n).await;
		}
		self.stats.set_enabled(window);
		let start = Instant::now();
		let got = run(&self.wrapped, &self.wrapped_ses, &case.sql, n).await;
		let wall = start.elapsed();
		self.stats.set_enabled(false);
		let want = run(&self.plain, &self.plain_ses, &case.sql, n).await;
		match case.normalise {
			true => assert_eq!(normalised(&got), normalised(&want), "{}", case.name),
			false => assert_eq!(got, want, "{}", case.name),
		}
		wall
	}

	/// The counters of one run under `profile` at 1 ms per round trip.
	async fn count(
		&mut self,
		case: &Case,
		profile: Profile,
		tracing: bool,
	) -> (RoundTrips, String) {
		self.stats.set_profile(profile);
		self.stats.set_latency(Duration::from_millis(1));
		self.stats.reset();
		self.stats.set_tracing(tracing);
		self.once(case, true).await;
		self.stats.set_tracing(false);
		let counted = self.stats.snapshot();
		let mut trace = String::new();
		for e in self.stats.take_trace() {
			writeln!(
				trace,
				"  r{} {} {} rows={} {}",
				e.round,
				e.class.name(),
				e.category.name(),
				e.rows.map(|r| r.to_string()).unwrap_or_else(|| "-".into()),
				e.key
			)
			.unwrap();
		}
		(counted, trace)
	}

	/// The median wall time of three runs, forward profile, at `latency`.
	async fn wall(&mut self, case: &Case, latency: Duration) -> Duration {
		self.stats.set_profile(Profile::Forward);
		self.stats.set_latency(latency);
		let mut walls = Vec::new();
		for _ in 0..3 {
			walls.push(self.once(case, true).await);
		}
		walls.sort();
		walls[1]
	}

	async fn measure(&mut self, case: &Case, tracing: bool) -> Measured {
		// Two uncounted runs under the forward profile warm the catalog and
		// statement caches and the scan's range sizing: the first compiles the
		// statement and keeps it, and the second is the first served from the
		// cache, which reads once what a kept statement then holds (its
		// table's layouts at the current catalog epoch). Range sizes follow
		// what earlier runs saw, so both run at the latency the counted run
		// sees. The budget counts the runs after both.
		self.stats.set_profile(Profile::Forward);
		self.stats.set_latency(Duration::from_millis(1));
		self.once(case, true).await;
		self.once(case, true).await;
		let (fwd, fwd_trace) = self.count(case, Profile::Forward, tracing).await;
		let (lsm, lsm_trace) = self.count(case, Profile::Lsm, tracing).await;
		let wall_1ms = self.wall(case, Duration::from_millis(1)).await;
		let wall_5ms = self.wall(case, Duration::from_millis(5)).await;
		let mut trace = String::new();
		if tracing {
			write!(
				trace,
				"trace {} [forward]\n{fwd_trace}trace {} [lsm]\n{lsm_trace}",
				case.name, case.name
			)
			.unwrap();
		}
		Measured {
			name: case.name,
			fwd,
			lsm,
			wall_1ms,
			wall_5ms,
			trace,
		}
	}
}

const CLASS_COLUMNS: [Class; CLASSES] = [
	Class::Get,
	Class::Getm,
	Class::RangeFirst,
	Class::RangePage,
	Class::CheckWrite,
	Class::BlindWrite,
	Class::Commit,
	Class::Begin,
];
const CATEGORY_COLUMNS: [Category; CATEGORIES] = [
	Category::Catalog,
	Category::Record,
	Category::Index,
	Category::Graph,
	Category::Ref,
	Category::Other,
];

fn table(label: &str, rows: &[Measured]) -> String {
	let mut out = format!("\n### {label}\n\n");
	let mut header = vec!["name".to_owned(), "RT fwd".to_owned(), "RT lsm".to_owned()];
	header.extend(CLASS_COLUMNS.iter().map(|c| c.name().to_owned()));
	header.push("views".to_owned());
	header.extend(CATEGORY_COLUMNS.iter().map(|c| c.name().to_owned()));
	header.push("wall ms @1ms".to_owned());
	header.push("wall ms @5ms".to_owned());
	writeln!(out, "| {} |", header.join(" | ")).unwrap();
	writeln!(out, "|{}|", vec!["---"; header.len()].join("|")).unwrap();
	for m in rows {
		let mut cells = vec![m.name.to_owned(), m.fwd.rounds.to_string(), m.lsm.rounds.to_string()];
		cells.extend(CLASS_COLUMNS.iter().map(|c| m.fwd.class(*c).to_string()));
		cells.push(m.fwd.views.to_string());
		cells.extend(CATEGORY_COLUMNS.iter().map(|c| m.fwd.category(*c).to_string()));
		cells.push(format!("{:.1}", m.wall_1ms.as_secs_f64() * 1e3));
		cells.push(format!("{:.1}", m.wall_5ms.as_secs_f64() * 1e3));
		writeln!(out, "| {} |", cells.join(" | ")).unwrap();
	}
	out
}

/// Measure the whole corpus on a wrapped datastore at `wrapped_path` against a
/// wrapper-free one at `plain_path`, print the table, and, when `budgeted`,
/// fail on any statement over its [`BUDGET`]; the statements `pinned` names fail over the count it
/// gives however `budgeted` is set.
async fn corpus_on(
	label: &str,
	wrapped_path: &str,
	plain_path: &str,
	budgeted: bool,
	pinned: &[(&str, u64)],
) {
	let tracing = std::env::var("SURREAL_RT_TRACE").is_ok_and(|v| v == "1");
	let stats = LatencyStats::new();
	let (wrapped, wrapped_ses) = open(wrapped_path, Some(&stats)).await;
	let (plain, plain_ses) = open(plain_path, None).await;
	let mut rig = Rig {
		wrapped,
		wrapped_ses,
		plain,
		plain_ses,
		stats,
		next: 0,
	};
	let mut rows = Vec::new();
	for case in corpus() {
		rows.push(rig.measure(&case, tracing).await);
	}
	println!("{}", table(label, &rows));
	for m in rows.iter().filter(|m| !m.trace.is_empty()) {
		println!("{}", m.trace);
	}
	let mut over = Vec::new();
	for (name, max) in pinned {
		match rows.iter().find(|m| m.name == *name) {
			Some(m) if m.fwd.rounds > *max => {
				over.push(format!("{name}: {} > {max}", m.fwd.rounds))
			}
			Some(_) => {}
			None => over.push(format!("{name}: pinned but not in the corpus")),
		}
	}
	assert!(over.is_empty(), "round-trip budget exceeded: {over:?}");
	if budgeted {
		let mut over = Vec::new();
		for m in &rows {
			match BUDGET.iter().find(|(name, _)| *name == m.name) {
				Some((_, max)) if m.fwd.rounds > *max => {
					over.push(format!("{}: {} > {max}", m.name, m.fwd.rounds))
				}
				Some(_) => {}
				None => over.push(format!("{}: no budget entry", m.name)),
			}
		}
		assert!(over.is_empty(), "round-trip budget exceeded: {over:?}");
	}
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn round_trips_memory() {
	corpus_on("memory", "memory", "memory", true, &[]).await;
}

#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread")]
async fn round_trips_surrealkv() {
	use temp_dir::TempDir;
	let wrapped = TempDir::new().unwrap();
	let plain = TempDir::new().unwrap();
	corpus_on(
		"surrealkv",
		&format!("surrealkv:{}", wrapped.path().to_string_lossy()),
		&format!("surrealkv:{}", plain.path().to_string_lossy()),
		false,
		SURREALKV_BUDGET,
	)
	.await;
}
