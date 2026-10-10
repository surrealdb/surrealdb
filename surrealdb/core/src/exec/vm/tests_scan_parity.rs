//! The reads of storage the VM runs de-duplicate a record several index
//! entries reach, build the rows of a nearest-neighbor search from its ids,
//! decode only the fields a statement reads, and bound the batches of wide
//! records they decode.

use std::collections::HashSet;

use super::batch::BATCHES;
use super::program::{Instr, Source};
use super::tests::{bind, outcome, planner};
use crate::exec::ExecutionContext;
use crate::exec::operators::test_util::TestDb;
use crate::val::Value;

const FAN_SCHEMA: &str = "
	DEFINE TABLE fan SCHEMALESS;
	DEFINE INDEX fan_grp_labels ON fan COLUMNS grp, labels.*;
	DEFINE TABLE fanl SCHEMALESS;
	DEFINE INDEX fanl_labels ON fanl COLUMNS labels.*;
	FOR $i IN 1..=40 {
		CREATE type::record('fan', $i) SET
			grp = ['g', 'h', 'i'][$i % 3],
			labels = ['p', 'q', 'r', 'p', ['s', 't', 'u'][$i % 3]],
			n = $i;
		CREATE type::record('fanl', $i) SET
			labels = ['p', 'q', ['s', 't', 'u'][$i % 3]],
			n = $i;
	};
	CREATE fan:none SET grp = 'g', n = 0;
	CREATE fanl:none SET n = 0;
";

/// Fan-out scans and unions.
const FANNING: &[&str] = &[
	"SELECT id FROM fan WHERE grp = 'g'",
	"SELECT * FROM fan WHERE grp = 'g'",
	"SELECT id FROM fan WHERE grp = 'g' ORDER BY id",
	"SELECT id FROM fan WHERE grp = 'g' ORDER BY id DESC LIMIT 4",
	"SELECT id FROM fan WHERE grp = 'g' ORDER BY labels, id",
	"SELECT id FROM fan WHERE grp = 'g' LIMIT 3",
	"SELECT id FROM fan WHERE grp = 'g' LIMIT 3 START 2",
	"SELECT id FROM fan WHERE grp = 'g' AND n > 10",
	"SELECT count() FROM fan WHERE grp = 'g' GROUP ALL",
	"SELECT grp, count() AS c FROM fan WHERE grp = 'g' OR grp = 'h' GROUP BY grp ORDER BY grp",
	"SELECT id FROM fan WHERE grp = 'g' OR grp = 'h'",
	"SELECT id FROM fan WHERE grp = 'g' OR grp = 'h' ORDER BY id",
	"SELECT id FROM fan WHERE grp = 'g' OR grp = 'h' ORDER BY id DESC",
	"SELECT id FROM fan WHERE grp IN ['g', 'i']",
	"SELECT id FROM fan WHERE grp IN ['g', 'i'] ORDER BY id",
	"SELECT id FROM fanl WHERE labels.* > 'p'",
	"SELECT id FROM fanl WHERE labels.* >= 'p' ORDER BY id",
	"SELECT id FROM fanl WHERE labels.* < 's'",
	"SELECT id FROM fanl WHERE labels.* CONTAINS 'q'",
	"SELECT id FROM fanl WHERE labels.* > 'p' OR labels.* CONTAINS 'u'",
	"SELECT count() FROM fanl WHERE labels.* > 'p' GROUP ALL",
	"SELECT VALUE n FROM fanl WHERE labels.* > 'p' ORDER BY n DESC LIMIT 5",
];

/// The record ids `rows` hold, in order.
fn ids(rows: &[Value]) -> Vec<Value> {
	rows.iter()
		.filter_map(|row| match row {
			Value::Object(object) => object.get("id").cloned(),
			_ => None,
		})
		.collect()
}

#[tokio::test]
async fn a_scan_that_fans_out_lists_each_record_once() {
	let db = TestDb::new(FAN_SCHEMA).await;
	let ctx = db.exec_ctx().await;
	for sql in FANNING {
		let got = vm_rows(&ctx, sql).await;
		// The same statement over a table scan reads each record once by
		// construction.
		let scan = sql.replacen(" WHERE ", " WITH NOINDEX WHERE ", 1);
		let want = vm_rows(&ctx, &scan).await;
		if sql.contains("ORDER BY") {
			assert_eq!(got, want, "`{sql}`");
		} else if sql.contains("LIMIT") {
			assert_eq!(got.len(), want.len(), "`{sql}`");
		} else {
			let (mut got, mut want) = (got.clone(), want);
			got.sort();
			want.sort();
			assert_eq!(got, want, "`{sql}`");
		}
		if sql.starts_with("SELECT id") || sql.starts_with("SELECT *") {
			let listed = ids(&got);
			let distinct: HashSet<_> = listed.iter().collect();
			assert_eq!(listed.len(), distinct.len(), "`{sql}` lists a record twice");
		}
	}
}

/// The rows `sql` returns on the VM, which must plan it.
async fn vm_rows(ctx: &ExecutionContext, sql: &str) -> Vec<Value> {
	let bound = bind(ctx, sql).await;
	let vm = super::plan(&planner(ctx), &bound)
		.await
		.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
	outcome(&vm, ctx).await.expect("the VM runs")
}

#[tokio::test]
async fn a_fanning_scan_returns_every_matching_record_and_no_more() {
	let db = TestDb::new(FAN_SCHEMA).await;
	let ctx = db.exec_ctx().await;
	// 40 records spread over three groups, plus `fan:none`, which is in `g`.
	let in_g = vm_rows(&ctx, "SELECT id FROM fan WHERE grp = 'g'").await;
	let oracle = vm_rows(&ctx, "SELECT id FROM fan WITH NOINDEX WHERE grp = 'g'").await;
	assert_eq!(in_g.len(), oracle.len());
	let mut got = ids(&in_g);
	let mut want = ids(&oracle);
	got.sort();
	want.sort();
	assert_eq!(got, want);
	// A record is counted once however many entries it owns.
	let counted = vm_rows(&ctx, "SELECT count() FROM fanl WHERE labels.* > 'p' GROUP ALL").await;
	let oracle =
		vm_rows(&ctx, "SELECT count() FROM fanl WITH NOINDEX WHERE labels.* > 'p' GROUP ALL").await;
	assert_eq!(counted, oracle);
	// A scan that fans out delivers no ordering, so the sort above it stays.
	let ordered = vm_rows(&ctx, "SELECT VALUE n FROM fanl WHERE labels.* > 'p' ORDER BY n").await;
	let mut sorted = ordered.clone();
	sorted.sort();
	assert_eq!(ordered, sorted);
}

const KNN_SCHEMA: &str = "
	DEFINE INDEX hn ON pts FIELDS point HNSW DIMENSION 2;
	FOR $i IN 0..6 {
		CREATE type::record('pts', $i) SET point = [<float> $i, 0f], name = 'p' + <string> $i;
	};
	DEFINE INDEX hn ON cmp FIELDS point HNSW DIMENSION 2;
	DEFINE FIELD label ON cmp TYPE string COMPUTED 'c' + <string> n;
	FOR $i IN 0..6 {
		CREATE type::record('cmp', $i) SET point = [<float> $i, 0f], n = $i;
	};
	DEFINE INDEX hn ON prm FIELDS point HNSW DIMENSION 2;
	DEFINE FIELD secret ON prm PERMISSIONS FOR select NONE;
	FOR $i IN 0..6 {
		CREATE type::record('prm', $i) SET point = [<float> $i, 0f], secret = $i;
	};
";

/// Whether the program `sql` lowers to builds the rows of its KNN scan from
/// ids.
async fn knn_ids_only(ctx: &ExecutionContext, sql: &str) -> bool {
	let bound = bind(ctx, sql).await;
	let physical = super::optimize::optimize(
		super::map::map(&planner(ctx), &bound).await.unwrap_or_else(|e| panic!("`{sql}`: {e:?}")),
	);
	let program = super::lower::lower(&planner(ctx), &physical).await.expect("lowers");
	let knn: Vec<bool> = program
		.pipelines
		.iter()
		.filter_map(|pipeline| match &pipeline.source {
			Source::Knn {
				ids_only,
				..
			} => Some(*ids_only),
			_ => None,
		})
		.collect();
	assert_eq!(knn.len(), 1, "`{sql}` reads one KNN source");
	knn[0]
}

#[tokio::test]
async fn a_knn_whose_rows_need_only_their_id_builds_them_from_the_search() {
	let db = TestDb::new(KNN_SCHEMA).await;
	let ctx = db.exec_ctx().await;
	let ids_only = [
		"SELECT id FROM pts WHERE point <|3,40|> [0f, 0f]",
		"SELECT count() FROM pts WHERE point <|3,40|> [0f, 0f] GROUP ALL",
		"SELECT id FROM pts WHERE point <|3,40|> [0f, 0f] ORDER BY id DESC",
	];
	let records = [
		"SELECT id, name FROM pts WHERE point <|3,40|> [0f, 0f]",
		"SELECT * FROM pts WHERE point <|3,40|> [0f, 0f]",
		"SELECT id FROM pts WHERE point <|3,40|> [0f, 0f] ORDER BY name",
		"SELECT id FROM pts WHERE name != 'p9' AND point <|3,40|> [0f, 0f]",
		"SELECT id FROM cmp WHERE point <|3,40|> [0f, 0f]",
		"SELECT id FROM prm WHERE point <|3,40|> [0f, 0f]",
	];
	for sql in ids_only {
		assert!(knn_ids_only(&ctx, sql).await, "`{sql}` reads its records");
	}
	for sql in records {
		assert!(!knn_ids_only(&ctx, sql).await, "`{sql}` builds its rows from ids");
	}
}

#[tokio::test]
async fn id_only_knn_rows_are_the_nearest_ids_and_read_no_record() {
	let db = TestDb::new(KNN_SCHEMA).await;
	let ctx = db.exec_ctx().await;
	for (sql, nearest) in [
		("SELECT id FROM pts WHERE point <|3,40|> [0f, 0f]", 3),
		("SELECT count() FROM pts WHERE point <|3,40|> [0f, 0f] GROUP ALL", 1),
		("SELECT id FROM pts WHERE point <|3,40|> [0f, 0f] ORDER BY id DESC", 3),
		("SELECT id FROM cmp WHERE point <|3,40|> [0f, 0f]", 3),
		("SELECT id FROM prm WHERE point <|3,40|> [0f, 0f]", 3),
		("SELECT id, name FROM pts WHERE point <|3,40|> [0f, 0f]", 3),
	] {
		BATCHES.with(|batches| batches.borrow_mut().clear());
		let got = vm_rows(&ctx, sql).await;
		let read = BATCHES.with(|batches| batches.borrow().iter().sum::<usize>());
		assert_eq!(got.len(), nearest, "`{sql}` returns {got:?}");
		if !sql.starts_with("SELECT count()") {
			// The three nearest of the points `[i, 0]` to the origin are 0, 1 and 2.
			let table = if sql.contains("FROM pts") {
				"pts"
			} else if sql.contains("FROM cmp") {
				"cmp"
			} else {
				"prm"
			};
			let mut listed = ids(&got);
			listed.sort();
			let want: Vec<Value> = (0..3i64)
				.map(|i| Value::RecordId(crate::val::RecordId::new(table.into(), i)))
				.collect();
			assert_eq!(listed, want, "`{sql}`");
		}
		let from_ids = knn_ids_only(&ctx, sql).await;
		assert_eq!(read == 0, from_ids, "`{sql}` read {read} records");
	}
}

const KNN_COMPUTED_PERMISSION_SCHEMA: &str = "
	DEFINE ACCESS user ON DATABASE TYPE RECORD;
	DEFINE TABLE kc SCHEMALESS PERMISSIONS FOR select WHERE dbl > 4;
	DEFINE FIELD dbl ON kc COMPUTED n * 2;
	DEFINE INDEX hn ON kc FIELDS point HNSW DIMENSION 2;
	FOR $i IN 0..6 { CREATE type::record('kc', $i) SET point = [<float> $i, 0f], n = $i; };
";

/// The gate of the KNN scan beneath `plan`, found through its projections,
/// layers and filters, and whether the node directly above the scan is a
/// permission that derives computed fields.
fn knn_scan(plan: &super::plan::Plan) -> Option<(&Option<super::plan::KnnFilter>, bool)> {
	use super::plan::Plan;
	match plan {
		Plan::Project {
			input,
			..
		}
		| Plan::Layer {
			input,
			..
		}
		| Plan::Filter {
			input,
			..
		} => knn_scan(input),
		Plan::PermissionFilter {
			input,
			computed,
			..
		} => match &**input {
			Plan::KnnScan {
				filter,
				..
			} => Some((filter, computed.is_some())),
			other => knn_scan(other),
		},
		Plan::KnnScan {
			filter,
			..
		} => Some((filter, false)),
		_ => None,
	}
}

#[tokio::test]
async fn a_knn_search_gates_on_a_permission_reading_a_computed_field_beneath_its_layers() {
	let db = TestDb::new(KNN_COMPUTED_PERMISSION_SCHEMA).await;
	let session = crate::dbs::Session::for_record(
		"test",
		"test",
		"user",
		crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "alice")),
	);
	let ctx = db.exec_ctx_as(&session, crate::kvs::TransactionType::Read).await;
	let sql = "SELECT * FROM kc WHERE point <|3,40|> [0f, 0f]";
	let bound = bind(&ctx, sql).await;
	let physical =
		super::map::map(&planner(&ctx), &bound).await.unwrap_or_else(|e| panic!("{e:?}"));

	let (filter, permission_above_scan) = knn_scan(&physical.root).expect("a KNN scan");
	assert!(permission_above_scan, "the permission sits directly above the scan");
	let permissions = &filter.as_ref().expect("gated").permissions;
	assert_eq!(permissions.len(), 1);
	assert_eq!(
		permissions[0].3,
		Some(super::plan::ComputedReads {
			table: "kc".into(),
			fields: Some(vec!["dbl".to_owned()]),
		})
	);
	super::lower::lower(&planner(&ctx), &super::optimize::optimize(physical))
		.await
		.expect("lowers");

	// Rows 0..=2 compute a `dbl` of at most 4 and are hidden, so the three
	// nearest rows alice sees are the three beyond them.
	let mut results = db.datastore().execute(sql, &session, None).await.expect("runs");
	let rows = results.pop().expect("a result").result.expect("selects");
	let mut ids: Vec<String> = match rows {
		crate::types::PublicValue::Array(rows) => rows
			.iter()
			.map(|row| match row {
				crate::types::PublicValue::Object(row) => {
					surrealdb_types::ToSql::to_sql(row.get("id").expect("an id"))
				}
				other => panic!("a row, got {other:?}"),
			})
			.collect(),
		other => panic!("an array, got {other:?}"),
	};
	ids.sort();
	assert_eq!(ids, ["kc:3", "kc:4", "kc:5"]);
}

const WIDE_SCHEMA: &str = "
	DEFINE TABLE t SCHEMALESS;
	DEFINE INDEX n_idx ON t FIELDS n;
	FOR $i IN 0..400 { CREATE t SET n = 1, emb = array::repeat(0.5f, 1024) };
";

/// A wide record decodes to about 66 KB, so the batch a 256 KiB budget takes
/// holds a handful of records; the default budget takes about 30. A `*`
/// selection decodes whole records, so each batch holds them whole.
#[tokio::test]
async fn wide_records_are_read_in_batches_sized_to_the_byte_budget() {
	let config = surrealdb_cnf::ConfigMap::empty().with_key_value("scan_batch_bytes", "256KiB");
	let db = TestDb::new_with_config(WIDE_SCHEMA, config).await;
	let ctx = db.exec_ctx().await;
	BATCHES.with(|batches| batches.borrow_mut().clear());
	let rows = vm_rows(&ctx, "SELECT * FROM t WHERE n = 1").await;
	let batches = BATCHES.with(|batches| batches.borrow().clone());
	assert_eq!(rows.len(), 400, "every record once");
	assert_eq!(ids(&rows).iter().collect::<HashSet<_>>().len(), 400);
	assert_eq!(batches.iter().sum::<usize>(), 400, "{batches:?}");
	assert!(batches[0] <= 4, "the probe follows the configured budget: {batches:?}");
	assert!(batches.iter().all(|len| *len <= 8), "every batch follows the budget: {batches:?}");
}

#[tokio::test]
async fn narrow_records_are_read_in_batches_that_grow_to_the_row_cap() {
	let db = TestDb::new(
		"DEFINE TABLE t SCHEMALESS;
		DEFINE INDEX n_idx ON t FIELDS n;
		FOR $i IN 0..3000 { CREATE t SET n = 1 };",
	)
	.await;
	let ctx = db.exec_ctx().await;
	BATCHES.with(|batches| batches.borrow_mut().clear());
	let rows = vm_rows(&ctx, "SELECT id FROM t WHERE n = 1").await;
	let batches = BATCHES.with(|batches| batches.borrow().clone());
	assert_eq!(rows.len(), 3000);
	assert_eq!(batches[..3], [32, 64, 128], "a batch at most doubles: {batches:?}");
	assert!(batches.iter().all(|len| *len <= 1000), "{batches:?}");
}

const PAGED_SCHEMA: &str = "
	DEFINE TABLE t SCHEMALESS;
	DEFINE FIELD n ON t TYPE int;
	DEFINE INDEX n_idx ON t FIELDS n;
	FOR $i IN 1..=3000 { CREATE type::record('t', $i) SET n = $i, m = $i % 7 };
";

/// A B-tree scan under its pipeline's own `LIMIT` reads no more records than
/// the limit takes, give or take one minimum entry read: the read that
/// reaches the limit asks for the rows still wanted rather than a full batch.
#[tokio::test]
async fn an_index_scan_under_a_limit_reads_no_further_than_the_limit() {
	let db = TestDb::new(PAGED_SCHEMA).await;
	let ctx = db.exec_ctx().await;
	BATCHES.with(|batches| batches.borrow_mut().clear());
	let rows = vm_rows(&ctx, "SELECT VALUE n FROM t WHERE n > 0 ORDER BY n DESC LIMIT 1000").await;
	let read = BATCHES.with(|batches| batches.borrow().iter().sum::<usize>());
	let want: Vec<Value> = (2001..=3000i64).rev().map(Value::from).collect();
	assert_eq!(rows, want);
	assert!((1000..=1000 + 32).contains(&read), "read {read} records for a page of 1000");
}

/// A limit applied past a sort the scan feeds bounds nothing the scan reads,
/// and a filter that rejects most entries still finds the rows the limit
/// takes.
#[tokio::test]
async fn an_index_scan_below_a_sort_or_a_filter_is_not_cut_short() {
	let db = TestDb::new(PAGED_SCHEMA).await;
	let ctx = db.exec_ctx().await;
	BATCHES.with(|batches| batches.borrow_mut().clear());
	let rows = vm_rows(&ctx, "SELECT VALUE n FROM t WHERE n > 0 ORDER BY m, n LIMIT 1").await;
	let read = BATCHES.with(|batches| batches.borrow().iter().sum::<usize>());
	assert_eq!(rows, vec![Value::from(7i64)]);
	assert_eq!(read, 3000, "the sort sees every record");
	let rows =
		vm_rows(&ctx, "SELECT VALUE n FROM t WHERE n > 0 AND m = 3 ORDER BY n DESC LIMIT 5").await;
	let want: Vec<Value> =
		(1..=3000i64).rev().filter(|n| n % 7 == 3).take(5).map(Value::from).collect();
	assert_eq!(rows, want);
}

const PREFILTER_SCHEMA: &str = "
	DEFINE TABLE big SCHEMALESS;
	FOR $i IN 1..=3000 {
		CREATE type::record('big', $i) SET n = $i, pad = string::repeat('x', 400), tags = ['a', <string> $i];
	};
";

/// A scan whose body runs on decoded rows, because it calls a function, judges
/// the stored bytes of each record by the filter directly above it and decodes
/// only the records that filter can admit: a predicate the bytes judge is the
/// scan's prefilter, and any other decodes every record.
#[tokio::test]
async fn a_scan_that_decodes_its_rows_decodes_only_those_its_filter_admits() {
	let db = TestDb::new(PREFILTER_SCHEMA).await;
	let ctx = db.exec_ctx().await;
	let scanned = || super::host::RECORDS_SCANNED.with(std::cell::Cell::get);
	for (sql, rows, decoded, prefiltered) in [
		("SELECT string::len(pad) AS l FROM big WHERE n > 2990", 10, 10, true),
		("SELECT string::len(pad) AS l FROM big WHERE n > 2990 AND n < 2995", 4, 4, true),
		("SELECT string::len(pad) AS l FROM big WHERE n < 3 OR n > 2998", 4, 4, true),
		("SELECT string::len(pad) AS l FROM big WHERE !(n < 2995)", 6, 6, true),
		("SELECT string::len(pad) AS l FROM big WHERE 2998 < n", 2, 2, true),
		("SELECT string::len(pad) AS l FROM big WHERE pad CONTAINS 'xyz'", 0, 0, true),
		("SELECT string::len(pad) AS l FROM big WHERE n + 1 > 2990", 11, 3000, false),
		(
			"SELECT string::len(pad) AS l FROM big WHERE n > 2990 AND string::len(pad) > 0",
			10,
			3000,
			false,
		),
	] {
		let bound = bind(&ctx, sql).await;
		let physical =
			super::optimize::optimize(super::map::map(&planner(&ctx), &bound).await.expect("maps"));
		let program = super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
		let pipeline = &program.pipelines[0];
		assert!(!pipeline.in_place, "`{sql}` runs on decoded rows");
		assert_eq!(pipeline.source.prefilter().is_some(), prefiltered, "`{sql}`");
		let before = scanned();
		assert_eq!(vm_rows(&ctx, sql).await.len(), rows, "`{sql}`");
		let read = scanned() - before;
		assert_eq!(read, decoded, "`{sql}` decoded {read} records");
	}
}

const DECODE_SCHEMA: &str = "
	DEFINE ANALYZER blank TOKENIZERS blank;
	DEFINE TABLE t SCHEMALESS;
	DEFINE INDEX n_idx ON t FIELDS n;
	DEFINE INDEX body_ft ON t FIELDS body FULLTEXT ANALYZER blank BM25;
	FOR $i IN 1..=12 {
		CREATE type::record('t', $i) SET
			n = $i % 3,
			label = 'l' + <string> $i,
			body = IF $i % 2 = 0 { 'even' } ELSE { 'odd' },
			emb = [0.5f, 0.5f];
	};
	DEFINE TABLE cmp SCHEMALESS;
	DEFINE INDEX n_idx ON cmp FIELDS n;
	DEFINE FIELD c ON cmp COMPUTED n + 1;
	FOR $i IN 1..=6 { CREATE type::record('cmp', $i) SET n = $i % 3, emb = [0.5f] };
";

/// The decode set of each index-backed source of the program `sql` lowers
/// to, as the fields listed and whether they are excluded; `None` for a
/// source that decodes whole records.
async fn decode_sets(ctx: &ExecutionContext, sql: &str) -> Vec<Option<(Vec<String>, bool)>> {
	let bound = bind(ctx, sql).await;
	let physical = super::optimize::optimize(
		super::map::map(&planner(ctx), &bound).await.unwrap_or_else(|e| panic!("`{sql}`: {e:?}")),
	);
	let program = super::lower::lower(&planner(ctx), &physical).await.expect("lowers");
	program
		.pipelines
		.iter()
		.filter_map(|pipeline| match &pipeline.source {
			Source::Index {
				decode,
				..
			}
			| Source::IndexUnion {
				decode,
				..
			}
			| Source::Bitmap {
				decode,
				..
			}
			| Source::FullText {
				decode,
				..
			} => Some(decode.as_ref().map(|d| d.listed())),
			_ => None,
		})
		.collect()
}

/// An index-backed source decodes the fields its statement's clauses name,
/// or every field but the `OMIT` targets of a `*` selection; it decodes whole
/// records where a clause reads the row other than by a field's name, or the
/// table defines a computed field.
#[tokio::test]
async fn an_index_source_decodes_only_the_fields_its_clauses_read() {
	let db = TestDb::new(DECODE_SCHEMA).await;
	let ctx = db.exec_ctx().await;
	let listed = |fields: &[&str], excluding: bool| {
		Some((fields.iter().map(|f| f.to_string()).collect::<Vec<_>>(), excluding))
	};
	for (sql, want) in [
		("SELECT label FROM t WHERE n = 1", listed(&["label", "n"], false)),
		("SELECT id FROM t WHERE n = 1 ORDER BY label", listed(&["label", "n"], false)),
		("SELECT n, count() AS c FROM t WHERE n = 1 GROUP BY n", listed(&["n"], false)),
		("SELECT * OMIT emb FROM t WHERE n = 1", listed(&["emb"], true)),
		("SELECT label FROM t WHERE n = 1 OR n = 2", listed(&["label", "n"], false)),
		("SELECT id FROM t WHERE body @@ 'even'", listed(&["body"], false)),
		("SELECT * FROM t WHERE n = 1", None),
		("SELECT type::field('label') AS l FROM t WHERE n = 1", None),
		("SELECT n FROM cmp WHERE n = 1", None),
	] {
		assert_eq!(decode_sets(&ctx, sql).await, vec![want], "`{sql}`");
	}
}

/// What a statement returns over an index-backed source that decodes part of
/// each record is what the same statement returns over a table scan, which
/// decodes the whole record.
#[tokio::test]
async fn a_source_decoding_part_of_each_record_returns_what_a_table_scan_returns() {
	let db = TestDb::new(DECODE_SCHEMA).await;
	let ctx = db.exec_ctx().await;
	for (sql, reference) in [
		("SELECT label FROM t WHERE n = 1", "SELECT label FROM t WITH NOINDEX WHERE n = 1"),
		(
			"SELECT id, label FROM t WHERE n = 1 ORDER BY label DESC",
			"SELECT id, label FROM t WITH NOINDEX WHERE n = 1 ORDER BY label DESC",
		),
		(
			"SELECT n, count() AS c FROM t WHERE n = 1 GROUP BY n",
			"SELECT n, count() AS c FROM t WITH NOINDEX WHERE n = 1 GROUP BY n",
		),
		(
			"SELECT * OMIT emb FROM t WHERE n = 1",
			"SELECT * OMIT emb FROM t WITH NOINDEX WHERE n = 1",
		),
		(
			"SELECT label FROM t WHERE n = 1 OR n = 2 ORDER BY label",
			"SELECT label FROM t WITH NOINDEX WHERE n = 1 OR n = 2 ORDER BY label",
		),
		(
			"SELECT id, body FROM t WHERE body @@ 'even'",
			"SELECT id, body FROM t WITH NOINDEX WHERE body = 'even'",
		),
		(
			"SELECT VALUE label FROM t WHERE n IN [0, 2]",
			"SELECT VALUE label FROM t WITH NOINDEX WHERE n IN [0, 2]",
		),
	] {
		let mut got = vm_rows(&ctx, sql).await;
		let mut want = vm_rows(&ctx, reference).await;
		assert!(!got.is_empty(), "`{sql}` returns rows");
		if !sql.contains("ORDER BY") {
			got.sort();
			want.sort();
		}
		assert_eq!(got, want, "`{sql}`");
	}
}

/// A reader of stored records given a decode set yields each record's id and
/// the listed fields it stores, and none it does not list.
#[tokio::test]
async fn a_stored_reader_decodes_only_the_listed_fields() {
	let db = TestDb::new(DECODE_SCHEMA).await;
	let ctx = db.exec_ctx().await;
	let decode = crate::exec::operators::scan::common::DecodeFields::new(["label".to_string()]);
	let mut reader = super::batch::RecordReader::stored(&ctx, 100, None, Some(decode));
	reader.push((1..=3i64).map(|i| crate::val::RecordId::new("t".into(), i)).collect());
	let mut rows = Vec::new();
	while !reader.is_drained() {
		rows.extend(reader.next_rows(&ctx).await.expect("reads"));
	}
	assert_eq!(rows.len(), 3, "{rows:?}");
	for row in &rows {
		let Value::Object(object) = row else {
			panic!("a record is an object: {row:?}");
		};
		let mut keys: Vec<&str> = object.keys().map(|k| k.as_str()).collect();
		keys.sort();
		assert_eq!(keys, ["id", "label"], "{row:?}");
	}
}

/// A declared kind is a hint: rows written before `DEFINE FIELD OVERWRITE`
/// retyped their field still hold what they held, and the typed comparison
/// the declaration selects answers them through its fallback.
#[tokio::test]
async fn a_declared_int_is_a_guarded_hint_over_retyped_rows() {
	let db = TestDb::new(
		"
		DEFINE TABLE n SCHEMAFULL;
		DEFINE FIELD v ON n TYPE string;
		CREATE n:1 SET v = '5';
		CREATE n:2 SET v = 'x';
		DEFINE FIELD OVERWRITE v ON n TYPE int;
		CREATE n:3 SET v = 7;
		",
	)
	.await;
	let ctx = db.exec_ctx().await;
	let sql = "SELECT VALUE id FROM n WHERE v > 3";
	let bound = bind(&ctx, sql).await;
	let physical = super::optimize::optimize(
		super::map::map(&planner(&ctx), &bound).await.unwrap_or_else(|e| panic!("`{sql}`: {e:?}")),
	);
	let program = super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
	// The filter is fused into the scan, so the typed comparison is the
	// filter form of `CompareFieldInt`; each carries the `fallback` that
	// compares a value that is not an integer.
	let typed = program.pipelines.iter().flat_map(|pipeline| pipeline.code.iter()).any(|instr| {
		matches!(
			instr,
			Instr::CompareFieldInt { .. }
				| Instr::JumpIfFieldInt { .. }
				| Instr::FilterFieldInt { .. }
		)
	});
	assert!(typed, "`{sql}` compares the declared int typed");
	// A string and an integer compare by type, and a string orders after every number.
	let mut got = vm_rows(&ctx, sql).await;
	got.sort();
	let want: Vec<Value> =
		(1..=3i64).map(|i| Value::RecordId(crate::val::RecordId::new("n".into(), i))).collect();
	assert_eq!(got, want, "`{sql}`");
}
