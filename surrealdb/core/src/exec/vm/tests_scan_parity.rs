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
		// A `COMPUTED` field or a field permission the rows do not read is not
		// evaluated for a row that holds only `id`.
		"SELECT id FROM cmp WHERE point <|3,40|> [0f, 0f]",
		"SELECT id FROM prm WHERE point <|3,40|> [0f, 0f]",
	];
	let records = [
		"SELECT id, name FROM pts WHERE point <|3,40|> [0f, 0f]",
		"SELECT * FROM pts WHERE point <|3,40|> [0f, 0f]",
		"SELECT id FROM pts WHERE point <|3,40|> [0f, 0f] ORDER BY name",
		"SELECT id FROM pts WHERE name != 'p9' AND point <|3,40|> [0f, 0f]",
		"SELECT id, label FROM cmp WHERE point <|3,40|> [0f, 0f]",
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

/// Tables whose record ids name their components: `reading` keyed by a city
/// and a datetime with millisecond parts, so several records share a whole
/// second, and `counter` keyed by a site, an int (some written as a float
/// equal to it, which the key stores as the int) and a tag.
const KEYED_SCHEMA: &str = "
	DEFINE TABLE reading SCHEMALESS;
	DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
	FOR $i IN 0..240 {
		CREATE reading SET
			city = ['A', 'B', 'C'][$i % 3],
			time = time::from_millis(1704067200000 + $i * 337),
			v = $i;
	};
	DEFINE TABLE counter SCHEMALESS;
	DEFINE FIELD id ON counter TYPE [site: string, n: int, tag: string];
	FOR $i IN 0..78 {
		CREATE counter SET
			site = ['a', 'b'][$i % 2],
			n = (($i % 13) - 3) * [1, 1.0][$i % 2],
			tag = ['x', 'y', 'z'][$i % 3],
			v = $i;
	};
";

/// The first millisecond of the `reading` rows.
const READING_EPOCH_MS: i64 = 1704067200000;

/// A deterministic stream of pseudo-random numbers (xorshift64).
struct Draws(u64);

impl Draws {
	fn next(&mut self) -> u64 {
		self.0 ^= self.0 << 13;
		self.0 ^= self.0 >> 7;
		self.0 ^= self.0 << 17;
		self.0
	}

	fn below(&mut self, n: u64) -> u64 {
		self.next() % n
	}

	fn pick<'a>(&mut self, items: &[&'a str]) -> &'a str {
		items[self.below(items.len() as u64) as usize]
	}
}

/// A datetime near the `reading` rows, with a sub-millisecond part at times,
/// written as a literal or as a call evaluated when the scan opens.
fn reading_time(draws: &mut Draws) -> String {
	use chrono::TimeZone as _;
	let ms = READING_EPOCH_MS - 2000 + draws.below(84_000) as i64;
	match draws.below(3) {
		0 => format!("time::from_millis({ms})"),
		_ => {
			let nanos = (ms.rem_euclid(1000) * 1_000_000) as u32 + draws.below(3) as u32 * 250_000;
			let at = chrono::Utc
				.timestamp_opt(ms.div_euclid(1000), nanos)
				.single()
				.expect("a datetime in range");
			format!("d'{}'", at.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
		}
	}
}

/// A random `WHERE` and ordering over `reading`'s components.
fn reading_clauses(draws: &mut Draws) -> String {
	let mut conjuncts = Vec::new();
	let city = draws.pick(&[
		"",
		"city = 'A'",
		"city = 'B'",
		"id[0] = 'C'",
		"city = 'Z'",
		"city IN ['A', 'C']",
		"city IN ['B', 'Z', 'B']",
		"city > 'A'",
		"city = 'A' OR city = 'C'",
	]);
	if !city.is_empty() {
		conjuncts.push(city.to_string());
	}
	match draws.below(8) {
		0 => {}
		1 => conjuncts.push(format!("time > {}", reading_time(draws))),
		2 => conjuncts.push(format!("time >= {}", reading_time(draws))),
		3 => conjuncts.push(format!("time < {}", reading_time(draws))),
		4 => conjuncts.push(format!("time <= {}", reading_time(draws))),
		5 => conjuncts.push(format!(
			"time >= {} AND time < {}",
			reading_time(draws),
			reading_time(draws)
		)),
		6 => conjuncts.push(format!(
			"{} < id[1] AND id[1] <= {}",
			reading_time(draws),
			reading_time(draws)
		)),
		_ => {
			let ms = READING_EPOCH_MS + draws.below(240) as i64 * 337;
			conjuncts.push(format!("time = time::from_millis({ms})"));
		}
	}
	if draws.below(4) == 0 {
		conjuncts.push("v % 2 = 0".to_string());
	}
	let mut clauses = String::new();
	if !conjuncts.is_empty() {
		clauses.push_str(" WHERE ");
		clauses.push_str(&conjuncts.join(" AND "));
	}
	clauses.push_str(draws.pick(&[
		"",
		"",
		" ORDER BY time",
		" ORDER BY time DESC",
		" ORDER BY time LIMIT 4",
		" ORDER BY time DESC LIMIT 3",
		" ORDER BY time LIMIT 3 START 5",
		" ORDER BY id",
		" ORDER BY id DESC LIMIT 5",
	]));
	clauses
}

/// A random int bound, of every number kind.
fn counter_number(draws: &mut Draws) -> &'static str {
	draws.pick(&["-4", "-3", "0", "2", "5", "9", "12", "5.0", "2.5", "-1.5dec", "7dec", "-0.5"])
}

/// A random `WHERE` and ordering over `counter`'s components.
fn counter_clauses(draws: &mut Draws) -> String {
	let mut conjuncts = Vec::new();
	let site = draws.pick(&["", "site = 'a'", "site = 'b'", "site IN ['a', 'b']", "site = 'q'"]);
	if !site.is_empty() {
		conjuncts.push(site.to_string());
	}
	match draws.below(7) {
		0 => {}
		1 => conjuncts.push(format!("n = {}", counter_number(draws))),
		2 => conjuncts.push(format!("n > {}", counter_number(draws))),
		3 => conjuncts.push(format!("n <= {}", counter_number(draws))),
		4 => conjuncts.push(format!(
			"n >= {} AND n < {}",
			counter_number(draws),
			counter_number(draws)
		)),
		5 => conjuncts.push(format!(
			"n IN [{}, {}, {}]",
			counter_number(draws),
			counter_number(draws),
			counter_number(draws)
		)),
		_ => conjuncts.push(format!("n = {} AND tag >= 'y'", counter_number(draws))),
	}
	let mut clauses = String::new();
	if !conjuncts.is_empty() {
		clauses.push_str(" WHERE ");
		clauses.push_str(&conjuncts.join(" AND "));
	}
	clauses.push_str(draws.pick(&[
		"",
		"",
		" ORDER BY n",
		" ORDER BY n DESC",
		" ORDER BY n, tag LIMIT 4",
		" ORDER BY n DESC, tag DESC LIMIT 3 START 1",
		" ORDER BY id",
		" ORDER BY id DESC LIMIT 5",
	]));
	clauses
}

/// Whether the program `sql` lowers to reads key ranges of a table.
async fn reads_key_ranges(ctx: &ExecutionContext, sql: &str) -> bool {
	let program = super::tests_sources::program_of(ctx, sql).await;
	program.pipelines.iter().any(|pipeline| {
		matches!(
			&pipeline.source,
			Source::Scan {
				range: Some(_),
				..
			}
		)
	})
}

/// The `ORDER BY` keys of `clauses`, each the path it orders by, in key
/// order: a statement's order, which leaves rows of equal keys in no order.
fn order_keys(clauses: &str) -> Vec<&str> {
	let Some((_, order)) = clauses.split_once(" ORDER BY ") else {
		return Vec::new();
	};
	let order = order.split(" LIMIT ").next().unwrap_or(order);
	order.split(", ").map(|key| key.strip_suffix(" DESC").unwrap_or(key)).collect()
}

/// Each statement of `clauses` over `table` returns, read through the key
/// ranges its predicate admits, the rows a scan of the whole table with the
/// same predicate returns: the same rows, and where the statement orders them
/// the same order, up to the order of rows whose `ORDER BY` keys are equal,
/// which the statement leaves open (a scan read backwards meets them in the
/// reverse of a forward scan's order). Under a `LIMIT` the rows that tie at
/// its edge may differ too, so an ordered statement is held to the sequence
/// of its rows' keys, and an unlimited one to its rows as well. At least
/// `ranged` of the statements read key ranges. Every row of `table` has a
/// distinct `v`.
async fn keyed_parity(
	ctx: &ExecutionContext,
	table: &str,
	clauses: impl Fn(&mut Draws) -> String,
	seed: u64,
	statements: usize,
	ranged: usize,
) {
	let mut draws = Draws(seed);
	let mut read_ranges = 0;
	for _ in 0..statements {
		let clauses = clauses(&mut draws);
		let sql = format!("SELECT VALUE v FROM {table}{clauses}");
		let scan = format!("SELECT VALUE v FROM {table} WITH NOINDEX{clauses}");
		let got = vm_rows(ctx, &sql).await;
		let want = vm_rows(ctx, &scan).await;
		let keys = order_keys(&clauses);
		if !keys.is_empty() {
			let keyed =
				vm_rows(ctx, &format!("SELECT VALUE [v, [{}]] FROM {table}", keys.join(", ")))
					.await;
			let key_of: std::collections::BTreeMap<Value, Value> = keyed
				.into_iter()
				.map(|row| match row {
					Value::Array(pair) => {
						let mut pair = pair.into_iter();
						(pair.next().expect("a v"), pair.next().expect("its keys"))
					}
					other => panic!("a row of `v` and its keys, not {other:?}"),
				})
				.collect();
			let keys_of = |rows: &[Value]| -> Vec<Value> {
				rows.iter().map(|v| key_of.get(v).cloned().expect("a row of the table")).collect()
			};
			assert_eq!(keys_of(&got), keys_of(&want), "`{sql}` orders its rows");
		}
		if keys.is_empty() || !sql.contains(" LIMIT ") {
			let (mut got, mut want) = (got, want);
			got.sort();
			want.sort();
			assert_eq!(got, want, "`{sql}`");
		}
		if reads_key_ranges(ctx, &sql).await {
			read_ranges += 1;
		}
		assert!(!reads_key_ranges(ctx, &scan).await, "`{scan}` reads key ranges");
	}
	assert!(read_ranges >= ranged, "only {read_ranges} statements over {table} read key ranges");
}

#[tokio::test]
async fn a_key_range_scan_returns_what_the_table_scan_returns() {
	let db = TestDb::new(KEYED_SCHEMA).await;
	let ctx = db.exec_ctx().await;
	keyed_parity(&ctx, "reading", reading_clauses, 0x9e37_79b9_7f4a_7c15, 600, 200).await;
	keyed_parity(&ctx, "counter", counter_clauses, 0x2545_f491_4f6c_dd1d, 400, 100).await;
}

#[tokio::test]
async fn a_key_range_scan_reads_only_its_ranges() {
	let db = TestDb::new(KEYED_SCHEMA).await;
	let ctx = db.exec_ctx().await;
	// Eighty `reading` rows are in city `B`; a range over a few seconds of
	// them reads those alone.
	let cases: &[(&str, usize)] = &[
		("SELECT VALUE v FROM reading WHERE city = 'B'", 80),
		(
			"SELECT VALUE v FROM reading WHERE city = 'B' AND time >= d'2024-01-01T00:00:10Z' \
			 AND time < d'2024-01-01T00:00:20Z'",
			10,
		),
		(
			"SELECT VALUE v FROM reading WHERE city IN ['A', 'C'] AND time < d'2024-01-01T00:00:01Z'",
			2,
		),
		("SELECT VALUE v FROM counter WHERE site = 'a' AND n = 2", 3),
	];
	for (sql, at_most) in cases {
		assert!(reads_key_ranges(&ctx, sql).await, "`{sql}` reads key ranges");
		super::host::RECORDS_SCANNED.with(|scanned| scanned.set(0));
		let rows = vm_rows(&ctx, sql).await;
		let scanned = super::host::RECORDS_SCANNED.with(|scanned| scanned.get());
		// A datetime bound widens to its whole second, so a range may read the
		// records of a second either side of it that the predicate refuses.
		assert!(scanned <= at_most + 6, "`{sql}` read {scanned} records for {} rows", rows.len());
		assert!(scanned >= rows.len(), "`{sql}` read {scanned} records for {} rows", rows.len());
	}
}

/// A feed of 2500 readings twenty to the second, more than the batches a
/// scan reads at once.
const TICK_SCHEMA: &str = "
	DEFINE TABLE tick SCHEMALESS;
	DEFINE FIELD id ON tick TYPE [feed: string, at: datetime];
	FOR $i IN 0..2500 {
		CREATE tick SET feed = 'f', at = time::from_millis(1704067200000 + $i * 50), v = $i;
	};
";

#[tokio::test]
async fn a_bounded_sort_over_a_datetime_component_stops_the_scan_past_its_second() {
	// A region hands its sort a batch's rows when it merges the batch, so
	// with batches out on workers the sort publishes its threshold only as
	// late as the region reads ahead, and the cut, judged later, is looser.
	// How far the scan reads is measured on a run with no workers, where each
	// batch is merged as soon as it runs; the rows are checked on both.
	let serial = surrealdb_cnf::ConfigMap::empty()
		.with_key_value("region_row_threshold", usize::MAX.to_string());
	for (db, bounded) in [
		(TestDb::new(TICK_SCHEMA).await, false),
		(TestDb::new_with_config(TICK_SCHEMA, serial).await, true),
	] {
		stops_past_its_second(&db.exec_ctx().await, bounded).await;
	}
}

/// The coarse cut over `tick`, with how far the scan reads checked where
/// `bounded`.
async fn stops_past_its_second(ctx: &ExecutionContext, bounded: bool) {
	let int = |i: i64| Value::Number(crate::val::Number::Int(i));
	for (sql, want) in [
		("SELECT VALUE v FROM tick WHERE feed = 'f' ORDER BY at LIMIT 3", vec![0, 1, 2]),
		(
			"SELECT VALUE v FROM tick WHERE feed = 'f' ORDER BY at DESC LIMIT 3",
			vec![2499, 2498, 2497],
		),
		(
			"SELECT VALUE v FROM tick WHERE feed = 'f' AND at > d'2024-01-01T00:00:05.5Z' \
			 ORDER BY at LIMIT 2 START 1",
			vec![112, 113],
		),
	] {
		let program = super::tests_sources::program_of(ctx, sql).await;
		let cut = program
			.pipelines
			.iter()
			.any(|pipeline| pipeline.source.topk().is_some_and(|topk| topk.coarse.is_some()));
		assert!(cut, "`{sql}` cuts its scan by the second");
		super::host::RECORDS_SCANNED.with(|scanned| scanned.set(0));
		let got = vm_rows(ctx, sql).await;
		let scanned = super::host::RECORDS_SCANNED.with(|scanned| scanned.get());
		assert_eq!(got, want.into_iter().map(int).collect::<Vec<_>>(), "`{sql}`");
		let scan = sql.replacen(" WHERE ", " WITH NOINDEX WHERE ", 1);
		assert_eq!(got, vm_rows(ctx, &scan).await, "`{sql}`");
		// The sort publishes its threshold once it first holds what it keeps,
		// and the scan stops at the first record of a later second: it reads
		// no more than the batch that record is in, not the whole feed.
		if bounded {
			assert!(scanned <= 1500, "`{sql}` read {scanned} records");
		}
	}
}

/// A feed of 2000 readings a second and a half apart, so that no second holds
/// two: several batches of records, each in a second of its own.
const SPREAD_SCHEMA: &str = "
	DEFINE TABLE spread SCHEMALESS;
	DEFINE FIELD id ON spread TYPE [feed: string, at: datetime];
	FOR $i IN 0..2000 {
		CREATE spread SET feed = 'f', at = time::from_millis(1704067200000 + $i * 1500), v = $i;
	};
";

/// A bounded sort over a datetime component of the record id, whose scan
/// decodes its rows, reads about as many records as the sort keeps, with
/// region workers and without: the scan hands the sort its first records
/// before it reads more, so the sort publishes its threshold while the cut
/// can still stop the scan.
#[tokio::test]
async fn a_bounded_sort_over_a_datetime_component_reads_about_what_it_keeps() {
	let serial = surrealdb_cnf::ConfigMap::empty()
		.with_key_value("region_row_threshold", usize::MAX.to_string());
	for db in
		[TestDb::new(SPREAD_SCHEMA).await, TestDb::new_with_config(SPREAD_SCHEMA, serial).await]
	{
		let ctx = db.exec_ctx().await;
		let int = |i: i64| Value::Number(crate::val::Number::Int(i));
		for (sql, want) in [
			(
				"SELECT * FROM spread WHERE feed = 'f' ORDER BY at DESC LIMIT 10",
				(1990..2000).rev().collect::<Vec<i64>>(),
			),
			("SELECT v, at FROM spread WHERE feed = 'f' ORDER BY at LIMIT 10", (0..10).collect()),
			(
				"SELECT v FROM spread WHERE feed = 'f' AND at >= d'2024-01-01T00:10:00Z' \
				 ORDER BY at LIMIT 3 START 2",
				vec![402, 403, 404],
			),
		] {
			let program = super::tests_sources::program_of(&ctx, sql).await;
			let cut = program.pipelines.iter().any(|pipeline| {
				!pipeline.in_place
					&& pipeline.source.topk().is_some_and(|topk| topk.coarse.is_some())
			});
			assert!(cut, "`{sql}` cuts a scan of decoded rows by the second");
			super::host::RECORDS_SCANNED.with(|scanned| scanned.set(0));
			let got = vm_rows(&ctx, sql).await;
			let scanned = super::host::RECORDS_SCANNED.with(|scanned| scanned.get());
			let values: Vec<Value> = got
				.iter()
				.map(|row| match row {
					Value::Object(object) => object.get("v").cloned().unwrap_or(Value::None),
					other => other.clone(),
				})
				.collect();
			assert_eq!(values, want.into_iter().map(int).collect::<Vec<_>>(), "`{sql}`");
			let scan = sql.replacen(" WHERE ", " WITH NOINDEX WHERE ", 1);
			assert_eq!(got, vm_rows(&ctx, &scan).await, "`{sql}`");
			assert!(scanned <= 24, "`{sql}` read {scanned} records");
		}
	}
}

/// The rows the bounded sort of `program` that publishes a threshold holds
/// when it first publishes one, fed rows whose keys rise, one at a time.
fn rows_held_at_first_threshold(program: &super::program::Program) -> usize {
	let registry = crate::exec::function::FunctionRegistry::with_builtins();
	let at = program
		.sorts
		.iter()
		.position(|spec| spec.pushdown)
		.expect("a sort that publishes its threshold");
	let keep = program.sorts[at].keep.expect("a bounded sort");
	let mut state =
		super::interp::State::new(program, Vec::new(), &registry).expect("the state starts");
	let cell = std::sync::Arc::new(crate::exec::topk_pushdown::TopKThresholdCell::default());
	state.topk_cells[at] = Some(std::sync::Arc::clone(&cell));
	for held in 1..=keep * 4 + 64 {
		state.sorts[at]
			.push((vec![Value::from(held as i64)], super::interp::SortRow::Value(Value::None)));
		state.keep_sorted(program, at as u16);
		if cell.snapshot().is_some() {
			return held;
		}
	}
	panic!("the sort never published a threshold");
}

/// A bounded sort whose threshold a scan of stored bytes reads (the top-k
/// pushdown of a table scan) publishes it once it holds a quarter more rows
/// than it keeps, at least sixteen more; only the sort that cuts a key-range
/// scan by the second of a datetime component publishes as soon as it holds
/// what it keeps.
#[tokio::test]
async fn only_a_coarse_cut_sort_publishes_its_threshold_once_full() {
	let db = TestDb::new(&format!(
		"{TICK_SCHEMA}
		DEFINE TABLE plain SCHEMALESS;
		FOR $i IN 0..40 {{ CREATE plain SET v = $i; }};"
	))
	.await;
	let ctx = db.exec_ctx().await;
	let pushed =
		super::tests_sources::program_of(&ctx, "SELECT * FROM plain ORDER BY v LIMIT 3").await;
	assert!(
		pushed
			.pipelines
			.iter()
			.any(|pipeline| pipeline.source.topk().is_some_and(|topk| topk.coarse.is_none())),
		"the table scan reads the sort's threshold from stored bytes"
	);
	assert!(pushed.sorts.iter().all(|spec| !spec.floors_at_keep));
	assert_eq!(rows_held_at_first_threshold(&pushed), 3 + 16);
	let wide =
		super::tests_sources::program_of(&ctx, "SELECT * FROM plain ORDER BY v LIMIT 100").await;
	assert_eq!(rows_held_at_first_threshold(&wide), 100 + 25);
	let coarse = super::tests_sources::program_of(
		&ctx,
		"SELECT VALUE v FROM tick WHERE feed = 'f' ORDER BY at LIMIT 3",
	)
	.await;
	assert!(coarse.sorts.iter().any(|spec| spec.floors_at_keep));
	assert_eq!(rows_held_at_first_threshold(&coarse), 3);
}

/// How the sort of `sql` reads its first key: `Some(true)` evaluated,
/// `Some(false)` read as a path of the row, `None` with no sort.
async fn first_sort_key_evaluated(ctx: &ExecutionContext, sql: &str) -> Option<bool> {
	use super::plan::{Plan, SortKeyRead};
	let bound = bind(ctx, sql).await;
	let physical = super::map::map(&planner(ctx), &bound).await.expect("statement maps");
	let mut node = &physical.root;
	loop {
		node = match node {
			Plan::Sort {
				keys,
				..
			} => return keys.first().map(|key| matches!(key.read, SortKeyRead::Computed(_))),
			Plan::Project {
				input,
				..
			}
			| Plan::Limit {
				input,
				..
			} => input,
			_ => return None,
		};
	}
}

/// An `ORDER BY id[i]` is evaluated over a table whose record ids name their
/// components, where the transparency of the components reads them as
/// `id[i]`; over any other table it is read as the path it spells, as it
/// always has been.
#[tokio::test]
async fn an_order_by_an_id_element_is_evaluated_only_over_a_keyed_table() {
	let db = TestDb::new(&format!(
		"{TICK_SCHEMA}
		DEFINE TABLE pair SCHEMALESS;
		CREATE pair:['b', 1] SET v = 1;
		CREATE pair:['a', 2] SET v = 2;"
	))
	.await;
	let ctx = db.exec_ctx().await;
	for (sql, evaluated) in [
		("SELECT * FROM pair ORDER BY id[0]", false),
		("SELECT v FROM pair ORDER BY id[1] DESC LIMIT 1", false),
		("SELECT * FROM pair ORDER BY v", false),
		("SELECT * FROM tick ORDER BY id[1] LIMIT 2", true),
		("SELECT v FROM tick ORDER BY id[0], v", true),
	] {
		assert_eq!(first_sort_key_evaluated(&ctx, sql).await, Some(evaluated), "`{sql}`");
	}
}

/// Twins of the [`KEYED_SCHEMA`] tables with no named components: the same
/// rows keyed by plain array ids, each component also stored as a field of
/// the record (an int component as the int its key holds).
const TWIN_SCHEMA: &str = "
	DEFINE TABLE reading_twin SCHEMALESS;
	FOR $i IN 0..240 {
		LET $city = ['A', 'B', 'C'][$i % 3];
		LET $time = time::from_millis(1704067200000 + $i * 337);
		CREATE type::record('reading_twin', [$city, $time]) SET city = $city, time = $time, v = $i;
	};
	DEFINE TABLE counter_twin SCHEMALESS;
	FOR $i IN 0..78 {
		LET $site = ['a', 'b'][$i % 2];
		LET $n = ($i % 13) - 3;
		LET $tag = ['x', 'y', 'z'][$i % 3];
		CREATE type::record('counter_twin', [$site, $n, $tag]) SET site = $site, n = $n, tag = $tag, v = $i;
	};
";

/// Each statement of `clauses` over keyed `table` returns what the same
/// statement over its unkeyed twin `twin` returns, the twin read by a table
/// scan of stored fields: the same rows, and where the statement orders them,
/// the same sequence of `ORDER BY` keys (rows whose keys tie are in no order).
/// An `ORDER BY id` is left out: the twin's array ids order by their key
/// bytes, which put a datetime's fraction of a second before the whole
/// second it follows.
async fn twin_parity(
	ctx: &ExecutionContext,
	table: &str,
	twin: &str,
	clauses: impl Fn(&mut Draws) -> String,
	seed: u64,
	statements: usize,
) {
	let mut draws = Draws(seed);
	let mut compared = 0;
	while compared < statements {
		let clauses = clauses(&mut draws);
		if clauses.contains("ORDER BY id") {
			continue;
		}
		compared += 1;
		let sql = format!("SELECT VALUE v FROM {table}{clauses}");
		let base = format!("SELECT VALUE v FROM {twin}{clauses}");
		let got = vm_rows(ctx, &sql).await;
		let want = vm_rows(ctx, &base).await;
		let keys = order_keys(&clauses);
		if keys.is_empty() || !clauses.contains(" LIMIT ") {
			let (mut got, mut want) = (got.clone(), want.clone());
			got.sort();
			want.sort();
			assert_eq!(got, want, "`{sql}` against `{base}`");
		}
		if !keys.is_empty() {
			let keyed =
				vm_rows(ctx, &format!("SELECT VALUE [v, [{}]] FROM {twin}", keys.join(", "))).await;
			let key_of: std::collections::BTreeMap<Value, Value> = keyed
				.into_iter()
				.map(|row| match row {
					Value::Array(pair) => {
						let mut pair = pair.into_iter();
						(pair.next().expect("a v"), pair.next().expect("its keys"))
					}
					other => panic!("a row of `v` and its keys, not {other:?}"),
				})
				.collect();
			let keys_of = |rows: &[Value]| -> Vec<Value> {
				rows.iter().map(|v| key_of.get(v).cloned().expect("a row of the twin")).collect()
			};
			assert_eq!(keys_of(&got), keys_of(&want), "`{sql}` against `{base}` orders its rows");
		}
	}
}

/// A table whose record ids name their components answers every predicate
/// and ordering over them as a table with plain array ids answers the same
/// statement over stored copies of the components: the key range is a
/// superset, the predicate above it is kept, and nothing about the answer
/// depends on how the rows were read.
#[tokio::test]
async fn a_keyed_table_answers_as_its_unkeyed_twin() {
	let db = TestDb::new(&format!("{KEYED_SCHEMA}{TWIN_SCHEMA}")).await;
	let ctx = db.exec_ctx().await;
	twin_parity(&ctx, "reading", "reading_twin", reading_clauses, 0x51ed_270b_27b4_e2a1, 400).await;
	twin_parity(&ctx, "counter", "counter_twin", counter_clauses, 0x0bad_5eed_c0ff_ee11, 300).await;
}
