//! How an export groups records into `INSERT` statements.
//!
//! An import runs one statement per transaction, so the grouping the exporter
//! chooses is what sets transaction size on the way back in. These tests pin the
//! grouping against the two things that decide it — the configured scan batch and
//! the table's index set — and check that changing it leaves the data identical.

use std::sync::Arc;

use anyhow::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_rpc::capabilities::Capabilities;
use surrealdb_types::Value;

use crate::helpers::new_ns_db;

/// Builds a memory datastore, optionally overriding `export_batch_size`.
async fn ds_with_batch_size(batch: Option<usize>) -> Result<Arc<Datastore>> {
	let mut builder = Datastore::builder().with_capabilities(Capabilities::all());
	if let Some(batch) = batch {
		builder = builder.with_config(
			surrealdb_cnf::ConfigMap::empty()
				.with_key_value("export_batch_size", batch.to_string()),
		);
	}
	let ds = builder.build_with_path("memory").await?;
	new_ns_db(&ds, "test", "test").await?;
	Ok(ds)
}

/// Builds a memory datastore that enforces `transaction_max_write_keys`, for the
/// import side of a round trip.
async fn ds_with_write_keys_limit(limit: usize) -> Result<Arc<Datastore>> {
	let ds = Datastore::builder()
		.with_capabilities(Capabilities::all())
		.with_config(
			surrealdb_cnf::ConfigMap::empty()
				.with_key_value("transaction_max_write_keys", limit.to_string()),
		)
		.build_with_path("memory")
		.await?;
	new_ns_db(&ds, "test", "test").await?;
	Ok(ds)
}

/// Exports the whole database as text.
async fn export_text(ds: &Datastore, ses: &Session) -> Result<String> {
	let (tx, rx) = surrealdb_core::channel::bounded::<Vec<u8>>(16);
	let task = ds.export(ses, tx).await?;
	let collector = tokio::spawn(async move {
		let mut out = Vec::new();
		while let Ok(chunk) = rx.recv().await {
			out.extend_from_slice(&chunk);
		}
		out
	});
	task.await?;
	Ok(String::from_utf8(collector.await?)?)
}

/// The number of records each `INSERT [ … ];` line in an export carries.
fn insert_group_sizes(sql: &str) -> Vec<usize> {
	sql.lines()
		.filter(|l| l.starts_with("INSERT [ "))
		// Records are rendered as `{ … }` objects joined by `, `, and the padded
		// field values in these fixtures hold no braces, so counting openers
		// counts records.
		.map(|l| l.matches('{').count())
		.collect()
}

/// Seeds with a document long enough that a full-text index over it costs
/// meaningfully more than the record itself, which is the case where grouping has
/// to tighten. A short field would correctly leave the grouping at the cap.
async fn seed(ds: &Datastore, ses: &Session, schema: &str, records: usize) -> Result<()> {
	seed_with_terms(ds, ses, schema, records, 100).await
}

/// Seeds `records` rows whose `bio` holds `terms` distinct words, so a test can
/// vary how many keys a full-text index writes per record.
async fn seed_with_terms(
	ds: &Datastore,
	ses: &Session,
	schema: &str,
	records: usize,
	terms: usize,
) -> Result<()> {
	ds.execute(schema, ses, None).await?;
	for i in 0..records {
		let bio: Vec<String> = (0..terms).map(|t| format!("w{t}")).collect();
		let sql = format!("CREATE person:{i} SET bio = '{}'", bio.join(" "));
		let mut res = ds.execute(&sql, ses, None).await?;
		res.remove(0).result?;
	}
	Ok(())
}

/// Seeds `records` rows whose `bio` words are unique to the row, so the group's
/// distinct vocabulary grows with every record it carries.
///
/// This is the shape with no slack in it. When rows share their words, one
/// delta-log key covers the whole group and the per-term estimate is far above
/// what the group writes; when they share none, the estimate is exactly met and
/// anything it leaves out is a group that overruns.
async fn seed_with_unique_terms(
	ds: &Datastore,
	ses: &Session,
	schema: &str,
	records: usize,
	terms: usize,
) -> Result<()> {
	ds.execute(schema, ses, None).await?;
	for i in 0..records {
		let bio: Vec<String> = (0..terms).map(|t| format!("r{i}w{t}")).collect();
		let sql = format!("CREATE person:{i} SET bio = '{}'", bio.join(" "));
		ds.execute(&sql, ses, None).await?.remove(0).result?;
	}
	Ok(())
}

const FULLTEXT_SCHEMA: &str = "DEFINE TABLE person SCHEMALESS;
	 DEFINE ANALYZER ab TOKENIZERS blank FILTERS lowercase;
	 DEFINE INDEX ix_bio ON person FIELDS bio FULLTEXT ANALYZER ab BM25";

/// With no index, records write few keys, so the grouping stays at the
/// configured scan batch and nothing tightens it.
#[tokio::test]
async fn plain_table_groups_at_the_configured_batch() -> Result<()> {
	let ds = ds_with_batch_size(Some(10)).await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	seed(&ds, &ses, "DEFINE TABLE person SCHEMALESS", 25).await?;

	let sql = export_text(&ds, &ses).await?;
	// 25 records at a batch of 10: the scan bound is what splits them.
	assert_eq!(insert_group_sizes(&sql), vec![10, 10, 5], "{sql}");
	Ok(())
}

/// A full-text index makes each record write keys in proportion to its distinct
/// term count, so the same records are grouped far more tightly than the scan
/// batch would group them.
#[tokio::test]
async fn full_text_table_groups_more_tightly_than_the_batch() -> Result<()> {
	let ds = ds_with_batch_size(Some(1000)).await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	// Enough terms that the key budget is comfortably what splits these records,
	// with headroom either way. A fixture sized to only just clear the bound flips
	// on any retune of the per-term estimate instead of on the behaviour it pins.
	seed_with_terms(&ds, &ses, FULLTEXT_SCHEMA, 200, 300).await?;

	let sql = export_text(&ds, &ses).await?;
	let groups = insert_group_sizes(&sql);
	let largest = *groups.iter().max().expect("the export must carry records");
	assert!(
		largest < 100,
		"a full-text table must group far below the 1000-record scan batch, got {groups:?}"
	);
	assert_eq!(groups.iter().sum::<usize>(), 200, "every record must still be exported");
	Ok(())
}

/// A full-text index writes per distinct term, so how many keys a record costs
/// scales with its document length. The grouping has to follow that: the same
/// row count over longer documents must be split into more statements.
///
/// This is what a fixed per-index allowance cannot do — it would hand both
/// tables the same grouping and leave the long-document one with the oversized
/// transaction that grouping exists to avoid.
#[tokio::test]
async fn grouping_follows_document_length() -> Result<()> {
	let mut sizes = Vec::new();
	for terms in [10usize, 600] {
		let ds = ds_with_batch_size(Some(1000)).await?;
		let ses = Session::owner().with_ns("test").with_db("test");
		seed_with_terms(&ds, &ses, FULLTEXT_SCHEMA, 60, terms).await?;
		let sql = export_text(&ds, &ses).await?;
		let groups = insert_group_sizes(&sql);
		assert_eq!(groups.iter().sum::<usize>(), 60, "every record must still be exported");
		sizes.push(*groups.iter().max().expect("the export must carry records"));
	}
	let (short_docs, long_docs) = (sizes[0], sizes[1]);
	assert!(
		long_docs < short_docs,
		"longer documents must group more tightly: \
		 {short_docs} for 10 terms vs {long_docs} for 600"
	);
	Ok(())
}

/// A plain index over an array field writes one entry per element, so a record
/// holding a long array costs far more keys than its field count suggests. The
/// grouping has to follow the array length, not the index count.
#[tokio::test]
async fn grouping_follows_array_index_fan_out() -> Result<()> {
	const SCHEMA: &str = "DEFINE TABLE person SCHEMALESS;
		 DEFINE INDEX ix_tags ON person FIELDS tags";
	let mut sizes = Vec::new();
	for tags in [1usize, 400] {
		let ds = ds_with_batch_size(Some(1000)).await?;
		let ses = Session::owner().with_ns("test").with_db("test");
		ds.execute(SCHEMA, &ses, None).await?;
		for i in 0..40 {
			let list: Vec<String> = (0..tags).map(|t| format!("'t{t}'")).collect();
			let sql = format!("CREATE person:{i} SET tags = [{}]", list.join(", "));
			ds.execute(&sql, &ses, None).await?.remove(0).result?;
		}
		let sql = export_text(&ds, &ses).await?;
		let groups = insert_group_sizes(&sql);
		assert_eq!(groups.iter().sum::<usize>(), 40, "every record must still be exported");
		sizes.push(*groups.iter().max().expect("the export must carry records"));
	}
	let (scalar_ish, fanned_out) = (sizes[0], sizes[1]);
	assert_eq!(scalar_ish, 40, "a one-element array must not tighten the grouping");
	assert!(
		fanned_out < scalar_ish,
		"a 400-element array must group more tightly: {scalar_ish} vs {fanned_out}"
	);
	Ok(())
}

/// Grouping is a transport concern: re-importing an export must reproduce the
/// same records however its `INSERT` lines were split.
#[tokio::test]
async fn regrouped_export_round_trips() -> Result<()> {
	let source = ds_with_batch_size(Some(1000)).await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	seed_with_terms(&source, &ses, FULLTEXT_SCHEMA, 120, 400).await?;
	let sql = export_text(&source, &ses).await?;
	assert!(insert_group_sizes(&sql).len() > 1, "the fixture must exercise several groups");

	// Replay into a fresh datastore and compare what came back. The target arms the
	// write-cardinality guard at the budget the grouping is sized against, so a
	// statement that writes more keys than the exporter estimated fails here rather
	// than passing on the estimate being unenforced. That is the whole point of
	// sizing by keys: a group has to fit the transaction it becomes.
	let target = ds_with_write_keys_limit(20_000).await?;
	let mut res = target.execute(&sql, &ses, None).await?;
	for r in res.drain(..) {
		r.result?;
	}

	const COUNT: &str = "SELECT VALUE count() FROM person GROUP ALL";
	let source_count = source.execute(COUNT, &ses, None).await?.remove(0).result?;
	let target_count = target.execute(COUNT, &ses, None).await?.remove(0).result?;
	assert_eq!(source_count, target_count);

	// The full-text index has to answer on the replayed side too, which is what
	// proves the regrouped statements rebuilt index state rather than just rows.
	const MATCHES: &str = "SELECT VALUE count() FROM person WHERE bio @@ 'w0' GROUP ALL";
	let hits = target.execute(MATCHES, &ses, None).await?.remove(0).result?;
	assert_eq!(hits, source.execute(MATCHES, &ses, None).await?.remove(0).result?);
	assert_ne!(hits, Value::None, "the replayed full-text index must match");
	Ok(())
}

/// A group has to fit the transaction it becomes, at the vocabulary where the
/// estimate has no slack: records sharing no words, so every term costs a
/// delta-log key of its own.
///
/// Anything the estimate leaves out of a *record's* cost shows up here: charging
/// only per term — as though the term allowance also covered the record, doc-id,
/// term-map and document-length keys — puts one extra record in each group, which
/// is enough to overrun the budget the group was sized for.
///
/// Records of uniform length, so this says nothing about a group whose vocabulary
/// is concentrated in a few long documents. The estimate derives its per-record
/// cost from the table's mean, which is not a ceiling for such a group.
#[tokio::test]
async fn a_disjoint_vocabulary_group_fits_its_own_import() -> Result<()> {
	let source = ds_with_batch_size(Some(1000)).await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	seed_with_unique_terms(&source, &ses, FULLTEXT_SCHEMA, 70, 300).await?;
	let sql = export_text(&source, &ses).await?;
	let groups = insert_group_sizes(&sql);
	assert!(groups.len() > 1, "the fixture must split into groups the budget decides");

	let target = ds_with_write_keys_limit(20_000).await?;
	let mut res = target.execute(&sql, &ses, None).await?;
	for r in res.drain(..) {
		r.result.map_err(|e| {
			anyhow::anyhow!(
				"a group the exporter sized must import within the budget: {e} ({groups:?})"
			)
		})?;
	}

	const COUNT: &str = "SELECT VALUE count() FROM person GROUP ALL";
	assert_eq!(
		target.execute(COUNT, &ses, None).await?.remove(0).result?,
		source.execute(COUNT, &ses, None).await?.remove(0).result?,
	);
	Ok(())
}

/// An export orders tables by name, so an `ENFORCED` relation whose table name
/// sorts before its endpoints' is replayed while those endpoints are still
/// missing. The restore must keep the graph anyway: enforcement is an admission
/// check on the write path, and an export is replayed as a whole.
///
/// `knows` sorting before `person` is the canonical naming, so this is the
/// ordinary case rather than a contrived one.
#[tokio::test]
async fn enforced_relation_survives_a_self_restore() -> Result<()> {
	let source = ds_with_batch_size(None).await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let mut res = source
		.execute(
			"DEFINE TABLE person SCHEMALESS;
			 DEFINE TABLE knows TYPE RELATION FROM person TO person ENFORCED;
			 CREATE person:a, person:b;
			 RELATE person:a->knows->person:b;",
			&ses,
			None,
		)
		.await?;
	for r in res.drain(..) {
		r.result?;
	}

	let sql = export_text(&source, &ses).await?;
	let tables: Vec<&str> = sql.lines().filter(|l| l.starts_with("-- TABLE: ")).collect();
	assert_eq!(
		tables,
		["-- TABLE: knows", "-- TABLE: person"],
		"the fixture must export the edge table before its endpoints"
	);

	// Every statement has to succeed: a restore that reports one rejected
	// `INSERT RELATION` among thousands of accepted lines is how this went
	// unnoticed, so the assertion is on the statements, not just the outcome.
	let target = ds_with_batch_size(None).await?;
	let mut res = target.execute(&sql, &ses, None).await?;
	for r in res.drain(..) {
		r.result?;
	}

	for query in [
		"SELECT VALUE count() FROM knows GROUP ALL",
		"SELECT VALUE ->knows->person FROM person:a",
		"SELECT VALUE <-knows<-person FROM person:b",
	] {
		let restored = target.execute(query, &ses, None).await?.remove(0).result?;
		assert_ne!(restored, Value::None, "`{query}` found nothing after the restore");
		assert_eq!(restored, source.execute(query, &ses, None).await?.remove(0).result?);
	}

	// Deferring the check must not disable it: the restored table still
	// refuses a new edge to a record that does not exist.
	let err = target
		.execute("RELATE person:a->knows->person:missing", &ses, None)
		.await?
		.remove(0)
		.result
		.expect_err("the restored table must still enforce its endpoints");
	assert!(err.to_string().contains("person:missing"), "unexpected error: {err}");
	Ok(())
}
