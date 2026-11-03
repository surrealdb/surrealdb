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
use surrealdb_iam::{Actor, Auth, Level, Role};
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

/// Builds a memory datastore for a fixture that stores a file value.
async fn ds_with_files() -> Result<Arc<Datastore>> {
	let caps = Capabilities::all();
	let ds = Datastore::builder().with_capabilities(caps).build_with_path("memory").await?;
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

/// Seeds `targets` referenced records and `records` referring ones, each holding
/// every target in one `array<record<…>>` `REFERENCE` field.
///
/// The referring table is named so it sorts before the referenced one, which is
/// the order an export emits and therefore the order the keys are written in.
async fn seed_referring_records(
	ds: &Datastore,
	ses: &Session,
	targets: usize,
	records: usize,
) -> Result<()> {
	const SCHEMA: &str = "DEFINE TABLE zz_person SCHEMALESS;
		 DEFINE TABLE aa_comment SCHEMALESS;
		 DEFINE FIELD authors ON aa_comment TYPE array<record<zz_person>> REFERENCE";
	ds.execute(SCHEMA, ses, None).await?;
	for i in 0..targets {
		ds.execute(&format!("CREATE zz_person:{i}"), ses, None).await?.remove(0).result?;
	}
	let authors: Vec<String> = (0..targets).map(|i| format!("zz_person:{i}")).collect();
	let authors = authors.join(", ");
	for i in 0..records {
		let sql = format!("CREATE aa_comment:{i} SET authors = [{authors}]");
		ds.execute(&sql, ses, None).await?.remove(0).result?;
	}
	Ok(())
}

/// The number of referring records each `INSERT` line of the fixture above
/// carries, ignoring the referenced table's own lines.
fn referring_group_sizes(sql: &str) -> Vec<usize> {
	sql.lines()
		.filter(|l| l.starts_with("INSERT ["))
		.map(|l| l.matches("id: aa_comment:").count())
		.filter(|n| *n > 0)
		.collect()
}

/// A `REFERENCE` field writes one back-reference key per record id it holds, and
/// those keys are written on the import path like any other. So a record whose
/// reference field names many ids costs far more keys than its field count
/// suggests, and the grouping has to follow the id count the same way it follows
/// an array index's fan-out.
///
/// The target arms the write-cardinality guard at the budget the grouping is
/// sized against, so a group charged as though its references were free fails
/// here rather than passing on the estimate being unenforced.
#[tokio::test]
async fn a_reference_bearing_group_fits_its_own_import() -> Result<()> {
	let source = ds_with_batch_size(Some(1000)).await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	seed_referring_records(&source, &ses, 200, 120).await?;

	let sql = export_text(&source, &ses).await?;
	let groups = referring_group_sizes(&sql);

	let target = ds_with_write_keys_limit(20_000).await?;
	let mut res = target.execute(&sql, &ses, None).await?;
	for r in res.drain(..) {
		r.result.map_err(|e| {
			anyhow::anyhow!(
				"a group the exporter sized must import within the budget: {e} ({groups:?})"
			)
		})?;
	}

	// Asserted after the import so a group sized as though references were free
	// reports the overrun it causes rather than the fixture shape.
	assert!(
		groups.len() > 1,
		"the fixture must split into groups the budget decides, got {groups:?}"
	);

	// The back-references have to answer on the replayed side, which is what
	// proves the reference keys the budget accounts for were actually written.
	const REFS: &str = "SELECT VALUE count(<~aa_comment) FROM ONLY zz_person:0";
	let restored = target.execute(REFS, &ses, None).await?.remove(0).result?;
	assert_eq!(restored, source.execute(REFS, &ses, None).await?.remove(0).result?);
	assert_eq!(
		restored,
		Value::Number(surrealdb_types::Number::Int(120)),
		"every referring record must be linked back"
	);
	Ok(())
}

/// The grouping follows how many ids a reference field holds, not how many
/// reference fields there are: the same row count over a longer author list must
/// be split into more statements.
#[tokio::test]
async fn grouping_follows_reference_fan_out() -> Result<()> {
	let mut sizes = Vec::new();
	for targets in [1usize, 200] {
		let ds = ds_with_batch_size(Some(1000)).await?;
		let ses = Session::owner().with_ns("test").with_db("test");
		seed_referring_records(&ds, &ses, targets, 120).await?;
		let sql = export_text(&ds, &ses).await?;
		let groups = referring_group_sizes(&sql);
		assert_eq!(
			groups.iter().sum::<usize>(),
			120,
			"every referring record must still be exported: {groups:?}"
		);
		sizes.push(*groups.iter().max().expect("the export must carry referring records"));
	}
	let (one_author, many_authors) = (sizes[0], sizes[1]);
	assert_eq!(one_author, 120, "a single-id reference must not tighten the grouping");
	assert!(
		many_authors < one_author,
		"a 200-id reference must group more tightly: {one_author} vs {many_authors}"
	);
	Ok(())
}

/// A view whose rows cannot be recomputed inside one transaction keeps them.
///
/// Recomputing a view is a single `DEFINE TABLE ... AS SELECT`, and a target
/// enforcing `transaction_max_write_keys` rejects one whose rows outgrow the
/// limit — even where the source built those same rows through writes that were
/// each well within it, one source record at a time. Nothing in the catalog
/// predicts that, so it is the case the dump cannot classify: it carries every
/// view's rows ahead of its definition, and what the rejection then costs is the
/// definition rather than the table and its data.
#[tokio::test]
async fn a_view_too_large_to_recompute_keeps_its_rows() -> Result<()> {
	let source = ds_with_batch_size(Some(10)).await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	source
		.execute("DEFINE TABLE src SCHEMALESS; DEFINE TABLE v AS SELECT n FROM src;", &ses, None)
		.await?;
	// Built one record at a time, so every write the source made is one a
	// limited target would also have accepted.
	for i in 0..200 {
		source.execute(&format!("CREATE src:{i} SET n = {i}"), &ses, None).await?;
	}

	let sql = export_text(&source, &ses).await?;
	assert!(
		sql.contains("-- TABLE DATA: v"),
		"the dump must carry the view's rows, since nothing here can tell whether its \
		 definition will replay:\n{sql}"
	);

	// A limit every emitted `INSERT` fits inside, and the whole view does not.
	let target = ds_with_write_keys_limit(100).await?;
	let errors: Vec<String> = target
		.execute(&sql, &ses, None)
		.await?
		.into_iter()
		.filter_map(|r| r.result.err().map(|e| e.to_string()))
		.collect();
	assert_eq!(
		errors.len(),
		1,
		"only the view's own definition may exceed the limit — the grouping sizes every \
		 `INSERT` against it: {errors:?}"
	);
	assert!(
		errors[0].contains("maximum number of key writes"),
		"the rejection must be the write-key guard: {errors:?}"
	);

	let restored = target.execute("SELECT VALUE n FROM v ORDER BY n", &ses, None).await?;
	let restored = restored.into_iter().next().expect("one statement").result?;
	let Value::Array(rows) = &restored else {
		anyhow::bail!("`SELECT` must answer with an array, got {restored:?}");
	};
	assert_eq!(
		rows.len(),
		200,
		"the rows the definition could not recompute must survive it: {restored:?}"
	);
	Ok(())
}

/// A sequence is exported at the position it reached, not the `START` it was
/// declared with.
///
/// The definition only records where the sequence began, so emitting it verbatim
/// rewinds the allocator while the exported records keep the ids already taken
/// from it. Everything downstream of a restore depends on this one value.
#[tokio::test]
async fn sequence_exports_its_current_position() -> Result<()> {
	let ds = ds_with_batch_size(None).await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	ds.execute("DEFINE SEQUENCE order_id BATCH 1 START 1", &ses, None).await?.remove(0).result?;
	for _ in 0..3 {
		ds.execute("RETURN sequence::nextval('order_id')", &ses, None).await?.remove(0).result?;
	}

	let sql = export_text(&ds, &ses).await?;
	assert!(
		sql.contains("DEFINE SEQUENCE order_id BATCH 1 START 4"),
		"the dump must carry the position the sequence reached: {sql}"
	);
	Ok(())
}

/// A sequence that has never been used exports its declared `START`.
///
/// The position is read from the per-node cursors, which do not exist yet, so
/// this pins that the declared value is what survives rather than a zero or a
/// value one past it.
#[tokio::test]
async fn unused_sequence_exports_its_declared_start() -> Result<()> {
	let ds = ds_with_batch_size(None).await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	ds.execute("DEFINE SEQUENCE order_id BATCH 10 START 7", &ses, None).await?.remove(0).result?;

	let sql = export_text(&ds, &ses).await?;
	assert!(
		sql.contains("DEFINE SEQUENCE order_id BATCH 10 START 7"),
		"an unused sequence must export its declared start: {sql}"
	);
	Ok(())
}

/// The whole point of exporting the position: a restored database keeps
/// allocating past the records it was restored with.
///
/// Both ways a sequence is consumed are covered, because only one of them
/// announces itself. Feeding a record id fails loudly on a collision; feeding an
/// ordinary field produces a duplicate and no error at all, so the count of
/// distinct values is what catches it.
#[tokio::test]
async fn restored_sequence_does_not_collide_with_imported_records() -> Result<()> {
	let source = ds_with_batch_size(None).await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	source
		.execute(
			"DEFINE SEQUENCE order_id BATCH 1 START 1;
			 DEFINE SEQUENCE invoice_no BATCH 1 START 1;
			 DEFINE TABLE thing SCHEMAFULL;
			 DEFINE FIELD id ON thing TYPE int DEFAULT sequence::nextval('order_id');
			 DEFINE TABLE inv SCHEMAFULL;
			 DEFINE FIELD no ON inv TYPE int DEFAULT sequence::nextval('invoice_no');",
			&ses,
			None,
		)
		.await?
		.drain(..)
		.try_for_each(|r| r.result.map(|_| ()))?;
	for _ in 0..3 {
		source
			.execute("CREATE thing CONTENT {}; CREATE inv CONTENT {};", &ses, None)
			.await?
			.drain(..)
			.try_for_each(|r| r.result.map(|_| ()))?;
	}

	let sql = export_text(&source, &ses).await?;

	let target = ds_with_batch_size(None).await?;
	target.execute(&sql, &ses, None).await?.drain(..).try_for_each(|r| r.result.map(|_| ()))?;

	// A record-id sequence: the write must be admitted at all.
	target
		.execute("CREATE thing CONTENT {}", &ses, None)
		.await?
		.remove(0)
		.result
		.map_err(|e| anyhow::anyhow!("restored record-id sequence collided: {e}"))?;

	// A plain-field sequence raises nothing on a repeat, so read the column back
	// and check the values are distinct. Three restored rows plus one new one.
	target.execute("CREATE inv CONTENT {}", &ses, None).await?.remove(0).result?;
	let nos = target
		.execute("SELECT VALUE no FROM inv", &ses, None)
		.await?
		.remove(0)
		.result?
		.into_array()
		.unwrap();
	let distinct: std::collections::BTreeSet<i64> =
		nos.iter().map(|v| v.clone().into_int().unwrap()).collect();
	assert_eq!(
		(nos.len(), distinct.len()),
		(4, 4),
		"the restored sequence must not re-issue a used value: {nos:?}"
	);
	Ok(())
}

/// A dump carries neither a table's layouts nor its migrations, and a restore
/// stamps what it reads at its own generation, so a row that has not been
/// through its table's steps has to be written as a read of it sees it: the
/// migrated value, and without the field the step removed.
#[tokio::test]
async fn a_row_behind_a_migration_step_exports_as_it_reads() -> Result<()> {
	let source = ds_with_batch_size(None).await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let mut res = source
		.execute(
			"DEFINE TABLE cfg SCHEMALESS;
			 DEFINE FIELD n ON cfg TYPE option<string>;
			 CREATE cfg:1 SET n = '5', legacy = 'x' RETURN NONE;
			 ALTER FIELD n ON cfg TYPE option<int> MIGRATE EXISTING ROWS AS n_int USING <int>$value FALLBACK 0;
			 ALTER TABLE cfg MIGRATE EXISTING ROWS AS drop_undeclared REMOVE UNDECLARED FIELDS;",
			&ses,
			None,
		)
		.await?;
	for r in res.drain(..) {
		r.result?;
	}

	const READ: &str = "SELECT * FROM cfg";
	let read = source.execute(READ, &ses, None).await?.remove(0).result?;
	assert_eq!(read, surrealdb_core::syn::value("[{ id: cfg:1, n: 5 }]")?);

	let sql = export_text(&source, &ses).await?;
	assert!(!sql.contains("legacy"), "a field the step removed must not be dumped: {sql}");
	assert!(!sql.contains("'5'"), "the dump must carry the migrated value: {sql}");

	let target = ds_with_batch_size(None).await?;
	let mut res = target.execute(&sql, &ses, None).await?;
	for r in res.drain(..) {
		r.result?;
	}
	assert_eq!(target.execute(READ, &ses, None).await?.remove(0).result?, read);
	Ok(())
}

/// A group is priced by the records it carries, and a migration step can make a
/// record write more index entries than the bytes stored for it do: a dump of
/// rows whose array a step expands has to group as tightly as rows that held
/// the long array to begin with.
#[tokio::test]
async fn grouping_follows_what_a_migration_step_makes_of_a_record() -> Result<()> {
	let ds = ds_with_batch_size(Some(1000)).await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let mut res = ds
		.execute(
			"DEFINE TABLE person SCHEMALESS;
			 DEFINE FIELD tags ON person TYPE option<array<string>>;",
			&ses,
			None,
		)
		.await?;
	for r in res.drain(..) {
		r.result?;
	}
	for i in 0..40 {
		let sql = format!("CREATE person:{i} SET tags = ['1'] RETURN NONE");
		ds.execute(&sql, &ses, None).await?.remove(0).result?;
	}
	// The rows stay as they were stored: the step is replayed on read and by the
	// rewrite job, and the index is defined over what a read of a row sees.
	let mut res = ds
		.execute(
			"ALTER FIELD tags ON person TYPE option<array<int>> MIGRATE EXISTING ROWS AS widen USING array::concat(array::map($value, |$v| <int>$v), array::range(0, 400)) FALLBACK [];
			 DEFINE INDEX ix_tags ON person FIELDS tags;",
			&ses,
			None,
		)
		.await?;
	for r in res.drain(..) {
		r.result?;
	}

	let sql = export_text(&ds, &ses).await?;
	let groups = insert_group_sizes(&sql);
	assert_eq!(groups.iter().sum::<usize>(), 40, "every record must still be exported");
	let widest = *groups.iter().max().expect("the export must carry records");
	assert!(
		widest < 40,
		"rows an array of 401 entries is read from must group more tightly than one: {groups:?}"
	);
	Ok(())
}

/// `/export` reads the whole database with the authority of root and applies no
/// table or field SELECT permission, so only a principal that bypasses record
/// permissions at the database level may use it. A roleless database-level token
/// passes the level-only `View` check the transport makes, but its reads are
/// permission checked on `/sql`; it must not receive an unfiltered dump
/// (GHSA-qh3v-rr48-x3cq). A database owner still exports.
#[tokio::test]
async fn export_refuses_a_permission_checked_principal() -> Result<()> {
	let ds = ds_with_batch_size(None).await?;

	// The advisory's vector: a roleless (RL:[]) database-level token.
	let roleless = Session {
		au: Arc::new(Auth::new(Actor::new(
			"token".to_owned(),
			vec![],
			Level::Database("test".to_owned(), "test".to_owned()),
		))),
		ns: Some("test".to_owned()),
		db: Some("test".to_owned()),
		..Session::default()
	};
	let (tx, _rx) = surrealdb_core::channel::bounded::<Vec<u8>>(16);
	let err = ds
		.export(&roleless, tx)
		.await
		.err()
		.expect("a roleless database-level token must not export");
	// The refusal must classify as not-allowed on the wire, not as a bare internal
	// error. Every transport reads this kind to pick its status (gRPC
	// PERMISSION_DENIED, HTTP 403); an unclassified refusal would instead arrive as
	// a retryable server fault. `anyhow_to_types_error` is the shared converter the
	// transports apply, so asserting its verdict covers all of them.
	let wire = surrealdb_core::err::anyhow_to_types_error(err);
	assert!(
		wire.is_not_allowed(),
		"the export refusal must classify as not-allowed, got {}",
		wire.kind_str(),
	);

	// A database owner bypasses record permissions and still exports.
	let owner =
		Session::for_level(Level::Database("test".to_owned(), "test".to_owned()), Role::Owner);
	let (tx, _rx) = surrealdb_core::channel::bounded::<Vec<u8>>(16);
	assert!(ds.export(&owner, tx).await.is_ok(), "a database owner must still be able to export",);
	Ok(())
}

/// Only a principal holding `Edit` on `Actor` in the database (`Owner` at or
/// above the database level) receives real `PASSHASH` and `PASSSCRAM` values in
/// `/export`; `Viewer` and `Editor` principals receive `[REDACTED]`.
#[tokio::test]
async fn export_redacts_user_secrets_for_non_owners() -> Result<()> {
	let ds = ds_with_batch_size(None).await?;
	let owner =
		Session::for_level(Level::Database("test".to_owned(), "test".to_owned()), Role::Owner)
			.with_ns("test")
			.with_db("test");
	ds.execute("DEFINE USER alice ON DATABASE PASSWORD 'supersecret' ROLES VIEWER;", &owner, None)
		.await?;

	let owner_sql = export_text(&ds, &owner).await?;
	assert!(
		owner_sql.contains("PASSHASH '$argon2"),
		"database owner export must preserve PASSHASH: {owner_sql}"
	);
	assert!(
		!owner_sql.contains("PASSHASH '[REDACTED]'"),
		"database owner export must not redact PASSHASH: {owner_sql}"
	);

	for role in [Role::Viewer, Role::Editor] {
		let ses = Session::for_level(Level::Database("test".to_owned(), "test".to_owned()), role)
			.with_ns("test")
			.with_db("test");
		let sql = export_text(&ds, &ses).await?;
		assert!(
			!sql.contains("PASSHASH '$argon2"),
			"{role:?} export must not disclose PASSHASH: {sql}"
		);
		assert!(
			sql.contains("PASSHASH '[REDACTED]'"),
			"{role:?} export must redact PASSHASH: {sql}"
		);
		assert!(
			sql.contains("PASSSCRAM '[REDACTED]'"),
			"{role:?} export must redact PASSSCRAM: {sql}"
		);
	}
	Ok(())
}

/// One record holding a value of every kind a record can store, each under a field
/// named for it, so a value that is lost or reshaped on the way back is reported
/// by the field that held it.
const EVERY_VALUE_KIND: &str = r#"CREATE kinds:1 SET
	tbl = type::table('s'),
	tbls = [type::table('select'), type::table('a b'), type::table('1x'), type::table('dog-1')],
	none_arr = [NONE, NULL],
	obj_none = { a: NONE, b: NULL },
	nul = NULL,
	b = true,
	i = 42,
	i_min = -9223372036854775808,
	f = 1.5,
	f_tiny = 2.2250738585072014e-308f,
	nz = -0f,
	inf = 1f / 0f,
	ninf = -1f / 0f,
	nan = 0f / 0f,
	dec = 1.25dec,
	dec_big = 1.23456789012345678901234567890dec,
	dec_zero = 0dec,
	dur = 1h2m3s4ms5us6ns,
	dur0 = 0ns,
	dt = d'2024-01-02T03:04:05.123456789Z',
	uu = u'018f0d8e-0000-7000-8000-000000000000',
	str = "quote \" and 'single' and \\ backslash\nnewline\ttab é 😀",
	str_empty = '',
	arr = [1, 'a', [2]],
	arr_empty = [],
	obj = { a: 1, 'k-d': 2, nested: { deep: [{ x: 'y' }] } },
	obj_empty = {},
	st = type::set(['a', 'b']),
	st_empty = type::set([]),
	geo = (1.5, -2.5),
	geo_poly = <geometry>{ type: 'Polygon', coordinates: [[[0, 0], [1, 0], [1, 1], [0, 0]]] },
	by = type::bytes('abc'),
	by_empty = type::bytes(''),
	rid = kinds:[1, { a: 2 }],
	rid_str = kinds:`a b'c"d`,
	rid_kw = `select`:1,
	rid_range = kinds:1..5,
	rid_uuid = kinds:u'018f0d8e-0000-7000-8000-000000000000',
	rid_neg = kinds:-5,
	rid_nested = kinds:[kinds:2],
	rng1 = 1..10,
	rng2 = ..5,
	rng3 = 'a'..='z',
	rng4 = 1>..=10,
	rng5 = 1..,
	rng6 = ..,
	rx = /a\/b.*/,
	fl = type::file('bucket', '/key/with space.txt');"#;

/// Reads the record in `sql` back as its object, so a field can be compared by name.
async fn read_record_object(
	ds: &Datastore,
	ses: &Session,
	sql: &str,
) -> Result<surrealdb_types::Object> {
	match ds.execute(sql, ses, None).await?.remove(0).result? {
		Value::Object(object) => Ok(object),
		other @ Value::None
		| other @ Value::Null
		| other @ Value::Bool(_)
		| other @ Value::Number(_)
		| other @ Value::String(_)
		| other @ Value::Bytes(_)
		| other @ Value::Duration(_)
		| other @ Value::Datetime(_)
		| other @ Value::Date(_)
		| other @ Value::Time(_)
		| other @ Value::Timestamp(_)
		| other @ Value::Interval(_)
		| other @ Value::Uuid(_)
		| other @ Value::Geometry(_)
		| other @ Value::Table(_)
		| other @ Value::RecordId(_)
		| other @ Value::File(_)
		| other @ Value::Range(_)
		| other @ Value::Regex(_)
		| other @ Value::Array(_)
		| other @ Value::Set(_) => anyhow::bail!("expected the record as an object, got {other:?}"),
	}
}

/// Whether two values are the same after a round trip. Every kind compares with
/// `=` except a float, which compares by its bits so `-0` and `0` are told apart,
/// and a NaN, which equals another NaN.
fn same_round_trip_value(before: &Value, after: &Value) -> bool {
	match (before, after) {
		(
			Value::Number(surrealdb_types::Number::Float(a)),
			Value::Number(surrealdb_types::Number::Float(b)),
		) => (a.is_nan() && b.is_nan()) || a.to_bits() == b.to_bits(),
		_ => before == after,
	}
}

/// Every value kind a record stores must come back equal after an export and an
/// import, under the field that holds it: a value that is dropped, reshaped or
/// turned into a different kind is reported by name.
#[tokio::test]
async fn every_value_kind_round_trips_through_export() -> Result<()> {
	let source = ds_with_files().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	source.execute(EVERY_VALUE_KIND, &ses, None).await?.remove(0).result?;
	let sql = export_text(&source, &ses).await?;

	let target = ds_with_files().await?;
	for r in target.execute(&sql, &ses, None).await? {
		r.result?;
	}

	const READ: &str = "SELECT * FROM ONLY kinds:1";
	let before = read_record_object(&source, &ses, READ).await?;
	let after = read_record_object(&target, &ses, READ).await?;
	for (field, value) in before.iter() {
		let restored = after.get(field).cloned().unwrap_or(Value::None);
		assert!(
			same_round_trip_value(value, &restored),
			"field `{field}` changed across export and import: {value:?} became {restored:?}\n{sql}"
		);
	}
	assert_eq!(before.len(), after.len(), "a field was added or dropped\n{sql}");
	Ok(())
}

/// Runs `sql` and returns the error of its last statement, or `None` when that
/// statement succeeded.
async fn last_statement_error(
	ds: &Datastore,
	ses: &Session,
	sql: &str,
) -> Result<Option<surrealdb_types::Error>> {
	let mut res = ds.execute(sql, ses, None).await?;
	let last = res.pop().expect("the statement must produce a result");
	Ok(last.result.err())
}

/// An import runs the DEFINE validation an interactive statement runs. A DEFAULT
/// that does not coerce to its field's TYPE is refused with the same error either
/// way, and the refused field is not stored, so later writes are not held to it.
#[tokio::test]
async fn import_refuses_a_default_that_does_not_coerce_to_its_type() -> Result<()> {
	const DEFINE: &str = "DEFINE TABLE item SCHEMAFULL; \
		DEFINE FIELD ops ON item TYPE set<string> DEFAULT {};";
	let ses = Session::owner().with_ns("test").with_db("test");

	let interactive = ds_with_batch_size(None).await?;
	let interactive_err = last_statement_error(&interactive, &ses, DEFINE)
		.await?
		.expect("an interactive DEFINE must refuse the DEFAULT");
	assert!(interactive_err.message().contains("does not coerce"), "{interactive_err:?}");

	let imported = ds_with_batch_size(None).await?;
	let import_err = last_statement_error(&imported, &ses, &format!("OPTION IMPORT; {DEFINE}"))
		.await?
		.expect("an import must refuse the DEFAULT as an interactive DEFINE does");
	assert_eq!(import_err.message(), interactive_err.message());

	let write = imported.execute("CREATE item:1", &ses, None).await?.remove(0);
	assert!(write.result.is_ok(), "a refused field must not be stored: {:?}", write.result);
	Ok(())
}

/// A write whose value does not coerce to its field's TYPE is a validation error,
/// not an internal one.
#[tokio::test]
async fn a_field_coercion_failure_is_a_validation_error() -> Result<()> {
	let ds = ds_with_batch_size(None).await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	ds.execute("DEFINE TABLE item SCHEMAFULL; DEFINE FIELD n ON item TYPE int;", &ses, None)
		.await?;
	let err = last_statement_error(&ds, &ses, "CREATE item:1 SET n = 'x'")
		.await?
		.expect("a string must not coerce to int");
	assert!(err.message().contains("Couldn't coerce value for field `n`"), "{err:?}");
	assert!(err.is_validation(), "a coercion failure must be a validation error: {err:?}");
	Ok(())
}
