//! Phase 3 of the plan: derived state — references, sequence positions and
//! index-backed reads, none of which appear in the records or in the catalog and
//! so are compared by querying both databases.
//!
//! Every structure here is a second copy of something, maintained by the write
//! path and read back through a different door. A reference key restates which
//! records name a given record. A sequence's position restates how far the
//! issuer has got. An index restates a field's values in an order a scan cannot
//! produce. A graph edge restates an adjacency both of its endpoints could
//! otherwise be searched for.
//!
//! What they share is that losing one is quiet. The records still restore, so
//! [`RoundTrip::assert_records_equal`] passes; the definitions still restore, so
//! [`RoundTrip::assert_catalog_equal`] passes; and because the definitions are
//! all the exporter writes down, the two exports still match, so
//! [`RoundTrip::assert_fixed_point`] passes too. A missing index answers with the
//! same rows a table scan would. A rewound sequence answers with a plausible
//! number. An unwritten reference key answers with an empty list, which is also
//! what a record with no referrers answers with.
//!
//! So each fixture below reads the structure through the door only it opens, and
//! does it twice:
//!
//! - against both databases with [`RoundTrip::assert_query_equal`], which is what makes the
//!   restore's answer wrong rather than merely unexpected, and
//! - against the restored database alone with an absolute expectation, because two databases that
//!   both lost the same thing agree with each other.
//!
//! Three habits recur.
//!
//! **An index is asserted through its plan, not only its rows.** An index that
//! failed to rebuild does not error: the planner finds no index, falls back to a
//! scan, and returns exactly the rows the index would have. Rows alone therefore
//! cannot separate a working index from an absent one, so every index fixture
//! also asserts that the operator only an index produces is in the restored
//! database's plan.
//!
//! **A reference is read after a write, not only before one.** A reference key
//! that is present but names the wrong record reads correctly once — the first
//! read is the one the restore just wrote. It is the next `UPDATE` or `DELETE`,
//! which diffs against that key and rewrites it, that tells a right key from a
//! wrong one.
//!
//! **Where the current behaviour is a loss, the fixture pins the loss** and says
//! so, so that repairing it fails here and the expectation is updated
//! deliberately rather than a restore quietly staying broken.

use anyhow::{Result, bail};
use surrealdb_types::{Number, ToSql, Value};

use super::RoundTrip;

/// Asserts the restored database answers `sql` with exactly `expected`,
/// rendered as SurrealQL.
///
/// [`RoundTrip::assert_query_equal`] establishes that the two databases agree;
/// this establishes what they agree on, which is the half that a loss suffered
/// identically by both would otherwise slip past. Comparing the rendering rather
/// than the value keeps `2` distinct from `2f`, which matters wherever the
/// fixture is counting.
async fn assert_target_renders(rt: &RoundTrip, sql: &str, expected: &str) -> Result<()> {
	let value = target_value(rt, sql).await?;
	assert_eq!(value.to_sql(), expected, "the restored database answered `{sql}` with {value:?}");
	Ok(())
}

/// The single value the restored database answers `sql` with.
async fn target_value(rt: &RoundTrip, sql: &str) -> Result<Value> {
	let mut values = rt.target_query(sql).await?;
	if values.len() != 1 {
		bail!("`{sql}` must answer with exactly one statement, got {}", values.len());
	}
	Ok(values.remove(0))
}

/// Asserts both databases plan `sql` the same way, and that the plan carries
/// `marker`.
///
/// `marker` is a fragment of the rendered plan that only appears when an index
/// answers the read — the operator's name, or the index's own name where the
/// operator reports it. Asserting it on the source first separates the two ways
/// this can fail: a fixture whose index never answered the query in the first
/// place is a fixture that proves nothing about a restore, and it must not be
/// reported as a lost index.
async fn assert_plans_through(rt: &RoundTrip, sql: &str, marker: &str) -> Result<()> {
	let explain = format!("{sql} EXPLAIN");
	let source = rt.source_query(&explain).await?;
	let [source] = source.as_slice() else {
		bail!("`{explain}` must answer with exactly one statement, got {}", source.len());
	};
	let source = source.to_sql();
	assert!(
		source.contains(marker),
		"the seeded database does not plan `{sql}` through `{marker}`, so this fixture says \
		 nothing about whether a restore keeps the index that would:\n  {source}"
	);

	rt.assert_query_equal(&explain).await?;

	let target = target_value(rt, &explain).await?.to_sql();
	assert!(
		target.contains(marker),
		"the restored database does not plan `{sql}` through `{marker}`. An index that failed \
		 to rebuild answers with the rows a scan answers with, so the plan is the only place \
		 this shows:\n  {target}"
	);
	Ok(())
}

/// Asserts the restored database refuses `sql`, with `needle` in the error.
///
/// A constraint is enforced by the structure that backs it, so a restore that
/// dropped the structure accepts what the source refused. That acceptance is the
/// failure, and it is silent — the write succeeds and the database is left
/// holding a row the schema says cannot exist.
async fn assert_target_rejects(rt: &RoundTrip, sql: &str, needle: &str) -> Result<()> {
	let Err(error) = rt.target_query(sql).await else {
		bail!("the restored database accepted `{sql}`, which the constraint must have refused");
	};
	let cause = error.root_cause().to_string();
	assert!(
		cause.contains(needle),
		"the restored database refused `{sql}`, but not for the expected reason — wanted an \
		 error mentioning `{needle}`, got: {cause}"
	);
	Ok(())
}

// ----------------------------------------------------------------------------
// References
// ----------------------------------------------------------------------------

/// A back-reference read purely by traversal survives, with no field to carry
/// it.
///
/// The referenced table declares nothing at all: `<~zz_comment` is answered from
/// reference keys the referring records' writes deposited, and those keys are
/// the whole of what makes the traversal non-empty. Nothing else in the round
/// trip can see them — the referring record's own `post` field restores either
/// way, and the referenced table's definition never mentions the relationship —
/// so a restore that wrote no keys leaves both databases holding identical
/// records and identical catalogs while one answers the traversal and the other
/// answers `[]`.
#[tokio::test]
async fn a_back_reference_survives_with_no_field_to_carry_it() -> Result<()> {
	let round_trip = RoundTrip::run(
		"DEFINE TABLE aa_post SCHEMALESS;
		 DEFINE TABLE zz_comment SCHEMALESS;
		 DEFINE FIELD post ON zz_comment TYPE record<aa_post> REFERENCE;
		 CREATE aa_post:one, aa_post:two;
		 CREATE zz_comment:a SET post = aa_post:one;
		 CREATE zz_comment:b SET post = aa_post:one;
		 CREATE zz_comment:c SET post = aa_post:two;",
	)
	.await?;

	round_trip.assert_all().await?;
	round_trip.assert_query_equal("SELECT VALUE <~zz_comment FROM aa_post ORDER BY id").await?;
	assert_target_renders(
		&round_trip,
		"SELECT VALUE <~zz_comment FROM aa_post ORDER BY id",
		"[[zz_comment:a, zz_comment:b], [zz_comment:c]]",
	)
	.await?;
	// The traversal reaches the referring records' own fields, so the keys are
	// addressing whole records rather than merely counting.
	round_trip.assert_query_equal("SELECT VALUE <~zz_comment.post FROM aa_post ORDER BY id").await
}

/// A record that references several targets from one array field deposits a key
/// per element, and every one of them survives.
///
/// A scalar reference field contributes at most one key per record, so a restore
/// that wrote the first key of a record and stopped would look correct there. An
/// array field is where per-record fan-out is observable: one referring record
/// has to appear under two different referenced records, and one referenced
/// record has to gather two different referrers.
#[tokio::test]
async fn a_back_reference_from_an_array_field_survives_the_restore() -> Result<()> {
	let round_trip = RoundTrip::run(
		"DEFINE TABLE person SCHEMALESS;
		 DEFINE FIELD comics ON person TYPE array<record<comic_book>> REFERENCE;
		 DEFINE TABLE comic_book SCHEMALESS;
		 DEFINE FIELD owners ON comic_book COMPUTED <~person;
		 CREATE person:one SET comics = [comic_book:a, comic_book:b];
		 CREATE person:two SET comics = [comic_book:a];
		 CREATE comic_book:a, comic_book:b;",
	)
	.await?;

	round_trip.assert_all().await?;
	round_trip.assert_query_equal("SELECT VALUE owners FROM comic_book ORDER BY id").await?;
	assert_target_renders(
		&round_trip,
		"SELECT VALUE owners FROM comic_book ORDER BY id",
		"[[person:one, person:two], [person:one]]",
	)
	.await?;
	// The `COMPUTED` field is one door onto the keys and the traversal is
	// another; a field that restored while the keys did not would answer the
	// first and not the second.
	assert_target_renders(
		&round_trip,
		"SELECT VALUE <~person FROM comic_book ORDER BY id",
		"[[person:one, person:two], [person:one]]",
	)
	.await
}

/// A reference from a table to itself survives.
///
/// Referrer and referent are the same table, so the keys are written into the
/// range being restored while that range is still being filled, and every record
/// is on both ends of the relationship at once. A restore that ordered the work
/// by table rather than by record would find nothing wrong with this fixture and
/// still lose it.
#[tokio::test]
async fn a_self_reference_survives_the_restore() -> Result<()> {
	let round_trip = RoundTrip::run(
		"DEFINE TABLE node SCHEMALESS;
		 DEFINE FIELD parent ON node TYPE option<record<node>> REFERENCE;
		 DEFINE FIELD children ON node COMPUTED <~node;
		 CREATE node:root;
		 CREATE node:a SET parent = node:root;
		 CREATE node:b SET parent = node:root;
		 CREATE node:c SET parent = node:a;",
	)
	.await?;

	round_trip.assert_all().await?;
	round_trip.assert_query_equal("SELECT id, children FROM node ORDER BY id").await?;
	assert_target_renders(
		&round_trip,
		"SELECT VALUE children FROM node ORDER BY id",
		"[[node:c], [], [], [node:a, node:b]]",
	)
	.await
}

/// A reference survives when its target table is restored after the record that
/// references it.
///
/// The export emits stored tables in name order, so a referring table whose name
/// sorts first is replayed in full — definition and records — before the
/// referenced table has been defined at all. The keys those records deposit name
/// a table that does not yet exist, and a restore that waited for the target, or
/// that treated the missing table as a reason to skip the key, would lose
/// exactly the references this fixture holds and nothing else.
#[tokio::test]
async fn a_reference_survives_its_target_table_being_restored_later() -> Result<()> {
	let round_trip = RoundTrip::run(
		"DEFINE TABLE aa_post SCHEMALESS;
		 DEFINE FIELD tags ON aa_post TYPE array<record<zz_tag>> REFERENCE;
		 DEFINE TABLE zz_tag SCHEMALESS;
		 DEFINE FIELD posts ON zz_tag COMPUTED <~aa_post;
		 CREATE zz_tag:x, zz_tag:y;
		 CREATE aa_post:1 SET tags = [zz_tag:x, zz_tag:y];
		 CREATE aa_post:2 SET tags = [zz_tag:y];",
	)
	.await?;

	// The ordering is the whole point of the fixture, so it is asserted rather
	// than assumed: were the export to start emitting the referenced table
	// first, this would silently become a duplicate of the ordinary case.
	let export = round_trip.export_a();
	let referrer_data = export
		.find("INSERT [ { id: aa_post:1")
		.expect("the export must carry the referring table's records");
	let target_definition =
		export.find("DEFINE TABLE zz_tag").expect("the export must carry the referenced table");
	assert!(
		referrer_data < target_definition,
		"this fixture only covers the late-target case while the referring table's records are \
		 replayed before the referenced table is defined:\n{export}"
	);

	round_trip.assert_all().await?;
	round_trip.assert_query_equal("SELECT VALUE posts FROM zz_tag ORDER BY id").await?;
	assert_target_renders(
		&round_trip,
		"SELECT VALUE posts FROM zz_tag ORDER BY id",
		"[[aa_post:1], [aa_post:1, aa_post:2]]",
	)
	.await
}

/// A restored reference still cascades when the record it names is deleted.
///
/// Reading a reference key tells you it exists; it does not tell you the key is
/// addressed so that the delete path finds it. `ON DELETE CASCADE` is the
/// clause that asks: the delete enumerates the referrers from the keys and
/// removes them, so a key written under the wrong address reads back correctly
/// and then fails to cascade, leaving a record pointing at a record that is
/// gone.
#[tokio::test]
async fn a_restored_reference_still_cascades_on_delete() -> Result<()> {
	let round_trip = RoundTrip::run(
		"DEFINE TABLE author SCHEMALESS;
		 DEFINE TABLE book SCHEMALESS;
		 DEFINE FIELD wrote ON book TYPE record<author> REFERENCE ON DELETE CASCADE;
		 CREATE author:ann, author:bo;
		 CREATE book:one SET wrote = author:ann;
		 CREATE book:two SET wrote = author:ann;
		 CREATE book:three SET wrote = author:bo;",
	)
	.await?;

	// Everything below mutates the restored database, so the two are compared
	// while they still hold the same thing.
	round_trip.assert_all().await?;

	round_trip.target_query("DELETE author:ann").await?;
	assert_target_renders(&round_trip, "SELECT VALUE id FROM book ORDER BY id", "[book:three]")
		.await?;
	// The seeded database is untouched, so the delete really removed something
	// rather than finding a table that was already empty.
	assert_eq!(
		round_trip.source_query("SELECT VALUE id FROM book ORDER BY id").await?[0].to_sql(),
		"[book:one, book:three, book:two]",
	);
	Ok(())
}

/// A restored reference is rewritten correctly by the next write against it.
///
/// A key that is present but names the wrong record answers the first read
/// correctly, because the first read is of the key the restore just wrote. The
/// write after it is what separates them: an update diffs the record's new
/// field value against the key set the record already has, so a key the restore
/// recorded under the wrong address is neither found nor removed, and the record
/// is left reachable from a parent it no longer names.
#[tokio::test]
async fn a_restored_reference_is_maintained_by_a_later_update() -> Result<()> {
	let round_trip = RoundTrip::run(
		"DEFINE TABLE node SCHEMALESS;
		 DEFINE FIELD parent ON node TYPE option<record<node>> REFERENCE;
		 DEFINE FIELD children ON node COMPUTED <~node;
		 CREATE node:root;
		 CREATE node:a SET parent = node:root;
		 CREATE node:b SET parent = node:root;",
	)
	.await?;

	round_trip.assert_all().await?;

	// Repointing one record has to move it on both ends: out of the old
	// parent's children and into the new one's.
	round_trip.target_query("UPDATE node:b SET parent = node:a RETURN NONE").await?;
	assert_target_renders(
		&round_trip,
		"SELECT VALUE children FROM node ORDER BY id",
		"[[node:b], [], [node:a]]",
	)
	.await?;

	// Clearing the field has to remove the key rather than leave it naming a
	// record that no longer points anywhere.
	round_trip.target_query("UPDATE node:b SET parent = NONE RETURN NONE").await?;
	assert_target_renders(
		&round_trip,
		"SELECT VALUE children FROM node ORDER BY id",
		"[[], [], [node:a]]",
	)
	.await
}

// ----------------------------------------------------------------------------
// Sequences
// ----------------------------------------------------------------------------

/// A view whose projection reads something other than its sources restores with
/// recomputed values rather than the ones that were exported.
///
/// A dump carries a view's definition and none of its records, so the restore
/// recomputes them. That is exact for a projection that is a function of its
/// source rows, and drifts for one that reads anything else — `time::now()`,
/// `rand()`, a value from outside the database.
///
/// This pins the trade-off rather than a repairable loss. Replaying the
/// definition writes the view's rows, so an `INSERT` carrying the same ids is
/// refused as a duplicate record: a dump that carried them too would report a
/// rejected statement for every view it holds. The read-only rule on view tables
/// is not what settles that — a restore runs under `OPTION IMPORT`, which exempts
/// writes to a view, so such an `INSERT` reaches the record and fails on the id
/// rather than on the table. The fixture asserts both halves — the value moved,
/// and what came back is still a view — because the second is what is being
/// bought with the first.
#[tokio::test]
async fn a_view_reading_beyond_its_sources_recomputes_its_rows() -> Result<()> {
	let round_trip = RoundTrip::run(
		"DEFINE TABLE source SCHEMALESS;
		 DEFINE TABLE observed AS SELECT id, time::now() AS seen FROM source;
		 CREATE source:1 SET n = 1;",
	)
	.await?;

	let seeded = round_trip.source_query("SELECT VALUE seen FROM observed:1").await?;
	let restored = round_trip.target_query("SELECT VALUE seen FROM observed:1").await?;
	assert_ne!(
		seeded.first().map(ToSql::to_sql),
		restored.first().map(ToSql::to_sql),
		"the restored view carries the exported value, so the recomputation this pins is gone"
	);

	// The view is still a view: it refuses a direct write, and a new source
	// record produces a new row without anything asking it to.
	assert_target_rejects(&round_trip, "CREATE observed:2 SET seen = 1", "view").await?;
	round_trip.target_query("CREATE source:2 SET n = 2").await?;
	assert_target_renders(
		&round_trip,
		"SELECT VALUE id FROM observed ORDER BY id",
		"[observed:1, observed:2]",
	)
	.await
}

/// A restored sequence continues past the positions the seeded database spent.
///
/// How far a sequence has advanced is not part of its definition, so an export
/// carries it the only way the dump format allows: as the `START` of the
/// definition it writes. The restored sequence therefore continues where the
/// seeded one had reached, and no value it hands out collides with a record the
/// restore itself wrote.
///
/// The price is that the two catalogs no longer agree for a sequence that has
/// been drawn from — the target's `START` is the position, the source's is where
/// it began — so this fixture compares records and the fixed point but not the
/// catalog, and asserts the divergence it expects instead. A sequence that was
/// never drawn from exports its declared `START` and round-trips identically;
/// `round_trip::catalog` covers that.
#[tokio::test]
async fn a_restored_sequence_continues_past_what_the_source_spent() -> Result<()> {
	let round_trip = RoundTrip::run(
		"DEFINE SEQUENCE ticket_no BATCH 10 START 100;
		 DEFINE SEQUENCE batched BATCH 7;
		 DEFINE TABLE ticket SCHEMALESS;
		 CREATE ticket:1 SET n = sequence::nextval('ticket_no'), b = sequence::nextval('batched');
		 CREATE ticket:2 SET n = sequence::nextval('ticket_no'), b = sequence::nextval('batched');
		 CREATE ticket:3 SET n = sequence::nextval('ticket_no'), b = sequence::nextval('batched');",
	)
	.await?;

	// Not `assert_all`: the catalog no longer matches for a sequence that has
	// been drawn from, which the closing assertion pins deliberately.
	round_trip.assert_records_equal().await?;
	round_trip.assert_fixed_point();
	// The records carry the positions that were spent, on both sides.
	assert_target_renders(
		&round_trip,
		"SELECT VALUE [n, b] FROM ticket ORDER BY id",
		"[[100, 0], [101, 1], [102, 2]]",
	)
	.await?;

	// The seeded database continues past what it spent — a sequence that did
	// not would make the target's answer below unremarkable.
	assert_eq!(
		round_trip
			.source_query("RETURN [sequence::nextval('ticket_no'), sequence::nextval('batched')]")
			.await?[0]
			.to_sql(),
		"[103, 3]",
	);

	// The restored one continues from the same place, so neither value collides
	// with a record the restore itself wrote.
	assert_target_renders(
		&round_trip,
		"RETURN [sequence::nextval('ticket_no'), sequence::nextval('batched')]",
		"[103, 3]",
	)
	.await?;
	assert_target_renders(&round_trip, "SELECT VALUE id FROM ticket WHERE n = 103", "[]").await?;

	// And the definitions differ by exactly the position the export carried: the
	// target's `START` is where the sequence had reached, the source's is where
	// it began.
	assert_target_renders(
		&round_trip,
		"INFO FOR DB.sequences.ticket_no",
		"'DEFINE SEQUENCE ticket_no BATCH 10 START 103'",
	)
	.await?;
	assert_eq!(
		round_trip.source_query("INFO FOR DB.sequences.ticket_no").await?[0].to_sql(),
		"'DEFINE SEQUENCE ticket_no BATCH 10 START 100'",
	);
	Ok(())
}

// ----------------------------------------------------------------------------
// Indexes
// ----------------------------------------------------------------------------

/// A restored unique index still refuses a duplicate.
///
/// Uniqueness is not a property of the records; it is a property of the index
/// entries a write collides with. A restore that recreated the definition
/// without repopulating the entries answers every read identically and then
/// accepts the first duplicate offered to it, which is the point at which the
/// database starts holding a row its own schema forbids.
#[tokio::test]
async fn a_restored_unique_index_still_rejects_a_duplicate() -> Result<()> {
	let round_trip = RoundTrip::run(
		"DEFINE TABLE doc SCHEMALESS;
		 DEFINE INDEX ix_uniq ON doc FIELDS code UNIQUE;
		 CREATE doc:1 SET code = 'a';
		 CREATE doc:2 SET code = 'b';
		 CREATE doc:3 SET code = 'c';",
	)
	.await?;

	round_trip.assert_all().await?;
	assert_plans_through(
		&round_trip,
		"SELECT VALUE id FROM doc WHERE code = 'b'",
		"index: 'ix_uniq'",
	)
	.await?;
	round_trip.assert_query_equal("SELECT VALUE id FROM doc WHERE code = 'b'").await?;
	assert_target_renders(&round_trip, "SELECT VALUE id FROM doc WHERE code = 'b'", "[doc:2]")
		.await?;

	// The entries have to carry the value a colliding write is matched against,
	// and the record that already holds it.
	assert_target_rejects(
		&round_trip,
		"CREATE doc:9 SET code = 'a'",
		"index `ix_uniq` already contains 'a', with record `doc:1`",
	)
	.await?;
	// A value nobody holds is still accepted, so the index is refusing a
	// collision rather than refusing every write.
	round_trip.target_query("CREATE doc:9 SET code = 'd'").await?;
	Ok(())
}

/// A restored full-text index still matches, and still scores what it scored.
///
/// A relevance score is computed from statistics the index accumulated while it
/// was populated — how often the term occurs in a document, how many documents
/// carry it, how long each document is. Matching alone would be satisfied by an
/// index rebuilt from the terms and nothing else; scoring is what asks whether
/// those statistics came back. A corpus where the two matching documents score
/// differently is what makes the answer informative: one term occurrence in a
/// short document outscores four in a long one, so a score that collapsed to a
/// constant would be visible.
#[tokio::test]
async fn a_restored_fulltext_index_still_matches_and_still_scores() -> Result<()> {
	let mut seed = String::from(
		"DEFINE TABLE doc SCHEMALESS;
		 DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
		 DEFINE INDEX ix_ft ON doc FIELDS body FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;\n",
	);
	// Filler documents the term does not occur in: the document frequency a
	// score depends on is only meaningful against a corpus.
	for i in 1..=10 {
		seed.push_str(&format!("CREATE doc:{i} SET body = 'filler text number {i}';\n"));
	}
	seed.push_str("CREATE doc:11 SET body = 'aardvark';\n");
	seed.push_str("CREATE doc:12 SET body = 'aardvark aardvark aardvark and more aardvark';\n");
	let round_trip = RoundTrip::run(&seed).await?;

	round_trip.assert_all().await?;
	assert_plans_through(
		&round_trip,
		"SELECT VALUE id FROM doc WHERE body @@ 'aardvark'",
		"index: 'ix_ft'",
	)
	.await?;

	let matched = "SELECT VALUE id FROM doc WHERE body @@ 'aardvark' ORDER BY id";
	round_trip.assert_query_equal(matched).await?;
	assert_target_renders(&round_trip, matched, "[doc:11, doc:12]").await?;

	let scored = "SELECT VALUE search::score(1) FROM doc WHERE body @1@ 'aardvark' ORDER BY id";
	round_trip.assert_query_equal(scored).await?;
	// Both scores are positive, and the shorter document scores higher — the
	// ordering is a statement about the statistics rather than about the match.
	let scores = target_value(&round_trip, scored).await?;
	let Value::Array(scores) = &scores else {
		bail!("`{scored}` must answer with an array, got {scores:?}");
	};
	let [Value::Number(Number::Float(short)), Value::Number(Number::Float(long))] =
		scores.as_slice()
	else {
		bail!("`{scored}` must answer with two float scores, got {scores:?}");
	};
	assert!(
		*long > 0.0 && short > long,
		"the restored index scored the two matches {short} and {long}; an index rebuilt with no \
		 statistics behind it scores every match alike"
	);

	// Highlighting reads the term offsets, which are stored separately from the
	// term-to-document mapping the match itself uses.
	let highlighted = "SELECT VALUE search::highlight('<b>', '</b>', 1) FROM doc WHERE body @1@ 'aardvark' ORDER BY id";
	round_trip.assert_query_equal(highlighted).await?;
	assert_target_renders(
		&round_trip,
		highlighted,
		"['<b>aardvark</b>', '<b>aardvark</b> <b>aardvark</b> <b>aardvark</b> and more <b>aardvark</b>']",
	)
	.await
}

/// A restored count index still answers the count from its own tally.
///
/// A count index holds a running total rather than a per-record entry, so it is
/// the one index kind whose loss is arithmetic: an index that came back empty
/// answers zero, and an index that came back holding the tally of a different
/// predicate answers a number that is merely wrong. The predicate is narrower
/// than the table so that a total taken from the wrong place — the table's own
/// record count — is a different number from the right one.
#[tokio::test]
async fn a_restored_count_index_still_answers_the_count() -> Result<()> {
	let round_trip = RoundTrip::run(
		"DEFINE TABLE doc SCHEMALESS;
		 DEFINE INDEX ix_count ON doc COUNT WHERE rank > 1;
		 CREATE doc:1 SET rank = 3;
		 CREATE doc:2 SET rank = 1;
		 CREATE doc:3 SET rank = 2;
		 CREATE doc:4 SET rank = 0;",
	)
	.await?;

	round_trip.assert_all().await?;
	// A count index reports no name in the plan; the operator it compiles to is
	// only reachable when one answers the predicate.
	assert_plans_through(
		&round_trip,
		"SELECT count() FROM doc WHERE rank > 1 GROUP ALL",
		"operator: 'IndexCountScan'",
	)
	.await?;

	let counted = "SELECT count() FROM doc WHERE rank > 1 GROUP ALL";
	round_trip.assert_query_equal(counted).await?;
	// Two of the four records match, so neither zero nor the table's own record
	// count is the right answer.
	assert_target_renders(&round_trip, counted, "[{ count: 2 }]").await?;

	// The tally is maintained from the restored state, not merely restored: a
	// record that enters the predicate has to move it.
	round_trip.target_query("UPDATE doc:2 SET rank = 5 RETURN NONE").await?;
	assert_target_renders(&round_trip, counted, "[{ count: 3 }]").await
}

/// A restored HNSW index still answers a nearest-neighbour query.
///
/// The index is a navigable graph built over the vectors as they were inserted;
/// nothing in the records or the definition describes the graph's links. A
/// restore that recreated the definition and left the graph empty returns no
/// neighbours at all, and one that rebuilt it over a subset returns the wrong
/// ones — both while the vectors themselves compare equal.
#[tokio::test]
async fn a_restored_hnsw_index_still_answers_a_knn_query() -> Result<()> {
	let round_trip = RoundTrip::run(
		"DEFINE TABLE point SCHEMALESS;
		 DEFINE INDEX ix_hnsw ON point FIELDS vec HNSW DIMENSION 3 DIST EUCLIDEAN TYPE F32 EFC 150 M 8;
		 CREATE point:origin SET vec = [0f, 0f, 0f];
		 CREATE point:near SET vec = [1f, 0f, 0f];
		 CREATE point:mid SET vec = [0f, 4f, 0f];
		 CREATE point:far SET vec = [0f, 0f, 9f];",
	)
	.await?;

	round_trip.assert_all().await?;
	assert_plans_through(
		&round_trip,
		"SELECT VALUE id FROM point WHERE vec <|2,40|> [0f, 0f, 0f]",
		"index: 'ix_hnsw'",
	)
	.await?;

	let knn = "SELECT VALUE id FROM point WHERE vec <|2,40|> [0f, 0f, 0f]";
	round_trip.assert_query_equal(knn).await?;
	// The two nearest, nearest first — a graph that came back partly linked
	// reaches some of the points and not the closest ones.
	assert_target_renders(&round_trip, knn, "[point:origin, point:near]").await?;

	// And it is still a graph being searched rather than a frozen answer: a new
	// point closer than the old winner has to displace it.
	round_trip.target_query("CREATE point:closer SET vec = [0f, 0f, 0.5f] RETURN NONE").await?;
	assert_target_renders(&round_trip, knn, "[point:origin, point:closer]").await
}

/// A restored index still serves an `ORDER BY` without a sort.
///
/// Reading an index in key order is the case rows cannot speak to at all: the
/// planner's fallback is to scan the table and sort it, which produces exactly
/// the same rows in exactly the same order. What separates them is the sort
/// itself, so the assertion is that the restored database's plan walks the index
/// and has no sort in it.
#[tokio::test]
async fn a_restored_index_still_serves_an_order_by() -> Result<()> {
	let round_trip = RoundTrip::run(
		"DEFINE TABLE doc SCHEMALESS;
		 DEFINE INDEX ix_rank ON doc FIELDS rank;
		 CREATE doc:1 SET rank = 30;
		 CREATE doc:2 SET rank = 10;
		 CREATE doc:3 SET rank = 20;",
	)
	.await?;

	round_trip.assert_all().await?;
	let ordered = "SELECT VALUE id FROM doc ORDER BY rank";
	assert_plans_through(&round_trip, ordered, "index: 'ix_rank'").await?;

	let plan = target_value(&round_trip, &format!("{ordered} EXPLAIN")).await?.to_sql();
	assert!(
		!plan.contains("SortByKey"),
		"the restored database sorts `{ordered}` rather than reading the index in order, which \
		 returns the same rows either way:\n  {plan}"
	);

	round_trip.assert_query_equal(ordered).await?;
	assert_target_renders(&round_trip, ordered, "[doc:2, doc:3, doc:1]").await
}

// ----------------------------------------------------------------------------
// Graph edges
// ----------------------------------------------------------------------------

/// A restored graph edge is traversable from both of its endpoints.
///
/// An edge record carries `in` and `out`, and restoring the record restores
/// those. What it does not restore is the adjacency a traversal reads: two
/// entries, one filed under each endpoint, which is how `->` and `<-` find the
/// edge without searching the table. A restore that wrote the record and neither
/// entry leaves the edge visible to `SELECT * FROM knows` and invisible to every
/// traversal, so the records compare equal on both sides.
#[tokio::test]
async fn a_restored_graph_edge_traverses_in_both_directions() -> Result<()> {
	let round_trip = RoundTrip::run(
		"DEFINE TABLE person SCHEMALESS;
		 CREATE person:a, person:b, person:c;
		 RELATE person:a -> knows:one -> person:b;
		 RELATE person:b -> knows:two -> person:c;",
	)
	.await?;

	round_trip.assert_all().await?;

	let forward = "SELECT VALUE ->knows->person FROM person ORDER BY id";
	let reverse = "SELECT VALUE <-knows<-person FROM person ORDER BY id";
	round_trip.assert_query_equal(forward).await?;
	round_trip.assert_query_equal(reverse).await?;
	assert_target_renders(&round_trip, forward, "[[person:b], [person:c], []]").await?;
	assert_target_renders(&round_trip, reverse, "[[], [person:a], [person:b]]").await?;

	// Two hops, so the traversal is reading the second endpoint's own adjacency
	// rather than only the one it started from.
	assert_target_renders(
		&round_trip,
		"SELECT VALUE ->knows->person->knows->person FROM person:a",
		"[[person:c]]",
	)
	.await?;
	// And the edge records themselves are reachable by traversal, not only by
	// scanning the edge table.
	assert_target_renders(&round_trip, "SELECT VALUE ->knows FROM person:a", "[[knows:one]]").await
}

/// A restored lightweight relation is traversable from both of its endpoints.
///
/// A lightweight relation stores no edge record at all — the adjacency entries
/// are the edge. So there is nothing for the record comparison to compare, and
/// the export cannot restore one by replaying an `INSERT`; it emits `RELATE`
/// instead, and the statement's own write is what recreates the entries. That
/// makes this the one edge shape whose data is carried by a different kind of
/// statement, and the only surface that reports it is a traversal.
#[tokio::test]
async fn a_restored_lightweight_relation_traverses_in_both_directions() -> Result<()> {
	let round_trip = RoundTrip::run(
		"DEFINE TABLE person SCHEMALESS;
		 DEFINE TABLE follows TYPE RELATION IN person OUT person LIGHTWEIGHT;
		 CREATE person:a, person:b, person:c;
		 RELATE person:a -> follows -> person:b;
		 RELATE person:b -> follows -> person:c;
		 RELATE person:a -> follows -> person:c;",
	)
	.await?;

	// The statement kind is the fixture's premise: a lightweight relation
	// emitted as an `INSERT` would be a different restore being tested.
	let export = round_trip.export_a();
	assert!(
		export.contains("RELATE person:a -> follows -> person:b;"),
		"a lightweight relation's edges have to be exported as `RELATE` statements:\n{export}"
	);

	round_trip.assert_all().await?;

	let forward = "SELECT VALUE ->follows->person FROM person ORDER BY id";
	let reverse = "SELECT VALUE <-follows<-person FROM person ORDER BY id";
	round_trip.assert_query_equal(forward).await?;
	round_trip.assert_query_equal(reverse).await?;
	assert_target_renders(&round_trip, forward, "[[person:b, person:c], [person:c], []]").await?;
	assert_target_renders(&round_trip, reverse, "[[], [person:a], [person:a, person:b]]").await?;

	// The synthesized edge records carry the canonical id derived from the two
	// endpoints, which is what makes replaying a `RELATE` idempotent rather than
	// additive.
	assert_target_renders(
		&round_trip,
		"SELECT VALUE id FROM follows ORDER BY id",
		"[follows:[person:a, person:b], follows:[person:a, person:c], follows:[person:b, person:c]]",
	)
	.await
}
