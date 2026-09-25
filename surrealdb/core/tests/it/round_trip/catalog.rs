//! Phase 2 of the plan: catalog fidelity — the definitions `INFO FOR DB` and
//! `INFO FOR TABLE` report, round-tripped and compared.
//!
//! A definition is the half of a database that no record dump shows. A restore
//! can replay every row correctly and still hand back a table whose `AS SELECT`
//! is gone, a field whose `ASSERT` no longer guards it, or an index that no
//! longer exists — and answer success on every statement while doing it. So the
//! oracle here is the catalog itself: `INFO FOR DB` and every `INFO FOR TABLE`,
//! compared as typed values between the seeded database and the restored one,
//! with the export's own fixed point alongside them.
//!
//! Each fixture below defines one kind the exporter emits and asserts
//! [`RoundTrip::assert_all`]. Two of them assert something else, and say so in
//! their own doc comments: they pin behaviour that is currently a loss, so that
//! changing it is a decision someone makes rather than one that happens.
//!
//! Two habits run through the file:
//!
//! - A fixture asserts that the clause it is about is *present in the surface being compared*, not
//!   only that the two sides agree. Two databases that both lost a clause agree perfectly, so
//!   equality alone is not evidence — [`assert_db_info_carries`] and [`assert_table_info_carries`]
//!   are what make the comparison mean something.
//! - Where a definition has behaviour, the behaviour is asserted too. A view that holds the right
//!   rows and has stopped tracking its source is a table, not a view, and no catalog comparison can
//!   tell the difference.

use anyhow::{Result, bail};
use surrealdb_rpc::export::Config;
use surrealdb_types::{ToSql, Value};

use super::RoundTrip;

/// Fails unless the source's `INFO FOR DB` carries every one of `needles`.
///
/// The round trip's catalog assertion compares two renderings, and two
/// renderings that both lost a clause compare equal. This is what states that
/// the clause under test reached the compared value in the first place.
async fn assert_db_info_carries(round_trip: &RoundTrip, needles: &[&str]) -> Result<()> {
	assert_info_carries(round_trip, "INFO FOR DB", needles).await
}

/// Fails unless the source's `INFO FOR TABLE <table>` carries every one of
/// `needles`. The table counterpart of [`assert_db_info_carries`].
async fn assert_table_info_carries(
	round_trip: &RoundTrip,
	table: &str,
	needles: &[&str],
) -> Result<()> {
	assert_info_carries(round_trip, &format!("INFO FOR TABLE {table}"), needles).await
}

async fn assert_info_carries(round_trip: &RoundTrip, sql: &str, needles: &[&str]) -> Result<()> {
	let rendered = single(round_trip.source_query(sql).await?, sql)?.to_sql();
	for needle in needles {
		assert!(
			rendered.contains(needle),
			"`{sql}` does not carry `{needle}`, so comparing it says nothing about that \
			 clause:\n{rendered}"
		);
	}
	Ok(())
}

/// The rendering of a single-statement query against the restored database.
async fn target_info(round_trip: &RoundTrip, sql: &str) -> Result<String> {
	Ok(single(round_trip.target_query(sql).await?, sql)?.to_sql())
}

fn single(mut values: Vec<Value>, sql: &str) -> Result<Value> {
	if values.len() != 1 {
		bail!("`{sql}` must answer with exactly one statement, got {}", values.len());
	}
	Ok(values.remove(0))
}

/// Every table kind and schema mode the catalog can hold survives a round trip.
///
/// The table definition is the one line every other definition hangs off, and
/// most of what it carries is a flag rather than a clause: a relation's
/// endpoints and whether they are enforced, whether writes are dropped, whether
/// the schema is closed. A flag that fails to restore leaves a table that still
/// exists and still holds its records, so nothing downstream announces it.
#[tokio::test]
async fn table_kinds_and_schema_modes_survive() -> Result<()> {
	let round_trip = RoundTrip::run(
		"DEFINE TABLE t_any TYPE ANY SCHEMALESS COMMENT 'anything goes';
		 DEFINE TABLE t_normal TYPE NORMAL SCHEMAFULL DROP;
		 DEFINE TABLE t_rel_loose TYPE RELATION SCHEMALESS;
		 DEFINE TABLE t_rel_enforced TYPE RELATION IN t_any OUT t_any ENFORCED SCHEMAFULL;
		 CREATE t_any:1 SET a = 1;
		 RELATE t_any:1->t_rel_enforced->t_any:1;",
	)
	.await?;

	assert_db_info_carries(
		&round_trip,
		&[
			"DEFINE TABLE t_any TYPE ANY SCHEMALESS COMMENT 'anything goes'",
			"DEFINE TABLE t_normal TYPE NORMAL DROP SCHEMAFULL",
			"DEFINE TABLE t_rel_loose TYPE RELATION SCHEMALESS",
			"DEFINE TABLE t_rel_enforced TYPE RELATION IN t_any OUT t_any ENFORCED SCHEMAFULL",
		],
	)
	.await?;
	round_trip.assert_all().await
}

/// Table permissions survive for all four verbs, including the forms the
/// renderer treats specially.
///
/// Permissions are a decision about who may read and write, so restoring them
/// wrongly is not a cosmetic loss in either direction: a dropped `PERMISSIONS
/// NONE` opens a table, and a dropped grant closes one. The renderer also has
/// form here — verbs sharing an expression are folded into one `FOR a, b`
/// clause, `FULL` and `NONE` render as words rather than expressions — so the
/// text that has to survive is not simply the text that was typed.
#[tokio::test]
async fn table_permissions_survive_every_verb() -> Result<()> {
	let round_trip = RoundTrip::run(
		"DEFINE TABLE doc SCHEMALESS PERMISSIONS
			FOR select WHERE published = true
			FOR create, update WHERE $auth.id != NONE
			FOR delete FULL;
		 DEFINE TABLE locked SCHEMALESS PERMISSIONS NONE;
		 DEFINE TABLE open SCHEMALESS PERMISSIONS FULL;",
	)
	.await?;

	assert_db_info_carries(
		&round_trip,
		&[
			// The four verbs, the folded pair, and both word-shaped grants.
			"PERMISSIONS FOR select WHERE published = true, FOR create, update WHERE $auth.id != \
			 NONE, FOR delete FULL",
			"DEFINE TABLE locked TYPE ANY SCHEMALESS PERMISSIONS NONE",
			"DEFINE TABLE open TYPE ANY SCHEMALESS PERMISSIONS FULL",
		],
	)
	.await?;
	round_trip.assert_all().await
}

/// A view whose name sorts before the table it selects from restores as a view
/// that is still tracking that table.
///
/// A view's definition is the only thing that makes it one. Replaying it
/// recomputes the rows, so a view has to be defined after every table it reads —
/// and when it is not, the failure is quiet in the worst way: the rows are
/// right, because something wrote them, and only the next write to the source
/// reveals that nothing recomputes them any more. Neither the record comparison
/// nor the catalog comparison can see that on its own, so this fixture writes to
/// the source on both sides afterwards and compares what the view then reports.
#[tokio::test]
async fn a_view_sorting_before_its_source_stays_live() -> Result<()> {
	assert_view_survives("aa_view").await
}

/// A view whose name sorts after the table it selects from restores as a view
/// that is still tracking that table.
///
/// The mirror of the case above, and not the same case: with the source table
/// already restored, replaying the view's definition recomputes its rows from
/// records that are already there, so anything the dump also emits for the view
/// itself lands on top of rows that exist.
#[tokio::test]
async fn a_view_sorting_after_its_source_stays_live() -> Result<()> {
	assert_view_survives("zz_view").await
}

/// Round-trips a database holding `view` over `person`, then writes a new
/// `person` to both databases and asserts the view answers the same on each.
async fn assert_view_survives(view: &str) -> Result<()> {
	let round_trip = RoundTrip::run(&format!(
		"DEFINE TABLE person SCHEMALESS;
		 CREATE person:1 SET age = 30;
		 CREATE person:2 SET age = 40;
		 DEFINE TABLE {view} AS SELECT count() AS c, math::sum(age) AS total FROM person GROUP ALL;"
	))
	.await?;

	let clause = "AS SELECT count() AS c, math::sum(age) AS total FROM person GROUP ALL";
	assert_db_info_carries(&round_trip, &[clause]).await?;
	round_trip.assert_all().await?;

	// Named on the restored side directly, so a view that came back as a plain
	// table holding the same rows is reported as the missing clause it is,
	// rather than as a difference between two renderings.
	let restored = target_info(&round_trip, "INFO FOR DB").await?;
	assert!(
		restored.contains(clause),
		"the restored `{view}` carries no view clause, so it is a table holding a frozen \
		 snapshot:\n{restored}"
	);

	// And the definition is doing its job. A view that has stopped recomputing
	// diverges from its source database only once the source table changes.
	round_trip.source_query("CREATE person:3 SET age = 50").await?;
	round_trip.target_query("CREATE person:3 SET age = 50").await?;
	round_trip.assert_query_equal(&format!("SELECT c, total FROM {view}")).await?;
	let after = round_trip.target_query(&format!("SELECT VALUE c FROM {view}")).await?;
	assert_eq!(
		after[0].to_sql(),
		"[3]",
		"the restored `{view}` did not count the record just written to its source table"
	);
	Ok(())
}

/// Every clause a field definition can carry survives a round trip.
///
/// A field definition is mostly a set of rules the database applies to future
/// writes — a type, an assertion, a default, a permission — and none of them
/// leaves a trace in the records that already exist. A restore that dropped one
/// would hand back a database holding exactly the right data and accepting
/// writes the original refused.
#[tokio::test]
async fn field_clauses_survive() -> Result<()> {
	let round_trip = RoundTrip::run(
		"DEFINE TABLE person SCHEMAFULL;
		 DEFINE FIELD name ON person TYPE string;
		 DEFINE FIELD address ON person TYPE object;
		 DEFINE FIELD address.city ON person TYPE string DEFAULT 'nowhere';
		 DEFINE FIELD tags ON person TYPE array;
		 DEFINE FIELD tags.* ON person TYPE string ASSERT $value != '';
		 DEFINE FIELD meta ON person TYPE object FLEXIBLE;
		 DEFINE FIELD serial ON person TYPE option<int> READONLY;
		 DEFINE FIELD shout ON person TYPE string VALUE string::uppercase(name) DEFAULT '';
		 DEFINE FIELD revision ON person TYPE int DEFAULT ALWAYS 0;
		 DEFINE FIELD score ON person TYPE int ASSERT $value >= 0 DEFAULT 0;
		 DEFINE FIELD doubled ON person COMPUTED score * 2;
		 DEFINE FIELD secret ON person TYPE option<string>
			PERMISSIONS FOR select WHERE $auth.admin = true FOR update NONE;
		 DEFINE TABLE comment SCHEMAFULL;
		 DEFINE FIELD author ON comment TYPE record<person> REFERENCE ON DELETE CASCADE;
		 CREATE person:1 SET name = 'ada', address = { city: 'london' }, tags = ['x'],
			meta = { free: 'form' }, serial = 1, score = 7;
		 CREATE comment:1 SET author = person:1;",
	)
	.await?;

	assert_table_info_carries(
		&round_trip,
		"person",
		&[
			// A nested path and an array element path are separate definitions,
			// addressed by a path rather than a name.
			"DEFINE FIELD address.city ON person TYPE string DEFAULT 'nowhere'",
			"DEFINE FIELD tags.* ON person TYPE string ASSERT $value != ''",
			"DEFINE FIELD meta ON person TYPE object FLEXIBLE",
			"DEFINE FIELD serial ON person TYPE none | int READONLY",
			"DEFINE FIELD shout ON person TYPE string DEFAULT '' VALUE string::uppercase(name)",
			"DEFINE FIELD revision ON person TYPE int DEFAULT ALWAYS 0",
			"DEFINE FIELD score ON person TYPE int DEFAULT 0 ASSERT $value >= 0",
			"DEFINE FIELD doubled ON person COMPUTED score * 2",
			"DEFINE FIELD secret ON person TYPE none | string PERMISSIONS FOR select WHERE \
			 $auth.admin = true, FOR create FULL, FOR update NONE",
		],
	)
	.await?;
	assert_table_info_carries(
		&round_trip,
		"comment",
		&["DEFINE FIELD author ON comment TYPE record<person> REFERENCE ON DELETE CASCADE"],
	)
	.await?;
	round_trip.assert_all().await
}

/// A relation table's fields survive, both the pair the table definition
/// generates and the ones the schema declares.
///
/// `in` and `out` are not written by the user: defining the table creates them,
/// and replaying that definition creates them again. So a dump that also carries
/// them is replaying a definition over one that already exists, which is a
/// different operation from the one every other field line performs — and a
/// relation's endpoints are the table's whole meaning, so a wrong answer here is
/// a broken graph rather than a missing clause.
#[tokio::test]
async fn relation_table_fields_survive() -> Result<()> {
	let round_trip = RoundTrip::run(
		"DEFINE TABLE person SCHEMALESS;
		 DEFINE TABLE likes TYPE RELATION IN person OUT person ENFORCED SCHEMAFULL;
		 DEFINE FIELD note ON likes TYPE option<string>;
		 DEFINE TABLE knows TYPE RELATION SCHEMALESS;
		 CREATE person:1;
		 CREATE person:2;
		 RELATE person:1->likes->person:2 SET note = 'hello';
		 RELATE person:1->knows->person:2;",
	)
	.await?;

	assert_table_info_carries(
		&round_trip,
		"likes",
		&[
			"DEFINE FIELD in ON likes TYPE record<person>",
			"DEFINE FIELD out ON likes TYPE record<person>",
			"DEFINE FIELD note ON likes TYPE none | string",
		],
	)
	.await?;
	// An unconstrained relation's endpoints are generated too, and are a
	// different type from the constrained pair above.
	assert_table_info_carries(
		&round_trip,
		"knows",
		&["DEFINE FIELD in ON knows TYPE record", "DEFINE FIELD out ON knows TYPE record"],
	)
	.await?;
	round_trip.assert_all().await
}

/// Every index kind survives a round trip, with the parameters that make it the
/// index it is.
///
/// An index is invisible to a query's result and visible only in its cost, so a
/// restore that dropped one answers every query correctly by scanning — except
/// for `UNIQUE`, where the lost index is a lost constraint, and `COUNT ...
/// WHERE`, where the lost predicate is a different number. The parameters
/// matter as much as the kind: an HNSW index restored with different
/// connectivity, or a full-text index restored without its analyzer, is not the
/// index that was exported.
#[tokio::test]
async fn index_kinds_survive() -> Result<()> {
	let round_trip = RoundTrip::run(
		"DEFINE TABLE item SCHEMAFULL;
		 DEFINE FIELD name ON item TYPE string;
		 DEFINE FIELD sku ON item TYPE string;
		 DEFINE FIELD status ON item TYPE string;
		 DEFINE FIELD embedding ON item TYPE array<float>;
		 DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
		 DEFINE INDEX ix_plain ON item FIELDS name;
		 DEFINE INDEX ix_unique ON item FIELDS sku UNIQUE;
		 DEFINE INDEX ix_multi ON item FIELDS status, name;
		 DEFINE INDEX ix_count ON item COUNT;
		 DEFINE INDEX ix_count_active ON item COUNT WHERE status = 'active';
		 DEFINE INDEX ix_search ON item FIELDS name FULLTEXT ANALYZER simple BM25(1.2,0.75) HIGHLIGHTS;
		 DEFINE INDEX ix_vector ON item FIELDS embedding HNSW DIMENSION 4 DIST EUCLIDEAN TYPE F32 EFC 150 M 8;
		 CREATE item:1 SET name = 'a lamp', sku = 'L-1', status = 'active', embedding = [1.0, 2.0, 3.0, 4.0];
		 CREATE item:2 SET name = 'a desk', sku = 'D-1', status = 'retired', embedding = [4.0, 3.0, 2.0, 1.0];",
	)
	.await?;

	assert_table_info_carries(
		&round_trip,
		"item",
		&[
			"DEFINE INDEX ix_plain ON item FIELDS name",
			"DEFINE INDEX ix_unique ON item FIELDS sku UNIQUE",
			"DEFINE INDEX ix_multi ON item FIELDS status, name",
			"DEFINE INDEX ix_count ON item COUNT",
			"DEFINE INDEX ix_count_active ON item COUNT WHERE status = 'active'",
			"DEFINE INDEX ix_search ON item FIELDS name FULLTEXT ANALYZER simple BM25(1.2,0.75) \
			 HIGHLIGHTS",
			// The tuning an HNSW definition keeps, including the parameters
			// derived at definition time rather than typed.
			"DEFINE INDEX ix_vector ON item FIELDS embedding HNSW DIMENSION 4 DIST EUCLIDEAN TYPE \
			 F32 EFC 150 M 8 M0 16 LM ",
		],
	)
	.await?;
	round_trip.assert_all().await
}

/// An analyzer survives with its tokenizers, its filter chain in order, and the
/// arguments its filters were configured with.
///
/// An analyzer is what a full-text index means by a word. Restoring one with a
/// shortened filter chain leaves every dependent index intact and quietly
/// matching different queries, and the chain is ordered, so the set alone is not
/// the definition.
#[tokio::test]
async fn analyzer_definitions_survive() -> Result<()> {
	let round_trip = RoundTrip::run(
		"DEFINE ANALYZER english TOKENIZERS blank,class,camel
			FILTERS lowercase,ascii,snowball(english),edgengram(1,10)
			COMMENT 'the full chain';
		 DEFINE ANALYZER bare TOKENIZERS blank;",
	)
	.await?;

	assert_db_info_carries(
		&round_trip,
		&[
			"DEFINE ANALYZER english TOKENIZERS BLANK,CLASS,CAMEL FILTERS LOWERCASE, ASCII, \
			 SNOWBALL(ENGLISH), EDGENGRAM(1,10) COMMENT 'the full chain'",
			"DEFINE ANALYZER bare TOKENIZERS BLANK",
		],
	)
	.await?;
	round_trip.assert_all().await
}

/// A table event survives with its condition and its body.
///
/// An event fires on writes that happen after the restore, so nothing already in
/// the database reflects whether it survived. A dump is also replayed under
/// `OPTION IMPORT`, which suppresses events — so the restore both must not run
/// them and must leave them defined.
#[tokio::test]
async fn event_definitions_survive() -> Result<()> {
	let round_trip = RoundTrip::run(
		"DEFINE TABLE person SCHEMALESS;
		 DEFINE TABLE audit SCHEMALESS;
		 DEFINE EVENT audited ON TABLE person WHEN $event = 'CREATE'
			THEN CREATE audit SET who = $after.id COMMENT 'writes an audit row';
		 CREATE person:1 SET name = 'ada';",
	)
	.await?;

	assert_table_info_carries(
		&round_trip,
		"person",
		&[
			"DEFINE EVENT audited ON person WHEN $event = 'CREATE' THEN (CREATE audit SET who = $after.id)",
		],
	)
	.await?;
	round_trip.assert_all().await
}

/// A parameter and a function survive, including a function whose body reads a
/// parameter the same dump defines.
///
/// A function body is stored text that is re-rendered on export and re-parsed on
/// import, and a body that reads `$limit` is only correct if the parameter is
/// restored too. Neither appears in any record, so a lost one is discovered by
/// the first query that calls it.
#[tokio::test]
async fn params_and_functions_survive() -> Result<()> {
	let round_trip = RoundTrip::run(
		"DEFINE PARAM $limit VALUE 10 COMMENT 'how many at most';
		 DEFINE FUNCTION fn::capped($n: int) -> int { RETURN math::min([$n, $limit]); }
			COMMENT 'reads a param';",
	)
	.await?;

	assert_db_info_carries(
		&round_trip,
		&[
			"DEFINE PARAM $limit VALUE 10 COMMENT 'how many at most'",
			"DEFINE FUNCTION fn::capped($n: int) -> int { RETURN math::min([$n, $limit]) } COMMENT \
			 'reads a param'",
		],
	)
	.await?;
	round_trip.assert_all().await?;

	// The restored pair still works together: the body resolves the parameter
	// rather than carrying the value it had when the dump was taken.
	round_trip.assert_query_equal("RETURN fn::capped(99)").await
}

/// A database user survives with the credential material that lets them sign in,
/// their roles, and their token and session durations.
///
/// A user's password never leaves the database in plaintext, so what a dump
/// carries is the stored hash — and `INFO` redacts exactly that, which is the
/// point of also comparing the export text: the hash has to survive and the two
/// surfaces see different halves of whether it did.
#[tokio::test]
async fn a_database_user_survives() -> Result<()> {
	let round_trip = RoundTrip::run(
		"DEFINE USER carol ON DATABASE PASSWORD 'a-password' ROLES EDITOR
			DURATION FOR TOKEN 1h, FOR SESSION 2h;",
	)
	.await?;

	assert_db_info_carries(
		&round_trip,
		&["DEFINE USER carol ON DATABASE", "ROLES EDITOR DURATION FOR TOKEN 1h, FOR SESSION 2h"],
	)
	.await?;
	// `INFO` redacts the hash on both sides, so it is the export that has to
	// carry it. A restore that dropped it would leave a user nobody can be.
	assert!(
		round_trip.export_a().contains("PASSHASH '$argon2id$"),
		"the export carries no password hash for the user it defines:\n{}",
		round_trip.export_a()
	);
	round_trip.assert_all().await
}

/// A sequence definition survives with its batch size, its start, and its
/// timeout.
///
/// The definition is all a dump carries: where the sequence has got to lives
/// outside it. This fixture pins the definition half only, and deliberately does
/// not draw from the sequence — what a restored sequence issues next is derived
/// state, and belongs to the phase that compares that.
///
/// The timeout is carried by the export text rather than by the catalog
/// comparison: `INFO FOR DB` renders a sequence without it, so that clause
/// survives or is lost in the dump alone.
#[tokio::test]
async fn a_sequence_definition_survives() -> Result<()> {
	let round_trip =
		RoundTrip::run("DEFINE SEQUENCE order_id BATCH 100 START 10 TIMEOUT 5s;").await?;

	assert_db_info_carries(&round_trip, &["DEFINE SEQUENCE order_id BATCH 100 START 10"]).await?;
	assert!(
		round_trip.export_a().contains("DEFINE SEQUENCE order_id BATCH 100 START 10 TIMEOUT 5s"),
		"the export carries no timeout for the sequence it defines, so nothing here compares \
		 it:\n{}",
		round_trip.export_a()
	);
	round_trip.assert_all().await
}

/// Database-level clauses survive, which they can only do if the dump carries a
/// `DEFINE DATABASE` line of its own.
///
/// These clauses belong to the database rather than to anything inside it, so
/// they appear in neither `INFO FOR DB` nor any `INFO FOR TABLE` — the catalog
/// comparison this phase is built on cannot see them at all. `INFO FOR NS` is
/// where they are visible, and the two databases carry the same name, so the two
/// renderings are comparable directly.
#[tokio::test]
async fn database_level_clauses_survive() -> Result<()> {
	let round_trip = RoundTrip::run(
		"DEFINE DATABASE OVERWRITE test STRICT COMMENT 'the source' CHANGEFEED 1d;
		 DEFINE TABLE person SCHEMALESS;
		 CREATE person:1 SET name = 'ada';",
	)
	.await?;

	let rendered = single(round_trip.source_query("INFO FOR NS").await?, "INFO FOR NS")?.to_sql();
	assert!(
		rendered.contains("DEFINE DATABASE test STRICT COMMENT 'the source' CHANGEFEED 1d"),
		"`INFO FOR NS` does not carry the database's own clauses, so comparing it says nothing \
		 about them:\n{rendered}"
	);
	round_trip.assert_query_equal("INFO FOR NS").await?;
	round_trip.assert_all().await
}

/// An access is not restored at all: the dump carries the redaction in place of
/// its key, and replaying that is refused.
///
/// An export replaces every signing and verification key with the literal
/// `[REDACTED]`, and the dump it produces is executable — so a statement
/// defining an access from it would install that published string as the
/// signing secret, and the restored database would verify tokens anyone can
/// forge. `DEFINE ACCESS` refuses a key equal to the placeholder for that
/// reason, which turns a silent authentication bypass into a per-statement
/// failure the import reports and an operator can act on.
///
/// What the round trip therefore loses is the access itself. That is the
/// intended outcome of the two available ones: a dump that cannot carry key
/// material cannot restore an access, and saying so is better than appearing to.
#[tokio::test]
async fn a_restored_access_is_refused_rather_than_signed_with_the_redaction() -> Result<()> {
	let (round_trip, results) = RoundTrip::run_allowing_import_errors(
		"DEFINE ACCESS token_access ON DATABASE TYPE JWT
			ALGORITHM HS512 KEY 'the-source-signing-key' DURATION FOR TOKEN 15m;
		 DEFINE ACCESS record_access ON DATABASE TYPE RECORD
			SIGNUP (CREATE person)
			SIGNIN (SELECT * FROM person)
			WITH JWT ALGORITHM HS256 KEY 'another-signing-key'
			DURATION FOR SESSION 1d;
		 DEFINE TABLE person SCHEMALESS;",
	)
	.await?;

	// What the dump carries in place of each key.
	let export = round_trip.export_a();
	assert!(
		!export.contains("the-source-signing-key") && !export.contains("another-signing-key"),
		"the export carries a signing key verbatim:\n{export}"
	);
	assert_eq!(
		export.matches("KEY '[REDACTED]'").count(),
		4,
		"each access renders a redacted verification key and a redacted issuer key:\n{export}"
	);

	// And what the restore does with it: refuses, naming the access.
	let failures: Vec<String> =
		results.iter().filter_map(|r| r.result.as_ref().err()).map(ToString::to_string).collect();
	assert_eq!(
		failures.len(),
		2,
		"both accesses must be refused rather than defined with the placeholder: {failures:?}"
	);
	for failure in &failures {
		assert!(
			failure.contains("redaction placeholder"),
			"the refusal must say why the key is not a key: {failure:?}"
		);
	}

	// The restored database holds neither access, and holds no access signed
	// with the published literal.
	let restored = target_info(&round_trip, "INFO FOR DB").await?;
	assert!(
		!restored.contains("KEY '[REDACTED]'"),
		"an access was restored with the redaction literal as its key:\n{restored}"
	);
	Ok(())
}

/// A database config survives the round trip, for both kinds the exporter emits.
///
/// A config definition renders as its body alone — `GRAPHQL ...`, `API ...` —
/// which is the form `INFO FOR DB` reports and is not a statement. The export
/// carries the `DEFINE CONFIG` that recreates it instead. The blast radius is
/// what makes this worth a fixture of its own: the config section precedes every
/// table, and a body the parser rejects ends the whole stream, so a dump that
/// loses its config head restores nothing at all rather than losing the config.
#[tokio::test]
async fn database_configs_survive() -> Result<()> {
	for config in ["GRAPHQL TABLES AUTO FUNCTIONS AUTO", "API PERMISSIONS FULL"] {
		let round_trip = RoundTrip::run(&format!(
			"DEFINE TABLE kept SCHEMALESS;
			 CREATE kept:1 SET a = 1;
			 DEFINE CONFIG {config};"
		))
		.await?;

		assert!(
			round_trip.export_a().contains(&format!("DEFINE CONFIG OVERWRITE {config};")),
			"the config section does not emit a `DEFINE CONFIG` statement for `{config}`:\n{}",
			round_trip.export_a()
		);

		// The table defined before the config has to arrive, which is what says the
		// config line was executed rather than merely surviving the parser.
		round_trip.assert_query_equal("SELECT VALUE id FROM kept").await?;
		round_trip.assert_all().await?;
	}
	Ok(())
}

/// A user's comment survives the round trip, and its contents stay inside the
/// statement that carries them.
///
/// A comment is free text, so the dump renders it as a string literal: it has to
/// come back verbatim, and the quoting has to hold whatever the text contains.
/// The fixture spends both halves on one comment — punctuation that closes a
/// string or an identifier, followed by a statement that would define a user
/// with every role. A restore that let that text out of its quoting would run
/// it, so the comment must come back whole and `intruder` must not exist.
#[tokio::test]
async fn a_user_comment_survives_with_its_contents_quoted() -> Result<()> {
	let injection =
		r#"a ' and a ` ; DEFINE USER intruder ON DATABASE PASSWORD 'x' ROLES OWNER; -- "#;
	let round_trip = RoundTrip::run(&format!(
		"DEFINE USER carol ON DATABASE PASSWORD 'a-password' ROLES VIEWER COMMENT \"{injection}\";"
	))
	.await?;

	assert_db_info_carries(&round_trip, &["COMMENT"]).await?;
	let info = single(round_trip.target_query("INFO FOR DB").await?, "INFO FOR DB")?;
	let rendered = info.to_sql();
	assert!(rendered.contains("COMMENT"), "the restored user carries no comment:\n{rendered}");

	// The comment reaches the dump as a quoted string, so what it carries stays
	// inside the statement rather than being executed alongside it. The text is
	// asserted against the user set rather than against the rendering, which
	// contains the comment — and so contains the name — either way.
	let Value::Object(users) = info.get("users") else {
		bail!("`INFO FOR DB` did not report a users object:\n{rendered}");
	};
	let names: Vec<&str> = users.keys().map(String::as_str).collect();
	assert_eq!(
		names,
		["carol"],
		"a comment's contents were executed as statements of their own:\n{rendered}"
	);
	round_trip.assert_all().await
}

/// Sections the exporter emits, each with the fixture that round-trips it.
///
/// This is the list the guard below holds the exporter to. A section named here
/// without a fixture is a gap that is at least visible; a section the exporter
/// emits that is not named here fails the guard.
const EXPORTED_SECTIONS: &[(&str, &str)] = &[
	("OPTION", "every fixture: the import mode line that precedes the dump"),
	("DATABASE", "database_level_clauses_survive"),
	("USERS", "a_database_user_survives"),
	("ACCESSES", "a_restored_access_carries_the_redaction_rather_than_the_key"),
	("PARAMS", "params_and_functions_survive"),
	("FUNCTIONS", "params_and_functions_survive"),
	("ANALYZERS", "analyzer_definitions_survive"),
	("CONFIGS", "database_configs_survive"),
	("SEQUENCES", "a_sequence_definition_survives"),
	("TABLE", "table_kinds_and_schema_modes_survive, and every fixture defining a table"),
	("TABLE DATA", "table_kinds_and_schema_modes_survive, and the phase comparing records"),
	// Sections whose definitions this file does not build: an API and a module
	// need a body to serve, a bucket needs a storage backend to point at.
	("APIS", "not covered here"),
	("BUCKETS", "not covered here"),
	("MODULES", "not covered here"),
];

/// Export-config fields whose section is not their own name in capitals.
///
/// Each table is emitted under a heading naming that table, so the field that
/// selects them all matches a section heading in the singular.
const RENAMED_CONFIG_FIELDS: &[(&str, &str)] =
	&[("tables", "TABLE"), ("database_definition", "DATABASE")];

/// Export-config fields that select no section of their own, with what they do
/// instead.
const NON_SECTION_CONFIG_FIELDS: &[(&str, &str)] = &[
	("records", "selects the TABLE DATA section of each table rather than a section of its own"),
	("versions", "selects no output: a versioned export is refused before anything is written"),
];

/// Every section the exporter emits has a fixture in this file, or is named as
/// not having one.
///
/// A round-trip suite is only as complete as the list of things it round-trips,
/// and that list rots silently: a new `DEFINE` kind ships with a new export
/// section, the dumps grow a statement nobody replays here, and every test in
/// this file still passes. So the guard is derived from the exporter rather than
/// written out — from the fields of the export config that select what is
/// emitted, and from the section headings of a dump that carries one of
/// everything this file knows how to define.
///
/// It fails when the exporter grows a section, when a config field appears or is
/// renamed, and when a section this file claims to cover stops being emitted.
/// The fix in each case is a line in [`EXPORTED_SECTIONS`] and a fixture to go
/// with it.
#[tokio::test]
async fn every_exported_section_has_a_round_trip_fixture() -> Result<()> {
	// One of everything this file defines, so the dump carries every section it
	// is able to produce. The config makes the import fail, which is the subject
	// of its own fixture above and does not bear on the export text read here.
	let (round_trip, _) = RoundTrip::run_allowing_import_errors(
		"DEFINE DATABASE OVERWRITE test CHANGEFEED 1d;
		 DEFINE USER carol ON DATABASE PASSWORD 'a-password' ROLES VIEWER;
		 DEFINE ACCESS token_access ON DATABASE TYPE JWT ALGORITHM HS512 KEY 'a-key';
		 DEFINE PARAM $limit VALUE 10;
		 DEFINE FUNCTION fn::noop() { RETURN 1; };
		 DEFINE ANALYZER simple TOKENIZERS blank;
		 DEFINE SEQUENCE order_id BATCH 100 START 0;
		 DEFINE CONFIG GRAPHQL TABLES AUTO FUNCTIONS AUTO;
		 DEFINE TABLE person SCHEMALESS;
		 CREATE person:1 SET name = 'ada';",
	)
	.await?;

	let declared: Vec<&str> = EXPORTED_SECTIONS.iter().map(|(section, _)| *section).collect();

	// Every section the exporter put in this dump is one the file accounts for.
	let emitted = section_headings(round_trip.export_a());
	for section in &emitted {
		assert!(
			declared.contains(&section.as_str()),
			"the export emits a `{section}` section that no fixture in this file round-trips. \
			 Add one, and a line to EXPORTED_SECTIONS. Sections in this dump: {emitted:?}"
		);
	}

	// And every section the file claims a fixture for is still emitted for a
	// database that defines one, so a section that stops being written is not
	// mistaken for one that is covered.
	for (section, fixture) in EXPORTED_SECTIONS {
		if *fixture == "not covered here" {
			continue;
		}
		assert!(
			emitted.contains(&(*section).to_owned()),
			"the fixture `{fixture}` claims to cover the `{section}` section, which this dump \
			 does not carry. Sections in this dump: {emitted:?}"
		);
	}

	// The other half of the guard: what the export config selects. A new
	// section arrives with a field that switches it on, which is visible here
	// even for a section this fixture cannot make the exporter write.
	for field in export_config_fields() {
		if NON_SECTION_CONFIG_FIELDS.iter().any(|(name, _)| *name == field) {
			continue;
		}
		let section = match RENAMED_CONFIG_FIELDS.iter().find(|(name, _)| *name == field) {
			Some((_, section)) => (*section).to_owned(),
			None => field.to_uppercase(),
		};
		assert!(
			declared.contains(&section.as_str()),
			"the export config selects `{field}`, which matches no section this file accounts \
			 for. Add a fixture and a line to EXPORTED_SECTIONS, or a line to \
			 NON_SECTION_CONFIG_FIELDS saying what the field does instead. Declared: {declared:?}"
		);
	}
	Ok(())
}

/// The section headings of an export, in the order the dump carries them, with
/// each table's name dropped so that `TABLE: person` counts as `TABLE`.
///
/// A heading is a comment line between two rules of dashes, which is what makes
/// it distinguishable from the dashes themselves.
fn section_headings(export: &str) -> Vec<String> {
	let mut headings = Vec::new();
	for line in export.lines() {
		let Some(text) = line.strip_prefix("-- ") else {
			continue;
		};
		if text.chars().all(|c| c == '-') {
			continue;
		}
		let heading = match text.split_once(':') {
			Some((kind, _)) => kind.trim(),
			None => text.trim(),
		};
		if !headings.iter().any(|h: &String| h == heading) {
			headings.push(heading.to_owned());
		}
	}
	headings
}

/// The field names of the export config, read from its derived `Debug`.
///
/// The config is the exporter's own statement of what a dump can contain, so
/// reading its fields is how the guard notices a section that this file has
/// never heard of. Reading them from `Debug` rather than listing them is the
/// whole point; a shape it cannot parse fails rather than returning fewer
/// fields, since a guard that quietly reads none passes everything.
fn export_config_fields() -> Vec<String> {
	let rendered = format!("{:?}", Config::default());
	let (_, rest) = rendered
		.split_once('{')
		.unwrap_or_else(|| panic!("the export config renders as a struct: {rendered}"));
	let (body, _) = rest
		.rsplit_once('}')
		.unwrap_or_else(|| panic!("the export config renders as a struct: {rendered}"));
	body.split(',')
		.map(str::trim)
		.filter(|field| !field.is_empty())
		.map(|field| {
			let (name, _) = field.split_once(':').unwrap_or_else(|| {
				panic!("the export config's `{field}` does not render as `name: value`: {rendered}")
			});
			assert!(
				name.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
				"`{name}` is not a field name, so the config no longer renders one field per \
				 comma: {rendered}"
			);
			name.to_owned()
		})
		.collect()
}
