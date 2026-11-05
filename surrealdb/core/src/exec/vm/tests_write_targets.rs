//! An `UPDATE` or `DELETE` whose targets mix a table, a record id or a mock
//! with a subquery, a parameter or a value writes each target as the statement
//! naming it alone would: under the same table gates, for every principal, and
//! on the record as stored rather than on the value that named it.
//!
//! Each statement runs on a fresh datastore as one principal; its answer and
//! the records of every table after it are compared with those of reference
//! statements that each name one target of the list.

use surrealdb_types::ToSql;

use crate::dbs::Session;
use crate::exec::operators::test_util::TestDb;
use crate::iam::{Level, Role};
use crate::val::Value;

/// Tables under each kind of table permission: `FULL`, `NONE`, a predicate
/// per clause, one reading `COMPUTED` fields, one reading the record id, one
/// holding statements, and one reading `$auth`.
const GATED_SCHEMA: &str = "
	DEFINE ACCESS user ON DATABASE TYPE RECORD;
	DEFINE TABLE open PERMISSIONS FULL;
	DEFINE TABLE shut PERMISSIONS NONE;
	DEFINE TABLE cond PERMISSIONS
		FOR select WHERE visible = true
		FOR create WHERE n < 10
		FOR update WHERE n < 10 AND $before.n != 5
		FOR delete WHERE locked != true;
	DEFINE TABLE comp PERMISSIONS FOR select, create, update, delete WHERE blocked != true;
	DEFINE FIELD blocked ON comp COMPUTED n > 5;
	DEFINE FIELD twice ON comp COMPUTED blocked OR n * 2 > 12;
	DEFINE TABLE selfref PERMISSIONS
		FOR select FULL
		FOR create, update, delete WHERE id.n != 3;
	DEFINE FIELD mirror ON selfref COMPUTED id.n;
	DEFINE TABLE stmt PERMISSIONS
		FOR select WHERE (SELECT VALUE n FROM open:1)[0] = 1
		FOR create, update, delete WHERE n IN (SELECT VALUE n FROM open);
	DEFINE TABLE owned PERMISSIONS
		FOR select, create, update, delete WHERE owner = $auth.id;
	CREATE open:1 SET n = 1;
	CREATE shut:1 SET n = 1;
	CREATE cond:1 SET n = 1, visible = true, locked = false;
	CREATE cond:2 SET n = 5, visible = false, locked = true;
	CREATE comp:1 SET n = 1;
	CREATE comp:2 SET n = 7;
	CREATE selfref:1 SET n = 1;
	CREATE selfref:3 SET n = 3;
	CREATE stmt:1 SET n = 1;
	CREATE stmt:2 SET n = 2;
	CREATE owned:1 SET owner = user:reader;
	CREATE owned:2 SET owner = user:other;
";

/// The tables [`GATED_SCHEMA`] defines, read back after each statement.
const GATED_TABLES: &[&str] = &["open", "shut", "cond", "comp", "selfref", "stmt", "owned"];

/// A table whose records carry fields a record user's view hides or derives:
/// a field guard, a computed field, and a computed field reading a guarded one.
const VIEWED_SCHEMA: &str = "
	DEFINE ACCESS user ON DATABASE TYPE RECORD;
	DEFINE TABLE doc PERMISSIONS FULL;
	DEFINE FIELD secret ON doc PERMISSIONS FOR select WHERE $auth.id = user:x;
	DEFINE FIELD veiled ON doc PERMISSIONS FOR select WHERE open > 1;
	DEFINE FIELD twice ON doc COMPUTED n * 2;
	DEFINE FIELD echoed ON doc COMPUTED secret PERMISSIONS FOR select WHERE n > 1;
	CREATE doc:1 SET n = 1, secret = 's1', veiled = 'v1', open = 1;
	CREATE doc:2 SET n = 2, secret = 's2', veiled = 'v2', open = 2;
";

fn record_user() -> Session {
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	Session::for_record("test", "test", "user", rid)
}

/// The principals each statement runs as: exempt ones, whose writes no gate
/// judges, and checked ones, whose every write is judged.
fn principals() -> Vec<(&'static str, Session)> {
	vec![
		("owner", TestDb::owner()),
		("record user", record_user()),
		(
			"database editor",
			Session::for_level(Level::Database("test".into(), "test".into()), Role::Editor),
		),
		(
			"database viewer",
			Session::for_level(Level::Database("test".into(), "test".into()), Role::Viewer),
		),
		(
			"namespace viewer",
			Session::for_level(Level::Namespace("test".into()), Role::Viewer).with_db("test"),
		),
	]
}

/// The answer of `sql`, run as `session` on a fresh datastore set up by
/// `schema`, and the records of each of `tables` after it, read by the owner.
/// The answer of several statements is the rows of each in turn, or the first
/// error one raised.
async fn written(
	schema: &str,
	tables: &[&str],
	sql: &str,
	session: &Session,
) -> (Result<Value, String>, Vec<String>) {
	let db = TestDb::quiescent(schema, true).await;
	let results = db.datastore().execute(sql, session, None).await.expect("runs");
	let mut answers = Vec::new();
	for result in results {
		answers.push(result.result.map(Value::from).map_err(|error| error.to_string()));
	}
	let answer = match answers.len() {
		1 => answers.remove(0),
		_ => answers
			.into_iter()
			.map(|answer| {
				let val = answer?;
				if let Value::Array(rows) = val {
					Ok(rows.0)
				} else {
					Ok(vec![val])
				}
			})
			.collect::<Result<Vec<Vec<Value>>, String>>()
			.map(|rows| Value::from(rows.into_iter().flatten().collect::<Vec<_>>())),
	};
	let mut state = Vec::new();
	for read in tables.iter().map(|table| format!("SELECT * FROM {table} ORDER BY id")) {
		let mut results =
			db.datastore().execute(&read, &TestDb::owner(), None).await.expect("runs");
		let rows = results.pop().expect("a result").result.map(Value::from);
		state.push(match rows {
			Ok(rows) => format!("{read}: {}", rows.to_sql()),
			Err(error) => format!("{read}: {error}"),
		});
	}
	(answer, state)
}

/// A table beside a subquery is written as the statements naming each alone
/// are, for every principal and under every kind of table permission: the same
/// answers, and the same records written, guards' side effects included. The
/// subqueries read no table, so what they name depends neither on who runs them
/// nor on what the table's records became.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_table_beside_a_subquery_is_gated_as_each_alone() {
	use futures::StreamExt as _;

	const CASES: [(&str, &str); 7] = [
		// The table's records, and nothing the subquery names.
		("UPDATE {t}, (SELECT VALUE {t}:1 FROM []) SET n += 1", "UPDATE {t} SET n += 1"),
		(
			"UPDATE {t}, (SELECT VALUE {t}:1 FROM []) SET n += 1 WHERE n < 5 RETURN BEFORE",
			"UPDATE {t} SET n += 1 WHERE n < 5 RETURN BEFORE",
		),
		("DELETE {t}, (SELECT VALUE {t}:1 FROM []) RETURN BEFORE", "DELETE {t} RETURN BEFORE"),
		// The table's records, then the record the subquery names.
		(
			"UPDATE {t}, (SELECT VALUE {t}:1 FROM ONLY 1) SET n += 1",
			"UPDATE {t} SET n += 1; UPDATE {t}:1 SET n += 1",
		),
		(
			"UPDATE {t}, (SELECT VALUE {t}:1 FROM ONLY 1) SET n += 1 WHERE n < 5 RETURN BEFORE",
			"UPDATE {t} SET n += 1 WHERE n < 5 RETURN BEFORE;
			UPDATE {t}:1 SET n += 1 WHERE n < 5 RETURN BEFORE",
		),
		(
			"UPDATE {t}, (SELECT VALUE { id: {t}:1 } FROM ONLY 1) SET m = 1 RETURN AFTER",
			"UPDATE {t} SET m = 1 RETURN AFTER; UPDATE {t}:1 SET m = 1 RETURN AFTER",
		),
		(
			"DELETE {t}, (SELECT VALUE {t}:1 FROM ONLY 1) RETURN BEFORE",
			"DELETE {t} RETURN BEFORE; DELETE {t}:1 RETURN BEFORE",
		),
	];

	let jobs: Vec<_> = principals()
		.into_iter()
		.flat_map(|(who, session)| {
			GATED_TABLES.iter().flat_map(move |&table| {
				let session = session.clone();
				CASES.iter().map(move |&(mixed, alone)| {
					(who, session.clone(), mixed.replace("{t}", table), alone.replace("{t}", table))
				})
			})
		})
		.collect();

	futures::stream::iter(jobs)
		.for_each_concurrent(8, |(who, session, mixed, alone)| async move {
			assert_eq!(
				written(GATED_SCHEMA, GATED_TABLES, &mixed, &session).await,
				written(GATED_SCHEMA, GATED_TABLES, &alone, &session).await,
				"{who}: `{mixed}` against `{alone}`"
			);
		})
		.await;
}

/// Tables whose clauses a stored record fails and an object naming it, with
/// fields of its own, would pass: `n` of `lim:2` is over the bound, and
/// `cond:2` is locked against deletes.
const FORGED_SCHEMA: &str = "
	DEFINE ACCESS user ON DATABASE TYPE RECORD;
	DEFINE TABLE open PERMISSIONS FULL;
	DEFINE TABLE lim PERMISSIONS FOR select FULL FOR create, update, delete WHERE n < 10;
	DEFINE TABLE cond PERMISSIONS FOR select FULL FOR create, update FULL
		FOR delete WHERE locked != true;
	DEFINE TABLE comp PERMISSIONS FOR select, create, update, delete WHERE blocked != true;
	DEFINE FIELD blocked ON comp COMPUTED n > 5;
	DEFINE TABLE owned PERMISSIONS
		FOR select, create, update, delete WHERE owner = $auth.id;
	CREATE open:1 SET n = 1;
	CREATE lim:1 SET n = 1;
	CREATE lim:2 SET n = 50;
	CREATE cond:2 SET n = 5, locked = true;
	CREATE comp:2 SET n = 7;
	CREATE owned:2 SET owner = user:other;
";

/// The tables [`FORGED_SCHEMA`] defines, read back after each statement.
const FORGED_TABLES: &[&str] = &["open", "lim", "cond", "comp", "owned"];

/// An object beside a table names the stored record its `id` names, and the
/// record as stored is what the table's permissions judge: fields the object
/// carries that would pass a clause the stored record fails admit nothing, for
/// every principal. Each list is written as the statements naming the table
/// and the record alone, and for a record user, whose writes are judged, the
/// stored record is left as it was.
#[tokio::test]
async fn an_object_beside_a_table_is_judged_as_the_stored_record() {
	for (who, session) in principals() {
		for (mixed, alone, kept) in [
			(
				"UPDATE open, { id: lim:2, n: 1 } SET m = 1 RETURN AFTER",
				"UPDATE open SET m = 1 RETURN AFTER; UPDATE lim:2 SET m = 1 RETURN AFTER",
				"{ id: lim:2, n: 50 }",
			),
			(
				"LET $o = { id: lim:2, n: 1 }; UPDATE open, $o SET m = 1 RETURN AFTER",
				"LET $o = { id: lim:2, n: 1 };
				UPDATE open SET m = 1 RETURN AFTER; UPDATE lim:2 SET m = 1 RETURN AFTER",
				"{ id: lim:2, n: 50 }",
			),
			(
				"UPDATE open, (SELECT VALUE { id: lim:2, n: 1 } FROM ONLY 1) SET m = 1 RETURN AFTER",
				"UPDATE open SET m = 1 RETURN AFTER; UPDATE lim:2 SET m = 1 RETURN AFTER",
				"{ id: lim:2, n: 50 }",
			),
			(
				"DELETE open, { id: lim:2, n: 1 } RETURN BEFORE",
				"DELETE open RETURN BEFORE; DELETE lim:2 RETURN BEFORE",
				"{ id: lim:2, n: 50 }",
			),
			(
				"DELETE open, { id: cond:2, locked: false } RETURN BEFORE",
				"DELETE open RETURN BEFORE; DELETE cond:2 RETURN BEFORE",
				"{ id: cond:2, locked: true, n: 5 }",
			),
			// A clause over a `COMPUTED` field the object sets to a passing value.
			(
				"UPDATE open, { id: comp:2, n: 1, blocked: false } SET m = 1 RETURN AFTER",
				"UPDATE open SET m = 1 RETURN AFTER; UPDATE comp:2 SET m = 1 RETURN AFTER",
				"{ blocked: true, id: comp:2, n: 7 }",
			),
			// A clause over `$auth`: the stored owner is another user.
			(
				"UPDATE open, { id: owned:2, owner: user:reader } SET m = 1 RETURN AFTER",
				"UPDATE open SET m = 1 RETURN AFTER; UPDATE owned:2 SET m = 1 RETURN AFTER",
				"{ id: owned:2, owner: user:other }",
			),
		] {
			let mixed_written = written(FORGED_SCHEMA, FORGED_TABLES, mixed, &session).await;
			if who == "record user" {
				assert!(
					mixed_written.1.iter().any(|records| records.contains(kept)),
					"{who}: `{mixed}` wrote a record its stored image refuses: {:?}",
					mixed_written.1
				);
			}
			assert_eq!(
				mixed_written,
				written(FORGED_SCHEMA, FORGED_TABLES, alone, &session).await,
				"{who}: `{mixed}` against `{alone}`"
			);
		}
		// A clause reading `$before`, judged on the stored record's image.
		let (mixed, alone) = (
			"UPDATE open, { id: cond:2, n: 1 } SET m = 1 RETURN AFTER",
			"UPDATE open SET m = 1 RETURN AFTER; UPDATE cond:2 SET m = 1 RETURN AFTER",
		);
		assert_eq!(
			written(GATED_SCHEMA, GATED_TABLES, mixed, &session).await,
			written(GATED_SCHEMA, GATED_TABLES, alone, &session).await,
			"{who}: `{mixed}` against `{alone}`"
		);
	}
}

/// A table or record the caller may not write names nothing in a list of
/// targets, whatever the rest of the list holds: the list writes what it writes
/// without it, and the caller is not refused the statement.
#[tokio::test]
async fn a_refused_table_in_a_list_names_nothing() {
	let reader = record_user();
	for (listed, without) in [
		("UPDATE shut, open:1 SET n += 1 RETURN AFTER", "UPDATE open:1 SET n += 1 RETURN AFTER"),
		("UPDATE open:1, shut SET n += 1 RETURN AFTER", "UPDATE open:1 SET n += 1 RETURN AFTER"),
		("DELETE shut, open:1 RETURN BEFORE", "DELETE open:1 RETURN BEFORE"),
		("UPDATE shut:1, shut:2 SET n += 1", "UPDATE shut:1 SET n += 1"),
		(
			"UPDATE shut, (SELECT VALUE open:1 FROM ONLY 1) SET n += 1 RETURN AFTER",
			"UPDATE (SELECT VALUE open:1 FROM ONLY 1) SET n += 1 RETURN AFTER",
		),
		(
			"UPDATE shut, (SELECT VALUE { id: open:1 } FROM ONLY 1) SET n += 1 RETURN AFTER",
			"UPDATE (SELECT VALUE { id: open:1 } FROM ONLY 1) SET n += 1 RETURN AFTER",
		),
		(
			"LET $o = { id: open:1 }; UPDATE shut, $o SET n += 1 RETURN AFTER",
			"LET $o = { id: open:1 }; UPDATE $o SET n += 1 RETURN AFTER",
		),
		(
			"UPDATE shut, open, (SELECT VALUE open:1 FROM ONLY 1) SET n += 1 RETURN AFTER",
			"UPDATE open, (SELECT VALUE open:1 FROM ONLY 1) SET n += 1 RETURN AFTER",
		),
	] {
		let listed_written = written(GATED_SCHEMA, GATED_TABLES, listed, &reader).await;
		assert!(listed_written.0.is_ok(), "`{listed}` failed: {:?}", listed_written.0);
		assert_eq!(
			listed_written,
			written(GATED_SCHEMA, GATED_TABLES, without, &reader).await,
			"`{listed}` against `{without}`"
		);
	}
}

/// A table beside a subquery or an object naming one of its records writes the
/// stored record, whose fields the caller's view hides or the value lacks: no
/// field is lost, for the owner and for a record user whose reads are checked,
/// and under `ONLY` as without it.
#[tokio::test]
async fn a_table_beside_a_value_writes_the_stored_record() {
	for (who, session) in [("owner", TestDb::owner()), ("record user", record_user())] {
		for (mixed, alone) in [
			(
				"UPDATE doc, (SELECT VALUE id FROM []) SET n += 1 RETURN AFTER",
				"UPDATE doc SET n += 1 RETURN AFTER",
			),
			(
				"UPDATE doc, (SELECT id FROM doc:1) SET n += 1 RETURN AFTER",
				"UPDATE doc SET n += 1 RETURN AFTER; UPDATE doc:1 SET n += 1 RETURN AFTER",
			),
			(
				"UPDATE doc, { id: doc:1 } SET n += 1 RETURN AFTER",
				"UPDATE doc SET n += 1 RETURN AFTER; UPDATE doc:1 SET n += 1 RETURN AFTER",
			),
			(
				"UPDATE (SELECT id FROM doc:2), { id: doc:1 } SET n += 1 RETURN AFTER",
				"UPDATE doc:2 SET n += 1 RETURN AFTER; UPDATE doc:1 SET n += 1 RETURN AFTER",
			),
			(
				"UPDATE ONLY doc:9, { id: doc:1 } SET n += 1 RETURN AFTER",
				"UPDATE ONLY doc:1 SET n += 1 RETURN AFTER",
			),
			(
				"DELETE doc, (SELECT id FROM doc:1) RETURN BEFORE",
				"DELETE doc RETURN BEFORE; DELETE doc:1 RETURN BEFORE",
			),
		] {
			let (answer, state) = written(VIEWED_SCHEMA, &["doc"], mixed, &session).await;
			if mixed.starts_with("UPDATE") {
				assert!(
					state.iter().any(|records| records.contains("secret: 's1'")),
					"{who}: `{mixed}` lost a field: {state:?}"
				);
			}
			assert_eq!(
				(answer, state),
				written(VIEWED_SCHEMA, &["doc"], alone, &session).await,
				"{who}: `{mixed}` against `{alone}`"
			);
		}
	}
}

/// Each target is evaluated when the write reaches it: a subquery after a
/// table reads the records the table's writes left, and a table after a
/// subquery reads the records the subquery's writes left, as the statements
/// naming each in turn do.
#[tokio::test]
async fn a_later_target_reads_what_an_earlier_one_wrote() {
	let schema = "CREATE t:1 SET n = 0; CREATE t:2 SET n = 5;";
	for (mixed, alone) in [
		(
			"UPDATE t, (SELECT VALUE id FROM t WHERE n = 1) SET n += 1 RETURN AFTER",
			"UPDATE t SET n += 1 RETURN AFTER; \
			 UPDATE (SELECT VALUE id FROM t WHERE n = 1) SET n += 1 RETURN AFTER",
		),
		(
			"UPDATE t:1, (SELECT VALUE id FROM t WHERE n = 1) SET n += 1 RETURN AFTER",
			"UPDATE t:1 SET n += 1 RETURN AFTER; \
			 UPDATE (SELECT VALUE id FROM t WHERE n = 1) SET n += 1 RETURN AFTER",
		),
		(
			"UPDATE (SELECT VALUE id FROM t WHERE n = 0), t SET n += 1 RETURN AFTER",
			"UPDATE (SELECT VALUE id FROM t WHERE n = 0) SET n += 1 RETURN AFTER; \
			 UPDATE t SET n += 1 RETURN AFTER",
		),
		(
			"DELETE t:1, (SELECT VALUE id FROM t) RETURN BEFORE",
			"DELETE t:1 RETURN BEFORE; DELETE (SELECT VALUE id FROM t) RETURN BEFORE",
		),
	] {
		let (answer, state) = written(schema, &["t"], mixed, &TestDb::owner()).await;
		assert_eq!(
			(answer, state),
			written(schema, &["t"], alone, &TestDb::owner()).await,
			"`{mixed}` against `{alone}`"
		);
	}
}

/// A table a block defined by writing to it, after an earlier read in the
/// block cached its absence, is written by a target that names it as a value,
/// alone or beside other targets, as by one naming it outright: the write
/// finds the table the block defined rather than the absence it read first.
#[tokio::test]
async fn a_table_a_block_defined_after_reading_its_absence_is_written() {
	let both = Value::from(crate::syn::value("[t:[1], t:[2]]").expect("parses"));
	let first = Value::from(crate::syn::value("[t:[1]]").expect("parses"));
	for (write, expected) in [
		("UPDATE t SET touched = true RETURN VALUE id", &both),
		("UPDATE type::table('t') SET touched = true RETURN VALUE id", &both),
		("UPDATE $tb SET touched = true RETURN VALUE id", &both),
		(
			"UPDATE (IF rand::int(1, 1) = 1 { t } ELSE { NONE }) SET touched = true RETURN VALUE id",
			&both,
		),
		(
			"UPDATE (IF rand::int(1, 1) = 1 { t:[1] } ELSE { NONE }) SET touched = true RETURN VALUE id",
			&first,
		),
		("UPDATE t, (SELECT VALUE t:[1] FROM []) SET touched = true RETURN VALUE id", &both),
		("DELETE type::table('t') RETURN VALUE $before.id", &both),
	] {
		let sql = format!(
			"{{ LET $tb = type::table('t'); LET $known = record::exists(t:[1]); \
			 CREATE t:[1], t:[2]; RETURN {write} }}"
		);
		let (answer, _) = written("", &["t"], &sql, &TestDb::owner()).await;
		assert_eq!(answer, Ok(expected.clone()), "{sql}");
	}
}

/// Points a nearest-neighbour condition ranks and documents a full-text one
/// matches.
const SEARCH_SCHEMA: &str = "
	CREATE pts:1 SET point = [0, 0];
	CREATE pts:2 SET point = [5, 5];
	DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
	DEFINE INDEX txt ON doc FIELDS txt FULLTEXT ANALYZER simple BM25;
	CREATE doc:1 SET txt = 'hello world';
	CREATE doc:2 SET txt = 'goodbye';
	CREATE doc:3 SET txt = 'hello again';
";

/// A nearest-neighbour condition over a list of targets holding stored records
/// or a nested `SELECT`, and a full-text condition over a list holding a nested
/// `SELECT` but no table, are refused, since no search answers them over the
/// list: the statement writes nothing. A full-text condition over a list
/// naming its table is answered from the index, and a value or a list of
/// values alone is written as ever.
#[tokio::test]
async fn a_search_no_index_answers_over_a_list_is_refused() {
	const STORED: &str = "whose targets mix a table or record with a subquery";
	const LISTED: &str = "whose targets list a subquery beside other targets but no table";
	for (write, refusal) in [
		(
			"UPDATE (SELECT VALUE pts:1 FROM ONLY 1), { id: pts:2 } SET k = true \
			 WHERE point <|1,EUCLIDEAN|> [0, 0]",
			LISTED,
		),
		(
			"LET $o = { id: pts:2 }; UPDATE (SELECT VALUE pts:1 FROM ONLY 1), $o SET k = true \
			 WHERE point <|1,EUCLIDEAN|> [0, 0]",
			LISTED,
		),
		(
			"DELETE (SELECT VALUE pts:1 FROM ONLY 1), (SELECT VALUE pts:2 FROM ONLY 1) \
			 WHERE point <|1,EUCLIDEAN|> [0, 0]",
			LISTED,
		),
		(
			"UPDATE pts:1, (SELECT VALUE pts:2 FROM ONLY 1) SET k = true \
			 WHERE point <|1,EUCLIDEAN|> [0, 0]",
			STORED,
		),
		("UPDATE pts:1, { id: pts:2 } SET k = true WHERE point <|1,EUCLIDEAN|> [0, 0]", STORED),
		(
			"UPDATE (SELECT VALUE doc:1 FROM ONLY 1), { id: doc:3 } SET k = true \
			 WHERE txt @@ 'hello'",
			LISTED,
		),
		(
			"LET $o = { id: doc:3 }; UPDATE (SELECT VALUE doc:1 FROM ONLY 1), $o SET k = true \
			 WHERE txt @1@ 'hello' RETURN id, search::score(1) AS score",
			LISTED,
		),
		("DELETE (SELECT VALUE doc:1 FROM ONLY 1), { id: doc:3 } WHERE txt @@ 'hello'", LISTED),
		(
			"UPDATE doc:1, (SELECT VALUE doc:3 FROM ONLY 1) SET k = true WHERE txt @@ 'hello'",
			LISTED,
		),
	] {
		let (answer, state) =
			written(SEARCH_SCHEMA, &["pts", "doc"], write, &TestDb::owner()).await;
		assert!(
			answer.as_ref().is_err_and(|error| error.contains(refusal)),
			"`{write}` answered {answer:?}"
		);
		assert_eq!(
			state,
			written(SEARCH_SCHEMA, &["pts", "doc"], "RETURN NONE", &TestDb::owner()).await.1,
			"`{write}` wrote"
		);
	}
	for (write, expected) in [
		(
			"UPDATE doc, (SELECT VALUE doc:3 FROM ONLY 1) SET k = true WHERE txt @@ 'hello' \
			 RETURN VALUE id",
			"[doc:1, doc:3, doc:3]",
		),
		("UPDATE { id: doc:3 } SET k = true WHERE txt @@ 'hello' RETURN VALUE id", "[doc:3]"),
		(
			"LET $a = { id: doc:1 }; LET $b = { id: doc:3 }; \
			 UPDATE $a, $b SET k = true WHERE txt @@ 'hello' RETURN VALUE id",
			"[NONE, NONE, doc:1, doc:3]",
		),
		(
			"UPDATE doc:1, { id: doc:3 } SET k = true WHERE txt @@ 'hello' RETURN VALUE id",
			"[doc:1, doc:3]",
		),
		(
			"UPDATE (SELECT VALUE doc:1 FROM ONLY 1) SET k = true WHERE txt @@ 'hello' \
			 RETURN VALUE id",
			"[doc:1]",
		),
		(
			"UPDATE { id: pts:1 } SET k = true WHERE point <|1,EUCLIDEAN|> [0, 0] RETURN VALUE id",
			"[]",
		),
	] {
		let (answer, _) = written(SEARCH_SCHEMA, &["pts", "doc"], write, &TestDb::owner()).await;
		assert_eq!(
			answer,
			Ok(Value::from(crate::syn::value(expected).expect("parses"))),
			"`{write}`"
		);
	}
}
