//! A list of record ids read as a whole source compiles, when the statement is planned, the code
//! its records are read and written by: the read units of the tables its ids name, beside the
//! tables the statement's other sources name, and the stages of a write of the records a
//! parameter's ids name. Each test runs statements through the datastore with the ids bound as a
//! client parameter, as a session would, and counts the table-read and stage faults the run took.

use super::fault::{READ_FAULTS, STAGE_FAULTS};
use crate::exec::operators::test_util::TestDb;

/// The table-read and stage faults taken running `sql` with `$ids` bound to the record ids of
/// `table` keyed by `keys`, each statement of which must answer.
async fn faults(db: &TestDb, sql: &str, table: &str, keys: &[i64]) -> (usize, usize) {
	let ids: Vec<surrealdb_types::Value> = keys
		.iter()
		.map(|key| surrealdb_types::Value::RecordId(surrealdb_types::RecordId::new(table, *key)))
		.collect();
	let mut vars = surrealdb_types::Variables::new();
	vars.insert("ids", surrealdb_types::Value::Array(ids.into()));
	READ_FAULTS.with(|faults| faults.set(0));
	STAGE_FAULTS.with(|faults| faults.set(0));
	for response in db.datastore().execute(sql, &TestDb::owner(), Some(vars)).await.expect("runs") {
		if let Err(e) = response.result {
			panic!("`{sql}` failed: {e}");
		}
	}
	(READ_FAULTS.with(std::cell::Cell::get), STAGE_FAULTS.with(std::cell::Cell::get))
}

const SETUP: &str = "
	DEFINE TABLE person SCHEMAFULL;
	DEFINE FIELD n ON person TYPE int;
	DEFINE FIELD double ON person COMPUTED n * 2;
	CREATE person:1 SET n = 1;
	CREATE person:2 SET n = 2;
	CREATE person:3 SET n = 3;
";

/// The table a list of ids written in `FROM` names keeps its read unit linked when a parameter
/// beside it may name any table, so reading the parameter's records of that table faults on
/// nothing.
#[tokio::test]
async fn a_written_list_beside_a_parameter_keeps_its_table() {
	let db = TestDb::new(SETUP).await;
	let (reads, _) = faults(&db, "SELECT n, double FROM [person:1], $ids", "person", &[2, 3]).await;
	assert_eq!(reads, 0, "the read unit of `person` is linked when the statement is planned");
}

/// `UPDATE` and `DELETE` of a parameter holding record ids of one table compile that table's
/// stages when the statement is planned, as the same list written as the target does.
#[tokio::test]
async fn a_write_of_a_parameter_list_compiles_its_stages_when_planned() {
	let db = TestDb::new(SETUP).await;
	for sql in ["UPDATE $ids SET n += 10", "UPDATE [person:1, person:2] SET n += 10", "DELETE $ids"]
	{
		let (_, stages) = faults(&db, sql, "person", &[1, 2]).await;
		assert_eq!(stages, 0, "`{sql}` loads no stage bundle while it runs");
	}
}
