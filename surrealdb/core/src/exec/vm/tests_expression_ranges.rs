//! A record range or a table that an expression in `FROM` evaluates to is a
//! set of records, read as a range or table a call or a parameter yields is
//! read: every record of the range or table, under the caller's permissions.
//! A single record id the expression evaluates to is the record it names.
//! Each holds for a `SELECT`, nested or not, and for a write whose targets mix
//! such an expression with a range, which reads its subjects through the same
//! leaf.

use crate::exec::operators::test_util::TestDb;
use crate::val::Value;

/// Every statement's value of `sql` run as `session`, an error rendered as
/// its message.
async fn values_as(db: &TestDb, session: &crate::dbs::Session, sql: &str) -> Vec<Value> {
	let results = db.datastore().execute(sql, session, None).await.expect("runs");
	results
		.into_iter()
		.map(|r| r.result.map(Value::from).unwrap_or_else(|e| Value::from(e.to_string())))
		.collect()
}

/// The value of the last statement of `sql`, run as the owner.
async fn last(db: &TestDb, sql: &str) -> Value {
	values_as(db, &TestDb::owner(), sql).await.pop().expect("a statement")
}

fn value(text: &str) -> Value {
	Value::from(crate::syn::value(text).expect("parses"))
}

const SETUP: &str = "CREATE rr:[1], rr:[2], rr:[5], rr:[6];";

/// `rr:[5]..` behind a branch the planner cannot fold.
const DYNAMIC_RANGE: &str = "(IF rand::int(1, 1) = 1 { rr:[5].. } ELSE { NONE })";

#[tokio::test]
async fn an_expression_range_in_from_reads_its_records() {
	let db = TestDb::new(SETUP).await;
	let mixed = format!("rr:[1]..=[2], {DYNAMIC_RANGE}");
	for (source, expected) in [
		(DYNAMIC_RANGE, "[rr:[5], rr:[6]]"),
		("(IF true { rr:[5].. } ELSE { NONE })", "[rr:[5], rr:[6]]"),
		("(IF rand::int(1, 1) = 1 { rr:[1]..[5] } ELSE { NONE })", "[rr:[1], rr:[2]]"),
		("(IF rand::int(1, 1) = 1 { rr:[1]..=[5] } ELSE { NONE })", "[rr:[1], rr:[2], rr:[5]]"),
		("(IF rand::int(1, 1) = 1 { rr:..[5] } ELSE { NONE })", "[rr:[1], rr:[2]]"),
		("(IF rand::int(1, 1) = 1 { rr:.. } ELSE { NONE })", "[rr:[1], rr:[2], rr:[5], rr:[6]]"),
		("{ a: rr:[5].. }.a", "[rr:[5], rr:[6]]"),
		// A table the expression evaluates to is read whole.
		("(IF rand::int(1, 1) = 1 { rr } ELSE { NONE })", "[rr:[1], rr:[2], rr:[5], rr:[6]]"),
		// A record id is the record it names, and no row where none answers.
		("(IF rand::int(1, 1) = 1 { rr:[5] } ELSE { NONE })", "[rr:[5]]"),
		("(IF rand::int(1, 1) = 1 { rr:[9] } ELSE { NONE })", "[]"),
		("(IF rand::int(1, 1) = 1 { NONE } ELSE { rr:[5].. })", "[]"),
		// The range among other sources.
		(&mixed, "[rr:[1], rr:[2], rr:[5], rr:[6]]"),
	] {
		let sql = format!("SELECT VALUE id FROM {source}");
		assert_eq!(last(&db, &sql).await, value(expected), "{sql}");
	}
}

#[tokio::test]
async fn an_expression_range_reads_as_a_called_range_does() {
	let db = TestDb::new(SETUP).await;
	for (expression, called) in [
		(DYNAMIC_RANGE, "array::first([rr:[5].., rand::int(1, 1)])"),
		("(IF rand::int(1, 1) = 1 { rr } ELSE { NONE })", "type::table('rr')"),
	] {
		for tail in ["", " LIMIT 1", " START 1 LIMIT 1", " LIMIT 0", " ORDER BY id DESC"] {
			let read = |source: &str| format!("SELECT VALUE id FROM {source}{tail}");
			let expected = last(&db, &read(called)).await;
			assert!(matches!(&expected, Value::Array(_)), "{}: {expected:?}", read(called));
			assert_eq!(last(&db, &read(expression)).await, expected, "{}", read(expression));
		}
	}
	// A range or table of a table that does not exist is refused, as a
	// called one is.
	for source in [
		"(IF rand::int(1, 1) = 1 { nope:[5].. } ELSE { NONE })",
		"(IF rand::int(1, 1) = 1 { nope } ELSE { NONE })",
	] {
		let sql = format!("SELECT * FROM {source}");
		assert_eq!(
			last(&db, &sql).await,
			Value::from("The table 'nope' does not exist".to_string()),
			"{sql}"
		);
	}
}

#[tokio::test]
async fn a_nested_expression_range_reads_per_outer_row() {
	let db =
		TestDb::new(&format!("{SETUP} CREATE ctl:1 SET go = true; CREATE ctl:2 SET go = false;"))
			.await;
	for (branch, expected) in [
		("rr:[5]..", "[{ id: ctl:1, r: [rr:[5], rr:[6]] }, { id: ctl:2, r: [] }]"),
		("rr:[5]", "[{ id: ctl:1, r: [rr:[5]] }, { id: ctl:2, r: [] }]"),
	] {
		let sql = format!(
			"SELECT id, (SELECT VALUE id FROM (IF $parent.go {{ {branch} }} ELSE {{ NONE }})) AS r
			 FROM ctl ORDER BY id"
		);
		assert_eq!(last(&db, &sql).await, value(expected), "{sql}");
	}
}

#[tokio::test]
async fn an_expression_range_reads_a_table_the_run_created() {
	let db = TestDb::new("").await;
	let sql = "BEGIN;
		CREATE fresh:[1], fresh:[2], fresh:[3];
		LET $r = SELECT VALUE id FROM (IF rand::int(1, 1) = 1 { fresh:[2].. } ELSE { NONE });
		RETURN $r;
		COMMIT;";
	let results = values_as(&db, &TestDb::owner(), sql).await;
	assert_eq!(results.get(3), Some(&value("[fresh:[2], fresh:[3]]")), "{results:?}");
}

#[tokio::test]
async fn writes_over_mixed_targets_reach_every_record_of_an_expression_range() {
	let table = "(IF rand::int(1, 1) = 1 { rr } ELSE { NONE })";
	for (write, check, expected) in [
		(
			format!("UPDATE rr:[1]..=[2], {DYNAMIC_RANGE} SET touched = true RETURN NONE"),
			"SELECT VALUE id FROM rr WHERE touched ORDER BY id",
			"[rr:[1], rr:[2], rr:[5], rr:[6]]",
		),
		(
			format!("UPDATE rr:[1]..=[1], {table} SET touched = true RETURN NONE"),
			"SELECT VALUE id FROM rr WHERE touched ORDER BY id",
			"[rr:[1], rr:[2], rr:[5], rr:[6]]",
		),
		(
			format!("UPSERT rr:[1]..=[1], {DYNAMIC_RANGE} SET touched = true RETURN NONE"),
			"SELECT VALUE id FROM rr WHERE touched ORDER BY id",
			"[rr:[1], rr:[5], rr:[6]]",
		),
		(
			format!("DELETE rr:[1]..=[1], {DYNAMIC_RANGE}"),
			"SELECT VALUE id FROM rr ORDER BY id",
			"[rr:[2]]",
		),
		(
			format!("UPDATE rr:[1]..=[1], {DYNAMIC_RANGE} SET touched = true RETURN VALUE id"),
			"SELECT VALUE id FROM rr WHERE touched ORDER BY id",
			"[rr:[1], rr:[5], rr:[6]]",
		),
	] {
		let db = TestDb::new(SETUP).await;
		let written = values_as(&db, &TestDb::owner(), &write).await;
		assert!(
			written.iter().all(|v| !matches!(v, Value::String(_))),
			"{write} failed: {written:?}"
		);
		assert_eq!(last(&db, check).await, value(expected), "{write}");
	}
}

#[tokio::test]
async fn an_expression_range_spanning_many_batches_reads_every_record() {
	let setup = "FOR $i IN 1..=1500 { CREATE type::record('rr', [$i]) };";
	for config in [
		surrealdb_cnf::ConfigMap::empty(),
		surrealdb_cnf::ConfigMap::empty().with_key_value("vm_quantum", "1"),
	] {
		let db = TestDb::new_with_config(setup, config).await;
		let read =
			last(&db, "SELECT VALUE id FROM (IF rand::int(1, 1) = 1 { rr:[2].. } ELSE { NONE })")
				.await;
		let Value::Array(ids) = read else {
			panic!("an array of ids, not {read:?}");
		};
		assert_eq!(ids.len(), 1499);
		assert_eq!(ids.first(), Some(&value("rr:[2]")));
		assert_eq!(ids.last(), Some(&value("rr:[1500]")));
		let counted = last(
			&db,
			"UPDATE rr:[1]..=[1], (IF rand::int(1, 1) = 1 { rr:[2].. } ELSE { NONE })
				SET touched = true RETURN NONE;
			 SELECT VALUE count() FROM rr WHERE touched GROUP ALL;",
		)
		.await;
		assert_eq!(counted, value("[1500]"));
	}
}

/// A table a record user may select and update only where `open`, with a
/// field the user may not select.
const GUARDED: &str = "DEFINE ACCESS user ON DATABASE TYPE RECORD;
	DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
	DEFINE TABLE rr SCHEMALESS PERMISSIONS
		FOR select, update WHERE open = true
		FOR create, delete NONE;
	DEFINE FIELD secret ON rr PERMISSIONS FOR select NONE;
	CREATE rr:[1] SET open = true, secret = 's1';
	CREATE rr:[2] SET open = true, secret = 's2';
	CREATE rr:[5] SET open = true, secret = 's5';
	CREATE rr:[6] SET open = false, secret = 's6';
	CREATE rr:[7] SET open = true, secret = 's7';";

fn record_user() -> crate::dbs::Session {
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "u"));
	crate::dbs::Session::for_record("test", "test", "user", rid)
}

#[tokio::test]
async fn an_expression_range_reads_under_the_callers_permissions() {
	let db = TestDb::new_with_auth(GUARDED).await;
	for source in [DYNAMIC_RANGE, "array::first([rr:[5].., rand::int(1, 1)])"] {
		let sql = format!("SELECT * FROM {source}");
		assert_eq!(
			values_as(&db, &record_user(), &sql).await.pop(),
			Some(value("[{ id: rr:[5], open: true }, { id: rr:[7], open: true }]")),
			"{sql}"
		);
	}
}

#[tokio::test]
async fn a_write_over_mixed_targets_writes_the_records_as_stored() {
	// Each target beside `rr:[1]..=[1]` is read through a value in `FROM`;
	// the write's subjects are the records as stored, which the write's
	// permission judges, so a field the caller may not select is kept.
	let mut wrong = Vec::new();
	for (target, touched) in [
		(DYNAMIC_RANGE, "[rr:[1], rr:[5], rr:[7]]"),
		("array::first([rr:[5].., rand::int(1, 1)])", "[rr:[1], rr:[5], rr:[7]]"),
		("(IF rand::int(1, 1) = 1 { rr:[5] } ELSE { NONE })", "[rr:[1], rr:[5]]"),
		("(IF rand::int(1, 1) = 1 { rr:[6] } ELSE { NONE })", "[rr:[1]]"),
		("array::first([rr:[5], rand::int(1, 1)])", "[rr:[1], rr:[5]]"),
		("(IF rand::int(1, 1) = 1 { rr } ELSE { NONE })", "[rr:[1], rr:[2], rr:[5], rr:[7]]"),
		("[rr:[5], rr:[6], rr:[7]]", "[rr:[1], rr:[5], rr:[7]]"),
		// An object names the record its `id` names; the record is the one
		// stored, not the object.
		(
			"(IF rand::int(1, 1) = 1 { { id: rr:[6], open: true, secret: 'forged' } } ELSE { NONE })",
			"[rr:[1]]",
		),
		(
			"(IF rand::int(1, 1) = 1 { { id: rr:[5], secret: 'forged' } } ELSE { NONE })",
			"[rr:[1], rr:[5]]",
		),
	] {
		let db = TestDb::new_with_auth(GUARDED).await;
		let write = format!("UPDATE rr:[1]..=[1], {target} SET touched = true RETURN AFTER");
		let returned = values_as(&db, &record_user(), &write).await;
		// The caller is shown its own view of what it wrote.
		let Some(Value::Array(rows)) = returned.first() else {
			panic!("{write}: {returned:?}");
		};
		for row in rows.iter() {
			let Value::Object(row) = row else {
				panic!("{write}: {row:?}");
			};
			assert!(!row.contains_key("secret"), "{write}: {row:?}");
		}
		let written = last(&db, "SELECT VALUE id FROM rr WHERE touched ORDER BY id").await;
		let kept = last(&db, "SELECT VALUE secret FROM rr ORDER BY id").await;
		if written != value(touched) || kept != value("['s1', 's2', 's5', 's6', 's7']") {
			wrong.push(format!("{write}: touched {written:?}, secrets {kept:?}"));
		}
	}
	assert!(wrong.is_empty(), "{wrong:#?}");
}

#[tokio::test]
async fn a_write_over_an_expression_range_reads_the_callers_view() {
	// A condition on a field the caller may not select matches no record,
	// and a data clause reads no value of it, whether the range is written out
	// or an expression evaluates to it.
	for target in ["rr:[5]..", DYNAMIC_RANGE] {
		let db = TestDb::new_with_auth(GUARDED).await;
		let write = format!("UPDATE rr:[1]..=[1], {target} SET leak = secret RETURN NONE");
		values_as(&db, &record_user(), &write).await;
		assert_eq!(
			last(&db, "SELECT VALUE leak FROM rr WHERE leak != NONE").await,
			value("[]"),
			"{write}"
		);
	}
	for target in ["rr:[5]..", DYNAMIC_RANGE] {
		let db = TestDb::new_with_auth(GUARDED).await;
		let write = format!(
			"UPDATE rr:[1]..=[1], {target} SET touched = true WHERE secret = 's5' RETURN NONE"
		);
		values_as(&db, &record_user(), &write).await;
		assert_eq!(
			last(&db, "SELECT VALUE id FROM rr WHERE touched ORDER BY id").await,
			value("[]"),
			"{write}"
		);
	}
}

#[tokio::test]
async fn a_read_beneath_a_write_reads_the_callers_view() {
	// A statement the write runs reads under the caller's permissions, not
	// as the write reads its own subjects.
	let nested = format!("(SELECT VALUE secret FROM {DYNAMIC_RANGE})");
	for write in [
		format!("UPDATE rr:[1]..=[1], {DYNAMIC_RANGE} SET seen = {nested} RETURN NONE"),
		format!(
			"UPDATE rr:[1]..=[1], {DYNAMIC_RANGE} SET touched = true
			 WHERE array::len(array::filter({nested}, |$s| $s != NONE)) = 0 RETURN NONE"
		),
	] {
		let db = TestDb::new_with_auth(GUARDED).await;
		let written = values_as(&db, &record_user(), &write).await;
		assert_eq!(written, vec![value("[]")], "{write}");
		assert_eq!(
			last(&db, "SELECT VALUE [id, seen, touched] FROM rr ORDER BY id").await,
			value(if write.contains("seen") {
				"[
					[rr:[1], [NONE, NONE], NONE],
					[rr:[2], NONE, NONE],
					[rr:[5], [NONE, NONE], NONE],
					[rr:[6], NONE, NONE],
					[rr:[7], [NONE, NONE], NONE],
				]"
			} else {
				"[
					[rr:[1], NONE, true],
					[rr:[2], NONE, NONE],
					[rr:[5], NONE, true],
					[rr:[6], NONE, NONE],
					[rr:[7], NONE, true],
				]"
			}),
			"{write}"
		);
	}
}

#[tokio::test]
async fn a_write_reaches_the_records_every_path_to_them_reaches() {
	// The select and update permissions differ, so a path that judged its
	// subjects by the one the other does not would touch other records.
	for permissions in [
		"FOR select NONE FOR update FULL",
		"FOR select WHERE open = true FOR update WHERE kind = 'a'",
		"FOR select FULL FOR update WHERE open = true",
	] {
		let setup = format!(
			"DEFINE ACCESS user ON DATABASE TYPE RECORD;
			 DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
			 DEFINE TABLE rr SCHEMALESS PERMISSIONS {permissions};
			 DEFINE FIELD secret ON rr PERMISSIONS FOR select NONE;
			 CREATE rr:[1] SET open = true, kind = 'a', secret = 's1';
			 CREATE rr:[5] SET open = true, kind = 'b', secret = 's5';
			 CREATE rr:[6] SET open = false, kind = 'a', secret = 's6';
			 CREATE rr:[7] SET open = false, kind = 'b', secret = 's7';"
		);
		// Each pair names the same records, once through an expression range
		// and once written out.
		for targets in [
			[format!("rr:[1]..=[1], {DYNAMIC_RANGE}"), "rr:[1]..=[1], rr:[5]..".to_string()],
			[DYNAMIC_RANGE.to_string(), "rr:[5]..".to_string()],
		] {
			let mut seen = Vec::new();
			for target in targets {
				let db = TestDb::new_with_auth(&setup).await;
				let write = format!("UPDATE {target} SET touched = true RETURN AFTER");
				let written = values_as(&db, &record_user(), &write).await;
				let stored = last(&db, "SELECT id, secret, touched FROM rr ORDER BY id").await;
				seen.push((write, written, stored));
			}
			let [(a, a_written, a_stored), (b, b_written, b_stored)] =
				<[_; 2]>::try_from(seen).expect("two paths");
			assert_eq!(
				(a_written, a_stored),
				(b_written, b_stored),
				"{permissions}: `{a}` / `{b}`"
			);
		}
	}
}

#[tokio::test]
async fn a_limit_over_an_expression_range_reads_no_further() {
	// The range is read as the pipeline takes its rows: under a small
	// quantum, reading every record of it before the limit applies would
	// reach a yield point once a quantum of records.
	const RECORDS: usize = 4000;
	let db = TestDb::new_with_config(
		&format!("FOR $i IN 1..={RECORDS} {{ CREATE type::record('rr', [$i]) }};"),
		surrealdb_cnf::ConfigMap::empty().with_key_value("vm_quantum", "16"),
	)
	.await;
	for source in [DYNAMIC_RANGE, "array::first([rr:[5].., rand::int(1, 1)])"] {
		for (tail, expected) in [
			(" LIMIT 2", "[rr:[5], rr:[6]]"),
			(" START 2 LIMIT 2", "[rr:[7], rr:[8]]"),
			(" WHERE id != rr:[6] LIMIT 2", "[rr:[5], rr:[7]]"),
		] {
			let sql = format!("SELECT VALUE id FROM {source}{tail}");
			super::fuel::INTERRUPT_CHECKS.with(|checks| checks.set(0));
			assert_eq!(last(&db, &sql).await, value(expected), "{sql}");
			let checks = super::fuel::INTERRUPT_CHECKS.with(std::cell::Cell::get);
			assert!(
				checks < 50,
				"`{sql}` reached {checks} yield points over a {RECORDS}-record range"
			);
		}
	}
}

#[tokio::test]
async fn a_table_a_block_defined_after_reading_its_absence_is_read() {
	// `record::exists` reads the table's absence before the block's write
	// defines it; a read that follows the write finds the table.
	let db = TestDb::new(SETUP).await;
	for (read, expected) in [
		(
			"SELECT VALUE id FROM (IF rand::int(1, 1) = 1 { t:[1].. } ELSE { NONE })",
			"[t:[1], t:[2]]",
		),
		("SELECT VALUE id FROM (IF rand::int(1, 1) = 1 { t } ELSE { NONE })", "[t:[1], t:[2]]"),
		("SELECT VALUE id FROM array::first([t:[1].., rand::int(1, 1)])", "[t:[1], t:[2]]"),
		(
			"UPDATE rr:[1]..=[1], (IF rand::int(1, 1) = 1 { t:[1].. } ELSE { NONE }) SET touched = true RETURN VALUE id",
			"[rr:[1], t:[1], t:[2]]",
		),
		(
			"UPDATE rr:[1]..=[1], (IF rand::int(1, 1) = 1 { t } ELSE { NONE }) SET touched = true RETURN VALUE id",
			"[rr:[1], t:[1], t:[2]]",
		),
	] {
		let sql = format!(
			"{{ LET $known = record::exists(t:[1]); CREATE t:[1], t:[2]; RETURN {read} }}; REMOVE TABLE t;"
		);
		let results = values_as(&db, &TestDb::owner(), &sql).await;
		assert_eq!(results.first(), Some(&value(expected)), "{sql}");
	}
}

/// `rr:[1]..=[1]` beside a range and beside a table of the undefined table
/// `nope`, each yielded by an expression and written out.
const UNDEFINED_TARGETS: [(&str, &str); 2] = [
	("(IF rand::int(1, 1) = 1 { nope:[1].. } ELSE { NONE })", "nope:[1].."),
	("(IF rand::int(1, 1) = 1 { nope } ELSE { NONE })", "nope"),
];

#[tokio::test]
async fn a_mixed_write_over_an_undefined_table_answers_as_written_out() {
	// A range or a table of an undefined table that an expression yields
	// answers as the same target written out does, to the owner and to a
	// record user: the statement's answer, the tables the catalog holds after
	// it, and the records it leaves.
	let mut wrong = Vec::new();
	for (who, session) in [("owner", TestDb::owner()), ("record user", record_user())] {
		for write in [
			"UPDATE rr:[1]..=[1], {target} SET touched = true RETURN VALUE id",
			"UPDATE rr:[1]..=[1], {target} SET touched = true WHERE true RETURN VALUE id",
			"DELETE rr:[1]..=[1], {target} RETURN VALUE id",
			"UPSERT rr:[1]..=[1], {target} SET touched = true RETURN VALUE record::tb(id)",
			"UPSERT rr:[1]..=[1], {target} SET touched = true WHERE true RETURN VALUE record::tb(id)",
		] {
			for (expression, written) in UNDEFINED_TARGETS {
				let mut seen = Vec::new();
				for target in [expression, written] {
					let db = TestDb::new_with_auth(GUARDED).await;
					let answer = values_as(&db, &session, &write.replace("{target}", target)).await;
					let tables = last(&db, "RETURN object::keys((INFO FOR DB).tables)").await;
					let records = last(&db, "SELECT VALUE [id, touched] FROM rr ORDER BY id").await;
					seen.push((answer, tables, records));
				}
				if seen[0] != seen[1] {
					wrong.push(format!("{who}: {write} over {expression} and {written}: {seen:?}"));
				}
			}
		}
	}
	assert!(wrong.is_empty(), "{wrong:#?}");
	// What both forms answer, and the records they leave: an `UPDATE` or a
	// `DELETE` naming an undefined table is refused to a caller whose reads
	// are not permission-checked, and writes nothing, not even the targets
	// before the undefined one; to a caller whose reads are checked the
	// absence is hidden, and the other targets are written as they are alone;
	// an `UPSERT` defines the table on demand.
	let (range, table) = (UNDEFINED_TARGETS[0].0, UNDEFINED_TARGETS[1].0);
	let refused = Value::from("The table 'nope' does not exist".to_string());
	for (who, session, write, answer, touched, kept) in [
		(
			"owner",
			TestDb::owner(),
			format!("UPDATE rr:[1]..=[1], {range} SET touched = true RETURN VALUE id"),
			refused.clone(),
			"[]",
			true,
		),
		(
			"owner",
			TestDb::owner(),
			format!("UPDATE rr:[1]..=[1], {table} SET touched = true RETURN VALUE id"),
			refused.clone(),
			"[]",
			true,
		),
		(
			"owner",
			TestDb::owner(),
			format!("DELETE rr:[1]..=[1], {range} RETURN VALUE id"),
			refused.clone(),
			"[]",
			true,
		),
		(
			"owner",
			TestDb::owner(),
			format!("DELETE rr:[1]..=[1], {table} RETURN VALUE id"),
			refused.clone(),
			"[]",
			true,
		),
		(
			"record user",
			record_user(),
			format!("UPDATE rr:[1]..=[1], {range} SET touched = true RETURN VALUE id"),
			value("[rr:[1]]"),
			"[rr:[1]]",
			true,
		),
		(
			"record user",
			record_user(),
			format!("UPDATE rr:[1]..=[1], {table} SET touched = true RETURN VALUE id"),
			value("[rr:[1]]"),
			"[rr:[1]]",
			true,
		),
		// The record user may not delete `rr:[1]`, so its delete is refused too.
		(
			"record user",
			record_user(),
			format!("DELETE rr:[1]..=[1], {range} RETURN VALUE id"),
			value("[]"),
			"[]",
			true,
		),
		(
			"record user",
			record_user(),
			format!("DELETE rr:[1]..=[1], {table} RETURN VALUE id"),
			value("[]"),
			"[]",
			true,
		),
		(
			"owner",
			TestDb::owner(),
			format!("UPSERT rr:[1]..=[1], {table} SET touched = true RETURN VALUE record::tb(id)"),
			value("['rr', 'nope']"),
			"[rr:[1]]",
			true,
		),
	] {
		let db = TestDb::new_with_auth(GUARDED).await;
		assert_eq!(values_as(&db, &session, &write).await, vec![answer], "{who}: {write}");
		assert_eq!(
			last(&db, "SELECT VALUE id FROM rr WHERE touched ORDER BY id").await,
			value(touched),
			"{who}: {write}"
		);
		assert_eq!(
			last(&db, "RETURN record::exists(rr:[1])").await,
			Value::from(kept),
			"{who}: {write}"
		);
	}
}

#[tokio::test]
async fn a_mixed_upsert_of_a_missing_record_writes_it_as_a_written_out_target_does() {
	let mut seen = Vec::new();
	for target in [
		"(IF rand::int(1, 1) = 1 { rr:[999] } ELSE { NONE })",
		"array::first([rr:[999], rand::int(1, 1)])",
		"rr:[999]",
	] {
		let db = TestDb::new(SETUP).await;
		let write = format!("UPSERT rr:[1]..=[1], {target} SET open = true RETURN VALUE id");
		let written = values_as(&db, &TestDb::owner(), &write).await;
		let stored = last(&db, "SELECT VALUE id FROM rr WHERE open ORDER BY id").await;
		seen.push((written, stored));
	}
	assert_eq!(seen[0], (vec![value("[rr:[1], rr:[999]]")], value("[rr:[1], rr:[999]]")));
	assert!(seen.iter().all(|other| *other == seen[0]), "{seen:#?}");
}

#[tokio::test]
async fn a_write_over_an_array_of_records_writes_them_in_order() {
	// Under a fan-out threshold of one the stored records are read
	// overlapped; they are written in the order the array names them, and a
	// missing one is passed over.
	let db = TestDb::new_with_config(
		SETUP,
		surrealdb_cnf::ConfigMap::empty().with_key_value("fan_out_row_threshold", "1"),
	)
	.await;
	let write = "UPDATE rr:[1]..=[1], (IF rand::int(1, 1) = 1 { [rr:[6], rr:[9], { id: rr:[2] }, rr:[5]] } ELSE { NONE })
		SET touched = true RETURN VALUE id";
	assert_eq!(last(&db, write).await, value("[rr:[1], rr:[6], rr:[2], rr:[5]]"));
}

#[tokio::test]
async fn a_write_over_written_out_targets_writes_the_records_as_stored() {
	// Targets that are all written out are read as a union of scans, each a
	// write's subjects read as stored, as a single written-out target's are:
	// the caller's view of a record is never stored back, and the write's own
	// permission judges which records it reaches.
	for (target, touched) in [
		("rr:[1], rr:[5]", "[rr:[1], rr:[5]]"),
		("rr:[1]..=[1], rr:[5]..", "[rr:[1], rr:[5], rr:[7]]"),
		("rr:[1]..=[1], rr", "[rr:[1], rr:[2], rr:[5], rr:[7]]"),
		("rr:[1]..=[1], rr:[6]", "[rr:[1]]"),
	] {
		let db = TestDb::new_with_auth(GUARDED).await;
		let write = format!("UPDATE {target} SET touched = true RETURN NONE");
		values_as(&db, &record_user(), &write).await;
		assert_eq!(
			last(&db, "SELECT VALUE id FROM rr WHERE touched ORDER BY id").await,
			value(touched),
			"{write}"
		);
		assert_eq!(
			last(&db, "SELECT VALUE secret FROM rr ORDER BY id").await,
			value("['s1', 's2', 's5', 's6', 's7']"),
			"{write}"
		);
	}
	// A select permission that refuses every record keeps neither a single
	// target nor several from the records the update permission admits.
	let setup = "DEFINE ACCESS user ON DATABASE TYPE RECORD;
		DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
		DEFINE TABLE rr SCHEMALESS PERMISSIONS FOR select NONE FOR update FULL;
		CREATE rr:[1], rr:[5], rr:[6];";
	for (target, touched) in [
		("rr:[5]..", "[rr:[5], rr:[6]]"),
		("rr:[1]..=[1], rr:[5]..", "[rr:[1], rr:[5], rr:[6]]"),
		("rr", "[rr:[1], rr:[5], rr:[6]]"),
		("rr:[1], rr:[5], rr:[6]", "[rr:[1], rr:[5], rr:[6]]"),
	] {
		let db = TestDb::new_with_auth(setup).await;
		let write = format!("UPDATE {target} SET touched = true");
		values_as(&db, &record_user(), &write).await;
		assert_eq!(
			last(&db, "SELECT VALUE id FROM rr WHERE touched ORDER BY id").await,
			value(touched),
			"{write}"
		);
	}
}

/// A table whose stored records have not passed its migration step.
const MIGRATED: &str = "
	DEFINE TABLE p SCHEMAFULL;
	DEFINE FIELD g ON p TYPE string;
	DEFINE FIELD h ON p TYPE option<int>;
	CREATE p:1 SET g = '1';
	CREATE p:2 SET g = '2';
	CREATE p:3 SET g = '3';
	ALTER FIELD g ON p TYPE int MIGRATE EXISTING ROWS AS g_int USING <int>$value FALLBACK 0;
";

#[tokio::test]
async fn a_write_over_mixed_targets_reads_records_through_their_migration_steps() {
	// A range or a table reached through a value beside a written-out target
	// is read as a write over it reads it: each record brought through the
	// steps it has not passed before the write's data clause reads it.
	for (mixed, alone) in [
		(
			"UPDATE p:3, (IF rand::int(1, 1) = 1 { p:1..=2 } ELSE { NONE }) SET h = g * 10 RETURN AFTER",
			"UPDATE p:3 SET h = g * 10 RETURN AFTER; UPDATE p:1..=2 SET h = g * 10 RETURN AFTER",
		),
		(
			"UPDATE p:3, (IF rand::int(1, 1) = 1 { p } ELSE { NONE }) SET h = g * 10 RETURN AFTER",
			"UPDATE p:3 SET h = g * 10 RETURN AFTER; UPDATE p SET h = g * 10 RETURN AFTER",
		),
	] {
		let db = TestDb::new_with_auth(MIGRATED).await;
		let answer = values_as(&db, &TestDb::owner(), mixed).await;
		let stored = last(&db, "SELECT id, g, h FROM p ORDER BY id").await;
		assert_eq!(
			stored,
			value("[{ g: 1, h: 10, id: p:1 }, { g: 2, h: 20, id: p:2 }, { g: 3, h: 30, id: p:3 }]"),
			"{mixed}: {answer:?}"
		);
		let reference = TestDb::new_with_auth(MIGRATED).await;
		let mut written = Vec::new();
		for rows in values_as(&reference, &TestDb::owner(), alone).await {
			if let Value::Array(rows) = rows {
				written.extend(rows.0);
			} else {
				written.push(rows);
			}
		}
		assert_eq!(answer, vec![Value::from(written)], "{mixed} against {alone}");
	}
}
