//! A running program compiles nothing but in a fault
//! ([`crate::exec::compiles`]). Each test runs statements through the
//! datastore, as a session would, and counts the compilations made while a
//! program ran outside a fault: a permission clause read by the code a run
//! reaches — a bucket's, a table's `SELECT` with the computed fields it reads,
//! a field's — is compiled when the statement is planned or served by the
//! compile cache in a fault, never compiled where it is judged.

use crate::exec::operators::test_util::TestDb;

fn record_user(table: &str, id: &str, access: &str) -> crate::dbs::Session {
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new(table, id));
	crate::dbs::Session::for_record("test", "test", access, rid)
}

/// The compilations made while a program ran, outside a fault, running each
/// of `statements` as `session`, which must each answer.
async fn running_compiles(
	db: &TestDb,
	session: &crate::dbs::Session,
	statements: &[&str],
) -> usize {
	let before = crate::exec::compiles::count();
	for sql in statements {
		for response in db.datastore().execute(sql, session, None).await.expect("runs") {
			if let Err(e) = response.result {
				// A refusal is an answer: the permission was judged.
				assert!(e.to_string().contains("permission"), "`{sql}` failed: {e}");
			}
		}
	}
	crate::exec::compiles::count() - before
}

#[tokio::test]
async fn a_bucket_permission_compiles_nothing_while_it_runs() {
	let caps =
		crate::dbs::Capabilities::all().with_experimental(crate::dbs::capabilities::Targets::All);
	let db = TestDb::new_with_capabilities(
		"
		DEFINE ACCESS user ON DATABASE TYPE RECORD;
		CREATE user:bob;
		DEFINE BUCKET gate BACKEND 'memory'
			PERMISSIONS WHERE $action = 'get' AND $file = f\"gate:/allowed.txt\";
		f\"gate:/allowed.txt\".put('ALLOWED');
		f\"gate:/denied.txt\".put('DENIED');
		",
		true,
		caps,
	)
	.await;
	let bob = record_user("user", "bob", "user");
	let statements = [
		"file::get(f\"gate:/allowed.txt\").?.to_string()",
		"file::get(f\"gate:/denied.txt\").?.to_string()",
	];
	// The first run fills the compile cache in faults; neither run compiles
	// outside one.
	assert_eq!(running_compiles(&db, &bob, &statements).await, 0, "a cold node");
	assert_eq!(running_compiles(&db, &bob, &statements).await, 0, "a warm node");
}

#[tokio::test]
async fn table_and_field_permissions_compile_nothing_while_they_run() {
	let db = TestDb::new_with_auth(
		"
		DEFINE ACCESS user ON DATABASE TYPE RECORD;
		DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
		CREATE user:alice;
		DEFINE TABLE doc SCHEMALESS PERMISSIONS FOR select WHERE hidden != true;
		DEFINE FIELD hidden ON doc COMPUTED status = 'secret';
		CREATE doc:pub SET status = 'public';
		CREATE doc:sec SET status = 'secret';
		DEFINE TABLE holder SCHEMALESS PERMISSIONS FULL;
		CREATE holder:1 SET doc = doc:sec;
		DEFINE TABLE scored SCHEMALESS PERMISSIONS FOR select FULL;
		DEFINE FIELD owner ON scored TYPE record<user>;
		DEFINE FIELD score ON scored TYPE int PERMISSIONS FOR select WHERE owner = $auth.id;
		CREATE scored:1 SET owner = user:alice, score = 10;
		CREATE scored:2 SET owner = user:alice, score = 40;
		DEFINE FUNCTION fn::tb() { RETURN 'scored'; } PERMISSIONS FULL;
		",
	)
	.await;
	let alice = record_user("user", "alice", "user");
	let statements = [
		"SELECT VALUE id FROM doc ORDER BY id",
		"SELECT VALUE id FROM doc:sec",
		"SELECT doc.status AS d FROM holder:1",
		"RETURN record::exists(doc:sec)",
		"SELECT VALUE score FROM type::table(fn::tb()) ORDER BY score DESC LIMIT 1",
	];
	for sql in statements {
		assert_eq!(running_compiles(&db, &alice, &[sql]).await, 0, "`{sql}` on a cold node");
	}
	assert_eq!(running_compiles(&db, &alice, &statements).await, 0, "a warm node");
}

#[tokio::test]
async fn a_generated_id_create_compiles_nothing_while_it_runs() {
	let db = TestDb::new(
		"
		DEFINE TABLE free SCHEMALESS;
		DEFINE TABLE full SCHEMAFULL;
		DEFINE FIELD score ON full TYPE option<int> ASSERT $value = NONE OR $value >= 0;
		DEFINE FIELD name ON full TYPE option<string> DEFAULT 'anon';
		DEFINE INDEX by_score ON full FIELDS score;
		",
	)
	.await;
	let owner = crate::dbs::Session::owner().with_ns("test").with_db("test");
	let statements = [
		"CREATE |free:50|",
		"CREATE |free:50| SET score = rand::int(0, 100) RETURN NONE",
		"CREATE |full:50| RETURN NONE",
		"CREATE |full:50| SET score = rand::int(0, 100)",
		"CREATE |made:50| SET score = rand::int(0, 100) RETURN NONE",
	];
	for sql in statements {
		assert_eq!(running_compiles(&db, &owner, &[sql]).await, 0, "`{sql}` on a cold node");
	}
	assert_eq!(running_compiles(&db, &owner, &statements).await, 0, "a warm node");
}
