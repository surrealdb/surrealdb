//! Sources the VM covers although their table's declarations are enforced at
//! run time rather than bound as nodes of the plan, and the semi and anti
//! joins the logical rules lift out of a filter or a table's permission. Each
//! statement answers what the operator tree answers — the same rows in the
//! same order, the same errors — for the owner and for a record user whose
//! reads are checked, and none is declined.

use super::tests::{bind, outcome, planner, without_generated_ids};
use crate::exec::ExecutionContext;
use crate::exec::operators::test_util::TestDb;
use crate::val::Value;

const SETUP: &str = "
	DEFINE TABLE person PERMISSIONS FULL;
	DEFINE FIELD active ON person TYPE bool;
	CREATE person:a SET active = true, name = 'a';
	CREATE person:b SET active = false, name = 'b';
	CREATE person:c SET active = true, name = 'c';

	DEFINE TABLE post PERMISSIONS FOR select WHERE author IN (SELECT VALUE id FROM person WHERE active = true);
	CREATE post:1 SET author = person:a, n = 1;
	CREATE post:2 SET author = person:b, n = 2;
	CREATE post:3 SET author = person:c, n = 3;
	CREATE post:4 SET author = person:a, n = 4;

	DEFINE TABLE gone PERMISSIONS FOR select WHERE author NOT IN (SELECT VALUE id FROM person WHERE active = false);
	CREATE gone:1 SET author = person:a;
	CREATE gone:2 SET author = person:b;

	DEFINE TABLE t PERMISSIONS FULL;
	DEFINE FIELD items.*.denied ON t PERMISSIONS FOR select NONE;
	DEFINE TABLE u PERMISSIONS FULL;
	DEFINE FIELD arr[*] ON u PERMISSIONS FOR select WHERE $value != 2;
	CREATE t:1 SET items = [{ shown: 1, denied: 'a' }, { shown: 2, denied: 'b' }], k = 1;
	CREATE t:2 SET items = [{ shown: 3 }], k = 2;
	CREATE u:1 SET arr = [1, 2, 3, 4], k = 1;
	CREATE u:2 SET arr = [2, 2], k = 2;

	DEFINE TABLE a PERMISSIONS FULL;
	DEFINE TABLE b PERMISSIONS FULL;
	CREATE a:1 SET k = 1, x = 5;
	CREATE a:2 SET k = 2, x = 6;
	CREATE a:3 SET k = 3, x = 7;
	CREATE b:1 SET k = 1, y = 10;
	CREATE b:2 SET k = 3, y = 20;
	CREATE b:3 SET k = 3, y = 5;

	DEFINE TABLE pts PERMISSIONS FULL;
	DEFINE FIELD vec ON pts COMPUTED [<float> n, 0f];
	FOR $i IN 0..6 { CREATE type::record('pts', $i) SET n = $i };
";

const READS: &[&str] = &[
	// A table permission that holds a membership, lifted into a join.
	"SELECT * FROM post ORDER BY n",
	"SELECT VALUE n FROM post WHERE n > 1 ORDER BY n",
	"SELECT * FROM gone ORDER BY id",
	"SELECT count() FROM post GROUP ALL",
	// A membership in the statement's own filter.
	"SELECT * FROM a WHERE k IN (SELECT VALUE k FROM b) ORDER BY id",
	"SELECT * FROM a WHERE k NOT IN (SELECT VALUE k FROM b) ORDER BY id",
	"SELECT * FROM a WHERE k IN (SELECT VALUE k FROM b WHERE y > 10) ORDER BY id",
	"SELECT * FROM a WHERE x > 5 AND k IN (SELECT VALUE k FROM b) ORDER BY id",
	// An element-level field permission the plan does not bind.
	"SELECT * FROM t ORDER BY id",
	"SELECT items FROM t:1",
	"SELECT VALUE items FROM t:1",
	"SELECT * FROM ONLY u:1",
	"SELECT VALUE arr FROM u ORDER BY id",
	"SELECT * FROM t, u ORDER BY id",
	// A brute-force KNN ranking a computed vector.
	"SELECT id FROM pts WHERE vec <|3,EUCLIDEAN|> [0f, 0f]",
	"SELECT id FROM pts WHERE vec <|3,EUCLIDEAN|> [0f, 0f] AND n > 0",
];

const WRITES: &[&str] = &[
	"UPDATE t:1 SET items = [{ shown: 9, denied: 'w' }] RETURN VALUE items",
	"UPDATE t:1 SET k = 5 RETURN AFTER",
	"UPDATE t:1 SET k = 5 RETURN BEFORE",
	"UPDATE t:1 SET k = 5 RETURN DIFF",
	"UPDATE t SET k = k + 1 WHERE items[0].denied = 'a' RETURN id, k",
	"UPDATE u:1 SET k = 3 RETURN AFTER",
	"CREATE t:9 SET items = [{ shown: 4, denied: 'x' }, { shown: 5 }] RETURN AFTER",
	"DELETE u:2 RETURN BEFORE",
];

fn reader() -> crate::dbs::Session {
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	crate::dbs::Session::for_record("test", "test", "user", rid)
}

/// `sql` run on a fresh database as `session`, on the VM or the operator tree.
async fn run(sql: &str, session: &crate::dbs::Session, on_vm: bool) -> Result<Vec<Value>, String> {
	let db = TestDb::new_with_auth(SETUP).await;
	let ctx: ExecutionContext = db.exec_ctx_as(session, crate::kvs::TransactionType::Write).await;
	let bound = bind(&ctx, sql).await;
	let op = if on_vm {
		super::plan(&planner(&ctx), &bound)
			.await
			.unwrap_or_else(|| panic!("the VM declined `{sql}`"))
	} else {
		planner(&ctx).plan_logical(bound).await.expect("the operator tree plans")
	};
	outcome(&op, &ctx).await.map(|rows| rows.into_iter().map(without_generated_ids).collect())
}

#[tokio::test]
async fn enforced_sources_and_lifted_joins_answer_what_the_operator_tree_answers() {
	for (who, session) in [("owner", TestDb::owner()), ("record user", reader())] {
		for sql in READS.iter().chain(WRITES) {
			let vm = run(sql, &session, true).await;
			let tree = run(sql, &session, false).await;
			assert_eq!(vm, tree, "`{sql}` for the {who}");
		}
	}
}

#[tokio::test]
async fn a_record_user_is_shown_what_the_field_permissions_allow() {
	let session = reader();
	let rows = run("SELECT * FROM ONLY t:1", &session, true).await.expect("reads");
	let [row] = rows.as_slice() else {
		panic!("one row expected, got {rows:?}");
	};
	assert!(!format!("{row:?}").contains("denied"), "a denied key was shown: {row:?}");
}
