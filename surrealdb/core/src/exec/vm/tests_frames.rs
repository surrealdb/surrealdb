//! Computed fields whose bodies are instructions — reading the reduced record
//! as the element they step from, and taking graph and reference lookups in
//! the computed field's frame — answer their values, for the owner and for a
//! record user whose reads are checked.

use super::golden::Golden;
use super::tests::{bind, outcome, planner};
use crate::exec::operators::test_util::TestDb;

const SETUP: &str = "
	DEFINE TABLE person PERMISSIONS FOR select WHERE hidden != true;
	DEFINE TABLE comment PERMISSIONS FOR select WHERE secret != true;
	DEFINE TABLE knows TYPE RELATION PERMISSIONS FULL;
	DEFINE FIELD author ON comment TYPE option<record<person>> REFERENCE;
	DEFINE FIELD pw ON person PERMISSIONS FOR select WHERE $auth.id = user:x;
	DEFINE FIELD comments ON person COMPUTED <~comment;
	DEFINE FIELD authored ON person COMPUTED <~(comment FIELD author);
	DEFINE FIELD friends ON person COMPUTED ->knows->person;
	DEFINE FIELD friend_names ON person COMPUTED ->knows->person.name;
	DEFINE FIELD label ON person COMPUTED string::concat(name, '!');
	DEFINE FIELD doubled ON person COMPUTED n * 2;
	DEFINE FIELD quad ON person COMPUTED doubled * 2;
	DEFINE FIELD sees_pw ON person COMPUTED pw;
	DEFINE FIELD first ON person COMPUTED tags[0] ?? 'none';
	DEFINE FIELD typed ON person TYPE int COMPUTED n + 1;
	CREATE person:1 SET name = 'a', n = 1, pw = 'p1', tags = ['x', 'y'];
	CREATE person:2 SET name = 'b', n = 2, hidden = true;
	CREATE person:3 SET name = 'c', n = 3, pw = 'p3';
	CREATE comment:1 SET author = person:1;
	CREATE comment:2 SET author = person:1, secret = true;
	CREATE comment:3 SET author = person:3;
	RELATE person:1->knows:k1->person:2;
	RELATE person:1->knows:k2->person:3;
";

/// Statements over the computed fields, each of which the VM lowers.
const READS: &[&str] = &[
	"SELECT * FROM person ORDER BY id",
	"SELECT comments, authored, friends, friend_names FROM person ORDER BY id",
	"SELECT label, doubled, quad, typed, first FROM person ORDER BY id",
	"SELECT sees_pw, pw FROM person ORDER BY id",
	"SELECT VALUE comments FROM ONLY person:1",
	"SELECT VALUE friends FROM ONLY person:1",
	"SELECT * FROM person WHERE doubled > 2 ORDER BY id",
	"SELECT id, comments FROM person WHERE array::len(comments) > 0 ORDER BY id",
];

fn reader() -> crate::dbs::Session {
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	crate::dbs::Session::for_record("test", "test", "user", rid)
}

#[tokio::test]
async fn computed_fields_as_instructions_answer_their_values() {
	let mut golden = Golden::new("computed_fields_as_instructions_answer_their_values");
	let db = TestDb::new_with_auth(SETUP).await;
	for (who, session) in [("owner", TestDb::owner()), ("record user", reader())] {
		let ctx = db.exec_ctx_as(&session, crate::kvs::TransactionType::Read).await;
		for sql in READS {
			let bound = bind(&ctx, sql).await;
			let vm = super::plan(&planner(&ctx), &bound)
				.await
				.unwrap_or_else(|| panic!("the VM declined `{sql}` for the {who}"));
			golden.rows(format!("{who}: {sql}"), &outcome(&vm, &ctx).await);
		}
	}
	golden.check();
}
