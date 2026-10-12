//! Tests of the observer stages run as instructions: each statement runs on
//! the VM, for the owner and for a checked record user, and its answer, the
//! keyspace it leaves — records, change feed entries and queued events — and
//! the notifications its commit sends are the recorded ones.

use super::golden::{Golden, render_changes, render_outcome};
use super::tests::{bind, outcome, planned, planner, without_fresh_uuids, without_versionstamp};
use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
use crate::exec::operators::test_util::TestDb;
use crate::val::Value;

/// A table whose writes every observer sees: synchronous events reading each of
/// the frame's parameters and a field the reader may not select, an `ASYNC`
/// event, an event that fails, one that returns, one that writes back to its
/// own table, a change feed, and an aggregated view.
pub(super) const OBSERVED_SCHEMA: &str = "
	DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
	CREATE user:reader;
	DEFINE TABLE item PERMISSIONS FULL CHANGEFEED 1d;
	DEFINE FIELD secret ON item PERMISSIONS FOR select WHERE $auth.id = user:x;
	DEFINE TABLE log PERMISSIONS FULL;
	DEFINE EVENT audit ON item THEN (
		CREATE log SET item = $value.id, ev = $event, before = $before.n, after = $after.n,
			secret = $after.secret, input = $input
	);
	DEFINE EVENT big ON item WHEN $after.n > 20 THEN (CREATE log SET big = $after.n), (CREATE log SET again = true);
	DEFINE EVENT gone ON item WHEN $event = 'DELETE' THEN (CREATE log SET gone = $before.id);
	DEFINE EVENT late ON item ASYNC WHEN $event = 'UPDATE' THEN (CREATE log SET late = true);
	DEFINE EVENT boom ON item WHEN $after.n = 13 THEN { THROW 'unlucky' };
	DEFINE EVENT ret ON item WHEN $after.n = 4 THEN { RETURN 1 };
	DEFINE EVENT back ON item WHEN $after.n = 7 THEN (UPDATE item:1 SET n = 8);
	DEFINE EVENT peek ON item WHEN $after.n = 5 THEN (
		CREATE log SET peeked = (SELECT VALUE secret FROM ONLY $after.id)
	);
	DEFINE TABLE item_count AS SELECT count() AS c, math::max(n) AS top FROM item GROUP ALL;
	CREATE item:1 SET n = 1, secret = 's1';
	CREATE item:2 SET n = 2, secret = 's2';
";

/// Writes to [`OBSERVED_SCHEMA`]'s table that fire, skip and fail its events.
pub(super) const OBSERVED_WRITES: &[&str] = &[
	"CREATE item:3 SET n = 3",
	"UPDATE item:1 SET n += 1",
	"UPDATE item SET n = 21",
	"UPDATE item:1 SET n = 1",
	"UPDATE item:2 SET n = 7",
	"UPDATE item:1 SET n = 13",
	"UPDATE item:1 SET n = 5",
	"UPDATE item:2 SET n = 4 RETURN AFTER",
	"DELETE item:1",
	"DELETE item",
	"INSERT INTO item { id: item:9, n: 9 }",
	"UPSERT item:8 SET n = 22",
	"UPDATE item:1 MERGE { m: 1 } RETURN NONE",
	"RELATE item:1->link->item:2 SET w = 1",
	"UPDATE type::table('item') SET n += 1",
	"UPDATE (SELECT VALUE id FROM item) SET n = 21",
];

type Observed = (Result<Vec<Value>, String>, Vec<(Vec<u8>, Option<Vec<u8>>)>, Vec<String>);

/// Run `sql` against a fresh [`OBSERVED_SCHEMA`] as `session`, with a live query
/// open on the table: its answer, the keyspace its commit changes, and the
/// notifications it sends.
async fn observed(sql: &str, session: &crate::dbs::Session) -> Observed {
	observed_in(OBSERVED_SCHEMA, "item", sql, session).await
}

/// An observed run as a golden body.
fn render_observed(observed: &Observed) -> String {
	format!(
		"{}\nkeyspace:\n{}notified:\n{}",
		render_outcome(&observed.0),
		render_changes(&observed.1),
		observed.2.join("\n")
	)
}

/// [`observed`] against `schema`, with the live query open on `table`.
async fn observed_in(
	schema: &str,
	table: &str,
	sql: &str,
	session: &crate::dbs::Session,
) -> Observed {
	observed_with(&[(schema, TestDb::owner())], table, sql, session).await
}

/// [`observed_in`] against a schema set up by `setup`, each part run as the
/// session beside it, in order.
async fn observed_with(
	setup: &[(&str, crate::dbs::Session)],
	table: &str,
	sql: &str,
	session: &crate::dbs::Session,
) -> Observed {
	let (db, notifications) = TestDb::quiescent_with_notifications("").await;
	for (schema, definer) in setup {
		let mut responses =
			db.datastore().execute(schema, definer, None).await.expect("the schema runs");
		for response in responses.drain(..) {
			response.result.expect("the schema sets up");
		}
	}
	let watcher = TestDb::owner().with_rt(true);
	let mut responses = db
		.datastore()
		.execute(&format!("LIVE SELECT * FROM {table}"), &watcher, None)
		.await
		.expect("the live query runs");
	responses.remove(0).result.expect("the live query opens");
	while notifications.try_recv().is_ok() {}
	let before = keyspace(&db).await;
	let ctx = db.exec_ctx_with_broker(session, crate::kvs::TransactionType::Write).await;
	let bound = bind(&ctx, sql).await;
	let op = planned(&planner(&ctx), &bound, sql).await;
	let rows = outcome(&op, &ctx).await;
	let txn = ctx.txn();
	if rows.is_ok() {
		txn.commit().await.expect("commit");
	} else {
		txn.cancel().await.expect("cancel");
	}
	let after = keyspace(&db).await;
	let node = *db.datastore().id().as_bytes();
	let queue = crate::key::schema::EventQueuePrefix {}.range().expect("the queue range builds");
	let mut changed: Vec<_> = changed(&before, &after)
		.into_iter()
		.map(|(key, value)| {
			// A queued event's key orders the queue by the clock and names the
			// node that wrote it, which differ between any two runs.
			if key.as_slice() >= queue.start().as_slice() && key.as_slice() < queue.end().as_slice()
			{
				return (b"<queued event>".to_vec(), value.map(|v| queued(&v)));
			}
			// A catalog version carries the print of a definition that holds
			// fresh uuids; the revision and the version before it compare.
			let version = key.len() > 14 && matches!(&key[11..14], b"!cv" | b"!ov");
			let value = value.map(|v| {
				if version {
					v[..v.len().min(17)].to_vec()
				} else {
					v
				}
			});
			(without(&without_versionstamp(&key), &node), value.map(|v| without(&v, &node)))
		})
		.collect();
	changed.sort();
	let mut sent = Vec::new();
	while let Ok(notification) = notifications.try_recv() {
		// A generated record key differs between any two runs.
		let mut what = format!("{:?} {:?}", notification.action, notification.result).into_bytes();
		mask_generated(&mut what);
		sent.push(String::from_utf8(what).expect("masking keeps the text"));
	}
	(rows.map(|rows| rows.into_iter().map(without_fresh_ids).collect()), changed, sent)
}

/// A queued event's entry, decoded, with the parameters it carries in name
/// order: they are held in a map whose order differs between any two runs.
fn queued(bytes: &[u8]) -> Vec<u8> {
	use crate::key::KVValue;
	let record =
		surrealdb_datastore::values::event_queue::AsyncEventRecord::kv_decode_value(bytes, ())
			.expect("a queued event decodes");
	let mut values: Vec<_> =
		record.values.iter().map(|(name, value)| format!("{name}={value:?}")).collect();
	values.sort();
	format!(
		"{} {} {:?} {:?} {} {} {} {} {} {:?} {:?}",
		record.attempt,
		record.event_depth,
		record.rid,
		record.cursor_record,
		record.fields_computed,
		record.ns,
		record.db,
		record.perms,
		record.auth_enabled,
		values,
		record.event_definition,
	)
	.into_bytes()
}

async fn keyspace(db: &TestDb) -> Vec<(Vec<u8>, Vec<u8>)> {
	let read = db
		.datastore()
		.transaction(crate::kvs::TransactionType::Read)
		.await
		.expect("a read transaction opens");
	let all = read
		.getr_raw(surrealdb_kvs::key::RawRange::every_key(), None)
		.await
		.expect("the keyspace reads");
	read.cancel().await.expect("the read transaction ends");
	all
}

/// The entries between `before` and `after` that were added, changed or
/// removed, a generated record id's key standing as one placeholder.
fn changed(
	before: &[(Vec<u8>, Vec<u8>)],
	after: &[(Vec<u8>, Vec<u8>)],
) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
	let mut out = Vec::new();
	for (key, value) in after {
		match before.iter().find(|(k, _)| k == key) {
			Some((_, old)) if old == value => {}
			_ => out.push((key.clone(), Some(value.clone()))),
		}
	}
	for (key, _) in before {
		if !after.iter().any(|(k, _)| k == key) {
			out.push((key.clone(), None));
		}
	}
	out
}

/// `bytes` with the node id zeroed, and fresh uuids and generated record keys
/// masked: all differ between two datastores.
fn without(bytes: &[u8], node: &[u8; 16]) -> Vec<u8> {
	let mut out = without_fresh_uuids(bytes);
	let mut i = 0;
	while i + 16 <= out.len() {
		if out[i..i + 16] == *node {
			out[i..i + 16].fill(0);
			i += 16;
		} else {
			i += 1;
		}
	}
	// A generated key is 20 alphanumerics; a change feed row's key carries a
	// versionstamp after `#`. Both are masked by length-preserving runs.
	mask_generated(&mut out);
	out
}

/// Replace each run of 20 lowercase alphanumerics — a generated record key —
/// with `x`s, so two runs that generated different keys compare alike.
fn mask_generated(bytes: &mut [u8]) {
	let mut start = None;
	for i in 0..=bytes.len() {
		let alnum = i < bytes.len() && (bytes[i].is_ascii_lowercase() || bytes[i].is_ascii_digit());
		match (alnum, start) {
			(true, None) => start = Some(i),
			(false, Some(s)) => {
				// A generated key may hold no digit at all.
				if i - s == 20 {
					bytes[s..i].fill(b'x');
				}
				start = None;
			}
			_ => {}
		}
	}
}

fn without_fresh_ids(value: Value) -> Value {
	match value {
		Value::RecordId(mut rid) => {
			if let crate::val::RecordIdKey::String(key) = &rid.key
				&& key.len() == 20
			{
				rid.key = crate::val::RecordIdKey::String("generated".into());
			}
			Value::RecordId(rid)
		}
		Value::Array(array) => {
			Value::Array(array.into_iter().map(without_fresh_ids).collect::<Vec<_>>().into())
		}
		Value::Object(object) => {
			Value::Object(object.into_iter().map(|(k, v)| (k, without_fresh_ids(v))).collect())
		}
		other => other,
	}
}

/// Whether the program lowered for `sql` runs the table's events and its
/// change feed as instructions.
async fn observes_by_instructions(sql: &str, session: &crate::dbs::Session) -> bool {
	use super::program::{Instr, WriteInstr};
	use super::write::ObserveInstr;
	let db = TestDb::new_with_auth(OBSERVED_SCHEMA).await;
	let ctx = db.exec_ctx_as(session, crate::kvs::TransactionType::Write).await;
	let bound = bind(&ctx, sql).await;
	let physical =
		super::optimize::optimize(super::map::map(&planner(&ctx), &bound).await.expect("maps"));
	let program = super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
	let program = super::tests::with_stages(&planner(&ctx), &program).await;
	ctx.txn().cancel().await.expect("cancel");
	let mut stack = vec![&program];
	let (mut events, mut changes) = (false, false);
	while let Some(program) = stack.pop() {
		stack.extend(program.units.iter().map(|unit| &**unit));
		for instr in program.pipelines.iter().flat_map(|pipeline| &pipeline.code) {
			match instr {
				Instr::Write(WriteInstr::Observe(ObserveInstr::Events {
					..
				})) => events = true,
				Instr::Write(WriteInstr::Observe(ObserveInstr::RecordChange)) => changes = true,
				_ => {}
			}
		}
	}
	events && changes
}

/// A write runs its table's events and appends to its change feed — for the
/// owner, and for a record user the events run for with permissions off — with
/// both observers run as instructions.
#[tokio::test]
async fn a_write_is_observed_by_its_events_and_change_feed() {
	let mut golden = Golden::new("a_write_is_observed_by_its_events_and_change_feed");
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	let reader = crate::dbs::Session::for_record("test", "test", "user", rid);
	for (who, session) in [("owner", TestDb::owner()), ("record user", reader)] {
		for sql in OBSERVED_WRITES {
			let vm = observed(sql, &session).await;
			golden.record(format!("{who}: {sql}"), render_observed(&vm));
			// `RELATE` writes the edge table, which has no events.
			if !sql.starts_with("RELATE") {
				assert!(
					observes_by_instructions(sql, &session).await,
					"`{sql}` for the {who} runs its observers by no instruction"
				);
			}
		}
	}
	golden.check();
}

/// The events run on the write path: a firing event's body writes, a failing
/// one fails the statement under the event's name, and a firing `ASYNC` event
/// is queued rather than run.
#[tokio::test]
async fn events_fire_as_their_definitions_say() {
	let owner = TestDb::owner();
	let (rows, changed, _) = observed("UPDATE item:1 SET n = 21", &owner).await;
	assert!(rows.is_ok(), "{rows:?}");
	let logged = changed.iter().filter(|(k, _)| k.windows(3).any(|w| w == b"log")).count();
	assert!(logged >= 3, "audit, big and again each create a log row: {logged}");
	assert!(
		changed.iter().any(|(k, _)| k == b"<queued event>"),
		"the ASYNC event is queued on an update"
	);
	let (rows, _, _) = observed("UPDATE item:1 SET n = 13", &owner).await;
	let message = rows.expect_err("the event throws");
	assert!(message.contains("Error while processing event boom"), "{message}");
}

/// An event's body runs with permissions off whoever wrote the record: a record
/// user's write fires an event whose statement reads a field that user may not
/// select, and the statement reads it.
#[tokio::test]
async fn an_event_reads_what_the_writer_may_not() {
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	let reader = crate::dbs::Session::for_record("test", "test", "user", rid);
	let (rows, changed, _) = observed("UPDATE item:1 SET n = 5", &reader).await;
	assert!(rows.is_ok(), "{rows:?}");
	let peeked = changed.iter().filter_map(|(_, value)| value.as_deref()).any(|value| {
		value.windows(6).any(|w| w == b"peeked") && value.windows(2).any(|w| w == b"s1")
	});
	assert!(peeked, "the event's statement read the guarded field");
}

/// An event whose clause writes back to its own table runs that write's events
/// by the id of the unit being lowered: the arena holds a unit of the table's
/// events that a unit lowered beneath it names again, and no more than one
/// such unit for each frame the events are lowered in.
#[tokio::test]
async fn events_writing_back_to_their_table_close_on_their_unit() {
	use super::program::{Instr, WriteInstr};
	use super::write::ObserveInstr;
	let db = TestDb::new_with_auth(OBSERVED_SCHEMA).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	let bound = bind(&ctx, "UPDATE item:2 SET n = 7").await;
	let physical =
		super::optimize::optimize(super::map::map(&planner(&ctx), &bound).await.expect("maps"));
	let program = super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
	ctx.txn().cancel().await.expect("cancel");
	let named = |unit: &super::program::Program| -> Vec<u32> {
		unit.pipelines
			.iter()
			.flat_map(|pipeline| &pipeline.code)
			.filter_map(|instr| match instr {
				Instr::Write(WriteInstr::Observe(ObserveInstr::Events {
					unit,
					..
				})) => Some(unit.0),
				_ => None,
			})
			.collect()
	};
	let item_events: Vec<u32> = program
		.units
		.iter()
		.enumerate()
		.filter(|(_, unit)| {
			unit.events.as_ref().is_some_and(|lowered| {
				lowered.events.iter().any(|event| event.target_table.as_str() == "item")
			})
		})
		.map(|(at, _)| at as u32)
		.collect();
	assert!(!item_events.is_empty(), "the table's events are no unit of the arena");
	assert!(
		item_events.len() <= 6,
		"the events were lowered again for every write: {item_events:?}"
	);
	let closed = item_events.iter().any(|&id| {
		program.units.iter().skip(id as usize + 1).any(|unit| named(unit).contains(&id))
	});
	assert!(closed, "no write beneath the table's events names their unit again");
}

/// A table whose events take a record through every action in turn — a create
/// updates the record, an update deletes it, a delete creates its successor —
/// until the successor's count reaches three.
const CYCLING_SCHEMA: &str = "
	DEFINE TABLE t PERMISSIONS FULL;
	DEFINE EVENT on_create ON t WHEN $event = 'CREATE' AND $after.n < 3 THEN (UPDATE $after.id SET n += 1);
	DEFINE EVENT on_update ON t WHEN $event = 'UPDATE' THEN (DELETE $after.id);
	DEFINE EVENT on_delete ON t WHEN $event = 'DELETE' THEN (CREATE type::record('t', $before.n) SET n = $before.n);
";

/// The write that starts [`CYCLING_SCHEMA`]'s chain.
const CYCLING_WRITE: &str = "CREATE t:start SET n = 0";

/// Events that cycle through every action of their table close on one unit
/// for each action and frame, and run on the VM to the record the chain settles
/// on.
#[tokio::test]
async fn events_cycling_through_every_action_close_on_their_units() {
	let owner = TestDb::owner();
	let vm = observed_in(CYCLING_SCHEMA, "t", CYCLING_WRITE, &owner).await;
	assert!(vm.0.is_ok(), "{:?}", vm.0);
	let mut golden = Golden::new("events_cycling_through_every_action_close_on_their_units");
	golden.record(CYCLING_WRITE, render_observed(&vm));
	golden.check();

	let db = TestDb::new_with_auth(CYCLING_SCHEMA).await;
	let ctx = db.exec_ctx_as(&owner, crate::kvs::TransactionType::Write).await;
	let bound = bind(&ctx, CYCLING_WRITE).await;
	let physical =
		super::optimize::optimize(super::map::map(&planner(&ctx), &bound).await.expect("maps"));
	let program = super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
	let program = super::tests::with_stages(&planner(&ctx), &program).await;
	ctx.txn().cancel().await.expect("cancel");
	// The units of the program and of the bundles of the stages it loads by
	// name, each holding the units it runs.
	let mut units = Vec::new();
	let mut stack: Vec<&super::program::Program> = vec![&program];
	while let Some(program) = stack.pop() {
		for unit in program.units.iter() {
			units.push(unit);
			stack.push(unit);
		}
	}
	let mut actions: Vec<String> = units
		.iter()
		.filter_map(|unit| unit.events.as_ref())
		.filter(|lowered| lowered.events.iter().any(|event| event.target_table.as_str() == "t"))
		.map(|lowered| format!("{:?}", lowered.action))
		.collect();
	actions.sort();
	actions.dedup();
	assert_eq!(actions, ["Create", "Delete", "Update"], "the chain's events are units");
}

/// A chain of event frames that holds [`OPEN_EVENT_UNITS`] lowerings of one
/// table's events for one action open is refused at lowering, so the statement
/// is declined whole, and the same chain within the bound settles on one
/// record.
///
/// [`OPEN_EVENT_UNITS`]: super::lower::OPEN_EVENT_UNITS
#[tokio::test]
async fn a_chain_of_event_frames_past_the_bound_is_declined() {
	use super::lower::OPEN_EVENT_UNITS;
	async fn run(
		past_the_bound: bool,
	) -> Option<(Result<Vec<Value>, String>, Result<Vec<Value>, String>)> {
		let db = TestDb::new_with_auth(CYCLING_SCHEMA).await;
		let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
		let bound = bind(&ctx, CYCLING_WRITE).await;
		let planner = planner(&ctx);
		let (ns, db_id) = planner.ns_db_ids().await.expect("the database exists");
		// The lowerings of the table's create events a chain beneath the
		// statement holds open.
		let held: Vec<_> = if past_the_bound {
			(0..OPEN_EVENT_UNITS)
				.map(|_| {
					planner
						.cycle_guard
						.try_enter_events(
							ns,
							db_id,
							crate::val::TableName::new("t"),
							crate::doc::Action::Create,
							OPEN_EVENT_UNITS,
						)
						.expect("within the bound")
				})
				.collect()
		} else {
			Vec::new()
		};
		let physical =
			super::optimize::optimize(super::map::map(&planner, &bound).await.expect("maps"));
		let lowered = super::lower::lower(&planner, &physical).await;
		if past_the_bound {
			let refused = lowered.expect_err("the lowering refuses the chain");
			assert!(refused.reason.contains("never settle"), "{}", refused.reason);
			assert!(super::plan(&planner, &bound).await.is_none(), "the VM ran the statement");
			drop(held);
			ctx.txn().cancel().await.expect("cancel");
			return None;
		}
		assert!(lowered.is_ok(), "the chain is refused within the bound");
		let op = super::plan(&planner, &bound).await.expect("the VM runs the statement");
		drop(held);
		let rows = outcome(&op, &ctx).await;
		let after = bind(&ctx, "SELECT * FROM t ORDER BY id").await;
		let read = super::plan(&planner, &after).await.expect("the VM plans the read");
		let table = outcome(&read, &ctx).await;
		ctx.txn().cancel().await.expect("cancel");
		Some((rows, table))
	}
	assert!(run(true).await.is_none(), "the VM ran a chain past the bound");
	let (rows, table) = run(false).await.expect("the chain within the bound runs");
	assert!(rows.is_ok(), "{rows:?}");
	let mut golden = Golden::new("a_chain_of_event_frames_past_the_bound_is_declined");
	golden.rows(CYCLING_WRITE, &rows);
	golden.rows("SELECT * FROM t ORDER BY id", &table);
	golden.check();
	let table = table.expect("the table reads");
	assert_eq!(table.len(), 1, "the chain settles on one record: {table:?}");
}

/// An event's clauses are bytecode of the write's program, and the statements
/// they run are programs of the VM's own.
#[tokio::test]
async fn an_events_clauses_are_bytecode() {
	use super::program::{Instr, WriteInstr};
	use super::write::ObserveInstr;
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	let reader = crate::dbs::Session::for_record("test", "test", "user", rid);
	for session in [TestDb::owner(), reader] {
		let db = TestDb::new_with_auth(OBSERVED_SCHEMA).await;
		let ctx = db.exec_ctx_as(&session, crate::kvs::TransactionType::Write).await;
		let bound = bind(&ctx, "UPDATE item:1 SET n += 1").await;
		let physical =
			super::optimize::optimize(super::map::map(&planner(&ctx), &bound).await.expect("maps"));
		let program = super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
		ctx.txn().cancel().await.expect("cancel");
		let unit = program
			.pipelines
			.iter()
			.flat_map(|pipeline| &pipeline.code)
			.find_map(|instr| match instr {
				Instr::Write(WriteInstr::Observe(ObserveInstr::Events {
					unit,
					..
				})) => Some(*unit),
				_ => None,
			})
			.expect("the events run as instructions");
		let holder = &program.units[unit.0 as usize];
		assert!(holder.events.is_some(), "the events unit holds no events");
		for spec in &holder.subqueries {
			let unit = program
				.units
				.get(spec.unit.0 as usize)
				.expect("an event's statement is a unit of the program");
			assert!(unit.unit.is_some(), "an event's statement is not a program of the VM's own");
		}
	}
}

/// A table whose events call user-defined functions, build and invoke
/// closures, and read other tables with statements in their conditions and
/// bodies, in expressions of every shape.
pub(super) const CLAUSED_SCHEMA: &str = "
	DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
	CREATE user:reader;
	DEFINE FUNCTION fn::bump($n: int) { RETURN $n + 1; };
	DEFINE FUNCTION fn::tag($s: string) { RETURN string::uppercase($s); };
	DEFINE TABLE stock PERMISSIONS FULL;
	DEFINE TABLE note PERMISSIONS FULL;
	DEFINE TABLE trail PERMISSIONS FULL;
	CREATE note:1 SET text = 'a', rank = 1;
	CREATE note:2 SET text = 'b', rank = 2;
	DEFINE EVENT udf ON stock WHEN fn::bump($after.n) > 3 THEN (
		CREATE trail SET kind = 'udf', n = fn::bump($after.n), tag = fn::tag($event)
	);
	DEFINE EVENT mapped ON stock WHEN $event != 'DELETE' THEN (
		CREATE trail SET kind = 'mapped', doubled = [1, 2, 3].map(|$x| $x * $after.n)
	);
	DEFINE EVENT folded ON stock WHEN $event = 'UPDATE' THEN {
		LET $f = |$a, $b| $a + $b + $after.n;
		CREATE trail SET kind = 'folded', total = $f(1, 2);
	};
	DEFINE EVENT peek ON stock WHEN (SELECT VALUE count() FROM note GROUP ALL)[0] >= 2 AND $after.n != 9 THEN {
		LET $top = (SELECT VALUE rank FROM note ORDER BY rank DESC LIMIT 1)[0];
		CREATE trail SET kind = 'peek', top = $top, seen = (SELECT VALUE text FROM note WHERE rank <= $after.n);
	};
	DEFINE EVENT deleted ON stock WHEN $event = 'DELETE' THEN (
		CREATE trail SET kind = 'deleted', was = $before.n, doubled = fn::bump($before.n) * 2
	);
	DEFINE EVENT later ON stock ASYNC WHEN $event = 'UPDATE' AND fn::bump($after.n) > 5 THEN (
		CREATE trail SET kind = 'later'
	);
";

/// Writes to [`CLAUSED_SCHEMA`]'s table that fire and skip its events.
pub(super) const CLAUSED_WRITES: &[&str] = &[
	"CREATE stock:1 SET n = 1",
	"CREATE stock:2 SET n = 9",
	"CREATE stock:3 SET n = 3",
	"UPDATE stock:3 SET n = 5",
	"UPDATE stock:3 SET n = 7",
	"UPDATE stock SET n = n + 1",
	"DELETE stock:3",
	"DELETE stock",
	"INSERT INTO stock { id: stock:4, n: 4 }",
	"UPSERT stock:5 SET n = 5",
	"FOR $i IN 1..=3 { CREATE stock SET n = $i }",
	"{ DEFINE EVENT extra ON stock THEN (CREATE trail SET kind = 'extra'); CREATE stock:6 SET n = 6; }",
	"{ CREATE stock:7 SET n = 7; REMOVE EVENT udf ON stock; UPDATE stock:7 SET n = 8; }",
];

/// The events of [`CLAUSED_SCHEMA`] run for the owner and for a checked record
/// user, each clause lowered from its logical form with the event's own
/// planner: user-defined functions, closures and statements inside expressions
/// are instructions and units of the write's program.
#[tokio::test]
async fn events_with_functions_closures_and_statements_run_as_instructions() {
	let mut golden =
		Golden::new("events_with_functions_closures_and_statements_run_as_instructions");
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	let reader = crate::dbs::Session::for_record("test", "test", "user", rid);
	for (who, session) in [("owner", TestDb::owner()), ("record user", reader)] {
		for sql in CLAUSED_WRITES {
			let vm = observed_in(CLAUSED_SCHEMA, "stock", sql, &session).await;
			golden.record(format!("{who}: {sql}"), render_observed(&vm));
		}
	}
	golden.check();
}

/// Events defined by a principal narrower than the owner run under the
/// limit of that principal: a statement and a user-defined function in their
/// clauses are lowered for the limited principal the event's frame runs as.
#[tokio::test]
async fn events_defined_by_a_narrower_principal_run_under_its_limit() {
	let mut golden = Golden::new("events_defined_by_a_narrower_principal_run_under_its_limit");
	let tables = "
		DEFINE TABLE stock PERMISSIONS FULL;
		DEFINE TABLE note PERMISSIONS FULL;
		DEFINE TABLE trail PERMISSIONS FULL;
		CREATE note:1 SET rank = 1;
		DEFINE FUNCTION fn::bump($n: int) { RETURN $n + 1; };
	";
	let events = "
		DEFINE EVENT limited ON stock THEN (
			CREATE trail SET kind = 'limited', n = fn::bump($after.n),
				seen = (SELECT VALUE rank FROM note)
		);
		DEFINE EVENT guarded ON stock WHEN fn::bump($after.n) > 2 AND (SELECT VALUE count() FROM note GROUP ALL)[0] > 0 THEN (
			CREATE trail SET kind = 'guarded'
		);
	";
	let editor = crate::dbs::Session::editor().with_ns("test").with_db("test");
	let setup = [(tables, TestDb::owner()), (events, editor)];
	for sql in ["CREATE stock:1 SET n = 1", "CREATE stock:2 SET n = 5", "UPDATE stock:2 SET n = 6"]
	{
		let vm = observed_with(&setup, "stock", sql, &TestDb::owner()).await;
		assert!(vm.0.is_ok(), "`{sql}`: {:?}", vm.0);
		golden.record(sql, render_observed(&vm));
	}
	golden.check();
}

/// The event clauses of [`CLAUSED_SCHEMA`], which call functions, build and
/// invoke closures and run statements, are bytecode: the write lowers with
/// its events as instructions of its program.
#[tokio::test]
async fn event_clauses_with_functions_closures_and_statements_are_bytecode() {
	let db = TestDb::new_with_auth(CLAUSED_SCHEMA).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	let bound = bind(&ctx, "UPDATE stock:1 SET n += 1").await;
	let physical =
		super::optimize::optimize(super::map::map(&planner(&ctx), &bound).await.expect("maps"));
	let program = super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
	ctx.txn().cancel().await.expect("cancel");
	let mut events = 0;
	let mut stack = vec![&program];
	while let Some(program) = stack.pop() {
		stack.extend(program.units.iter().map(|unit| &**unit));
		events += usize::from(program.events.is_some());
	}
	assert!(events > 0, "the events run as instructions");
}

/// A table whose events are redefined between the lowering of a write and its
/// run fails the write rather than running the events it was lowered for.
#[tokio::test]
async fn a_write_lowered_before_an_event_is_defined_fails_closed() {
	let db = TestDb::new_with_auth(CLAUSED_SCHEMA).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	let bound = bind(&ctx, "CREATE stock:20 SET n = 20").await;
	let op = super::plan(&planner(&ctx), &bound).await.expect("the VM plans the write");
	let define =
		bind(&ctx, "DEFINE EVENT late ON stock THEN (CREATE trail SET kind = 'late')").await;
	let define = super::plan(&planner(&ctx), &define).await.expect("the VM plans the definition");
	outcome(&define, &ctx).await.expect("the event is defined");
	let failure = outcome(&op, &ctx).await.expect_err("the write is not run against other events");
	assert!(failure.contains("changed after the write was lowered"), "{failure}");
	ctx.txn().cancel().await.expect("cancel");
}

/// Tables whose records live subscriptions watch: a table permission hiding
/// some records, a field only one user may select, a `COMPUTED` field, a record
/// link, an event writing to a watched table, a view over the table, a
/// referencing table whose records a delete cascades to, and an edge table.
pub(super) const LIVES_SCHEMA: &str = "
	DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
	CREATE user:reader, user:x;
	DEFINE TABLE item SCHEMALESS PERMISSIONS FOR select WHERE hidden != true,
		FOR create, update, delete FULL;
	DEFINE FIELD secret ON item PERMISSIONS FOR select WHERE $auth.id = user:x;
	DEFINE FIELD double ON item COMPUTED n * 2;
	DEFINE FIELD owner ON item TYPE option<record<user>>;
	DEFINE TABLE log PERMISSIONS FULL;
	DEFINE EVENT audit ON item WHEN $event != 'DELETE' THEN (CREATE log SET item = $value.id, n = $after.n);
	DEFINE TABLE item_count AS SELECT count() AS c, math::max(n) AS top FROM item GROUP ALL;
	DEFINE TABLE comment PERMISSIONS FULL;
	DEFINE FIELD on_item ON comment TYPE option<record<item>> REFERENCE ON DELETE CASCADE;
	DEFINE TABLE link TYPE RELATION PERMISSIONS FULL;
	CREATE item:1 SET n = 1, secret = 's1', owner = user:reader;
	CREATE item:2 SET n = 2, secret = 's2', hidden = true;
	CREATE comment:1 SET on_item = item:1, text = 'hi';
";

/// The subscriptions each subscriber opens.
const LIVES: &[&str] = &[
	"LIVE SELECT * FROM item",
	"LIVE SELECT DIFF FROM item",
	"LIVE SELECT * FROM item WHERE n > 2",
	"LIVE SELECT n, double, secret, $before.n AS was, $event AS ev FROM item",
	"LIVE SELECT VALUE n FROM item",
	"LIVE SELECT * FROM item FETCH owner",
	"LIVE SELECT * FROM item WHERE $after.secret = 's1'",
	"LIVE SELECT id, n FROM item WHERE (SELECT VALUE n FROM ONLY item:1) >= 0",
	"LIVE SELECT * FROM item WHERE n = 13 OR string::len(<string> n) > 5",
	"LIVE SELECT * FROM log",
	"LIVE SELECT * FROM item_count",
	"LIVE SELECT * FROM comment",
	"LIVE SELECT * FROM link",
];

/// Writes that create, update, reveal, hide and delete the watched records,
/// directly and through events, views and cascades.
pub(super) const LIVE_WRITES: &[&str] = &[
	"CREATE item:3 SET n = 3, secret = 's3'",
	"UPDATE item:1 SET n += 1",
	"UPDATE item:2 SET hidden = false",
	"UPDATE item:1 SET hidden = true",
	"UPSERT item:5 SET n = 5",
	"UPSERT item:1 SET n = 6",
	"DELETE item:1",
	"DELETE item",
	"INSERT INTO item { id: item:9, n: 9, secret: 's9' }",
	"INSERT INTO item [{ id: item:1, n: 10 }] ON DUPLICATE KEY UPDATE n = 11",
	"RELATE item:1->link->item:2 SET w = 1",
	"UPDATE item SET n = 4",
	"UPDATE type::table('item') SET n += 1",
	"UPDATE item:1 MERGE { owner: user:x }",
	"UPDATE item:1 SET n = 13",
	"CREATE item:7 SET n = 7, hidden = true",
	"FOR $i IN [20, 21, 22] { UPDATE item:1 SET n = $i }",
];

/// Blocks whose later writes notify subscriptions under definitions the block
/// changed: a field's permission opened and closed, the table's permission,
/// and a `COMPUTED` body. The table's new permission hides from a checked
/// subscriber only the image the block's last write leaves, never the image
/// a notification is the counterpart of: a checked subscriber is told of a
/// record whose counterpart it may not see.
pub(super) const LIVE_REDEFINING_WRITES: &[&str] = &[
	"{ UPDATE item:1 SET n = 20; DEFINE FIELD OVERWRITE secret ON item PERMISSIONS FULL; UPDATE item:1 SET n = 21; }",
	"{ UPDATE item:1 SET n = 20; DEFINE FIELD OVERWRITE secret ON item PERMISSIONS NONE; UPDATE item:1 SET n = 21; }",
	"{ UPDATE item:1 SET n = 20; DEFINE TABLE OVERWRITE item SCHEMALESS PERMISSIONS FOR select WHERE n != 19, FOR create, update, delete FULL; UPDATE item:1 SET n = 21; UPDATE item:1 SET n = 19; }",
	"{ UPDATE item:1 SET n = 20; DEFINE FIELD OVERWRITE double ON item COMPUTED n * 3; UPDATE item:1 SET n = 21; }",
];

/// The sessions subscriptions are opened as, and writes are made as: the
/// owner, a record user who may not select `secret`, and one who may.
fn live_sessions() -> Vec<(&'static str, crate::dbs::Session)> {
	let record = |key: &str| {
		let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", key));
		crate::dbs::Session::for_record("test", "test", "user", rid).with_rt(true)
	};
	vec![("owner", TestDb::owner().with_rt(true)), ("reader", record("reader")), ("x", record("x"))]
}

/// Run `sql` as `writer` against a fresh [`LIVES_SCHEMA`] with every one of
/// [`LIVES`] open for every one of [`live_sessions`]: its answer and the
/// notifications its commit sends, each naming its subscription by the order it
/// was opened, in that order.
async fn live_notified(
	sql: &str,
	writer: &crate::dbs::Session,
) -> (Result<Vec<Value>, String>, Vec<(usize, String)>) {
	live_notified_in(LIVES_SCHEMA, LIVES, sql, writer).await
}

/// [`live_notified`] against `schema`, with each of `lives` open for every one of
/// [`live_sessions`].
async fn live_notified_in(
	schema: &str,
	lives: &[&str],
	sql: &str,
	writer: &crate::dbs::Session,
) -> (Result<Vec<Value>, String>, Vec<(usize, String)>) {
	let (db, notifications) = TestDb::new_with_notifications("").await;
	{
		let owner = TestDb::owner();
		let mut responses =
			db.datastore().execute(schema, &owner, None).await.expect("the schema runs");
		for response in responses.drain(..) {
			response.result.expect("the schema sets up");
		}
	}
	let mut ids = Vec::new();
	for (_, session) in live_sessions() {
		for live in lives {
			let mut responses =
				db.datastore().execute(live, &session, None).await.expect("the live query runs");
			match responses.remove(0).result.expect("the live query opens") {
				crate::types::PublicValue::Uuid(id) => ids.push(id),
				other => panic!("`{live}` answered {other:?}"),
			}
		}
	}
	while notifications.try_recv().is_ok() {}
	let rows = {
		let ctx = db.exec_ctx_with_broker(writer, crate::kvs::TransactionType::Write).await;
		let bound = bind(&ctx, sql).await;
		let op = planned(&planner(&ctx), &bound, sql).await;
		let rows = outcome(&op, &ctx).await;
		let txn = ctx.txn();
		if rows.is_ok() {
			txn.commit().await.expect("commit");
		} else {
			txn.cancel().await.expect("cancel");
		}
		rows
	};
	let mut sent = Vec::new();
	while let Ok(notification) = notifications.try_recv() {
		let live = ids.iter().position(|id| *id == notification.id).unwrap_or(usize::MAX);
		// A generated record key differs between any two runs.
		let mut what = format!(
			"{:?} {:?} {:?}",
			notification.action, notification.record, notification.result
		)
		.into_bytes();
		mask_generated(&mut what);
		sent.push((live, String::from_utf8(what).expect("masking keeps the text")));
	}
	// Subscriptions are notified in the order the catalog lists them, which
	// is their random ids' order: within one subscription the order is the
	// write's.
	sent.sort_by_key(|(live, _)| *live);
	(rows.map(|rows| rows.into_iter().map(without_fresh_ids).collect()), sent)
}

/// Every subscription is told its records, actions, projections, diffs and
/// fetched links, for writes made by the owner and by record users, to
/// subscriptions opened by each, with the table permission, the field
/// permission and the `WHERE` deciding.
#[tokio::test]
async fn subscriptions_are_notified_of_the_writes_they_watch() {
	let mut golden = Golden::new("subscriptions_are_notified_of_the_writes_they_watch");
	let sessions = live_sessions();
	for (who, writer) in &sessions[..2] {
		for sql in LIVE_WRITES {
			super::write::LIVES_LOWERED.with(|runs| runs.set(0));
			super::write::LIVES_DECLINED.with(|runs| runs.set(0));
			let vm = live_notified(sql, writer).await;
			let (lowered, unlowered) = (
				super::write::LIVES_LOWERED.with(std::cell::Cell::get),
				super::write::LIVES_DECLINED.with(std::cell::Cell::get),
			);
			golden.record(
				format!("{who}: {sql}"),
				format!("{}\nnotified:\n{}", render_outcome(&vm.0), listed(&vm.1)),
			);
			if vm.0.is_ok() {
				assert!(!vm.1.is_empty(), "`{sql}` by the {who} notified nobody");
				assert!(lowered > 0, "`{sql}` by the {who} ran no lowered subscription");
			}
			assert_eq!(
				unlowered, 0,
				"`{sql}` by the {who} evaluated a subscription without a unit"
			);
		}
	}
	golden.check();
}

/// A write in a block the owner runs notifies subscriptions under the
/// definitions the block changed before it: a field opened to the subscriber
/// is shown, one closed to it is cut, and the table's permission and a
/// `COMPUTED` body are the redefined ones.
#[tokio::test]
async fn subscriptions_are_notified_under_the_definitions_a_block_changed() {
	let mut golden =
		Golden::new("subscriptions_are_notified_under_the_definitions_a_block_changed");
	let sessions = live_sessions();
	for (who, writer) in &sessions[..1] {
		for sql in LIVE_REDEFINING_WRITES {
			super::write::LIVES_DECLINED.with(|runs| runs.set(0));
			let vm = live_notified(sql, writer).await;
			let unlowered = super::write::LIVES_DECLINED.with(std::cell::Cell::get);
			assert!(vm.0.is_ok(), "`{sql}` by the {who}: {:?}", vm.0);
			assert!(!vm.1.is_empty(), "`{sql}` by the {who} notified nobody");
			assert_eq!(
				unlowered, 0,
				"`{sql}` by the {who} evaluated a subscription without a unit"
			);
			golden.record(
				format!("{who}: {sql}"),
				format!("{}\nnotified:\n{}", render_outcome(&vm.0), listed(&vm.1)),
			);
		}
	}
	golden.check();
}

fn listed(sent: &[(usize, String)]) -> String {
	sent.iter().map(|(live, what)| format!("{live}: {what}")).collect::<Vec<_>>().join("\n  ")
}

/// A write to records subscriptions watch compiles nothing while it runs: each
/// subscription's unit was compiled as its statement arrived, or, for a table
/// whose catalog changed since, when the write was planned, and the write's
/// context looks it up.
#[tokio::test]
async fn a_write_to_watched_records_compiles_nothing() {
	for (sql, redefined) in [
		"CREATE item:3 SET n = 3, secret = 's3'",
		"UPDATE item:1 SET n += 1",
		"UPDATE item SET n = 4",
		// A table no view reads and no record references: a delete of an
		// `item` recalculates the view over it, and cascades, whose work is
		// not the subscriptions'.
		"DELETE comment:1",
		"UPSERT item:5 SET n = 5",
		"INSERT INTO item { id: item:9, n: 9 }",
	]
	.into_iter()
	.flat_map(|sql| [(sql, false), (sql, true)])
	{
		let (db, notifications) = TestDb::new_with_notifications("").await;
		let owner = TestDb::owner();
		for response in db.datastore().execute(LIVES_SCHEMA, &owner, None).await.unwrap() {
			response.result.expect("the schema sets up");
		}
		for (_, session) in live_sessions() {
			for live in LIVES {
				let mut responses = db.datastore().execute(live, &session, None).await.unwrap();
				responses.remove(0).result.expect("the live query opens");
			}
		}
		if redefined {
			// The subscriptions' tables' catalogs change after they registered.
			db.run(
				"DEFINE FIELD OVERWRITE double ON item COMPUTED n * 5; DEFINE FIELD note ON comment",
			)
			.await;
		}
		while notifications.try_recv().is_ok() {}
		let ctx = db.exec_ctx_with_broker(&owner, crate::kvs::TransactionType::Write).await;
		let bound = bind(&ctx, sql).await;
		super::live::COMPILED.with(|compiled| compiled.set(0));
		let op = super::plan(&planner(&ctx), &bound)
			.await
			.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
		// The units were compiled as each LIVE arrived, for the catalog as it
		// was; a changed catalog's are compiled as the write is planned.
		assert_eq!(
			super::live::COMPILED.with(std::cell::Cell::get) > 0,
			redefined,
			"`{sql}` planned units it should not have, or none it should, redefined: {redefined}"
		);
		// A subscription's condition that reads a table whose declarations
		// the plan does not bind resolves them on its first read, and the
		// compile cache keeps them per class of principal: resolve `item`'s
		// for the owner and for each subscriber before the run is counted.
		let item = crate::val::TableName::from("item");
		let mut warm = vec![owner.clone()];
		warm.extend(live_sessions().into_iter().map(|(_, session)| session));
		for session in &warm {
			let reader = db.exec_ctx_as(session, crate::kvs::TransactionType::Read).await;
			for checked in [false, true] {
				crate::exec::operators::scan::pipeline::build_field_state(
					&reader, &item, checked, None,
				)
				.await
				.expect("the field state resolves");
			}
			crate::exec::operators::fetch::table_select_permission(&reader, &item)
				.await
				.expect("the permission resolves");
			// The permission derives the computed fields it reads in a frame
			// whose reads are not checked, and resolves them for that frame.
			crate::exec::operators::scan::pipeline::build_field_state(
				&reader.with_perms_suppressed(true),
				&item,
				false,
				None,
			)
			.await
			.expect("the derivation's field state resolves");
			reader.txn().cancel().await.expect("cancel");
		}
		// The stored clauses the subscriptions' frames evaluate run as units
		// the compile cache keeps per frame, compiled on their first
		// evaluation: a write cancelled before it commits evaluates them
		// once.
		for response in
			db.datastore().execute(&format!("BEGIN; {sql}; CANCEL;"), &owner, None).await.unwrap()
		{
			let _ = response.result;
		}
		while notifications.try_recv().is_ok() {}
		crate::exec::planner::COMPILES.with(|compiles| compiles.set(0));
		super::lower::LOWERINGS.with(|lowerings| lowerings.set(0));
		super::live::COMPILED.with(|compiled| compiled.set(0));
		super::write::LIVES_LOWERED.with(|runs| runs.set(0));
		super::write::LIVES_DECLINED.with(|runs| runs.set(0));
		let rows = outcome(&op, &ctx).await;
		assert!(rows.is_ok(), "`{sql}`: {rows:?}");
		ctx.txn().commit().await.expect("commit");
		assert_eq!(
			(
				crate::exec::planner::COMPILES.with(std::cell::Cell::get),
				super::lower::LOWERINGS.with(std::cell::Cell::get),
				super::live::COMPILED.with(std::cell::Cell::get),
			),
			(0, 0, 0),
			"`{sql}` compiled while it ran (planner, lowering, units), redefined: {redefined}"
		);
		assert!(
			super::write::LIVES_LOWERED.with(std::cell::Cell::get) > 0,
			"`{sql}` notified no subscription by its unit"
		);
		assert_eq!(
			super::write::LIVES_DECLINED.with(std::cell::Cell::get),
			0,
			"`{sql}` notified a subscription with no unit, redefined: {redefined}"
		);
		assert!(notifications.try_recv().is_ok(), "`{sql}` sent nothing");
	}
}

/// A write to a table no subscription watches runs nothing of the
/// subscriptions' work, which its program holds as an instruction.
#[tokio::test]
async fn a_write_nobody_watches_notifies_nobody() {
	let db = TestDb::new_with_auth(LIVES_SCHEMA).await;
	for sql in LIVE_WRITES {
		let ctx =
			db.exec_ctx_with_broker(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
		let bound = bind(&ctx, sql).await;
		let physical =
			super::optimize::optimize(super::map::map(&planner(&ctx), &bound).await.expect("maps"));
		let program = super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
		if program.write.is_some() {
			assert!(
				super::tests::write_families(
					&super::tests::with_stages(&planner(&ctx), &program).await
				)
				.contains(&"observe"),
				"`{sql}` notifies the subscriptions by no instruction"
			);
		}
		super::write::LIVES_RUN.with(|runs| runs.set(0));
		let op = super::plan(&planner(&ctx), &bound)
			.await
			.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
		let _ = outcome(&op, &ctx).await;
		ctx.txn().cancel().await.expect("cancel");
		assert_eq!(
			super::write::LIVES_RUN.with(std::cell::Cell::get),
			0,
			"`{sql}` did the subscriptions' work with no subscription open"
		);
	}
}

/// The base schema of the census of LIVE shapes: `item` with a field only
/// `user:x` may select, a computed field, a relation to itself, a function, a
/// table gated on its records, and a view.
const CENSUS_SCHEMA: &str = "
	DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
	CREATE user:reader;
	CREATE user:x;
	DEFINE TABLE item PERMISSIONS FULL;
	DEFINE FIELD secret ON item PERMISSIONS FOR select WHERE $auth.id = user:x;
	DEFINE FIELD double ON item COMPUTED n * 2;
	DEFINE FIELD owner ON item TYPE option<record<user>>;
	DEFINE FUNCTION fn::big($n: int) { RETURN $n > 2; };
	DEFINE TABLE link TYPE RELATION FROM item TO item PERMISSIONS FULL;
	DEFINE TABLE gated PERMISSIONS FOR select WHERE n > 1, FOR create, update, delete FULL;
	DEFINE TABLE item_count AS SELECT count() AS c, math::max(n) AS top FROM item GROUP ALL;
	CREATE item:1 SET n = 1, secret = 's1', owner = user:reader, meta = { a: { b: 'deep' } };
	CREATE item:2 SET n = 2, secret = 's2', owner = user:x;
	CREATE item:3 SET n = 3, secret = 's3';
	RELATE item:1->link->item:2;
	CREATE gated:1 SET n = 2;
";

/// Each LIVE shape of the census, with the table whose subscriptions it is.
const CENSUS_SHAPES: &[(&str, &str)] = &[
	("item", "LIVE SELECT * FROM item"),
	("item", "LIVE SELECT DIFF FROM item"),
	("item", "LIVE SELECT VALUE n FROM item"),
	("item", "LIVE SELECT n AS renamed, secret FROM item"),
	("item", "LIVE SELECT meta.a.b AS deep, n FROM item"),
	("item", "LIVE SELECT *, n * 2 AS dbl, owner.id AS who FROM item"),
	("item", "LIVE SELECT *, $event AS ev, $value.n AS vn, $auth.id AS me FROM item"),
	("item", "LIVE SELECT * FROM item WHERE $before.n != $after.n"),
	("item", "LIVE SELECT * FROM item WHERE $auth.id = user:x"),
	("item", "LIVE SELECT * FROM item WHERE n IN (SELECT VALUE n FROM item)"),
	("item", "LIVE SELECT * FROM item WHERE string::len(<string> n) > 0"),
	("item", "LIVE SELECT * FROM item WHERE fn::big(n)"),
	("item", "LIVE SELECT ->link->item.n AS friends FROM item"),
	("item", "LIVE SELECT * FROM item FETCH owner"),
	("item", "LIVE SELECT * FROM item FETCH owner, secret"),
	("item", "LIVE SELECT id, double FROM item WHERE double > 2"),
	("item", "LIVE SELECT secret, meta FROM item"),
	("item", "LIVE SELECT * FROM item WHERE (SELECT VALUE n FROM ONLY item:1) >= 0"),
	("gated", "LIVE SELECT * FROM gated"),
	("item_count", "LIVE SELECT * FROM item_count"),
	("link", "LIVE SELECT * FROM link"),
];

/// The writes the census runs under each shape's subscriptions.
const CENSUS_WRITES: &[&str] = &[
	"CREATE item:9 SET n = 3, secret = 's9', owner = user:reader",
	"UPDATE item:1 SET n += 1, meta.a.b = 'shallow'",
	"UPDATE item:2 SET n = 1, meta = { a: { b: 'x' } } RETURN NONE",
	"DELETE item:2",
	"UPDATE gated:1 SET n = 3",
	"CREATE gated:2 SET n = 0",
	"RELATE item:2->link->item:3 SET w = 1",
];

/// Every LIVE shape a subscriber may open lowers to a unit: opened as the owner
/// and by each record user, every subscription its table holds that the
/// subscriber is served compiles, and the refusal of each one that does not is
/// reported.
#[tokio::test]
async fn every_live_shape_lowers_to_a_unit() {
	let mut refusals = Vec::new();
	for (table, shape) in CENSUS_SHAPES {
		let (db, _notifications) = TestDb::new_with_notifications("").await;
		for response in db.datastore().execute(CENSUS_SCHEMA, &TestDb::owner(), None).await.unwrap()
		{
			response.result.expect("the schema sets up");
		}
		for (who, session) in live_sessions() {
			let mut responses = db.datastore().execute(shape, &session, None).await.unwrap();
			if let Err(error) = responses.remove(0).result {
				refusals.push(format!("`{shape}` as {who}: registration failed: {error}"));
			}
		}
		let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
		let txn = ctx.txn();
		let ns = txn.expect_ns_by_name("test").await.expect("namespace test").namespace_id;
		let database = txn.expect_db_by_name("test", "test").await.expect("database test");
		let table = crate::val::TableName::from(*table);
		let lives = txn.all_tb_lives(ns, database.database_id, &table, None).await.unwrap();
		for subscription in lives.iter() {
			if crate::exec::operators::write::stages::lives::subscriber(subscription).is_none() {
				continue;
			}
			if let Err(refused) =
				super::lower_live::compile(ctx.ctx(), &txn, "test", "test", &table, subscription)
					.await
			{
				refusals.push(format!("`{shape}` ({}): {}", subscription.id, refused.reason));
			}
		}
		txn.cancel().await.expect("cancel");
	}
	assert!(refusals.is_empty(), "LIVE shapes that do not lower:\n{}", refusals.join("\n"));
}

/// Each census shape's subscribers are notified by their units of every census
/// write, by the owner and by a record user: the answer each write gives and the
/// notifications each subscription receives, against the golden.
#[tokio::test]
async fn every_live_shape_notifies_its_subscribers() {
	let mut golden = Golden::new("every_live_shape_notifies_its_subscribers");
	let mut delivered = 0;
	for (_, shape) in CENSUS_SHAPES {
		for sql in CENSUS_WRITES {
			for (who, writer) in &live_sessions()[..2] {
				super::write::LIVES_DECLINED.with(|runs| runs.set(0));
				let (rows, sent) = live_notified_in(CENSUS_SCHEMA, &[*shape], sql, writer).await;
				assert_eq!(
					super::write::LIVES_DECLINED.with(std::cell::Cell::get),
					0,
					"`{shape}` by `{sql}` as {who} has a subscription with no unit"
				);
				golden.record(
					format!("{who}: {shape} | {sql}"),
					format!("{}\nnotified:\n  {}", render_outcome(&rows), listed(&sent)),
				);
				delivered += sent.len();
			}
		}
	}
	assert!(delivered > 0, "the census delivered no notification");
	golden.check();
}

/// A write whose table only its data names runs with the table's subscriptions
/// compiled in a fault, when the node holds no unit for them at the table's
/// catalog version: the write itself compiles nothing, and its subscribers
/// are still notified.
#[tokio::test]
async fn a_table_only_the_data_names_compiles_its_units_in_a_fault() {
	let (db, notifications) = TestDb::new_with_notifications("").await;
	let owner = TestDb::owner();
	for response in db.datastore().execute(LIVES_SCHEMA, &owner, None).await.unwrap() {
		response.result.expect("the schema sets up");
	}
	for (_, session) in live_sessions() {
		for live in LIVES {
			let mut responses = db.datastore().execute(live, &session, None).await.unwrap();
			responses.remove(0).result.expect("the live query opens");
		}
	}
	// The table's catalog changes after its subscriptions registered, so the
	// node holds their units for the old version only, and no write plans them.
	db.run("DEFINE FIELD OVERWRITE double ON item COMPUTED n * 5").await;
	while notifications.try_recv().is_ok() {}
	let sql = "UPDATE type::table('item') SET n = 4";
	let ctx = db.exec_ctx_with_broker(&owner, crate::kvs::TransactionType::Write).await;
	let bound = bind(&ctx, sql).await;
	let op = planned(&planner(&ctx), &bound, sql).await;
	super::live::COMPILED.with(|compiled| compiled.set(0));
	let compiles = crate::exec::compiles::count();
	let faults = crate::exec::compiles::faults();
	let rows = outcome(&op, &ctx).await;
	assert!(rows.is_ok(), "`{sql}`: {rows:?}");
	ctx.txn().commit().await.expect("commit");
	assert!(
		crate::exec::compiles::faults() > faults,
		"`{sql}` compiled its subscriptions outside a fault"
	);
	assert_eq!(crate::exec::compiles::count(), compiles, "`{sql}` compiled while it ran");
	assert!(notifications.try_recv().is_ok(), "`{sql}` sent nothing");
}

/// A context the datastore did not build has no node cache. Its write contexts
/// still hold one entry for each subscription: the unit of each one it serves,
/// compiled for that context and kept by nothing.
#[tokio::test]
async fn units_are_compiled_for_a_context_with_no_node_cache() {
	let db = TestDb::new_with_auth(LIVES_SCHEMA).await;
	for (_, session) in live_sessions() {
		for live in LIVES {
			let mut responses = db.datastore().execute(live, &session, None).await.unwrap();
			responses.remove(0).result.expect("the live query opens");
		}
	}
	let txn = std::sync::Arc::new(
		db.datastore().transaction(crate::kvs::TransactionType::Read).await.unwrap(),
	);
	let mut context = crate::ctx::Context::new_test();
	context.set_transaction(std::sync::Arc::clone(&txn));
	let frozen = context.freeze();
	let namespace = txn.expect_ns_by_name("test").await.expect("namespace test");
	let database = txn.expect_db_by_name("test", "test").await.expect("database test");
	let table = crate::val::TableName::from("item");
	let lives =
		txn.all_tb_lives(namespace.namespace_id, database.database_id, &table, None).await.unwrap();
	assert!(!lives.is_empty(), "the census subscriptions are open");
	let units = super::live::units_for(&frozen, &namespace, &database, &table, &lives)
		.await
		.expect("the units resolve");
	assert_eq!(units.len(), lives.len(), "one entry for each subscription");
	for (subscription, unit) in lives.iter().zip(units.iter()) {
		assert_eq!(
			unit.is_some(),
			crate::exec::operators::write::stages::lives::subscriber(subscription).is_some(),
			"`{}` is served a unit exactly when it is served",
			subscription.id,
		);
	}
}

/// Reset the counters of units compiled.
fn reset_compiled() {
	super::live::COMPILED.with(|compiled| compiled.set(0));
	super::live::COMPILED_WRITABLE.with(|compiled| compiled.set(0));
}

/// A bare `LIVE` statement compiles its unit as it arrives, in a read
/// transaction, and none in the write transaction that registers it. A `LIVE`
/// in a transaction block compiles nothing as it registers: its unit is
/// compiled as the first write to its table is planned.
#[tokio::test]
async fn a_bare_live_compiles_its_unit_before_its_transaction() {
	let (db, _notifications) = TestDb::new_with_notifications("").await;
	let owner = TestDb::owner();
	db.run(LIVES_SCHEMA).await;
	let (_, reader) = live_sessions().remove(1);

	reset_compiled();
	let mut responses =
		db.datastore().execute("LIVE SELECT * FROM item", &reader, None).await.unwrap();
	responses.remove(0).result.expect("the live query opens");
	assert!(
		super::live::COMPILED.with(std::cell::Cell::get) >= 1,
		"a bare LIVE compiled no unit as it arrived"
	);
	assert_eq!(
		super::live::COMPILED_WRITABLE.with(std::cell::Cell::get),
		0,
		"a bare LIVE compiled in a write transaction"
	);

	reset_compiled();
	let mut responses = db
		.datastore()
		.execute("BEGIN; LIVE SELECT * FROM item; COMMIT;", &reader, None)
		.await
		.unwrap();
	responses.remove(0).result.expect("the live query opens in a block");
	assert_eq!(
		super::live::COMPILED.with(std::cell::Cell::get),
		0,
		"a LIVE in a block compiled as it registered"
	);

	let ctx = db.exec_ctx_with_broker(&owner, crate::kvs::TransactionType::Write).await;
	let sql = "CREATE item:3 SET n = 3";
	let bound = bind(&ctx, sql).await;
	reset_compiled();
	let _op = planned(&planner(&ctx), &bound, sql).await;
	assert_eq!(
		super::live::COMPILED.with(std::cell::Cell::get),
		1,
		"planning compiled the in-block unit and no other"
	);
	ctx.txn().cancel().await.expect("cancel");
}

/// A subscription whose clauses do not lower fails to register, with the
/// refusal's reason, and leaves nothing registered.
#[tokio::test]
async fn a_live_whose_unit_is_refused_fails_to_register() {
	let (db, _notifications) = TestDb::new_with_notifications("").await;
	db.run(LIVES_SCHEMA).await;
	let owner = TestDb::owner().with_rt(true);

	reset_compiled();
	super::live::REFUSE.with(|refuse| refuse.set(true));
	let mut responses =
		db.datastore().execute("LIVE SELECT * FROM item", &owner, None).await.unwrap();
	super::live::REFUSE.with(|refuse| refuse.set(false));
	let error = responses.remove(0).result.expect_err("the refusal fails the statement");
	assert_eq!(
		error.to_string(),
		"An error occurred: LIVE query cannot be served: its clauses do not compile: refused for a test"
	);
	assert_eq!(super::live::COMPILED_WRITABLE.with(std::cell::Cell::get), 0);

	let txn = db.datastore().transaction(crate::kvs::TransactionType::Read).await.unwrap();
	let namespace = txn.expect_ns_by_name("test").await.expect("namespace test");
	let database = txn.expect_db_by_name("test", "test").await.expect("database test");
	let table = crate::val::TableName::from("item");
	let lives =
		txn.all_tb_lives(namespace.namespace_id, database.database_id, &table, None).await.unwrap();
	assert!(lives.is_empty(), "the refused registration rolled back");
	txn.cancel().await.expect("cancel");
}

/// A subscription registered after a definition its unit read changed between
/// its arrival and its registration is notified by the definition as it is
/// then: a function redefined leaves the held unit stale, and a field
/// redefined leaves none held.
#[tokio::test]
async fn a_live_registered_after_ddl_raced_its_arrival_notifies_by_the_new_catalog() {
	use crate::types::{PublicAction, PublicValue};

	async fn raced(
		schema: &str,
		ddl: &'static str,
		live: &str,
	) -> (TestDb, crate::channel::Receiver<crate::types::PublicNotification>) {
		let (db, notifications) = TestDb::new_with_notifications("").await;
		db.run(schema).await;
		let ds = std::sync::Arc::clone(db.datastore());
		let hook: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
			Box::pin(async move {
				for response in ds.execute(ddl, &TestDb::owner(), None).await.unwrap() {
					response.result.expect("the racing definition applies");
				}
			});
		crate::dbs::executor::AFTER_LIVE_ARRIVAL.with(|slot| *slot.borrow_mut() = Some(hook));
		let owner = TestDb::owner().with_rt(true);
		let mut responses = db.datastore().execute(live, &owner, None).await.unwrap();
		responses.remove(0).result.expect("the live query opens");
		(db, notifications)
	}

	// A function redefined: the table's version is unchanged, so the unit
	// compiled at arrival is held, and the first write finds it stale.
	let (db, notifications) = raced(
		"DEFINE TABLE lv; DEFINE FUNCTION fn::ok() { RETURN false };",
		"DEFINE FUNCTION OVERWRITE fn::ok() { RETURN true }",
		"LIVE SELECT * FROM lv WHERE fn::ok()",
	)
	.await;
	reset_compiled();
	db.run("CREATE lv:1 SET v = 1").await;
	assert!(
		super::live::COMPILED.with(std::cell::Cell::get) > 0,
		"the stale unit was not compiled again"
	);
	let sent = notifications.recv().await.expect("the write notifies");
	assert_eq!(sent.action, PublicAction::Create);
	assert!(notifications.try_recv().is_err(), "one notification");

	// A field redefined: the table's version moved, so nothing is held and
	// the first write compiles the unit against the new definition.
	let (db, notifications) = raced(
		"DEFINE TABLE lv; DEFINE FIELD double ON lv COMPUTED v * 2;",
		"DEFINE FIELD OVERWRITE double ON lv COMPUTED v * 5",
		"LIVE SELECT * FROM lv",
	)
	.await;
	db.run("CREATE lv:1 SET v = 1").await;
	let sent = notifications.recv().await.expect("the write notifies");
	assert_eq!(sent.action, PublicAction::Create);
	let expected: PublicValue = crate::syn::value("{ id: lv:1, v: 1, double: 5 }").unwrap();
	assert_eq!(sent.result, expected);
}

/// Whether the program lowered for `sql` over `schema` derives the table's
/// computed fields onto the images its observers read.
async fn derives_for_observers(schema: &str, sql: &str) -> bool {
	use super::program::{Instr, WriteInstr};
	use super::write::SchemaInstr;
	let db = TestDb::new(schema).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	let bound = bind(&ctx, sql).await;
	let physical =
		super::optimize::optimize(super::map::map(&planner(&ctx), &bound).await.expect("maps"));
	let program = super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
	let program = super::tests::with_stages(&planner(&ctx), &program).await;
	ctx.txn().cancel().await.expect("cancel");
	let mut stack = vec![&program];
	while let Some(program) = stack.pop() {
		stack.extend(program.units.iter().map(|unit| &**unit));
		let derives = program.pipelines.iter().flat_map(|pipeline| &pipeline.code).any(|instr| {
			matches!(instr, Instr::Write(WriteInstr::Schema(SchemaInstr::Observe { .. })))
		});
		if derives {
			return true;
		}
	}
	false
}

/// A table whose `id` names its components derives them for the observers of
/// every form of write though it declares no other computed field, because
/// the components are computed fields themselves; a table that names none and
/// declares none derives nothing, and pays nothing for it.
#[tokio::test]
async fn a_table_with_named_id_components_derives_them_for_its_observers() {
	let keyed = "
		DEFINE TABLE reading SCHEMALESS;
		DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
	";
	for sql in [
		"CREATE reading SET city = 'London', time = d'2024-01-01T00:00:00Z', temp = 1",
		"UPDATE reading SET temp = 2",
		"UPSERT reading SET city = 'London', time = d'2024-01-01T00:00:00Z', temp = 3",
		"INSERT INTO reading { city: 'Rome', time: d'2024-01-01T00:00:00Z', temp: 4 }",
		"DELETE reading",
	] {
		assert!(derives_for_observers(keyed, sql).await, "`{sql}` derives no component");
	}
	let plain = "DEFINE TABLE plain SCHEMALESS;";
	for sql in [
		"CREATE plain SET n = 1",
		"UPDATE plain SET n = 2",
		"INSERT INTO plain { n: 3 }",
		"DELETE plain",
	] {
		assert!(!derives_for_observers(plain, sql).await, "`{sql}` derives for a table with none");
	}
}

/// A subscription to a table whose `id` names its components is notified of
/// them as fields of the record, in whole and under a projection, and its
/// `WHERE` reads them: the record the notification carries is the one the
/// subscriber would read, though no record stores a component.
#[tokio::test]
async fn subscriptions_are_notified_of_named_id_components() {
	use crate::types::{PublicAction, PublicValue};

	async fn open(db: &TestDb, live: &str) -> crate::types::PublicUuid {
		let owner = TestDb::owner().with_rt(true);
		let mut responses =
			db.datastore().execute(live, &owner, None).await.expect("the live query runs");
		match responses.remove(0).result.expect("the live query opens") {
			PublicValue::Uuid(id) => id,
			other => panic!("`{live}` answered {other:?}"),
		}
	}

	let (db, notifications) = TestDb::new_with_notifications("").await;
	db.run(
		"DEFINE TABLE reading SCHEMALESS;
		DEFINE FIELD id ON reading TYPE [city: string, time: datetime];",
	)
	.await;
	let everything = open(&db, "LIVE SELECT * FROM reading").await;
	let paris = open(&db, "LIVE SELECT city, temp FROM reading WHERE city = 'Paris'").await;
	while notifications.try_recv().is_ok() {}

	// A record of another city reaches the subscription that reads every one.
	db.run("CREATE reading SET city = 'London', time = d'2024-01-01T00:00:00Z', temp = 1").await;
	let sent = notifications.recv().await.expect("the create notifies");
	assert_eq!(sent.id, everything);
	assert_eq!(sent.action, PublicAction::Create);
	let expected: PublicValue = crate::syn::value(
		"{ city: 'London', id: reading:['London', d'2024-01-01T00:00:00Z'], temp: 1, time: d'2024-01-01T00:00:00Z' }",
	)
	.unwrap();
	assert_eq!(sent.result, expected);
	assert!(notifications.try_recv().is_err(), "the Paris subscription is told of London");

	// A record of the other city reaches both, the second as it projects.
	db.run("CREATE reading SET city = 'Paris', time = d'2024-01-02T00:00:00Z', temp = 5").await;
	let mut both = [
		notifications.recv().await.expect("the first subscription is told"),
		notifications.recv().await.expect("the second subscription is told"),
	];
	assert!(notifications.try_recv().is_err(), "two subscriptions, two notifications");
	both.sort_by_key(|sent| sent.id != everything);
	assert_eq!(both[0].id, everything);
	assert_eq!(both[0].action, PublicAction::Create);
	let expected: PublicValue = crate::syn::value(
		"{ city: 'Paris', id: reading:['Paris', d'2024-01-02T00:00:00Z'], temp: 5, time: d'2024-01-02T00:00:00Z' }",
	)
	.unwrap();
	assert_eq!(both[0].result, expected);
	assert_eq!(both[1].id, paris);
	assert_eq!(both[1].action, PublicAction::Create);
	let expected: PublicValue = crate::syn::value("{ city: 'Paris', temp: 5 }").unwrap();
	assert_eq!(both[1].result, expected);

	// An update and a delete carry the components of the record they are about.
	db.run("UPDATE reading:['London', d'2024-01-01T00:00:00Z'] SET temp = 2").await;
	let sent = notifications.recv().await.expect("the update notifies");
	assert_eq!(sent.id, everything);
	assert_eq!(sent.action, PublicAction::Update);
	let expected: PublicValue = crate::syn::value(
		"{ city: 'London', id: reading:['London', d'2024-01-01T00:00:00Z'], temp: 2, time: d'2024-01-01T00:00:00Z' }",
	)
	.unwrap();
	assert_eq!(sent.result, expected);
	assert!(notifications.try_recv().is_err(), "the Paris subscription is told of London");

	db.run("DELETE reading:['London', d'2024-01-01T00:00:00Z']").await;
	let sent = notifications.recv().await.expect("the delete notifies");
	assert_eq!(sent.id, everything);
	assert_eq!(sent.action, PublicAction::Delete);
	assert_eq!(sent.result, expected);
	assert!(notifications.try_recv().is_err(), "the Paris subscription is told of London");
}
