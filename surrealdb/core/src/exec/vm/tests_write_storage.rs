//! Tests of the storage stages and structural checks run as instructions: each
//! write runs on a fresh database, for the owner and for a checked record user,
//! and its answers and every key and value it left are the recorded ones.

use super::golden::{Golden, render_changes, render_outcome};
use super::tests::*;
use crate::exec::operators::test_util::TestDb;
use crate::exec::operators::write::stage::StageId;
use crate::val::Value;

/// Tables of every kind a structural check or a store dispatches on.
pub(super) const STORAGE_SCHEMA: &str = "
	DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
	DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
	CREATE user:reader;
	DEFINE TABLE plain PERMISSIONS FULL;
	DEFINE TABLE feed CHANGEFEED 1d PERMISSIONS FULL;
	DEFINE TABLE uniq PERMISSIONS FULL;
	DEFINE INDEX by_k ON uniq FIELDS k UNIQUE;
	DEFINE TABLE rel TYPE RELATION IN plain OUT plain ENFORCED PERMISSIONS FULL;
	DEFINE TABLE light TYPE RELATION IN plain OUT plain LIGHTWEIGHT PERMISSIONS FULL;
	DEFINE TABLE gone DROP PERMISSIONS FULL;
	DEFINE TABLE normal_only TYPE NORMAL PERMISSIONS FULL;
	DEFINE TABLE agg AS SELECT count() AS c FROM plain GROUP ALL;
	DEFINE TABLE carried TYPE RELATION IN plain OUT plain PERMISSIONS FULL;
	DEFINE FIELD weight ON carried TYPE int INLINE;
	DEFINE FIELD label ON carried TYPE string INLINE;
	DEFINE TABLE loose TYPE RELATION PERMISSIONS FULL;
	DEFINE TABLE owner PERMISSIONS FULL;
	DEFINE TABLE pet PERMISSIONS FULL;
	DEFINE FIELD owner ON pet TYPE option<record<owner>> REFERENCE;
	DEFINE FIELD friends ON pet TYPE option<array<record<owner>>> REFERENCE;
	CREATE owner:a, owner:b, owner:c;
	CREATE pet:1 SET owner = owner:a, friends = [owner:b];
	DEFINE TABLE held PERMISSIONS FULL;
	DEFINE FIELD by_cascade ON held TYPE option<record<owner>> REFERENCE ON DELETE CASCADE;
	DEFINE FIELD by_unset ON held TYPE option<record<owner>> REFERENCE ON DELETE UNSET;
	DEFINE FIELD by_reject ON held TYPE option<record<plain>> REFERENCE ON DELETE REJECT;
	CREATE held:1 SET by_cascade = owner:c;
	CREATE held:2 SET by_unset = owner:b;
	CREATE held:3 SET by_reject = plain:2;
	RELATE plain:2->loose:l1->plain:1;
	CREATE plain:1 SET n = 1;
	CREATE plain:2 SET n = 2;
	CREATE uniq:1 SET k = 1;
	RELATE plain:1->rel:e1->plain:2 SET w = 1;
	RELATE plain:1->carried:c1->plain:2 SET weight = 1, label = 'a';
";

/// Writes that store, overwrite, skip, and refuse, across the tables above.
pub(super) const STORAGE_WRITES: &[&str] = &[
	"CREATE plain:3 SET n = 3",
	"CREATE plain:1 SET n = 9",
	"CREATE plain SET n = 4",
	"UPSERT plain:1 SET n = 5",
	"UPSERT plain:8 SET n = 8",
	"UPDATE plain SET n += 1",
	"UPDATE plain:1 SET n = 1",
	"UPDATE plain:99 SET n = 1",
	"DELETE plain:2",
	"DELETE plain",
	"CREATE feed:1 SET a = 1",
	"UPSERT feed:1 SET a = 2",
	"CREATE uniq:2 SET k = 2",
	"CREATE uniq:3 SET k = 1",
	"UPDATE uniq:1 SET k = 5",
	"CREATE gone:1 SET a = 1",
	"UPSERT gone:1 SET a = 1",
	"CREATE rel:9 SET x = 1",
	"UPSERT rel:9 SET x = 1",
	"CREATE normal_only:1 SET x = 1",
	"RELATE plain:1->normal_only->plain:2",
	"UPDATE agg SET x = 1",
	"CREATE agg:1 SET x = 1",
	"INSERT INTO plain { id: plain:7, n: 7 }",
	"INSERT INTO plain [{ id: plain:1, n: 1 }, { id: plain:6, n: 6 }]",
	"INSERT RELATION INTO rel { id: rel:5, in: plain:1, out: plain:2 }",
	"INSERT RELATION INTO light { in: plain:1, out: plain:2 }",
	"INSERT INTO normal_only { id: normal_only:2 }",
	"RELATE plain:2->rel->plain:1 SET w = 2",
	"RELATE plain:1->rel->plain:3",
	"RELATE plain:1->light->plain:2",
	"RELATE plain:1->light->plain:2 SET w = 1",
	"UPDATE rel SET w += 1",
	"DELETE rel",
	"CREATE plain:1, plain:10 SET n = 0",
	"RELATE plain:2->carried->plain:1 SET weight = 5, label = 'b'",
	"RELATE plain:1->carried:c1->plain:2 SET weight = 2, label = 'c'",
	"UPDATE carried:c1 SET weight = 9",
	"UPDATE carried SET label = 'z'",
	"UPDATE carried:c1 SET in = plain:2",
	"UPDATE carried:c1 SET out = plain:3",
	"RELATE plain:1->loose->plain:2",
	"RELATE plain:1->loose->plain:404",
	"RELATE plain:1->rel->plain:404",
	"RELATE plain:1->plain->plain:2",
	"RELATE rel:e1->loose->plain:2",
	"DELETE carried:c1",
	"CREATE plain:20 SET v = array::fold(array::range(0, 300), [], |$acc, $_| [$acc])",
	"UPDATE plain:1 SET v = array::fold(array::range(0, 300), [], |$acc, $_| [$acc])",
	"CREATE pet:2 SET owner = owner:b",
	"CREATE pet:3 SET friends = [owner:a, owner:b, owner:a]",
	"UPDATE pet:1 SET owner = owner:b",
	"UPDATE pet:1 SET friends += owner:c",
	"UPDATE pet:1 SET friends = []",
	"UPDATE pet:1 UNSET owner",
	"UPDATE pet:1 SET name = 'rex'",
	"DELETE pet:1",
	"DELETE pet",
	"DELETE rel:e1",
	"DELETE carried:c1 RETURN BEFORE",
	"DELETE plain:1",
	"DELETE plain:1->light WHERE out = plain:2",
	"DELETE owner:c",
	"DELETE owner:b",
	"DELETE owner",
	"DELETE plain:2",
	"DELETE held:3",
	"DELETE plain:1, plain:99",
];

/// Tables whose indexes are of every shape an index's entries are derived in:
/// a single column, compound columns, an array column taken element by
/// element, a flattened array column, a nested path, a self-dereference,
/// `UNIQUE` indexes over each, and a table whose B-tree index sits beside
/// kinds that keep their own structure.
pub(super) const INDEX_SCHEMA: &str = "
	DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
	DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
	CREATE user:reader;
	DEFINE TABLE item PERMISSIONS FULL;
	DEFINE INDEX by_name ON item FIELDS name;
	DEFINE INDEX by_pair ON item FIELDS a, b;
	DEFINE INDEX by_tag ON item FIELDS tags;
	DEFINE INDEX by_flat ON item FIELDS name, tags…;
	DEFINE INDEX by_deep ON item FIELDS meta.k, meta.list.*.v;
	DEFINE INDEX by_self ON item FIELDS id.name;
	DEFINE TABLE ukey PERMISSIONS FULL;
	DEFINE INDEX u_email ON ukey FIELDS email UNIQUE;
	DEFINE INDEX u_pair ON ukey FIELDS a, b UNIQUE;
	DEFINE INDEX u_tags ON ukey FIELDS tags UNIQUE;
	DEFINE ANALYZER simple TOKENIZERS blank;
	DEFINE TABLE mixed PERMISSIONS FULL;
	DEFINE INDEX m_name ON mixed FIELDS name;
	DEFINE INDEX m_count ON mixed COUNT;
	DEFINE INDEX m_where ON mixed COUNT WHERE name = 'm';
	DEFINE INDEX m_text ON mixed FIELDS body FULLTEXT ANALYZER simple BM25;
	DEFINE INDEX m_uniq ON mixed FIELDS code UNIQUE;
	DEFINE TABLE guarded PERMISSIONS FOR select WHERE n > 1 FOR create, update, delete FULL;
	DEFINE INDEX g_n ON guarded FIELDS n;
	DEFINE INDEX g_u ON guarded FIELDS k UNIQUE;
	DEFINE TABLE link TYPE RELATION PERMISSIONS FULL;
	DEFINE INDEX l_w ON link FIELDS w;
	DEFINE INDEX l_ends ON link FIELDS in, out UNIQUE;
	CREATE item:1 SET name = 'a', a = 1, b = 2, tags = ['x', 'y'], meta = { k: 1, list: [{ v: 1 }, { v: 2 }] };
	CREATE item:2 SET name = 'a', tags = [['x'], 'z'];
	CREATE ukey:1 SET email = 'e@x', a = 1, b = 1, tags = [1, 2];
	CREATE ukey:2 SET email = NONE, a = NULL;
	CREATE mixed:1 SET name = 'n', body = 'hello world', code = 1;
	CREATE guarded:1 SET n = 1, k = 1;
	CREATE guarded:2 SET n = 2, k = 2;
	RELATE item:1->link:l1->item:2 SET w = 1;
";

/// Writes that add, move, keep and remove index entries, and that conflict on
/// a `UNIQUE` index, across the tables of [`INDEX_SCHEMA`].
pub(super) const INDEX_WRITES: &[&str] = &[
	"CREATE item:3 SET name = 'b', a = 1, b = 2, tags = ['x', 'x', 'z'], meta = { k: [1, 2] }",
	"CREATE item:4 SET name = NONE",
	"CREATE item:5 SET name = NULL, tags = []",
	"CREATE item:6 SET tags = [[1, 2], [3]], meta = { list: [{ v: [1, 1] }] }",
	"CREATE item:7 SET name = 1, a = 1.0, b = 2dec",
	"CREATE item SET name = 'generated'",
	"UPDATE item:1 SET name = 'c'",
	"UPDATE item:1 SET tags += 'w'",
	"UPDATE item:1 SET name = 'a'",
	"UPDATE item:1 SET meta.list[0].v = 9",
	"UPDATE item SET a += 1",
	"UPDATE item:2 UNSET tags",
	"UPSERT item:9 SET name = 'u'",
	"UPSERT item:1 SET name = 'u', tags = 'scalar'",
	"INSERT INTO item [{ id: item:8, name: 'i' }, { id: item:1, name: 'dup' }]",
	"INSERT INTO item { id: item:1, name: 'again' } ON DUPLICATE KEY UPDATE name = 'moved'",
	"DELETE item:1",
	"DELETE item",
	"CREATE ukey:3 SET email = 'e@x'",
	"CREATE ukey:3 SET email = 'f@x', a = 1, b = 1",
	"CREATE ukey:3 SET email = 'f@x', a = 1.0, b = 1dec",
	"CREATE ukey:3 SET tags = [2, 3]",
	"CREATE ukey:3 SET tags = [3, 3]",
	"CREATE ukey:3 SET tags = [3, 4]",
	"CREATE ukey:3 SET email = NONE",
	"CREATE ukey:3 SET email = NULL, a = NULL, b = 1",
	"CREATE ukey:3, ukey:4 SET email = 'same'",
	"UPDATE ukey:1 SET email = 'g@x'",
	"UPDATE ukey:1 SET email = 'e@x'",
	"UPDATE ukey:2 SET email = 'e@x'",
	"UPDATE ukey SET tags = [9]",
	"UPSERT ukey:1 SET email = 'e@x', a = 2",
	"UPSERT ukey:5 SET email = 'e@x'",
	"INSERT INTO ukey { id: ukey:6, email: 'e@x' }",
	"INSERT IGNORE INTO ukey { id: ukey:6, email: 'e@x' }",
	"DELETE ukey:1",
	"DELETE ukey",
	"CREATE mixed:2 SET name = 'm', body = 'foo bar', code = 2",
	"CREATE mixed:2 SET name = 'm', code = 1",
	"UPDATE mixed:1 SET body = 'bye', name = 'm'",
	"UPDATE mixed:1 SET name = 'n2'",
	"DELETE mixed:1",
	"DELETE mixed",
	"CREATE guarded:3 SET n = 3, k = 3",
	"CREATE guarded:3 SET n = 3, k = 1",
	"UPDATE guarded SET n += 10",
	"UPDATE guarded:1 SET k = 2",
	"DELETE guarded",
	"DELETE guarded:1",
	"RELATE item:2->link->item:1 SET w = 2",
	"RELATE item:1->link->item:2 SET w = 3",
	"UPDATE link:l1 SET w = 5",
	"INSERT RELATION INTO link { id: link:l2, in: item:2, out: item:1, w: 1 }",
	"DELETE link",
	"DELETE item:2",
	"{ CREATE item:50 SET name = 'x', extra = 1; DEFINE INDEX late ON item FIELDS extra; CREATE item:51 SET name = 'y', extra = 1; UPDATE item:51 SET extra = 2; DELETE item:50 }",
	"{ CREATE item:52 SET name = 'x'; REMOVE INDEX by_name ON item; CREATE item:53 SET name = 'z'; UPDATE item:52 SET name = 'a' }",
	"{ DEFINE INDEX late ON item FIELDS name UNIQUE; CREATE item:60 SET name = 'q'; CREATE item:61 SET name = 'q' }",
	"{ DEFINE FIELD extra ON item TYPE int; CREATE item:70 SET extra = 1; ALTER FIELD extra ON item TYPE string; CREATE item:71 SET extra = 'a'; DELETE item:70 }",
	"FOR $i IN 0..4 { CREATE type::record('item', 80 + $i) SET name = 'l', extra = $i; IF $i = 1 { DEFINE INDEX loop_ix ON item FIELDS extra } }",
];

/// Run `sql` against a fresh `schema` as `session` and answer its rows and
/// every entry it added, changed or removed.
async fn stored_as(
	schema: &str,
	sql: &str,
	session: &crate::dbs::Session,
) -> (Result<Vec<Value>, String>, Vec<(Vec<u8>, Option<Vec<u8>>)>) {
	let db = TestDb::quiescent(schema, true).await;
	let before = keyspace(&db).await;
	let ctx = db.exec_ctx_as(session, crate::kvs::TransactionType::Write).await;
	let bound = bind(&ctx, sql).await;
	let op = planned(&planner(&ctx), &bound, sql).await;
	let bodies = rust_bodies();
	let plans = cascade_plans();
	let steps = super::write::INDEX_STEPS.get();
	let rows = outcome(&op, &ctx).await;
	RAN_STEPS.with(|ran| ran.set(super::write::INDEX_STEPS.get() - steps));
	RAN_BODIES.with(|ran| ran.set(rust_bodies() - bodies));
	RAN_PLANS.with(|ran| ran.set(cascade_plans() - plans));
	// A generated id is drawn afresh on every run: each one the write reports
	// is masked wherever its bytes were stored, as is the datastore's node id,
	// which a full-text or count index's keys carry.
	let mut generated = vec![db.datastore().id().as_bytes().to_vec()];
	if let Ok(rows) = &rows {
		for row in rows {
			collect_generated(row, &mut generated);
		}
	}
	let rows = rows
		.map(|rows| rows.into_iter().map(super::tests::without_generated_ids).collect::<Vec<_>>())
		.map_err(|error| without_generated_text(&error));
	let txn = ctx.txn();
	if rows.is_ok() {
		txn.commit().await.unwrap_or_else(|error| panic!("`{sql}` did not commit: {error}"));
	} else {
		txn.cancel().await.expect("cancel");
	}
	let after = keyspace(&db).await;
	let masked = changes(&before, &after)
		.into_iter()
		.map(|(key, value)| (masked(&key, &generated), value.map(|v| masked(&v, &generated))))
		.collect();
	(rows, masked)
}

/// The generated ids `value` holds: string keys of the length the id generator
/// draws.
fn collect_generated(value: &Value, out: &mut Vec<Vec<u8>>) {
	match value {
		Value::RecordId(rid) => {
			if let crate::val::RecordIdKey::String(key) = &rid.key
				&& key.len() == 20
			{
				out.push(key.as_bytes().to_vec());
			}
		}
		Value::Array(array) => array.iter().for_each(|v| collect_generated(v, out)),
		Value::Object(object) => object.values().for_each(|v| collect_generated(v, out)),
		_ => {}
	}
}

/// `text` with each record id key of the length the id generator draws —
/// twenty lowercase letters or digits after a `:` — replaced by `generated`.
fn without_generated_text(text: &str) -> String {
	let bytes = text.as_bytes();
	let mut out = String::with_capacity(text.len());
	let mut at = 0;
	while at < bytes.len() {
		let key = &bytes[at + 1..(at + 21).min(bytes.len())];
		let ends = bytes.get(at + 21).is_none_or(|b| !b.is_ascii_alphanumeric());
		if bytes[at] == b':'
			&& key.len() == 20
			&& ends && key.iter().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
		{
			out.push_str(":generated");
			at += 21;
		} else {
			out.push(bytes[at] as char);
			at += 1;
		}
	}
	out
}

/// `bytes` with each of `generated` replaced by a placeholder of its length.
fn masked(bytes: &[u8], generated: &[Vec<u8>]) -> Vec<u8> {
	let mut out = bytes.to_vec();
	for id in generated {
		let mut at = 0;
		while at + id.len() <= out.len() {
			if out[at..at + id.len()] == id[..] {
				out[at..at + id.len()].fill(b'#');
				at += id.len();
			} else {
				at += 1;
			}
		}
	}
	out
}

/// How many nested statements this thread has lowered to the VM.
fn nested_on_vm() -> usize {
	crate::exec::operators::write::scope::NESTED_ON_VM.with(std::cell::Cell::get)
}

/// How many times this thread ran a Rust body the VM lowers wherever the
/// catalog holds what it was lowered from: the views stage's, a purge's
/// dispatch of its reference repairs, and the reference stages'.
fn rust_bodies() -> usize {
	use crate::exec::operators::write::stages::{purge, refs, view};
	view::VIEW_BODIES.with(std::cell::Cell::get)
		+ purge::CASCADE_DISPATCHES.with(std::cell::Cell::get)
		+ refs::REFERENCE_BODIES.with(std::cell::Cell::get)
}

thread_local! {
	/// How many of [`rust_bodies`] the write [`stored_as`] last ran took,
	/// setting up its schema aside.
	static RAN_BODIES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	/// How many plans of a cascade's statement the write [`stored_as`] last
	/// ran asked for.
	static RAN_PLANS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	/// How many index instructions the write [`stored_as`] last ran executed.
	static RAN_STEPS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// How many times this thread asked for the plan of a cascade's statement.
fn cascade_plans() -> usize {
	crate::exec::operators::write::scope::CASCADE_PLANS.with(std::cell::Cell::get)
}

/// How many statements of a cascade this thread has run as units.
fn cascade_units_run() -> usize {
	super::write::CASCADE_UNITS_RUN.with(std::cell::Cell::get)
}

/// [`RAN_BODIES`].
fn ran_bodies() -> usize {
	RAN_BODIES.with(std::cell::Cell::get)
}

/// The rows or the error of a write and the entries it left, as a golden body.
fn render_stored(
	rows: &Result<Vec<Value>, String>,
	stored: &[(Vec<u8>, Option<Vec<u8>>)],
) -> String {
	format!("{}\nentries:\n{}", render_outcome(rows), render_changes(stored))
}

/// Every write of `writes` against `schema` answers and stores what the golden
/// `name` records, for the owner and for a checked record user, with each stage
/// of `lowered` run as the instructions of its family and none of
/// [`rust_bodies`] run.
async fn stores_as_recorded(
	name: &'static str,
	schema: &str,
	writes: &[&str],
	lowered: &[StageId],
) {
	let mut golden = Golden::new(name);
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	let reader = crate::dbs::Session::for_record("test", "test", "user", rid);
	for (who, session) in [("owner", TestDb::owner()), ("record user", reader)] {
		for sql in writes {
			// A block the VM plans again late-bound after its schema
			// statements (`super::tests::DECLINED_WHOLE`) compiles the write
			// units of its cascades as it runs.
			let declined = super::tests::declined_whole(sql, who == "record user");
			let (rows, stored) = stored_as(schema, sql, &session).await;
			assert!(
				declined || RAN_PLANS.with(std::cell::Cell::get) == 0,
				"`{sql}` for the {who} planned the statements of its cascades as it ran"
			);
			assert!(
				declined || ran_bodies() == 0,
				"`{sql}` for the {who} ran a Rust body with nested statements on the VM"
			);
			// Each of `item`'s six indexes is its own instruction.
			if schema == INDEX_SCHEMA && sql.starts_with("CREATE item:3") {
				assert_eq!(
					RAN_STEPS.with(std::cell::Cell::get),
					6,
					"`{sql}` for the {who} maintained its entries"
				);
			}
			golden.record(format!("{who}: {sql}"), render_stored(&rows, &stored));
			let db = TestDb::new_with_auth(schema).await;
			let ctx = db.exec_ctx_as(&session, crate::kvs::TransactionType::Write).await;
			let bound = bind(&ctx, sql).await;
			let outcome = match super::map::map(&planner(&ctx), &bound).await {
				Ok(physical) => {
					let physical = super::optimize::optimize(physical);
					super::lower::lower(&planner(&ctx), &physical).await
				}
				Err(why) => Err(why),
			};
			let program = match outcome {
				Ok(program) => program,
				Err(why) if declined && why.reason == super::speculate::DATA_DEPENDENT_DDL => {
					ctx.txn().cancel().await.expect("cancel");
					continue;
				}
				Err(why) => panic!("`{sql}` lowers: {why:?}"),
			};
			let specs: Vec<_> = program
				.write
				.iter()
				.chain(program.groups.iter().filter_map(|group| group.write.as_ref()))
				.collect();
			// A block folds away what follows a statement known to raise.
			assert!(!specs.is_empty() || rows.is_err(), "`{sql}` lowered no write");
			if !specs.is_empty() {
				let families = super::tests::write_families(
					&super::tests::with_stages(&planner(&ctx), &program).await,
				);
				for id in lowered {
					let family = super::tests::family_of(*id);
					assert!(
						families.contains(&family),
						"`{sql}` for the {who} runs {} by no {family} instruction: {families:?}",
						id.name()
					);
				}
			}
			ctx.txn().cancel().await.expect("cancel");
		}
	}
	golden.check();
}

/// A write stores its records, edges and index entries and answers what it
/// answers — for the owner and for a checked record user — with the storage
/// stages and structural checks run as instructions.
#[tokio::test]
async fn a_write_stores_its_records() {
	stores_as_recorded(
		"a_write_stores_its_records",
		STORAGE_SCHEMA,
		STORAGE_WRITES,
		&[
			StageId::CheckRecordExists,
			StageId::CheckTableNotView,
			StageId::ClearRecordData,
			StageId::StoreRecordData,
			StageId::StoreEdgesData,
			StageId::StoreInlineAdjacencyData,
			StageId::ProcessTableReferences,
			StageId::CleanupTableReferences,
			StageId::PurgeRecordData,
			StageId::StoreIndexData,
			StageId::ReclaimDocId,
		],
	)
	.await;
}

/// A write's index entries, `UNIQUE` conflicts and doc-id releases across every
/// index shape, with the index stages run as instructions.
#[tokio::test]
async fn index_entries_are_stored_per_index_shape() {
	stores_as_recorded(
		"index_entries_are_stored_per_index_shape",
		INDEX_SCHEMA,
		INDEX_WRITES,
		&[StageId::StoreIndexData, StageId::ReclaimDocId, StageId::StoreRecordData],
	)
	.await;
}

/// A write lowered before its table gains an index, and run after, fails the
/// statement rather than maintain an index it was not lowered for, and leaves
/// nothing behind.
#[tokio::test]
async fn index_entries_refuse_an_index_defined_after_lowering() {
	let sql = "CREATE item:30 SET name = 'p', extra = [1, 2]";
	let db = TestDb::quiescent(INDEX_SCHEMA, true).await;
	let before = keyspace(&db).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	let bound = bind(&ctx, sql).await;
	let op = super::plan(&planner(&ctx), &bound).await.expect("the VM plans the write");
	let define_sql = "DEFINE INDEX by_extra ON item FIELDS extra UNIQUE";
	let define = bind(&ctx, define_sql).await;
	let define = planned(&planner(&ctx), &define, define_sql).await;
	outcome(&define, &ctx).await.expect("the index is defined");
	let failure = outcome(&op, &ctx).await.expect_err("the write is refused");
	assert!(
		failure.contains("changed after the write was lowered"),
		"the write failed otherwise: {failure}"
	);
	ctx.txn().cancel().await.expect("cancel");
	assert!(changes(&before, &keyspace(&db).await).is_empty(), "the refused write left keys");
}

/// Tables whose deletes repair what references them, several levels deep and
/// by every strategy.
pub(super) const CASCADE_SCHEMA: &str = "
	DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
	DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
	CREATE user:reader;
	DEFINE TABLE root PERMISSIONS FULL;
	DEFINE TABLE mid PERMISSIONS FULL;
	DEFINE TABLE leaf PERMISSIONS FULL;
	DEFINE TABLE side PERMISSIONS FULL;
	DEFINE TABLE guarded PERMISSIONS FOR select, create, update FULL, FOR delete NONE;
	DEFINE FIELD up ON mid TYPE option<record<root>> REFERENCE ON DELETE CASCADE;
	DEFINE FIELD up ON leaf TYPE option<record<mid>> REFERENCE ON DELETE CASCADE;
	DEFINE FIELD many ON side TYPE option<array<record<root>>> REFERENCE ON DELETE UNSET;
	DEFINE FIELD one ON side TYPE option<record<mid>> REFERENCE ON DELETE UNSET;
	DEFINE FIELD kept ON side TYPE option<record<leaf>> REFERENCE ON DELETE IGNORE;
	DEFINE FIELD held ON side TYPE option<record<guarded>> REFERENCE ON DELETE REJECT;
	DEFINE FIELD by ON guarded TYPE option<record<root>> REFERENCE ON DELETE CASCADE;
	DEFINE FIELD noted ON side TYPE option<record<side>> REFERENCE ON DELETE THEN (UPDATE $this SET note = 'gone');
	CREATE root:1, root:2, root:3;
	CREATE mid:1 SET up = root:1;
	CREATE mid:2 SET up = root:1;
	CREATE mid:3 SET up = root:2;
	CREATE leaf:1 SET up = mid:1;
	CREATE leaf:2 SET up = mid:2;
	CREATE leaf:3 SET up = mid:3;
	CREATE guarded:1 SET by = root:3;
	CREATE side:1 SET many = [root:1, root:2, root:1], one = mid:1, kept = leaf:1;
	CREATE side:2 SET held = guarded:1;
	CREATE side:3 SET noted = side:1;
	RELATE root:1->link:l1->root:2;
	RELATE leaf:1->link:l2->root:3;
";

/// Deletes that cascade, unset, ignore, reject and run their own repair.
pub(super) const CASCADE_WRITES: &[&str] = &[
	"DELETE root:1",
	"DELETE root:2",
	"DELETE root:3",
	"DELETE root",
	"DELETE mid:1",
	"DELETE mid",
	"DELETE leaf:1",
	"DELETE guarded:1",
	"DELETE side:1",
	"DELETE root:1, mid:3",
	"DELETE root:1 RETURN BEFORE",
	"UPDATE mid:1 SET up = root:2",
	"UPDATE side:1 SET many -= root:1",
	"DELETE type::table('root')",
	"DELETE type::record('mid', 1)",
	"UPDATE type::table('side') SET many -= root:1, one = mid:2",
	"CREATE type::record('side', 9) SET many = [root:2], held = guarded:1",
	"FOR $r IN [root:2, root:3] { DELETE $r }",
	"{ DELETE root:1; DEFINE FIELD up2 ON leaf TYPE option<record<root>> REFERENCE ON DELETE CASCADE; CREATE leaf:9 SET up2 = root:2; DELETE root:2; REMOVE FIELD up2 ON leaf; DELETE root:3 }",
	"{ DEFINE FIELD owner ON fresh TYPE option<record<root>> REFERENCE ON DELETE CASCADE; CREATE fresh:1 SET owner = root:3; DELETE root:3; RETURN (SELECT * FROM fresh) }",
	"FOR $i IN 0..3 { DEFINE FIELD IF NOT EXISTS owner ON fresh2 TYPE option<record<root>> REFERENCE ON DELETE CASCADE; CREATE type::record('fresh2', $i) SET owner = root:3; IF $i = 1 { DELETE root:3 } }",
	"{ DEFINE FIELD late ON side TYPE option<record<mid>> REFERENCE ON DELETE UNSET; CREATE side:8 SET late = mid:3; DELETE mid:3; ALTER FIELD late ON side REFERENCE ON DELETE REJECT; CREATE side:9 SET late = mid:2; DELETE mid:2 }",
];

/// A delete's cascades — nested deletes and updates of what references the
/// record, several levels deep — leave the recorded entries, for the owner and
/// for a checked record user.
#[tokio::test]
async fn nest_cascades_store_their_repairs() {
	let before = cascade_units_run();
	stores_as_recorded("nest_cascades_store_their_repairs", CASCADE_SCHEMA, CASCADE_WRITES, &[])
		.await;
	assert!(cascade_units_run() > before, "no cascade ran on the VM");
}

/// Cascades that run deep, that run around a cycle back to where they began,
/// that run past the computation depth, and that reach tables with views,
/// with hidden rows and with guarded edges.
pub(super) const DEEP_CASCADE_SCHEMA: &str = "
	DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
	DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
	CREATE user:reader;
	DEFINE TABLE ring PERMISSIONS FULL;
	DEFINE FIELD next ON ring TYPE option<record<ring>> REFERENCE ON DELETE CASCADE;
	CREATE ring:1 SET next = ring:2;
	CREATE ring:2 SET next = ring:3;
	CREATE ring:3 SET next = ring:1;
	DEFINE TABLE pair PERMISSIONS FULL;
	DEFINE FIELD other ON pair TYPE option<record<pair>> REFERENCE ON DELETE CASCADE;
	DEFINE FIELD loose ON pair TYPE option<record<pair>> REFERENCE ON DELETE UNSET;
	CREATE pair:a SET other = pair:b;
	CREATE pair:b SET loose = pair:a;
	DEFINE TABLE chain PERMISSIONS FULL;
	DEFINE FIELD up ON chain TYPE option<record<chain>> REFERENCE ON DELETE CASCADE;
	CREATE chain:0;
	FOR $i IN 1..40 { CREATE type::record('chain', $i) SET up = type::record('chain', $i - 1) };
	DEFINE TABLE deep PERMISSIONS FULL;
	DEFINE FIELD up ON deep TYPE option<record<deep>> REFERENCE ON DELETE CASCADE;
	CREATE deep:0;
	FOR $i IN 1..200 { CREATE type::record('deep', $i) SET up = type::record('deep', $i - 1) };
	DEFINE TABLE hidden PERMISSIONS FOR select NONE, FOR create, update, delete FULL;
	DEFINE FIELD of ON hidden TYPE option<record<ring>> REFERENCE ON DELETE CASCADE;
	CREATE hidden:1 SET of = ring:2;
	DEFINE TABLE sealed PERMISSIONS FOR select, create FULL, FOR update, delete NONE;
	RELATE hidden:1->sealed:s1->chain:5;
	RELATE ring:3->sealed:s2->pair:a;
	DEFINE TABLE item PERMISSIONS FULL;
	DEFINE FIELD owner ON item TYPE option<record<ring | chain>> REFERENCE ON DELETE CASCADE;
	DEFINE TABLE totals AS SELECT owner, count() AS n, math::sum(amount) AS total,
		math::max(amount) AS high FROM item GROUP BY owner;
	CREATE item:1 SET owner = ring:1, amount = 3;
	CREATE item:2 SET owner = ring:2, amount = 5;
	CREATE item:3 SET owner = ring:2, amount = 8;
	CREATE item:4 SET owner = chain:20, amount = 13;
	CREATE item:5 SET owner = chain:39, amount = 21;
";

/// Deletes that cascade around a cycle, down a long chain, and through
/// records a view aggregates.
pub(super) const DEEP_CASCADE_WRITES: &[&str] = &[
	"DELETE ring:1",
	"DELETE ring:3 RETURN BEFORE",
	"DELETE ring",
	"DELETE pair:a",
	"DELETE pair:b",
	"DELETE chain:0",
	"DELETE chain:30",
	"DELETE item:3",
	"UPDATE item:2 SET owner = chain:1",
	"DELETE ring:2, chain:10",
];

/// Cascades deep and cyclic, and reaching views, hidden records and guarded
/// edges leave the recorded entries, for the owner and for a checked record
/// user; their nested statements run on the VM.
#[tokio::test]
async fn nest_deep_cascades_store_their_repairs() {
	let before = cascade_units_run();
	stores_as_recorded(
		"nest_deep_cascades_store_their_repairs",
		DEEP_CASCADE_SCHEMA,
		DEEP_CASCADE_WRITES,
		&[],
	)
	.await;
	assert!(cascade_units_run() > before, "no cascade ran on the VM");
}

/// A write loop's cascades are lowered when the statement arrives, however
/// deep they run, and a run asks for no plan of them.
#[tokio::test]
async fn nest_a_write_loops_cascades_run_without_planning() {
	let sql = "FOR $i IN 1..=3 { DELETE type::record('chain', $i * 10) }";
	let units = cascade_units_run();
	let (rows, stored) = stored_as(DEEP_CASCADE_SCHEMA, sql, &TestDb::owner()).await;
	assert!(cascade_units_run() > units, "no cascade ran on the VM");
	assert_eq!(RAN_PLANS.with(std::cell::Cell::get), 0, "the loop planned its cascades as it ran");
	let mut golden = Golden::new("nest_a_write_loops_cascades_run_without_planning");
	golden.record(sql, render_stored(&rows, &stored));
	golden.check();
}

/// A cascade longer than the computation depth fails where the depth runs out
/// and writes nothing, for the owner and for a checked record user. The VM runs
/// a cascade's nested writes as frames of its driver, so the chain's length
/// does not bear on the thread's stack.
#[tokio::test]
async fn nest_a_cascade_past_the_computation_depth_fails() {
	let mut golden = Golden::new("nest_a_cascade_past_the_computation_depth_fails");
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	let reader = crate::dbs::Session::for_record("test", "test", "user", rid);
	for (who, session) in [("owner", TestDb::owner()), ("record user", reader)] {
		for sql in ["DELETE deep:0", "DELETE deep:150"] {
			let (rows, stored) = stored_as(DEEP_CASCADE_SCHEMA, sql, &session).await;
			golden.record(format!("{who}: {sql}"), render_stored(&rows, &stored));
			if sql == "DELETE deep:0" {
				let failure = rows.expect_err("the cascade runs past the depth");
				assert!(
					failure.contains("excessive computation depth"),
					"`{sql}` for the {who} failed otherwise: {failure}"
				);
				assert!(stored.is_empty(), "`{sql}` for the {who} wrote {} entries", stored.len());
			}
		}
	}
	golden.check();
}

/// Sources of aggregated views of every kind a view row keeps stats for,
/// grouped and whole.
pub(super) const VIEW_SCHEMA: &str = "
	DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
	DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
	CREATE user:reader;
	DEFINE TABLE sale PERMISSIONS FULL;
	DEFINE TABLE by_region AS SELECT region, count() AS n, math::sum(amount) AS total,
		math::mean(amount) AS mean, math::min(amount) AS low, math::max(amount) AS high
		FROM sale GROUP BY region;
	DEFINE TABLE overall AS SELECT count() AS n, math::sum(amount) AS total FROM sale GROUP ALL;
	DEFINE TABLE big AS SELECT * FROM sale WHERE amount > 10;
	CREATE sale:1 SET region = 'n', amount = 5;
	CREATE sale:2 SET region = 'n', amount = 15;
	CREATE sale:3 SET region = 's', amount = 7;
";

/// Writes that add to, change, move between and empty the views' groups.
pub(super) const VIEW_WRITES: &[&str] = &[
	"CREATE sale:4 SET region = 'n', amount = 20",
	"CREATE sale:5 SET region = 'e', amount = 1",
	"UPDATE sale:1 SET amount = 30",
	"UPDATE sale:2 SET amount = 1",
	"UPDATE sale:3 SET region = 'n'",
	"UPDATE sale SET amount += 1",
	"UPSERT sale:6 SET region = 's', amount = 11",
	"DELETE sale:2",
	"DELETE sale:3",
	"DELETE sale",
	"INSERT INTO sale [{ id: sale:7, region: 'w', amount: 3 }, { id: sale:8, region: 'w', amount: 9 }]",
	"CREATE sale:9, sale:10 SET region = 'l', amount = 4",
	"UPDATE sale SET amount = 0 WHERE region = 'n'",
	"DELETE sale WHERE amount > 6",
];

/// Maintaining views — each view row driven through the view table's write
/// order, an extreme recomputed from its group — leaves the recorded entries,
/// for the owner and for a checked record user, and compiles no nested
/// statement as it runs.
#[tokio::test]
async fn nest_views_store_their_rows() {
	let before = nested_on_vm();
	stores_as_recorded("nest_views_store_their_rows", VIEW_SCHEMA, VIEW_WRITES, &[]).await;
	assert_eq!(nested_on_vm(), before, "a view's maintenance compiled a nested statement");
}

/// Views of views, views whose tables have events, indexes and a change feed
/// of their own, and a materialized view with a condition, fed by one table.
pub(super) const VIEW_OF_VIEW_SCHEMA: &str = "
	DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
	DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
	CREATE user:reader;
	DEFINE TABLE sale PERMISSIONS FOR select WHERE $auth.id = user:x, FOR create, update, delete FULL;
	DEFINE TABLE log PERMISSIONS FULL;
	DEFINE TABLE big AS SELECT id AS of, region, amount, amount * 2 AS doubled FROM sale
		WHERE amount > 10;
	DEFINE INDEX by_region ON big FIELDS region;
	DEFINE EVENT noted ON big THEN (
		UPSERT type::record('log', ['big', $event, $value.id, $before.amount, $after.amount])
			SET view = 'big', ev = $event, before = $before.amount, after = $after.amount
	);
	DEFINE TABLE by_region AS SELECT region, count() AS n, math::sum(amount) AS total,
		math::max(amount) AS high FROM big GROUP BY region;
	DEFINE EVENT counted ON by_region WHEN $event != 'DELETE' THEN (
		UPSERT type::record('log', ['by_region', $event, $value.id, $after.n, $after.high])
			SET view = 'by_region', ev = $event, n = $after.n, high = $after.high
	);
	DEFINE TABLE regions AS SELECT count() AS groups, math::sum(total) AS total FROM by_region
		GROUP ALL;
	DEFINE TABLE watched CHANGEFEED 1d AS SELECT region FROM big;
	CREATE sale:1 SET region = 'n', amount = 5;
	CREATE sale:2 SET region = 'n', amount = 15;
	CREATE sale:3 SET region = 's', amount = 17;
";

/// Writes that bring records into and out of the first view, and so add,
/// change and remove the rows the second and third views derive from it.
pub(super) const VIEW_OF_VIEW_WRITES: &[&str] = &[
	"CREATE sale:4 SET region = 'n', amount = 20",
	"UPDATE sale:1 SET amount = 30",
	"UPDATE sale:2 SET amount = 1",
	"UPDATE sale:3 SET region = 'n'",
	"UPDATE sale:3 SET amount = 16",
	"UPDATE sale SET amount += 1",
	"DELETE sale:2",
	"DELETE sale:3",
	"DELETE sale",
	"INSERT INTO sale [{ id: sale:7, region: 'w', amount: 13 }, { id: sale:8, region: 'w', amount: 9 }]",
	"UPSERT sale:9 SET region = 'e', amount = 50",
	"FOR $i IN 1..=3 { UPDATE sale:2 SET amount = $i * 10 }",
	"UPDATE type::table('sale') SET amount = 12",
];

/// A write whose views feed views, and whose view tables have indexes, events
/// and a change feed, leaves the recorded entries, for the owner and for a
/// checked record user who may not read the source, with every view — and every
/// view of a view — maintained as instructions.
#[tokio::test]
async fn views_of_views_store_their_rows() {
	stores_as_recorded(
		"views_of_views_store_their_rows",
		VIEW_OF_VIEW_SCHEMA,
		VIEW_OF_VIEW_WRITES,
		&[
			StageId::ProcessTableViews(crate::doc::Action::Create),
			StageId::ProcessTableViews(crate::doc::Action::Update),
			StageId::ProcessTableViews(crate::doc::Action::Delete),
		],
	)
	.await;
}

/// Aggregated views that keep each kind of extreme, over a table the checked
/// record user may not read, keyed by one group and by none.
const EXTREMES_SCHEMA: &str = "
	DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
	CREATE user:reader;
	DEFINE TABLE reading PERMISSIONS FOR select WHERE $auth.id = user:x, FOR create, update, delete FULL;
	DEFINE TABLE by_sensor AS SELECT sensor, math::min(value) AS low, math::max(value) AS high,
		time::min(at) AS first, time::max(at) AS last, count() AS n
		FROM reading WHERE value >= 0 GROUP BY sensor;
	DEFINE TABLE overall AS SELECT math::max(value) AS high, math::min(value) AS low FROM reading
		GROUP ALL;
	CREATE reading:1 SET sensor = 'a', value = 5, at = d'2024-01-01T00:00:00Z';
	CREATE reading:2 SET sensor = 'a', value = 9, at = d'2024-02-01T00:00:00Z';
	CREATE reading:3 SET sensor = 'a', value = 1, at = d'2024-03-01T00:00:00Z';
	CREATE reading:4 SET sensor = 'b', value = 7, at = d'2024-01-15T00:00:00Z';
	CREATE reading:5 SET sensor = 'b', value = 7, at = d'2024-01-20T00:00:00Z';
";

/// Writes that take the extreme away from a group, move it down or up, move
/// a record between groups and out of the view's condition, and empty groups.
const EXTREMES_WRITES: &[&str] = &[
	"DELETE reading:2",
	"DELETE reading:3",
	"DELETE reading:1",
	"DELETE reading:4",
	"DELETE reading:5",
	"DELETE reading",
	"UPDATE reading:2 SET value = 2",
	"UPDATE reading:3 SET value = 100",
	"UPDATE reading:1 SET at = d'2023-01-01T00:00:00Z'",
	"UPDATE reading:3 SET at = d'2020-01-01T00:00:00Z'",
	"UPDATE reading:2 SET sensor = 'b'",
	"UPDATE reading:4 SET value = -1",
	"UPDATE reading:1 SET value = 5",
	"CREATE reading:6 SET sensor = 'a', value = 9, at = d'2024-02-01T00:00:00Z'",
	"{ DELETE reading:2; DELETE reading:6; UPDATE reading:1 SET value = 3; }",
	"FOR $r IN [reading:2, reading:3, reading:1] { DELETE $r }",
	"UPDATE reading SET value = 4",
];

/// An aggregated view that keeps extremes recomputes the one a removed or
/// changed record held over the group's remaining rows, by a statement lowered
/// with the write.
#[tokio::test]
async fn aggregated_extremes_are_recalculated() {
	stores_as_recorded(
		"aggregated_extremes_are_recalculated",
		EXTREMES_SCHEMA,
		EXTREMES_WRITES,
		&[
			StageId::ProcessTableViews(crate::doc::Action::Create),
			StageId::ProcessTableViews(crate::doc::Action::Update),
			StageId::ProcessTableViews(crate::doc::Action::Delete),
		],
	)
	.await;
}

/// A write planned after a view of its table is defined, in the same
/// transaction, maintains that view as instructions.
#[tokio::test]
async fn views_follow_a_view_defined_before_planning() {
	let sql = "CREATE sale:30 SET region = 'q', amount = 40";
	let db = TestDb::quiescent(VIEW_OF_VIEW_SCHEMA, true).await;
	let before = keyspace(&db).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	let define_sql = "DEFINE TABLE late AS SELECT amount FROM sale";
	let define = bind(&ctx, define_sql).await;
	let define = planned(&planner(&ctx), &define, define_sql).await;
	outcome(&define, &ctx).await.expect("the view is defined");
	let bound = bind(&ctx, sql).await;
	let op = planned(&planner(&ctx), &bound, sql).await;
	let bodies = rust_bodies();
	let rows = outcome(&op, &ctx).await;
	assert_eq!(rust_bodies(), bodies, "the write ran a Rust body to maintain its views");
	ctx.txn().commit().await.expect("commit");
	let written = changes(&before, &keyspace(&db).await);
	assert!(
		written.iter().any(|(key, _)| key.windows(4).any(|w| w == b"late")),
		"the view defined before planning was not maintained"
	);
	let mut golden = Golden::new("views_follow_a_view_defined_before_planning");
	golden.record(sql, render_stored(&rows, &written));
	golden.check();
}

/// A write lowered before a view of its table is defined, and run after, does
/// not run against the views it was lowered for: it fails, and leaves no row
/// of the view it did not see.
#[tokio::test]
async fn a_write_lowered_before_a_view_is_defined_fails_closed() {
	let db = TestDb::quiescent(VIEW_OF_VIEW_SCHEMA, true).await;
	let before = keyspace(&db).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	let bound = bind(&ctx, "CREATE sale:30 SET region = 'q', amount = 40").await;
	let op = super::plan(&planner(&ctx), &bound).await.expect("the VM plans the write");
	let define_sql = "DEFINE TABLE late AS SELECT amount FROM sale";
	let define = bind(&ctx, define_sql).await;
	let define = planned(&planner(&ctx), &define, define_sql).await;
	outcome(&define, &ctx).await.expect("the view is defined");
	let bodies = rust_bodies();
	let failure = outcome(&op, &ctx).await.expect_err("the write is not run against other views");
	assert!(failure.contains("changed after the write was lowered"), "{failure}");
	assert_eq!(rust_bodies(), bodies, "the write ran a Rust body to maintain its views");
	ctx.txn().cancel().await.expect("cancel");
	assert_eq!(before, keyspace(&db).await, "the failed write left state behind");
}

/// Records repaired by `THEN` expressions that read the referencing record:
/// a statement rewriting it, an expression failing for some, one answering a
/// value and one returning, over a table the checked record user may not read.
const CUSTOM_REPAIR_SCHEMA: &str = "
	DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
	DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
	CREATE user:reader;
	DEFINE TABLE doc PERMISSIONS FULL;
	DEFINE TABLE note PERMISSIONS FOR select WHERE $auth.id = user:x, FOR create, update, delete FULL;
	DEFINE FIELD about ON note TYPE option<record<doc>> REFERENCE ON DELETE THEN (
		UPDATE $this SET orphaned = true, was = $reference, title = $this.title + ' (orphaned)'
	);
	DEFINE FIELD pinned ON note TYPE option<record<doc>> REFERENCE ON DELETE THEN
		IF $this.locked { THROW 'locked: ' + <string> $this.title };
	DEFINE FIELD quiet ON note TYPE option<record<doc>> REFERENCE ON DELETE THEN $this.title;
	DEFINE FIELD done ON note TYPE option<record<doc>> REFERENCE ON DELETE THEN { RETURN $this.title };
	DEFINE FIELD also ON note TYPE option<record<doc>> REFERENCE ON DELETE CASCADE;
	CREATE doc:1, doc:2, doc:3, doc:4, doc:5;
	CREATE note:1 SET about = doc:1, title = 'one';
	CREATE note:2 SET pinned = doc:2, locked = true, title = 'two';
	CREATE note:3 SET pinned = doc:3, locked = false, quiet = doc:3, title = 'three';
	CREATE note:4 SET quiet = doc:4, done = doc:4, about = doc:4, title = 'four';
	CREATE note:5 SET also = doc:5, about = doc:1, title = 'five';
";

/// Deletes whose repairs rewrite, refuse, read and return; and writes to the
/// referencing table named by an expression.
const CUSTOM_REPAIR_WRITES: &[&str] = &[
	"DELETE doc:1",
	"DELETE doc:2",
	"DELETE doc:3",
	"DELETE doc:4",
	"DELETE doc:5",
	"DELETE doc:1, doc:3, doc:4",
	"DELETE doc",
	"DELETE type::table('doc')",
	"FOR $d IN [doc:1, doc:4] { DELETE $d }",
	"UPDATE type::table('note') SET about = doc:3",
	"CREATE type::record('note', 9) SET about = doc:2, quiet = doc:2",
	"INSERT INTO note { id: note:10, pinned: doc:5, locked: true }",
	"DELETE type::table('note')",
	"UPSERT type::record('note', 1) SET about = doc:5",
];

/// A delete's repairs — its reference strategies dispatched as instructions,
/// each referencing table's repairs lowered on first sight, a `THEN` repair's
/// expression bytecode reading the referencing record — and writes to a
/// referencing table named by an expression leave the recorded entries, for
/// the owner and for a checked record user.
#[tokio::test]
async fn views_custom_repairs_store_their_rows() {
	stores_as_recorded(
		"views_custom_repairs_store_their_rows",
		CUSTOM_REPAIR_SCHEMA,
		CUSTOM_REPAIR_WRITES,
		&[
			StageId::ProcessTableReferences,
			StageId::CleanupTableReferences,
			StageId::PurgeRecordData,
		],
	)
	.await;
}

/// Tables whose vector indexes keep their own structure beside a B-tree
/// index and a `COUNT` index with a guard.
pub(super) const VECTOR_SCHEMA: &str = "
	DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
	DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
	CREATE user:reader;
	DEFINE TABLE hn PERMISSIONS FULL;
	DEFINE INDEX hn_vec ON hn FIELDS emb HNSW DIMENSION 3 DISTANCE EUCLIDEAN;
	DEFINE INDEX hn_tag ON hn FIELDS tag;
	DEFINE TABLE ann PERMISSIONS FULL;
	DEFINE INDEX ann_vec ON ann FIELDS emb DISKANN DIMENSION 3 DIST EUCLIDEAN TYPE F32;
	DEFINE INDEX ann_hot ON ann COUNT WHERE tag = 'hot';
	CREATE hn:1 SET emb = [1, 2, 3], tag = 'a';
	CREATE hn:2 SET emb = [4, 5, 6], tag = 'b';
	CREATE ann:1 SET emb = [1, 2, 3], tag = 'hot';
	CREATE ann:2 SET emb = [4, 5, 6], tag = 'cold';
";

/// Writes that add, move, keep, empty and remove vector entries, and that
/// flip a `COUNT` guard.
pub(super) const VECTOR_WRITES: &[&str] = &[
	"CREATE hn:3 SET emb = [7, 8, 9], tag = 'c'",
	"CREATE hn:4 SET tag = 'd'",
	"CREATE hn:5 SET emb = [1, 2], tag = 'e'",
	"UPDATE hn:1 SET emb = [3, 2, 1]",
	"UPDATE hn:1 SET tag = 'z'",
	"UPDATE hn:2 UNSET emb",
	"UPSERT hn:6 SET emb = [0, 0, 0]",
	"DELETE hn:1",
	"DELETE hn",
	"CREATE ann:3 SET emb = [7, 8, 9], tag = 'hot'",
	"CREATE ann:4 SET tag = 'hot'",
	"UPDATE ann:1 SET emb = [3, 2, 1]",
	"UPDATE ann:1 SET tag = 'cold'",
	"UPDATE ann:2 SET tag = 'hot'",
	"UPDATE ann SET tag = 'hot'",
	"DELETE ann:1",
	"DELETE ann",
];

/// A write's vector entries and a guarded count, with every index of the table
/// maintained as an instruction.
#[tokio::test]
async fn vector_entries_are_stored_per_index() {
	stores_as_recorded(
		"vector_entries_are_stored_per_index",
		VECTOR_SCHEMA,
		VECTOR_WRITES,
		&[StageId::StoreIndexData, StageId::ReclaimDocId, StageId::StoreRecordData],
	)
	.await;
}
/// Every write of every corpus of the storage stages lowers its storage
/// stages, those of the stage units its tables dispatch to and those of the
/// statements its cascades run, to instructions; over the corpora whose tables
/// have no views or events, so does every stage of every other kind.
#[tokio::test]
async fn every_storage_stage_of_the_corpus_lowers_to_instructions() {
	let corpora: [(&str, &[&str], bool); 6] = [
		(STORAGE_SCHEMA, STORAGE_WRITES, false),
		(INDEX_SCHEMA, INDEX_WRITES, true),
		(VECTOR_SCHEMA, VECTOR_WRITES, true),
		(CASCADE_SCHEMA, CASCADE_WRITES, true),
		(DEEP_CASCADE_SCHEMA, DEEP_CASCADE_WRITES, false),
		(CUSTOM_REPAIR_SCHEMA, CUSTOM_REPAIR_WRITES, true),
	];
	for (schema, writes, every_stage) in corpora {
		for sql in writes {
			let db = TestDb::new_with_auth(schema).await;
			let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
			let bound = bind(&ctx, sql).await;
			let outcome = match super::map::map(&planner(&ctx), &bound).await {
				Ok(physical) => {
					let physical = super::optimize::optimize(physical);
					super::lower::lower(&planner(&ctx), &physical).await
				}
				Err(why) => Err(why),
			};
			// A block declined whole for a catalog change it reads
			// (`super::tests::DECLINED_WHOLE`) runs no stage of its own.
			let program = match outcome {
				Ok(_) if super::tests::declined_whole(sql, false) => {
					panic!("`{sql}` lowered, listed as declined whole")
				}
				Ok(program) => program,
				Err(why)
					if why.reason == super::speculate::DATA_DEPENDENT_DDL
						&& super::tests::declined_whole(sql, false) =>
				{
					ctx.txn().cancel().await.expect("cancel");
					continue;
				}
				Err(why) => panic!("`{sql}` lowers: {why:?}"),
			};
			let families = super::tests::write_families(
				&super::tests::with_stages(&planner(&ctx), &program).await,
			);
			let expected: &[&str] = if every_stage {
				&["gate", "storage"]
			} else {
				&["storage"]
			};
			for family in expected {
				assert!(
					families.contains(family),
					"`{sql}` runs no {family} instruction: {families:?}"
				);
			}
			ctx.txn().cancel().await.expect("cancel");
		}
	}
}
