//! Sources that read an index, a bitmap, a lightweight relation or a table
//! under a top-k pushdown hold plain data, and the host reads them through
//! [`super::services::Services`]. Each answers its rows, for the owner and for
//! record users under every kind of table permission; the table's permission
//! is applied once, by the program; a principal of another tenant is refused;
//! and running the program compiles nothing.

use std::sync::Arc;

use super::golden::Golden;
use super::program::{Program, Source};
use super::tests::{bind, outcome, planner};
use crate::exec::ExecutionContext;
use crate::exec::operators::test_util::TestDb;
use crate::val::Value;

/// The tables a read of each source is made against: one whose permission is
/// full, one conditional on the record's owner and one that is denied. Each
/// holds a full-text, a vector and two B-tree indexes, a field a checked
/// caller may not read and a computed one.
fn tables() -> String {
	let mut setup = String::from("DEFINE ANALYZER blank TOKENIZERS blank;\n");
	for (table, permission) in [
		("open", "FULL"),
		("mine", "FOR select WHERE owner = $auth OR owner = $auth.id"),
		("locked", "FOR select NONE"),
	] {
		setup.push_str(&format!(
			"DEFINE TABLE {table} SCHEMALESS PERMISSIONS {permission};
			 DEFINE FIELD secret ON {table} PERMISSIONS FOR select NONE;
			 DEFINE FIELD label ON {table} COMPUTED 'row ' + <string> n;
			 DEFINE INDEX {table}_ft ON {table} FIELDS body FULLTEXT ANALYZER blank BM25;
			 DEFINE INDEX {table}_vec ON {table} FIELDS emb HNSW DIMENSION 3 DISTANCE EUCLIDEAN;
			 DEFINE INDEX {table}_n ON {table} FIELDS n;
			 DEFINE INDEX {table}_m ON {table} FIELDS m;
			 FOR $i IN 1..=24 {{
				 CREATE type::record('{table}', $i) SET
					 owner = IF $i % 2 = 0 {{ user:alice }} ELSE {{ user:bob }},
					 body = IF $i % 3 = 0 {{ 'alpha beta' }} ELSE {{ 'beta gamma' }},
					 emb = [<float> $i, 0.0, 0.0],
					 n = $i % 4,
					 m = $i % 5,
					 secret = 's';
			 }};\n"
		));
	}
	setup.push_str(
		"DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
		 DEFINE TABLE pal TYPE RELATION IN person OUT person LIGHTWEIGHT PERMISSIONS FULL;
		 DEFINE TABLE private_pal TYPE RELATION IN person OUT person LIGHTWEIGHT
			 PERMISSIONS FOR select WHERE in.name = 'a';
		 DEFINE TABLE sealed_pal TYPE RELATION IN person OUT person LIGHTWEIGHT
			 PERMISSIONS FOR select NONE;
		 CREATE person:a SET name = 'a';
		 CREATE person:b SET name = 'b';
		 CREATE person:c SET name = 'c';
		 RELATE person:a->pal->person:b;
		 RELATE person:b->pal->person:c;
		 RELATE person:c->pal->person:a;
		 RELATE person:a->private_pal->person:b;
		 RELATE person:b->private_pal->person:c;
		 RELATE person:a->sealed_pal->person:c;
		 DEFINE TABLE pairs SCHEMALESS PERMISSIONS FULL;
		 DEFINE INDEX by_pair ON pairs FIELDS kind, age;
		 FOR $i IN 1..=40 {
			 CREATE type::record('pairs', $i) SET kind = $i % 4, age = ($i * 7) % 11;
		 };
		 DEFINE TABLE item SCHEMALESS PERMISSIONS FULL;
		 DEFINE TABLE grp SCHEMALESS PERMISSIONS FULL;
		 DEFINE INDEX item_g ON item FIELDS g;
		 FOR $i IN 1..=120 {
			 CREATE type::record('item', $i) SET n = ($i * 37) % 101, g = $i % 3;
		 };
		 CREATE grp:0 SET g = 0;
		 CREATE grp:1 SET g = 1;
		 CREATE grp:2 SET g = 2;
		 DEFINE FUNCTION fn::top($d: int) {
			 LET $a = SELECT VALUE n FROM item ORDER BY n DESC LIMIT 5;
			 RETURN IF $d > 0 { array::concat($a, fn::top($d - 1)) } ELSE { $a };
		 } PERMISSIONS FULL;",
	);
	setup
}

/// Statements, each read through a different source, against table `t`.
const TABLE_READS: &[&str] = &[
	// A full-text query.
	"SELECT id, secret, label FROM t WHERE body @1@ 'alpha' ORDER BY id",
	"SELECT id, label FROM t WHERE body @1@ 'beta' ORDER BY id LIMIT 5",
	"SELECT count() FROM t WHERE body @1@ 'alpha' GROUP ALL",
	// The nearest neighbours of a vector.
	"SELECT id, secret, label FROM t WHERE emb <|4, 40|> [4.75, 0.0, 0.0] ORDER BY id",
	"SELECT id FROM t WHERE emb <|10, 40|> [2.0, 0.0, 0.0] ORDER BY id",
	// A B-tree index, alone, ordered and in a union.
	"SELECT id, secret, label FROM t WHERE n = 1 ORDER BY id",
	"SELECT id FROM t WHERE n > 1 ORDER BY n DESC, id LIMIT 4",
	"SELECT id FROM t WHERE n = 1 OR n = 3 ORDER BY id",
	"SELECT id, n FROM t WHERE n = 1 OR n = 3 ORDER BY n DESC LIMIT 5",
	"SELECT id, n FROM t WHERE n IN [0, 2] ORDER BY n LIMIT 3 START 2",
	// A fused bitmap, with and without an overflow fallback.
	"SELECT id, secret, label FROM t WHERE n = 1 OR m = 2 ORDER BY id",
	"SELECT id FROM t WHERE n = 1 AND m = 2 ORDER BY id",
	"SELECT count() FROM t WHERE n = 1 OR m = 2 GROUP ALL",
];

/// Statements against the lightweight relations.
const EDGE_READS: &[&str] = &[
	"SELECT in, out FROM pal ORDER BY in",
	"SELECT * FROM pal ORDER BY in",
	"SELECT in, out FROM private_pal ORDER BY in",
	"SELECT in, out FROM sealed_pal",
	"SELECT count() FROM pal GROUP ALL",
	"SELECT * FROM pal LIMIT 1",
	"SELECT * FROM pal WHERE in = person:a",
];

/// Statements against a table whose compound index delivers an order a union
/// of its branches merges.
const UNION_READS: &[&str] = &[
	"SELECT id, age FROM pairs WHERE kind = 1 OR kind = 2 ORDER BY age",
	"SELECT id, age FROM pairs WHERE kind = 1 OR kind = 2 ORDER BY age DESC",
	"SELECT id, age FROM pairs WHERE kind = 1 OR kind = 2 ORDER BY age LIMIT 3 START 2",
	"SELECT id, age FROM pairs WHERE kind = 1 OR kind = 2 ORDER BY age DESC LIMIT 3",
	"SELECT id, age FROM pairs WHERE kind = 1 OR kind = 2 ORDER BY age DESC LIMIT 1",
	"SELECT id, age FROM pairs WHERE kind = 1 OR kind = 2 ORDER BY age LIMIT 0",
	"SELECT id, age FROM pairs WHERE kind IN [1, 2, 3] ORDER BY age DESC LIMIT 7",
	"SELECT id, age FROM pairs WHERE kind IN [1, 2, 3] ORDER BY age LIMIT 7 START 4",
	"SELECT id FROM pairs WHERE kind = 1 OR kind = 2 LIMIT 3",
];

/// Statements whose scan reads under a top-k pushdown.
const TOP_K_READS: &[&str] = &[
	"SELECT n FROM item ORDER BY n DESC LIMIT 3",
	"SELECT n FROM item ORDER BY n LIMIT 3",
	"SELECT n FROM item ORDER BY n DESC LIMIT 7 START 2",
	"SELECT n, g FROM item WHERE g = 1 ORDER BY n DESC LIMIT 4",
	"SELECT n, g FROM item ORDER BY n DESC, g LIMIT 6",
	"SELECT id, (SELECT VALUE n FROM item WHERE g = $parent.g ORDER BY n DESC LIMIT 2) AS top \
	 FROM grp ORDER BY id",
	"SELECT id, (SELECT VALUE n FROM item ORDER BY n LIMIT 3) AS low FROM grp ORDER BY id",
	"fn::top(3)",
	"[fn::top(1), fn::top(2)]",
];

pub(super) fn session(user: &str) -> crate::dbs::Session {
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", user));
	crate::dbs::Session::for_record("test", "test", "user", rid)
}

/// A record user whose tenant is not the database the data is in.
fn foreign() -> crate::dbs::Session {
	crate::dbs::Session {
		au: Arc::new(crate::iam::Auth::for_record(
			"user:tobie".to_owned(),
			"other",
			"test",
			"user",
		)),
		..session("tobie")
	}
}

/// Run `sql` on the VM over `ctx`, answering its rows and how many
/// compilations running the plan made; `None` where the VM declines the
/// statement.
pub(super) async fn run(
	ctx: &ExecutionContext,
	sql: &str,
) -> Option<(Result<Vec<Value>, String>, usize)> {
	let bound = bind(ctx, sql).await;
	let op = super::plan(&planner(ctx), &bound).await?;
	let before = crate::exec::compiles::count();
	let rows = outcome(&op, ctx).await;
	Some((rows, crate::exec::compiles::count() - before))
}

/// The program `sql` lowers to for `ctx`.
pub(super) async fn program_of(ctx: &ExecutionContext, sql: &str) -> Program {
	let bound = bind(ctx, sql).await;
	let planner = planner(ctx);
	let physical =
		super::optimize::optimize(super::map::map(&planner, &bound).await.expect("statement maps"));
	super::lower::lower(&planner, &physical).await.expect("program lowers")
}

/// The kinds of source `source` and the sources it holds read.
fn kinds(source: &Source, out: &mut Vec<&'static str>) {
	match source {
		Source::Scan {
			topk,
			..
		} => out.push(if topk.is_some() {
			"scan with top-k"
		} else {
			"scan"
		}),
		Source::Record {
			..
		} => out.push("record"),
		Source::Index {
			..
		} => out.push("index"),
		Source::IndexUnion {
			merge,
			..
		} => out.push(if merge.is_some() {
			"merged union"
		} else {
			"union"
		}),
		Source::Bitmap {
			fallback,
			..
		} => {
			out.push("bitmap");
			if let Some(fallback) = fallback {
				out.push("bitmap fallback");
				kinds(fallback, out);
			}
		}
		Source::FullText {
			..
		} => out.push("full-text"),
		Source::Knn {
			..
		} => out.push("knn"),
		Source::LightweightEdges {
			..
		} => out.push("edges"),
		Source::Union(sources) => sources.iter().for_each(|s| kinds(s, out)),
		_ => {}
	}
}

async fn kinds_of(ctx: &ExecutionContext, sql: &str) -> Vec<&'static str> {
	let program = program_of(ctx, sql).await;
	let mut out = Vec::new();
	for pipeline in &program.pipelines {
		kinds(&pipeline.source, &mut out);
	}
	out
}

fn queries() -> Vec<String> {
	let mut out = Vec::new();
	for table in ["open", "mine", "locked"] {
		out.extend(TABLE_READS.iter().map(|sql| sql.replace(" t ", &format!(" {table} "))));
	}
	out.extend(EDGE_READS.iter().map(|sql| (*sql).to_owned()));
	out.extend(TOP_K_READS.iter().map(|sql| (*sql).to_owned()));
	out.extend(UNION_READS.iter().map(|sql| (*sql).to_owned()));
	out.sort();
	out.dedup();
	out
}

/// Every statement answers its rows, for the owner and for two record users,
/// and compiles nothing while it runs: the table's permission, a field's and
/// the computed field are the program's, applied to the rows each source reads,
/// whatever the permission is.
#[tokio::test]
async fn sources_answer_their_rows_and_compile_nothing() {
	let mut golden = Golden::new("sources_answer_their_rows_and_compile_nothing");
	let db = TestDb::new_with_auth(&tables()).await;
	let mut declined = Vec::new();
	let mut compiled = Vec::new();
	let mut nonempty = 0;
	for (who, session) in
		[("owner", TestDb::owner()), ("alice", session("alice")), ("bob", session("bob"))]
	{
		let ctx = db.exec_ctx_as(&session, crate::kvs::TransactionType::Read).await;
		for sql in queries() {
			let Some((vm, compiles)) = run(&ctx, &sql).await else {
				// A vector predicate over a table a checked caller may not
				// select from is not planned by the VM.
				let not_planned =
					who != "owner" && sql.contains("FROM locked") && sql.contains("<|");
				if !not_planned {
					declined.push(format!("`{sql}` for {who}"));
				}
				continue;
			};
			golden.rows(format!("{who}: {sql}"), &vm);
			if matches!(&vm, Ok(rows) if !rows.is_empty()) {
				nonempty += 1;
			}
			if compiles > 0 {
				compiled.push(format!("`{sql}` for {who}: {compiles} compilations"));
			}
		}
	}
	assert!(compiled.is_empty(), "{compiled:#?}");
	assert!(declined.is_empty(), "the VM declined {declined:#?}");
	assert!(nonempty > 100, "the statements read too little: {nonempty}");
	golden.check();
}

/// The statements read through each kind of source they name.
#[tokio::test]
async fn the_statements_read_through_the_sources_they_name() {
	let db = TestDb::new_with_auth(&tables()).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let mut seen = std::collections::BTreeSet::new();
	for sql in queries() {
		seen.extend(kinds_of(&ctx, &sql).await);
	}
	for kind in [
		"scan with top-k",
		"index",
		"merged union",
		"bitmap",
		"bitmap fallback",
		"full-text",
		"knn",
		"edges",
	] {
		assert!(seen.contains(kind), "no statement read through a {kind} source: {seen:?}");
	}
}

/// A table's permission is applied once, by the program, to the rows a
/// source reads: the program holds the one permission of the table, and a
/// record user sees exactly the rows the permission admits, no fewer for a
/// double filter and no more for a gap. A nearest-neighbour search also
/// judges the permission of each candidate inside the search, so that the
/// rows it returns are the nearest the user may see; its program holds the
/// permission a second time for that.
#[tokio::test]
async fn a_table_permission_applies_once_to_what_each_source_reads() {
	let mut golden = Golden::new("a_table_permission_applies_once_to_what_each_source_reads");
	let db = TestDb::new_with_auth(&tables()).await;
	let alice = db.exec_ctx_as(&session("alice"), crate::kvs::TransactionType::Read).await;
	for sql in [
		"SELECT id FROM mine WHERE body @1@ 'beta' ORDER BY id",
		"SELECT id FROM mine WHERE n = 1 ORDER BY id",
		"SELECT id FROM mine WHERE n = 1 OR n = 3 ORDER BY id",
		"SELECT id FROM mine WHERE n = 1 OR m = 2 ORDER BY id",
		"SELECT id FROM private_pal",
	] {
		let program = program_of(&alice, sql).await;
		assert_eq!(program.permissions.len(), 1, "`{sql}`");
	}
	let nearest = "SELECT id FROM mine WHERE emb <|24, 40|> [3.0, 0.0, 0.0] ORDER BY id";
	assert_eq!(program_of(&alice, nearest).await.permissions.len(), 2, "`{nearest}`");
	// Alice owns the even rows: of the rows holding `beta`, those only.
	let (rows, _) = run(&alice, "SELECT VALUE id FROM mine WHERE body @1@ 'beta' ORDER BY id")
		.await
		.expect("covered");
	let ids: Vec<i64> = rows
		.expect("reads")
		.into_iter()
		.map(|v| match v {
			Value::RecordId(rid) => match rid.key {
				crate::val::RecordIdKey::Number(n) => n,
				other => panic!("{other:?}"),
			},
			other => panic!("{other:?}"),
		})
		.collect();
	assert_eq!(ids.len(), 12, "{ids:?}");
	assert!(ids.iter().all(|id| id % 2 == 0), "{ids:?}");
	// A nearest-neighbour search returns the k nearest rows the permission
	// admits: Alice sees the even rows, so the four nearest are 2, 4, 6 and 8.
	let sql = "SELECT VALUE id FROM mine WHERE emb <|4, 40|> [1.0, 0.0, 0.0] ORDER BY id";
	let (nearest, _) = run(&alice, sql).await.expect("covered");
	golden.rows(sql, &nearest);
	// A denied table yields nothing, and a count over it is zero.
	for sql in [
		"SELECT * FROM locked WHERE n = 1",
		"SELECT * FROM locked WHERE n = 1 OR m = 2",
		"SELECT * FROM sealed_pal",
	] {
		let (rows, _) = run(&alice, sql).await.expect("covered");
		assert_eq!(rows, Ok(vec![]), "`{sql}`");
	}
	let (rows, _) = run(&alice, "SELECT count() FROM locked WHERE n = 1 OR m = 2 GROUP ALL")
		.await
		.expect("covered");
	assert_eq!(rows.expect("reads").len(), 1);
	golden.check();
}

/// A principal of another tenant is refused by every source before it reads
/// anything.
#[tokio::test]
async fn a_principal_of_another_tenant_is_refused_by_every_source() {
	let db = TestDb::new_with_auth(&tables()).await;
	let owner = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let ctx = db.exec_ctx_as(&foreign(), crate::kvs::TransactionType::Read).await;
	for sql in [
		"SELECT * FROM open WHERE body @1@ 'beta'",
		"SELECT * FROM open WHERE emb <|4, 40|> [1.0, 0.0, 0.0]",
		"SELECT * FROM open WHERE n = 1",
		"SELECT * FROM open WHERE n = 1 OR n = 3",
		"SELECT * FROM open WHERE n = 1 OR m = 2",
		"SELECT * FROM pal",
		"SELECT n FROM item ORDER BY n DESC LIMIT 3",
	] {
		let bound = bind(&owner, sql).await;
		let vm = super::plan(&planner(&owner), &bound).await.expect("the VM covers it");
		let vm = outcome(&vm, &ctx).await;
		assert!(vm.is_err(), "`{sql}` was read: {vm:?}");
	}
}

/// A program whose scan is ordered under a limit runs as often as it is
/// started, a run inside another run of the same program included, each run
/// with the threshold of its own sort.
#[tokio::test]
async fn a_top_k_threshold_belongs_to_one_run_of_its_program() {
	let mut golden = Golden::new("a_top_k_threshold_belongs_to_one_run_of_its_program");
	let db = TestDb::new_with_auth(&tables()).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	for sql in TOP_K_READS {
		let (vm, _) = run(&ctx, sql).await.expect("covered");
		golden.rows(*sql, &vm);
	}
	let program = program_of(&ctx, "SELECT n FROM item ORDER BY n DESC LIMIT 3").await;
	assert!(program.sorts.iter().any(|sort| sort.pushdown));
	let topk: Vec<_> = program.pipelines.iter().filter_map(|p| p.source.topk()).collect();
	assert_eq!(topk.len(), 1);
	assert!(program.sorts[topk[0].sort as usize].pushdown);
	// One program started twice at once: each run rejects by its own sort's
	// threshold, and answers what one run alone answers.
	for sql in [
		"SELECT n FROM item ORDER BY n DESC LIMIT 3",
		"SELECT id, (SELECT VALUE n FROM item WHERE g = $parent.g ORDER BY n DESC LIMIT 2) AS top \
		 FROM grp ORDER BY id",
	] {
		let bound = bind(&ctx, sql).await;
		let op = super::plan(&planner(&ctx), &bound).await.expect("covered");
		let (a, b) = futures::future::join(outcome(&op, &ctx), outcome(&op, &ctx)).await;
		let (alone, _) = run(&ctx, sql).await.expect("covered");
		assert_eq!(a, alone, "`{sql}`");
		assert_eq!(b, alone, "`{sql}`");
	}
	golden.check();
}

/// A sort whose first key is compared under `COLLATE` or `NUMERIC` publishes
/// no threshold: the scan compares plain values, which orders such keys
/// differently.
#[tokio::test]
async fn a_collated_or_numeric_first_key_publishes_no_top_k_threshold() {
	let db = TestDb::new_with_auth(&tables()).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	for sql in [
		"SELECT n FROM item ORDER BY n COLLATE DESC LIMIT 3",
		"SELECT n FROM item ORDER BY n NUMERIC DESC LIMIT 3",
		"SELECT n FROM item ORDER BY n COLLATE DESC, g DESC LIMIT 3",
	] {
		let program = program_of(&ctx, sql).await;
		assert!(program.sorts.iter().all(|sort| !sort.pushdown), "`{sql}`");
		assert!(program.pipelines.iter().all(|p| p.source.topk().is_none()), "`{sql}`");
	}
	// The control: the same shape under the default comparison publishes one.
	let program = program_of(&ctx, "SELECT n FROM item ORDER BY n DESC, g DESC LIMIT 3").await;
	assert!(program.sorts.iter().any(|sort| sort.pushdown));
}

/// An index union that delivers its statement's order merges its branches
/// ascending and descending, and stops reading once a limit is spent.
#[tokio::test]
async fn an_index_union_merges_in_either_direction_and_stops_at_its_limit() {
	let mut golden =
		Golden::new("an_index_union_merges_in_either_direction_and_stops_at_its_limit");
	let db = TestDb::new_with_auth(&tables()).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	for sql in UNION_READS {
		let (vm, _) = run(&ctx, sql).await.expect("covered");
		golden.rows(*sql, &vm);
	}
	golden.check();
	for sql in [
		"SELECT id, age FROM pairs WHERE kind = 1 OR kind = 2 ORDER BY age",
		"SELECT id, age FROM pairs WHERE kind = 1 OR kind = 2 ORDER BY age DESC LIMIT 3",
	] {
		let kinds = kinds_of(&ctx, sql).await;
		assert!(kinds.contains(&"merged union"), "`{sql}`: {kinds:?}");
	}
}

/// A table that is a lightweight relation in the catalog a statement is
/// planned against is read as its edges, and one that is not is read as a
/// record range; a table found the other way when the program runs is an
/// error and not a re-plan.
#[tokio::test]
async fn a_lightweight_relation_is_decided_when_the_statement_is_planned() {
	let db = TestDb::new_with_auth(&tables()).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	assert_eq!(kinds_of(&ctx, "SELECT * FROM pal").await, ["edges"]);
	assert_eq!(kinds_of(&ctx, "SELECT * FROM person").await, ["scan"]);
	assert_eq!(kinds_of(&ctx, "SELECT * FROM pal:[person:a, person:b]").await, ["edges"]);
	// Planned as a record range, then redefined as a lightweight relation.
	let run_ddl = |sql: &'static str| {
		let ctx = &ctx;
		async move {
			let bound = bind(ctx, sql).await;
			let op = super::plan(&planner(ctx), &bound).await.expect("covered");
			outcome(&op, ctx).await.expect("runs");
		}
	};
	run_ddl("DEFINE TABLE plain_then_light SCHEMALESS").await;
	let bound = bind(&ctx, "SELECT * FROM plain_then_light").await;
	let op = super::plan(&planner(&ctx), &bound).await.expect("covered");
	run_ddl("REMOVE TABLE plain_then_light").await;
	run_ddl("DEFINE TABLE plain_then_light TYPE RELATION IN person OUT person LIGHTWEIGHT").await;
	let refused = outcome(&op, &ctx).await;
	assert!(
		refused.as_ref().is_err_and(|e| e.contains("lightweight")),
		"a scan planned as a record range read a lightweight relation: {refused:?}"
	);
}
