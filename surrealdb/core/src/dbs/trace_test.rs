//! The trace a streaming execution sends when its request asks for one: what
//! it carries, which statement it belongs to, and that it stands or falls with
//! that statement's outcome.

use std::sync::Arc;

use surrealdb_rpc::{QUERY_STREAM_BUFFER, Trace, TraceKinds};
use surrealdb_types::Value as PublicValue;

use crate::catalog::providers::CatalogProvider;
use crate::dbs::{QueryStreamItem, Session};
use crate::kvs::{Datastore, QueryRequest, TransactionType};

async fn datastore() -> Arc<Datastore> {
	let ds = Datastore::new("memory").await.expect("a memory datastore");
	let tx = ds.transaction(TransactionType::Write).await.expect("a write transaction");
	tx.ensure_ns_db(None, "test", "test").await.expect("the test namespace and database");
	tx.commit().await.expect("committing the namespace");
	for result in ds.execute("DEFINE TABLE person", &session(), None).await.expect("the seed runs")
	{
		result.result.expect("the seed succeeds");
	}
	ds
}

fn session() -> Session {
	Session::owner().with_ns("test").with_db("test").with_rt(true)
}

async fn items(
	ds: &Arc<Datastore>,
	sql: &str,
	trace: TraceKinds,
	held: bool,
) -> Vec<QueryStreamItem> {
	items_as(ds, &session(), sql, trace, held).await
}

async fn items_as(
	ds: &Arc<Datastore>,
	session: &Session,
	sql: &str,
	trace: TraceKinds,
	held: bool,
) -> Vec<QueryStreamItem> {
	let (tx, rx) = async_channel::bounded(QUERY_STREAM_BUFFER);
	let mut request = QueryRequest::new(sql, session).with_trace(trace);
	if held {
		request = request.with_held_transactions();
	}
	let job = ds.run_streaming(request, tx).expect("the query parses");
	let drain = async {
		let mut items = Vec::new();
		while let Ok(item) = rx.recv().await {
			items.push(item);
		}
		items
	};
	let (outcome, items) = futures::future::join(job.run, drain).await;
	outcome.expect("the query runs");
	items
}

/// Every trace item, by statement.
fn traces(items: &[QueryStreamItem]) -> Vec<(usize, &Trace)> {
	items
		.iter()
		.filter_map(|item| {
			let QueryStreamItem::Trace {
				index,
				trace,
			} = item
			else {
				return None;
			};
			Some((*index, &**trace))
		})
		.collect()
}

/// Where statement `index`'s trace item sits relative to its `Finished`.
fn trace_precedes_finished(items: &[QueryStreamItem], index: usize) -> bool {
	let at = |finished: bool| {
		items.iter().position(|item| {
			item.index() == index
				&& matches!(item, QueryStreamItem::Finished { .. }) == finished
				&& matches!(item, QueryStreamItem::Trace { .. } | QueryStreamItem::Finished { .. })
		})
	};
	matches!((at(false), at(true)), (Some(trace), Some(finished)) if trace < finished)
}

#[tokio::test]
async fn a_let_reports_what_it_bound_on_its_own_statement() {
	let ds = datastore().await;
	let items = items(
		&ds,
		"LET $vip = [person:a, person:b]; SELECT * FROM 1; LET $n = 3;",
		TraceKinds::VARIABLES,
		false,
	)
	.await;
	let traces = traces(&items);
	assert_eq!(traces.len(), 2, "one trace per LET: {traces:?}");
	assert_eq!(traces[0].0, 0);
	assert_eq!(
		traces[0].1.variables,
		vec![(
			"vip".to_owned(),
			PublicValue::Array(
				vec![
					PublicValue::RecordId(surrealdb_types::RecordId::new("person", "a")),
					PublicValue::RecordId(surrealdb_types::RecordId::new("person", "b")),
				]
				.into()
			)
		)]
	);
	assert_eq!(traces[1].0, 2);
	assert_eq!(traces[1].1.variables, vec![("n".to_owned(), PublicValue::from_t(3i64))]);
	assert!(trace_precedes_finished(&items, 0));
	assert!(trace_precedes_finished(&items, 2));
}

#[tokio::test]
async fn a_live_query_reports_its_id() {
	let ds = datastore().await;
	let items = items(&ds, "LIVE SELECT * FROM person;", TraceKinds::SUBSCRIPTIONS, false).await;
	let Some(QueryStreamItem::Value {
		value: PublicValue::Uuid(id),
		..
	}) = items.iter().find(|item| matches!(item, QueryStreamItem::Value { .. }))
	else {
		panic!("a LIVE SELECT answers its id: {items:?}");
	};
	let traces = traces(&items);
	assert_eq!(traces.len(), 1, "{traces:?}");
	assert_eq!(traces[0].1.subscriptions, vec![*id]);
	assert!(trace_precedes_finished(&items, 0));
}

/// A kind not asked for is not recorded, and no trace at all is sent for a
/// request that asks for none.
#[tokio::test]
async fn only_the_kinds_asked_for_are_sent() {
	let ds = datastore().await;
	let sql = "LET $a = 1; LIVE SELECT * FROM person;";
	assert!(traces(&items(&ds, sql, TraceKinds::NONE, false).await).is_empty());
	let only_variables = items(&ds, sql, TraceKinds::VARIABLES, false).await;
	let only_variables = traces(&only_variables);
	assert_eq!(only_variables.len(), 1);
	assert!(only_variables[0].1.subscriptions.is_empty());
	let only_lives = items(&ds, sql, TraceKinds::SUBSCRIPTIONS, false).await;
	let only_lives = traces(&only_lives);
	assert_eq!(only_lives.len(), 1);
	assert_eq!(only_lives[0].0, 1);
	assert!(only_lives[0].1.variables.is_empty());
}

/// A statement whose block is rolled back is retracted, and its trace with
/// it: no trace item is sent for it, held or not.
#[tokio::test]
async fn a_rolled_back_statement_sends_no_trace() {
	for held in [false, true] {
		let ds = datastore().await;
		let items = items(
			&ds,
			"BEGIN; LET $a = 1; LIVE SELECT * FROM person; THROW 'no'; COMMIT;",
			TraceKinds::VARIABLES.union(TraceKinds::SUBSCRIPTIONS),
			held,
		)
		.await;
		assert!(traces(&items).is_empty(), "held: {held}: {items:?}");
	}
}

/// A `LET` that fails binds nothing and reports nothing.
#[tokio::test]
async fn a_failed_let_sends_no_trace() {
	let ds = datastore().await;
	let items = items(&ds, "LET $a = <int> 'x';", TraceKinds::VARIABLES, false).await;
	assert!(traces(&items).is_empty(), "{items:?}");
}

const GRAPH: &str = "
	CREATE person:a, person:b, person:c, person:d;
	CREATE place:lib SET name = 'Library';
	CREATE place:cafe SET name = 'Cafe';
	CREATE post:p1, post:p2;
	RELATE person:a->customer_of:a_lib->place:lib SET since = 2019;
	RELATE person:a->customer_of:a_cafe->place:cafe SET since = 2023;
	RELATE person:b->customer_of:b_lib->place:lib SET since = 2024;
	RELATE person:a->works_at:a_lib->place:lib;
	RELATE person:a->knows:ab->person:b;
	RELATE person:b->knows:bc->person:c;
	RELATE person:c->knows:cd->person:d;
	RELATE person:a->knows:ac->person:c;
	RELATE person:b->likes:b_p1->post:p1;
	RELATE person:c->likes:c_p2->post:p2;
";

async fn graph() -> Arc<Datastore> {
	let ds = datastore().await;
	for result in ds.execute(GRAPH, &session(), None).await.expect("the graph is written") {
		result.result.expect("every write succeeds");
	}
	ds
}

/// Statement `index`'s relations as `in-edge-out` keys, sorted, across every
/// trace item it sent.
fn relations(items: &[QueryStreamItem], index: usize) -> Vec<String> {
	let key = |id: &Option<surrealdb_types::RecordId>| match id {
		Some(id) => surrealdb_types::ToSql::to_sql(id),
		None => "NONE".to_owned(),
	};
	let mut out: Vec<String> = traces(items)
		.into_iter()
		.filter(|(at, _)| *at == index)
		.flat_map(|(_, trace)| trace.relations.iter())
		.map(|r| format!("{} {} {}", key(&r.r#in), key(&Some(r.edge.clone())), key(&r.out)))
		.collect();
	out.sort();
	out.dedup();
	out
}

fn partial(items: &[QueryStreamItem], index: usize) -> bool {
	traces(items).into_iter().any(|(at, trace)| at == index && trace.partial)
}

fn rels(entries: &[&str]) -> Vec<String> {
	let mut out: Vec<String> = entries.iter().map(|e| (*e).to_owned()).collect();
	out.sort();
	out
}

/// What reaches the result is traced, cut to what leads to it: another edge
/// table of the same records, a filtered edge and a dead end are not.
#[tokio::test]
async fn a_returned_lookup_reports_the_edges_behind_its_value() {
	let ds = graph().await;
	let sql = "
		SELECT ->customer_of->place AS p FROM person:a;
		SELECT ->knows->person->likes->post AS p FROM person:a;
		SELECT VALUE ->customer_of[WHERE since > 2020]->place FROM person:a;
		RETURN person:c->knows->person->likes->post;
		SELECT * FROM person:a->knows->person;
	";
	let items = items(&ds, sql, TraceKinds::RELATIONS, false).await;
	assert_eq!(
		relations(&items, 0),
		rels(&["person:a customer_of:a_cafe place:cafe", "person:a customer_of:a_lib place:lib"])
	);
	assert_eq!(
		relations(&items, 1),
		rels(&[
			"person:a knows:ab person:b",
			"person:a knows:ac person:c",
			"person:b likes:b_p1 post:p1",
			"person:c likes:c_p2 post:p2",
		])
	);
	assert_eq!(relations(&items, 2), rels(&["person:a customer_of:a_cafe place:cafe"]));
	assert_eq!(relations(&items, 3), Vec::<String>::new(), "person:d likes no post");
	assert_eq!(
		relations(&items, 4),
		rels(&["person:a knows:ab person:b", "person:a knows:ac person:c"])
	);
}

/// A field read after the lookup does not cut its trace: the anchor is the
/// records the lookup answered.
#[tokio::test]
async fn a_field_read_after_the_lookup_keeps_its_edges() {
	let ds = graph().await;
	let items = items(
		&ds,
		"SELECT ->customer_of->place.name AS n FROM person:a;",
		TraceKinds::RELATIONS,
		false,
	)
	.await;
	assert_eq!(
		relations(&items, 0),
		rels(&["person:a customer_of:a_cafe place:cafe", "person:a customer_of:a_lib place:lib"])
	);
}

/// A recursion is traced at every depth.
#[tokio::test]
async fn a_recursion_reports_every_depth() {
	let ds = graph().await;
	let items =
		items(&ds, "RETURN person:a.{..3+collect}(->knows->person);", TraceKinds::RELATIONS, false)
			.await;
	assert_eq!(
		relations(&items, 0),
		rels(&[
			"person:a knows:ab person:b",
			"person:a knows:ac person:c",
			"person:b knows:bc person:c",
			"person:c knows:cd person:d",
		])
	);
}

/// A condition is not the result: nothing it walks is reported, and it does
/// not make the trace partial. A lookup fed through a function does.
#[tokio::test]
async fn conditions_are_not_traced_and_functions_are_partial() {
	let ds = graph().await;
	let sql = "
		SELECT id FROM person WHERE ->knows->person CONTAINS person:b;
		SELECT count(->knows) AS n FROM person:a;
	";
	let items = items(&ds, sql, TraceKinds::RELATIONS, false).await;
	assert_eq!(relations(&items, 0), Vec::<String>::new());
	assert!(!partial(&items, 0));
	assert_eq!(relations(&items, 1), Vec::<String>::new());
	assert!(partial(&items, 1));
}

/// A write's `RETURN` that walks the graph is traced; one that reads the ids
/// the write stored walks nothing, and its `SET` is what it writes.
#[tokio::test]
async fn a_writes_return_clause_is_traced() {
	let ds = graph().await;
	let sql = "
		UPDATE person:a SET friends = ->knows->person RETURN ->knows->person AS f;
		UPDATE person:a SET friends = ->knows->person RETURN friends;
	";
	let items = items(&ds, sql, TraceKinds::RELATIONS, false).await;
	assert_eq!(
		relations(&items, 0),
		rels(&["person:a knows:ab person:b", "person:a knows:ac person:c"])
	);
	assert_eq!(relations(&items, 1), Vec::<String>::new());
}

/// The trace follows the rows: a row `LIMIT` drops reports nothing.
#[tokio::test]
async fn a_row_the_limit_drops_reports_nothing() {
	let ds = graph().await;
	let items = items(
		&ds,
		"SELECT id, ->knows->person AS f FROM person ORDER BY id LIMIT 1;",
		TraceKinds::RELATIONS,
		false,
	)
	.await;
	assert_eq!(
		relations(&items, 0),
		rels(&["person:a knows:ab person:b", "person:a knows:ac person:c"])
	);
}

/// A request that asks for no trace gets the same rows, and no trace.
#[tokio::test]
async fn tracing_does_not_change_the_rows() {
	let ds = graph().await;
	let sql = "SELECT id, ->knows->person AS f, ->knows->person->likes->post AS p FROM person ORDER BY id;";
	let rows = |items: &[QueryStreamItem]| -> Vec<String> {
		items
			.iter()
			.filter_map(|item| {
				let QueryStreamItem::Rows {
					values,
					..
				} = item
				else {
					return None;
				};
				Some(format!("{values:?}"))
			})
			.collect()
	};
	let plain = items(&ds, sql, TraceKinds::NONE, false).await;
	let traced = items(&ds, sql, TraceKinds::RELATIONS, false).await;
	assert_eq!(rows(&plain), rows(&traced));
	assert!(traces(&plain).is_empty());
	assert!(!traces(&traced).is_empty());
}

const PERMISSIONS: &str = "
	DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid ) DURATION FOR TOKEN 1h, FOR SESSION 1h;
	DEFINE TABLE pal SCHEMALESS PERMISSIONS FOR select WHERE hidden != true;
	DEFINE TABLE knows TYPE RELATION SCHEMALESS PERMISSIONS FOR select FULL;
	DEFINE TABLE blocks TYPE RELATION SCHEMALESS PERMISSIONS FOR select NONE;
	DEFINE TABLE follows TYPE RELATION SCHEMALESS PERMISSIONS FOR select FULL;
	DEFINE FIELD OVERWRITE out ON follows TYPE record<pal> PERMISSIONS FOR select NONE;
	CREATE pal:a, pal:b;
	CREATE pal:h SET hidden = true;
	RELATE pal:a->knows:ab->pal:b;
	RELATE pal:a->knows:ah->pal:h;
	RELATE pal:a->blocks:ab->pal:b;
	RELATE pal:a->follows:ab->pal:b;
	RELATE pal:a->follows:ah->pal:h;
";

/// SECURITY: a record user is reported only what it may read: no hidden
/// vertex, no edge of a table it cannot read, and the ends of an edge as its
/// field permissions show them.
#[tokio::test]
async fn a_record_user_is_traced_only_what_it_may_read() {
	let ds = datastore().await;
	for result in ds.execute(PERMISSIONS, &session(), None).await.expect("the setup runs") {
		result.result.expect("every setup statement succeeds");
	}
	let user = Session::for_record(
		"test",
		"test",
		"user",
		PublicValue::RecordId(surrealdb_types::RecordId::new("pal", "a")),
	);
	let sql = "
		RETURN pal:a->knows->pal;
		RETURN pal:a->blocks->pal;
		RETURN pal:a->follows;
		RETURN pal:a->follows->pal;
		RETURN (pal:a->follows)[0];
	";
	let items = items_as(&ds, &user, sql, TraceKinds::RELATIONS, false).await;
	assert_eq!(relations(&items, 0), rels(&["pal:a knows:ab pal:b"]), "pal:h is hidden");
	assert_eq!(relations(&items, 1), Vec::<String>::new(), "blocks cannot be read");
	assert_eq!(
		relations(&items, 2),
		rels(&["pal:a follows:ab NONE", "pal:a follows:ah NONE"]),
		"the out field of follows is hidden"
	);
	assert_eq!(
		relations(&items, 3),
		rels(&["pal:a follows:ab NONE"]),
		"a walk that continues past the edges still shows their ends as the user reads them"
	);
	assert_eq!(
		relations(&items, 4),
		rels(&["pal:a follows:ab NONE"]),
		"a step that cuts the edges reports their ends as the user reads them"
	);
}

/// A reference lookup reports `[referrer, referenced]`, the record holding the
/// field first, for what it answered.
#[tokio::test]
async fn a_reference_lookup_reports_its_references() {
	let ds = datastore().await;
	let setup = "
		DEFINE FIELD author ON post TYPE option<record<person>> REFERENCE;
		CREATE person:a, person:b;
		CREATE post:1 SET author = person:a;
		CREATE post:2 SET author = person:a;
		CREATE post:3 SET author = person:b;
	";
	for result in ds.execute(setup, &session(), None).await.expect("the setup runs") {
		result.result.expect("every setup statement succeeds");
	}
	let items = items(
		&ds,
		"SELECT id, <~post AS posts FROM person:a; SELECT id FROM person WHERE count(<~post) > 1;",
		TraceKinds::REFERENCES,
		false,
	)
	.await;
	let references = |index: usize| {
		let mut out: Vec<String> = traces(&items)
			.into_iter()
			.filter(|(at, _)| *at == index)
			.flat_map(|(_, trace)| trace.references.iter())
			.map(|(from, to)| {
				format!(
					"{} {}",
					surrealdb_types::ToSql::to_sql(from),
					surrealdb_types::ToSql::to_sql(to)
				)
			})
			.collect();
		out.sort();
		out
	};
	assert_eq!(references(0), vec!["post:1 person:a".to_owned(), "post:2 person:a".to_owned()]);
	assert_eq!(references(1), Vec::<String>::new(), "a condition is not traced");
}

const LINKED: &str = "
	DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid ) DURATION FOR TOKEN 1h, FOR SESSION 1h;
	DEFINE TABLE post SCHEMALESS PERMISSIONS FOR select FULL;
	DEFINE TABLE secret SCHEMALESS PERMISSIONS FOR select FULL;
	DEFINE FIELD owner ON secret PERMISSIONS FOR select NONE;
	CREATE org:sdb SET name = 'SurrealDB';
	CREATE person:tobie SET name = 'Tobie', org = org:sdb;
	CREATE tag:rust SET label = 'rust';
	CREATE tag:db SET label = 'db';
	CREATE post:1 SET title = 'Hello', author = person:tobie, tags = [tag:rust, tag:db];
	CREATE secret:1 SET owner = person:tobie;
";

async fn linked() -> Arc<Datastore> {
	let ds = datastore().await;
	for result in ds.execute(LINKED, &session(), None).await.expect("the setup runs") {
		result.result.expect("every setup statement succeeds");
	}
	ds
}

/// Statement `index`'s links as `record field target` keys, sorted.
fn links(items: &[QueryStreamItem], index: usize) -> Vec<String> {
	let mut out: Vec<String> = traces(items)
		.into_iter()
		.filter(|(at, _)| *at == index)
		.flat_map(|(_, trace)| trace.links.iter())
		.map(|link| {
			format!(
				"{} {} {}",
				surrealdb_types::ToSql::to_sql(&link.record),
				link.field,
				surrealdb_types::ToSql::to_sql(&link.target)
			)
		})
		.collect();
	out.sort();
	out.dedup();
	out
}

/// A link the result reads through is reported; one it returns without
/// reading through, or reads in a condition, is not.
#[tokio::test]
async fn a_link_the_result_reads_through_is_reported() {
	let ds = linked().await;
	let sql = "
		SELECT title, author.name FROM post:1;
		SELECT VALUE tags.*.label FROM post:1;
		SELECT title FROM post WHERE author.name = 'Tobie';
		SELECT author FROM post:1;
		SELECT author.org.name FROM post:1;
		RETURN (SELECT * FROM ONLY post:1).author.name;
	";
	let items = items(&ds, sql, TraceKinds::LINKS, false).await;
	assert_eq!(links(&items, 0), vec!["post:1 author person:tobie".to_owned()]);
	assert_eq!(
		links(&items, 1),
		vec!["post:1 tags tag:db".to_owned(), "post:1 tags tag:rust".to_owned()]
	);
	assert_eq!(links(&items, 2), Vec::<String>::new());
	assert_eq!(links(&items, 3), Vec::<String>::new());
	assert_eq!(
		links(&items, 4),
		vec!["person:tobie org org:sdb".to_owned(), "post:1 author person:tobie".to_owned()]
	);
	// A holder only a subquery names is not evaluated twice: no link, and the
	// trace says it is partial.
	assert_eq!(links(&items, 5), Vec::<String>::new());
	assert!(traces(&items).iter().any(|(at, trace)| *at == 5 && trace.partial));
}

/// Tracing links does not move a value in the result.
#[tokio::test]
async fn tracing_links_does_not_change_the_rows() {
	let ds = linked().await;
	let sql = "SELECT title, author.name, tags.*.label FROM post:1;";
	let rows = |items: &[QueryStreamItem]| -> Vec<String> {
		items
			.iter()
			.filter_map(|item| {
				let QueryStreamItem::Rows {
					values,
					..
				} = item
				else {
					return None;
				};
				Some(format!("{values:?}"))
			})
			.collect()
	};
	let plain = items(&ds, sql, TraceKinds::NONE, false).await;
	let traced = items(&ds, sql, TraceKinds::LINKS, false).await;
	assert_eq!(rows(&plain), rows(&traced));
	assert!(!links(&traced, 0).is_empty());
}

/// SECURITY: a field the record user cannot read holds no link it is told of.
#[tokio::test]
async fn a_hidden_field_reports_no_link() {
	let ds = linked().await;
	let user = Session::for_record(
		"test",
		"test",
		"user",
		PublicValue::RecordId(surrealdb_types::RecordId::new("person", "tobie")),
	);
	let hidden =
		items_as(&ds, &user, "SELECT owner.name FROM secret:1;", TraceKinds::LINKS, false).await;
	assert_eq!(links(&hidden, 0), Vec::<String>::new());
	// The same read by a caller who may see the field reports the link.
	let owner = items(&ds, "SELECT owner.name FROM secret:1;", TraceKinds::LINKS, false).await;
	assert_eq!(links(&owner, 0), vec!["secret:1 owner person:tobie".to_owned()]);
}

/// A walk both ways reports each edge as `[in, edge, out]` whichever end it
/// was reached from: each adjacency entry is read along one direction.
#[tokio::test]
async fn a_walk_both_ways_keeps_each_edges_orientation() {
	let ds = graph().await;
	let items = items(
		&ds,
		"RETURN person:b<->knows; RETURN person:b<->knows<->person;",
		TraceKinds::RELATIONS,
		false,
	)
	.await;
	assert_eq!(
		relations(&items, 0),
		rels(&["person:a knows:ab person:b", "person:b knows:bc person:c"])
	);
	assert!(relations(&items, 1).contains(&"person:a knows:ab person:b".to_owned()));
	assert!(!relations(&items, 1).iter().any(|r| r.starts_with("person:b knows:ab")));
}

/// A step that keeps only some of what a lookup answered reports only the
/// edges behind what it keeps.
#[tokio::test]
async fn a_step_that_cuts_the_answer_cuts_the_trace() {
	let ds = graph().await;
	let sql = "
		LET $i = 0;
		RETURN (person:a->knows->person)[$i];
		RETURN (person:a->knows->person)[WHERE id = person:c];
		SELECT VALUE ->knows->person[$] FROM person:a;
	";
	let items = items(&ds, sql, TraceKinds::RELATIONS, false).await;
	let first = relations(&items, 1);
	assert_eq!(first.len(), 1, "only the edge behind the element kept: {first:?}");
	assert_eq!(relations(&items, 2), rels(&["person:a knows:ac person:c"]));
	let last = relations(&items, 3);
	assert_eq!(last.len(), 1, "only the edge behind the last element: {last:?}");
	for index in 1..=3 {
		assert!(!partial(&items, index), "statement {index} is traced whole");
	}
}

/// A `LET` value past the cap is never copied into the trace: its name is
/// reported with `NONE`, and `variables` is truncated.
#[tokio::test]
async fn a_large_let_value_is_reported_by_name() {
	let ds = datastore().await;
	let setup = "FOR $i IN 0..20000 { \
		CREATE t SET s = 'a value long enough to take the rows past the cap' };";
	for result in ds.execute(setup, &session(), None).await.expect("the setup runs") {
		result.result.expect("the setup succeeds");
	}
	let items =
		items(&ds, "LET $big = (SELECT * FROM t); LET $small = 1;", TraceKinds::VARIABLES, false)
			.await;
	let traces = traces(&items);
	let big = traces
		.iter()
		.find(|(at, _)| *at == 0)
		.unwrap_or_else(|| panic!("the large LET is traced: {:?}", &items[..items.len().min(3)]))
		.1;
	assert_eq!(big.variables, vec![("big".to_owned(), PublicValue::None)]);
	assert!(big.truncated.contains(TraceKinds::VARIABLES));
	let small = traces.iter().find(|(at, _)| *at == 1).expect("the small LET is traced").1;
	assert_eq!(small.variables, vec![("small".to_owned(), PublicValue::from_t(1i64))]);
	assert!(small.truncated.is_empty());
}

/// A page over the rows a graph in `FROM` names drops rows the lookup already
/// walked: the trace says it is partial rather than reporting their edges.
#[tokio::test]
async fn a_page_over_a_graph_source_is_partial() {
	let ds = graph().await;
	let items =
		items(&ds, "SELECT * FROM person:a->knows->person LIMIT 1;", TraceKinds::RELATIONS, false)
			.await;
	assert_eq!(relations(&items, 0), Vec::<String>::new());
	assert!(partial(&items, 0));
}

/// A link held in a set is recorded for each record id in it.
#[tokio::test]
async fn a_set_of_links_is_recorded() {
	let ds = linked().await;
	let setup = "CREATE post:2 SET labels = <set> [tag:rust, tag:db];";
	for result in ds.execute(setup, &session(), None).await.expect("the setup runs") {
		result.result.expect("the setup succeeds");
	}
	let items =
		items(&ds, "SELECT VALUE labels.*.label FROM post:2;", TraceKinds::LINKS, false).await;
	assert_eq!(
		links(&items, 0),
		vec!["post:2 labels tag:db".to_owned(), "post:2 labels tag:rust".to_owned()]
	);
}

/// Tracing a reference lookup keeps the order its records are read in: each
/// subject over every record in turn.
#[tokio::test]
async fn tracing_references_keeps_their_order() {
	let ds = datastore().await;
	let setup = "
		DEFINE FIELD author ON post TYPE option<record<person>> REFERENCE;
		DEFINE FIELD author ON note TYPE option<record<person>> REFERENCE;
		CREATE person:a, person:b;
		CREATE post:1 SET author = person:a;
		CREATE post:2 SET author = person:b;
		CREATE note:1 SET author = person:a;
		CREATE note:2 SET author = person:b;
	";
	for result in ds.execute(setup, &session(), None).await.expect("the setup runs") {
		result.result.expect("every setup statement succeeds");
	}
	let sql = "RETURN [person:a, person:b]<~(post, note);";
	let value = |items: &[QueryStreamItem]| -> Vec<String> {
		items
			.iter()
			.filter_map(|item| {
				let QueryStreamItem::Rows {
					values,
					..
				} = item
				else {
					return None;
				};
				Some(format!("{values:?}"))
			})
			.collect()
	};
	let plain = items(&ds, sql, TraceKinds::NONE, false).await;
	let traced = items(&ds, sql, TraceKinds::REFERENCES, false).await;
	assert_eq!(value(&plain), value(&traced));
	assert!(!traces(&traced).is_empty());
}

/// A link read through a step that keeps part of it is recorded for what the
/// step keeps.
#[tokio::test]
async fn a_link_read_through_an_index_is_recorded() {
	let ds = linked().await;
	let items =
		items(&ds, "SELECT tags[0].label AS first FROM post:1;", TraceKinds::LINKS, false).await;
	assert_eq!(links(&items, 0), vec!["post:1 tags tag:rust".to_owned()]);
}

/// A cutting step that keeps an object still leads to the lookup held in it,
/// and one that cuts link reads it cannot prune marks the trace partial.
#[tokio::test]
async fn a_cutting_step_keeps_lookups_held_in_objects_and_marks_cut_links_partial() {
	let ds = graph().await;
	let kept = items(
		&ds,
		"RETURN [{ friend: person:a->knows->person }][0];",
		TraceKinds::RELATIONS,
		false,
	)
	.await;
	assert_eq!(
		relations(&kept, 0),
		rels(&["person:a knows:ab person:b", "person:a knows:ac person:c"])
	);
	let ds = linked().await;
	let cut = items(
		&ds,
		"RETURN [post:1.author.name, post:1.tags[0].label][0];",
		TraceKinds::LINKS,
		false,
	)
	.await;
	assert_eq!(links(&cut, 0), Vec::<String>::new());
	assert!(traces(&cut).iter().any(|(at, trace)| *at == 0 && trace.partial));
}
