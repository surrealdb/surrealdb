//! A function body is lexically scoped. It reads its arguments, the names it
//! binds itself, the row names, the session's own values, the request's
//! parameters (a query parameter, or a session variable set over RPC), and the
//! database's parameters. It never reads a binding its caller made with `LET`
//! or `FOR`, at any depth of calls. A privileged frame, where code a definer
//! wrote runs (a table or field permission, a `VALUE` or `ASSERT`, a computed
//! field, an event, a gate), holds no caller binding and no request parameter.
//! It sees the session's own values and the database's parameters, however the
//! code is reached and whoever compiled it. Each test runs the same statements
//! as more than one principal on one datastore, so a unit compiled for one
//! caller and reused for the next must follow the same scope rules.

use crate::exec::operators::test_util::TestDb;
use crate::types::PublicVariables;
use crate::val::Value;

fn record_user(user: &str) -> crate::dbs::Session {
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", user));
	crate::dbs::Session::for_record("test", "test", "user", rid)
}

const SETUP: &str = "
	DEFINE ACCESS user ON DATABASE TYPE RECORD;
	DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
	DEFINE PARAM $allowed VALUE false PERMISSIONS FULL;
	DEFINE PARAM $limit VALUE 10 PERMISSIONS FULL;
	DEFINE FUNCTION fn::allowed() { RETURN $allowed } PERMISSIONS FULL;
	DEFINE FUNCTION fn::lim() { RETURN $limit } PERMISSIONS FULL;
	DEFINE FUNCTION fn::lim_block() { LET $one = 1; RETURN $limit * $one } PERMISSIONS FULL;
	DEFINE TABLE viafn SCHEMALESS PERMISSIONS FOR select, create, update WHERE fn::allowed();
	DEFINE TABLE direct SCHEMALESS PERMISSIONS FOR select WHERE $allowed;
	DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
	CREATE user:alice, user:bob;
	CREATE viafn:1 SET v = 1;
	CREATE direct:1 SET v = 1;
";

/// Every statement's value of `sql` run as `session` with `vars`, an error
/// rendered as its message.
async fn values_with(
	db: &TestDb,
	session: &crate::dbs::Session,
	vars: Option<PublicVariables>,
	sql: &str,
) -> Vec<Value> {
	let results = db.datastore().execute(sql, session, vars).await.expect("runs");
	results
		.into_iter()
		.map(|r| r.result.map(Value::from).unwrap_or_else(|e| Value::from(e.to_string())))
		.collect()
}

async fn values(db: &TestDb, session: &crate::dbs::Session, sql: &str) -> Vec<Value> {
	values_with(db, session, None, sql).await
}

/// The query parameter `$allowed = true` and the session variable
/// `$allowed = true` a client sets over RPC.
fn allowed_as_param() -> PublicVariables {
	let mut vars = PublicVariables::default();
	vars.insert("allowed", true);
	vars
}

fn parse(text: &str) -> Value {
	Value::from(crate::syn::value(text).expect("parses"))
}

/// A record user who binds the name a table permission's function reads, by
/// a `LET`, a query parameter or a session variable, can neither read nor
/// write through the permission, and a second record user, served what the
/// first compiled, is refused alike.
#[tokio::test]
async fn a_callers_bindings_do_not_reach_a_permission_function() {
	let db = TestDb::new_with_auth(SETUP).await;
	let alice = values(
		&db,
		&record_user("alice"),
		"LET $allowed = true;
		 SELECT VALUE id FROM viafn;
		 UPDATE viafn:1 SET v = 100 RETURN VALUE v;
		 RETURN { LET $allowed = true; RETURN SELECT VALUE id FROM viafn };
		 FOR $allowed IN [true] { UPDATE viafn:1 SET v = 200 };",
	)
	.await;
	assert_eq!(alice[1], parse("[]"), "alice's LET widened the select permission");
	assert_eq!(alice[2], parse("[]"), "alice's LET widened the update permission");
	assert_eq!(alice[3], parse("[]"), "alice's block LET widened the select permission");
	let by_param = values_with(
		&db,
		&record_user("alice"),
		Some(allowed_as_param()),
		"SELECT VALUE id FROM viafn; RETURN fn::allowed();",
	)
	.await;
	assert_eq!(by_param[0], parse("[]"), "alice's query parameter widened the permission");
	let bob = values(&db, &record_user("bob"), "SELECT VALUE id FROM viafn;").await;
	assert_eq!(bob[0], parse("[]"), "bob was served a permission alice's LET widened");
	let owner = values(&db, &TestDb::owner(), "SELECT VALUE v FROM viafn:1;").await;
	assert_eq!(owner[0], parse("[1]"), "a record user's write got through the permission");
}

/// A permission predicate that reads the name directly, rather than through a
/// function, is hidden from the caller's bindings the same way.
#[tokio::test]
async fn a_callers_bindings_do_not_reach_a_direct_permission() {
	let db = TestDb::new_with_auth(SETUP).await;
	let alice =
		values(&db, &record_user("alice"), "LET $allowed = true; SELECT VALUE id FROM direct;")
			.await;
	assert_eq!(alice[1], parse("[]"), "a LET reached a direct table permission");
	let by_param = values_with(
		&db,
		&record_user("alice"),
		Some(allowed_as_param()),
		"SELECT VALUE id FROM direct;",
	)
	.await;
	assert_eq!(by_param[0], parse("[]"), "a query parameter reached a direct table permission");
}

/// An ordinary call reads the request's parameters — a query parameter, a
/// session variable — and never what its caller binds with `LET` or `FOR`,
/// for every principal in turn: a name no binding of the body or of the
/// request holds is the database's parameter.
#[tokio::test]
async fn an_ordinary_call_reads_no_binding_of_its_caller() {
	let db = TestDb::new_with_auth(SETUP).await;
	let sql = "LET $limit = 1000;
		RETURN fn::lim();
		RETURN fn::lim_block();
		RETURN (SELECT VALUE fn::lim() FROM [1])[0];
		RETURN { LET $limit = 3; RETURN fn::lim() };
		FOR $limit IN [5] { UPSERT t:loop SET v = fn::lim() };
		SELECT VALUE v FROM t:loop;";
	for session in [record_user("alice"), TestDb::owner(), record_user("bob")] {
		let got = values(&db, &session, sql).await;
		let who = format!("{:?}", session.au);
		assert_eq!(got[1], parse("10"), "{who}: a top-level call under a LET");
		assert_eq!(got[2], parse("10"), "{who}: a block-local call under a LET");
		assert_eq!(got[3], parse("10"), "{who}: a call in a projection under a LET");
		assert_eq!(got[4], parse("10"), "{who}: a call under a block's LET");
		assert_eq!(got[6], parse("[10]"), "{who}: a call in a write under a FOR variable");
	}
	let by_param = values_with(
		&db,
		&TestDb::owner(),
		Some({
			let mut vars = PublicVariables::default();
			vars.insert("limit", 77);
			vars
		}),
		"RETURN fn::lim();
		 LET $limit = 1000;
		 RETURN fn::lim();
		 RETURN [1].map(|$x| fn::lim());
		 RETURN [1].map(|$x| [$limit, fn::lim()]);",
	)
	.await;
	assert_eq!(by_param[0], parse("77"), "a query parameter did not reach a call");
	assert_eq!(by_param[2], parse("77"), "a LET shadowed a query parameter inside a call");
	assert_eq!(by_param[3], parse("[77]"), "a query parameter did not reach a call in a closure");
	assert_eq!(
		by_param[4],
		parse("[[1000, 77]]"),
		"a LET a closure captured reached the function it calls"
	);
	let mut session = TestDb::owner();
	session.variables.insert("limit", 88);
	let by_rpc =
		values(&db, &session, "RETURN fn::lim(); LET $limit = 1000; RETURN fn::lim();").await;
	assert_eq!(by_rpc[0], parse("88"), "a session variable set over RPC did not reach a call");
	assert_eq!(by_rpc[2], parse("88"), "a LET shadowed a session variable inside a call");
}

/// A function body reads the database parameter of a name its caller binds
/// with `LET`, whether the caller binds it before or after the plan is made:
/// the request's `LET` of a constant is not folded into the body.
#[tokio::test]
async fn a_function_body_reads_a_database_parameter_whatever_its_caller_binds() {
	let db = TestDb::new_with_auth(SETUP).await;
	let got = values(
		&db,
		&TestDb::owner(),
		"RETURN fn::lim(); LET $limit = 1; RETURN fn::lim(); LET $limit = 2; RETURN fn::lim();",
	)
	.await;
	assert_eq!(got[0], parse("10"));
	assert_eq!(got[2], parse("10"));
	assert_eq!(got[4], parse("10"));
}

/// A request's `LET` of a name a `DEFINE PARAM` holds is read by the caller
/// and not by a function body, which reads the stored parameter.
#[tokio::test]
async fn a_let_of_a_stored_parameters_name_is_not_read_by_a_body() {
	let db = TestDb::new_with_auth(
		"DEFINE PARAM $q VALUE 3 PERMISSIONS FULL;
		 DEFINE FUNCTION fn::g() { RETURN $q } PERMISSIONS FULL;
		 DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;",
	)
	.await;
	let got = values(
		&db,
		&TestDb::owner(),
		"LET $q = 5;
		 RETURN [$q, fn::g()];
		 FOR $q IN [6] { UPSERT t:1 SET v = fn::g() };
		 SELECT VALUE v FROM t:1;",
	)
	.await;
	assert_eq!(got[1], parse("[5, 3]"));
	assert_eq!(got[3], parse("[3]"));
}

/// A body whose predicate or source names a free name plans it as the
/// database's parameter, whatever constant the request's `LET` gave the name:
/// no caller binding is folded into the plan.
#[tokio::test]
async fn a_callers_let_is_not_folded_into_a_bodys_predicate() {
	let db = TestDb::new_with_auth(
		"DEFINE PARAM $k VALUE 3 PERMISSIONS FULL;
		 DEFINE TABLE rows SCHEMALESS PERMISSIONS FULL;
		 DEFINE FUNCTION fn::seen() { RETURN (SELECT VALUE id FROM rows WHERE k = $k) }
			PERMISSIONS FULL;
		 DEFINE FUNCTION fn::from() { RETURN (SELECT VALUE $this FROM [$k]) }
			PERMISSIONS FULL;
		 CREATE rows:three SET k = 3;
		 CREATE rows:five SET k = 5;",
	)
	.await;
	for session in [TestDb::owner(), TestDb::owner()] {
		let got = values(
			&db,
			&session,
			"LET $k = 5;
			 RETURN fn::seen();
			 RETURN (SELECT VALUE id FROM rows WHERE k = $k);
			 RETURN fn::from();",
		)
		.await;
		assert_eq!(got[1], parse("[rows:three]"), "a LET was folded into a body's predicate");
		assert_eq!(got[2], parse("[rows:five]"), "the caller's own predicate read its LET");
		assert_eq!(got[3], parse("[3]"), "a LET was folded into a body's source");
	}
}

/// A body reads the arguments of its own call and not the argument of the
/// call that entered its caller, however deep the calls nest.
#[tokio::test]
async fn a_nested_body_reads_no_binding_of_the_bodies_around_it() {
	let db = TestDb::new_with_auth(
		"DEFINE PARAM $p VALUE 'stored' PERMISSIONS FULL;
		 DEFINE FUNCTION fn::inner() { RETURN $p } PERMISSIONS FULL;
		 DEFINE FUNCTION fn::mid($p: any) { RETURN [$p, fn::inner()] } PERMISSIONS FULL;
		 DEFINE FUNCTION fn::outer() { LET $p = 'local'; RETURN [$p, fn::mid('arg'), fn::inner()] }
			PERMISSIONS FULL;",
	)
	.await;
	let got = values(&db, &TestDb::owner(), "LET $p = 'caller'; RETURN fn::outer();").await;
	assert_eq!(got[1], parse("['local', ['arg', 'stored'], 'stored']"));
}

/// A closure called inside a body reads what the body reads, and a closure
/// created in a body captures no binding of the body's caller.
#[tokio::test]
async fn a_closure_in_a_body_captures_no_binding_of_the_bodys_caller() {
	let db = TestDb::new_with_auth(
		"DEFINE PARAM $q VALUE 3 PERMISSIONS FULL;
		 DEFINE FUNCTION fn::via_closure() { RETURN [1].map(|$x| $x + $q) } PERMISSIONS FULL;
		 DEFINE FUNCTION fn::made() { LET $f = |$x| $x + $q; RETURN $f(1) } PERMISSIONS FULL;",
	)
	.await;
	let got =
		values(&db, &TestDb::owner(), "LET $q = 5; RETURN fn::via_closure(); RETURN fn::made();")
			.await;
	assert_eq!(got[1], parse("[4]"));
	assert_eq!(got[2], parse("4"));
}

/// A privileged frame reads the session's own values and the database's
/// parameters, and nothing a caller binds, for a clause evaluated on a write.
#[tokio::test]
async fn a_write_clause_frame_holds_none_of_the_callers_bindings() {
	let db = TestDb::new_with_auth(
		"
		DEFINE ACCESS user ON DATABASE TYPE RECORD;
		DEFINE PARAM $limit VALUE 10 PERMISSIONS FULL;
		DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
		DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
		DEFINE FIELD seen ON t VALUE $limit;
		DEFINE FIELD checked ON t TYPE int ASSERT $limit = 10;
		DEFINE FIELD computed ON t COMPUTED $limit;
		CREATE user:alice;
	",
	)
	.await;
	let got = values(
		&db,
		&record_user("alice"),
		"LET $limit = 1000;
		 CREATE t:1 SET v = 1, checked = 5 RETURN VALUE seen;
		 RETURN (SELECT VALUE computed FROM ONLY t:1);",
	)
	.await;
	assert_eq!(got[1], parse("[10]"), "a field VALUE or ASSERT read the caller's LET");
	assert_eq!(got[2], parse("10"), "a COMPUTED field read the caller's LET");
}

/// A `DEFINE EVENT` body reads the session's values and the database's
/// parameters, and no binding the statement that fires it made.
#[tokio::test]
async fn an_event_frame_holds_none_of_the_callers_bindings() {
	let db = TestDb::new_with_auth(
		"
		DEFINE ACCESS user ON DATABASE TYPE RECORD;
		DEFINE PARAM $limit VALUE 10 PERMISSIONS FULL;
		DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
		DEFINE TABLE src SCHEMALESS PERMISSIONS FULL;
		DEFINE TABLE log SCHEMALESS PERMISSIONS FULL;
		DEFINE EVENT ev ON src WHEN true THEN (CREATE log SET v = $limit);
		CREATE user:alice;
	",
	)
	.await;
	let got =
		values(&db, &TestDb::owner(), "LET $limit = 1000; CREATE src:1; SELECT VALUE v FROM log;")
			.await;
	assert_eq!(got[2], parse("[10]"), "an event body read the statement's LET");
}

/// A query parameter the client sends reaches neither a field clause nor an
/// event body, which read the database's parameter of the name.
#[tokio::test]
async fn a_query_parameter_does_not_reach_a_write_clause_frame() {
	let db = TestDb::new_with_auth(
		"
		DEFINE ACCESS user ON DATABASE TYPE RECORD;
		DEFINE PARAM $limit VALUE 10 PERMISSIONS FULL;
		DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
		DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
		DEFINE FIELD seen ON t VALUE $limit;
		DEFINE TABLE src SCHEMALESS PERMISSIONS FULL;
		DEFINE TABLE log SCHEMALESS PERMISSIONS FULL;
		DEFINE EVENT ev ON src WHEN true THEN (CREATE log SET v = $limit);
		CREATE user:alice;
	",
	)
	.await;
	let mut vars = PublicVariables::default();
	vars.insert("limit", 1000);
	let got = values_with(
		&db,
		&record_user("alice"),
		Some(vars),
		"CREATE t:1 SET v = 1 RETURN VALUE seen;
		 CREATE src:1;
		 SELECT VALUE v FROM log;",
	)
	.await;
	assert_eq!(got[0], parse("[10]"), "a field VALUE read the query parameter");
	assert_eq!(got[2], parse("[10]"), "an event body read the query parameter");
}

/// The ways a caller binds `$allowed` to `true` around code it runs: a query
/// `LET`, a block's `LET`, a `FOR` variable, a closure's argument, a query
/// parameter and a session variable set over RPC. Each wraps `read`, an
/// expression, into statements whose `at`'th answers it; `None` is the
/// caller binding nothing.
fn read_wrappers(read: &str) -> Vec<(&'static str, String, usize)> {
	vec![
		("none", format!("RETURN {read};"), 0),
		("let", format!("LET $allowed = true; RETURN {read};"), 1),
		("block", format!("RETURN {{ LET $allowed = true; RETURN {read} }};"), 0),
		(
			"for",
			format!(
				"FOR $allowed IN [true] {{ UPSERT probe:got SET v = {read} }};
				 SELECT VALUE v FROM ONLY probe:got;"
			),
			1,
		),
		("closure", format!("RETURN [true].map(|$allowed| {read})[0];"), 0),
		("param", format!("RETURN {read};"), 0),
		("session", format!("RETURN {read};"), 0),
	]
}

/// The write counterpart of `read_wrappers`. Each statement is run and its answer is not compared.
fn write_wrappers(write: &str) -> Vec<(&'static str, String)> {
	vec![
		("let", format!("LET $allowed = true; {write};")),
		("block", format!("{{ LET $allowed = true; {write} }};")),
		("for", format!("FOR $allowed IN [true] {{ {write} }};")),
		("closure", format!("RETURN [true].map(|$allowed| ({write}));")),
		("param", format!("{write};")),
		("session", format!("{write};")),
	]
}

/// `sql` run as a record user binding `$allowed` the way `how` names.
async fn as_binding(db: &TestDb, how: &str, sql: &str) -> Vec<Value> {
	match how {
		"param" => values_with(db, &record_user("alice"), Some(allowed_as_param()), sql).await,
		"session" => {
			let mut session = record_user("alice");
			session.variables.insert("allowed", true);
			values(db, &session, sql).await
		}
		_ => values(db, &record_user("alice"), sql).await,
	}
}

/// Every way a caller binds a name, against every way a read or a write
/// reaches a permission that reads the name: directly, through a function with
/// a single expression body, and through one with a block of statements. The
/// permission answers as the database's parameter of the name has it, and that
/// parameter is what opens it.
#[tokio::test]
async fn no_caller_binding_reaches_a_permission_however_it_is_reached() {
	let guards = ["$allowed", "fn::allowed()", "fn::allowed_block()"];
	let mut setup = String::from(
		"DEFINE ACCESS user ON DATABASE TYPE RECORD;
		 DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
		 DEFINE PARAM $allowed VALUE false PERMISSIONS FULL;
		 DEFINE FUNCTION fn::allowed() { RETURN $allowed } PERMISSIONS FULL;
		 DEFINE FUNCTION fn::allowed_block() { LET $one = 1; RETURN $allowed AND $one = 1 }
			PERMISSIONS FULL;
		 DEFINE TABLE probe SCHEMALESS PERMISSIONS FULL;
		 DEFINE TABLE holder SCHEMALESS PERMISSIONS FULL;
		 DEFINE TABLE edge TYPE RELATION SCHEMALESS PERMISSIONS FULL;
		 DEFINE TABLE fielded SCHEMALESS PERMISSIONS FULL;
		 CREATE user:alice;
		 CREATE fielded:1;",
	);
	for (at, guard) in guards.iter().enumerate() {
		setup.push_str(&format!(
			"DEFINE TABLE r{at} SCHEMALESS PERMISSIONS FOR select WHERE {guard};
			 DEFINE TABLE w{at} SCHEMALESS
				PERMISSIONS FOR select FULL, FOR create, update, delete WHERE {guard};
			 DEFINE FIELD secret{at} ON fielded PERMISSIONS FOR select WHERE {guard};
			 CREATE r{at}:1 SET v = 1;
			 CREATE w{at}:1 SET v = 1;
			 CREATE holder:{at} SET link = r{at}:1;
			 RELATE holder:{at}->edge->r{at}:1;
			 UPDATE fielded:1 SET secret{at} = 'x';"
		));
	}
	let db = TestDb::new_with_auth(&setup).await;
	let reads = |at: usize| {
		[
			format!("(SELECT VALUE id FROM r{at})"),
			format!("(SELECT VALUE id FROM r{at}:1)"),
			format!("(SELECT VALUE link.id FROM holder:{at})"),
			format!("(SELECT VALUE link FROM holder:{at} FETCH link)"),
			format!("(SELECT VALUE ->edge->r{at} FROM holder:{at})"),
			format!("(SELECT VALUE secret{at} FROM fielded:1)"),
		]
	};
	let writes = |at: usize| {
		[
			format!("UPDATE w{at}:1 SET v = 2"),
			format!("DELETE w{at}:1"),
			format!("UPSERT w{at}:1 SET v = 3"),
			format!("CREATE w{at}:2 SET v = 4"),
		]
	};
	let state = |at: usize| format!("SELECT VALUE v FROM w{at} ORDER BY id;");
	let mut denied = Vec::new();
	for (at, guard) in guards.iter().enumerate() {
		for read in reads(at) {
			let mut baseline = None;
			for (how, sql, answer) in read_wrappers(&read) {
				let got = as_binding(&db, how, &sql).await[answer].clone();
				match &baseline {
					None => baseline = Some(got),
					Some(base) => assert_eq!(&got, base, "{guard}: `{read}` bound by {how}"),
				}
			}
			denied.push((at, read, baseline.expect("the read answered")));
		}
		for write in writes(at) {
			for (how, sql) in write_wrappers(&write) {
				as_binding(&db, how, &sql).await;
				let kept = values(&db, &TestDb::owner(), &state(at)).await;
				assert_eq!(kept[0], parse("[1]"), "{guard}: `{write}` bound by {how} got through");
			}
		}
	}
	// The database's parameter opens every permission the caller could not.
	db.run("DEFINE PARAM OVERWRITE $allowed VALUE true PERMISSIONS FULL;").await;
	for (at, read, base) in denied {
		let got = values(&db, &record_user("alice"), &format!("RETURN {read};")).await;
		assert_ne!(
			got[0], base,
			"{}: `{read}` answers alike once the parameter allows",
			guards[at]
		);
	}
	for (at, guard) in guards.iter().enumerate() {
		values(&db, &record_user("alice"), &format!("UPDATE w{at}:1 SET v = 2;")).await;
		let changed = values(&db, &TestDb::owner(), &state(at)).await;
		assert_eq!(changed[0], parse("[2]"), "{guard}: the parameter did not open the write");
	}
}

/// Every way a caller binds a name, against every way a read reaches a
/// `COMPUTED` field that reads the name — a scan, a record id, a record link,
/// a `FETCH`, a graph hop, a table or record the read names by data, on a
/// table with and without a `select` permission: the field computes what the
/// database's parameter of the name holds, and the parameter is what changes
/// it.
#[tokio::test]
async fn no_caller_binding_reaches_a_computed_field_however_it_is_read() {
	let db = TestDb::new_with_auth(
		"DEFINE ACCESS user ON DATABASE TYPE RECORD;
		 DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
		 DEFINE PARAM $allowed VALUE false PERMISSIONS FULL;
		 DEFINE TABLE probe SCHEMALESS PERMISSIONS FULL;
		 DEFINE TABLE open SCHEMALESS PERMISSIONS FULL;
		 DEFINE FIELD c ON open COMPUTED $allowed;
		 DEFINE TABLE checked SCHEMALESS PERMISSIONS FOR select WHERE id != NONE;
		 DEFINE FIELD c ON checked COMPUTED $allowed;
		 DEFINE TABLE holder SCHEMALESS PERMISSIONS FULL;
		 DEFINE TABLE edge TYPE RELATION SCHEMALESS PERMISSIONS FULL;
		 CREATE user:alice;
		 CREATE open:1, checked:1;
		 CREATE holder:open SET link = open:1;
		 CREATE holder:checked SET link = checked:1;
		 RELATE holder:open->edge->open:1;
		 RELATE holder:checked->edge->checked:1;",
	)
	.await;
	let reads = |table: &str| {
		[
			format!("(SELECT VALUE c FROM {table})"),
			format!("(SELECT VALUE c FROM {table}:1)"),
			format!("{table}:1.c"),
			format!("(SELECT * FROM {table})"),
			format!("(SELECT VALUE link.c FROM holder:{table})"),
			format!("(SELECT VALUE link FROM holder:{table} FETCH link)"),
			format!("(SELECT VALUE ->edge->{table}.c FROM holder:{table})"),
			format!("(SELECT VALUE c FROM type::table('{table}'))"),
			format!("(SELECT VALUE c FROM type::record('{table}', 1))"),
		]
	};
	let mut computed = Vec::new();
	for table in ["open", "checked"] {
		for read in reads(table) {
			let mut baseline = None;
			for (how, sql, answer) in read_wrappers(&read) {
				let got = as_binding(&db, how, &sql).await[answer].clone();
				match &baseline {
					None => baseline = Some(got),
					Some(base) => assert_eq!(&got, base, "`{read}` bound by {how}"),
				}
			}
			computed.push((read, baseline.expect("the read answered")));
		}
	}
	// The database's parameter is what the field computes.
	db.run("DEFINE PARAM OVERWRITE $allowed VALUE true PERMISSIONS FULL;").await;
	for (read, base) in computed {
		let got = values(&db, &record_user("alice"), &format!("RETURN {read};")).await;
		assert_ne!(got[0], base, "`{read}` answers alike once the parameter changes");
	}
}

/// Every way a caller binds a name, against a function's and a parameter's
/// `PERMISSIONS` clause that reads the name. The clause is definer code and
/// runs in a privileged frame whoever calls it, so it answers as the database's
/// parameter of the name has it, and that parameter is what opens it.
#[tokio::test]
async fn no_caller_binding_reaches_a_definition_guard() {
	let db = TestDb::new_with_auth(
		"DEFINE ACCESS user ON DATABASE TYPE RECORD;
		 DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
		 DEFINE PARAM $allowed VALUE false PERMISSIONS FULL;
		 DEFINE TABLE probe SCHEMALESS PERMISSIONS FULL;
		 DEFINE TABLE flags SCHEMALESS PERMISSIONS FULL;
		 CREATE flags:yes SET k = true, ok = true;
		 CREATE flags:no SET k = false, ok = false;
		 DEFINE FUNCTION fn::guarded() { RETURN 'opened' }
			PERMISSIONS WHERE (SELECT VALUE ok FROM flags WHERE k = $allowed)[0] = true;
		 DEFINE PARAM $guarded VALUE 'opened'
			PERMISSIONS WHERE (SELECT VALUE ok FROM flags WHERE k = $allowed)[0] = true;
		 CREATE user:alice;",
	)
	.await;
	let opened = parse("'opened'");
	for read in ["fn::guarded()", "$guarded"] {
		for (how, sql, answer) in read_wrappers(read) {
			let got = as_binding(&db, how, &sql).await[answer].clone();
			assert_ne!(got, opened, "`{read}` bound by {how} opened its guard");
		}
	}
	db.run("DEFINE PARAM OVERWRITE $allowed VALUE true PERMISSIONS FULL;").await;
	for read in ["fn::guarded()", "$guarded"] {
		let got = values(&db, &record_user("alice"), &format!("RETURN {read};")).await;
		assert_eq!(got[0], opened, "the parameter did not open the guard of `{read}`");
	}
}

/// A record user's `UPSERT` of a target is admitted by the target's `update`
/// clause, whichever order would write the record: a `NONE` `update` writes
/// nothing, not even a record its create order would make. The record a
/// subject names is read before an order is chosen, so a stored record is
/// updated under the `update` clause alone, as an `UPDATE` of it is, and a
/// `NONE` `create` refuses only a record that is not stored.
#[tokio::test]
async fn an_upsert_is_admitted_by_its_update_clause() {
	let db = TestDb::new_with_auth(
		"DEFINE ACCESS user ON DATABASE TYPE RECORD;
		 DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
		 DEFINE TABLE creates SCHEMALESS
			PERMISSIONS FOR select, create FULL, FOR update, delete NONE;
		 DEFINE TABLE updates SCHEMALESS
			PERMISSIONS FOR select, update FULL, FOR create, delete NONE;
		 CREATE user:alice;
		 CREATE creates:old SET v = 0;
		 CREATE updates:old SET v = 0;",
	)
	.await;
	let alice = values(
		&db,
		&record_user("alice"),
		"UPSERT creates:new SET v = 1 RETURN VALUE v;
		 UPSERT creates:old SET v = 1 RETURN VALUE v;
		 UPSERT updates:new SET v = 1 RETURN VALUE v;
		 UPSERT updates:old SET v = 1 RETURN VALUE v;",
	)
	.await;
	for (at, answer) in alice.iter().take(3).enumerate() {
		assert_eq!(answer, &parse("[]"), "upsert {at} wrote through a NONE clause");
	}
	assert_eq!(alice[3], parse("[1]"), "a stored record is updated under its update clause");
	let owner = values(
		&db,
		&TestDb::owner(),
		"SELECT VALUE v FROM creates ORDER BY id; SELECT VALUE v FROM updates ORDER BY id;",
	)
	.await;
	assert_eq!(owner[0], parse("[0]"));
	assert_eq!(owner[1], parse("[1]"));
}

/// A statement held in definer code — a permission predicate's, a field
/// `VALUE`'s, an event body's, a membership a permission tests — reads no
/// caller binding either: its own condition reads the database's parameter
/// of a name the caller binds, by a `LET`, a block's `LET`, a query parameter
/// or a session variable.
#[tokio::test]
async fn a_statement_in_definer_code_reads_no_caller_binding() {
	let db = TestDb::new_with_auth(
		"DEFINE ACCESS user ON DATABASE TYPE RECORD;
		 DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
		 DEFINE PARAM $allowed VALUE false PERMISSIONS FULL;
		 DEFINE TABLE flags SCHEMALESS PERMISSIONS FULL;
		 CREATE flags:yes SET k = true, ok = true;
		 CREATE flags:no SET k = false, ok = false;
		 DEFINE TABLE gated SCHEMALESS PERMISSIONS
			FOR select WHERE (SELECT VALUE ok FROM flags WHERE k = $allowed)[0] = true;
		 CREATE gated:1;
		 DEFINE TABLE member SCHEMALESS PERMISSIONS
			FOR select WHERE $allowed IN (SELECT VALUE k FROM flags WHERE ok = true);
		 CREATE member:1;
		 DEFINE TABLE w SCHEMALESS PERMISSIONS FULL;
		 DEFINE FIELD seen ON w VALUE (SELECT VALUE ok FROM flags WHERE k = $allowed)[0];
		 DEFINE TABLE src SCHEMALESS PERMISSIONS FULL;
		 DEFINE TABLE log SCHEMALESS PERMISSIONS FULL;
		 DEFINE EVENT ev ON src THEN (
			CREATE log SET of = $after.id, ok = (SELECT VALUE ok FROM flags WHERE k = $allowed)[0]
		 );
		 CREATE user:alice;",
	)
	.await;
	for (at, how) in ["none", "let", "block", "param", "session"].into_iter().enumerate() {
		let probes = format!(
			"[
				(SELECT VALUE id FROM gated),
				(CREATE ONLY w:{at} SET v = 1 RETURN VALUE seen),
				(CREATE src:{at} RETURN NONE),
				(SELECT VALUE ok FROM log WHERE of = src:{at}),
				(SELECT VALUE id FROM member)
			]"
		);
		let (sql, answer) = match how {
			"let" => (format!("LET $allowed = true; RETURN {probes};"), 1),
			"block" => (format!("RETURN {{ LET $allowed = true; RETURN {probes} }};"), 0),
			_ => (format!("RETURN {probes};"), 0),
		};
		let got = as_binding(&db, how, &sql).await;
		assert_eq!(got[answer], parse("[[], false, [], [false], []]"), "bound by {how}");
	}
}

/// A statement in the argument of a call that definer code makes, to a
/// function whose `AUTH LIMIT` narrows the caller, runs in the call's frame
/// inside the privileged one, and reads none of the caller's block names: its
/// condition reads the database's parameter of a name the caller's block binds.
#[tokio::test]
async fn a_statement_in_a_narrowed_calls_argument_reads_no_caller_binding() {
	let db = TestDb::new_with_auth(
		"DEFINE PARAM $allowed VALUE false PERMISSIONS FULL;
		 DEFINE TABLE flags SCHEMALESS PERMISSIONS FULL;
		 CREATE flags:yes SET k = true, ok = true;
		 CREATE flags:no SET k = false, ok = false;
		 DEFINE TABLE w SCHEMALESS PERMISSIONS FULL;
		 DEFINE TABLE src SCHEMALESS PERMISSIONS FULL;
		 DEFINE TABLE log SCHEMALESS PERMISSIONS FULL;",
	)
	.await;
	// Defined by a database editor, so the function's `AUTH LIMIT` narrows
	// the owner who writes below.
	let editor = crate::dbs::Session::editor().with_ns("test").with_db("test");
	let defined = values(
		&db,
		&editor,
		"DEFINE FUNCTION fn::narrowed($v: any) { RETURN $v } PERMISSIONS FULL;
		 DEFINE FIELD seen ON w VALUE fn::narrowed((SELECT VALUE ok FROM flags WHERE k = $allowed)[0]);
		 DEFINE EVENT ev ON src THEN (
			CREATE log SET of = $after.id,
				ok = fn::narrowed((SELECT VALUE ok FROM flags WHERE k = $allowed)[0])
		 );",
	)
	.await;
	assert_eq!(defined, vec![Value::None, Value::None, Value::None], "the editor's definitions");
	let probes = "[
		(CREATE ONLY w:1 SET v = 1 RETURN VALUE seen),
		(CREATE src:1 RETURN NONE),
		(SELECT VALUE ok FROM log WHERE of = src:1)
	]";
	let got = values(
		&db,
		&TestDb::owner(),
		&format!("RETURN {{ LET $allowed = true; RETURN {probes} }};"),
	)
	.await;
	assert_eq!(got[0], parse("[false, [], [false]]"), "a caller's block LET reached the argument");
}

/// A permission's own `LET`, in the predicate's block or in the body of a
/// function the predicate calls, is read by the statement it holds; a caller's
/// binding of the same name reaches neither. The predicate's block answers its
/// last expression.
#[tokio::test]
async fn a_permission_reads_its_own_let_and_no_callers() {
	let db = TestDb::new_with_auth(
		"DEFINE ACCESS user ON DATABASE TYPE RECORD;
		 DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
		 DEFINE TABLE member SCHEMALESS PERMISSIONS FULL;
		 CREATE member:1 SET user = user:alice;
		 DEFINE FUNCTION fn::member() {
			LET $u = $auth.id;
			RETURN (SELECT VALUE id FROM member WHERE user = $u) != [];
		 } PERMISSIONS FULL;
		 DEFINE TABLE blocked SCHEMALESS PERMISSIONS FOR select WHERE {
			LET $u = $auth.id;
			(SELECT VALUE id FROM member WHERE user = $u) != [];
		 };
		 DEFINE TABLE called SCHEMALESS PERMISSIONS FOR select WHERE fn::member();
		 CREATE blocked:1;
		 CREATE called:1;
		 CREATE user:alice, user:bob;",
	)
	.await;
	let sql = "SELECT VALUE id FROM blocked; SELECT VALUE id FROM called;";
	let alice = values(&db, &record_user("alice"), sql).await;
	assert_eq!(alice[0], parse("[blocked:1]"), "the predicate block's own LET was not read");
	assert_eq!(alice[1], parse("[called:1]"), "the called function's own LET was not read");
	let bob = values(&db, &record_user("bob"), sql).await;
	assert_eq!(bob[0], parse("[]"));
	assert_eq!(bob[1], parse("[]"));
	let shadowed = values(&db, &record_user("alice"), &format!("LET $u = user:bob; {sql}")).await;
	assert_eq!(shadowed[1], parse("[blocked:1]"), "a caller's LET reached the predicate block");
	assert_eq!(shadowed[2], parse("[called:1]"), "a caller's LET reached the called function");
	let by_bob = values(&db, &record_user("bob"), &format!("LET $u = user:alice; {sql}")).await;
	assert_eq!(by_bob[1], parse("[]"), "a caller's LET opened the predicate block");
	assert_eq!(by_bob[2], parse("[]"), "a caller's LET opened the called function");
}

/// A checked principal outside its tenant is refused, before any table's
/// clause is consulted, whether the statement names its table or the run
/// does, and nothing is written.
#[tokio::test]
async fn a_principal_outside_its_tenant_is_refused_whatever_the_clause() {
	let db = TestDb::new_with_auth(
		"DEFINE ACCESS user ON DATABASE TYPE RECORD;
		 DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
		 DEFINE TABLE shut SCHEMALESS PERMISSIONS NONE;
		 CREATE shut:1 SET v = 1;
		 CREATE user:alice;",
	)
	.await;
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "alice"));
	let mut outsider = crate::dbs::Session::for_record("test", "elsewhere", "user", rid);
	outsider.db = Some("test".to_owned());
	for sql in ["UPDATE shut:1 SET v = 2", "UPDATE type::table('shut') SET v = 2", "DELETE shut"] {
		let results = db.datastore().execute(sql, &outsider, None).await.expect("runs");
		match &results[0].result {
			Err(e) => assert!(e.is_not_allowed(), "`{sql}`: expected NotAllowed, got {e}"),
			Ok(v) => panic!("`{sql}` answered {v:?} to a principal outside its tenant"),
		}
	}
	let owner = values(&db, &TestDb::owner(), "SELECT VALUE v FROM shut;").await;
	assert_eq!(owner[0], parse("[1]"), "a principal outside its tenant wrote");
}
