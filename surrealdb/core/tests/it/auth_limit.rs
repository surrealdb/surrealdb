use anyhow::Result;
use surrealdb_core::dbs::Session;
use surrealdb_iam::{Level, Role};
use surrealdb_types::ToSql;

use crate::helpers::{Test, new_ds, skip_ok};

#[tokio::test]
async fn auth_limit_diff_role() -> Result<()> {
	let (_, dbs) = new_ds("test", "test", true).await?;

	let ses_owner = Session::owner().with_ns("test").with_db("test");
	let ses_editor = Session::editor().with_ns("test").with_db("test");
	let sql = "
			DEFINE FUNCTION fn::a() {
				DEFINE USER x ON DATABASE ROLES OWNER PASSWORD 'pass';
			};
		";
	let res = &mut dbs.execute(sql, &ses_editor, None).await?;
	assert_eq!(res.len(), 1);
	//
	skip_ok(res, 1)?;
	//
	let sql = "
		fn::a();
	";
	let res = &mut dbs.execute(sql, &ses_owner, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_err());
	assert_eq!(
		tmp.unwrap_err().to_string(),
		"IAM error: Not enough permissions to perform this action"
	);
	//
	Ok(())
}

#[tokio::test]
async fn auth_limit_diff_level() -> Result<()> {
	let (_, dbs) = new_ds("test", "test", true).await?;

	let ses_ns = Session::for_level(Level::Namespace("test".to_string()), Role::Owner)
		.with_ns("test")
		.with_db("test");

	let ses_db =
		Session::for_level(Level::Database("test".to_string(), "test".to_string()), Role::Owner)
			.with_ns("test")
			.with_db("test");

	let sql = "
			DEFINE FUNCTION fn::a() {
				DEFINE USER x ON NAMESPACE ROLES OWNER PASSWORD 'pass';
			};
		";
	let res = &mut dbs.execute(sql, &ses_db, None).await?;
	assert_eq!(res.len(), 1);
	//
	skip_ok(res, 1)?;
	//
	let sql = "
		fn::a();
	";
	let res = &mut dbs.execute(sql, &ses_ns, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_err());
	assert_eq!(
		tmp.unwrap_err().to_string(),
		"IAM error: Not enough permissions to perform this action"
	);
	//
	Ok(())
}

/// ALTER FUNCTION must recompute auth_limit from the altering principal.
/// A NS-owner defines a harmless function; a DB-owner alters it to inject a
/// namespace-level DEFINE USER. The recomputed auth_limit (DB-owner) must
/// prevent the escalation even when invoked by a NS-owner.
#[tokio::test]
async fn auth_limit_alter_function_recomputes() -> Result<()> {
	let (_, dbs) = new_ds("test", "test", true).await?;

	let ses_ns = Session::for_level(Level::Namespace("test".to_string()), Role::Owner)
		.with_ns("test")
		.with_db("test");

	let ses_db =
		Session::for_level(Level::Database("test".to_string(), "test".to_string()), Role::Owner)
			.with_ns("test")
			.with_db("test");

	// NS-owner defines a function with a safe body
	let sql = "DEFINE FUNCTION fn::escalate() { RETURN 'safe'; };";
	let res = &mut dbs.execute(sql, &ses_ns, None).await?;
	skip_ok(res, 1)?;

	// DB-owner alters the function body to attempt namespace-level escalation
	let sql = "
		ALTER FUNCTION fn::escalate() {
			DEFINE USER x ON NAMESPACE ROLES OWNER PASSWORD 'pass';
		};
	";
	let res = &mut dbs.execute(sql, &ses_db, None).await?;
	skip_ok(res, 1)?;

	// NS-owner invokes -- auth_limit was recomputed to DB-owner level on ALTER,
	// so the embedded DEFINE USER ON NAMESPACE must fail
	let sql = "fn::escalate();";
	let res = &mut dbs.execute(sql, &ses_ns, None).await?;
	assert_eq!(res.len(), 1);

	let tmp = res.remove(0).result;
	assert!(tmp.is_err());
	assert_eq!(
		tmp.unwrap_err().to_string(),
		"IAM error: Not enough permissions to perform this action"
	);

	Ok(())
}

/// ALTER API must recompute auth_limit from the altering principal.
/// A NS-owner defines an API; a DB-owner alters its handler to inject a
/// namespace-level DEFINE USER. The recomputed auth_limit (DB-owner) must
/// prevent the escalation: no new user should appear in INFO FOR NS.
#[tokio::test]
async fn auth_limit_alter_api_recomputes() -> Result<()> {
	let (not, dbs) = new_ds("test", "test", true).await?;

	let ses_ns = Session::for_level(Level::Namespace("test".to_string()), Role::Owner)
		.with_ns("test")
		.with_db("test");

	let ses_db =
		Session::for_level(Level::Database("test".to_string(), "test".to_string()), Role::Owner)
			.with_ns("test")
			.with_db("test");

	// NS-owner defines a safe API
	let sql = r#"
		DEFINE API "/test/escalate"
			FOR get THEN {
				{ status: 200, body: 'safe' };
			};
	"#;
	let res = &mut dbs.execute(sql, &ses_ns, None).await?;
	skip_ok(res, 1)?;

	// DB-owner alters the handler to attempt namespace-level escalation
	let sql = r#"
		ALTER API "/test/escalate"
			FOR get THEN {
				DEFINE USER backdoor ON NAMESPACE PASSWORD 'pass' ROLES OWNER;
				{ status: 200, body: 'escalated' };
			};
	"#;
	let res = &mut dbs.execute(sql, &ses_db, None).await?;
	skip_ok(res, 1)?;

	// NS-owner invokes the API -- the handler body must be blocked by the
	// recomputed auth_limit, producing a 500 response with the IAM error.
	let sql = r#"RETURN api::invoke("/test/escalate");"#;
	let res = &mut dbs.execute(sql, &ses_ns, None).await?;
	assert_eq!(res.len(), 1);
	let response = res.remove(0).result?.to_sql();
	assert!(
		response.contains("500"),
		"Expected status 500 from blocked API handler, got: {response}"
	);
	assert!(
		response.contains("Not enough permissions"),
		"Expected IAM error in API response, got: {response}"
	);

	// Belt-and-suspenders: verify the backdoor user was NOT created
	let mut t = Test::new_ds_session(dbs, not, ses_ns, "INFO FOR NS").await?;
	let info = t.next_value()?.to_sql();
	assert!(
		!info.contains("backdoor"),
		"Privilege escalation succeeded: namespace user 'backdoor' was created"
	);

	Ok(())
}

/// A field's ASSERT may not hold a schema statement, whoever defines the
/// field: a DB-editor's `id` ASSERT embedding a DEFINE USER ROLES OWNER is
/// refused at DEFINE, so no later write can run the embedded escalation.
#[tokio::test]
async fn auth_limit_id_field_assert() -> Result<()> {
	let (_, dbs) = new_ds("test", "test", true).await?;

	let ses_owner = Session::owner().with_ns("test").with_db("test");
	let ses_editor = Session::editor().with_ns("test").with_db("test");

	let sql = "
		DEFINE FIELD id ON ta ASSERT (DEFINE USER escalate_assert ON DATABASE ROLES OWNER PASSWORD 'pass') OR true;
	";
	let refused = dbs.execute(sql, &ses_editor, None).await.expect_err("the DEFINE is refused");
	assert!(
		refused
			.to_string()
			.contains("The ASSERT clause must not have effects: `DEFINE` changes the schema"),
		"{refused}"
	);

	let res = &mut dbs.execute("CREATE ta:foo;", &ses_owner, None).await?;
	skip_ok(res, 1)?;

	// Belt-and-suspenders: the escalated user must not have been created.
	let info = &mut dbs.execute("INFO FOR DB", &ses_owner, None).await?;
	let info = info.remove(0).result?.to_sql();
	assert!(
		!info.contains("escalate_assert"),
		"Privilege escalation succeeded: an owner-level user was created"
	);

	Ok(())
}

/// The principal a function body runs under is derived from the caller and the
/// definition read, once per pairing, and answered again for every later call
/// of the pairing: a recursion, a call per row, a later statement. Every one of
/// those calls is narrowed by the definer's limit, and no call is raised above
/// its caller.
#[tokio::test]
async fn auth_limit_holds_for_every_call_of_a_function() -> Result<()> {
	let (_, dbs) = new_ds("test", "test", true).await?;

	let ses_owner = Session::owner().with_ns("test").with_db("test");
	let ses_editor = Session::editor().with_ns("test").with_db("test");

	// The base case of the recursion attempts what only an owner may do.
	let escalating = "
		DEFINE FUNCTION fn::deep($n: int) {
			IF $n <= 0 {
				DEFINE USER OVERWRITE escalated ON DATABASE ROLES OWNER PASSWORD 'pass';
				RETURN 0;
			};
			RETURN fn::deep($n - 1) + 1;
		};
	";
	let res = &mut dbs.execute(escalating, &ses_editor, None).await?;
	skip_ok(res, 1)?;

	// Called by the owner, every call runs as the editor that defined it, however
	// many calls of the pairing came first.
	for sql in [
		"fn::deep(0);",
		"fn::deep(4);",
		"SELECT VALUE fn::deep(n) FROM [{ n: 2 }, { n: 0 }, { n: 7 }];",
		"fn::deep(1);",
	] {
		let res = &mut dbs.execute(sql, &ses_owner, None).await?;
		assert_eq!(res.len(), 1);
		let tmp = res.remove(0).result;
		assert!(tmp.is_err(), "the editor's limit was not applied to: {sql}");
		assert_eq!(
			tmp.unwrap_err().to_string(),
			"IAM error: Not enough permissions to perform this action",
			"{sql}"
		);
	}
	let info = &mut dbs.execute("INFO FOR DB", &ses_owner, None).await?;
	let info = info.remove(0).result?.to_sql();
	// The definition of the function names the user; only a created user is defined as one.
	assert!(
		!info.contains("DEFINE USER escalated ON"),
		"an owner-level user was created by a limited call"
	);

	// A function the owner defines runs as the owner when the owner calls it,
	// and as the caller, never above it, when the editor does.
	let owned = "
		DEFINE FUNCTION fn::owned($n: int) {
			IF $n <= 0 {
				DEFINE USER OVERWRITE made ON DATABASE ROLES OWNER PASSWORD 'pass';
				RETURN 0;
			};
			RETURN fn::owned($n - 1) + 1;
		};
	";
	let res = &mut dbs.execute(owned, &ses_owner, None).await?;
	skip_ok(res, 1)?;
	for sql in ["fn::owned(3);", "SELECT VALUE fn::owned(n) FROM [{ n: 1 }, { n: 2 }];"] {
		let res = &mut dbs.execute(sql, &ses_editor, None).await?;
		assert_eq!(res.len(), 1);
		let tmp = res.remove(0).result;
		assert!(tmp.is_err(), "the editor was raised to the owner's limit by: {sql}");
		assert_eq!(
			tmp.unwrap_err().to_string(),
			"IAM error: Not enough permissions to perform this action",
			"{sql}"
		);
	}
	let res = &mut dbs.execute("fn::owned(3);", &ses_owner, None).await?;
	assert_eq!(res.len(), 1);
	assert_eq!(res.remove(0).result?.to_sql(), "3");
	let info = &mut dbs.execute("INFO FOR DB", &ses_owner, None).await?;
	let info = info.remove(0).result?.to_sql();
	assert!(info.contains("DEFINE USER made ON"), "the owner's own call did not run as the owner");

	Ok(())
}

/// A function redefined by a principal with a narrower limit between two calls
/// by the same caller is narrowed by the definition the second call reads.
#[tokio::test]
async fn auth_limit_follows_the_definition_read() -> Result<()> {
	let (_, dbs) = new_ds("test", "test", true).await?;

	let ses_owner = Session::owner().with_ns("test").with_db("test");
	let ses_editor = Session::editor().with_ns("test").with_db("test");

	let body = "
		DEFINE USER OVERWRITE probe ON DATABASE ROLES OWNER PASSWORD 'pass';
		RETURN 1;
	";
	let res = &mut dbs
		.execute(&format!("DEFINE FUNCTION fn::probe() {{ {body} }};"), &ses_owner, None)
		.await?;
	skip_ok(res, 1)?;

	let res = &mut dbs.execute("fn::probe();", &ses_owner, None).await?;
	assert_eq!(res.remove(0).result?.to_sql(), "1");

	let res = &mut dbs
		.execute(&format!("DEFINE FUNCTION OVERWRITE fn::probe() {{ {body} }};"), &ses_editor, None)
		.await?;
	skip_ok(res, 1)?;

	let res = &mut dbs.execute("fn::probe();", &ses_owner, None).await?;
	let tmp = res.remove(0).result;
	assert!(tmp.is_err(), "the redefinition's limit was not applied");
	assert_eq!(
		tmp.unwrap_err().to_string(),
		"IAM error: Not enough permissions to perform this action"
	);

	Ok(())
}

/// A field's clauses run under the `AUTH LIMIT` of the user who defined the
/// field, so a call of a function in them is made for that user narrowed: a
/// database editor's `VALUE`, `ASSERT`, `DEFAULT`, `PERMISSIONS` and
/// `COMPUTED` clauses calling a function answer every writer alike.
#[tokio::test]
async fn auth_limit_field_clauses_call_functions_for_every_writer() -> Result<()> {
	let (_, dbs) = new_ds("test", "test", true).await?;
	let ses_owner = Session::owner().with_ns("test").with_db("test");
	let ses_editor = Session::editor().with_ns("test").with_db("test");
	let sql = "
		DEFINE FUNCTION fn::double($n: int) { RETURN $n * 2; };
		DEFINE FIELD a ON p TYPE int;
		DEFINE FIELD f ON p COMPUTED fn::double(a);
		DEFINE FIELD g ON p VALUE f + 1;
		DEFINE FIELD v ON p VALUE fn::double($value) ASSERT fn::double($value) > 0;
		DEFINE FIELD d ON p DEFAULT fn::double(21);
		DEFINE FIELD w ON p PERMISSIONS FOR create, update WHERE fn::double(1) = 2;
	";
	let res = &mut dbs.execute(sql, &ses_editor, None).await?;
	skip_ok(res, 7)?;
	for (ses, id) in [(&ses_owner, 1), (&ses_editor, 2)] {
		let sql = format!("CREATE ONLY p:{id} SET a = 2, v = 3, w = true");
		let res = &mut dbs.execute(&sql, ses, None).await?;
		let created = res.remove(0).result?.to_sql();
		assert_eq!(created, format!("{{ a: 2, d: 42, f: 4, g: 5, id: p:{id}, v: 6, w: true }}"));
	}
	let res = &mut dbs.execute("CREATE p:3 SET a = 2, v = -1", &ses_owner, None).await?;
	let refused = res.remove(0).result.expect_err("the ASSERT admitted -2").to_string();
	assert!(refused.contains("fn::double($value) > 0"), "{refused}");
	Ok(())
}

/// A field's VALUE may not hold a schema statement, whoever defines the
/// field: an editor's field that would define an owner is refused at DEFINE,
/// so an owner's write cannot run it.
#[tokio::test]
async fn auth_limit_field_value_statement() -> Result<()> {
	let (_, dbs) = new_ds("test", "test", true).await?;
	let ses_owner = Session::owner().with_ns("test").with_db("test");
	let ses_editor = Session::editor().with_ns("test").with_db("test");
	let sql = "DEFINE FIELD x ON t VALUE (DEFINE USER escalate_value ON DATABASE ROLES OWNER PASSWORD 'pass');";
	let refused = dbs.execute(sql, &ses_editor, None).await.expect_err("the DEFINE is refused");
	assert!(
		refused
			.to_string()
			.contains("The VALUE clause must not have effects: `DEFINE` changes the schema"),
		"{refused}"
	);
	let res = &mut dbs.execute("CREATE t:1;", &ses_owner, None).await?;
	skip_ok(res, 1)?;
	let info = dbs.execute("INFO FOR DB", &ses_owner, None).await?.remove(0).result?.to_sql();
	assert!(!info.contains("escalate_value"), "an owner-level user was created: {info}");
	Ok(())
}
