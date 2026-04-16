mod helpers;
use anyhow::Result;
use helpers::{Test, new_ds, skip_ok};
use surrealdb_core::dbs::Session;
use surrealdb_core::iam::{Level, Role};
use surrealdb_types::ToSql;

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
