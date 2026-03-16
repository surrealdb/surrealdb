mod helpers;
use anyhow::Result;
use helpers::{new_ds, skip_ok};
use surrealdb_core::dbs::Session;
use surrealdb_core::iam::{Level, Role};

#[tokio::test]
async fn auth_limit_diff_role() -> Result<()> {
	let dbs = new_ds("test", "test").await?.with_auth_enabled(true).with_notifications();

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
	let dbs = new_ds("test", "test").await?.with_auth_enabled(true).with_notifications();

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
