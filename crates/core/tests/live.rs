mod helpers;
use std::time::Duration;

use anyhow::Result;
use helpers::{new_ds, skip_ok};
use surrealdb_core::dbs::{Action, Session, Variables};
use surrealdb_core::expr::Kind;
use surrealdb_core::val::RecordId;
use surrealdb_core::{strand, syn};

#[tokio::test]
async fn live_permissions() -> Result<()> {
	let dbs = new_ds().await?.with_auth_enabled(true).with_notifications();

	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
	let sql = "
			DEFINE TABLE test SCHEMAFULL PERMISSIONS
				FOR create WHERE { THROW 'create' }
				FOR select WHERE { THROW 'select' }
				FOR update WHERE { THROW 'update' }
				FOR delete WHERE { THROW 'delete' };
			CREATE test:1;
		";
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	skip_ok(res, 1)?;
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: test:1,
			},
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let ses = Session::for_record(
		"test",
		"test",
		"test",
		RecordId::new("user".to_owned(), strand!("test").to_owned()).into(),
	)
	.with_rt(true);
	let sql = "
		LIVE SELECT * FROM type::table('test');
		CREATE test:2;
	";
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	skip_ok(res, 1)?;
	//
	let tmp = res.remove(0).result.unwrap_err().to_string();
	let val = "An error occurred: create".to_string();
	assert_eq!(tmp, val);
	//
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
	let sql = "CREATE test:3;";
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: test:3,
			},
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn live_document_reduction() -> Result<()> {
	// Create a new datastore with notifications enabled
	let dbs = new_ds().await?.with_auth_enabled(true).with_notifications();
	let Some(channel) = dbs.notifications() else {
		unreachable!("No notification channel");
	};

	// Create sessions for owner and record user
	let ses_owner = Session::owner().with_ns("test").with_db("test").with_rt(true);
	let ses_record = Session::for_record(
		"test",
		"test",
		"test",
		RecordId::new("user".to_owned(), strand!("test").to_owned()).into(),
	)
	.with_rt(true);

	// Setup the scenario
	let sql = "
			DEFINE TABLE test SCHEMAFULL PERMISSIONS FULL;
			DEFINE FIELD visible ON test PERMISSIONS FULL;
			DEFINE FIELD hidden ON test PERMISSIONS NONE;
		";
	let res = &mut dbs.execute(sql, &ses_owner, None).await?;
	assert_eq!(res.len(), 3);
	skip_ok(res, 3)?;

	////////////////////////////////////////////////////////////

	// Create a simple live query
	let sql = "LIVE SELECT * FROM test;";
	let res = &mut dbs.execute(sql, &ses_record, None).await?;
	assert_eq!(res.len(), 1);
	let lqid = res.remove(0).result?;
	assert_eq!(lqid.kind(), Some(Kind::Uuid));

	////////////////////////////////////////////////////////////

	// Create a record
	let sql = "CREATE test:1 SET hidden = 123, visible = 'abc';";
	let res = &mut dbs.execute(sql, &ses_owner, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: test:1,
				visible: 'abc',
				hidden: 123,
			},
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);

	// Receive the notification
	let tmp = channel.recv().await?;
	assert_eq!(tmp.action, Action::Create);

	// Check the notification
	let val = syn::value(
		"{
			id: test:1,
			visible: 'abc',
		}",
	)
	.unwrap();
	assert_eq!(tmp.result, val);

	////////////////////////////////////////////////////////////

	// Update the record
	let sql = "UPDATE test:1 SET hidden = 456, visible = 'def';";
	let res = &mut dbs.execute(sql, &ses_owner, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: test:1,
				visible: 'def',
				hidden: 456,
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);

	// Receive the notification
	let tmp = channel.recv().await?;
	assert_eq!(tmp.action, Action::Update);

	// Check the notification
	let val = syn::value(
		"{
			id: test:1,
			visible: 'def',
		}",
	)
	.unwrap();
	assert_eq!(tmp.result, val);

	////////////////////////////////////////////////////////////

	// Delete the record
	let sql = "DELETE test:1;";
	let res = &mut dbs.execute(sql, &ses_owner, None).await?;
	assert_eq!(res.len(), 1);
	skip_ok(res, 1)?;

	// Receive the notification
	let tmp = channel.recv().await?;
	assert_eq!(tmp.action, Action::Delete);

	// Check the notification
	let val = syn::value(
		"{
			id: test:1,
			visible: 'def',
		}",
	)
	.unwrap();
	assert_eq!(tmp.result, val);

	////////////////////////////////////////////////////////////

	// Kill the live query
	let sql = "KILL $uuid";
	let res = &mut dbs
		.execute(sql, &ses_owner, Some(Variables(map!("uuid".to_string() => lqid))))
		.await?;
	assert_eq!(res.len(), 1);
	skip_ok(res, 1)?;

	// Receive the notification
	let tmp = channel.recv().await?;
	assert_eq!(tmp.action, Action::Killed);

	// Create a live query with a WHERE clause
	let sql = "LIVE SELECT * FROM test WHERE hidden = 123;";
	let res = &mut dbs.execute(sql, &ses_record, None).await?;
	assert_eq!(res.len(), 1);
	let lqid = res.remove(0).result?;
	assert_eq!(lqid.kind(), Some(Kind::Uuid));

	////////////////////////////////////////////////////////////

	// Create a record
	let sql = "CREATE test:2 SET hidden = 123, visible = 'abc';";
	let res = &mut dbs.execute(sql, &ses_owner, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: test:2,
				visible: 'abc',
				hidden: 123,
			},
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);

	// Assert no notification is received
	tokio::time::sleep(Duration::from_secs(1)).await;
	let res = channel.try_recv();
	assert!(res.is_err());

	////////////////////////////////////////////////////////////

	// Test passed!
	Ok(())
}
