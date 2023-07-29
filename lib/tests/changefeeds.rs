mod parse;
use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
use surrealdb::sql::Value;

#[tokio::test]
async fn table_change_feeds() -> Result<(), Error> {
	let sql = "
        DEFINE TABLE person CHANGEFEED 1h;
		DEFINE FIELD name ON TABLE person
			ASSERT
				IF $input THEN
					$input = /^[A-Z]{1}[a-z]+$/
				ELSE
					true
				END
			VALUE
				IF $input THEN
					'Name: ' + $input
				ELSE
					$value
				END
		;
		UPDATE person:test CONTENT { name: 'Tobie' };
		UPDATE person:test REPLACE { name: 'jaime' };
		UPDATE person:test MERGE { name: 'Jaime' };
		UPDATE person:test SET name = 'tobie';
		UPDATE person:test SET name = 'Tobie';
		DELETE person:test;
		CREATE person:1000 SET name = 'Yusuke';
        SHOW CHANGES FOR TABLE person SINCE 0;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let start_ts = 0u64;
	let end_ts = start_ts + 1;
	dbs.tick_at(start_ts).await?;
	let res = &mut dbs.execute(&sql, &ses, None).await?;
	dbs.tick_at(end_ts).await?;
	assert_eq!(res.len(), 10);
	// DEFINE TABLE
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	// DEFINE FIELD
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	// UPDATE CONTENT
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				name: 'Name: Tobie',
			}
		]",
	);
	assert_eq!(tmp, val);
	// UPDATE REPLACE
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Found 'Name: jaime' for field `name`, with record `person:test`, but field must conform to: IF $input THEN $input = /^[A-Z]{1}[a-z]+$/ ELSE true END"#
	));
	// UPDATE MERGE
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				name: 'Name: Jaime',
			}
		]",
	);
	assert_eq!(tmp, val);
	// UPDATE SET
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Found 'Name: tobie' for field `name`, with record `person:test`, but field must conform to: IF $input THEN $input = /^[A-Z]{1}[a-z]+$/ ELSE true END"#
	));
	// UPDATE SET
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				name: 'Name: Tobie',
			}
		]",
	);
	assert_eq!(tmp, val);
	// DELETE
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	// CREATE
	let _tmp = res.remove(0).result?;
	// SHOW CHANGES
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				versionstamp: 65536,
				changes: [
					{
						update: {
							id: person:test,
							name: 'Name: Tobie'
						}
					}
				]
			},
			{
				versionstamp: 131072,
				changes: [
					{
						update: {
							id: person:test,
							name: 'Name: Jaime'
						}
					}
				]
			},
			{
				versionstamp: 196608,
				changes: [
					{
						update: {
							id: person:test,
							name: 'Name: Tobie'
						}
					}
				]
			},
			{
				versionstamp: 262144,
				changes: [
					{
						delete: {
							id: person:test
						}
					}
				]
			},
			{
				versionstamp: 327680,
				changes: [
					{
						update: {
							id: person:1000,
							name: 'Name: Yusuke'
						}
					}
				]
			}
		]",
	);
	assert_eq!(tmp, val);
	// Retain for 1h
	let sql = "
        SHOW CHANGES FOR TABLE person SINCE 0;
	";
	dbs.tick_at(end_ts + 3599).await?;
	let res = &mut dbs.execute(&sql, &ses, None).await?;
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, val);
	// GC after 1hs
	dbs.tick_at(end_ts + 3600).await?;
	let res = &mut dbs.execute(&sql, &ses, None).await?;
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	Ok(())
}
