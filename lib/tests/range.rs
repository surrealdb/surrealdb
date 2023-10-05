mod parse;
use parse::Parse;
mod helpers;
use helpers::new_ds;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::sql::Value;

#[tokio::test]
async fn select_start_limit_fetch() -> Result<(), Error> {
	let sql = "
		CREATE tag:rs SET name = 'Rust';
		CREATE tag:go SET name = 'Golang';
		CREATE tag:js SET name = 'JavaScript';
		CREATE person:tobie SET tags = [tag:rs, tag:go, tag:js];
		CREATE person:jaime SET tags = [tag:js];
		SELECT * FROM person LIMIT 1 FETCH tags;
		SELECT * FROM person START 1 LIMIT 1 FETCH tags;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 7);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: tag:rs,
				name: 'Rust'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: tag:go,
				name: 'Golang'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: tag:js,
				name: 'JavaScript'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:tobie,
				tags: [tag:rs, tag:go, tag:js]
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:jaime,
				tags: [tag:js]
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:jaime,
				tags: [
					{
						id: tag:js,
						name: 'JavaScript'
					}
				]
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:tobie,
				tags: [
					{
						id: tag:rs,
						name: 'Rust'
					},
					{
						id: tag:go,
						name: 'Golang'
					},
					{
						id: tag:js,
						name: 'JavaScript'
					}
				]
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}
