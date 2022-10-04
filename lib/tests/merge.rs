mod parse;
use parse::Parse;
use surrealdb::sql::Value;
use surrealdb::Datastore;
use surrealdb::Error;
use surrealdb::Session;

#[tokio::test]
async fn merge_record() -> Result<(), Error> {
	let sql = "
		UPDATE person:test SET name.initials = 'TMH', name.first = 'Tobie', name.last = 'Morgan Hitchcock';
		UPDATE person:test MERGE {
			name: {
				title: 'Mr',
				initials: NONE,
				suffix: ['BSc', 'MSc'],
			}
		};
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				name: {
					initials: 'TMH',
					first: 'Tobie',
					last: 'Morgan Hitchcock',
				}
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				name: {
					title: 'Mr',
					first: 'Tobie',
					last: 'Morgan Hitchcock',
					suffix: ['BSc', 'MSc'],
				}
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}
