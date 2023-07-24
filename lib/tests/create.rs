mod parse;
use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
use surrealdb::sql::Value;

#[tokio::test]
async fn create_with_id() -> Result<(), Error> {
	let sql = "
		CREATE person:test SET name = 'Tester';
		CREATE person SET id = person:tobie, name = 'Tobie';
		CREATE person CONTENT { id: person:jaime, name: 'Jaime' };
		CREATE user CONTENT { id: 1, name: 'Robert' };
		CREATE city CONTENT { id: 'london', name: 'London' };
		CREATE city CONTENT { id: '8e60244d-95f6-4f95-9e30-09a98977efb0', name: 'London' };
		CREATE temperature CONTENT { id: ['London', '2022-09-30T20:25:01.406828Z'], name: 'London' };
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 7);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				name: 'Tester'
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
				name: 'Tobie'
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
				name: 'Jaime'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: user:1,
				name: 'Robert'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: city:london,
				name: 'London'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: city:⟨8e60244d-95f6-4f95-9e30-09a98977efb0⟩,
				name: 'London'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: temperature:['London', '2022-09-30T20:25:01.406828Z'],
				name: 'London'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn create_on_none_values_with_unique_index() -> Result<(), Error> {
	let sql = "
		DEFINE INDEX national_id_idx ON foo FIELDS national_id UNIQUE;
		CREATE foo SET name = 'John Doe';
		CREATE foo SET name = 'Jane Doe';
	";

	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	for _ in 0..3 {
		let _ = res.remove(0).result?;
	}
	Ok(())
}

#[tokio::test]
async fn create_with_unique_index_on_no_flatten_fields() -> Result<(), Error> {
	let sql = "
		DEFINE INDEX test ON user FIELDS account, tags, emails UNIQUE;
		CREATE user:1 SET account = 'Apple', tags = ['one', 'two'], emails = ['a@example.com', 'b@example.com'];
		CREATE user:2 SET account = 'Apple', tags = ['two', 'three'], emails = ['a@example.com', 'b@example.com'];
		CREATE user:3 SET account = 'Apple', tags = ['one', 'two'], emails = ['a@example.com', 'b@example.com'];
		CREATE user:4 SET account = 'Apple', tags = ['two', 'three'], emails = ['a@example.com', 'b@example.com'];
	";

	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	for _ in 0..3 {
		let _ = res.remove(0).result?;
	}
	//
	let tmp = res.remove(0).result;
	if let Err(e) = tmp {
		assert_eq!(e.to_string(), "Database index `test` already contains ['Apple', ['one', 'two'], ['a@example.com', 'b@example.com']], with record `user:3`");
	} else {
		panic!("An error was expected.")
	}
	//
	let tmp = res.remove(0).result;
	if let Err(e) = tmp {
		assert_eq!(e.to_string(), "Database index `test` already contains ['Apple', ['two', 'three'], ['a@example.com', 'b@example.com']], with record `user:4`");
	} else {
		panic!("An error was expected.")
	}
	Ok(())
}

#[tokio::test]
async fn create_with_unique_index_on_one_flatten_fields() -> Result<(), Error> {
	let sql = "
		DEFINE INDEX test ON user FIELDS account, tags[*], emails UNIQUE;
		CREATE user:1 SET account = 'Apple', tags = ['one', 'two'], emails = ['a@example.com', 'b@example.com'];
		CREATE user:2 SET account = 'Apple', tags = ['two', 'three'], emails = ['a@example.com', 'b@example.com'];
	";

	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	for _ in 0..2 {
		let _ = res.remove(0).result?;
	}
	//
	let tmp = res.remove(0).result;
	if let Err(e) = tmp {
		assert_eq!(e.to_string(), "Database index `test` already contains ['Apple', 'two', ['a@example.com', 'b@example.com']], with record `user:2`");
	} else {
		panic!("An error was expected.")
	}
	Ok(())
}

#[tokio::test]
async fn create_with_unique_index_on_one_flatten_fields_with_sub_values() -> Result<(), Error> {
	let sql = "
		DEFINE INDEX test ON user FIELDS account, tags[*], emails.*.value UNIQUE;
		CREATE user:1 SET account = 'Apple', tags = ['one', 'two'], emails = [ { value:'a@example.com'} , { value:'b@example.com' } ];
		CREATE user:2 SET account = 'Apple', tags = ['two', 'three'], emails = [ { value:'a@example.com'} , { value:'b@example.com' } ];
	";

	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	for _ in 0..2 {
		let _ = res.remove(0).result?;
	}
	//
	let tmp = res.remove(0).result;
	if let Err(e) = tmp {
		assert_eq!(e.to_string(), "Database index `test` already contains ['Apple', 'two', ['a@example.com', 'b@example.com']], with record `user:2`");
	} else {
		panic!("An error was expected.")
	}
	Ok(())
}

#[tokio::test]
async fn create_with_unique_index_on_two_flatten_fields() -> Result<(), Error> {
	let sql = "
		DEFINE INDEX test ON user FIELDS account, tags[*], emails[*] UNIQUE;
		CREATE user:1 SET account = 'Apple', tags = ['one', 'two'], emails = ['a@example.com', 'b@example.com'];
		CREATE user:2 SET account = 'Apple', tags = ['one', 'two'], emails = ['b@example.com', 'c@example.com'];
	";

	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	for _ in 0..2 {
		let _ = res.remove(0).result?;
	}
	let tmp = res.remove(0).result;
	//
	if let Err(e) = tmp {
		assert_eq!(e.to_string(), "Database index `test` already contains ['Apple', 'two', 'b@example.com'], with record `user:2`");
	} else {
		panic!("An error was expected.")
	}
	Ok(())
}
