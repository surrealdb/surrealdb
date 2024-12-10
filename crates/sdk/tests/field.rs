mod parse;
use parse::Parse;
mod helpers;
use crate::helpers::Test;
use helpers::new_ds;
use helpers::with_enough_stack;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::sql::Thing;
use surrealdb::sql::Value;

#[tokio::test]
async fn field_definition_value_reference() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE product;
		DEFINE FIELD subproducts ON product VALUE ->contains->product;
		CREATE product:one, product:two;
		RELATE product:one->contains:test->product:two;
		SELECT * FROM product;
		UPDATE product;
		SELECT * FROM product;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 7);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: product:one,
				subproducts: [],
			},
			{
				id: product:two,
				subproducts: [],
			},
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: contains:test,
				in: product:one,
				out: product:two,
			},
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: product:one,
				subproducts: [],
			},
			{
				id: product:two,
				subproducts: [],
			},
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: product:one,
				subproducts: [
					product:two,
				],
			},
			{
				id: product:two,
				subproducts: [],
			},
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: product:one,
				subproducts: [
					product:two,
				],
			},
			{
				id: product:two,
				subproducts: [],
			},
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn field_definition_value_reference_with_future() -> Result<(), Error> {
	with_enough_stack(async {
		let sql = "
		DEFINE TABLE product;
		DEFINE FIELD subproducts ON product VALUE <future> { ->contains->product };
		CREATE product:one, product:two;
		RELATE product:one->contains:test->product:two;
		SELECT * FROM product;
		UPDATE product;
		SELECT * FROM product;
	";
		let dbs = new_ds().await?;
		let ses = Session::owner().with_ns("test").with_db("test");
		let res = &mut dbs.execute(sql, &ses, None).await?;
		assert_eq!(res.len(), 7);
		//
		let tmp = res.remove(0).result;
		assert!(tmp.is_ok());
		//
		let tmp = res.remove(0).result;
		assert!(tmp.is_ok());
		//
		let tmp = res.remove(0).result?;
		let val = Value::parse(
			"[
			{
				id: product:one,
				subproducts: [],
			},
			{
				id: product:two,
				subproducts: [],
			},
		]",
		);
		assert_eq!(tmp, val);
		//
		let tmp = res.remove(0).result?;
		let val = Value::parse(
			"[
			{
				id: contains:test,
				in: product:one,
				out: product:two,
			},
		]",
		);
		assert_eq!(tmp, val);
		//
		let tmp = res.remove(0).result?;
		let val = Value::parse(
			"[
			{
				id: product:one,
				subproducts: [
					product:two,
				],
			},
			{
				id: product:two,
				subproducts: [],
			},
		]",
		);
		assert_eq!(tmp, val);
		//
		let tmp = res.remove(0).result?;
		let val = Value::parse(
			"[
			{
				id: product:one,
				subproducts: [
					product:two,
				],
			},
			{
				id: product:two,
				subproducts: [],
			},
		]",
		);
		assert_eq!(tmp, val);
		//
		let tmp = res.remove(0).result?;
		let val = Value::parse(
			"[
			{
				id: product:one,
				subproducts: [
					product:two,
				],
			},
			{
				id: product:two,
				subproducts: [],
			},
		]",
		);
		assert_eq!(tmp, val);
		//
		Ok(())
	})
}

#[tokio::test]
async fn field_definition_edge_permissions() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE user SCHEMAFULL;
		DEFINE TABLE business SCHEMAFULL;
		DEFINE FIELD owner ON TABLE business TYPE record<user>;
		DEFINE TABLE contact TYPE RELATION SCHEMAFULL PERMISSIONS FOR select, create WHERE in.owner.id = $auth.id;
		INSERT INTO user (id, name) VALUES (user:one, 'John'), (user:two, 'Lucy');
		INSERT INTO business (id, owner) VALUES (business:one, user:one), (business:two, user:two);
	";
	let dbs = new_ds().await?.with_auth_enabled(true);
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 6);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: user:one,
			},
			{
				id: user:two,
			},
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: business:one,
				owner: user:one,
			},
			{
				id: business:two,
				owner: user:two,
			},
		]",
	);
	assert_eq!(tmp, val);
	//
	let sql = "
		RELATE business:one->contact:one->business:two;
		RELATE business:two->contact:two->business:one;
	";
	let ses = Session::for_record("test", "test", "test", Thing::from(("user", "one")).into());
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: contact:one,
				in: business:one,
				out: business:two,
			},
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn field_definition_readonly() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE person SCHEMAFULL;
		DEFINE FIELD birthdate ON person TYPE datetime READONLY;
		CREATE person:test SET birthdate = d'2023-12-13T21:27:55.632Z';
		UPSERT person:test SET birthdate = d'2023-12-13T21:27:55.632Z';
		UPSERT person:test SET birthdate = d'2024-12-13T21:27:55.632Z';
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				birthdate: d'2023-12-13T21:27:55.632Z',
				id: person:test
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				birthdate: d'2023-12-13T21:27:55.632Z',
				id: person:test
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Found changed value for field `birthdate`, with record `person:test`, but field is readonly",

		),
		"{}",
		tmp.unwrap_err().to_string()
	);
	//
	Ok(())
}

#[tokio::test]
async fn field_definition_flexible_array_any() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE user SCHEMAFULL;
		DEFINE FIELD custom ON user TYPE option<array>;
		DEFINE FIELD custom.* ON user FLEXIBLE TYPE any;
		CREATE user:one CONTENT { custom: ['sometext'] };
		CREATE user:two CONTENT { custom: [ ['sometext'] ] };
		CREATE user:three CONTENT { custom: [ { key: 'sometext' } ] };
	";
	let mut t = Test::new(sql).await?;
	t.skip_ok(3)?;
	t.expect_val(
		"[
			{
				custom: [
					'sometext'
				],
				id: user:one
			},
		]",
	)?;
	t.expect_val(
		"[
			{
				custom: [
					[
						'sometext'
					]
				],
				id: user:two
			},
		]",
	)?;
	t.expect_val(
		"[
			{
				custom: [
					{
						key: 'sometext'
					}
				],
				id: user:three
			}
		]",
	)?;
	Ok(())
}

#[tokio::test]
async fn field_definition_array_any() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE user SCHEMAFULL;
		DEFINE FIELD custom ON user TYPE array<any>;
		INFO FOR TABLE user;
	";
	let mut t = Test::new(sql).await?;
	t.skip_ok(2)?;
	t.expect_val(
		"
{
	events: {  },
	fields: { custom: 'DEFINE FIELD custom ON user TYPE array PERMISSIONS FULL' },
	indexes: {  },
	lives: {  },
	tables: {  }
}
		",
	)?;
	Ok(())
}
