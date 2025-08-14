mod helpers;
use helpers::new_ds;
use surrealdb::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::val::{Array, RecordId};
use surrealdb_core::{strand, syn};

use crate::helpers::Test;

#[tokio::test]
async fn field_definition_value_reference() -> Result<()> {
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
	tmp.unwrap();
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
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
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: contains:test,
				in: product:one,
				out: product:two,
			},
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
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
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
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
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
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
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn field_definition_edge_permissions() -> Result<()> {
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
	tmp.unwrap();
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: user:one,
			},
			{
				id: user:two,
			},
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
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
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let sql = "
		RELATE business:one->contact:one->business:two;
		RELATE business:two->contact:two->business:one;
	";
	let ses = Session::for_record(
		"test",
		"test",
		"test",
		RecordId::new("user".to_owned(), strand!("one").to_owned()).into(),
	);
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: contact:one,
				in: business:one,
				out: business:two,
			},
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Array::new().into();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn field_definition_readonly() -> Result<()> {
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
	tmp.unwrap();
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				birthdate: d'2023-12-13T21:27:55.632Z',
				id: person:test
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				birthdate: d'2023-12-13T21:27:55.632Z',
				id: person:test
			}
		]",
	)
	.unwrap();
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
async fn field_definition_flexible_array_any() -> Result<()> {
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
async fn field_definition_array_any() -> Result<()> {
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
