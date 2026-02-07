mod helpers;
use anyhow::Result;
use helpers::new_ds;
use surrealdb_core::dbs::Session;
use surrealdb_core::syn;
use surrealdb_types::{Array, RecordId, Value};

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
	let dbs = new_ds("test", "test").await?;
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
		INSERT INTO user (id) VALUES (user:one), (user:two);
		INSERT INTO business (id, owner) VALUES (business:one, user:one), (business:two, user:two);
	";
	let dbs = new_ds("test", "test").await?.with_auth_enabled(true);
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
		Value::RecordId(RecordId::new("user", "one".to_string())),
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
	let val = Value::Array(Array::new());
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn field_computed_graph_query_permissions() -> Result<()> {
	// Setup: Define tables with permissions and a COMPUTED field that does a graph traversal
	let sql = "
		DEFINE TABLE person SCHEMAFULL
			PERMISSIONS FOR select FULL;
		DEFINE FIELD name ON person TYPE string;
		DEFINE TABLE account SCHEMAFULL
			PERMISSIONS FOR select WHERE id = $auth;
		DEFINE FIELD name ON account TYPE string;
		DEFINE TABLE has_account TYPE RELATION SCHEMAFULL
			PERMISSIONS FOR select FULL;
		DEFINE FIELD person ON account COMPUTED <-has_account[0].in ?? NONE;
		DEFINE ACCESS account_access ON DATABASE TYPE RECORD
			SIGNUP (CREATE account SET name = $name)
			SIGNIN (SELECT * FROM account WHERE name = $name);
	";
	let dbs = new_ds("test", "test").await?.with_auth_enabled(true);
	let owner_ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &owner_ses, None).await?;
	// 7 DEFINE statements
	for _ in 0..7 {
		res.remove(0).result.unwrap();
	}

	// Create data as owner
	let sql = "
		CREATE person:one SET name = 'Alice';
		CREATE account:alpha SET name = 'alpha';
		RELATE person:one->has_account->account:alpha;
	";
	let res = &mut dbs.execute(sql, &owner_ses, None).await?;
	assert_eq!(res.len(), 3);
	for _ in 0..3 {
		res.remove(0).result.unwrap();
	}

	// Query as record-authenticated user (account:alpha)
	let ses = Session::for_record(
		"test",
		"test",
		"account_access",
		Value::RecordId(RecordId::new("account", "alpha".to_string())),
	);
	let sql = "SELECT * FROM account:alpha";
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: account:alpha,
				name: 'alpha',
				person: person:one,
			},
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn field_assert_subquery_permissions() -> Result<()> {
	// Setup: Define tables with an ASSERT that uses a graph sub-query
	let sql = "
		DEFINE TABLE person SCHEMAFULL
			PERMISSIONS FOR select, create FULL;
		DEFINE FIELD name ON person TYPE string;
		DEFINE TABLE account SCHEMAFULL
			PERMISSIONS FOR select, create, update FULL;
		DEFINE FIELD name ON account TYPE string;
		DEFINE TABLE has_account TYPE RELATION SCHEMAFULL
			PERMISSIONS FOR select, create FULL;
		DEFINE FIELD verified ON account TYPE bool
			DEFAULT false
			ASSERT $value = false OR (<-has_account.in IS NOT NONE);
		DEFINE ACCESS account_access ON DATABASE TYPE RECORD
			SIGNUP (CREATE account SET name = $name)
			SIGNIN (SELECT * FROM account WHERE name = $name);
	";
	let dbs = new_ds("test", "test").await?.with_auth_enabled(true);
	let owner_ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &owner_ses, None).await?;
	// 7 DEFINE statements
	for _ in 0..7 {
		res.remove(0).result.unwrap();
	}

	// Create data as owner
	let sql = "
		CREATE person:one SET name = 'Alice';
		CREATE account:alpha SET name = 'alpha';
		RELATE person:one->has_account->account:alpha;
	";
	let res = &mut dbs.execute(sql, &owner_ses, None).await?;
	assert_eq!(res.len(), 3);
	for _ in 0..3 {
		res.remove(0).result.unwrap();
	}

	// Now try to set verified=true as record-authenticated user
	// The ASSERT should pass because <-has_account.in IS NOT NONE
	let ses = Session::for_record(
		"test",
		"test",
		"account_access",
		Value::RecordId(RecordId::new("account", "alpha".to_string())),
	);
	let sql = "UPDATE account:alpha SET verified = true";
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: account:alpha,
				name: 'alpha',
				verified: true,
			},
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn field_assert_subquery_with_where_clause() -> Result<()> {
	// A SELECT subquery with WHERE inside ASSERT returns empty results
	let sql = "
		DEFINE TABLE account SCHEMAFULL
			PERMISSIONS FOR select, create, update FULL;
		DEFINE FIELD name ON account TYPE string;
		DEFINE TABLE email_address SCHEMAFULL
			PERMISSIONS FOR select, create FULL;
		DEFINE FIELD address ON email_address TYPE string;
		DEFINE TABLE has_email TYPE RELATION SCHEMAFULL
			PERMISSIONS FOR select, create FULL;
		DEFINE FIELD verified ON has_email TYPE bool DEFAULT true;
		DEFINE FIELD email ON account TYPE option<record<email_address>>
			ASSERT $value = NONE OR
				$value IN (SELECT VALUE out FROM has_email
					WHERE in = $this.id
						AND out = $value
						AND verified = true
				);
	";
	let dbs = new_ds("test", "test").await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = dbs.execute(sql, &ses, None).await?;
	for r in &res {
		r.result.as_ref().unwrap();
	}

	let sql = "
		CREATE account:alpha SET name = 'alpha';
		CREATE email_address:one SET address = 'alpha@test.com';
		RELATE account:alpha->has_email->email_address:one SET verified = true;
	";
	let res = &mut dbs.execute(sql, &ses, None).await?;
	for _ in 0..3 {
		res.remove(0).result.unwrap();
	}

	// Verify the subquery works outside of ASSERT context
	let sql = "SELECT VALUE out FROM has_email WHERE in = account:alpha AND out = email_address:one AND verified = true";
	let res = &mut dbs.execute(sql, &ses, None).await?;
	let tmp = res.remove(0).result?;
	let val = syn::value("[email_address:one]").unwrap();
	assert_eq!(tmp, val, "Subquery should return results outside ASSERT");

	// The ASSERT subquery should find the has_email relation and pass
	let sql = "UPDATE account:alpha SET email = email_address:one";
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: account:alpha,
				email: email_address:one,
				name: 'alpha',
			},
		]",
	)
	.unwrap();
	assert_eq!(tmp, val, "ASSERT with WHERE clause subquery should pass when relation exists");

	// ASSERT correctly rejects invalid values
	let sql = "CREATE email_address:two SET address = 'other@test.com'";
	let res = &mut dbs.execute(sql, &ses, None).await?;
	res.remove(0).result.unwrap();

	let sql = "UPDATE account:alpha SET email = email_address:two";
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert!(
		res.remove(0).result.is_err(),
		"ASSERT should reject value when no matching relation exists"
	);

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
	let dbs = new_ds("test", "test").await?;
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
		DEFINE FIELD OVERWRITE custom.* ON user TYPE any;
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
