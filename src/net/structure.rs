use std::collections::BTreeMap;
use std::sync::Arc;

use warp::Filter;

use surrealdb::sql::statements::DefineFieldStatement;
use surrealdb::sql::{Kind, Object, Value};
use surrealdb::Session;

use crate::dbs::DB;
use crate::err::Error;
use crate::net::output;
use crate::net::session;

pub fn config() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
	// Set base path
	let base = warp::path("structure").and(warp::path::end());
	// Set opts method
	let opts = base.and(warp::options()).map(warp::reply);
	// Set post method
	let post = base.and(warp::post()).and(session::build()).and_then(handler);
	// Specify route
	opts.or(post)
}

async fn handler(session: Session) -> Result<impl warp::Reply, warp::Rejection> {
	// Check the permissions
	if !session.au.is_kv() {
		return Err(warp::reject::custom(Error::InvalidAuth));
	}
	// Get a database reference
	let db = DB.get().unwrap();
	// Extract the Namespace header value
	let nsv = match session.ns {
		Some(ns) => ns,
		None => return Err(warp::reject::custom(Error::NoNsHeader)),
	};
	// Extract the DB header value
	let dbv = match session.db {
		Some(db) => db,
		None => return Err(warp::reject::custom(Error::NoDbHeader)),
	};
	// Create a transaction
	let mut txn = match db.transaction(false, false).await {
		Ok(txn) => txn,
		Err(e) => return Err(warp::reject::custom(Error::Db(e))),
	};
	// Get all table definitions
	let tables = match txn.all_tb(&nsv, &dbv).await {
		Ok(tables) => tables,
		Err(e) => return Err(warp::reject::custom(Error::Db(e))),
	};
	// Temp storage for structures that will be output
	let mut table_definitions = BTreeMap::new();
	for table in tables.iter() {
		// Get all fields of this table
		let fields = txn.all_fd(&nsv, &dbv, &table.name).await;
		if fields.is_err() {
			continue;
		}
		// Create an array of the fields, this will change the output for array type fields
		let processed_fields = get_formatted_fields(fields.unwrap());
		// Insert the table name => fields array into the table_definitions map
		table_definitions.insert(table.name.to_string(), processed_fields);
	}

	// Send response
	Ok(output::json(&table_definitions))
}

// I feel this could be done tidier... but i don't know how and it works...
// Idea is:
// Array fields return as two separate fields, for ex:
// {name: "field_name", type: "array"}
// {name: "field_name[*]", type: "record(thing)"}
// We want to take field_name, and field_name[*] and combine them into one field, using a "sub_type" instead.
fn get_formatted_fields(fields: Arc<[DefineFieldStatement]>) -> Vec<Object> {
	let mut fields_collect: BTreeMap<String, Object> = BTreeMap::new();

	for field in fields.iter() {
		// The type of our "sub_type" value if applicable, otherwise null
		let mut sub_field_type: Value = Value::Null;
		// The type of our field
		let field_type = field.kind.as_ref().unwrap();

		// If our field is an array, and we've already processed the original field def, we can skip it.
		if field.name.to_string().contains("[*]")
			&& fields_collect.contains_key(&field.name.to_string().replace("[*]", ""))
		{
			continue;
		}

		// If the field type is an array, look through our fields and find it's other field definition to get the underlying type
		let out_field_type = match field_type {
			Kind::Array => {
				match fields.iter().find(|f| f.name.to_string() == field.name.to_string() + "[*]") {
					None => Value::from(field_type.to_string()),
					Some(arr_field_type) => {
						// Set the sub type of this array
						sub_field_type =
							Value::from(arr_field_type.kind.as_ref().unwrap().to_string());
						Value::from(field_type.to_string())
					}
				}
			}
			_ => Value::from(field_type.to_string()),
		};

		// Finally insert it...
		fields_collect.insert(
			field.name.to_string(),
			Object::from(map! {
				"name".to_string() => Value::from(field.name.to_string()),
				"type".to_string() => out_field_type,
				"sub_type".to_string() => sub_field_type
			}),
		);
	}

	// Return it as an array for the response
	fields_collect.values().cloned().collect()
}
