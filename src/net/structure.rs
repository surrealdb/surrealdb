use std::collections::BTreeMap;

use warp::Filter;

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
		let fields = fields.unwrap();

		// Create a temp array for storing all field objects of this table
		let mut fields_collect: Vec<Object> = Vec::new();
		fields.iter().for_each(|field| {
			let field_type = field.kind.clone().unwrap();
			// Add an object of {"name":"x", "type":"string"} or {"name":"x", "type":{"type": "record", "name": "table_name"}}
			fields_collect.push(Object::from(map! {
				"name".to_string() => Value::from(field.name.clone().to_string()),
				"type".to_string() => match field_type {
					Kind::Record(record) => Value::from(map!{
						"type".to_string() => Value::from("record"),
						"table".to_string() => Value::from(record.iter().map(|ref v| v.to_string()).collect::<Vec<_>>().join(", "))
					}),
					_ => Value::from(field_type.to_string())
				}
			}));
		});
		// Insert the table name => fields array into the table_definitions map
		table_definitions.insert(table.name.to_string(), fields_collect);
	}

	// Send response
	Ok(output::json(&table_definitions))
}
