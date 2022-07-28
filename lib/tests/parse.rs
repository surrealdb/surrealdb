use surrealdb::sql::json;
use surrealdb::sql::thing;
use surrealdb::sql::Thing;
use surrealdb::sql::Value;

pub trait Parse<T> {
	fn parse(val: &str) -> T;
}

impl Parse<Value> for Value {
	fn parse(val: &str) -> Value {
		json(val).unwrap()
	}
}

impl Parse<Thing> for Thing {
	fn parse(val: &str) -> Thing {
		thing(val).unwrap()
	}
}
