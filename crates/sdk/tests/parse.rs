use surrealdb::sql::Thing;
use surrealdb::sql::Value;
use surrealdb::sql::thing;
use surrealdb::sql::value;

#[allow(dead_code)]
pub trait Parse<T> {
	fn parse(val: &str) -> T;
}

impl Parse<Value> for Value {
	fn parse(val: &str) -> Value {
		value(val).unwrap()
	}
}

impl Parse<Thing> for Thing {
	fn parse(val: &str) -> Thing {
		thing(val).unwrap()
	}
}
