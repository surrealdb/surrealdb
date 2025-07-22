use surrealdb::expr::Value;
use surrealdb::sql::{SqlValue, Thing, thing, value};

#[allow(dead_code)]
pub trait Parse<T> {
	fn parse(val: &str) -> T;
}

impl Parse<Value> for Value {
	fn parse(val: &str) -> Value {
		SqlValue::parse(val).into()
	}
}

impl Parse<SqlValue> for SqlValue {
	fn parse(val: &str) -> SqlValue {
		value(val).unwrap()
	}
}

impl Parse<Thing> for Thing {
	fn parse(val: &str) -> Thing {
		thing(val).unwrap()
	}
}
