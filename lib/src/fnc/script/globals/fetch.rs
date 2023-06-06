use crate::sql::value::Value;
use js::prelude::Rest;
use js::Result;

#[js::bind(object, public)]
#[quickjs(rename = "fetch")]
#[allow(unused_variables)]
pub fn fetch(args: Rest<Value>) -> Result<Value> {
	Ok(Value::None)
}
