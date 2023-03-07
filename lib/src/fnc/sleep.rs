use crate::err::Error;
use crate::sql::Value;

/// Sleep during the provided duration parameter.
pub async fn sleep((val,): (Value,)) -> Result<Value, Error> {
	tokio::time::sleep(val.as_duration().into()).await;
	Ok(Value::None)
}
