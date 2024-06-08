use crate::err::Error;
use crate::sql::value::Value;

pub fn sanitize((arg,): (String,)) -> Result<Value, Error> {
	Ok(ammonia::clean(&arg).into())
}

#[cfg(test)]
mod tests {
	use crate::sql::Value;

	#[test]
	fn sanitize_html() {
		let value = super::sanitize((String::from("<div>Hello world!</div>"),)).unwrap();
		assert_eq!(value, Value::Strand("<div>Hello world!</div>".into()));

		let value = super::sanitize((String::from("XSS<script>attack</script>"),)).unwrap();
		assert_eq!(value, Value::Strand("XSS".into()));
	}
}
