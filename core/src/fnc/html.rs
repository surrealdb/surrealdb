use crate::err::Error;
use crate::sql::value::Value;

pub fn encode((arg,): (String,)) -> Result<Value, Error> {
	Ok(ammonia::clean_text(&arg).to_string().into())
}

pub fn sanitize((arg,): (String,)) -> Result<Value, Error> {
	Ok(ammonia::clean(&arg).into())
}

#[cfg(test)]
mod tests {
	use crate::sql::Value;

	#[test]
	fn encode_html() {
		let value = super::encode((String::from("<div>Hello world!</div>"),)).unwrap();
		assert_eq!(value, Value::Strand("&lt;div&gt;Hello&#32;world!&lt;&#47;div&gt;".into()));

		let value = super::encode((String::from("SurrealDB"),)).unwrap();
		assert_eq!(value, Value::Strand("SurrealDB".into()));
	}

	#[test]
	fn sanitize_html() {
		let value = super::sanitize((String::from("<div>Hello world!</div>"),)).unwrap();
		assert_eq!(value, Value::Strand("<div>Hello world!</div>".into()));

		let value = super::sanitize((String::from("XSS<script>attack</script>"),)).unwrap();
		assert_eq!(value, Value::Strand("XSS".into()));
	}
}
