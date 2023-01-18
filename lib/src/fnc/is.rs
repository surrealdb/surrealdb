use crate::err::Error;
use crate::sql::value::Value;
use chrono::NaiveDateTime;
use once_cell::sync::Lazy;
use regex::Regex;
use semver::Version;
use std::char;
use url::Url;
use uuid::Uuid;

#[rustfmt::skip] static LATITUDE_RE: Lazy<Regex> = Lazy::new(|| Regex::new("^[-+]?([1-8]?\\d(\\.\\d+)?|90(\\.0+)?)$").unwrap());
#[rustfmt::skip] static LONGITUDE_RE: Lazy<Regex> = Lazy::new(|| Regex::new("^[-+]?([1-8]?\\d(\\.\\d+)?|90(\\.0+)?)$").unwrap());

#[inline]
pub fn alphanum((arg,): (String,)) -> Result<Value, Error> {
	Ok(arg.chars().all(char::is_alphanumeric).into())
}

#[inline]
pub fn alpha((arg,): (String,)) -> Result<Value, Error> {
	Ok(arg.chars().all(char::is_alphabetic).into())
}

#[inline]
pub fn ascii((arg,): (String,)) -> Result<Value, Error> {
	Ok(arg.is_ascii().into())
}

#[inline]
pub fn datetime((arg, fmt): (String, String)) -> Result<Value, Error> {
	Ok(NaiveDateTime::parse_from_str(&arg, &fmt).is_ok().into())
}

#[inline]
pub fn domain((arg,): (String,)) -> Result<Value, Error> {
	Ok(addr::parse_domain_name(arg.as_str()).is_ok().into())
}

#[inline]
pub fn email((arg,): (String,)) -> Result<Value, Error> {
	Ok(addr::parse_email_address(arg.as_str()).is_ok().into())
}

#[inline]
pub fn hexadecimal((arg,): (String,)) -> Result<Value, Error> {
	Ok(arg.chars().all(|x| char::is_ascii_hexdigit(&x)).into())
}

#[inline]
pub fn latitude((arg,): (String,)) -> Result<Value, Error> {
	Ok(LATITUDE_RE.is_match(arg.as_str()).into())
}

#[inline]
pub fn longitude((arg,): (String,)) -> Result<Value, Error> {
	Ok(LONGITUDE_RE.is_match(arg.as_str()).into())
}

#[inline]
pub fn numeric((arg,): (String,)) -> Result<Value, Error> {
	Ok(arg.chars().all(char::is_numeric).into())
}

#[inline]
pub fn semver((arg,): (String,)) -> Result<Value, Error> {
	Ok(Version::parse(arg.as_str()).is_ok().into())
}

#[inline]
pub fn url((arg,): (String,)) -> Result<Value, Error> {
	Ok(Url::parse(&arg).is_ok().into())
}

#[inline]
pub fn uuid((arg,): (Value,)) -> Result<Value, Error> {
	Ok(match arg {
		Value::Strand(v) => Uuid::parse_str(v.as_string().as_str()).is_ok(),
		Value::Uuid(_) => true,
		_ => false,
	}
	.into())
}

#[cfg(test)]
mod tests {
	use crate::sql::value::Value;

	#[test]
	fn alphanum() {
		let value = super::alphanum((String::from("abc123"),)).unwrap();
		assert_eq!(value, Value::True);

		let value = super::alphanum((String::from("y%*"),)).unwrap();
		assert_eq!(value, Value::False);
	}

	#[test]
	fn alpha() {
		let value = super::alpha((String::from("abc"),)).unwrap();
		assert_eq!(value, Value::True);

		let value = super::alpha((String::from("1234"),)).unwrap();
		assert_eq!(value, Value::False);
	}

	#[test]
	fn ascii() {
		let value = super::ascii((String::from("abc"),)).unwrap();
		assert_eq!(value, Value::True);

		let value = super::ascii((String::from("中国"),)).unwrap();
		assert_eq!(value, Value::False);
	}

	#[test]
	fn domain() {
		let value = super::domain((String::from("食狮.中国"),)).unwrap();
		assert_eq!(value, Value::True);

		let value = super::domain((String::from("example-.com"),)).unwrap();
		assert_eq!(value, Value::False);
	}

	#[test]
	fn email() {
		let input = (String::from("user@[fd79:cdcb:38cc:9dd:f686:e06d:32f3:c123]"),);
		let value = super::email(input).unwrap();
		assert_eq!(value, Value::True);

		let input = (String::from("john..doe@example.com"),);
		let value = super::email(input).unwrap();
		assert_eq!(value, Value::False);
	}

	#[test]
	fn hexadecimal() {
		let value = super::hexadecimal((String::from("00FF00"),)).unwrap();
		assert_eq!(value, Value::True);

		let value = super::hexadecimal((String::from("SurrealDB"),)).unwrap();
		assert_eq!(value, Value::False);
	}

	#[test]
	fn latitude() {
		let value = super::latitude((String::from("-0.118092"),)).unwrap();
		assert_eq!(value, Value::True);

		let value = super::latitude((String::from("12345"),)).unwrap();
		assert_eq!(value, Value::False);
	}

	#[test]
	fn longitude() {
		let value = super::longitude((String::from("51.509865"),)).unwrap();
		assert_eq!(value, Value::True);

		let value = super::longitude((String::from("12345"),)).unwrap();
		assert_eq!(value, Value::False);
	}

	#[test]
	fn numeric() {
		let value = super::numeric((String::from("12345"),)).unwrap();
		assert_eq!(value, Value::True);

		let value = super::numeric((String::from("abcde"),)).unwrap();
		assert_eq!(value, Value::False);
	}

	#[test]
	fn semver() {
		let value = super::semver((String::from("1.0.0"),)).unwrap();
		assert_eq!(value, Value::True);

		let value = super::semver((String::from("1.0"),)).unwrap();
		assert_eq!(value, Value::False);
	}

	#[test]
	fn uuid() {
		let input = (String::from("123e4567-e89b-12d3-a456-426614174000").into(),);
		let value = super::uuid(input).unwrap();
		assert_eq!(value, Value::True);

		let input = (String::from("foo-bar").into(),);
		let value = super::uuid(input).unwrap();
		assert_eq!(value, Value::False);
	}
}
