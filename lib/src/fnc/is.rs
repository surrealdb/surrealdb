use crate::ctx::Context;
use crate::err::Error;
use crate::sql::value::Value;
use once_cell::sync::Lazy;
use regex::Regex;
use semver::Version;
use std::char;
use uuid::Uuid;

#[rustfmt::skip] static LATITUDE_RE: Lazy<Regex> = Lazy::new(|| Regex::new("^[-+]?([1-8]?\\d(\\.\\d+)?|90(\\.0+)?)$").unwrap());
#[rustfmt::skip] static LONGITUDE_RE: Lazy<Regex> = Lazy::new(|| Regex::new("^[-+]?([1-8]?\\d(\\.\\d+)?|90(\\.0+)?)$").unwrap());

#[inline]
pub fn alphanum(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(args.remove(0).as_string().chars().all(char::is_alphanumeric).into())
}

#[inline]
pub fn alpha(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(args.remove(0).as_string().chars().all(char::is_alphabetic).into())
}

#[inline]
pub fn ascii(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(args.remove(0).as_string().is_ascii().into())
}

#[inline]
pub fn domain(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(addr::parse_domain_name(args.remove(0).as_string().as_str()).is_ok().into())
}

#[inline]
pub fn email(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(addr::parse_email_address(args.remove(0).as_string().as_str()).is_ok().into())
}

#[inline]
pub fn hexadecimal(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(args.remove(0).as_string().chars().all(|x| char::is_ascii_hexdigit(&x)).into())
}

#[inline]
pub fn latitude(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(LATITUDE_RE.is_match(args.remove(0).as_string().as_str()).into())
}

#[inline]
pub fn longitude(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(LONGITUDE_RE.is_match(args.remove(0).as_string().as_str()).into())
}

#[inline]
pub fn numeric(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(args.remove(0).as_string().chars().all(char::is_numeric).into())
}

#[inline]
pub fn semver(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(Version::parse(args.remove(0).as_string().as_str()).is_ok().into())
}

#[inline]
pub fn uuid(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(match args.remove(0) {
		Value::Strand(v) => Uuid::parse_str(v.as_string().as_str()).is_ok().into(),
		Value::Uuid(_) => true.into(),
		_ => false.into(),
	})
}

#[cfg(test)]
mod tests {
	use crate::sql::value::Value;

	#[test]
	fn alphanum() {
		let value = super::alphanum(&Default::default(), vec!["abc123".into()]).unwrap();
		assert_eq!(value, Value::True);

		let value = super::alphanum(&Default::default(), vec!["y%*".into()]).unwrap();
		assert_eq!(value, Value::False);
	}

	#[test]
	fn alpha() {
		let value = super::alpha(&Default::default(), vec!["abc".into()]).unwrap();
		assert_eq!(value, Value::True);

		let value = super::alpha(&Default::default(), vec!["1234".into()]).unwrap();
		assert_eq!(value, Value::False);
	}

	#[test]
	fn ascii() {
		let value = super::ascii(&Default::default(), vec!["abc".into()]).unwrap();
		assert_eq!(value, Value::True);

		let value = super::ascii(&Default::default(), vec!["中国".into()]).unwrap();
		assert_eq!(value, Value::False);
	}

	#[test]
	fn domain() {
		let value = super::domain(&Default::default(), vec!["食狮.中国".into()]).unwrap();
		assert_eq!(value, Value::True);

		let value = super::domain(&Default::default(), vec!["example-.com".into()]).unwrap();
		assert_eq!(value, Value::False);
	}

	#[test]
	fn email() {
		let input = vec!["user@[fd79:cdcb:38cc:9dd:f686:e06d:32f3:c123]".into()];
		let value = super::email(&Default::default(), input).unwrap();
		assert_eq!(value, Value::True);

		let input = vec!["john..doe@example.com".into()];
		let value = super::email(&Default::default(), input).unwrap();
		assert_eq!(value, Value::False);
	}

	#[test]
	fn hexadecimal() {
		let value = super::hexadecimal(&Default::default(), vec!["00FF00".into()]).unwrap();
		assert_eq!(value, Value::True);

		let value = super::hexadecimal(&Default::default(), vec!["SurrealDB".into()]).unwrap();
		assert_eq!(value, Value::False);
	}

	#[test]
	fn latitude() {
		let value = super::latitude(&Default::default(), vec!["-0.118092".into()]).unwrap();
		assert_eq!(value, Value::True);

		let value = super::latitude(&Default::default(), vec![12345.into()]).unwrap();
		assert_eq!(value, Value::False);
	}

	#[test]
	fn longitude() {
		let value = super::longitude(&Default::default(), vec!["51.509865".into()]).unwrap();
		assert_eq!(value, Value::True);

		let value = super::longitude(&Default::default(), vec![12345.into()]).unwrap();
		assert_eq!(value, Value::False);
	}

	#[test]
	fn numeric() {
		let value = super::numeric(&Default::default(), vec![12345.into()]).unwrap();
		assert_eq!(value, Value::True);

		let value = super::numeric(&Default::default(), vec!["abcde".into()]).unwrap();
		assert_eq!(value, Value::False);
	}

	#[test]
	fn semver() {
		let value = super::semver(&Default::default(), vec!["1.0.0".into()]).unwrap();
		assert_eq!(value, Value::True);

		let value = super::semver(&Default::default(), vec!["1.0".into()]).unwrap();
		assert_eq!(value, Value::False);
	}

	#[test]
	fn uuid() {
		let input = vec!["123e4567-e89b-12d3-a456-426614174000".into()];
		let value = super::uuid(&Default::default(), input).unwrap();
		assert_eq!(value, Value::True);

		let input = vec!["foo-bar".into()];
		let value = super::uuid(&Default::default(), input).unwrap();
		assert_eq!(value, Value::False);
	}
}
