use crate::err::Error;
use crate::fnc::util::string;
use crate::sql::value::Value;

/// Returns `true` if a string of this length is too much to allocate.
fn limit(name: &str, n: usize) -> Result<(), Error> {
	const LIMIT: usize = 2usize.pow(20);
	if n > LIMIT {
		Err(Error::InvalidArguments {
			name: name.to_owned(),
			message: format!("Output must not exceed {LIMIT} bytes."),
		})
	} else {
		Ok(())
	}
}

pub fn concat(args: Vec<Value>) -> Result<Value, Error> {
	let strings = args.into_iter().map(Value::as_string).collect::<Vec<_>>();
	limit("string::concat", strings.iter().map(String::len).sum::<usize>())?;
	Ok(strings.concat().into())
}

pub fn contains((val, check): (String, String)) -> Result<Value, Error> {
	Ok(val.contains(&check).into())
}

pub fn ends_with((val, chr): (String, String)) -> Result<Value, Error> {
	Ok(val.ends_with(&chr).into())
}

pub fn join(args: Vec<Value>) -> Result<Value, Error> {
	let mut args = args.into_iter().map(Value::as_string);
	let chr = args.next().ok_or_else(|| Error::InvalidArguments {
		name: String::from("string::join"),
		message: String::from("Expected at least one argument"),
	})?;

	let strings = args.collect::<Vec<_>>();
	limit(
		"string::join",
		strings
			.len()
			.saturating_mul(chr.len())
			.saturating_add(strings.iter().map(String::len).sum::<usize>()),
	)?;

	// FIXME: Use intersperse to avoid intermediate allocation once stable
	// https://github.com/rust-lang/rust/issues/79524
	Ok(strings.join(&chr).into())
}

pub fn len((string,): (String,)) -> Result<Value, Error> {
	let num = string.chars().count() as i64;
	Ok(num.into())
}

pub fn lowercase((string,): (String,)) -> Result<Value, Error> {
	Ok(string.to_lowercase().into())
}

pub fn repeat((val, num): (String, usize)) -> Result<Value, Error> {
	limit("string::repeat", val.len().saturating_mul(num))?;
	Ok(val.repeat(num).into())
}

pub fn replace((val, old, new): (String, String, String)) -> Result<Value, Error> {
	if new.len() > old.len() {
		let increase = new.len() - old.len();
		limit(
			"string::replace",
			val.len().saturating_add(val.matches(&old).count().saturating_mul(increase)),
		)?;
	}
	Ok(val.replace(&old, &new).into())
}

pub fn reverse((string,): (String,)) -> Result<Value, Error> {
	Ok(string.chars().rev().collect::<String>().into())
}

pub fn slice((val, beg, lim): (String, Option<isize>, Option<isize>)) -> Result<Value, Error> {
	// Only count the chars if we need to and only do it once.
	let mut char_count = usize::MAX;
	let mut count_chars = || {
		if char_count == usize::MAX {
			char_count = val.chars().count();
		}
		char_count
	};

	let skip = match beg {
		Some(v) if v < 0 => count_chars().saturating_sub(v.unsigned_abs()),
		Some(v) => v as usize,
		None => 0,
	};

	let take = match lim {
		Some(v) if v < 0 => count_chars().saturating_sub(skip).saturating_sub(v.unsigned_abs()),
		Some(v) => v as usize,
		None => usize::MAX,
	};

	Ok(if skip > 0 || take < usize::MAX {
		val.chars().skip(skip).take(take).collect::<String>()
	} else {
		val
	}
	.into())
}

pub fn slug((string,): (String,)) -> Result<Value, Error> {
	Ok(string::slug::slug(string).into())
}

pub fn split((val, chr): (String, String)) -> Result<Value, Error> {
	Ok(val.split(&chr).collect::<Vec<&str>>().into())
}

pub fn starts_with((val, chr): (String, String)) -> Result<Value, Error> {
	Ok(val.starts_with(&chr).into())
}

pub fn trim((string,): (String,)) -> Result<Value, Error> {
	Ok(string.trim().into())
}

pub fn uppercase((string,): (String,)) -> Result<Value, Error> {
	Ok(string.to_uppercase().into())
}

pub fn words((string,): (String,)) -> Result<Value, Error> {
	Ok(string.split_whitespace().collect::<Vec<&str>>().into())
}

pub mod distance {

	use crate::err::Error;
	use crate::sql::Value;

	pub fn hamming((_, _): (String, String)) -> Result<Value, Error> {
		Err(Error::FeatureNotYetImplemented {
			feature: "string::distance::hamming() function".to_string(),
		})
	}

	pub fn levenshtein((_, _): (String, String)) -> Result<Value, Error> {
		Err(Error::FeatureNotYetImplemented {
			feature: "string::distance::levenshtein() function".to_string(),
		})
	}
}

pub mod is {
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

	pub fn alphanum((arg,): (String,)) -> Result<Value, Error> {
		Ok(arg.chars().all(char::is_alphanumeric).into())
	}

	pub fn alpha((arg,): (String,)) -> Result<Value, Error> {
		Ok(arg.chars().all(char::is_alphabetic).into())
	}

	pub fn ascii((arg,): (String,)) -> Result<Value, Error> {
		Ok(arg.is_ascii().into())
	}

	pub fn datetime((arg, fmt): (String, String)) -> Result<Value, Error> {
		Ok(NaiveDateTime::parse_from_str(&arg, &fmt).is_ok().into())
	}
	pub fn domain((arg,): (String,)) -> Result<Value, Error> {
		Ok(addr::parse_domain_name(arg.as_str()).is_ok().into())
	}

	pub fn email((arg,): (String,)) -> Result<Value, Error> {
		Ok(addr::parse_email_address(arg.as_str()).is_ok().into())
	}

	pub fn hexadecimal((arg,): (String,)) -> Result<Value, Error> {
		Ok(arg.chars().all(|x| char::is_ascii_hexdigit(&x)).into())
	}

	pub fn latitude((arg,): (String,)) -> Result<Value, Error> {
		Ok(LATITUDE_RE.is_match(arg.as_str()).into())
	}

	pub fn longitude((arg,): (String,)) -> Result<Value, Error> {
		Ok(LONGITUDE_RE.is_match(arg.as_str()).into())
	}

	pub fn numeric((arg,): (String,)) -> Result<Value, Error> {
		Ok(arg.chars().all(char::is_numeric).into())
	}

	pub fn semver((arg,): (String,)) -> Result<Value, Error> {
		Ok(Version::parse(arg.as_str()).is_ok().into())
	}

	pub fn url((arg,): (String,)) -> Result<Value, Error> {
		Ok(Url::parse(&arg).is_ok().into())
	}

	pub fn uuid((arg,): (Value,)) -> Result<Value, Error> {
		Ok(match arg {
			Value::Strand(v) => Uuid::parse_str(v.as_string().as_str()).is_ok(),
			Value::Uuid(_) => true,
			_ => false,
		}
		.into())
	}
}

pub mod similarity {

	use crate::err::Error;
	use crate::fnc::util::string::fuzzy::Fuzzy;
	use crate::sql::Value;

	pub fn fuzzy((a, b): (String, String)) -> Result<Value, Error> {
		Ok(a.as_str().fuzzy_score(b.as_str()).into())
	}

	pub fn jaro((_, _): (String, String)) -> Result<Value, Error> {
		Err(Error::FeatureNotYetImplemented {
			feature: "string::similarity::jaro() function".to_string(),
		})
	}

	pub fn smithwaterman((a, b): (String, String)) -> Result<Value, Error> {
		Ok(a.as_str().fuzzy_score(b.as_str()).into())
	}
}

#[cfg(test)]
mod tests {
	use super::{contains, slice};
	use crate::sql::Value;

	#[test]
	fn string_slice() {
		fn test(initial: &str, beg: Option<isize>, end: Option<isize>, expected: &str) {
			assert_eq!(slice((initial.to_owned(), beg, end)).unwrap(), Value::from(expected));
		}

		let string = "abcdefg";
		test(string, None, None, string);
		test(string, Some(2), None, &string[2..]);
		test(string, Some(2), Some(3), &string[2..5]);
		test(string, Some(2), Some(-1), "cdef");
		test(string, Some(-2), None, "fg");
		test(string, Some(-4), Some(2), "de");
		test(string, Some(-4), Some(-1), "def");

		let string = "你好世界";
		test(string, None, None, string);
		test(string, Some(1), None, "好世界");
		test(string, Some(-1), None, "界");
		test(string, Some(-2), Some(1), "世");
	}

	#[test]
	fn string_contains() {
		fn test(base: &str, contained: &str, expected: bool) {
			assert_eq!(
				contains((base.to_string(), contained.to_string())).unwrap(),
				Value::from(expected)
			);
		}

		test("", "", true);
		test("", "a", false);
		test("a", "", true);
		test("abcde", "bcd", true);
		test("abcde", "cbcd", false);
		test("好世界", "世", true);
		test("好世界", "你好", false);
	}

	#[test]
	fn is_alphanum() {
		let value = super::is::alphanum((String::from("abc123"),)).unwrap();
		assert_eq!(value, Value::Bool(true));

		let value = super::is::alphanum((String::from("y%*"),)).unwrap();
		assert_eq!(value, Value::Bool(false));
	}

	#[test]
	fn is_alpha() {
		let value = super::is::alpha((String::from("abc"),)).unwrap();
		assert_eq!(value, Value::Bool(true));

		let value = super::is::alpha((String::from("1234"),)).unwrap();
		assert_eq!(value, Value::Bool(false));
	}

	#[test]
	fn is_ascii() {
		let value = super::is::ascii((String::from("abc"),)).unwrap();
		assert_eq!(value, Value::Bool(true));

		let value = super::is::ascii((String::from("中国"),)).unwrap();
		assert_eq!(value, Value::Bool(false));
	}

	#[test]
	fn is_domain() {
		let value = super::is::domain((String::from("食狮.中国"),)).unwrap();
		assert_eq!(value, Value::Bool(true));

		let value = super::is::domain((String::from("example-.com"),)).unwrap();
		assert_eq!(value, Value::Bool(false));
	}

	#[test]
	fn is_email() {
		let input = (String::from("user@[fd79:cdcb:38cc:9dd:f686:e06d:32f3:c123]"),);
		let value = super::is::email(input).unwrap();
		assert_eq!(value, Value::Bool(true));

		let input = (String::from("john..doe@example.com"),);
		let value = super::is::email(input).unwrap();
		assert_eq!(value, Value::Bool(false));
	}

	#[test]
	fn is_hexadecimal() {
		let value = super::is::hexadecimal((String::from("00FF00"),)).unwrap();
		assert_eq!(value, Value::Bool(true));

		let value = super::is::hexadecimal((String::from("SurrealDB"),)).unwrap();
		assert_eq!(value, Value::Bool(false));
	}

	#[test]
	fn is_latitude() {
		let value = super::is::latitude((String::from("-0.118092"),)).unwrap();
		assert_eq!(value, Value::Bool(true));

		let value = super::is::latitude((String::from("12345"),)).unwrap();
		assert_eq!(value, Value::Bool(false));
	}

	#[test]
	fn is_longitude() {
		let value = super::is::longitude((String::from("51.509865"),)).unwrap();
		assert_eq!(value, Value::Bool(true));

		let value = super::is::longitude((String::from("12345"),)).unwrap();
		assert_eq!(value, Value::Bool(false));
	}

	#[test]
	fn is_numeric() {
		let value = super::is::numeric((String::from("12345"),)).unwrap();
		assert_eq!(value, Value::Bool(true));

		let value = super::is::numeric((String::from("abcde"),)).unwrap();
		assert_eq!(value, Value::Bool(false));
	}

	#[test]
	fn is_semver() {
		let value = super::is::semver((String::from("1.0.0"),)).unwrap();
		assert_eq!(value, Value::Bool(true));

		let value = super::is::semver((String::from("1.0"),)).unwrap();
		assert_eq!(value, Value::Bool(false));
	}

	#[test]
	fn is_uuid() {
		let input = (String::from("123e4567-e89b-12d3-a456-426614174000").into(),);
		let value = super::is::uuid(input).unwrap();
		assert_eq!(value, Value::Bool(true));

		let input = (String::from("foo-bar").into(),);
		let value = super::is::uuid(input).unwrap();
		assert_eq!(value, Value::Bool(false));
	}
}
