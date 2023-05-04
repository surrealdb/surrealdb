use crate::err::Error;
use crate::fnc::util::string;
use crate::sql::value::Value;

pub fn concat(args: Vec<Value>) -> Result<Value, Error> {
	Ok(args.into_iter().map(|x| x.as_string()).collect::<Vec<_>>().concat().into())
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
	// FIXME: Use intersperse to avoid intermediate allocation once stable
	// https://github.com/rust-lang/rust/issues/79524
	let val = args.collect::<Vec<_>>().join(&chr);
	Ok(val.into())
}

pub fn len((string,): (String,)) -> Result<Value, Error> {
	let num = string.chars().count() as i64;
	Ok(num.into())
}

pub fn lowercase((string,): (String,)) -> Result<Value, Error> {
	Ok(string.to_lowercase().into())
}

pub fn repeat((val, num): (String, usize)) -> Result<Value, Error> {
	const LIMIT: usize = 2usize.pow(20);
	if val.len().saturating_mul(num) > LIMIT {
		Err(Error::InvalidArguments {
			name: String::from("string::repeat"),
			message: format!("Output must not exceed {LIMIT} bytes."),
		})
	} else {
		Ok(val.repeat(num).into())
	}
}

pub fn replace((val, old, new): (String, String, String)) -> Result<Value, Error> {
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
	Ok(string::slug(string).into())
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
}
