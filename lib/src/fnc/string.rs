use crate::err::Error;
use crate::fnc::util::string;
use crate::sql::value::Value;

pub fn concat(args: Vec<Value>) -> Result<Value, Error> {
	Ok(args.into_iter().map(|x| x.as_string()).collect::<Vec<_>>().concat().into())
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

pub fn length((string,): (String,)) -> Result<Value, Error> {
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
	let val = match beg {
		Some(v) if v < 0 => {
			val.chars().skip(val.len().saturating_sub(v.unsigned_abs())).collect::<String>()
		}
		Some(v) => val.chars().skip(v as usize).collect::<String>(),
		None => val,
	};
	let val = match lim {
		Some(v) if v < 0 => {
			val.chars().take(val.len().saturating_sub(v.unsigned_abs())).collect::<String>()
		}
		Some(v) => val.chars().take(v as usize).collect::<String>(),
		None => val,
	};
	Ok(val.into())
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
