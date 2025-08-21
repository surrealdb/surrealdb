use anyhow::{Result, ensure};

use super::args::{Any, Cast, Optional};
use crate::cnf::GENERATION_ALLOCATION_LIMIT;
use crate::err::Error;
use crate::fnc::util::string;
use crate::val::{Regex, Strand, Value};

/// Returns `true` if a string of this length is too much to allocate.
fn limit(name: &str, n: usize) -> Result<()> {
	ensure!(
		n <= *GENERATION_ALLOCATION_LIMIT,
		Error::InvalidArguments {
			name: name.to_owned(),
			message: format!("Output must not exceed {} bytes.", *GENERATION_ALLOCATION_LIMIT),
		}
	);
	Ok(())
}

pub fn concat(Any(args): Any) -> Result<Value> {
	let strings = args.into_iter().map(Value::as_raw_string).collect::<Vec<_>>();
	limit("string::concat", strings.iter().map(String::len).sum::<usize>())?;
	Ok(strings.concat().into())
}

pub fn contains((val, check): (String, String)) -> Result<Value> {
	Ok(val.contains(&check).into())
}

pub fn ends_with((val, chr): (String, String)) -> Result<Value> {
	Ok(val.ends_with(&chr).into())
}

pub fn join(Any(args): Any) -> Result<Value> {
	let mut args = args.into_iter().map(Value::as_raw_string);
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

pub fn len((string,): (String,)) -> Result<Value> {
	let num = string.chars().count() as i64;
	Ok(num.into())
}

pub fn lowercase((string,): (String,)) -> Result<Value> {
	Ok(string.to_lowercase().into())
}

pub fn repeat((val, num): (String, i64)) -> Result<Value> {
	//TODO: Deal with truncation of neg:
	let num = num as usize;
	limit("string::repeat", val.len().saturating_mul(num))?;
	Ok(val.repeat(num).into())
}

pub fn matches((val, Cast(regex)): (String, Cast<Regex>)) -> Result<Value> {
	Ok(regex.0.is_match(&val).into())
}

pub fn replace((val, search, replace): (String, Value, String)) -> Result<Value> {
	match search {
		Value::Strand(search) => {
			if replace.len() > search.len() {
				let increase = replace.len() - search.len();
				limit(
					"string::replace",
					val.len().saturating_add(
						val.matches(search.as_str()).count().saturating_mul(increase),
					),
				)?;
			}
			Ok(val.replace(search.as_str(), &replace).into())
		}
		Value::Regex(search) => {
			let mut new_val = String::with_capacity(val.len());
			let mut last = 0;

			for m in search.0.find_iter(&val) {
				// Push everything until the match
				new_val.push_str(&val[last..m.start()]);

				// Push replacement
				new_val.push_str(&replace);

				// Abort early if we'd exceed the allowed limit
				limit("string::replace", new_val.len())?;

				last = m.end();
			}

			// Finally, push anything after the last match
			new_val.push_str(&val[last..]);
			limit("string::replace", new_val.len())?;
			Ok(new_val.into())
		}
		_ => Err(anyhow::Error::new(Error::InvalidArguments {
			name: "string::replace".to_string(),
			message: format!(
				"Argument 2 was the wrong type. Expected a string but found {}",
				search
			),
		})),
	}
}
pub fn reverse((string,): (String,)) -> Result<Value> {
	Ok(string.chars().rev().collect::<String>().into())
}

pub fn slice(
	(val, Optional(beg), Optional(lim)): (String, Optional<i64>, Optional<i64>),
) -> Result<Value> {
	// Only count the chars if we need to and only do it once.
	let mut str_len_cache = None;
	let mut str_len = || *str_len_cache.get_or_insert_with(|| val.chars().count() as i64);

	let beg = if let Some(v) = beg {
		if v < 0 {
			str_len().saturating_add(v).max(0) as usize
		} else {
			v as usize
		}
	} else {
		0
	};

	let take = match lim {
		Some(v) => {
			if v < 0 {
				let len = str_len().saturating_add(v).max(0) as usize;
				len.saturating_sub(beg)
			} else {
				v as usize
			}
		}
		None => usize::MAX,
	};

	if take == 0 {
		return Ok(String::new().into());
	}

	Ok(val.chars().skip(beg).take(take).collect::<String>().into())
}

pub fn slug((string,): (String,)) -> Result<Value> {
	Ok(string::slug::slug(string).into())
}

pub fn split((val, chr): (String, String)) -> Result<Value> {
	// TODO: Null byte validity
	Ok(val
		.split(&chr)
		.map(|x| Value::from(Strand::new(x.to_owned()).unwrap()))
		.collect::<Vec<_>>()
		.into())
}

pub fn starts_with((val, chr): (String, String)) -> Result<Value> {
	Ok(val.starts_with(&chr).into())
}

pub fn trim((string,): (String,)) -> Result<Value> {
	Ok(string.trim().into())
}

pub fn uppercase((string,): (String,)) -> Result<Value> {
	Ok(string.to_uppercase().into())
}

pub fn words((string,): (String,)) -> Result<Value> {
	Ok(string
		.split_whitespace()
		.map(|v| Value::from(Strand::new(v.to_owned()).unwrap()))
		.collect::<Vec<_>>()
		.into())
}

pub mod distance {

	use anyhow::Result;
	use strsim;

	use crate::err::Error;
	use crate::val::Value;

	/// Calculate the Damerau-Levenshtein distance between two strings
	/// via [`strsim::damerau_levenshtein`].
	pub fn damerau_levenshtein((a, b): (String, String)) -> Result<Value> {
		Ok(strsim::damerau_levenshtein(&a, &b).into())
	}

	/// Calculate the normalized Damerau-Levenshtein distance between two
	/// strings via [`strsim::normalized_damerau_levenshtein`].
	pub fn normalized_damerau_levenshtein((a, b): (String, String)) -> Result<Value> {
		Ok(strsim::normalized_damerau_levenshtein(&a, &b).into())
	}

	/// Calculate the Hamming distance between two strings
	/// via [`strsim::hamming`].
	///
	/// Will result in an [`Error::InvalidArguments`] if the given strings are
	/// of different lengths.
	pub fn hamming((a, b): (String, String)) -> Result<Value> {
		match strsim::hamming(&a, &b) {
			Ok(v) => Ok(v.into()),
			Err(_) => Err(anyhow::Error::new(Error::InvalidArguments {
				name: "string::distance::hamming".into(),
				message: "Strings must be of equal length.".into(),
			})),
		}
	}

	/// Calculate the Levenshtein distance between two strings
	/// via [`strsim::levenshtein`].
	pub fn levenshtein((a, b): (String, String)) -> Result<Value> {
		Ok(strsim::levenshtein(&a, &b).into())
	}

	/// Calculate the normalized Levenshtein distance between two strings
	/// via [`strsim::normalized_levenshtein`].
	pub fn normalized_levenshtein((a, b): (String, String)) -> Result<Value> {
		Ok(strsim::normalized_levenshtein(&a, &b).into())
	}

	/// Calculate the OSA distance &ndash; a variant of the Levenshtein distance
	/// that allows for transposition of adjacent characters &ndash; between two
	/// strings via [`strsim::osa_distance`].
	pub fn osa_distance((a, b): (String, String)) -> Result<Value> {
		Ok(strsim::osa_distance(&a, &b).into())
	}
}

pub mod html {
	use anyhow::Result;

	use crate::val::Value;

	pub fn encode((arg,): (String,)) -> Result<Value> {
		Ok(ammonia::clean_text(&arg).into())
	}

	pub fn sanitize((arg,): (String,)) -> Result<Value> {
		Ok(ammonia::clean(&arg).into())
	}
}

pub mod is {
	use std::char;
	use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
	use std::sync::LazyLock;

	use anyhow::{Result, bail};
	use chrono::NaiveDateTime;
	use regex::Regex;
	use semver::Version;
	use ulid::Ulid;
	use url::Url;
	use uuid::Uuid;

	use crate::err::Error;
	use crate::fnc::args::Optional;
	use crate::syn;
	use crate::val::{Datetime, Value};

	#[rustfmt::skip] static LATITUDE_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new("^[-+]?([1-8]?\\d(\\.\\d+)?|90(\\.0+)?)$").unwrap());
	#[rustfmt::skip] static LONGITUDE_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new("^[-+]?(180(\\.0+)?|((1[0-7]\\d)|([1-9]?\\d))(\\.\\d+)?)$").unwrap());

	pub fn alphanum((arg,): (String,)) -> Result<Value> {
		if arg.is_empty() {
			Ok(Value::Bool(false))
		} else {
			Ok(arg.chars().all(char::is_alphanumeric).into())
		}
	}

	pub fn alpha((arg,): (String,)) -> Result<Value> {
		if arg.is_empty() {
			Ok(Value::Bool(false))
		} else {
			Ok(arg.chars().all(char::is_alphabetic).into())
		}
	}

	pub fn ascii((arg,): (String,)) -> Result<Value> {
		if arg.is_empty() {
			Ok(Value::Bool(false))
		} else {
			Ok(arg.is_ascii().into())
		}
	}

	pub fn datetime((arg, Optional(fmt)): (String, Optional<String>)) -> Result<Value> {
		Ok(match fmt {
			Some(fmt) => NaiveDateTime::parse_from_str(&arg, &fmt).is_ok().into(),
			None => Datetime::try_from(arg.as_ref()).is_ok().into(),
		})
	}

	pub fn domain((arg,): (String,)) -> Result<Value> {
		Ok(addr::parse_domain_name(arg.as_str()).is_ok().into())
	}

	pub fn email((arg,): (String,)) -> Result<Value> {
		Ok(addr::parse_email_address(arg.as_str()).is_ok().into())
	}

	pub fn hexadecimal((arg,): (String,)) -> Result<Value> {
		if arg.is_empty() {
			Ok(Value::Bool(false))
		} else {
			Ok(arg.chars().all(|x| char::is_ascii_hexdigit(&x)).into())
		}
	}

	pub fn ip((arg,): (String,)) -> Result<Value> {
		Ok(arg.parse::<IpAddr>().is_ok().into())
	}

	pub fn ipv4((arg,): (String,)) -> Result<Value> {
		Ok(arg.parse::<Ipv4Addr>().is_ok().into())
	}

	pub fn ipv6((arg,): (String,)) -> Result<Value> {
		Ok(arg.parse::<Ipv6Addr>().is_ok().into())
	}

	pub fn latitude((arg,): (String,)) -> Result<Value> {
		Ok(LATITUDE_RE.is_match(arg.as_str()).into())
	}

	pub fn longitude((arg,): (String,)) -> Result<Value> {
		Ok(LONGITUDE_RE.is_match(arg.as_str()).into())
	}

	pub fn numeric((arg,): (String,)) -> Result<Value> {
		if arg.is_empty() {
			Ok(Value::Bool(false))
		} else {
			Ok(arg.chars().all(char::is_numeric).into())
		}
	}

	pub fn semver((arg,): (String,)) -> Result<Value> {
		Ok(Version::parse(arg.as_str()).is_ok().into())
	}

	pub fn url((arg,): (String,)) -> Result<Value> {
		Ok(Url::parse(&arg).is_ok().into())
	}

	pub fn uuid((arg,): (String,)) -> Result<Value> {
		Ok(Uuid::parse_str(arg.as_ref()).is_ok().into())
	}

	pub fn ulid((arg,): (String,)) -> Result<Value> {
		Ok(Ulid::from_string(arg.as_ref()).is_ok().into())
	}

	pub fn record((arg, Optional(tb)): (String, Optional<Value>)) -> Result<Value> {
		let res = match syn::record_id(&arg) {
			Ok(t) => match tb {
				Some(Value::Strand(tb)) => t.table.as_str() == tb.as_str(),
				Some(Value::Table(tb)) => t.table.as_str() == tb.as_str(),
				Some(_) => {
					bail!(Error::InvalidArguments {
						name: "string::is::record()".into(),
						message:
							"Expected an optional string or table type for the second argument"
								.into(),
					})
				}
				None => true,
			},
			_ => false,
		};

		Ok(res.into())
	}
}

pub mod similarity {
	use std::sync::LazyLock;

	use anyhow::Result;
	use fuzzy_matcher::FuzzyMatcher;
	use fuzzy_matcher::skim::SkimMatcherV2;

	use crate::val::Value;
	static MATCHER: LazyLock<SkimMatcherV2> =
		LazyLock::new(|| SkimMatcherV2::default().ignore_case());

	use strsim;

	pub fn fuzzy(arg: (String, String)) -> Result<Value> {
		smithwaterman(arg)
	}

	/// Calculate the Jaro similarity between two strings
	/// via [`strsim::jaro`].
	pub fn jaro((a, b): (String, String)) -> Result<Value> {
		Ok(strsim::jaro(&a, &b).into())
	}

	/// Calculate the Jaro-Winkler similarity between two strings
	/// via [`strsim::jaro_winkler`].
	pub fn jaro_winkler((a, b): (String, String)) -> Result<Value> {
		Ok(strsim::jaro_winkler(&a, &b).into())
	}

	pub fn smithwaterman((a, b): (String, String)) -> Result<Value> {
		Ok(MATCHER.fuzzy_match(&a, &b).unwrap_or(0).into())
	}

	/// Calculate the Sørensen-Dice similarity between two strings
	/// via [`strsim::sorensen_dice`].
	pub fn sorensen_dice((a, b): (String, String)) -> Result<Value> {
		Ok(strsim::sorensen_dice(&a, &b).into())
	}
}

pub mod semver {

	use anyhow::Result;
	use semver::Version;

	use crate::err::Error;
	use crate::val::Value;

	fn parse_version(ver: &str, func: &str, msg: &str) -> Result<Version> {
		Version::parse(ver)
			.map_err(|_| Error::InvalidArguments {
				name: String::from(func),
				message: String::from(msg),
			})
			.map_err(anyhow::Error::new)
	}

	pub fn compare((left, right): (String, String)) -> Result<Value> {
		let left = parse_version(
			&left,
			"string::semver::compare",
			"Invalid semantic version string for left argument",
		)?;
		let right = parse_version(
			&right,
			"string::semver::compare",
			"Invalid semantic version string for right argument",
		)?;

		Ok((left.cmp(&right) as i32).into())
	}

	pub fn major((version,): (String,)) -> Result<Value> {
		parse_version(&version, "string::semver::major", "Invalid semantic version")
			.map(|v| v.major.into())
	}

	pub fn minor((version,): (String,)) -> Result<Value> {
		parse_version(&version, "string::semver::minor", "Invalid semantic version")
			.map(|v| v.minor.into())
	}

	pub fn patch((version,): (String,)) -> Result<Value> {
		parse_version(&version, "string::semver::patch", "Invalid semantic version")
			.map(|v| v.patch.into())
	}

	pub mod inc {
		use anyhow::Result;

		use crate::fnc::string::semver::parse_version;
		use crate::val::Value;

		pub fn major((version,): (String,)) -> Result<Value> {
			parse_version(&version, "string::semver::inc::major", "Invalid semantic version").map(
				|mut version| {
					version.major += 1;
					version.minor = 0;
					version.patch = 0;
					version.to_string().into()
				},
			)
		}

		pub fn minor((version,): (String,)) -> Result<Value> {
			parse_version(&version, "string::semver::inc::minor", "Invalid semantic version").map(
				|mut version| {
					version.minor += 1;
					version.patch = 0;
					version.to_string().into()
				},
			)
		}

		pub fn patch((version,): (String,)) -> Result<Value> {
			parse_version(&version, "string::semver::inc::patch", "Invalid semantic version").map(
				|mut version| {
					version.patch += 1;
					version.to_string().into()
				},
			)
		}
	}

	pub mod set {
		use anyhow::Result;

		use crate::fnc::string::semver::parse_version;
		use crate::val::Value;

		pub fn major((version, value): (String, i64)) -> Result<Value> {
			// TODO: Deal with negative trunc:
			let value = value as u64;
			parse_version(&version, "string::semver::set::major", "Invalid semantic version").map(
				|mut version| {
					version.major = value;
					version.to_string().into()
				},
			)
		}

		pub fn minor((version, value): (String, i64)) -> Result<Value> {
			// TODO: Deal with negative trunc:
			let value = value as u64;
			parse_version(&version, "string::semver::set::minor", "Invalid semantic version").map(
				|mut version| {
					version.minor = value;
					version.to_string().into()
				},
			)
		}

		pub fn patch((version, value): (String, i64)) -> Result<Value> {
			// TODO: Deal with negative trunc:
			let value = value as u64;

			parse_version(&version, "string::semver::set::patch", "Invalid semantic version").map(
				|mut version| {
					version.patch = value;
					version.to_string().into()
				},
			)
		}
	}
}

#[cfg(test)]
mod tests {
	use super::{matches, replace, slice};
	use crate::fnc::args::{Cast, Optional};
	use crate::val::Value;

	#[test]
	fn string_slice() {
		#[track_caller]
		fn test(initial: &str, beg: Option<i64>, end: Option<i64>, expected: &str) {
			assert_eq!(
				slice((initial.to_owned(), Optional(beg), Optional(end))).unwrap(),
				Value::from(expected)
			);
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
	fn string_replace() {
		#[track_caller]
		fn test(base: &str, pattern: Value, replacement: &str, expected: &str) {
			assert_eq!(
				replace((base.to_string(), pattern.clone(), replacement.to_string())).unwrap(),
				Value::from(expected),
				"replace({},{},{})",
				base,
				pattern,
				replacement
			);
		}

		test("foo bar", Value::Regex("foo".parse().unwrap()), "bar", "bar bar");
		test("foo bar", "bar".into(), "foo", "foo foo");
	}

	#[test]
	fn string_matches() {
		#[track_caller]
		fn test(base: &str, regex: &str, expected: bool) {
			assert_eq!(
				matches((base.to_string(), Cast(regex.parse().unwrap()))).unwrap(),
				Value::from(expected),
				"matches({},{})",
				base,
				regex
			);
		}

		test("bar", "foo", false);
		test("", "foo", false);
		test("foo bar", "foo", true);
		test("foo bar", "bar", true);
	}

	#[test]
	fn html_encode() {
		let value = super::html::encode((String::from("<div>Hello world!</div>"),)).unwrap();
		assert_eq!(value, Value::Strand("&lt;div&gt;Hello&#32;world!&lt;&#47;div&gt;".into()));

		let value = super::html::encode((String::from("SurrealDB"),)).unwrap();
		assert_eq!(value, Value::Strand("SurrealDB".into()));
	}

	#[test]
	fn html_sanitize() {
		let value = super::html::sanitize((String::from("<div>Hello world!</div>"),)).unwrap();
		assert_eq!(value, Value::Strand("<div>Hello world!</div>".into()));

		let value = super::html::sanitize((String::from("XSS<script>attack</script>"),)).unwrap();
		assert_eq!(value, Value::Strand("XSS".into()));
	}

	#[test]
	fn semver_compare() {
		let value = super::semver::compare((String::from("1.2.3"), String::from("1.0.0"))).unwrap();
		assert_eq!(value, Value::from(1));

		let value = super::semver::compare((String::from("1.2.3"), String::from("1.2.3"))).unwrap();
		assert_eq!(value, Value::from(0));

		let value = super::semver::compare((String::from("1.0.0"), String::from("1.2.3"))).unwrap();
		assert_eq!(value, Value::from(-1));
	}

	#[test]
	fn semver_extract() {
		let value = super::semver::major((String::from("1.2.3"),)).unwrap();
		assert_eq!(value, Value::from(1));

		let value = super::semver::minor((String::from("1.2.3"),)).unwrap();
		assert_eq!(value, Value::from(2));

		let value = super::semver::patch((String::from("1.2.3"),)).unwrap();
		assert_eq!(value, Value::from(3));
	}

	#[test]
	fn semver_increment() {
		let value = super::semver::inc::major((String::from("1.2.3"),)).unwrap();
		assert_eq!(value, Value::from("2.0.0"));

		let value = super::semver::inc::minor((String::from("1.2.3"),)).unwrap();
		assert_eq!(value, Value::from("1.3.0"));

		let value = super::semver::inc::patch((String::from("1.2.3"),)).unwrap();
		assert_eq!(value, Value::from("1.2.4"));
	}

	#[test]
	fn semver_set() {
		let value = super::semver::set::major((String::from("1.2.3"), 9)).unwrap();
		assert_eq!(value, Value::from("9.2.3"));

		let value = super::semver::set::minor((String::from("1.2.3"), 9)).unwrap();
		assert_eq!(value, Value::from("1.9.3"));

		let value = super::semver::set::patch((String::from("1.2.3"), 9)).unwrap();
		assert_eq!(value, Value::from("1.2.9"));
	}
}
