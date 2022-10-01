use crate::err::Error;
use crate::sql::error::Error::{ExcessiveDepth, ParserError};
use crate::sql::error::IResult;
use crate::sql::query::{query, Query};
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use nom::Err;
use std::str;

pub fn parse(input: &str) -> Result<Query, Error> {
	parse_impl(input, query)
}

pub fn thing(input: &str) -> Result<Thing, Error> {
	parse_impl(input, super::thing::thing)
}

pub fn json(input: &str) -> Result<Value, Error> {
	parse_impl(input, super::value::json)
}

fn parse_impl<O>(input: &str, parser: impl Fn(&str) -> IResult<&str, O>) -> Result<O, Error> {
	let _parsing = depth::begin();

	match input.trim().len() {
		0 => Err(Error::QueryEmpty),
		_ => match parser(input) {
			Ok((_, parsed)) => Ok(parsed),
			Err(Err::Error(e)) | Err(Err::Failure(e)) => Err(match e {
				ParserError(e) => {
					let (s, l, c) = locate(input, e);
					Error::InvalidQuery {
						line: l,
						char: c,
						sql: s.to_string(),
					}
				}
				ExcessiveDepth => {
					// TODO: Replace when https://github.com/surrealdb/surrealdb/pull/241 lands.
					Error::TooManySubqueries
				}
			}),
			_ => unreachable!(),
		},
	}
}

fn truncate(s: &str, l: usize) -> &str {
	match s.char_indices().nth(l) {
		None => s,
		Some((i, _)) => &s[..i],
	}
}

fn locate<'a>(input: &str, tried: &'a str) -> (&'a str, usize, usize) {
	let index = input.len() - tried.len();
	let tried = truncate(tried, 100);
	let lines = input.split('\n').collect::<Vec<&str>>();
	let lines = lines.iter().map(|l| l.len()).enumerate();
	let (mut total, mut chars) = (0, 0);
	for (line, size) in lines {
		total += size + 1;
		if index < total {
			let line_num = line + 1;
			let char_num = index - chars;
			return (tried, line_num, char_num);
		}
		chars += size + 1;
	}
	(tried, 0, 0)
}

pub(crate) mod depth {
	use crate::sql::Error::ExcessiveDepth;
	use nom::Err;
	use std::cell::Cell;
	use std::time::{Duration, Instant};

	/// Maximum bytes of call stack (guard against stack overflow).
	const SIZE_LIMIT: usize = 500_000;
	/// Maximum time to parse something (guard against exponential runtime).
	const TIME_LIMIT: Duration = Duration::from_secs(4);

	thread_local! {
		/// Approximate address of the stack frame where the parsing began, and exact time when it
		/// began.
		///
		/// If None, parsing as a test.
		static INITIAL: Cell<Option<(usize, Instant)>> = Cell::new(None);
	}

	/// Get approximate address of stack frame.
	#[inline(always)]
	fn measure() -> usize {
		#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
		return {
			let on_stack = 0x4BAD1DEA;
			&on_stack as *const _ as usize
		};

		#[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
		0
	}

	/// Call when starting the parser to reset the call stack depth measurement and start time.
	///
	/// Returns a struct that, when dropped, will clear the effect.
	#[inline(never)]
	pub(super) fn begin() -> Parsing {
		INITIAL.with(|initial| initial.set(Some((measure(), Instant::now()))));
		Parsing
	}

	#[must_use]
	#[non_exhaustive]
	pub(super) struct Parsing;

	impl Drop for Parsing {
		fn drop(&mut self) {
			INITIAL.with(|initial| {
				let old = initial.replace(None);
				debug_assert!(old.is_some());
			});
		}
	}

	/// Call in recursive parsing code paths to limit call stack depth and parsing time.
	#[inline(never)]
	#[must_use = "use ? to error if the limit is exceeded"]
	pub(crate) fn limit() -> Result<(), Err<crate::sql::Error<&'static str>>> {
		if let Some((initial_frame, initial_time)) = INITIAL.with(|initial| initial.get()) {
			if measure().saturating_add(SIZE_LIMIT) > initial_frame
				&& initial_time.elapsed() < TIME_LIMIT
			{
				Ok(())
			} else {
				Err(Err::Failure(ExcessiveDepth))
			}
		} else {
			#[cfg(not(test))]
			debug_assert!(false, "sql::parser::depth::begin not called during non-test parsing");
			Ok(())
		}
	}

	#[cfg(test)]
	mod tests {
		use super::*;
		use std::sync::atomic::{AtomicUsize, Ordering};

		#[test]
		fn no_stack_overflow() {
			let _parsing = begin();

			static CALLS: AtomicUsize = AtomicUsize::new(0);

			fn recursive(i: &str) -> Result<(), Err<crate::sql::Error<&str>>> {
				CALLS.fetch_add(1, Ordering::Relaxed);
				limit()?;
				recursive(i)
			}

			assert!(recursive("foo").is_err());
			println!("calls (stack size): {}", CALLS.load(Ordering::Relaxed));
			assert!(CALLS.load(Ordering::Relaxed) >= 1000);
		}

		#[test]
		//#[ignore = "takes 5 seconds"]
		fn timeout() {
			let _parsing = begin();

			static CALLS: AtomicUsize = AtomicUsize::new(0);

			fn recursive_expensive(i: &str) -> Result<(), Err<crate::sql::Error<&str>>> {
				CALLS.fetch_add(1, Ordering::Relaxed);
				limit()?;
				std::thread::sleep(Duration::from_secs(1));
				recursive_expensive(i)
			}

			assert!(recursive_expensive("foo").is_err());

			let expected = TIME_LIMIT.as_secs() as usize + 1;

			println!("calls (timeout): {}", CALLS.load(Ordering::Relaxed));
			assert!((expected.saturating_sub(1)..=expected.saturating_add(1))
				.contains(&CALLS.load(Ordering::Relaxed)));
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql;
	use std::time::Instant;

	#[test]
	fn no_ending() {
		let sql = "SELECT * FROM test";
		let res = parse(sql);
		assert!(res.is_ok());
	}

	#[test]
	fn parse_query_string() {
		let sql = "SELECT * FROM test;";
		let res = parse(sql);
		assert!(res.is_ok());
	}

	#[test]
	fn trim_query_string() {
		let sql = "    SELECT    *    FROM    test    ;    ";
		let res = parse(sql);
		assert!(res.is_ok());
	}

	#[test]
	fn parse_complex_rubbish() {
		let sql = "    SELECT    *    FROM    test    ; /* shouldbespace */ ;;;    ";
		let res = parse(sql);
		assert!(res.is_ok());
	}

	#[test]
	fn parse_complex_failure() {
		let sql = "    SELECT    *    FROM    { }} ";
		let res = parse(sql);
		assert!(res.is_err());
	}

	#[test]
	fn parser_try() {
		let sql = "
			SELECT
				*,
				tags[$].value,
				3s as duration,
				1.345 AS number,
				test AS `some thing`,
				'2012-04-23T18:25:43.511Z' AS utctime,
				'2012-04-23T18:25:43.511-08:00' AS pacifictime,
				{ key: (3 + 1 + 2), other: 9 * 7, 'some thing': { otherkey: 'text', } } AS object
			FROM $param, test, temp, test:thingy, |test:10|, |test:1..10|
			WHERE IF true THEN 'YAY' ELSE 'OOPS' END
				AND (0.1341, 0.5719) INSIDE { type: 'Polygon', coordinates: [[[0.1341, 0.5719], [0.1341, 0.5719]]] }
				AND (3 + 3 * 4)=6
				AND 3 + 3 * 4 = 6
				AND ages CONTAINS 18
				AND if IS true
			SPLIT test.things
			VERSION '2019-01-01'
			TIMEOUT 2w;

			CREATE person SET name = 'Tobie', age += 18;
		";
		let res = parse(sql);
		assert!(res.is_ok());
		let tmp = res.unwrap();

		let enc: Vec<u8> = Vec::from(&tmp);
		let dec: Query = Query::from(enc);
		assert_eq!(tmp, dec);
	}

	#[test]
	fn depth_limit() {
		fn nested_functions(n: usize) -> String {
			let mut ret = String::from("SELECT * FROM ");
			for _ in 0..n {
				ret.push_str("array::sort(");
			}
			ret.push_str("[0]");
			for _ in 0..n {
				ret.push(')');
			}
			ret
		}

		for n in 0..=16 {
			let query = nested_functions(n);
			let start = Instant::now();
			let ok = sql::parse(&query).is_ok();
			let duration = start.elapsed().as_secs_f32();
			println!("{n},{duration:.6},{ok}");
		}
	}
}
