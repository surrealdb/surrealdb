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
	depth::reset();

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
	use crate::cnf::MAX_RECURSIVE_QUERIES;
	use crate::sql::Error::ExcessiveDepth;
	use nom::Err;
	use std::cell::Cell;
	use std::thread::panicking;

	thread_local! {
		/// How many recursion levels deep parsing is.
		static INITIAL: Cell<usize> = Cell::default();
	}

	/// Call when starting the parser to reset initial parsing state.
	#[inline(never)]
	pub(super) fn reset() {
		INITIAL.with(|initial| {
			debug_assert_eq!(initial.get(), 0);
			initial.set(0)
		});
	}

	/// Call at least once in recursive parsing code paths to limit recursion depth.
	#[inline(never)]
	#[must_use = "must store and implicitly drop when returning"]
	pub(crate) fn dive() -> Result<Diving, Err<crate::sql::Error<&'static str>>> {
		INITIAL.with(|initial| {
			let depth = initial.get();
			// TODO: Replace when https://github.com/surrealdb/surrealdb/pull/241 lands.
			if depth < MAX_RECURSIVE_QUERIES {
				initial.replace(depth + 1);
				Ok(Diving)
			} else {
				Err(Err::Failure(ExcessiveDepth))
			}
		})
	}

	#[must_use]
	#[non_exhaustive]
	pub(crate) struct Diving;

	impl Drop for Diving {
		fn drop(&mut self) {
			INITIAL.with(|initial| {
				if let Some(depth) = initial.get().checked_sub(1) {
					initial.replace(depth);
				} else {
					debug_assert!(panicking());
				}
			});
		}
	}

	#[cfg(test)]
	mod tests {
		use super::*;
		use std::sync::atomic::{AtomicUsize, Ordering};

		#[test]
		fn no_stack_overflow() {
			static CALLS: AtomicUsize = AtomicUsize::new(0);

			fn recursive(i: &str) -> Result<(), Err<crate::sql::Error<&str>>> {
				let _diving = dive()?;
				CALLS.fetch_add(1, Ordering::Relaxed);
				recursive(i)
			}

			reset();
			assert!(recursive("foo").is_err());

			assert_eq!(CALLS.load(Ordering::Relaxed), MAX_RECURSIVE_QUERIES);
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql;
	use std::time::{Duration, Instant};

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

		for n in (0..=64).step_by(4) {
			let query = nested_functions(n);
			let start = Instant::now();
			let ok = sql::parse(&query).is_ok();
			if n < 8 {
				assert!(ok);
			} else if n > 32 {
				assert!(!ok);
			}
			assert!(start.elapsed() < Duration::from_secs(5));
			// let duration = start.elapsed().as_secs_f32();
			// println!("{n},{duration:.6},{ok}");
		}
	}
}
