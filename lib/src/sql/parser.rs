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
				ExcessiveDepth => Error::ComputationDepthExceeded,
			}),
			_ => unreachable!(),
		},
	}
}

fn truncate(s: &str, l: usize) -> &str {
	// TODO: use s.floor_char_boundary once https://github.com/rust-lang/rust/issues/93743 lands
	match s.char_indices().nth(l) {
		None => s,
		Some((i, _)) => &s[..i],
	}
}

fn locate<'a>(input: &str, tried: &'a str) -> (&'a str, usize, usize) {
	let index = input.len() - tried.len();
	let tried = truncate(tried, 100);
	let lines = input.split('\n').map(|l| l.len()).enumerate();
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
	use crate::cnf::MAX_COMPUTATION_DEPTH;
	use crate::sql::Error::ExcessiveDepth;
	use nom::Err;
	use std::cell::Cell;
	use std::thread::panicking;

	thread_local! {
		/// How many recursion levels deep parsing is currently.
		static DEPTH: Cell<u8> = Cell::default();
	}

	/// Call when starting the parser to reset the recursion depth.
	#[inline(never)]
	pub(super) fn reset() {
		DEPTH.with(|cell| {
			debug_assert_eq!(cell.get(), 0);
			cell.set(0)
		});
	}

	/// Call at least once in recursive parsing code paths to limit recursion depth.
	#[inline(never)]
	#[must_use = "must store and implicitly drop when returning"]
	pub(crate) fn dive() -> Result<Diving, Err<crate::sql::Error<&'static str>>> {
		DEPTH.with(|cell| {
			let depth = cell.get();
			if depth < MAX_COMPUTATION_DEPTH {
				cell.replace(depth + 1);
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
			DEPTH.with(|cell| {
				if let Some(depth) = cell.get().checked_sub(1) {
					cell.replace(depth);
				} else {
					debug_assert!(panicking());
				}
			});
		}
	}

	#[cfg(test)]
	mod tests {
		use super::*;
		use std::sync::atomic::{AtomicU8, Ordering};

		#[test]
		fn no_stack_overflow() {
			static CALLS: AtomicU8 = AtomicU8::new(0);

			fn recursive(i: &str) -> Result<(), Err<crate::sql::Error<&str>>> {
				let _diving = dive()?;
				CALLS.fetch_add(1, Ordering::Relaxed);
				recursive(i)
			}

			reset();
			assert!(recursive("foo").is_err());

			assert_eq!(CALLS.load(Ordering::Relaxed), MAX_COMPUTATION_DEPTH);
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
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
	fn parse_ok_recursion() {
		let sql = "SELECT * FROM ((SELECT * FROM (5))) * 5;";
		let res = parse(sql);
		assert!(res.is_ok());
	}

	#[test]
	fn parse_also_ok_recursion() {
		let sql = "SELECT * FROM (((( SELECT * FROM ((5)) + ((5)) + ((5)) )))) * ((( function() {return 5;} )));";
		let start = Instant::now();
		let res = parse(sql);
		let elapsed = start.elapsed();
		assert!(res.is_ok());
		assert!(elapsed < Duration::from_millis(150), "previously took ~15ms in debug")
	}

	#[test]
	fn parse_recursion_mixed() {
		recursive("", "SELECT * FROM ((((", "5 * 5", ")))) * 5", 3, false);
		recursive("", "SELECT * FROM ((((", "5 * 5", ")))) * 5", 8, true);
	}

	#[test]
	fn parse_recursion_select() {
		for p in 1..=3 {
			recursive("SELECT * FROM ", "(SELECT * FROM ", "5", ")", 6usize.pow(p), p > 1);
		}
	}

	#[test]
	fn parse_recursion_javascript() {
		for p in 1..=3 {
			recursive("SELECT * FROM ", "function() {", "return 5;", "}", 10usize.pow(p), p > 1);
		}
	}

	#[test]
	fn parse_recursion_value_subquery() {
		for p in 1..=4 {
			recursive("SELECT * FROM ", "(", "5", ")", 10usize.pow(p), p > 1);
		}
	}

	#[test]
	fn parse_recursion_value_geometry() {
		for n in [3, 40, 100] {
			recursive(
				"SELECT * FROM ",
				r#"{type: "GeometryCollection",geometries: ["#,
				r#"{type: "MultiPoint",coordinates: [[10.0, 11.2],[10.5, 11.9]]}"#,
				"]}",
				n,
				n > 30,
			);
		}
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

	fn recursive(
		prefix: &str,
		recursive_start: &str,
		base: &str,
		recursive_end: &str,
		n: usize,
		excessive: bool,
	) {
		let mut sql = String::from(prefix);
		for _ in 0..n {
			sql.push_str(recursive_start);
		}
		sql.push_str(base);
		for _ in 0..n {
			sql.push_str(recursive_end);
		}
		let start = Instant::now();
		let res = parse(&sql);
		let elapsed = start.elapsed();
		if excessive {
			assert!(
				matches!(res, Err(Error::ComputationDepthExceeded)),
				"expected computation depth exceeded, got {:?}",
				res
			);
		} else {
			res.unwrap();
		}
		// The parser can terminate faster in the excessive case.
		let cutoff = if excessive {
			250
		} else {
			500
		};
		assert!(
			elapsed < Duration::from_millis(cutoff),
			"previously much faster to parse {n} in debug mode"
		)
	}
}
