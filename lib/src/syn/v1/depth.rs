use super::ParseError;
use crate::cnf::MAX_COMPUTATION_DEPTH;
use nom::Err;
use std::cell::Cell;
use std::thread::panicking;

thread_local! {
	/// How many recursion levels deep parsing is currently.
	static DEPTH: Cell<u8> = Cell::default();
}

/// Scale down `MAX_COMPUTATION_DEPTH` for parsing because:
///  - Only a few intermediate parsers, collectively sufficient to limit depth, call dive.
///  - Some of the depth budget during execution is for futures, graph traversal, and
///    other operations that don't exist during parsing.
///  - The parser currently runs in exponential time, so a lower limit guards against
///    CPU-intensive, time-consuming parsing.
const DEPTH_PER_DIVE: u8 = 4;

/// Call when starting the parser to reset the recursion depth.
#[inline(never)]
pub(super) fn reset() {
	DEPTH.with(|cell| {
		debug_assert_eq!(cell.get(), 0, "previous parsing stopped abruptly");
		cell.set(0)
	});
}

/// Call at least once in recursive parsing code paths to limit recursion depth.
#[inline(never)]
#[must_use = "must store and implicitly drop when returning"]
pub(crate) fn dive<I>(position: I) -> Result<Diving, Err<ParseError<I>>> {
	DEPTH.with(|cell| {
		let depth = cell.get().saturating_add(DEPTH_PER_DIVE);
		if depth <= *MAX_COMPUTATION_DEPTH {
			cell.replace(depth);
			Ok(Diving)
		} else {
			Err(Err::Failure(ParseError::ExcessiveDepth(position)))
		}
	})
}

#[must_use]
#[non_exhaustive]
pub(crate) struct Diving;

impl Drop for Diving {
	fn drop(&mut self) {
		DEPTH.with(|cell| {
			if let Some(depth) = cell.get().checked_sub(DEPTH_PER_DIVE) {
				cell.replace(depth);
			} else {
				debug_assert!(panicking());
			}
		});
	}
}

#[cfg(test)]
mod tests {

	use super::super::{super::super::syn, query};
	use super::*;
	use crate::sql::{Query, Value};
	use nom::Finish;
	use serde::Serialize;
	use std::{
		collections::HashMap,
		time::{Duration, Instant},
	};

	#[test]
	fn no_ending() {
		let sql = "SELECT * FROM test";
		syn::parse(sql).unwrap();
	}

	#[test]
	fn parse_query_string() {
		let sql = "SELECT * FROM test;";
		syn::parse(sql).unwrap();
	}

	#[test]
	fn trim_query_string() {
		let sql = "    SELECT    *    FROM    test    ;    ";
		syn::parse(sql).unwrap();
	}

	#[test]
	fn parse_complex_rubbish() {
		let sql = "    SELECT    *    FROM    test    ; /* shouldbespace */ ;;;    ";
		syn::parse(sql).unwrap();
	}

	#[test]
	fn parse_complex_failure() {
		let sql = "    SELECT    *    FROM    { }} ";
		syn::parse(sql).unwrap_err();
	}

	#[test]
	fn parse_ok_recursion() {
		let sql = "SELECT * FROM ((SELECT * FROM (5))) * 5;";
		syn::parse(sql).unwrap();
	}

	#[test]
	fn parse_ok_recursion_deeper() {
		let sql = "SELECT * FROM (((( SELECT * FROM ((5)) + ((5)) + ((5)) )))) * ((( function() {return 5;} )));";
		let start = Instant::now();
		syn::parse(sql).unwrap();
		let elapsed = start.elapsed();
		assert!(
			elapsed < Duration::from_millis(2000),
			"took {}ms, previously took ~1000ms in debug",
			elapsed.as_millis()
		)
	}

	#[test]
	fn parse_recursion_cast() {
		for n in [10, 100, 500] {
			recursive("SELECT * FROM ", "<int>", "5", "", n, n > 50);
		}
	}

	#[test]
	fn parse_recursion_geometry() {
		for n in [1, 50, 100] {
			recursive(
				"SELECT * FROM ",
				r#"{type: "GeometryCollection",geometries: ["#,
				r#"{type: "MultiPoint",coordinates: [[10.0, 11.2],[10.5, 11.9]]}"#,
				"]}",
				n,
				n > 25,
			);
		}
	}

	#[test]
	fn parse_recursion_javascript() {
		for n in [10, 1000] {
			recursive("SELECT * FROM ", "function() {", "return 5;", "}", n, n > 500);
		}
	}

	#[test]
	fn parse_recursion_mixed() {
		for n in [3, 15, 75] {
			recursive("", "SELECT * FROM ((((", "5 * 5", ")))) * 5", n, n > 5);
		}
	}

	#[test]
	fn parse_recursion_select() {
		for n in [5, 10, 100] {
			recursive("SELECT * FROM ", "(SELECT * FROM ", "5", ")", n, n > 15);
		}
	}

	#[test]
	fn parse_recursion_value_subquery() {
		for p in 1..=4 {
			recursive("SELECT * FROM ", "(", "5", ")", 10usize.pow(p), p > 1);
		}
	}

	#[test]
	fn parse_recursion_if_subquery() {
		for p in 1..=3 {
			recursive("SELECT * FROM ", "IF true THEN ", "5", " ELSE 4 END", 6usize.pow(p), p > 1);
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
			VERSION '2019-01-01T08:00:00Z'
			TIMEOUT 2w;

		CREATE person SET name = 'Tobie', age += 18;
		";
		let tmp = syn::parse(sql).unwrap();

		let enc: Vec<u8> = Vec::from(&tmp);
		let dec: Query = Query::from(enc);
		assert_eq!(tmp, dec);
	}

	#[test]
	fn parser_full() {
		let sql = std::fs::read("test.surql").unwrap();
		let sql = std::str::from_utf8(&sql).unwrap();
		let res = syn::parse(sql);
		let tmp = res.unwrap();

		let enc: Vec<u8> = Vec::from(&tmp);
		let dec: Query = Query::from(enc);
		assert_eq!(tmp, dec);
	}

	#[test]
	#[cfg_attr(debug_assertions, ignore)]
	fn json_benchmark() {
		// From the top level of the repository,
		// cargo test sql::syn::tests::json_benchmark --package surrealdb --lib --release -- --nocapture --exact

		#[derive(Clone, Serialize)]
		struct Data {
			boolean: bool,
			integer: i32,
			decimal: f32,
			string: String,
			inner: Option<Box<Self>>,
			inners: Vec<Self>,
			inner_map: HashMap<String, Self>,
		}

		let inner = Data {
			boolean: true,
			integer: -1,
			decimal: 0.5,
			string: "foo".to_owned(),
			inner: None,
			inners: Vec::new(),
			inner_map: HashMap::new(),
		};
		let inners = vec![inner.clone(); 10];

		let data = Data {
			boolean: false,
			integer: 42,
			decimal: 9000.0,
			string: "SurrealDB".to_owned(),
			inner_map: inners.iter().enumerate().map(|(i, d)| (i.to_string(), d.clone())).collect(),
			inners,
			inner: Some(Box::new(inner)),
		};

		let json = serde_json::to_string(&data).unwrap();
		let json_pretty = serde_json::to_string_pretty(&data).unwrap();

		let benchmark = |de: fn(&str) -> Value| {
			let time = Instant::now();
			const ITERATIONS: u32 = 32;
			for _ in 0..ITERATIONS {
				std::hint::black_box(de(std::hint::black_box(&json)));
				std::hint::black_box(de(std::hint::black_box(&json_pretty)));
			}
			time.elapsed().as_secs_f32() / (2 * ITERATIONS) as f32
		};

		println!("sql::json took {:.10}s/iter", benchmark(|s| crate::sql::json(s).unwrap()));
	}

	/// Try parsing a query with O(n) recursion depth and expect to fail if and only if
	/// `excessive` is true.
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
		let res = query(&sql).finish();
		let elapsed = start.elapsed();
		if excessive {
			assert!(
				matches!(res, Err(ParseError::ExcessiveDepth(_))),
				"expected computation depth exceeded, got {:?}",
				res
			);
		} else {
			res.unwrap();
		}
		// The parser can terminate faster in the excessive case.
		let cutoff = if excessive {
			500
		} else {
			1000
		};
		assert!(
			elapsed < Duration::from_millis(cutoff),
			"took {}ms, previously much faster to parse {n} in debug mode",
			elapsed.as_millis()
		)
	}
}
