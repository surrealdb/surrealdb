use crate::sql::{Datetime, Duration, Idiom, Query, Range, Thing, Value};
use crate::{err::Error, sql::Subquery};
use nom::{Err, Finish};

pub mod literal;
mod part;
mod stmt;

mod block;
pub(crate) mod builtin;
mod comment;
mod common;
mod depth;
mod ending;
mod error;
mod expression;
mod function;
mod idiom;
mod kind;
mod omit;
mod operator;
mod special;
mod subquery;
mod thing;
mod value;

pub use error::{IResult, ParseError};

#[cfg(test)]
pub(crate) mod test;

fn query(i: &str) -> IResult<&str, Query> {
	let (i, v) = stmt::statements(i)?;
	if !i.is_empty() {
		return Err(Err::Failure(ParseError::ExplainedExpected {
			tried: i,
			expected: "query to end",
			explained: "perhaps missing a semicolon on the previous statement?",
		}));
	}
	Ok((i, Query(v)))
}

/// Parses a SurrealQL [`Query`]
///
/// During query parsing, the total depth of calls to parse values (including arrays, expressions,
/// functions, objects, sub-queries), Javascript values, and geometry collections count against
/// a computation depth limit. If the limit is reached, parsing will return
/// [`Error::ComputationDepthExceeded`], as opposed to spending more time and potentially
/// overflowing the call stack.
///
/// If you encounter this limit and believe that it should be increased,
/// please [open an issue](https://github.com/surrealdb/surrealdb/issues)!
#[instrument(level = "debug", name = "parser", skip_all, fields(length = input.len()))]
pub fn parse(input: &str) -> Result<Query, Error> {
	parse_impl(input, query)
}

/// Parses a SurrealQL [`Value`].
#[instrument(level = "debug", name = "parser", skip_all, fields(length = input.len()))]
pub fn value(input: &str) -> Result<Value, Error> {
	parse_impl(input, value::value)
}

/// Parses JSON into an inert SurrealQL [`Value`]
#[instrument(level = "debug", name = "parser", skip_all, fields(length = input.len()))]
pub fn json(input: &str) -> Result<Value, Error> {
	parse_impl(input, value::json)
}
/// Parses a SurrealQL Subquery [`Subquery`]
#[instrument(level = "debug", name = "parser", skip_all, fields(length = input.len()))]
pub fn subquery(input: &str) -> Result<Subquery, Error> {
	parse_impl(input, subquery::subquery)
}

/// Parses a SurrealQL [`Idiom`]
#[instrument(level = "debug", name = "parser", skip_all, fields(length = input.len()))]
pub fn idiom(input: &str) -> Result<Idiom, Error> {
	parse_impl(input, idiom::plain)
}

pub fn datetime(input: &str) -> Result<Datetime, Error> {
	parse_impl(input, literal::datetime)
}

pub fn datetime_raw(input: &str) -> Result<Datetime, Error> {
	parse_impl(input, literal::datetime_all_raw)
}

pub fn duration(input: &str) -> Result<Duration, Error> {
	parse_impl(input, literal::duration)
}

pub fn path_like(input: &str) -> Result<Value, Error> {
	parse_impl(input, value::path_like)
}

pub fn range(input: &str) -> Result<Range, Error> {
	parse_impl(input, literal::range)
}

/// Parses a SurrealQL [`Thing`]
pub fn thing(input: &str) -> Result<Thing, Error> {
	parse_impl(input, thing::thing)
}

pub fn thing_raw(input: &str) -> Result<Thing, Error> {
	parse_impl(input, thing::thing_raw)
}

fn parse_impl<O>(input: &str, parser: impl Fn(&str) -> IResult<&str, O>) -> Result<O, Error> {
	// Reset the parse depth limiter
	depth::reset();

	// Check the length of the input
	match input.trim().len() {
		// The input query was empty
		0 => Err(Error::QueryEmpty),
		// Continue parsing the query
		_ => match parser(input).finish() {
			// The query was parsed successfully
			Ok((v, parsed)) if v.is_empty() => Ok(parsed),
			// There was unparsed SQL remaining
			Ok((_, _)) => Err(Error::QueryRemaining),
			// There was an error when parsing the query
			Err(e) => Err(Error::InvalidQuery(e.render_on(input))),
		},
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use serde::Serialize;
	use std::{
		collections::HashMap,
		time::{Duration, Instant},
	};

	#[test]
	fn no_ending() {
		let sql = "SELECT * FROM test";
		parse(sql).unwrap();
	}

	#[test]
	fn parse_query_string() {
		let sql = "SELECT * FROM test;";
		parse(sql).unwrap();
	}

	#[test]
	fn trim_query_string() {
		let sql = "    SELECT    *    FROM    test    ;    ";
		parse(sql).unwrap();
	}

	#[test]
	fn parse_complex_rubbish() {
		let sql = "    SELECT    *    FROM    test    ; /* shouldbespace */ ;;;    ";
		parse(sql).unwrap();
	}

	#[test]
	fn parse_complex_failure() {
		let sql = "    SELECT    *    FROM    { }} ";
		parse(sql).unwrap_err();
	}

	#[test]
	fn parse_ok_recursion() {
		let sql = "SELECT * FROM ((SELECT * FROM (5))) * 5;";
		parse(sql).unwrap();
	}

	#[test]
	fn parse_ok_recursion_deeper() {
		let sql = "SELECT * FROM (((( SELECT * FROM ((5)) + ((5)) + ((5)) )))) * ((( function() {return 5;} )));";
		let start = Instant::now();
		parse(sql).unwrap();
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
		let tmp = parse(sql).unwrap();

		let enc: Vec<u8> = Vec::from(&tmp);
		let dec: Query = Query::from(enc);
		assert_eq!(tmp, dec);
	}

	#[test]
	fn parser_full() {
		let sql = std::fs::read("test.surql").unwrap();
		let sql = std::str::from_utf8(&sql).unwrap();
		let res = parse(sql);
		let tmp = res.unwrap();

		let enc: Vec<u8> = Vec::from(&tmp);
		let dec: Query = Query::from(enc);
		assert_eq!(tmp, dec);
	}

	#[test]
	#[cfg_attr(debug_assertions, ignore)]
	fn json_benchmark() {
		// From the top level of the repository,
		// cargo test sql::parser::tests::json_benchmark --package surrealdb --lib --release -- --nocapture --exact

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

	#[test]
	fn single_query() {
		let sql = "CREATE test";
		let res = query(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("CREATE test;", format!("{}", out))
	}

	#[test]
	fn multiple_query() {
		let sql = "CREATE test; CREATE temp;";
		let res = query(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("CREATE test;\nCREATE temp;", format!("{}", out))
	}

	#[test]
	fn multiple_query_semicolons() {
		let sql = "CREATE test;;;CREATE temp;;;";
		let res = query(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("CREATE test;\nCREATE temp;", format!("{}", out))
	}

	#[test]
	fn multiple_query_semicolons_comments() {
		let sql = "CREATE test;;;CREATE temp;;;/* some comment */";
		let res = query(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("CREATE test;\nCREATE temp;", format!("{}", out))
	}

	#[test]
	fn multiple_query_semicolons_multi_comments() {
		let sql = "CREATE test;;;CREATE temp;;;/* some comment */;;;/* other comment */";
		let res = query(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("CREATE test;\nCREATE temp;", format!("{}", out))
	}
}
