use crate::err::Error;
use crate::sql::query::{query, Query};
use nom::Err;
use std::str;

#[allow(dead_code)]
pub fn parse(input: &str) -> Result<Query, Error> {
	match input.trim().len() {
		0 => Err(Error::EmptyError),
		_ => match query(input) {
			Ok((_, query)) => Ok(query),
			Err(Err::Error(e)) => Err(Error::ParseError {
				pos: input.len() - e.input.len(),
				sql: String::from(e.input),
			}),
			Err(Err::Failure(e)) => Err(Error::ParseError {
				pos: input.len() - e.input.len(),
				sql: String::from(e.input),
			}),
			Err(Err::Incomplete(_)) => Err(Error::EmptyError),
		},
	}
}

#[cfg(test)]
mod tests {

	use super::*;

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
				-999999999999999.9999999 AS double,
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

		let enc = serde_cbor::to_vec(&tmp).unwrap();
		let dec: Query = serde_cbor::from_slice(&enc).unwrap();
		assert_eq!(tmp, dec);
	}
}
