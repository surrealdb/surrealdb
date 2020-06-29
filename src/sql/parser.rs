use crate::err::Error;
use crate::sql::query::{query, Query};
use nom::Err;
use std::str;

#[allow(dead_code)]
pub fn parse(input: &str) -> Result<Query, Error> {
	match query(input) {
		Ok((_, query)) => {
			if query.empty() {
				Err(Error::EmptyError)
			} else {
				Ok(query)
			}
		}
		Err(Err::Error((i, _))) => Err(Error::ParseError {
			pos: input.len() - i.len(),
			sql: String::from(i),
		}),
		Err(Err::Failure((i, _))) => Err(Error::ParseError {
			pos: input.len() - i.len(),
			sql: String::from(i),
		}),
		Err(Err::Incomplete(_)) => Err(Error::EmptyError),
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
		println!("{}", res.err().unwrap())
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
				{ key: (3 + 1 + 2), 'some thing': { otherkey: 'text', } } AS object
			FROM $param, test, temp, test:thingy, |test:10|, |test:1..10|
			WHERE IF true THEN 'YAY' ELSE 'OOPS' END
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
		println!("{:#?}", serde_json::to_string(&tmp).unwrap());
		println!("{}", serde_cbor::to_vec(&tmp).unwrap().len());
		println!("{}", tmp)
	}
}
