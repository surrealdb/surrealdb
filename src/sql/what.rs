use crate::sql::common::commas;
use crate::sql::model::{model, Model};
use crate::sql::param::{param, Param};
use crate::sql::regex::{regex, Regex};
use crate::sql::table::{table, Table};
use crate::sql::thing::{thing, Thing};
use nom::branch::alt;
use nom::combinator::map;
use nom::multi::separated_nonempty_list;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Whats(Vec<What>);

impl fmt::Display for Whats {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", self.0.iter().map(|ref v| format!("{}", v)).collect::<Vec<_>>().join(", "))
	}
}

pub fn whats(i: &str) -> IResult<&str, Whats> {
	let (i, v) = separated_nonempty_list(commas, what)(i)?;
	Ok((i, Whats(v)))
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum What {
	Param(Param),
	Model(Model),
	Table(Table),
	Thing(Thing),
	Regex(Regex),
}

impl fmt::Display for What {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match *self {
			What::Param(ref p) => write!(f, "{}", p),
			What::Model(ref t) => write!(f, "{}", t),
			What::Table(ref t) => write!(f, "{}", t),
			What::Thing(ref t) => write!(f, "{}", t),
			What::Regex(ref t) => write!(f, "{}", t),
		}
	}
}

pub fn what(i: &str) -> IResult<&str, What> {
	alt((
		map(param, |v| What::Param(v)),
		map(model, |v| What::Model(v)),
		map(regex, |v| What::Regex(v)),
		map(thing, |v| What::Thing(v)),
		map(table, |v| What::Table(v)),
	))(i)
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn what_table() {
		let sql = "test";
		let res = what(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			out,
			What::Table(Table {
				name: String::from("test"),
			})
		);
	}

	#[test]
	fn what_param() {
		let sql = "$test";
		let res = what(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			out,
			What::Param(Param {
				name: String::from("test"),
			})
		);
	}

	#[test]
	fn what_thing() {
		let sql = "test:tester";
		let res = what(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			out,
			What::Thing(Thing {
				table: String::from("test"),
				id: String::from("tester"),
			})
		);
	}

	#[test]
	fn what_regex() {
		let sql = "/test/";
		let res = what(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			out,
			What::Regex(Regex {
				value: String::from("test"),
			})
		);
	}

	#[test]
	fn what_model() {
		let sql = "|test:1000|";
		let res = what(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			out,
			What::Model(Model {
				table: String::from("test"),
				count: Some(1000),
				range: None,
			})
		);
	}

	#[test]
	fn what_multiple() {
		let sql = "test, $test, /test/, test:tester, |test:1000|, |test:1..10|";
		let res = whats(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			out,
			Whats(vec![
				What::Table(Table {
					name: String::from("test"),
				}),
				What::Param(Param {
					name: String::from("test"),
				}),
				What::Regex(Regex {
					value: String::from("test"),
				}),
				What::Thing(Thing {
					table: String::from("test"),
					id: String::from("tester"),
				}),
				What::Model(Model {
					table: String::from("test"),
					count: Some(1000),
					range: None,
				}),
				What::Model(Model {
					table: String::from("test"),
					count: None,
					range: Some((1, 10)),
				}),
			])
		);
	}
}
