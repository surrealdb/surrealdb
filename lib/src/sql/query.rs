use crate::sql::error::IResult;
use crate::sql::fmt::Pretty;
use crate::sql::statement::{statements, Statement, Statements};
use derive::Store;
use nom::combinator::all_consuming;
use serde::{Deserialize, Serialize};
use std::fmt::Write;
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::str;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Store, Hash)]
pub struct Query(pub Statements);

impl Deref for Query {
	type Target = Vec<Statement>;
	fn deref(&self) -> &Self::Target {
		&self.0 .0
	}
}

impl IntoIterator for Query {
	type Item = Statement;
	type IntoIter = std::vec::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl Display for Query {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(Pretty::from(f), "{}", &self.0)
	}
}

pub fn query(i: &str) -> IResult<&str, Query> {
	let (i, v) = all_consuming(statements)(i)?;
	Ok((i, Query(v)))
}

#[cfg(test)]
mod tests {

	use super::*;

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
