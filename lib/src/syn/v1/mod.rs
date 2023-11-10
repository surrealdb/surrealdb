use crate::sql::Query;
use nom::Err;

mod literal;
mod part;
mod stmt;

mod block;
mod builtin;
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

pub fn query(i: &str) -> IResult<&str, Query> {
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

#[cfg(not(feature = "experimental_parser"))]
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
