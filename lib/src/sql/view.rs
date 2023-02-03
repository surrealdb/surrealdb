use crate::sql::comment::shouldbespace;
use crate::sql::cond::{cond, Cond};
use crate::sql::error::IResult;
use crate::sql::field::{fields, Fields};
use crate::sql::group::{group, Groups};
use crate::sql::table::{tables, Tables};
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::preceded;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub struct View {
	pub expr: Fields,
	pub what: Tables,
	pub cond: Option<Cond>,
	pub group: Option<Groups>,
}

impl fmt::Display for View {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "AS SELECT {} FROM {}", self.expr, self.what)?;
		if let Some(ref v) = self.cond {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.group {
			write!(f, " {v}")?
		}
		Ok(())
	}
}

pub fn view(i: &str) -> IResult<&str, View> {
	let (i, _) = tag_no_case("AS")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = opt(tag_no_case("("))(i)?;
	let (i, _) = tag_no_case("SELECT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, expr) = fields(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("FROM")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, what) = tables(i)?;
	let (i, cond) = opt(preceded(shouldbespace, cond))(i)?;
	let (i, group) = opt(preceded(shouldbespace, group))(i)?;
	let (i, _) = opt(tag_no_case(")"))(i)?;
	Ok((
		i,
		View {
			expr,
			what,
			cond,
			group,
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn view_simple() {
		let sql = "AS SELECT * FROM test";
		let res = view(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("AS SELECT * FROM test", format!("{}", out))
	}

	#[test]
	fn view_brackets() {
		let sql = "AS (SELECT * FROM test)";
		let res = view(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("AS SELECT * FROM test", format!("{}", out))
	}

	#[test]
	fn view_brackets_where() {
		let sql = "AS (SELECT temp FROM test WHERE temp IS NOT NONE)";
		let res = view(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("AS SELECT temp FROM test WHERE temp != NONE", format!("{}", out))
	}

	#[test]
	fn view_brackets_group() {
		let sql = "AS (SELECT temp FROM test WHERE temp IS NOT NONE GROUP BY temp)";
		let res = view(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("AS SELECT temp FROM test WHERE temp != NONE GROUP BY temp", format!("{}", out))
	}
}
