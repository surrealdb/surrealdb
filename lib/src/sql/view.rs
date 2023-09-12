use crate::sql::comment::shouldbespace;
use crate::sql::cond::{cond, Cond};
use crate::sql::error::IResult;
use crate::sql::field::{fields, Fields};
use crate::sql::group::{group, Groups};
use crate::sql::table::{tables, Tables};
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::combinator::{cut, opt};
use nom::sequence::preceded;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

use super::error::{expect_tag_no_case, expected};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[revisioned(revision = 1)]
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
	let select_view = |i| {
		let (i, _) = tag_no_case("SELECT")(i)?;
		cut(|i| {
			let (i, _) = shouldbespace(i)?;
			let (i, expr) = fields(i)?;
			let (i, _) = shouldbespace(i)?;
			let (i, _) = expect_tag_no_case("FROM")(i)?;
			let (i, _) = shouldbespace(i)?;
			let (i, what) = tables(i)?;
			let (i, cond) = opt(preceded(shouldbespace, cond))(i)?;
			let (i, group) = opt(preceded(shouldbespace, group))(i)?;
			Ok((i, (expr, what, cond, group)))
		})(i)
	};

	let select_view_delimited = |i| {
		let (i, _) = tag("(")(i)?;
		cut(|i| {
			let (i, res) = select_view(i)?;
			let (i, _) = tag(")")(i)?;
			Ok((i, res))
		})(i)
	};

	let (i, _) = tag_no_case("AS")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, (expr, what, cond, group)) =
		expected("SELECT or `(`", cut(alt((select_view, select_view_delimited))))(i)?;
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
		let out = res.unwrap().1;
		assert_eq!("AS SELECT * FROM test", format!("{}", out))
	}

	#[test]
	fn view_brackets() {
		let sql = "AS (SELECT * FROM test)";
		let res = view(sql);
		let out = res.unwrap().1;
		assert_eq!("AS SELECT * FROM test", format!("{}", out))
	}

	#[test]
	fn view_brackets_where() {
		let sql = "AS (SELECT temp FROM test WHERE temp IS NOT NONE)";
		let res = view(sql);
		let out = res.unwrap().1;
		assert_eq!("AS SELECT temp FROM test WHERE temp != NONE", format!("{}", out))
	}

	#[test]
	fn view_brackets_group() {
		let sql = "AS (SELECT temp FROM test WHERE temp IS NOT NONE GROUP BY temp)";
		let res = view(sql);
		let out = res.unwrap().1;
		assert_eq!("AS SELECT temp FROM test WHERE temp != NONE GROUP BY temp", format!("{}", out))
	}

	#[test]
	fn view_disallow_unbalanced_brackets() {
		let sql = "AS (SELECT temp FROM test WHERE temp IS NOT NONE GROUP BY temp";
		view(sql).unwrap_err();
		let sql = "AS SELECT temp FROM test WHERE temp IS NOT NONE GROUP BY temp)";
		let (i, _) = view(sql).unwrap();
		// The above test won't return an error since the trailing ) might be part of a another
		// pair.
		assert_eq!(i, ")");
	}
}
