use super::{
	super::{comment::shouldbespace, error::expect_tag_no_case, literal::tables, IResult},
	cond,
	field::fields,
	group,
};
use crate::{sql::View, syn::v1::error::expected};
use nom::{
	branch::alt,
	bytes::complete::{tag, tag_no_case},
	combinator::{cut, opt},
	sequence::preceded,
};

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
