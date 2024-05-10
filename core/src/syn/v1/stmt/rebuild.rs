use super::super::{comment::shouldbespace, error::expect_tag_no_case, literal::ident, IResult};
use crate::sql::statements::{RebuildIndexStatement, RebuildStatement};
use nom::{
	bytes::complete::tag_no_case,
	combinator::{cut, opt},
	sequence::tuple,
};

pub fn rebuild(i: &str) -> IResult<&str, RebuildStatement> {
	let (i, _) = tag_no_case("REBUILD")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, ix) = index(i)?;
	Ok((i, RebuildStatement::Index(ix)))
}

pub fn index(i: &str) -> IResult<&str, RebuildIndexStatement> {
	let (i, _) = tag_no_case("INDEX")(i)?;
	let (i, if_exists) =
		opt(tuple((shouldbespace, tag_no_case("IF"), shouldbespace, tag_no_case("EXISTS"))))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = cut(ident)(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = expect_tag_no_case("ON")(i)?;
	let (i, _) = opt(tuple((shouldbespace, tag_no_case("TABLE"))))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, what) = cut(ident)(i)?;
	Ok((
		i,
		RebuildIndexStatement {
			name,
			what,
			if_exists: if_exists.is_some(),
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::statements::rebuild::RebuildIndexStatement;
	use crate::sql::Ident;

	#[test]
	fn check_rebuild_serialize() {
		let stm = RebuildStatement::Index(RebuildIndexStatement {
			name: Ident::from("test"),
			what: Ident::from("test"),
			if_exists: false,
		});
		let enc: Vec<u8> = stm.into();
		assert_eq!(16, enc.len());
	}

	/// REBUILD INDEX tests

	#[test]
	fn rebuild_index() {
		let sql = "REBUILD INDEX test ON test";
		let res = rebuild(sql);
		let out = res.unwrap().1;
		assert_eq!("REBUILD INDEX test ON test", format!("{}", out))
	}

	#[test]
	fn rebuild_index_if_exists() {
		let sql = "REBUILD INDEX IF EXISTS test ON test";
		let res = rebuild(sql);
		let out = res.unwrap().1;
		assert_eq!("REBUILD INDEX IF EXISTS test ON test", format!("{}", out))
	}
}
