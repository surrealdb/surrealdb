use super::super::{
	comment::{mightbespace, shouldbespace},
	common::commas,
	idiom::plain,
	operator::assigner,
	value::value,
	IResult,
};
use crate::sql::Data;
use nom::{branch::alt, bytes::complete::tag_no_case, combinator::cut, multi::separated_list1};

pub fn data(i: &str) -> IResult<&str, Data> {
	alt((set, unset, patch, merge, replace, content))(i)
}

fn set(i: &str) -> IResult<&str, Data> {
	let (i, _) = tag_no_case("SET")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(separated_list1(
		commas,
		cut(|i| {
			let (i, l) = plain(i)?;
			let (i, _) = mightbespace(i)?;
			let (i, o) = assigner(i)?;
			let (i, _) = mightbespace(i)?;
			let (i, r) = value(i)?;
			Ok((i, (l, o, r)))
		}),
	))(i)?;
	Ok((i, Data::SetExpression(v)))
}

fn unset(i: &str) -> IResult<&str, Data> {
	let (i, _) = tag_no_case("UNSET")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(separated_list1(commas, plain))(i)?;
	Ok((i, Data::UnsetExpression(v)))
}

fn patch(i: &str) -> IResult<&str, Data> {
	let (i, _) = tag_no_case("PATCH")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(value)(i)?;
	Ok((i, Data::PatchExpression(v)))
}

fn merge(i: &str) -> IResult<&str, Data> {
	let (i, _) = tag_no_case("MERGE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(value)(i)?;
	Ok((i, Data::MergeExpression(v)))
}

fn replace(i: &str) -> IResult<&str, Data> {
	let (i, _) = tag_no_case("REPLACE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(value)(i)?;
	Ok((i, Data::ReplaceExpression(v)))
}

fn content(i: &str) -> IResult<&str, Data> {
	let (i, _) = tag_no_case("CONTENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(value)(i)?;
	Ok((i, Data::ContentExpression(v)))
}

pub fn single(i: &str) -> IResult<&str, Data> {
	let (i, v) = value(i)?;
	Ok((i, Data::SingleExpression(v)))
}

pub fn values(i: &str) -> IResult<&str, Data> {
	let (i, _) = tag_no_case("(")(i)?;
	// TODO: look at call tree here.
	let (i, fields) = separated_list1(commas, plain)(i)?;
	let (i, _) = tag_no_case(")")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("VALUES")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, values) = separated_list1(commas, |i| {
		let (i, _) = tag_no_case("(")(i)?;
		let (i, v) = separated_list1(commas, value)(i)?;
		let (i, _) = tag_no_case(")")(i)?;
		Ok((i, v))
	})(i)?;
	Ok((
		i,
		Data::ValuesExpression(
			values.into_iter().map(|row| fields.iter().cloned().zip(row).collect()).collect(),
		),
	))
}

pub fn update(i: &str) -> IResult<&str, Data> {
	let (i, _) = tag_no_case("ON DUPLICATE KEY UPDATE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = separated_list1(commas, |i| {
		let (i, l) = plain(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, o) = assigner(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, r) = value(i)?;
		Ok((i, (l, o, r)))
	})(i)?;
	Ok((i, Data::UpdateExpression(v)))
}

#[cfg(test)]
mod test {
	use super::*;

	#[test]
	fn data_set_statement() {
		let sql = "SET field = true";
		let res = data(sql);
		let out = res.unwrap().1;
		assert_eq!("SET field = true", format!("{}", out));
	}

	#[test]
	fn data_set_statement_multiple() {
		let sql = "SET field = true, other.field = false";
		let res = data(sql);
		let out = res.unwrap().1;
		assert_eq!("SET field = true, other.field = false", format!("{}", out));
	}

	#[test]
	fn data_unset_statement() {
		let sql = "UNSET field";
		let res = data(sql);
		let out = res.unwrap().1;
		assert_eq!("UNSET field", format!("{}", out));
	}

	#[test]
	fn data_unset_statement_multiple_fields() {
		let sql = "UNSET field, other.field";
		let res = data(sql);
		let out = res.unwrap().1;
		assert_eq!("UNSET field, other.field", format!("{}", out));
	}

	#[test]
	fn data_patch_statement() {
		let sql = "PATCH [{ field: true }]";
		let res = patch(sql);
		let out = res.unwrap().1;
		assert_eq!("PATCH [{ field: true }]", format!("{}", out));
	}

	#[test]
	fn data_merge_statement() {
		let sql = "MERGE { field: true }";
		let res = data(sql);
		let out = res.unwrap().1;
		assert_eq!("MERGE { field: true }", format!("{}", out));
	}

	#[test]
	fn data_content_statement() {
		let sql = "CONTENT { field: true }";
		let res = data(sql);
		let out = res.unwrap().1;
		assert_eq!("CONTENT { field: true }", format!("{}", out));
	}

	#[test]
	fn data_replace_statement() {
		let sql = "REPLACE { field: true }";
		let res = data(sql);
		let out = res.unwrap().1;
		assert_eq!("REPLACE { field: true }", format!("{}", out));
	}

	#[test]
	fn data_values_statement() {
		let sql = "(one, two, three) VALUES ($param, true, [1, 2, 3]), ($param, false, [4, 5, 6])";
		let res = values(sql);
		let out = res.unwrap().1;
		assert_eq!(
			"(one, two, three) VALUES ($param, true, [1, 2, 3]), ($param, false, [4, 5, 6])",
			format!("{}", out)
		);
	}

	#[test]
	fn data_update_statement() {
		let sql = "ON DUPLICATE KEY UPDATE field = true, other.field = false";
		let res = update(sql);
		let out = res.unwrap().1;
		assert_eq!("ON DUPLICATE KEY UPDATE field = true, other.field = false", format!("{}", out));
	}
}
