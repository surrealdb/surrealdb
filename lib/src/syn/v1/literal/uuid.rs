use super::super::{common::is_hex, IResult};
use crate::sql::Uuid;
use nom::{
	branch::alt,
	bytes::complete::take_while_m_n,
	character::complete::char,
	combinator::recognize,
	sequence::{delimited, tuple},
};

pub fn uuid(i: &str) -> IResult<&str, Uuid> {
	alt((uuid_single, uuid_double))(i)
}

fn uuid_single(i: &str) -> IResult<&str, Uuid> {
	delimited(char('\''), uuid_raw, char('\''))(i)
}

fn uuid_double(i: &str) -> IResult<&str, Uuid> {
	delimited(char('\"'), uuid_raw, char('\"'))(i)
}

fn uuid_raw(i: &str) -> IResult<&str, Uuid> {
	let (i, v) = recognize(tuple((
		take_while_m_n(8, 8, is_hex),
		char('-'),
		take_while_m_n(4, 4, is_hex),
		char('-'),
		alt((
			char('1'),
			char('2'),
			char('3'),
			char('4'),
			char('5'),
			char('6'),
			char('7'),
			char('8'),
		)),
		take_while_m_n(3, 3, is_hex),
		char('-'),
		take_while_m_n(4, 4, is_hex),
		char('-'),
		take_while_m_n(12, 12, is_hex),
	)))(i)?;
	Ok((i, Uuid::try_from(v).unwrap()))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn uuid_v1() {
		let sql = "e72bee20-f49b-11ec-b939-0242ac120002";
		let res = uuid_raw(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("'e72bee20-f49b-11ec-b939-0242ac120002'", format!("{}", out));
		assert_eq!(out, Uuid::try_from("e72bee20-f49b-11ec-b939-0242ac120002").unwrap());
	}

	#[test]
	fn uuid_v4() {
		let sql = "b19bc00b-aa98-486c-ae37-c8e1c54295b1";
		let res = uuid_raw(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("'b19bc00b-aa98-486c-ae37-c8e1c54295b1'", format!("{}", out));
		assert_eq!(out, Uuid::try_from("b19bc00b-aa98-486c-ae37-c8e1c54295b1").unwrap());
	}
}
