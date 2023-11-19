use super::{
	error::expected,
	literal::{ident_raw, number::integer},
	value::{array, object},
	IResult,
};
use crate::sql::{id::Gen, Id, Thing};
use nom::{
	branch::alt,
	bytes::complete::tag,
	character::complete::char,
	combinator::{map, value},
	sequence::delimited,
};

pub fn thing(i: &str) -> IResult<&str, Thing> {
	expected("a thing", alt((thing_raw, thing_single, thing_double)))(i)
}

fn thing_single(i: &str) -> IResult<&str, Thing> {
	delimited(char('\''), thing_raw, char('\''))(i)
}

fn thing_double(i: &str) -> IResult<&str, Thing> {
	delimited(char('\"'), thing_raw, char('\"'))(i)
}

pub fn thing_raw(i: &str) -> IResult<&str, Thing> {
	let (i, t) = ident_raw(i)?;
	let (i, _) = char(':')(i)?;
	let (i, v) = alt((
		value(Id::Generate(Gen::Rand), tag("rand()")),
		value(Id::Generate(Gen::Ulid), tag("ulid()")),
		value(Id::Generate(Gen::Uuid), tag("uuid()")),
		id,
	))(i)?;
	Ok((
		i,
		Thing {
			tb: t,
			id: v,
		},
	))
}

pub fn id(i: &str) -> IResult<&str, Id> {
	alt((
		map(integer, Id::Number),
		map(ident_raw, Id::String),
		map(object, Id::Object),
		map(array, Id::Array),
	))(i)
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::array::Array;
	use crate::sql::object::Object;
	use crate::sql::value::Value;

	#[test]
	fn thing_normal() {
		let sql = "test:id";
		let res = thing(sql);
		let out = res.unwrap().1;
		assert_eq!("test:id", format!("{}", out));
		assert_eq!(
			out,
			Thing {
				tb: String::from("test"),
				id: Id::from("id"),
			}
		);
	}

	#[test]
	fn thing_integer() {
		let sql = "test:001";
		let res = thing(sql);
		let out = res.unwrap().1;
		assert_eq!("test:1", format!("{}", out));
		assert_eq!(
			out,
			Thing {
				tb: String::from("test"),
				id: Id::from(1),
			}
		);
	}

	#[test]
	fn thing_quoted_backtick() {
		let sql = "`test`:`id`";
		let res = thing(sql);
		let out = res.unwrap().1;
		assert_eq!("test:id", format!("{}", out));
		assert_eq!(
			out,
			Thing {
				tb: String::from("test"),
				id: Id::from("id"),
			}
		);
	}

	#[test]
	fn thing_quoted_brackets() {
		let sql = "⟨test⟩:⟨id⟩";
		let res = thing(sql);
		let out = res.unwrap().1;
		assert_eq!("test:id", format!("{}", out));
		assert_eq!(
			out,
			Thing {
				tb: String::from("test"),
				id: Id::from("id"),
			}
		);
	}

	#[test]
	fn thing_object() {
		let sql = "test:{ location: 'GBR', year: 2022 }";
		let res = thing(sql);
		let out = res.unwrap().1;
		assert_eq!("test:{ location: 'GBR', year: 2022 }", format!("{}", out));
		assert_eq!(
			out,
			Thing {
				tb: String::from("test"),
				id: Id::Object(Object::from(map! {
					"location".to_string() => Value::from("GBR"),
					"year".to_string() => Value::from(2022),
				})),
			}
		);
	}

	#[test]
	fn thing_array() {
		let sql = "test:['GBR', 2022]";
		let res = thing(sql);
		let out = res.unwrap().1;
		assert_eq!("test:['GBR', 2022]", format!("{}", out));
		assert_eq!(
			out,
			Thing {
				tb: String::from("test"),
				id: Id::Array(Array::from(vec![Value::from("GBR"), Value::from(2022)])),
			}
		);
	}

	#[test]
	fn id_int() {
		let sql = "001";
		let res = id(sql);
		let out = res.unwrap().1;
		assert_eq!(Id::from(1), out);
		assert_eq!("1", format!("{}", out));
	}

	#[test]
	fn id_number() {
		let sql = "100";
		let res = id(sql);
		let out = res.unwrap().1;
		assert_eq!(Id::from(100), out);
		assert_eq!("100", format!("{}", out));
	}

	#[test]
	fn id_string() {
		let sql = "test";
		let res = id(sql);
		let out = res.unwrap().1;
		assert_eq!(Id::from("test"), out);
		assert_eq!("test", format!("{}", out));
	}

	#[test]
	fn id_numeric() {
		let sql = "⟨100⟩";
		let res = id(sql);
		let out = res.unwrap().1;
		assert_eq!(Id::from("100"), out);
		assert_eq!("⟨100⟩", format!("{}", out));
	}

	#[test]
	fn id_either() {
		let sql = "100test";
		let res = id(sql);
		let out = res.unwrap().1;
		assert_eq!(Id::from("100test"), out);
		assert_eq!("100test", format!("{}", out));
	}
}
