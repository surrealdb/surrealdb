use crate::sql::comment::mightbespace;
use crate::sql::number::{number, Number};
use nom::bytes::complete::tag;
use nom::IResult;
use serde::ser::SerializeSeq;
use serde::ser::SerializeTupleStruct;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Deserialize)]
pub struct Point(Number, Number);

impl fmt::Display for Point {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "({}, {})", self.0, self.1)
	}
}

impl Serialize for Point {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if serializer.is_human_readable() {
			let mut arr = serializer.serialize_seq(Some(2))?;
			arr.serialize_element(&self.0)?;
			arr.serialize_element(&self.1)?;
			arr.end()
		} else {
			let mut val = serializer.serialize_tuple_struct("Point", 2)?;
			val.serialize_field(&self.0)?;
			val.serialize_field(&self.1)?;
			val.end()
		}
	}
}

pub fn point(i: &str) -> IResult<&str, Point> {
	let (i, _) = tag("(")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, lat) = number(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag(",")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, lng) = number(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag(")")(i)?;
	Ok((i, Point(lat, lng)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn point_simple() {
		let sql = "(0, 0)";
		let res = point(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("(0, 0)", format!("{}", out));
	}

	#[test]
	fn point_complex() {
		let sql = "(51.509865, -0.118092)";
		let res = point(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("(51.509865, -0.118092)", format!("{}", out));
	}
}
