use crate::sql::comment::mightbespace;
use crate::sql::common::commas;
use crate::sql::point::{point, Point};
use nom::bytes::complete::tag;
use nom::multi::separated_list1;
use nom::IResult;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Deserialize)]
pub struct Polygon {
	pub points: Vec<Point>,
}

impl fmt::Display for Polygon {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"( {} )",
			self.points.iter().map(|ref v| format!("{}", v)).collect::<Vec<_>>().join(", "),
		)
	}
}

impl Serialize for Polygon {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if serializer.is_human_readable() {
			serializer.serialize_some(&self.points)
		} else {
			let mut val = serializer.serialize_struct("Polygon", 1)?;
			val.serialize_field("points", &self.points)?;
			val.end()
		}
	}
}

pub fn polygon(i: &str) -> IResult<&str, Polygon> {
	let (i, _) = tag("(")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = separated_list1(commas, point)(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag(")")(i)?;
	Ok((
		i,
		Polygon {
			points: v,
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn polygon_simple() {
		let sql = "( (0, 0), (0, 0), (0, 0) )";
		let res = polygon(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("( (0, 0), (0, 0), (0, 0) )", format!("{}", out));
	}

	#[test]
	fn polygon_complex() {
		let sql = "( (51.509865, -0.118092), (51.509865, -0.118092), (51.509865, -0.118092) )";
		let res = polygon(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			"( (51.509865, -0.118092), (51.509865, -0.118092), (51.509865, -0.118092) )",
			format!("{}", out)
		);
	}
}
