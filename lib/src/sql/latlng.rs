use crate::sql::comment::mightbespace;
use crate::sql::error::{Error, IResult};
use crate::sql::geometry::{DOUBLE, SINGLE};
use crate::sql::number::integer;
use crate::sql::Operator::Dec;
use geo::Point;
use nom::branch::alt;
use nom::character::complete::i64;
use nom::character::streaming::char;
use nom::combinator::map;
use nom::complete::tag;
use nom::error::ParseError;
use nom::number::complete::double;
use nom::sequence::{delimited, tuple};
use nom::Parser;
use std::fmt;
use std::fmt::Formatter;
use std::ops::Mul;
use std::rc::Rc;

/// Indicates one of four directions used for geographic orientation.
/// North and South are vertical, East and West are horizontal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CardinalDirection {
	North,
	South,
	East,
	West,
}

/// In terms of latitude and longitude, the following rules apply:
/// - North and East are positive
/// - South and West are negative
///
/// This implementation allows callers to multiply a floating point
/// number by an associated [`CardinalDirection`] in order to produce
/// a signed value representing the correct latitude or longitude.
impl Mul<CardinalDirection> for f64 {
	type Output = f64;

	fn mul(self, rhs: CardinalDirection) -> Self::Output {
		match rhs {
			CardinalDirection::North => self,
			CardinalDirection::South => self * -1.0,
			CardinalDirection::East => self,
			CardinalDirection::West => self * -1.0,
		}
	}
}

/// Represents a degree of latitude or longitude and an associated
/// [`CardinalDirection`]. For example "N 40.446°" and "W 79.982°"
/// are both representable by this type.
#[derive(Debug, Clone, Copy, PartialEq)]
struct CardinalDegree(CardinalDirection, f64);

impl CardinalDegree {
	fn value(&self) -> f64 {
		self.1
	}

	fn direction(&self) -> CardinalDirection {
		self.0
	}
}

impl From<(f64, f64, f64, CardinalDirection)> for CardinalDegree {
	fn from(value: (f64, f64, f64, CardinalDirection)) -> Self {
		let (degrees, minutes, seconds, direction) = value;
		CardinalDegree(direction, degrees + minutes / 60.0 + seconds / 3600.0)
	}
}

/// Represents a latitude and longitude in decimal degrees.
/// https://en.wikipedia.org/wiki/Decimal_degrees
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct DecimalDegrees {
	lat: CardinalDegree,
	lng: CardinalDegree,
}

impl From<(CardinalDegree, CardinalDegree)> for DecimalDegrees {
	fn from(value: (CardinalDegree, CardinalDegree)) -> Self {
		DecimalDegrees {
			lat: value.0,
			lng: value.1,
		}
	}
}

impl Into<Point> for DecimalDegrees {
	/// Converts the decimal degrees into a [`Point`] by taking into account
	/// the direction of each degree.
	fn into(self) -> Point {
		Point::new(self.lng.value() * self.lng.direction(), self.lat.value() * self.lat.direction())
	}
}

/// Parses vertical directions (North or South). Callers may want to
/// parse vertical separate from horizontal because you can't parse
/// two vertical directions, i.e. N 0.00° S 0.00° is invalid.
fn vertical_dir(input: &str) -> IResult<&str, CardinalDirection> {
	let (i, direction) = alt((
		map(char('N'), |_| CardinalDirection::North),
		map(char('S'), |_| CardinalDirection::South),
	))(input)?;
	Ok((i, direction))
}

/// Parses horizontal directions (East or West). Callers may want to
/// parse horizontal separate from vertical because you can't parse
/// two horizontal directions, i.e. E 0.00° W 0.00° is invalid.
fn horizontal_dir(input: &str) -> IResult<&str, CardinalDirection> {
	let (i, direction) = alt((
		map(char('E'), |_| CardinalDirection::East),
		map(char('W'), |_| CardinalDirection::West),
	))(input)?;
	Ok((i, direction))
}

/// Parses a floating point number, optionally ending in a ° symbol.
fn degree(input: &str) -> IResult<&str, f64> {
	let (i, degrees) = double(input)?;
	let (i, _) = char('°')(i)?;
	Ok((i, degrees))
}

/// Parses a degree cardinal (e.g. 40.6892°N) or a cardinal degree (e.g. N40.6892°).
/// The direction can either be before or after the degree. Callers should provide the
/// [`CardinalDirection`] parser. This is required because if the caller has already
/// parsed the vertical direction, they shouldn't be able to parse the horizontal
/// direction.
fn cardinal_degree<'i: 't, 't>(
	i: &'i str,
	direction: fn(input: &str) -> IResult<&str, CardinalDirection>,
) -> IResult<&'i str, CardinalDegree, Error<&'i str>> {
	alt((
		map(tuple((direction, mightbespace, degree)), |(dir, _, deg)| CardinalDegree(dir, deg)),
		map(tuple((degree, direction)), |(deg, dir)| CardinalDegree(dir, deg)),
	))(i)
}

fn dms_components(i: &str) -> IResult<&str, (f64, f64, f64)> {
	let (i, degrees) = degree(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, minutes) = double(i)?;
	let (i, _) = char(SINGLE)(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, seconds) = double(i)?;
	let (i, _) = char(DOUBLE)(i)?;
	Ok((i, (degrees, minutes, seconds)))
}

/// Parses a single [`CardinalDegree`] in DMS format. The following formats are supported.
/// - 40°60'3600"N
/// - 40.0°60.0'3600.0"N
/// - 40° 60' 3600" N
/// - N40°60'3600"
/// - N 40° 60' 3600"
fn dms_dir(
	i: &str,
	direction: fn(input: &str) -> IResult<&str, CardinalDirection>,
) -> IResult<&str, CardinalDegree> {
	alt((
		map(
			tuple((mightbespace, direction, mightbespace, dms_components)),
			|(_, dir, _, (deg, min, sec))| (deg, min, sec, dir).into(),
		),
		map(
			tuple((mightbespace, dms_components, mightbespace, direction)),
			|(_, (deg, min, sec), _, dir)| (deg, min, sec, dir).into(),
		),
	))(i)
}

/// Parses a latitude or longitude represented in decimal degrees.
/// The following formats are supported.
///
/// - 40.6892°N 74.0445°W
/// - N40.6892° W74.0445°
/// - N 40.6892° W 74.0445°
pub(crate) fn decimal_degree(i: &str) -> IResult<&str, DecimalDegrees> {
	let (i, vertical) = cardinal_degree(i, vertical_dir)?;
	let (i, _) = mightbespace(i)?;
	let (i, horizontal) = cardinal_degree(i, horizontal_dir)?;
	Ok((i, (vertical, horizontal).into()))
}

/// Parses a latitude or longitude represented in DMS format.
pub(crate) fn dms(i: &str) -> IResult<&str, DecimalDegrees> {
	let (i, vertical) = dms_dir(i, vertical_dir)?;
	let (i, _) = mightbespace(i)?;
	let (i, horizontal) = dms_dir(i, horizontal_dir)?;
	(Ok((i, (vertical, horizontal).into())))
}

#[cfg(test)]
mod tests {
	use crate::sql::latlng::*;

	impl fmt::Display for CardinalDirection {
		fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
			match self {
				CardinalDirection::North => write!(f, "N"),
				CardinalDirection::South => write!(f, "S"),
				CardinalDirection::East => write!(f, "E"),
				CardinalDirection::West => write!(f, "W"),
			}
		}
	}

	impl fmt::Display for CardinalDegree {
		fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
			// todo: precision?
			write!(f, "{:.2}°{}", self.1, self.0)
		}
	}

	impl fmt::Display for DecimalDegrees {
		fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
			write!(f, "{} {}", self.lat, self.lng)
		}
	}

	fn example_dd() -> DecimalDegrees {
		DecimalDegrees {
			lat: CardinalDegree(CardinalDirection::North, 40.6892),
			lng: CardinalDegree(CardinalDirection::West, 74.0445),
		}
	}

	#[test]
	fn test_display_decimal_degrees() {
		assert_eq!("40.69°N 74.04°W", &format!("{}", example_dd()));
	}

	#[test]
	fn test_decimal_degrees_to_point() {
		let point: Point<f64> = example_dd().into();
		assert_eq!(point.x(), -74.0445);
		assert_eq!(point.y(), 40.6892);
	}

	#[test]
	fn test_parse_cardinal_degree() {
		let parse_vertical = |input: &str, expect: &str| {
			let (_, dd) = cardinal_degree(input, vertical_dir).unwrap();
			assert_eq!(format!("{}", dd), expect);
		};
		let parse_horizontal = |input: &str, expect: &str| {
			let (_, dd) = cardinal_degree(input, horizontal_dir).unwrap();
			assert_eq!(format!("{}", dd), expect);
		};

		parse_vertical("40.6892°N", "40.69°N");
		parse_vertical("N 40.6892°", "40.69°N");
		parse_vertical("N40.6892°", "40.69°N");

		parse_horizontal("40.6892°E", "40.69°E");
		parse_horizontal("E 40.6892°", "40.69°E");
		parse_horizontal("E40.6892°", "40.69°E");
	}

	#[test]
	fn test_parse_decimal_degrees() {
		let dd = example_dd();
		assert_eq!(decimal_degree("40.6892°N 74.0445°W").unwrap().1, dd);
		assert_eq!(decimal_degree("N40.6892° W74.0445°").unwrap().1, dd);
		assert_eq!(decimal_degree("N 40.6892° W 74.0445°").unwrap().1, dd);
	}

	#[test]
	fn test_parse_dms() {
		let dd = DecimalDegrees {
			lat: CardinalDegree(CardinalDirection::North, 42.0),
			lng: CardinalDegree(CardinalDirection::West, 81.0),
		};
		assert_eq!(dms(r#"40°60'3600"N 79°60'3600"W"#).unwrap().1, dd);
		assert_eq!(dms(r#"40.0°60.0'3600.0"N 79.0°60.0'3600.0"W"#).unwrap().1, dd);
		assert_eq!(dms(r#"40° 60' 3600" N 79° 60' 3600" W"#).unwrap().1, dd);
		assert_eq!(dms(r#"N40°60'3600" W79°60'3600""#).unwrap().1, dd);
		assert_eq!(dms(r#"N 40° 60' 3600" W 79° 60' 3600""#).unwrap().1, dd);
	}
}
