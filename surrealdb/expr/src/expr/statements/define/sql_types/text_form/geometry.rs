//! The PostgreSQL text forms of the geometric types (ADR 0014 D6): `point`, `lseg`, `path`,
//! `polygon`, `box`, `line`, and `circle`.
//!
//! Every function is pure. A form is written from plain coordinates and read to them, so the
//! caller maps the coordinates to and from geometry values. The forms are the ones PostgreSQL
//! prints and reads.

use anyhow::{Result, bail};

/// A point as its `x` and `y` coordinates.
pub type Point = (f64, f64);

/// Formats a `float8` as PostgreSQL prints it: the shortest digits that read back exactly, in
/// plain notation for decimal exponents from -4 to 14, and in `1e+15` or `1e-05` form outside
/// that range. `NaN`, `Infinity`, and `-Infinity` are spelled as PostgreSQL spells them.
pub fn format_float(value: f64) -> String {
	if value.is_nan() {
		return "NaN".to_string();
	}
	if value.is_infinite() {
		return if value > 0.0 {
			"Infinity"
		} else {
			"-Infinity"
		}
		.to_string();
	}
	if value == 0.0 {
		return if value.is_sign_negative() {
			"-0"
		} else {
			"0"
		}
		.to_string();
	}
	let scientific = format!("{value:e}");
	let Some((mantissa, exponent)) = scientific.split_once('e') else {
		return format!("{value}");
	};
	let Ok(exponent) = exponent.parse::<i32>() else {
		return format!("{value}");
	};
	if (-4..15).contains(&exponent) {
		format!("{value}")
	} else {
		let sign = if exponent < 0 {
			'-'
		} else {
			'+'
		};
		format!("{mantissa}e{sign}{:02}", exponent.unsigned_abs())
	}
}

/// The text form of a point: `(x,y)`.
pub fn format_point(point: Point) -> String {
	format!("({},{})", format_float(point.0), format_float(point.1))
}

/// The text form of a line segment: `[(x1,y1),(x2,y2)]`.
pub fn format_lseg(start: Point, end: Point) -> String {
	format!("[{},{}]", format_point(start), format_point(end))
}

/// The text form of a path: `[(x,y),...]` when open, `((x,y),...)` when closed.
pub fn format_path(points: &[Point], closed: bool) -> String {
	let list = format_point_list(points);
	if closed {
		format!("({list})")
	} else {
		format!("[{list}]")
	}
}

/// The text form of a polygon: `((x,y),...)`.
pub fn format_polygon(points: &[Point]) -> String {
	format!("({})", format_point_list(points))
}

/// The text form of a box: the high corner, then the low corner, with no outer parentheses:
/// `(x2,y2),(x1,y1)`.
pub fn format_box(high: Point, low: Point) -> String {
	format!("{},{}", format_point(high), format_point(low))
}

/// The text form of a line `A x + B y + C = 0`: `{A,B,C}`.
pub fn format_line(a: f64, b: f64, c: f64) -> String {
	format!("{{{},{},{}}}", format_float(a), format_float(b), format_float(c))
}

/// The text form of a circle: `<(x,y),r>`.
pub fn format_circle(center: Point, radius: f64) -> String {
	format!("<{},{}>", format_point(center), format_float(radius))
}

fn format_point_list(points: &[Point]) -> String {
	points.iter().map(|point| format_point(*point)).collect::<Vec<_>>().join(",")
}

/// Parses the text form of a point, `(x,y)`. The parentheses may be left out.
pub fn parse_point(text: &str) -> Result<Point> {
	let trimmed = text.trim();
	let inner = match trimmed.strip_prefix('(').and_then(|rest| rest.strip_suffix(')')) {
		Some(inner) => inner,
		None => trimmed,
	};
	parse_pair(inner)
}

/// Parses the text form of a line segment: two points in `[...]` or `(...)`, or bare.
pub fn parse_lseg(text: &str) -> Result<(Point, Point)> {
	let points = parse_points(text)?;
	match points.as_slice() {
		[start, end] => Ok((*start, *end)),
		_ => bail!("a line segment has two points, found {}", points.len()),
	}
}

/// Parses the text form of a path. A path written with `[` is open and one written with `(` is
/// closed; a bare list is closed. Returns the points and whether the path is closed.
pub fn parse_path(text: &str) -> Result<(Vec<Point>, bool)> {
	let closed = !text.trim_start().starts_with('[');
	let points = parse_points(text)?;
	if points.is_empty() {
		bail!("a path has at least one point");
	}
	Ok((points, closed))
}

/// Parses the text form of a polygon: its points in `(...)`, or bare.
pub fn parse_polygon(text: &str) -> Result<Vec<Point>> {
	let points = parse_points(text)?;
	if points.is_empty() {
		bail!("a polygon has at least one point");
	}
	Ok(points)
}

/// Parses the text form of a box: two corners, with or without outer parentheses. Returns the
/// corners in the order written; the caller takes the high and low corners from them.
pub fn parse_box(text: &str) -> Result<(Point, Point)> {
	let points = parse_points(text)?;
	match points.as_slice() {
		[first, second] => Ok((*first, *second)),
		_ => bail!("a box has two corners, found {}", points.len()),
	}
}

/// Parses the text form of a line `{A,B,C}`.
pub fn parse_line(text: &str) -> Result<(f64, f64, f64)> {
	let inner = text
		.trim()
		.strip_prefix('{')
		.and_then(|rest| rest.strip_suffix('}'))
		.ok_or_else(|| anyhow::anyhow!("a line is written {{A,B,C}}: {text:?}"))?;
	let parts = inner.split(',').map(parse_number).collect::<Result<Vec<f64>>>()?;
	match parts.as_slice() {
		[a, b, c] => Ok((*a, *b, *c)),
		_ => bail!("a line has three coefficients, found {}", parts.len()),
	}
}

/// Parses the text form of a circle `<(x,y),r>`, with the angle brackets or the parentheses of
/// PostgreSQL's alternative `((x,y),r)` form.
pub fn parse_circle(text: &str) -> Result<(Point, f64)> {
	let trimmed = text.trim();
	let inner = trimmed
		.strip_prefix('<')
		.and_then(|rest| rest.strip_suffix('>'))
		.or_else(|| trimmed.strip_prefix('(').and_then(|rest| rest.strip_suffix(')')))
		.unwrap_or(trimmed);
	let (center, radius) = inner
		.rsplit_once(',')
		.ok_or_else(|| anyhow::anyhow!("a circle is written <(x,y),r>: {text:?}"))?;
	Ok((parse_point(center)?, parse_number(radius)?))
}

/// The points of a list, accepted bare or wrapped in one pair of `[...]` or `(...)`.
fn parse_points(text: &str) -> Result<Vec<Point>> {
	let trimmed = text.trim();
	match parse_point_list(trimmed) {
		Ok(points) => Ok(points),
		Err(bare) => {
			let inner = trimmed
				.strip_prefix('[')
				.and_then(|rest| rest.strip_suffix(']'))
				.or_else(|| trimmed.strip_prefix('(').and_then(|rest| rest.strip_suffix(')')));
			match inner {
				Some(inner) => parse_point_list(inner.trim()),
				None => Err(bare),
			}
		}
	}
}

/// Parses `(x,y),(x,y),...` with no outer delimiters. An empty text is an empty list.
fn parse_point_list(text: &str) -> Result<Vec<Point>> {
	let mut points = Vec::new();
	let mut rest = text.trim();
	while !rest.is_empty() {
		let Some(after_open) = rest.strip_prefix('(') else {
			bail!("expected a point in parentheses: {text:?}");
		};
		let Some(close) = after_open.find(')') else {
			bail!("a point is not closed: {text:?}");
		};
		points.push(parse_pair(&after_open[..close])?);
		rest = after_open[close + 1..].trim_start();
		if let Some(next) = rest.strip_prefix(',') {
			rest = next.trim_start();
			if rest.is_empty() {
				bail!("a list ends with a comma: {text:?}");
			}
		} else if !rest.is_empty() {
			bail!("expected a comma between points: {text:?}");
		}
	}
	Ok(points)
}

/// Parses `x,y` into its two coordinates.
fn parse_pair(text: &str) -> Result<Point> {
	let Some((x, y)) = text.split_once(',') else {
		bail!("a point is written x,y: {text:?}");
	};
	Ok((parse_number(x)?, parse_number(y)?))
}

fn parse_number(text: &str) -> Result<f64> {
	let trimmed = text.trim();
	match trimmed.parse::<f64>() {
		Ok(value) => Ok(value),
		Err(_) => bail!("not a number: {trimmed:?}"),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn floats_print_as_postgresql_prints_them() {
		assert_eq!(format_float(1.0), "1");
		assert_eq!(format_float(-2.5), "-2.5");
		assert_eq!(format_float(0.0), "0");
		assert_eq!(format_float(-0.0), "-0");
		assert_eq!(format_float(0.1 + 0.2), "0.30000000000000004");
		assert_eq!(format_float(0.0001), "0.0001");
		assert_eq!(format_float(0.00001), "1e-05");
		assert_eq!(format_float(1e14), "100000000000000");
		assert_eq!(format_float(1e15), "1e+15");
		assert_eq!(format_float(1.5e20), "1.5e+20");
		assert_eq!(format_float(f64::NAN), "NaN");
		assert_eq!(format_float(f64::NEG_INFINITY), "-Infinity");
	}

	#[test]
	fn text_forms_match_postgresql() {
		assert_eq!(format_point((1.0, 2.0)), "(1,2)");
		assert_eq!(format_lseg((1.0, 2.0), (3.0, 4.0)), "[(1,2),(3,4)]");
		assert_eq!(format_path(&[(1.0, 2.0), (3.0, 4.0)], false), "[(1,2),(3,4)]");
		assert_eq!(format_path(&[(0.0, 0.0), (1.0, 1.0)], true), "((0,0),(1,1))");
		assert_eq!(format_polygon(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0)]), "((0,0),(1,0),(1,1))");
		assert_eq!(format_box((3.0, 4.0), (0.0, 0.0)), "(3,4),(0,0)");
		assert_eq!(format_line(1.0, 2.0, 3.0), "{1,2,3}");
		assert_eq!(format_circle((1.0, 2.0), 3.0), "<(1,2),3>");
	}

	#[test]
	fn text_forms_round_trip() {
		assert_eq!(parse_point("(1.5,-2)").unwrap(), (1.5, -2.0));
		assert_eq!(parse_point(" 4 , 5 ").unwrap(), (4.0, 5.0));
		assert_eq!(parse_lseg("[(1,2),(3,4)]").unwrap(), ((1.0, 2.0), (3.0, 4.0)));
		assert_eq!(parse_lseg("(1,2),(3,4)").unwrap(), ((1.0, 2.0), (3.0, 4.0)));
		assert_eq!(parse_path("[(1,2),(3,4)]").unwrap(), (vec![(1.0, 2.0), (3.0, 4.0)], false));
		assert_eq!(parse_path("((0,0),(1,1))").unwrap(), (vec![(0.0, 0.0), (1.0, 1.0)], true));
		assert_eq!(parse_polygon("((0,0),(1,0),(1,1))").unwrap().len(), 3);
		assert_eq!(parse_box("(3,4),(0,0)").unwrap(), ((3.0, 4.0), (0.0, 0.0)));
		assert_eq!(parse_box("((3,4),(0,0))").unwrap(), ((3.0, 4.0), (0.0, 0.0)));
		assert_eq!(parse_line("{1,2,3}").unwrap(), (1.0, 2.0, 3.0));
		assert_eq!(parse_circle("<(1,2),3>").unwrap(), ((1.0, 2.0), 3.0));
		assert_eq!(parse_circle("((1,2),3)").unwrap(), ((1.0, 2.0), 3.0));
	}

	#[test]
	fn text_forms_refuse_malformed_input() {
		assert!(parse_point("(1)").is_err());
		assert!(parse_point("(1,x)").is_err());
		assert!(parse_lseg("[(1,2)]").is_err());
		assert!(parse_lseg("[(1,2),(3,4),(5,6)]").is_err());
		assert!(parse_path("[]").is_err());
		assert!(parse_path("[(1,2),(3,4)").is_err());
		assert!(parse_polygon("((0,0),(1,0),)").is_err());
		assert!(parse_box("(1,2)").is_err());
		assert!(parse_line("1,2,3").is_err());
		assert!(parse_line("{1,2}").is_err());
		assert!(parse_circle("(1,2)").is_err());
	}
}
