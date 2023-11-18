use super::super::{
	comment::mightbespace,
	common::{
		closebraces, closebracket, closeparentheses, commas, delimited_list0, delimited_list1,
		openbraces, openbracket, openparentheses,
	},
	depth, IResult,
};
use crate::sql::Geometry;
use geo::{LineString, Point, Polygon};
use nom::{
	branch::alt,
	bytes::complete::tag,
	character::complete::char,
	combinator::opt,
	number::complete::double,
	sequence::{delimited, preceded, terminated},
};

pub fn geometry(i: &str) -> IResult<&str, Geometry> {
	let _diving = depth::dive(i)?;
	alt((simple, normal))(i)
}

fn simple(i: &str) -> IResult<&str, Geometry> {
	let (i, _) = openparentheses(i)?;
	let (i, x) = double(i)?;
	let (i, _) = commas(i)?;
	let (i, y) = double(i)?;
	let (i, _) = closeparentheses(i)?;
	Ok((i, Geometry::Point((x, y).into())))
}

fn normal(i: &str) -> IResult<&str, Geometry> {
	let (i, _) = openbraces(i)?;
	let (i, v) = alt((point, line, polygon, multipoint, multiline, multipolygon, collection))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = opt(char(','))(i)?;
	let (i, _) = closebraces(i)?;
	Ok((i, v))
}

fn point(i: &str) -> IResult<&str, Geometry> {
	let (i, v) = alt((
		|i| {
			let (i, _) = preceded(key_type, point_type)(i)?;
			let (i, _) = commas(i)?;
			let (i, v) = preceded(key_vals, point_vals)(i)?;
			Ok((i, v))
		},
		|i| {
			let (i, v) = preceded(key_vals, point_vals)(i)?;
			let (i, _) = commas(i)?;
			let (i, _) = preceded(key_type, point_type)(i)?;
			Ok((i, v))
		},
	))(i)?;
	Ok((i, v.into()))
}

fn line(i: &str) -> IResult<&str, Geometry> {
	let (i, v) = alt((
		|i| {
			let (i, _) = preceded(key_type, line_type)(i)?;
			let (i, _) = commas(i)?;
			let (i, v) = preceded(key_vals, line_vals)(i)?;
			Ok((i, v))
		},
		|i| {
			let (i, v) = preceded(key_vals, line_vals)(i)?;
			let (i, _) = commas(i)?;
			let (i, _) = preceded(key_type, line_type)(i)?;
			Ok((i, v))
		},
	))(i)?;
	Ok((i, v.into()))
}

fn polygon(i: &str) -> IResult<&str, Geometry> {
	let (i, v) = alt((
		|i| {
			let (i, _) = preceded(key_type, polygon_type)(i)?;
			let (i, _) = commas(i)?;
			let (i, v) = preceded(key_vals, polygon_vals)(i)?;
			Ok((i, v))
		},
		|i| {
			let (i, v) = preceded(key_vals, polygon_vals)(i)?;
			let (i, _) = commas(i)?;
			let (i, _) = preceded(key_type, polygon_type)(i)?;
			Ok((i, v))
		},
	))(i)?;
	Ok((i, v.into()))
}

fn multipoint(i: &str) -> IResult<&str, Geometry> {
	let (i, v) = alt((
		|i| {
			let (i, _) = preceded(key_type, multipoint_type)(i)?;
			let (i, _) = commas(i)?;
			let (i, v) = preceded(key_vals, multipoint_vals)(i)?;
			Ok((i, v))
		},
		|i| {
			let (i, v) = preceded(key_vals, multipoint_vals)(i)?;
			let (i, _) = commas(i)?;
			let (i, _) = preceded(key_type, multipoint_type)(i)?;
			Ok((i, v))
		},
	))(i)?;
	Ok((i, v.into()))
}

fn multiline(i: &str) -> IResult<&str, Geometry> {
	let (i, v) = alt((
		|i| {
			let (i, _) = preceded(key_type, multiline_type)(i)?;
			let (i, _) = commas(i)?;
			let (i, v) = preceded(key_vals, multiline_vals)(i)?;
			Ok((i, v))
		},
		|i| {
			let (i, v) = preceded(key_vals, multiline_vals)(i)?;
			let (i, _) = commas(i)?;
			let (i, _) = preceded(key_type, multiline_type)(i)?;
			Ok((i, v))
		},
	))(i)?;
	Ok((i, v.into()))
}

fn multipolygon(i: &str) -> IResult<&str, Geometry> {
	let (i, v) = alt((
		|i| {
			let (i, _) = preceded(key_type, multipolygon_type)(i)?;
			let (i, _) = commas(i)?;
			let (i, v) = preceded(key_vals, multipolygon_vals)(i)?;
			Ok((i, v))
		},
		|i| {
			let (i, v) = preceded(key_vals, multipolygon_vals)(i)?;
			let (i, _) = commas(i)?;
			let (i, _) = preceded(key_type, multipolygon_type)(i)?;
			Ok((i, v))
		},
	))(i)?;
	Ok((i, v.into()))
}

fn collection(i: &str) -> IResult<&str, Geometry> {
	let (i, v) = alt((
		|i| {
			let (i, _) = preceded(key_type, collection_type)(i)?;
			let (i, _) = commas(i)?;
			let (i, v) = preceded(key_geom, collection_vals)(i)?;
			Ok((i, v))
		},
		|i| {
			let (i, v) = preceded(key_geom, collection_vals)(i)?;
			let (i, _) = commas(i)?;
			let (i, _) = preceded(key_type, collection_type)(i)?;
			Ok((i, v))
		},
	))(i)?;
	Ok((i, v.into()))
}

//
//
//

fn point_vals(i: &str) -> IResult<&str, Point<f64>> {
	let (i, v) = coordinate(i)?;
	Ok((i, v.into()))
}

fn line_vals(i: &str) -> IResult<&str, LineString<f64>> {
	let (i, v) =
		delimited_list0(openbracket, commas, terminated(coordinate, mightbespace), char(']'))(i)?;
	Ok((i, v.into()))
}

fn polygon_vals(i: &str) -> IResult<&str, Polygon<f64>> {
	let (i, mut e) =
		delimited_list1(openbracket, commas, terminated(line_vals, mightbespace), char(']'))(i)?;
	let v = e.split_off(1);
	// delimited_list1 guarantees there is atleast one value.
	let e = e.into_iter().next().unwrap();
	Ok((i, Polygon::new(e, v)))
}

fn multipoint_vals(i: &str) -> IResult<&str, Vec<Point<f64>>> {
	let (i, v) =
		delimited_list0(openbracket, commas, terminated(point_vals, mightbespace), char(']'))(i)?;
	Ok((i, v))
}

fn multiline_vals(i: &str) -> IResult<&str, Vec<LineString<f64>>> {
	let (i, v) =
		delimited_list0(openbracket, commas, terminated(line_vals, mightbespace), char(']'))(i)?;
	Ok((i, v))
}

fn multipolygon_vals(i: &str) -> IResult<&str, Vec<Polygon<f64>>> {
	let (i, v) =
		delimited_list0(openbracket, commas, terminated(polygon_vals, mightbespace), char(']'))(i)?;
	Ok((i, v))
}

fn collection_vals(i: &str) -> IResult<&str, Vec<Geometry>> {
	let (i, v) =
		delimited_list0(openbracket, commas, terminated(geometry, mightbespace), char(']'))(i)?;
	Ok((i, v))
}

//
//
//

fn coordinate(i: &str) -> IResult<&str, (f64, f64)> {
	let (i, _) = openbracket(i)?;
	let (i, x) = double(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(',')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, y) = double(i)?;
	let (i, _) = closebracket(i)?;
	Ok((i, (x, y)))
}

//
//
//

fn point_type(i: &str) -> IResult<&str, &str> {
	let (i, v) = alt((
		delimited(char('\''), tag("Point"), char('\'')),
		delimited(char('\"'), tag("Point"), char('\"')),
	))(i)?;
	Ok((i, v))
}

fn line_type(i: &str) -> IResult<&str, &str> {
	let (i, v) = alt((
		delimited(char('\''), tag("LineString"), char('\'')),
		delimited(char('\"'), tag("LineString"), char('\"')),
	))(i)?;
	Ok((i, v))
}

fn polygon_type(i: &str) -> IResult<&str, &str> {
	let (i, v) = alt((
		delimited(char('\''), tag("Polygon"), char('\'')),
		delimited(char('\"'), tag("Polygon"), char('\"')),
	))(i)?;
	Ok((i, v))
}

fn multipoint_type(i: &str) -> IResult<&str, &str> {
	let (i, v) = alt((
		delimited(char('\''), tag("MultiPoint"), char('\'')),
		delimited(char('\"'), tag("MultiPoint"), char('\"')),
	))(i)?;
	Ok((i, v))
}

fn multiline_type(i: &str) -> IResult<&str, &str> {
	let (i, v) = alt((
		delimited(char('\''), tag("MultiLineString"), char('\'')),
		delimited(char('\"'), tag("MultiLineString"), char('\"')),
	))(i)?;
	Ok((i, v))
}

fn multipolygon_type(i: &str) -> IResult<&str, &str> {
	let (i, v) = alt((
		delimited(char('\''), tag("MultiPolygon"), char('\'')),
		delimited(char('\"'), tag("MultiPolygon"), char('\"')),
	))(i)?;
	Ok((i, v))
}

fn collection_type(i: &str) -> IResult<&str, &str> {
	let (i, v) = alt((
		delimited(char('\''), tag("GeometryCollection"), char('\'')),
		delimited(char('\"'), tag("GeometryCollection"), char('\"')),
	))(i)?;
	Ok((i, v))
}

//
//
//

fn key_type(i: &str) -> IResult<&str, &str> {
	let (i, v) = alt((
		tag("type"),
		delimited(char('\''), tag("type"), char('\'')),
		delimited(char('\"'), tag("type"), char('\"')),
	))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(':')(i)?;
	let (i, _) = mightbespace(i)?;
	Ok((i, v))
}

fn key_vals(i: &str) -> IResult<&str, &str> {
	let (i, v) = alt((
		tag("coordinates"),
		delimited(char('\''), tag("coordinates"), char('\'')),
		delimited(char('\"'), tag("coordinates"), char('\"')),
	))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(':')(i)?;
	let (i, _) = mightbespace(i)?;
	Ok((i, v))
}

fn key_geom(i: &str) -> IResult<&str, &str> {
	let (i, v) = alt((
		tag("geometries"),
		delimited(char('\''), tag("geometries"), char('\'')),
		delimited(char('\"'), tag("geometries"), char('\"')),
	))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(':')(i)?;
	let (i, _) = mightbespace(i)?;
	Ok((i, v))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn simple() {
		let sql = "(-0.118092, 51.509865)";
		let res = geometry(sql);
		let out = res.unwrap().1;
		assert_eq!("(-0.118092, 51.509865)", format!("{}", out));
	}

	#[test]
	fn point() {
		let sql = r#"{
			type: 'Point',
			coordinates: [-0.118092, 51.509865]
		}"#;
		let res = geometry(sql);
		let out = res.unwrap().1;
		assert_eq!("(-0.118092, 51.509865)", format!("{}", out));
	}

	#[test]
	fn polygon_exterior() {
		let sql = r#"{
			type: 'Polygon',
			coordinates: [
				[
					[-0.38314819, 51.37692386], [0.1785278, 51.37692386],
					[0.1785278, 51.61460570], [-0.38314819, 51.61460570],
					[-0.38314819, 51.37692386]
				]
			]
		}"#;
		let res = geometry(sql);
		let out = res.unwrap().1;
		assert_eq!("{ type: 'Polygon', coordinates: [[[-0.38314819, 51.37692386], [0.1785278, 51.37692386], [0.1785278, 51.6146057], [-0.38314819, 51.6146057], [-0.38314819, 51.37692386]]] }", format!("{}", out));
	}

	#[test]
	fn polygon_interior() {
		let sql = r#"{
			type: 'Polygon',
			coordinates: [
				[
					[-0.38314819, 51.37692386], [0.1785278, 51.37692386],
					[0.1785278, 51.61460570], [-0.38314819, 51.61460570],
					[-0.38314819, 51.37692386]
				],
				[
					[-0.38314819, 51.37692386], [0.1785278, 51.37692386],
					[0.1785278, 51.61460570], [-0.38314819, 51.61460570],
					[-0.38314819, 51.37692386]
				]
			]
		}"#;
		let res = geometry(sql);
		let out = res.unwrap().1;
		assert_eq!("{ type: 'Polygon', coordinates: [[[-0.38314819, 51.37692386], [0.1785278, 51.37692386], [0.1785278, 51.6146057], [-0.38314819, 51.6146057], [-0.38314819, 51.37692386]], [[[-0.38314819, 51.37692386], [0.1785278, 51.37692386], [0.1785278, 51.6146057], [-0.38314819, 51.6146057], [-0.38314819, 51.37692386]]]] }", format!("{}", out));
	}
}
