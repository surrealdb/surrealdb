pub fn geometry(i: &str) -> IResult<&str, Geometry> {
	let _diving = crate::sql::depth::dive(i)?;
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
		delimited(char(SINGLE), tag("Point"), char(SINGLE)),
		delimited(char(DOUBLE), tag("Point"), char(DOUBLE)),
	))(i)?;
	Ok((i, v))
}

fn line_type(i: &str) -> IResult<&str, &str> {
	let (i, v) = alt((
		delimited(char(SINGLE), tag("LineString"), char(SINGLE)),
		delimited(char(DOUBLE), tag("LineString"), char(DOUBLE)),
	))(i)?;
	Ok((i, v))
}

fn polygon_type(i: &str) -> IResult<&str, &str> {
	let (i, v) = alt((
		delimited(char(SINGLE), tag("Polygon"), char(SINGLE)),
		delimited(char(DOUBLE), tag("Polygon"), char(DOUBLE)),
	))(i)?;
	Ok((i, v))
}

fn multipoint_type(i: &str) -> IResult<&str, &str> {
	let (i, v) = alt((
		delimited(char(SINGLE), tag("MultiPoint"), char(SINGLE)),
		delimited(char(DOUBLE), tag("MultiPoint"), char(DOUBLE)),
	))(i)?;
	Ok((i, v))
}

fn multiline_type(i: &str) -> IResult<&str, &str> {
	let (i, v) = alt((
		delimited(char(SINGLE), tag("MultiLineString"), char(SINGLE)),
		delimited(char(DOUBLE), tag("MultiLineString"), char(DOUBLE)),
	))(i)?;
	Ok((i, v))
}

fn multipolygon_type(i: &str) -> IResult<&str, &str> {
	let (i, v) = alt((
		delimited(char(SINGLE), tag("MultiPolygon"), char(SINGLE)),
		delimited(char(DOUBLE), tag("MultiPolygon"), char(DOUBLE)),
	))(i)?;
	Ok((i, v))
}

fn collection_type(i: &str) -> IResult<&str, &str> {
	let (i, v) = alt((
		delimited(char(SINGLE), tag("GeometryCollection"), char(SINGLE)),
		delimited(char(DOUBLE), tag("GeometryCollection"), char(DOUBLE)),
	))(i)?;
	Ok((i, v))
}

//
//
//

fn key_type(i: &str) -> IResult<&str, &str> {
	let (i, v) = alt((
		tag("type"),
		delimited(char(SINGLE), tag("type"), char(SINGLE)),
		delimited(char(DOUBLE), tag("type"), char(DOUBLE)),
	))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(':')(i)?;
	let (i, _) = mightbespace(i)?;
	Ok((i, v))
}

fn key_vals(i: &str) -> IResult<&str, &str> {
	let (i, v) = alt((
		tag("coordinates"),
		delimited(char(SINGLE), tag("coordinates"), char(SINGLE)),
		delimited(char(DOUBLE), tag("coordinates"), char(DOUBLE)),
	))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(':')(i)?;
	let (i, _) = mightbespace(i)?;
	Ok((i, v))
}

fn key_geom(i: &str) -> IResult<&str, &str> {
	let (i, v) = alt((
		tag("geometries"),
		delimited(char(SINGLE), tag("geometries"), char(SINGLE)),
		delimited(char(DOUBLE), tag("geometries"), char(DOUBLE)),
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
