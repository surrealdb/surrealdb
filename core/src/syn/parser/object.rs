use std::collections::BTreeMap;

use geo_types::{LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon};
use reblessive::Stk;

use crate::{
	enter_object_recursion,
	sql::{Block, Geometry, Object, Strand, Value},
	syn::{
		parser::{mac::expected, ParseError, ParseErrorKind, ParseResult, Parser},
		token::{t, Span, TokenKind},
	},
};

use super::mac::unexpected;

impl Parser<'_> {
	/// Parse an production which starts with an `{`
	///
	/// Either a block statemnt, a object or geometry.
	pub(super) async fn parse_object_like(
		&mut self,
		ctx: &mut Stk,
		start: Span,
	) -> ParseResult<Value> {
		if self.eat(t!("}")) {
			// empty object, just return
			enter_object_recursion!(_this = self => {
				return Ok(Value::Object(Object::default()));
			})
		}

		// Check first if it can be an object.
		if self.peek_token_at(1).kind == t!(":") {
			enter_object_recursion!(this = self => {
			   return this.parse_object_or_geometry(ctx, start).await;
			})
		}

		// not an object so instead parse as a block.
		self.parse_block(ctx, start).await.map(Box::new).map(Value::Block)
	}

	/// Parse a production starting with an `{` as either an object or a geometry.
	///
	/// This function tries to match an object to an geometry like object and if it is unable
	/// fallsback to parsing normal objects.
	async fn parse_object_or_geometry(&mut self, ctx: &mut Stk, start: Span) -> ParseResult<Value> {
		// empty object was already matched previously so next must be a key.
		let key = self.parse_object_key()?;
		expected!(self, t!(":"));
		// the order of fields of a geometry does not matter so check if it is any of geometry like keys
		// "type" : could be the type of the object.
		// "collections": could be a geometry collection.
		// "geometry": could be the values of geometry.
		match key.as_str() {
			"type" => {
				// for it to be geometry the next value must be a strand like.
				let token = self.peek();
				let strand = self.token_value::<Strand>(token);
				match strand.as_ref().map(|x| x.as_str()) {
					Ok("Point") => {
						// we matched a type correctly but the field containing the geometry value
						// can still be wrong.
						//
						// we can unwrap strand since we just matched it to not be an err.
						self.parse_geometry_after_type(
							ctx,
							start,
							key,
							strand.unwrap(),
							Self::to_point,
							|x| Value::Geometry(Geometry::Point(x)),
						)
						.await
					}
					Ok("LineString") => {
						self.parse_geometry_after_type(
							ctx,
							start,
							key,
							strand.unwrap(),
							Self::to_line,
							|x| Value::Geometry(Geometry::Line(x)),
						)
						.await
					}
					Ok("Polygon") => {
						self.parse_geometry_after_type(
							ctx,
							start,
							key,
							strand.unwrap(),
							Self::to_polygon,
							|x| Value::Geometry(Geometry::Polygon(x)),
						)
						.await
					}
					Ok("MultiPoint") => {
						self.parse_geometry_after_type(
							ctx,
							start,
							key,
							strand.unwrap(),
							Self::to_multipoint,
							|x| Value::Geometry(Geometry::MultiPoint(x)),
						)
						.await
					}
					Ok("MultiLineString") => {
						self.parse_geometry_after_type(
							ctx,
							start,
							key,
							strand.unwrap(),
							Self::to_multiline,
							|x| Value::Geometry(Geometry::MultiLine(x)),
						)
						.await
					}
					Ok("MultiPolygon") => {
						self.parse_geometry_after_type(
							ctx,
							start,
							key,
							strand.unwrap(),
							Self::to_multipolygon,
							|x| Value::Geometry(Geometry::MultiPolygon(x)),
						)
						.await
					}
					Ok("GeometryCollection") => {
						self.next();
						let strand = strand.unwrap();
						if !self.eat(t!(",")) {
							// missing next field, not a geometry.
							return self
								.parse_object_from_map(
									ctx,
									BTreeMap::from([(key, Value::Strand(strand))]),
									start,
								)
								.await
								.map(Value::Object);
						}
						let coord_key = self.parse_object_key()?;
						expected!(self, t!(":"));
						if coord_key != "geometries" {
							// invalid field key, not a Geometry
							return self
								.parse_object_from_key(
									ctx,
									coord_key,
									BTreeMap::from([(key, Value::Strand(strand))]),
									start,
								)
								.await
								.map(Value::Object);
						}
						let value = ctx.run(|ctx| self.parse_value_field(ctx)).await?;
						let comma = self.eat(t!(","));
						if !self.eat(t!("}")) {
							if !comma {
								// No brace after no comma, missing brace.
								return Err(ParseError::new(
									ParseErrorKind::UnclosedDelimiter {
										expected: t!("}"),
										should_close: start,
									},
									self.last_span(),
								));
							}

							// A comma and then no brace. more then two fields, not a geometry.
							return self
								.parse_object_from_map(
									ctx,
									BTreeMap::from([
										(key, Value::Strand(strand)),
										(coord_key, value),
									]),
									start,
								)
								.await
								.map(Value::Object);
						}

						if let Value::Array(x) = value {
							// test first to avoid a cloning.
							if x.iter().all(|x| matches!(x, Value::Geometry(_))) {
								let geometries =
									x.0.into_iter()
										.map(|x| {
											if let Value::Geometry(x) = x {
												x
											} else {
												unreachable!()
											}
										})
										.collect();

								return Ok(Value::Geometry(Geometry::Collection(geometries)));
							}

							return Ok(Value::Object(Object(BTreeMap::from([
								(key, Value::Strand(strand)),
								(coord_key, Value::Array(x)),
							]))));
						}

						Ok(Value::Object(Object(BTreeMap::from([
							(key, Value::Strand(strand)),
							(coord_key, value),
						]))))
					}
					Ok(_) => {
						self.pop_peek();
						if !self.eat(t!(",")) {
							self.expect_closing_delimiter(t!("}"), start)?;
							Ok(Value::Object(Object(BTreeMap::from([(
								key,
								Value::Strand(strand.unwrap()),
							)]))))
						} else {
							self.parse_object_from_map(
								ctx,
								BTreeMap::from([(key, Value::Strand(strand.unwrap()))]),
								start,
							)
							.await
							.map(Value::Object)
						}
					}
					_ => self
						.parse_object_from_key(ctx, key, BTreeMap::new(), start)
						.await
						.map(Value::Object),
				}
			}
			"coordinates" => {
				// found coordinates field, next must be a coordinates value but we don't know
				// which until we match type.
				let value = ctx.run(|ctx| self.parse_value_field(ctx)).await?;
				if !self.eat(t!(",")) {
					// no comma object must end early.
					self.expect_closing_delimiter(t!("}"), start)?;
					return Ok(Value::Object(Object(BTreeMap::from([(key, value)]))));
				}

				if self.eat(t!("}")) {
					// object ends early.
					return Ok(Value::Object(Object(BTreeMap::from([(key, value)]))));
				}

				let type_key = self.parse_object_key()?;
				expected!(self, t!(":"));
				if type_key != "type" {
					// not the right field, return object.
					return self
						.parse_object_from_key(ctx, type_key, BTreeMap::from([(key, value)]), start)
						.await
						.map(Value::Object);
				}
				let peek = self.peek();
				let strand = self.token_value::<Strand>(peek);
				// match the type and then match the coordinates field to a value of that type.
				let (ate_comma, type_value) = match strand.as_ref().map(|x| x.as_str()) {
					Ok("Point") => {
						self.next();
						let ate_comma = self.eat(t!(","));
						if self.eat(t!("}")) {
							if let Some(point) = Self::to_point(&value) {
								return Ok(Value::Geometry(Geometry::Point(point)));
							}
						}
						// At this point the value does not match, or there are more fields.
						// since we matched `Ok("Point")` strand cannot be an error so this unwrap
						// will never panic.
						(ate_comma, Value::Strand(strand.unwrap()))
					}
					Ok("LineString") => {
						self.next();
						let ate_comma = self.eat(t!(","));
						if self.eat(t!("}")) {
							if let Some(point) = Self::to_line(&value) {
								return Ok(Value::Geometry(Geometry::Line(point)));
							}
						}
						(ate_comma, Value::Strand(strand.unwrap()))
					}
					Ok("Polygon") => {
						self.next();
						let ate_comma = self.eat(t!(","));
						if self.eat(t!("}")) {
							if let Some(point) = Self::to_polygon(&value) {
								return Ok(Value::Geometry(Geometry::Polygon(point)));
							}
						}
						(ate_comma, Value::Strand(strand.unwrap()))
					}
					Ok("MultiPoint") => {
						self.next();
						let ate_comma = self.eat(t!(","));
						if self.eat(t!("}")) {
							if let Some(point) = Self::to_multipolygon(&value) {
								return Ok(Value::Geometry(Geometry::MultiPolygon(point)));
							}
						}
						(ate_comma, Value::Strand(strand.unwrap()))
					}
					Ok("MultiLineString") => {
						self.next();
						let ate_comma = self.eat(t!(","));
						if self.eat(t!("}")) {
							if let Some(point) = Self::to_multiline(&value) {
								return Ok(Value::Geometry(Geometry::MultiLine(point)));
							}
						}
						(ate_comma, Value::Strand(strand.unwrap()))
					}
					Ok("MultiPolygon") => {
						self.next();
						let ate_comma = self.eat(t!(","));
						if self.eat(t!("}")) {
							if let Some(point) = Self::to_multipolygon(&value) {
								return Ok(Value::Geometry(Geometry::MultiPolygon(point)));
							}
						}
						(ate_comma, Value::Strand(strand.unwrap()))
					}
					_ => {
						let value = ctx.run(|ctx| self.parse_value_field(ctx)).await?;
						(self.eat(t!(",")), value)
					}
				};
				// type field or coordinates value didn't match or the object continues after to
				// fields.

				if !ate_comma {
					self.expect_closing_delimiter(t!("}"), start)?;
					return Ok(Value::Object(Object(BTreeMap::from([
						(key, value),
						(type_key, type_value),
					]))));
				}
				self.parse_object_from_map(
					ctx,
					BTreeMap::from([(key, value), (type_key, type_value)]),
					start,
				)
				.await
				.map(Value::Object)
			}
			"geometries" => {
				let value = ctx.run(|ctx| self.parse_value_field(ctx)).await?;
				if !self.eat(t!(",")) {
					self.expect_closing_delimiter(t!("}"), start)?;
					return Ok(Value::Object(Object(BTreeMap::from([(key, value)]))));
				}
				let type_key = self.parse_object_key()?;
				expected!(self, t!(":"));
				if type_key != "type" {
					return self
						.parse_object_from_key(ctx, type_key, BTreeMap::from([(key, value)]), start)
						.await
						.map(Value::Object);
				}
				let peek = self.peek();
				let strand = self.token_value::<Strand>(peek);
				let (ate_comma, type_value) =
					if let Ok("GeometryCollection") = strand.as_ref().map(|x| x.as_str()) {
						self.next();
						let ate_comma = self.eat(t!(","));
						if self.eat(t!("}")) {
							if let Value::Array(ref x) = value {
								if x.iter().all(|x| matches!(x, Value::Geometry(_))) {
									let Value::Array(x) = value else {
										unreachable!()
									};
									let geometries = x
										.into_iter()
										.map(|x| {
											if let Value::Geometry(x) = x {
												x
											} else {
												unreachable!()
											}
										})
										.collect();
									return Ok(Value::Geometry(Geometry::Collection(geometries)));
								}
							}
						}
						(ate_comma, Value::Strand(strand.unwrap()))
					} else {
						let value = ctx.run(|ctx| self.parse_value_field(ctx)).await?;
						(self.eat(t!(",")), value)
					};

				if !ate_comma {
					self.expect_closing_delimiter(t!("}"), start)?;
					return Ok(Value::Object(Object(BTreeMap::from([
						(key, value),
						(type_key, type_value),
					]))));
				}
				self.parse_object_from_map(
					ctx,
					BTreeMap::from([(key, value), (type_key, type_value)]),
					start,
				)
				.await
				.map(Value::Object)
			}
			_ => self
				.parse_object_from_key(ctx, key, BTreeMap::new(), start)
				.await
				.map(Value::Object),
		}
	}

	async fn parse_geometry_after_type<F, Fm, R>(
		&mut self,
		ctx: &mut Stk,
		start: Span,
		key: String,
		strand: Strand,
		capture: F,
		map: Fm,
	) -> ParseResult<Value>
	where
		F: FnOnce(&Value) -> Option<R>,
		Fm: FnOnce(R) -> Value,
	{
		// eat the strand with the type name.
		self.next();
		if !self.eat(t!(",")) {
			// there is not second field. not a geometry
			self.expect_closing_delimiter(t!("}"), start)?;
			return Ok(Value::Object(Object(BTreeMap::from([(key, Value::Strand(strand))]))));
		}
		let coord_key = self.parse_object_key()?;
		expected!(self, t!(":"));
		if coord_key != "coordinates" {
			// next field was not correct, fallback to parsing plain object.
			return self
				.parse_object_from_key(
					ctx,
					coord_key,
					BTreeMap::from([(key, Value::Strand(strand))]),
					start,
				)
				.await
				.map(Value::Object);
		}
		let value = ctx.run(|ctx| self.parse_value_field(ctx)).await?;
		let comma = self.eat(t!(","));
		if !self.eat(t!("}")) {
			// the object didn't end, either an error or not a geometry.
			if !comma {
				return Err(ParseError::new(
					ParseErrorKind::UnclosedDelimiter {
						expected: t!("}"),
						should_close: start,
					},
					self.last_span(),
				));
			}

			return self
				.parse_object_from_map(
					ctx,
					BTreeMap::from([(key, Value::Strand(strand)), (coord_key, value)]),
					start,
				)
				.await
				.map(Value::Object);
		}

		let Some(v) = capture(&value) else {
			// failed to match the geometry value, just a plain object.
			return Ok(Value::Object(Object(BTreeMap::from([
				(key, Value::Strand(strand)),
				(coord_key, value),
			]))));
		};
		// successfully matched the value, it is a geometry.
		Ok(map(v))
	}

	fn to_multipolygon(v: &Value) -> Option<MultiPolygon<f64>> {
		let mut res = Vec::new();
		let Value::Array(v) = v else {
			return None;
		};
		for x in v.iter() {
			res.push(Self::to_polygon(x)?);
		}
		Some(MultiPolygon::new(res))
	}

	fn to_multiline(v: &Value) -> Option<MultiLineString<f64>> {
		let mut res = Vec::new();
		let Value::Array(v) = v else {
			return None;
		};
		for x in v.iter() {
			res.push(Self::to_line(x)?);
		}
		Some(MultiLineString::new(res))
	}

	fn to_multipoint(v: &Value) -> Option<MultiPoint<f64>> {
		let mut res = Vec::new();
		let Value::Array(v) = v else {
			return None;
		};
		for x in v.iter() {
			res.push(Self::to_point(x)?);
		}
		Some(MultiPoint::new(res))
	}

	fn to_polygon(v: &Value) -> Option<Polygon<f64>> {
		let mut res = Vec::new();
		let Value::Array(v) = v else {
			return None;
		};
		if v.is_empty() {
			return None;
		}
		let first = Self::to_line(&v[0])?;
		for x in &v[1..] {
			res.push(Self::to_line(x)?);
		}
		Some(Polygon::new(first, res))
	}

	fn to_line(v: &Value) -> Option<LineString<f64>> {
		let mut res = Vec::new();
		let Value::Array(v) = v else {
			return None;
		};
		for x in v.iter() {
			res.push(Self::to_point(x)?);
		}
		Some(LineString::from(res))
	}

	fn to_point(v: &Value) -> Option<Point<f64>> {
		let Value::Array(v) = v else {
			return None;
		};
		if v.len() != 2 {
			return None;
		}
		// FIXME: This truncates decimals and large integers into a f64.
		let Value::Number(ref a) = v.0[0] else {
			return None;
		};
		let Value::Number(ref b) = v.0[1] else {
			return None;
		};
		Some(Point::from((a.clone().try_into().ok()?, b.clone().try_into().ok()?)))
	}

	async fn parse_object_from_key(
		&mut self,
		ctx: &mut Stk,
		key: String,
		mut map: BTreeMap<String, Value>,
		start: Span,
	) -> ParseResult<Object> {
		let v = ctx.run(|ctx| self.parse_value_field(ctx)).await?;
		map.insert(key, v);
		if !self.eat(t!(",")) {
			self.expect_closing_delimiter(t!("}"), start)?;
			return Ok(Object(map));
		}
		self.parse_object_from_map(ctx, map, start).await
	}

	/// Parses an object.
	///
	/// Expects the span of the starting `{` as an argument.
	///
	/// # Parser state
	/// Expects the first `{` to already have been eaten.
	pub(super) async fn parse_object(&mut self, ctx: &mut Stk, start: Span) -> ParseResult<Object> {
		enter_object_recursion!(this = self => {
			this.parse_object_from_map(ctx, BTreeMap::new(), start).await
		})
	}

	async fn parse_object_from_map(
		&mut self,
		ctx: &mut Stk,
		mut map: BTreeMap<String, Value>,
		start: Span,
	) -> ParseResult<Object> {
		loop {
			if self.eat(t!("}")) {
				return Ok(Object(map));
			}

			let (key, value) = self.parse_object_entry(ctx).await?;
			// TODO: Error on duplicate key?
			map.insert(key, value);

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!("}"), start)?;
				return Ok(Object(map));
			}
		}
	}

	/// Parses a block of statements
	///
	/// # Parser State
	/// Expects the starting `{` to have already been eaten and its span to be handed to this
	/// functions as the `start` parameter.
	pub(super) async fn parse_block(&mut self, ctx: &mut Stk, start: Span) -> ParseResult<Block> {
		let mut statements = Vec::new();
		loop {
			while self.eat(t!(";")) {}
			if self.eat(t!("}")) {
				break;
			}

			let stmt = ctx.run(|ctx| self.parse_entry(ctx)).await?;
			statements.push(stmt);
			if !self.eat(t!(";")) {
				self.expect_closing_delimiter(t!("}"), start)?;
				break;
			}
		}
		Ok(Block(statements))
	}

	/// Parse a single entry in the object, i.e. `field: value + 1` in the object `{ field: value +
	/// 1 }`
	async fn parse_object_entry(&mut self, ctx: &mut Stk) -> ParseResult<(String, Value)> {
		let text = self.parse_object_key()?;
		expected!(self, t!(":"));
		let value = ctx.run(|ctx| self.parse_value_field(ctx)).await?;
		Ok((text, value))
	}

	/// Parses the key of an object, i.e. `field` in the object `{ field: 1 }`.
	pub fn parse_object_key(&mut self) -> ParseResult<String> {
		let token = self.peek();
		match token.kind {
			TokenKind::Keyword(_)
			| TokenKind::Language(_)
			| TokenKind::Algorithm(_)
			| TokenKind::Distance(_)
			| TokenKind::VectorType(_) => {
				self.pop_peek();
				let str = self.lexer.reader.span(token.span);
				// Lexer should ensure that the token is valid utf-8
				let str = std::str::from_utf8(str).unwrap().to_owned();
				Ok(str)
			}
			TokenKind::Identifier | TokenKind::Strand => {
				self.pop_peek();
				let str = self.lexer.string.take().unwrap();
				Ok(str)
			}
			TokenKind::Number(_) => {
				self.pop_peek();
				Ok(self.lexer.string.take().unwrap())
			}
			x => unexpected!(self, x, "an object key"),
		}
	}
}

#[cfg(test)]
mod test {
	use super::*;
	use crate::syn::Parse;

	#[test]
	fn block_value() {
		let sql = "{ 80 }";
		let out = Value::parse(sql);
		assert_eq!(sql, out.to_string())
	}

	#[test]
	fn block_ifelse() {
		let sql = "{ RETURN IF true THEN 50 ELSE 40 END; }";
		let out = Value::parse(sql);
		assert_eq!(sql, out.to_string())
	}

	#[test]
	fn block_multiple() {
		let sql = r#"{

	LET $person = (SELECT * FROM person WHERE first = $first AND last = $last AND birthday = $birthday);

	RETURN IF $person[0].id THEN
		$person[0]
	ELSE
		(CREATE person SET first = $first, last = $last, birthday = $birthday)
	END;

}"#;
		let out = Value::parse(sql);
		assert_eq!(sql, format!("{:#}", out))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::syn::Parse;

	#[test]
	fn simple() {
		let sql = "(-0.118092, 51.509865)";
		let out = Value::parse(sql);
		assert!(matches!(out, Value::Geometry(_)));
		assert_eq!("(-0.118092, 51.509865)", format!("{}", out));
	}

	#[test]
	fn point() {
		let sql = r#"{
			type: 'Point',
			coordinates: [-0.118092, 51.509865]
		}"#;
		let out = Value::parse(sql);
		assert!(matches!(out, Value::Geometry(_)));
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
		let out = Value::parse(sql);
		assert!(matches!(out, Value::Geometry(_)));
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
		let out = Value::parse(sql);
		assert!(matches!(out, Value::Geometry(_)));
		assert_eq!("{ type: 'Polygon', coordinates: [[[-0.38314819, 51.37692386], [0.1785278, 51.37692386], [0.1785278, 51.6146057], [-0.38314819, 51.6146057], [-0.38314819, 51.37692386]], [[[-0.38314819, 51.37692386], [0.1785278, 51.37692386], [0.1785278, 51.6146057], [-0.38314819, 51.6146057], [-0.38314819, 51.37692386]]]] }", format!("{}", out));
	}
}
