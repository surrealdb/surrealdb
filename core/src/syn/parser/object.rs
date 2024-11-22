use std::collections::BTreeMap;

use reblessive::Stk;

use crate::{
	sql::{Block, Geometry, Object, Strand, Value},
	syn::{
		error::bail,
		lexer::compound,
		parser::{enter_object_recursion, mac::expected, ParseResult, Parser},
		token::{t, Glued, Span, TokenKind},
	},
};

use super::mac::unexpected;

impl Parser<'_> {
	/// Parse an production which starts with an `{`
	///
	/// Either a block statemnt, an object or geometry.
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

		// Now check first if it can be an object.
		if self.glue_and_peek1()?.kind == t!(":") {
			enter_object_recursion!(this = self => {
			   return this.parse_object_or_geometry(ctx, start).await;
			})
		}

		// not an object so instead parse as a block.
		self.parse_block(ctx, start).await.map(Box::new).map(Value::Block)
	}

	async fn parse_object_or_geometry_after_type(
		&mut self,
		ctx: &mut Stk,
		start: Span,
		key: String,
	) -> ParseResult<Value> {
		expected!(self, t!(":"));
		// for it to be geometry the next value must be a strand like.
		let (t!("\"") | t!("'") | TokenKind::Glued(Glued::Strand)) = self.peek_kind() else {
			return self
				.parse_object_from_key(ctx, key, BTreeMap::new(), start)
				.await
				.map(Value::Object);
		};

		// We know it is a strand so check if the type is one of the allowe geometry types.
		// If it is, there are some which all take roughly the save type of value and produce a
		// similar output, which is parsed with parse_geometry_after_type
		//
		// GeometryCollection however has a different object key for its value, so it is handled
		// appart from the others.
		let type_value = self.next_token_value::<Strand>()?.0;
		match type_value.as_str() {
			"Point" => {
				// we matched a type correctly but the field containing the geometry value
				// can still be wrong.
				//
				// we can unwrap strand since we just matched it to not be an err.
				self.parse_geometry_after_type(
					ctx,
					start,
					key,
					type_value,
					Geometry::array_to_point,
					|x| Value::Geometry(Geometry::Point(x)),
				)
				.await
			}
			"LineString" => {
				self.parse_geometry_after_type(
					ctx,
					start,
					key,
					type_value,
					Geometry::array_to_line,
					|x| Value::Geometry(Geometry::Line(x)),
				)
				.await
			}
			"Polygon" => {
				self.parse_geometry_after_type(
					ctx,
					start,
					key,
					type_value,
					Geometry::array_to_polygon,
					|x| Value::Geometry(Geometry::Polygon(x)),
				)
				.await
			}
			"MultiPoint" => {
				self.parse_geometry_after_type(
					ctx,
					start,
					key,
					type_value,
					Geometry::array_to_multipoint,
					|x| Value::Geometry(Geometry::MultiPoint(x)),
				)
				.await
			}
			"MultiLineString" => {
				self.parse_geometry_after_type(
					ctx,
					start,
					key,
					type_value,
					Geometry::array_to_multiline,
					|x| Value::Geometry(Geometry::MultiLine(x)),
				)
				.await
			}
			"MultiPolygon" => {
				self.parse_geometry_after_type(
					ctx,
					start,
					key,
					type_value,
					Geometry::array_to_multipolygon,
					|x| Value::Geometry(Geometry::MultiPolygon(x)),
				)
				.await
			}
			"GeometryCollection" => {
				if !self.eat(t!(",")) {
					// missing next field, not a geometry.
					return self
						.parse_object_from_map(
							ctx,
							BTreeMap::from([(key, Value::Strand(type_value.into()))]),
							start,
						)
						.await
						.map(Value::Object);
				}

				let coord_key = self.parse_object_key()?;
				if coord_key != "geometries" {
					expected!(self, t!(":"));
					// invalid field key, not a Geometry
					return self
						.parse_object_from_key(
							ctx,
							coord_key,
							BTreeMap::from([(key, Value::Strand(type_value.into()))]),
							start,
						)
						.await
						.map(Value::Object);
				}

				expected!(self, t!(":"));

				let value = ctx.run(|ctx| self.parse_value_inherit(ctx)).await?;

				// check for an object end, if it doesn't end it is not a geometry.
				if !self.eat(t!(",")) {
					self.expect_closing_delimiter(t!("}"), start)?;
				} else {
					if self.peek_kind() != t!("}") {
						// A comma and then no brace. more then two fields, not a geometry.
						return self
							.parse_object_from_map(
								ctx,
								BTreeMap::from([
									(key, Value::Strand(type_value.into())),
									(coord_key, value),
								]),
								start,
							)
							.await
							.map(Value::Object);
					}
					self.pop_peek();
				}

				// try to convert to the right value.
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
						(key, Value::Strand(type_value.into())),
						(coord_key, Value::Array(x)),
					]))));
				}

				// Couldn't convert so it is a normal object.
				Ok(Value::Object(Object(BTreeMap::from([
					(key, Value::Strand(type_value.into())),
					(coord_key, value),
				]))))
			}
			// key was not one of the allowed keys so it is a normal object.
			_ => {
				let object = BTreeMap::from([(key, Value::Strand(type_value.into()))]);

				if self.eat(t!(",")) {
					self.parse_object_from_map(ctx, object, start).await.map(Value::Object)
				} else {
					self.expect_closing_delimiter(t!("}"), start)?;
					Ok(Value::Object(Object(object)))
				}
			}
		}
	}

	async fn parse_object_or_geometry_after_coordinates(
		&mut self,
		ctx: &mut Stk,
		start: Span,
		key: String,
	) -> ParseResult<Value> {
		expected!(self, t!(":"));

		// found coordinates field, next must be a coordinates value but we don't know
		// which until we match type.
		let value = ctx.run(|ctx| self.parse_value_inherit(ctx)).await?;

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
		if type_key != "type" {
			expected!(self, t!(":"));
			// not the right field, return object.
			return self
				.parse_object_from_key(ctx, type_key, BTreeMap::from([(key, value)]), start)
				.await
				.map(Value::Object);
		}
		expected!(self, t!(":"));

		let (t!("\"") | t!("'")) = self.peek_kind() else {
			// not the right value also move back to parsing an object.
			return self
				.parse_object_from_key(ctx, type_key, BTreeMap::from([(key, value)]), start)
				.await
				.map(Value::Object);
		};

		let type_value = self.next_token_value::<Strand>()?.0;
		let ate_comma = self.eat(t!(","));
		// match the type and then match the coordinates field to a value of that type.
		match type_value.as_str() {
			"Point" => {
				if self.eat(t!("}")) {
					if let Some(point) = Geometry::array_to_point(&value) {
						return Ok(Value::Geometry(Geometry::Point(point)));
					}
				}
			}
			"LineString" => {
				if self.eat(t!("}")) {
					if let Some(point) = Geometry::array_to_line(&value) {
						return Ok(Value::Geometry(Geometry::Line(point)));
					}
				}
			}
			"Polygon" => {
				if self.eat(t!("}")) {
					if let Some(point) = Geometry::array_to_polygon(&value) {
						return Ok(Value::Geometry(Geometry::Polygon(point)));
					}
				}
			}
			"MultiPoint" => {
				if self.eat(t!("}")) {
					if let Some(point) = Geometry::array_to_multipolygon(&value) {
						return Ok(Value::Geometry(Geometry::MultiPolygon(point)));
					}
				}
			}
			"MultiLineString" => {
				if self.eat(t!("}")) {
					if let Some(point) = Geometry::array_to_multiline(&value) {
						return Ok(Value::Geometry(Geometry::MultiLine(point)));
					}
				}
			}
			"MultiPolygon" => {
				if self.eat(t!("}")) {
					if let Some(point) = Geometry::array_to_multipolygon(&value) {
						return Ok(Value::Geometry(Geometry::MultiPolygon(point)));
					}
				}
			}
			_ => {}
		};

		// type field or coordinates value didn't match or the object continues after to
		// fields.

		if !ate_comma {
			self.expect_closing_delimiter(t!("}"), start)?;
			return Ok(Value::Object(Object(BTreeMap::from([
				(key, value),
				(type_key, Value::Strand(type_value.into())),
			]))));
		}

		self.parse_object_from_map(
			ctx,
			BTreeMap::from([(key, value), (type_key, Value::Strand(type_value.into()))]),
			start,
		)
		.await
		.map(Value::Object)
	}

	async fn parse_object_or_geometry_after_geometries(
		&mut self,
		ctx: &mut Stk,
		start: Span,
		key: String,
	) -> ParseResult<Value> {
		// 'geometries' key can only happen in a GeometryCollection, so try to parse that.
		expected!(self, t!(":"));

		let value = ctx.run(|ctx| self.parse_value_inherit(ctx)).await?;

		// if the object ends here, it is not a geometry.
		if !self.eat(t!(",")) || self.peek_kind() == t!("}") {
			self.expect_closing_delimiter(t!("}"), start)?;
			return Ok(Value::Object(Object(BTreeMap::from([(key, value)]))));
		}

		// parse the next objectkey
		let type_key = self.parse_object_key()?;
		// it if isn't 'type' this object is not a geometry, so bail.
		if type_key != "type" {
			expected!(self, t!(":"));
			return self
				.parse_object_from_key(ctx, type_key, BTreeMap::from([(key, value)]), start)
				.await
				.map(Value::Object);
		}
		expected!(self, t!(":"));
		// check if the next key is a strand.
		let (t!("\"") | t!("'")) = self.peek_kind() else {
			// not the right value also move back to parsing an object.
			return self
				.parse_object_from_key(ctx, type_key, BTreeMap::from([(key, value)]), start)
				.await
				.map(Value::Object);
		};

		let type_value = self.next_token_value::<Strand>()?.0;
		let ate_comma = self.eat(t!(","));

		if type_value == "GeometryCollection" && self.eat(t!("}")) {
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

		// Either type value didn't match or gemoetry value didn't match.
		// Regardless the current object is not a geometry.

		if !ate_comma {
			self.expect_closing_delimiter(t!("}"), start)?;
			return Ok(Value::Object(Object(BTreeMap::from([
				(key, value),
				(type_key, Value::Strand(type_value.into())),
			]))));
		}

		self.parse_object_from_map(
			ctx,
			BTreeMap::from([(key, value), (type_key, Value::Strand(type_value.into()))]),
			start,
		)
		.await
		.map(Value::Object)
	}

	/// Parse a production starting with an `{` as either an object or a geometry.
	///
	/// This function tries to match an object to an geometry like object and if it is unable
	/// fallsback to parsing normal objects.
	async fn parse_object_or_geometry(&mut self, ctx: &mut Stk, start: Span) -> ParseResult<Value> {
		// empty object was already matched previously so next must be a key.
		let key = self.parse_object_key()?;
		// the order of fields of a geometry does not matter so check if it is any of geometry like keys
		// "type" : could be the type of the object.
		// "collections": could be a geometry collection.
		// "geometry": could be the values of geometry.
		match key.as_str() {
			"type" => self.parse_object_or_geometry_after_type(ctx, start, key).await,
			"coordinates" => self.parse_object_or_geometry_after_coordinates(ctx, start, key).await,
			"geometries" => self.parse_object_or_geometry_after_geometries(ctx, start, key).await,
			_ => {
				expected!(self, t!(":"));
				self.parse_object_from_key(ctx, key, BTreeMap::new(), start)
					.await
					.map(Value::Object)
			}
		}
	}

	async fn parse_geometry_after_type<F, Fm, R>(
		&mut self,
		ctx: &mut Stk,
		start: Span,
		key: String,
		strand: String,
		capture: F,
		map: Fm,
	) -> ParseResult<Value>
	where
		F: FnOnce(&Value) -> Option<R>,
		Fm: FnOnce(R) -> Value,
	{
		if !self.eat(t!(",")) {
			// there is not second field. not a geometry
			self.expect_closing_delimiter(t!("}"), start)?;
			return Ok(Value::Object(Object(BTreeMap::from([(
				key,
				Value::Strand(strand.into()),
			)]))));
		}
		let coord_key = self.parse_object_key()?;
		if coord_key != "coordinates" {
			expected!(self, t!(":"));
			// next field was not correct, fallback to parsing plain object.
			return self
				.parse_object_from_key(
					ctx,
					coord_key,
					BTreeMap::from([(key, Value::Strand(strand.into()))]),
					start,
				)
				.await
				.map(Value::Object);
		}
		expected!(self, t!(":"));
		let value = ctx.run(|ctx| self.parse_value_inherit(ctx)).await?;
		let comma = self.eat(t!(","));
		if !self.eat(t!("}")) {
			// the object didn't end, either an error or not a geometry.
			if !comma {
				bail!("Unexpected token, expected delimiter `}}`",
					@self.recent_span(),
					@start => "expected this delimiter to close"
				);
			}

			return self
				.parse_object_from_map(
					ctx,
					BTreeMap::from([(key, Value::Strand(strand.into())), (coord_key, value)]),
					start,
				)
				.await
				.map(Value::Object);
		}

		let Some(v) = capture(&value) else {
			// failed to match the geometry value, just a plain object.
			return Ok(Value::Object(Object(BTreeMap::from([
				(key, Value::Strand(strand.into())),
				(coord_key, value),
			]))));
		};
		// successfully matched the value, it is a geometry.
		Ok(map(v))
	}

	async fn parse_object_from_key(
		&mut self,
		ctx: &mut Stk,
		key: String,
		mut map: BTreeMap<String, Value>,
		start: Span,
	) -> ParseResult<Object> {
		let v = ctx.run(|ctx| self.parse_value_inherit(ctx)).await?;
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
	pub async fn parse_block(&mut self, ctx: &mut Stk, start: Span) -> ParseResult<Block> {
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
		let value = ctx.run(|ctx| self.parse_value_inherit(ctx)).await?;
		Ok((text, value))
	}

	/// Parses the key of an object, i.e. `field` in the object `{ field: 1 }`.
	pub(super) fn parse_object_key(&mut self) -> ParseResult<String> {
		let token = self.peek();
		match token.kind {
			x if Self::kind_is_keyword_like(x) => {
				self.pop_peek();
				let str = self.lexer.span_str(token.span);
				Ok(str.to_string())
			}
			TokenKind::Identifier => {
				self.pop_peek();
				let str = self.lexer.string.take().unwrap();
				Ok(str)
			}
			t!("\"") | t!("'") | TokenKind::Glued(Glued::Strand) => {
				let str = self.next_token_value::<Strand>()?.0;
				Ok(str)
			}
			TokenKind::Digits => {
				self.pop_peek();
				let span = self.lexer.lex_compound(token, compound::number)?.span;
				let str = self.lexer.span_str(span);
				Ok(str.to_string())
			}
			TokenKind::Glued(Glued::Number) => {
				self.pop_peek();
				let str = self.lexer.span_str(token.span);
				Ok(str.to_string())
			}
			_ => unexpected!(self, token, "an object key"),
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
		assert_eq!("{ type: 'Polygon', coordinates: [[[-0.38314819, 51.37692386], [0.1785278, 51.37692386], [0.1785278, 51.6146057], [-0.38314819, 51.6146057], [-0.38314819, 51.37692386]], [[-0.38314819, 51.37692386], [0.1785278, 51.37692386], [0.1785278, 51.6146057], [-0.38314819, 51.6146057], [-0.38314819, 51.37692386]]] }", format!("{}", out));
	}
}
