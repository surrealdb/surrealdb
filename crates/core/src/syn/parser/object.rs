use std::collections::BTreeMap;

use reblessive::Stk;

use crate::{
	sql::{Block, Geometry, Object, Strand, Value},
	syn::{
		lexer::compound,
		parser::{enter_object_recursion, mac::expected, ParseResult, Parser},
		token::{t, Glued, Span, TokenKind},
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

		// Now check first if it can be an object.
		if self.glue_and_peek1()?.kind == t!(":") {
			enter_object_recursion!(this = self => {
			   return this.parse_object_or_geometry(ctx, start).await;
			})
		}

		// not an object so instead parse as a block.
		self.parse_block(ctx, start).await.map(Box::new).map(Value::Block)
	}

	async fn convert_to_geometry(&mut self, geometry_type: &str, object: Object) -> Value {
		// since geometry differ in the second key (geometries or coordinates), they are handled differently
		match geometry_type {
			"GeometryCollection" => self
				.convert_to_geometry_collection(object.clone())
				.await
				.unwrap_or(Value::Object(object)),
			"Point" | "LineString" | "Polygon" | "MultiPoint" | "MultiLineString"
			| "MultiPolygon" => self
				.convert_to_geometry_non_collections(geometry_type, object.clone())
				.await
				.unwrap_or(Value::Object(object)),
			_ => Value::Object(object),
		}
	}

	async fn convert_to_geometry_collection(&mut self, object: Object) -> ParseResult<Value> {
		// check if the object has the correct fields.
		if let Some(Value::Array(x)) = object.get("geometries") {
			// try to convert to a geometry collection.
			if x.iter().all(|x| matches!(x, Value::Geometry(_))) {
				let geometries = x
					.iter()
					.map(|x| {
						if let Value::Geometry(x) = x {
							x.clone()
						} else {
							// unsure if unreachable or error here or just default back to return object
							unreachable!()
						}
					})
					.collect();

				return Ok(Value::Geometry(Geometry::Collection(geometries)));
			}
		}
		return Ok(Value::Object(object));
	}

	async fn convert_to_geometry_non_collections(
		&mut self,
		geometry_type: &str,
		object: Object,
	) -> ParseResult<Value> {
		// check if the object has the correct fields.
		let coordinates = match object.get("coordinates") {
			Some(Value::Array(v)) => Value::Array(v.clone()),
			_ => return Ok(Value::Object(object)),
		};
		// convert the coordinates to a geometry.
		let g = match geometry_type {
			"Point" => {
				Geometry::array_to_point(&coordinates).and_then(|x| Some(Geometry::Point(x)))
			}
			"LineString" => {
				Geometry::array_to_line(&coordinates).and_then(|x| Some(Geometry::Line(x)))
			}
			"Polygon" => {
				Geometry::array_to_polygon(&coordinates).and_then(|x| Some(Geometry::Polygon(x)))
			}
			"MultiPoint" => Geometry::array_to_multipoint(&coordinates)
				.and_then(|x| Some(Geometry::MultiPoint(x))),
			"MultiLineString" => Geometry::array_to_multiline(&coordinates)
				.and_then(|x| Some(Geometry::MultiLine(x))),
			"MultiPolygon" => Geometry::array_to_multipolygon(&coordinates)
				.and_then(|x| Some(Geometry::MultiPolygon(x))),
			// unsure if unreachable or error here or just default back to return object
			_ => unreachable!(),
		};

		if let Some(g) = g {
			return Ok(Value::Geometry(g));
		} else {
			return Ok(Value::Object(object));
		}
	}

	/// Parse a production starting with an `{` as either an object or a geometry.
	///
	/// This function tries to match an object to an geometry like object and if it is unable
	/// fallsback to parsing normal objects.
	async fn parse_object_or_geometry(&mut self, ctx: &mut Stk, start: Span) -> ParseResult<Value> {
		// empty object was already matched previously so next must be a key.
		let key = self.parse_object_key()?;

		//parse object
		expected!(self, t!(":"));
		let object = self.parse_object_from_key(ctx, key, BTreeMap::new(), start).await?;

		//check if type + any is a possiblity
		if object.len() != 2 {
			// object has more then two fields, not a geometry.
			return Ok(Value::Object(object));
		};

		//check if type is present
		match object.get("type") {
			Some(Value::Strand(s)) => {
				//check if type value is a sign to convert to data type
				//add new data types conversions here
				let o_type = s.as_str();
				match o_type {
					"Point" | "LineString" | "Polygon" | "MultiPoint" | "MultiLineString"
					| "MultiPolygon" | "GeometryCollection" => {
						return Ok(self.convert_to_geometry(o_type, object.clone()).await);
					}
					//unknown type, just return object
					_ => return Ok(Value::Object(object)),
				}
			}
			_ => {
				// type field was not a strand, not a geometry.
				return Ok(Value::Object(object));
			}
		}
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
