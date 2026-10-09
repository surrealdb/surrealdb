//! Parsing of the GQL data-modifying statements: `INSERT`, `SET`, `REMOVE`
//! and `DELETE` (§13 of `doc/gql/GQL.g4`).
//!
//! `SET`/`REMOVE`/`DELETE` operate on variables bound by a preceding `MATCH`;
//! `INSERT` describes new nodes and edges via an insert graph pattern
//! (`insertGraphPattern`, GQL.g4:852). The insert pattern is structurally
//! simpler than a MATCH pattern (no `WHERE`, no quantifiers, no path-search
//! prefix, no path variable, a single plain label rather than a label
//! expression, and only the two directed edge forms), so it has its own
//! parser here rather than reusing the MATCH pattern parser; only leaf helpers
//! are shared.

use crate::gql::ast::{
	DeleteStatement, DetachMode, GqlExpr, Ident, InsertEdge, InsertEdgeDir, InsertNode, InsertPath,
	InsertStatement, RemoveItem, RemoveStatement, SetItem, SetStatement,
};
use crate::gql::parser::mac::{expected, unexpected};
use crate::gql::parser::{ParseResult, Parser};
use crate::gql::token::{Keyword, Span, TokenKind, t};
use crate::syn::error::bail;

/// An insert element filler: its variable, its label, and its properties.
type InsertFiller = (Option<Ident>, Option<Ident>, Vec<(Ident, GqlExpr)>);

impl Parser<'_> {
	/// Parse a `SET` statement: `SET setItem (, setItem)*` (GQL.g4:427). The
	/// `SET` keyword must already be consumed; `start` is its span.
	pub(super) fn parse_set_statement(&mut self, start: Span) -> ParseResult<SetStatement> {
		let mut items = Vec::new();
		loop {
			let item_start = self.peek().span;
			let var = self.parse_ident()?;
			let token = self.peek();
			let item = match token.kind {
				// setPropertyItem: `a.p = v`
				t!(".") => {
					self.pop_peek();
					let prop = self.parse_ident()?;
					expected!(self, t!("="));
					let value = self.parse_expr()?;
					SetItem::Property {
						var,
						prop,
						value,
						span: item_start.covers(self.last_span()),
					}
				}
				// setAllPropertiesItem: `a = { k: v, … }`
				t!("=") => {
					self.pop_peek();
					let props = self.parse_property_map()?;
					SetItem::AllProperties {
						var,
						props,
						span: item_start.covers(self.last_span()),
					}
				}
				// setLabelItem: `a:Label` / `a IS Label` (rejected in lowering).
				t!(":") | t!("IS") => {
					self.pop_peek();
					let label = self.parse_ident()?;
					SetItem::Label {
						var,
						label,
						span: item_start.covers(self.last_span()),
					}
				}
				_ => unexpected!(self, token, "`.`, `=`, `:` or `IS`"),
			};
			items.push(item);
			if !self.eat(t!(",")) {
				break;
			}
		}
		Ok(SetStatement {
			items,
			span: start.covers(self.last_span()),
		})
	}

	/// Parse a `REMOVE` statement: `REMOVE removeItem (, removeItem)*`
	/// (GQL.g4:455). The `REMOVE` keyword must already be consumed.
	pub(super) fn parse_remove_statement(&mut self, start: Span) -> ParseResult<RemoveStatement> {
		let mut items = Vec::new();
		loop {
			let item_start = self.peek().span;
			let var = self.parse_ident()?;
			let token = self.peek();
			let item = match token.kind {
				// removePropertyItem: `a.p`
				t!(".") => {
					self.pop_peek();
					let prop = self.parse_ident()?;
					RemoveItem::Property {
						var,
						prop,
						span: item_start.covers(self.last_span()),
					}
				}
				// removeLabelItem: `a:Label` / `a IS Label` (rejected in lowering).
				t!(":") | t!("IS") => {
					self.pop_peek();
					let label = self.parse_ident()?;
					RemoveItem::Label {
						var,
						label,
						span: item_start.covers(self.last_span()),
					}
				}
				_ => unexpected!(self, token, "`.`, `:` or `IS`"),
			};
			items.push(item);
			if !self.eat(t!(",")) {
				break;
			}
		}
		Ok(RemoveStatement {
			items,
			span: start.covers(self.last_span()),
		})
	}

	/// Parse a `DELETE` statement: `deleteItem (, deleteItem)*` (GQL.g4:482).
	/// The `DELETE` keyword (and any `DETACH`/`NODETACH` prefix) must already be
	/// consumed; `detach` is the resolved mode and `start` the prefix span.
	pub(super) fn parse_delete_statement(
		&mut self,
		detach: DetachMode,
		start: Span,
	) -> ParseResult<DeleteStatement> {
		let mut items = Vec::new();
		loop {
			// deleteItem = valueExpression; the lowering checks each is a bound
			// variable reference.
			items.push(self.parse_expr()?);
			if !self.eat(t!(",")) {
				break;
			}
		}
		Ok(DeleteStatement {
			detach,
			items,
			span: start.covers(self.last_span()),
		})
	}

	/// Parse an `INSERT` statement: `INSERT insertGraphPattern` (GQL.g4:421).
	/// The `INSERT` keyword must already be consumed; `start` is its span.
	pub(super) fn parse_insert_statement(&mut self, start: Span) -> ParseResult<InsertStatement> {
		let mut paths = Vec::new();
		loop {
			paths.push(self.parse_insert_path()?);
			if !self.eat(t!(",")) {
				break;
			}
		}
		Ok(InsertStatement {
			paths,
			span: start.covers(self.last_span()),
		})
	}

	/// Parse a single insert path: `insertNodePattern (insertEdgePattern
	/// insertNodePattern)*` (GQL.g4:860).
	fn parse_insert_path(&mut self) -> ParseResult<InsertPath> {
		let start = self.peek().span;
		let first = self.parse_insert_node()?;
		let mut steps = Vec::new();
		while let Some(edge) = self.parse_insert_edge()? {
			let node = self.parse_insert_node()?;
			steps.push((edge, node));
		}
		Ok(InsertPath {
			start: first,
			steps,
			span: start.covers(self.last_span()),
		})
	}

	/// Parse an insert node pattern: `( insertElementPatternFiller? )`
	/// (GQL.g4:864).
	fn parse_insert_node(&mut self) -> ParseResult<InsertNode> {
		let open = expected!(self, t!("(")).span;
		let (var, label, props) = self.parse_insert_filler()?;
		self.expect_closing_delimiter(t!(")"), open)?;
		Ok(InsertNode {
			var,
			label,
			props,
			span: open.covers(self.last_span()),
		})
	}

	/// Parse an insert edge pattern (GQL.g4:868). Returns `None` when the next
	/// token does not open an edge. Only the two directed bracketed forms
	/// (`-[…]->` and `<-[…]-`) are supported; undirected and multidirectional
	/// forms are rejected.
	fn parse_insert_edge(&mut self) -> ParseResult<Option<InsertEdge>> {
		let token = self.peek();
		let direction = match token.kind {
			t!("-[") => InsertEdgeDir::Right,
			t!("<-[") => InsertEdgeDir::Left,
			t!("~[") | t!("<~[") => {
				bail!("Undirected INSERT edges are not supported", @token.span);
			}
			_ => return Ok(None),
		};
		let open = token.span;
		self.pop_peek();
		let (var, label, props) = self.parse_insert_filler()?;
		let close = self.next();
		match (direction, close.kind) {
			(InsertEdgeDir::Right, t!("]->")) | (InsertEdgeDir::Left, t!("]-")) => {}
			// `-[…]-` (ANY) and `<-[…]->` (LEFT_OR_RIGHT) are multidirectional.
			_ => {
				bail!(
					"Only directed INSERT edges (`-[…]->` or `<-[…]-`) are supported",
					@close.span
				);
			}
		}
		Ok(Some(InsertEdge {
			var,
			label,
			direction,
			props,
			span: open.covers(self.last_span()),
		}))
	}

	/// Parse an insert element filler: `elementVariableDeclaration?
	/// labelAndPropertySetSpecification?` (GQL.g4:886). All parts are optional;
	/// the label is a single plain table name (not a label expression).
	fn parse_insert_filler(&mut self) -> ParseResult<InsertFiller> {
		// Optional variable declaration.
		let token = self.peek();
		let var = match token.kind {
			kind if Self::token_can_be_ident(kind) => Some(self.parse_ident()?),
			// A reserved word cannot be a variable; `IS` introduces a label and
			// is handled below.
			TokenKind::Keyword(keyword) if !matches!(keyword, Keyword::Is) => {
				bail!(
					"`{}` is a reserved word and cannot be used as a variable name",
					self.span_str(token.span),
					@token.span => "use a `\"…\"` or `` `…` `` delimited identifier instead"
				);
			}
			_ => None,
		};
		// Optional label (introduced by `:` or `IS`, `isOrColon` GQL.g4:1024).
		let label = if self.eat(t!(":")) || self.eat(t!("IS")) {
			Some(self.parse_ident()?)
		} else {
			None
		};
		// Optional property map.
		let props = if self.peek_kind() == t!("{") {
			self.parse_property_map()?
		} else {
			Vec::new()
		};
		Ok((var, label, props))
	}

	/// Parse a property map `{ k: v, … }` (`propertyKeyValuePairList`,
	/// GQL.g4:1018) shared by `SET … = {…}` and insert fillers. The opening
	/// brace must be next.
	fn parse_property_map(&mut self) -> ParseResult<Vec<(Ident, GqlExpr)>> {
		let open = expected!(self, t!("{")).span;
		let mut props = Vec::new();
		if self.peek_kind() != t!("}") {
			loop {
				let key = self.parse_ident()?;
				expected!(self, t!(":"));
				let value = self.parse_expr()?;
				props.push((key, value));
				if !self.eat(t!(",")) {
					break;
				}
			}
		}
		self.expect_closing_delimiter(t!("}"), open)?;
		Ok(props)
	}
}
