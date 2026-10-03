//! SQL:2023 Part 16 (SQL/PGQ) property graph parser.
//!
//! Covers:
//! - `CREATE PROPERTY GRAPH ...`
//! - `DROP PROPERTY GRAPH ...`
//! - `GRAPH_TABLE(name, MATCH path_pattern COLUMNS (...))`
//! - GPML patterns: vertices, edges, paths, quantifiers, filters

use crate::ast::graph::{
	CreatePropertyGraphStmt, DropPropertyGraphStmt, EdgeTableDef, GraphColumnDef,
	GraphEdgeDirection, GraphEdgePattern, GraphElementPattern, GraphPathPattern, GraphPattern,
	GraphQuantifier, GraphVertexPattern, VertexTableDef,
};
use crate::ast::stmt::TableRef;
use crate::error::{ErrorKind, ParseError, ParseResult};
use crate::lexer::keyword::{Keyword, KeywordCategory};
use crate::lexer::token::TokenKind;
use crate::parser::Parser;
use crate::span::Span;

impl<'a> Parser<'a> {
	/// Parse `CREATE PROPERTY GRAPH` statement tail after `CREATE PROPERTY`.
	pub fn parse_create_property_graph_stmt_tail(
		&mut self,
		start_span: Span,
	) -> ParseResult<CreatePropertyGraphStmt> {
		self.expect(&TokenKind::Keyword(Keyword::Graph), "'GRAPH'")?;
		let if_not_exists = if self.eat(&TokenKind::Keyword(Keyword::If))? {
			self.expect(&TokenKind::Keyword(Keyword::Not), "'NOT'")?;
			self.expect(&TokenKind::Keyword(Keyword::Exists), "'EXISTS'")?;
			true
		} else {
			false
		};

		let name = self.parse_qualified_name()?;

		self.expect(&TokenKind::Keyword(Keyword::Vertex), "'VERTEX'")?;
		self.expect(&TokenKind::Keyword(Keyword::Tables), "'TABLES'")?;
		self.expect(&TokenKind::LParen, "'('")?;
		let mut vertex_tables = Vec::new();
		loop {
			vertex_tables.push(self.parse_vertex_table_def()?);
			if !self.eat(&TokenKind::Comma)? {
				break;
			}
		}
		self.expect(&TokenKind::RParen, "')'")?;

		let mut edge_tables = Vec::new();
		if self.eat(&TokenKind::Keyword(Keyword::Edge))? {
			self.expect(&TokenKind::Keyword(Keyword::Tables), "'TABLES'")?;
			self.expect(&TokenKind::LParen, "'('")?;
			loop {
				edge_tables.push(self.parse_edge_table_def()?);
				if !self.eat(&TokenKind::Comma)? {
					break;
				}
			}
			self.expect(&TokenKind::RParen, "')'")?;
		}

		let end_span = self.peek().span;
		Ok(CreatePropertyGraphStmt {
			if_not_exists,
			name,
			vertex_tables,
			edge_tables,
			span: start_span.merge(end_span),
		})
	}

	/// Parse a vertex table definition within `CREATE PROPERTY GRAPH`.
	pub fn parse_vertex_table_def(&mut self) -> ParseResult<VertexTableDef> {
		let start_span = self.peek().span;
		let table_name = self.parse_qualified_name()?;

		let mut key_columns = Vec::new();
		if self.eat(&TokenKind::Keyword(Keyword::Key))? {
			self.expect(&TokenKind::LParen, "'('")?;
			loop {
				let (col, _) = self.parse_ident()?;
				key_columns.push(col);
				if !self.eat(&TokenKind::Comma)? {
					break;
				}
			}
			self.expect(&TokenKind::RParen, "')'")?;
		}

		let label = if self.eat(&TokenKind::Keyword(Keyword::Label))? {
			let (l, _) = self.parse_ident()?;
			Some(l)
		} else {
			None
		};

		let mut properties = Vec::new();
		if self.eat(&TokenKind::Keyword(Keyword::Properties))? {
			self.expect(&TokenKind::LParen, "'('")?;
			loop {
				let (p, _) = self.parse_ident()?;
				properties.push(p);
				if !self.eat(&TokenKind::Comma)? {
					break;
				}
			}
			self.expect(&TokenKind::RParen, "')'")?;
		}

		let end_span = self.peek().span;
		Ok(VertexTableDef {
			table_name,
			key_columns,
			label,
			properties,
			span: start_span.merge(end_span),
		})
	}

	/// Parse an edge table definition within `CREATE PROPERTY GRAPH`.
	pub fn parse_edge_table_def(&mut self) -> ParseResult<EdgeTableDef> {
		let start_span = self.peek().span;
		let table_name = self.parse_qualified_name()?;

		let mut key_columns = Vec::new();
		if self.eat(&TokenKind::Keyword(Keyword::Key))? {
			self.expect(&TokenKind::LParen, "'('")?;
			loop {
				let (col, _) = self.parse_ident()?;
				key_columns.push(col);
				if !self.eat(&TokenKind::Comma)? {
					break;
				}
			}
			self.expect(&TokenKind::RParen, "')'")?;
		}

		self.expect(&TokenKind::Keyword(Keyword::Source), "'SOURCE'")?;
		let mut source_key = Vec::new();
		if self.eat(&TokenKind::Keyword(Keyword::Key))? {
			self.expect(&TokenKind::LParen, "'('")?;
			loop {
				let (col, _) = self.parse_ident()?;
				source_key.push(col);
				if !self.eat(&TokenKind::Comma)? {
					break;
				}
			}
			self.expect(&TokenKind::RParen, "')'")?;
		}
		self.expect(&TokenKind::Keyword(Keyword::References), "'REFERENCES'")?;
		let source_table = self.parse_qualified_name()?;
		let mut source_references = Vec::new();
		if self.eat(&TokenKind::LParen)? {
			loop {
				let (col, _) = self.parse_ident()?;
				source_references.push(col);
				if !self.eat(&TokenKind::Comma)? {
					break;
				}
			}
			self.expect(&TokenKind::RParen, "')'")?;
		}

		self.expect(&TokenKind::Keyword(Keyword::Target), "'TARGET'")?;
		let mut target_key = Vec::new();
		if self.eat(&TokenKind::Keyword(Keyword::Key))? {
			self.expect(&TokenKind::LParen, "'('")?;
			loop {
				let (col, _) = self.parse_ident()?;
				target_key.push(col);
				if !self.eat(&TokenKind::Comma)? {
					break;
				}
			}
			self.expect(&TokenKind::RParen, "')'")?;
		}
		self.expect(&TokenKind::Keyword(Keyword::References), "'REFERENCES'")?;
		let target_table = self.parse_qualified_name()?;
		let mut target_references = Vec::new();
		if self.eat(&TokenKind::LParen)? {
			loop {
				let (col, _) = self.parse_ident()?;
				target_references.push(col);
				if !self.eat(&TokenKind::Comma)? {
					break;
				}
			}
			self.expect(&TokenKind::RParen, "')'")?;
		}

		let label = if self.eat(&TokenKind::Keyword(Keyword::Label))? {
			let (l, _) = self.parse_ident()?;
			Some(l)
		} else {
			None
		};

		let mut properties = Vec::new();
		if self.eat(&TokenKind::Keyword(Keyword::Properties))? {
			self.expect(&TokenKind::LParen, "'('")?;
			loop {
				let (p, _) = self.parse_ident()?;
				properties.push(p);
				if !self.eat(&TokenKind::Comma)? {
					break;
				}
			}
			self.expect(&TokenKind::RParen, "')'")?;
		}

		let end_span = self.peek().span;
		Ok(EdgeTableDef {
			table_name,
			key_columns,
			source_table,
			source_key,
			source_references,
			target_table,
			target_key,
			target_references,
			label,
			properties,
			span: start_span.merge(end_span),
		})
	}

	/// Parse `DROP PROPERTY GRAPH` statement tail after `DROP PROPERTY`.
	pub fn parse_drop_property_graph_stmt_tail(
		&mut self,
		start_span: Span,
	) -> ParseResult<DropPropertyGraphStmt> {
		self.expect(&TokenKind::Keyword(Keyword::Graph), "'GRAPH'")?;
		let if_exists = if self.eat(&TokenKind::Keyword(Keyword::If))? {
			self.expect(&TokenKind::Keyword(Keyword::Exists), "'EXISTS'")?;
			true
		} else {
			false
		};

		let name = self.parse_qualified_name()?;
		let cascade = if self.eat(&TokenKind::Keyword(Keyword::Cascade))? {
			true
		} else {
			self.eat(&TokenKind::Keyword(Keyword::Restrict))?;
			false
		};

		let end_span = self.peek().span;
		Ok(DropPropertyGraphStmt {
			if_exists,
			name,
			cascade,
			span: start_span.merge(end_span),
		})
	}

	/// Parse `GRAPH_TABLE(...)` table factor tail.
	pub fn parse_graph_table_tail(&mut self, start_span: Span) -> ParseResult<TableRef> {
		self.expect(&TokenKind::LParen, "'('")?;
		let graph_name = self.parse_qualified_name()?;
		self.expect(&TokenKind::Comma, "','")?;
		self.expect(&TokenKind::Keyword(Keyword::Match), "'MATCH'")?;
		let pattern = self.parse_graph_pattern()?;

		let mut columns = Vec::new();
		if self.eat(&TokenKind::Keyword(Keyword::Columns))? {
			self.expect(&TokenKind::LParen, "'('")?;
			loop {
				let col_start = self.peek().span;
				let expr = self.parse_expr()?;
				let alias = if self.eat(&TokenKind::Keyword(Keyword::As))?
					|| self.is_ident_or_unreserved()
				{
					let (a, _) = self.parse_ident()?;
					Some(a)
				} else {
					None
				};
				let col_end = self.peek().span;
				columns.push(GraphColumnDef {
					expr,
					alias,
					span: col_start.merge(col_end),
				});
				if !self.eat(&TokenKind::Comma)? {
					break;
				}
			}
			self.expect(&TokenKind::RParen, "')'")?;
		}

		let rparen = self.expect(&TokenKind::RParen, "')'")?;
		let alias = self.parse_table_alias()?;
		let end_span = alias.as_ref().map(|a| a.span).unwrap_or(rparen.span);

		Ok(TableRef::GraphTable {
			graph_name,
			pattern,
			columns,
			alias,
			span: start_span.merge(end_span),
		})
	}

	/// Parse a graph pattern (comma-separated path patterns with optional WHERE).
	pub fn parse_graph_pattern(&mut self) -> ParseResult<GraphPattern> {
		let start_span = self.peek().span;
		let mut paths = Vec::new();
		loop {
			paths.push(self.parse_graph_path_pattern()?);
			if !self.eat(&TokenKind::Comma)? {
				break;
			}
		}

		let where_clause = if self.eat(&TokenKind::Keyword(Keyword::Where))? {
			Some(self.parse_expr()?)
		} else {
			None
		};

		let end_span = self.peek().span;
		Ok(GraphPattern {
			paths,
			where_clause,
			span: start_span.merge(end_span),
		})
	}

	/// Parse a single graph path pattern.
	pub fn parse_graph_path_pattern(&mut self) -> ParseResult<GraphPathPattern> {
		let start_span = self.peek().span;

		// Check for path variable: `p = (a)...`
		let path_variable = if self.peek().kind != TokenKind::LParen {
			let (var, _) = self.parse_ident()?;
			self.expect(&TokenKind::Operator("="), "'='")?;
			Some(var)
		} else {
			None
		};

		let mut elements = Vec::new();
		// Path starts with vertex pattern
		elements.push(GraphElementPattern::Vertex(self.parse_graph_vertex_pattern()?));

		// Followed by zero or more (edge, vertex) pairs
		while self.is_edge_start() {
			elements.push(GraphElementPattern::Edge(self.parse_graph_edge_pattern()?));
			elements.push(GraphElementPattern::Vertex(self.parse_graph_vertex_pattern()?));
		}

		let end_span = self.peek().span;
		Ok(GraphPathPattern {
			path_variable,
			elements,
			span: start_span.merge(end_span),
		})
	}

	/// Parse a vertex pattern: `( [variable] [:label]* [WHERE expr] )`.
	pub fn parse_graph_vertex_pattern(&mut self) -> ParseResult<GraphVertexPattern> {
		let lparen = self.expect(&TokenKind::LParen, "'('")?;
		let variable = if self.is_ident_or_unreserved()
			&& self.peek().kind != TokenKind::Keyword(Keyword::Where)
		{
			let (v, _) = self.parse_ident()?;
			Some(v)
		} else {
			None
		};

		let mut labels = Vec::new();
		while self.eat(&TokenKind::Colon)? {
			let (l, _) = self.parse_ident()?;
			labels.push(l);
		}

		let where_clause = if self.eat(&TokenKind::Keyword(Keyword::Where))? {
			Some(self.parse_expr()?)
		} else {
			None
		};

		let rparen = self.expect(&TokenKind::RParen, "')'")?;
		Ok(GraphVertexPattern {
			variable,
			labels,
			where_clause,
			span: lparen.span.merge(rparen.span),
		})
	}

	/// Parse an edge pattern (`->`, `<-`, `<->`, `-[...]->`, etc.).
	pub fn parse_graph_edge_pattern(&mut self) -> ParseResult<GraphEdgePattern> {
		let start_tok = self.peek().clone();
		let mut direction;

		match &start_tok.kind {
			TokenKind::Operator("->") => {
				self.bump()?;
				return Ok(GraphEdgePattern {
					direction: GraphEdgeDirection::Right,
					variable: None,
					labels: Vec::new(),
					quantifier: None,
					where_clause: None,
					span: start_tok.span,
				});
			}
			TokenKind::Operator("<->") => {
				self.bump()?;
				return Ok(GraphEdgePattern {
					direction: GraphEdgeDirection::Bidirectional,
					variable: None,
					labels: Vec::new(),
					quantifier: None,
					where_clause: None,
					span: start_tok.span,
				});
			}
			TokenKind::Operator("<-") => {
				self.bump()?;
				if self.eat(&TokenKind::LBracket)? {
					direction = GraphEdgeDirection::Left;
				} else {
					return Ok(GraphEdgePattern {
						direction: GraphEdgeDirection::Left,
						variable: None,
						labels: Vec::new(),
						quantifier: None,
						where_clause: None,
						span: start_tok.span,
					});
				}
			}
			TokenKind::Operator("-") => {
				self.bump()?;
				if self.eat(&TokenKind::LBracket)? {
					direction = GraphEdgeDirection::Any;
				} else if self.peek().kind == TokenKind::Operator(">") {
					let end = self.bump()?.span;
					return Ok(GraphEdgePattern {
						direction: GraphEdgeDirection::Right,
						variable: None,
						labels: Vec::new(),
						quantifier: None,
						where_clause: None,
						span: start_tok.span.merge(end),
					});
				} else {
					return Ok(GraphEdgePattern {
						direction: GraphEdgeDirection::Any,
						variable: None,
						labels: Vec::new(),
						quantifier: None,
						where_clause: None,
						span: start_tok.span,
					});
				}
			}
			_ => {
				return Err(ParseError::new(
					start_tok.span,
					ErrorKind::UnexpectedToken {
						expected: "edge pattern ('->', '<-', '-[', '<-[', or '-')",
						found: start_tok.kind.to_string(),
					},
				));
			}
		}

		// Parse bracketed edge content: `[var:Label* WHERE expr]`
		let variable = if self.is_ident_or_unreserved()
			&& self.peek().kind != TokenKind::Keyword(Keyword::Where)
		{
			let (v, _) = self.parse_ident()?;
			Some(v)
		} else {
			None
		};

		let mut labels = Vec::new();
		while self.eat(&TokenKind::Colon)? {
			let (l, _) = self.parse_ident()?;
			labels.push(l);
		}

		let quantifier = self.parse_optional_graph_quantifier()?;

		let where_clause = if self.eat(&TokenKind::Keyword(Keyword::Where))? {
			Some(self.parse_expr()?)
		} else {
			None
		};

		self.expect(&TokenKind::RBracket, "']'")?;

		// Closing edge delimiter: `->`, `-`, or `>`
		let end_span;
		match &self.peek().kind {
			TokenKind::Operator("->") => {
				let end_tok = self.bump()?;
				end_span = end_tok.span;
				if direction == GraphEdgeDirection::Left {
					direction = GraphEdgeDirection::Bidirectional;
				} else {
					direction = GraphEdgeDirection::Right;
				}
			}
			TokenKind::Operator("-") => {
				let end_tok = self.bump()?;
				end_span = end_tok.span;
			}
			TokenKind::Operator(">") => {
				let end_tok = self.bump()?;
				end_span = end_tok.span;
				if direction == GraphEdgeDirection::Left {
					direction = GraphEdgeDirection::Bidirectional;
				} else {
					direction = GraphEdgeDirection::Right;
				}
			}
			_ => {
				return Err(ParseError::new(
					self.peek().span,
					ErrorKind::UnexpectedToken {
						expected: "closing edge delimiter ('->' or '-')",
						found: self.peek().kind.to_string(),
					},
				));
			}
		}

		Ok(GraphEdgePattern {
			direction,
			variable,
			labels,
			quantifier,
			where_clause,
			span: start_tok.span.merge(end_span),
		})
	}

	/// Parse an optional graph edge quantifier: `*`, `+`, `?`, `{n}`, `{m,n}`, `{m,}`.
	fn parse_optional_graph_quantifier(&mut self) -> ParseResult<Option<GraphQuantifier>> {
		match &self.peek().kind {
			TokenKind::Operator("*") => {
				self.bump()?;
				Ok(Some(GraphQuantifier::ZeroOrMore))
			}
			TokenKind::Operator("+") => {
				self.bump()?;
				Ok(Some(GraphQuantifier::OneOrMore))
			}
			TokenKind::Operator("?") => {
				self.bump()?;
				Ok(Some(GraphQuantifier::ZeroOrOne))
			}
			TokenKind::LBrace => {
				self.bump()?;
				let min_tok = self.peek().clone();
				let min = match &min_tok.kind {
					TokenKind::Integer(i) => {
						self.bump()?;
						*i as u32
					}
					_ => {
						return Err(ParseError::new(
							min_tok.span,
							ErrorKind::UnexpectedToken {
								expected: "integer quantifier",
								found: min_tok.kind.to_string(),
							},
						));
					}
				};

				if self.eat(&TokenKind::Comma)? {
					let max = if self.peek().kind == TokenKind::RBrace {
						None
					} else {
						let max_tok = self.peek().clone();
						match &max_tok.kind {
							TokenKind::Integer(i) => {
								self.bump()?;
								Some(*i as u32)
							}
							_ => {
								return Err(ParseError::new(
									max_tok.span,
									ErrorKind::UnexpectedToken {
										expected: "integer quantifier or '}'",
										found: max_tok.kind.to_string(),
									},
								));
							}
						}
					};
					self.expect(&TokenKind::RBrace, "'}'")?;
					Ok(Some(GraphQuantifier::Range(min, max)))
				} else {
					self.expect(&TokenKind::RBrace, "'}'")?;
					Ok(Some(GraphQuantifier::Exact(min)))
				}
			}
			_ => Ok(None),
		}
	}

	/// Helper to determine if current token can start an edge pattern.
	fn is_edge_start(&self) -> bool {
		matches!(
			&self.peek().kind,
			TokenKind::Operator("-")
				| TokenKind::Operator("->")
				| TokenKind::Operator("<-")
				| TokenKind::Operator("<->")
		)
	}

	/// Helper to check if current token can be an identifier or unreserved keyword.
	pub(crate) fn is_ident_or_unreserved(&self) -> bool {
		match &self.peek().kind {
			TokenKind::Ident(_) | TokenKind::QuotedIdent(_) => true,
			TokenKind::Keyword(kw) => kw.category() != KeywordCategory::Reserved,
			_ => false,
		}
	}
}
