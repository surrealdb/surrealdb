//! Expression parser implementing PostgreSQL `a_expr`, `b_expr`, and `c_expr`.

use crate::ast::expr::{ColumnRef, Expr, QualifiedName, TypeName, WhenClause};
use crate::ast::literal::Literal;
use crate::ast::op::{BinaryOperator, UnaryOperator};
use crate::error::{ErrorKind, ParseError, ParseResult};
use crate::lexer::keyword::Keyword;
use crate::lexer::token::TokenKind;
use crate::parser::Parser;
use crate::parser::precedence::{infix_binding_power, prefix_binding_power};
use crate::span::Span;

impl<'a> Parser<'a> {
	/// Parse an expression using Pratt operator precedence climbing.
	///
	/// gram.y: a_expr
	pub fn parse_expr(&mut self) -> ParseResult<Expr> {
		self.parse_expr_bp(0)
	}

	/// Parse an expression with minimum binding power.
	pub fn parse_expr_bp(&mut self, min_bp: u8) -> ParseResult<Expr> {
		self.enter_recursion(self.peek().span)?;

		// Prefix / Prefix operator handling
		let mut left = if let Some(prefix_bp) = prefix_binding_power(&self.peek().kind) {
			let op_tok = self.bump()?;
			let right = self.parse_expr_bp(prefix_bp)?;
			let span = op_tok.span.merge(right.span());

			let unary_op = match &op_tok.kind {
				TokenKind::Keyword(Keyword::Not) => UnaryOperator::Not,
				TokenKind::Operator("+") => UnaryOperator::Plus,
				TokenKind::Operator("-") => UnaryOperator::Minus,
				TokenKind::Operator("~") => UnaryOperator::BitNot,
				TokenKind::Operator(s) => UnaryOperator::Custom(s.to_string()),
				_ => {
					return Err(ParseError::new(
						op_tok.span,
						ErrorKind::InvalidExpression("invalid prefix operator"),
					));
				}
			};

			Expr::UnaryOp {
				op: unary_op,
				expr: Box::new(right),
				span,
			}
		} else {
			self.parse_primary_expr()?
		};

		// Infix / Postfix operator loop
		while let Some((l_bp, r_bp, _assoc)) = infix_binding_power(&self.peek().kind) {
			if l_bp < min_bp {
				break;
			}

			let op_tok = self.peek().clone();

			// 1. Typecast `::`
			if op_tok.kind == TokenKind::DoubleColon {
				self.bump()?; // consume '::'
				let data_type = self.parse_type_name()?;
				let span = left.span().merge(data_type.span);
				left = Expr::TypeCast {
					expr: Box::new(left),
					data_type,
					span,
				};
				continue;
			}

			// 2. Subscript or Slice `[`
			if op_tok.kind == TokenKind::LBracket {
				self.bump()?; // consume '['
				let (subscript, is_slice, upper) = if self.eat(&TokenKind::Colon)? {
					// Slice without lower bound: `[:upper]`
					let upper = if self.peek().kind == TokenKind::RBracket {
						None
					} else {
						Some(Box::new(self.parse_expr()?))
					};
					(None, true, upper)
				} else {
					let first = self.parse_expr()?;
					if self.eat(&TokenKind::Colon)? {
						// Slice with lower bound: `[lower:upper]` or `[lower:]`
						let upper = if self.peek().kind == TokenKind::RBracket {
							None
						} else {
							Some(Box::new(self.parse_expr()?))
						};
						(Some(Box::new(first)), true, upper)
					} else {
						// Standard single subscript `[index]`
						(Some(Box::new(first)), false, None)
					}
				};

				let r_bracket = self.expect(&TokenKind::RBracket, "']'")?;
				let span = left.span().merge(r_bracket.span);

				left = if is_slice {
					Expr::Slice {
						expr: Box::new(left),
						lower: subscript,
						upper,
						span,
					}
				} else if let Some(index) = subscript {
					Expr::Subscript {
						expr: Box::new(left),
						index,
						span,
					}
				} else {
					return Err(ParseError::new(
						span,
						ErrorKind::InvalidExpression("empty array subscript"),
					));
				};
				continue;
			}

			// 3. COLLATE
			if op_tok.kind == TokenKind::Keyword(Keyword::Collate) {
				self.bump()?; // consume 'COLLATE'
				let collation = self.parse_qualified_name()?;
				let span = left.span().merge(collation.span);
				left = Expr::Collate {
					expr: Box::new(left),
					collation,
					span,
				};
				continue;
			}

			// 4. IS [NOT] NULL / TRUE / FALSE / UNKNOWN / DISTINCT FROM
			if op_tok.kind == TokenKind::Keyword(Keyword::Is) {
				self.bump()?; // consume 'IS'
				let negated = self.eat(&TokenKind::Keyword(Keyword::Not))?;

				match &self.peek().kind {
					TokenKind::Keyword(Keyword::Null) => {
						let null_tok = self.bump()?;
						let span = left.span().merge(null_tok.span);
						left = Expr::IsNull {
							expr: Box::new(left),
							negated,
							span,
						};
					}
					TokenKind::Keyword(Keyword::True) => {
						let true_tok = self.bump()?;
						let span = left.span().merge(true_tok.span);
						left = Expr::IsTrue {
							expr: Box::new(left),
							negated,
							span,
						};
					}
					TokenKind::Keyword(Keyword::False) => {
						let false_tok = self.bump()?;
						let span = left.span().merge(false_tok.span);
						left = Expr::IsFalse {
							expr: Box::new(left),
							negated,
							span,
						};
					}
					TokenKind::Keyword(Keyword::Unknown) => {
						let unk_tok = self.bump()?;
						let span = left.span().merge(unk_tok.span);
						left = Expr::IsUnknown {
							expr: Box::new(left),
							negated,
							span,
						};
					}
					TokenKind::Keyword(Keyword::Distinct) => {
						self.bump()?; // consume 'DISTINCT'
						self.expect(&TokenKind::Keyword(Keyword::From), "'FROM'")?;
						let right = self.parse_expr_bp(r_bp)?;
						let span = left.span().merge(right.span());
						left = Expr::IsDistinctFrom {
							left: Box::new(left),
							right: Box::new(right),
							negated,
							span,
						};
					}
					_ => {
						return Err(ParseError::new(
							self.peek().span,
							ErrorKind::UnexpectedToken {
								expected: "NULL, TRUE, FALSE, UNKNOWN, or DISTINCT FROM",
								found: self.peek().kind.to_string(),
							},
						));
					}
				}
				continue;
			}

			// 5. ISNULL / NOTNULL keywords
			if op_tok.kind == TokenKind::Keyword(Keyword::Isnull) {
				let tok = self.bump()?;
				let span = left.span().merge(tok.span);
				left = Expr::IsNull {
					expr: Box::new(left),
					negated: false,
					span,
				};
				continue;
			}
			if op_tok.kind == TokenKind::Keyword(Keyword::Notnull) {
				let tok = self.bump()?;
				let span = left.span().merge(tok.span);
				left = Expr::IsNull {
					expr: Box::new(left),
					negated: true,
					span,
				};
				continue;
			}

			// 6. BETWEEN [SYMMETRIC] low AND high
			if op_tok.kind == TokenKind::Keyword(Keyword::Between) {
				self.bump()?; // consume 'BETWEEN'
				let symmetric = self.eat(&TokenKind::Keyword(Keyword::Symmetric))?;
				let low = self.parse_expr_bp(r_bp)?;
				self.expect(&TokenKind::Keyword(Keyword::And), "'AND'")?;
				let high = self.parse_expr_bp(r_bp)?;
				let span = left.span().merge(high.span());
				left = Expr::Between {
					expr: Box::new(left),
					low: Box::new(low),
					high: Box::new(high),
					negated: false,
					symmetric,
					span,
				};
				continue;
			}

			// 7. IN (list...)
			if op_tok.kind == TokenKind::Keyword(Keyword::In) {
				self.bump()?; // consume 'IN'
				self.expect(&TokenKind::LParen, "'('")?;
				let mut list = Vec::new();
				while self.peek().kind != TokenKind::RParen {
					list.push(self.parse_expr()?);
					if !self.eat(&TokenKind::Comma)? {
						break;
					}
				}
				let r_paren = self.expect(&TokenKind::RParen, "')'")?;
				let span = left.span().merge(r_paren.span);
				left = Expr::InList {
					expr: Box::new(left),
					list,
					negated: false,
					span,
				};
				continue;
			}

			// 8. General Binary Operator
			self.bump()?; // consume operator
			let binary_op = token_to_binary_op(&op_tok.kind)?;
			let right = self.parse_expr_bp(r_bp)?;
			let span = left.span().merge(right.span());
			left = Expr::BinaryOp {
				left: Box::new(left),
				op: binary_op,
				right: Box::new(right),
				span,
			};
		}

		self.leave_recursion();
		Ok(left)
	}

	/// Parse a primary expression (literal, column reference, parenthesis, array, row, case, cast).
	///
	/// gram.y: c_expr
	pub fn parse_primary_expr(&mut self) -> ParseResult<Expr> {
		let tok = self.peek().clone();

		match &tok.kind {
			// Integer literal
			TokenKind::Integer(n) => {
				self.bump()?;
				Ok(Expr::Literal {
					lit: Literal::Integer(*n),
					span: tok.span,
				})
			}
			TokenKind::IntegerRaw(s) => {
				self.bump()?;
				let val = parse_int_literal(s);
				Ok(Expr::Literal {
					lit: Literal::Integer(val),
					span: tok.span,
				})
			}

			// Float literal
			TokenKind::Float(f) => {
				self.bump()?;
				Ok(Expr::Literal {
					lit: Literal::Float(*f),
					span: tok.span,
				})
			}
			TokenKind::FloatRaw(s) => {
				self.bump()?;
				let val = s.replace('_', "").parse::<f64>().unwrap_or(0.0);
				Ok(Expr::Literal {
					lit: Literal::Float(val),
					span: tok.span,
				})
			}

			// String constant
			TokenKind::StringConstant(s) => {
				self.bump()?;
				Ok(Expr::Literal {
					lit: Literal::String(s.clone()),
					span: tok.span,
				})
			}

			// Dollar-quoted string
			TokenKind::DollarQuotedString {
				content,
				..
			} => {
				self.bump()?;
				Ok(Expr::Literal {
					lit: Literal::String(content.to_string()),
					span: tok.span,
				})
			}

			// Bit-string literal
			TokenKind::BitString {
				is_hex,
				content,
			} => {
				self.bump()?;
				Ok(Expr::Literal {
					lit: Literal::BitString {
						is_hex: *is_hex,
						value: content.to_string(),
					},
					span: tok.span,
				})
			}

			// Boolean TRUE
			TokenKind::Keyword(Keyword::True) => {
				self.bump()?;
				Ok(Expr::Literal {
					lit: Literal::Boolean(true),
					span: tok.span,
				})
			}

			// Boolean FALSE
			TokenKind::Keyword(Keyword::False) => {
				self.bump()?;
				Ok(Expr::Literal {
					lit: Literal::Boolean(false),
					span: tok.span,
				})
			}

			// NULL literal
			TokenKind::Keyword(Keyword::Null) => {
				self.bump()?;
				Ok(Expr::Literal {
					lit: Literal::Null,
					span: tok.span,
				})
			}

			// Positional parameter `$1`, `$2`
			TokenKind::Param(id) => {
				self.bump()?;
				Ok(Expr::Param {
					id: *id,
					span: tok.span,
				})
			}

			// Parenthesized expression `(expr)` or Row `(a, b, c)`
			TokenKind::LParen => {
				let l_paren = self.bump()?;
				let first = self.parse_expr()?;

				if self.eat(&TokenKind::Comma)? {
					// Row constructor `(a, b, ...)`
					let mut fields = vec![first];
					while self.peek().kind != TokenKind::RParen {
						fields.push(self.parse_expr()?);
						if !self.eat(&TokenKind::Comma)? {
							break;
						}
					}
					let r_paren = self.expect(&TokenKind::RParen, "')'")?;
					let span = l_paren.span.merge(r_paren.span);
					Ok(Expr::Row {
						fields,
						span,
					})
				} else {
					let r_paren = self.expect(&TokenKind::RParen, "')'")?;
					let span = l_paren.span.merge(r_paren.span);
					Ok(Expr::Grouping {
						expr: Box::new(first),
						span,
					})
				}
			}

			// Explicit Array constructor `ARRAY[...]`
			TokenKind::Keyword(Keyword::Array) => {
				let arr_tok = self.bump()?;
				self.expect(&TokenKind::LBracket, "'['")?;
				let mut elements = Vec::new();
				while self.peek().kind != TokenKind::RBracket {
					elements.push(self.parse_expr()?);
					if !self.eat(&TokenKind::Comma)? {
						break;
					}
				}
				let r_bracket = self.expect(&TokenKind::RBracket, "']'")?;
				let span = arr_tok.span.merge(r_bracket.span);
				Ok(Expr::ArrayConstructor {
					elements,
					span,
				})
			}

			// Explicit Row constructor `ROW(...)`
			TokenKind::Keyword(Keyword::Row) => {
				let row_tok = self.bump()?;
				self.expect(&TokenKind::LParen, "'('")?;
				let mut fields = Vec::new();
				while self.peek().kind != TokenKind::RParen {
					fields.push(self.parse_expr()?);
					if !self.eat(&TokenKind::Comma)? {
						break;
					}
				}
				let r_paren = self.expect(&TokenKind::RParen, "')'")?;
				let span = row_tok.span.merge(r_paren.span);
				Ok(Expr::Row {
					fields,
					span,
				})
			}

			// CAST(expr AS type)
			TokenKind::Keyword(Keyword::Cast) => {
				let cast_tok = self.bump()?;
				self.expect(&TokenKind::LParen, "'('")?;
				let expr = self.parse_expr()?;
				self.expect(&TokenKind::Keyword(Keyword::As), "'AS'")?;
				let data_type = self.parse_type_name()?;
				let r_paren = self.expect(&TokenKind::RParen, "')'")?;
				let span = cast_tok.span.merge(r_paren.span);
				Ok(Expr::TypeCast {
					expr: Box::new(expr),
					data_type,
					span,
				})
			}

			// CASE [operand] WHEN ... THEN ... [ELSE ...] END
			TokenKind::Keyword(Keyword::Case) => self.parse_case_expr(),

			// Identifiers and function calls
			TokenKind::Ident(_) | TokenKind::QuotedIdent(_) => {
				let qname = self.parse_qualified_name()?;
				self.parse_ident_or_func(qname)
			}
			TokenKind::Keyword(kw)
				if kw.category() != crate::lexer::keyword::KeywordCategory::Reserved =>
			{
				let qname = self.parse_qualified_name()?;
				self.parse_ident_or_func(qname)
			}

			_ => Err(ParseError::new(
				tok.span,
				ErrorKind::UnexpectedToken {
					expected: "expression",
					found: tok.kind.to_string(),
				},
			)),
		}
	}

	/// Helper to finish parsing an identifier or function call.
	fn parse_ident_or_func(&mut self, qname: QualifiedName) -> ParseResult<Expr> {
		// Check if followed by `(` -> Function Call
		if self.peek().kind == TokenKind::LParen {
			self.bump()?; // consume '('
			let distinct = self.eat(&TokenKind::Keyword(Keyword::Distinct))?;
			let mut args = Vec::new();

			// Check for `*` in COUNT(*)
			if self.peek().kind == TokenKind::Operator("*") {
				let star_tok = self.bump()?;
				args.push(Expr::ColumnRef(ColumnRef {
					fields: vec!["*".to_string()],
					span: star_tok.span,
				}));
			} else {
				while self.peek().kind != TokenKind::RParen {
					args.push(self.parse_expr()?);
					if !self.eat(&TokenKind::Comma)? {
						break;
					}
				}
			}

			let r_paren = self.expect(&TokenKind::RParen, "')'")?;
			let span = qname.span.merge(r_paren.span);
			Ok(Expr::FunctionCall {
				name: qname,
				args,
				distinct,
				span,
			})
		} else {
			Ok(Expr::ColumnRef(ColumnRef {
				fields: qname.parts,
				span: qname.span,
			}))
		}
	}

	/// Parse a CASE expression (`CASE ... END`).
	///
	/// gram.y: CaseExpr
	fn parse_case_expr(&mut self) -> ParseResult<Expr> {
		let case_tok = self.expect(&TokenKind::Keyword(Keyword::Case), "'CASE'")?;

		// Check if simple case with operand: `CASE operand WHEN ...`
		let operand = if self.peek().kind != TokenKind::Keyword(Keyword::When) {
			Some(Box::new(self.parse_expr()?))
		} else {
			None
		};

		let mut branches = Vec::new();
		while self.peek().kind == TokenKind::Keyword(Keyword::When) {
			let when_tok = self.bump()?;
			let when = self.parse_expr()?;
			self.expect(&TokenKind::Keyword(Keyword::Then), "'THEN'")?;
			let then = self.parse_expr()?;
			let span = when_tok.span.merge(then.span());
			branches.push(WhenClause {
				when,
				then,
				span,
			});
		}

		if branches.is_empty() {
			return Err(ParseError::new(
				self.peek().span,
				ErrorKind::UnexpectedToken {
					expected: "'WHEN'",
					found: self.peek().kind.to_string(),
				},
			));
		}

		let else_expr = if self.eat(&TokenKind::Keyword(Keyword::Else))? {
			Some(Box::new(self.parse_expr()?))
		} else {
			None
		};

		let end_tok = self.expect(&TokenKind::Keyword(Keyword::End), "'END'")?;
		let span = case_tok.span.merge(end_tok.span);

		Ok(Expr::Case {
			operand,
			branches,
			else_expr,
			span,
		})
	}

	/// Parse a qualified name (`ident` or `ident.ident...`).
	pub fn parse_qualified_name(&mut self) -> ParseResult<QualifiedName> {
		let (first, start_span) = self.parse_ident()?;
		let mut parts = vec![first];
		let mut end_span = start_span;

		while self.eat(&TokenKind::Dot)? {
			let (part, part_span) = self.parse_ident()?;
			parts.push(part);
			end_span = part_span;
		}

		let span = start_span.merge(end_span);
		Ok(QualifiedName::new(parts, span))
	}

	/// Parse a single identifier token.
	pub fn parse_ident(&mut self) -> ParseResult<(String, Span)> {
		let tok = self.peek().clone();
		match &tok.kind {
			TokenKind::Ident(s) => {
				self.bump()?;
				Ok((s.to_string(), tok.span))
			}
			TokenKind::QuotedIdent(s) => {
				self.bump()?;
				Ok((s.clone(), tok.span))
			}
			TokenKind::Keyword(kw)
				if kw.category() != crate::lexer::keyword::KeywordCategory::Reserved =>
			{
				self.bump()?;
				Ok((kw.as_str().to_lowercase(), tok.span))
			}
			_ => Err(ParseError::new(
				tok.span,
				ErrorKind::UnexpectedToken {
					expected: "identifier",
					found: tok.kind.to_string(),
				},
			)),
		}
	}

	/// Parse a PostgreSQL data type name (e.g. `INTEGER`, `VARCHAR(255)`, `TEXT[]`).
	pub fn parse_type_name(&mut self) -> ParseResult<TypeName> {
		let name = self.parse_qualified_name()?;
		let mut modifiers = Vec::new();
		let mut array_bounds = Vec::new();
		let mut end_span = name.span;

		// Check for modifiers e.g. `(255)` or `(10, 2)`
		if self.eat(&TokenKind::LParen)? {
			while self.peek().kind != TokenKind::RParen {
				let tok = self.bump()?;
				modifiers.push(tok.kind.to_string());
				if !self.eat(&TokenKind::Comma)? {
					break;
				}
			}
			let r_paren = self.expect(&TokenKind::RParen, "')'")?;
			end_span = end_span.merge(r_paren.span);
		}

		// Check for array dimensions e.g. `[]` or `[10]`
		while self.eat(&TokenKind::LBracket)? {
			let bound = if let TokenKind::Integer(n) = self.peek().kind {
				self.bump()?;
				Some(n)
			} else {
				None
			};
			let r_bracket = self.expect(&TokenKind::RBracket, "']'")?;
			end_span = end_span.merge(r_bracket.span);
			array_bounds.push(bound);
		}

		let span = name.span.merge(end_span);
		Ok(TypeName {
			name,
			modifiers,
			array_bounds,
			span,
		})
	}
}

/// Helper to convert token kind to binary operator.
fn token_to_binary_op(tok: &TokenKind<'_>) -> ParseResult<BinaryOperator> {
	match tok {
		TokenKind::Keyword(Keyword::Or) => Ok(BinaryOperator::Or),
		TokenKind::Keyword(Keyword::And) => Ok(BinaryOperator::And),
		TokenKind::Keyword(Keyword::Like) => Ok(BinaryOperator::Like),
		TokenKind::Keyword(Keyword::Ilike) => Ok(BinaryOperator::ILike),
		TokenKind::Keyword(Keyword::Similar) => Ok(BinaryOperator::SimilarTo),

		TokenKind::Operator("+") => Ok(BinaryOperator::Add),
		TokenKind::Operator("-") => Ok(BinaryOperator::Sub),
		TokenKind::Operator("*") => Ok(BinaryOperator::Mul),
		TokenKind::Operator("/") => Ok(BinaryOperator::Div),
		TokenKind::Operator("%") => Ok(BinaryOperator::Mod),
		TokenKind::Operator("^") => Ok(BinaryOperator::Exp),

		TokenKind::Operator("=") => Ok(BinaryOperator::Eq),
		TokenKind::Operator("<>") | TokenKind::Operator("!=") => Ok(BinaryOperator::NotEq),
		TokenKind::Operator("<") => Ok(BinaryOperator::Lt),
		TokenKind::Operator("<=") => Ok(BinaryOperator::LtEq),
		TokenKind::Operator(">") => Ok(BinaryOperator::Gt),
		TokenKind::Operator(">=") => Ok(BinaryOperator::GtEq),

		TokenKind::Operator("||") => Ok(BinaryOperator::Concat),
		TokenKind::Operator("&") => Ok(BinaryOperator::BitAnd),
		TokenKind::Operator("|") => Ok(BinaryOperator::BitOr),
		TokenKind::Operator("#") => Ok(BinaryOperator::BitXor),
		TokenKind::Operator("<<") => Ok(BinaryOperator::ShiftLeft),
		TokenKind::Operator(">>") => Ok(BinaryOperator::ShiftRight),

		TokenKind::Operator("->") => Ok(BinaryOperator::Arrow),
		TokenKind::Operator("->>") => Ok(BinaryOperator::LongArrow),
		TokenKind::Operator("#>") => Ok(BinaryOperator::HashArrow),
		TokenKind::Operator("#>>") => Ok(BinaryOperator::HashLongArrow),
		TokenKind::Operator("@>") => Ok(BinaryOperator::Contains),
		TokenKind::Operator("<@") => Ok(BinaryOperator::ContainedBy),

		TokenKind::Operator("~") => Ok(BinaryOperator::RegexMatch),
		TokenKind::Operator("~*") => Ok(BinaryOperator::RegexMatchCi),
		TokenKind::Operator("!~") => Ok(BinaryOperator::RegexNotMatch),
		TokenKind::Operator("!~*") => Ok(BinaryOperator::RegexNotMatchCi),

		TokenKind::Operator(s) => Ok(BinaryOperator::Custom(s.to_string())),

		_ => Err(ParseError::new(
			Span::empty(0),
			ErrorKind::InvalidExpression("unsupported binary operator"),
		)),
	}
}

/// Helper to parse integer string with base prefixes (0x, 0o, 0b).
fn parse_int_literal(s: &str) -> i64 {
	let clean = s.replace('_', "");
	if let Some(hex) = clean.strip_prefix("0x").or_else(|| clean.strip_prefix("0X")) {
		i64::from_str_radix(hex, 16).unwrap_or(0)
	} else if let Some(oct) = clean.strip_prefix("0o").or_else(|| clean.strip_prefix("0O")) {
		i64::from_str_radix(oct, 8).unwrap_or(0)
	} else if let Some(bin) = clean.strip_prefix("0b").or_else(|| clean.strip_prefix("0B")) {
		i64::from_str_radix(bin, 2).unwrap_or(0)
	} else {
		clean.parse::<i64>().unwrap_or(0)
	}
}
