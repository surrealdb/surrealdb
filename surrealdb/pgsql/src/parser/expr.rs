//! Expression parser implementing PostgreSQL `a_expr`, `b_expr`, and `c_expr`.

use crate::ast::expr::{
	ColumnRef, Expr, NullsOrder, OrderByItem, OrderDirection, QualifiedName, Quantifier, TypeName,
	WhenClause, WindowFrame, WindowFrameBound, WindowFrameExclusion, WindowFrameUnits, WindowSpec,
};
use crate::ast::literal::Literal;
use crate::ast::op::{BinaryOperator, UnaryOperator};
use crate::error::{ErrorKind, ParseError, ParseResult};
use crate::lexer::keyword::Keyword;
use crate::lexer::token::TokenKind;
use crate::parser::Parser;
use crate::parser::precedence::prefix_binding_power;
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
		let mut loop_depth: u32 = 0;
		let res = self.parse_expr_bp_inner(min_bp, &mut loop_depth);
		self.leave_recursion_by(1 + loop_depth);
		res
	}

	#[inline(never)]
	fn parse_subquery_box(&mut self) -> ParseResult<Box<crate::ast::stmt::SelectStmt>> {
		self.parse_select_stmt().map(Box::new)
	}

	/// Parse the array operand of `left <op> ANY|SOME|ALL (array)` after its opening parenthesis.
	fn parse_quantified_array(
		&mut self,
		left: Expr,
		op: BinaryOperator,
		quantifier: Quantifier,
	) -> ParseResult<Expr> {
		let array = self.parse_expr()?;
		let r_paren = self.expect(&TokenKind::RParen, "')' closing quantified array")?;
		let span = left.span().merge(r_paren.span);
		Ok(Expr::QuantifiedArray {
			expr: Box::new(left),
			op,
			quantifier,
			array: Box::new(array),
			span,
		})
	}

	/// Whether `expr`, looking through parentheses, is the decimal integer literal
	/// 9223372036854775808 as written in the source: digits (and `_`) only, the magnitude of the
	/// int8 minimum.
	fn is_int8_minimum_magnitude(&self, mut expr: &Expr) -> bool {
		while let Expr::Grouping {
			expr: inner,
			..
		} = expr
		{
			expr = inner;
		}
		let Expr::Literal {
			lit: Literal::Float(_),
			span,
		} = expr
		else {
			return false;
		};
		span.slice(self.src()).is_some_and(|text| {
			text.bytes().all(|b| b.is_ascii_digit() || b == b'_')
				&& text.replace('_', "").parse::<u64>() == Ok(1 << 63)
		})
	}

	fn parse_expr_bp_inner(&mut self, min_bp: u8, loop_depth: &mut u32) -> ParseResult<Expr> {
		// Prefix / Prefix operator handling
		let mut left = if let Some(prefix_bp) = prefix_binding_power(&self.peek().kind) {
			let op_tok = self.bump()?;
			let right = self.parse_expr_bp(prefix_bp)?;
			let span = op_tok.span.merge(right.span());

			let unary_op = if op_tok.kind == TokenKind::Keyword(Keyword::Not) {
				UnaryOperator::Not
			} else if op_tok.kind == TokenKind::Operator("+") {
				UnaryOperator::Plus
			} else if op_tok.kind == TokenKind::Operator("-") {
				UnaryOperator::Minus
			} else if op_tok.kind == TokenKind::Operator("~") {
				UnaryOperator::BitNot
			} else if let TokenKind::Operator(s) = &op_tok.kind {
				UnaryOperator::Custom(s.to_string())
			} else {
				return Err(ParseError::new(
					op_tok.span,
					ErrorKind::InvalidExpression("invalid prefix operator"),
				));
			};

			// gram.y: a_expr: '-' a_expr %prec UMINUS (doNegate) folds a minus into the numeric
			// constant it applies to. The decimal 9223372036854775808 is a number beyond int8
			// until it is negated, and then it is the integer `i64::MIN`.
			if unary_op == UnaryOperator::Minus && self.is_int8_minimum_magnitude(&right) {
				Expr::Literal {
					lit: Literal::Integer(i64::MIN),
					span,
				}
			} else {
				Expr::UnaryOp {
					op: unary_op,
					expr: Box::new(right),
					span,
				}
			}
		} else {
			self.parse_primary_expr()?
		};

		// Infix / Postfix operator loop
		while let Some((l_bp, r_bp, _assoc)) = self.peek_infix_bp()? {
			if l_bp < min_bp {
				break;
			}

			self.enter_recursion(self.peek().span)?;
			*loop_depth += 1;

			let negated = self.eat(&TokenKind::Keyword(Keyword::Not))?;

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

				if self.peek().kind == TokenKind::Keyword(Keyword::Null) {
					let null_tok = self.bump()?;
					let span = left.span().merge(null_tok.span);
					left = Expr::IsNull {
						expr: Box::new(left),
						negated,
						span,
					};
				} else if self.peek().kind == TokenKind::Keyword(Keyword::True) {
					let true_tok = self.bump()?;
					let span = left.span().merge(true_tok.span);
					left = Expr::IsTrue {
						expr: Box::new(left),
						negated,
						span,
					};
				} else if self.peek().kind == TokenKind::Keyword(Keyword::False) {
					let false_tok = self.bump()?;
					let span = left.span().merge(false_tok.span);
					left = Expr::IsFalse {
						expr: Box::new(left),
						negated,
						span,
					};
				} else if self.peek().kind == TokenKind::Keyword(Keyword::Unknown) {
					let unk_tok = self.bump()?;
					let span = left.span().merge(unk_tok.span);
					left = Expr::IsUnknown {
						expr: Box::new(left),
						negated,
						span,
					};
				} else if self.peek().kind == TokenKind::Keyword(Keyword::Distinct) {
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
				} else if self.peek().kind == TokenKind::Keyword(Keyword::Bound) {
					let bound_tok = self.bump()?;
					let span = left.span().merge(bound_tok.span);
					left = Expr::IsBound {
						expr: Box::new(left),
						negated,
						span,
					};
				} else {
					return Err(ParseError::new(
						self.peek().span,
						ErrorKind::UnexpectedToken {
							expected: "NULL, TRUE, FALSE, UNKNOWN, DISTINCT FROM, or BOUND",
							found: self.peek().kind.to_string(),
						},
					));
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
					negated,
					symmetric,
					span,
				};
				continue;
			}

			// 7. IN (list...) or IN (SELECT ...)
			if op_tok.kind == TokenKind::Keyword(Keyword::In) {
				self.bump()?; // consume 'IN'
				self.expect(&TokenKind::LParen, "'('")?;
				if matches!(
					self.peek().kind,
					TokenKind::Keyword(Keyword::Select | Keyword::With | Keyword::Values)
				) {
					let sub = self.parse_subquery_box()?;
					let r_paren = self.expect(&TokenKind::RParen, "')'")?;
					let span = left.span().merge(r_paren.span);
					left = Expr::InSubquery {
						expr: Box::new(left),
						subquery: sub,
						negated,
						span,
					};
					continue;
				}
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
					negated,
					span,
				};
				continue;
			}

			// 8. General Binary Operator
			self.bump()?; // consume operator
			let binary_op = if op_tok.kind == TokenKind::Keyword(Keyword::Like) && negated {
				BinaryOperator::NotLike
			} else if op_tok.kind == TokenKind::Keyword(Keyword::Ilike) && negated {
				BinaryOperator::NotILike
			} else if op_tok.kind == TokenKind::Keyword(Keyword::Similar) && negated {
				self.expect(&TokenKind::Keyword(Keyword::To), "'TO'")?;
				BinaryOperator::NotSimilarTo
			} else if op_tok.kind == TokenKind::Keyword(Keyword::Similar) && !negated {
				self.expect(&TokenKind::Keyword(Keyword::To), "'TO'")?;
				BinaryOperator::SimilarTo
			} else {
				token_to_binary_op(&op_tok.kind)?
			};
			let quantifier = if self.peek().kind == TokenKind::Keyword(Keyword::Any) {
				Some(Quantifier::Any)
			} else if self.peek().kind == TokenKind::Keyword(Keyword::Some) {
				Some(Quantifier::Some)
			} else if self.peek().kind == TokenKind::Keyword(Keyword::All) {
				Some(Quantifier::All)
			} else {
				None
			};
			if let Some(quantifier) = quantifier {
				self.bump()?; // consume ANY / SOME / ALL
				self.expect(&TokenKind::LParen, "'(' after quantifier")?;
				if !matches!(
					self.peek().kind,
					TokenKind::Keyword(Keyword::Select | Keyword::With | Keyword::Values)
				) {
					left = self.parse_quantified_array(left, binary_op, quantifier)?;
					continue;
				}
				let sub = self.parse_subquery_box()?;
				let r_paren = self.expect(&TokenKind::RParen, "')' closing quantified subquery")?;
				let span = left.span().merge(r_paren.span);
				left = Expr::Quantified {
					expr: Box::new(left),
					op: binary_op,
					quantifier,
					subquery: sub,
					span,
				};
				continue;
			}
			let right = self.parse_expr_bp(r_bp)?;
			let span = left.span().merge(right.span());
			left = Expr::BinaryOp {
				left: Box::new(left),
				op: binary_op,
				right: Box::new(right),
				span,
			};
		}

		Ok(left)
	}

	/// Parse a primary expression (literal, column reference, parenthesis, array, row, case, cast).
	///
	/// gram.y: c_expr
	pub fn parse_primary_expr(&mut self) -> ParseResult<Expr> {
		let tok = self.peek().clone();

		if let TokenKind::Integer(n) = &tok.kind {
			self.bump()?;
			Ok(Expr::Literal {
				lit: Literal::Integer(*n),
				span: tok.span,
			})
		} else if let TokenKind::IntegerRaw(s) = &tok.kind {
			let Some(lit) = raw_integer_literal(s) else {
				return Err(ParseError::new(
					tok.span,
					ErrorKind::NumericOverflow((*s).to_string()),
				));
			};
			self.bump()?;
			Ok(Expr::Literal {
				lit,
				span: tok.span,
			})
		} else if let TokenKind::Float(f) = &tok.kind {
			self.bump()?;
			Ok(Expr::Literal {
				lit: Literal::Float(*f),
				span: tok.span,
			})
		} else if let TokenKind::FloatRaw(s) = &tok.kind {
			self.bump()?;
			let val = s.replace('_', "").parse::<f64>().unwrap_or(0.0);
			Ok(Expr::Literal {
				lit: Literal::Float(val),
				span: tok.span,
			})
		} else if let TokenKind::StringConstant(s) = &tok.kind {
			self.bump()?;
			Ok(Expr::Literal {
				lit: Literal::String(s.clone()),
				span: tok.span,
			})
		} else if let TokenKind::DollarQuotedString {
			content,
			..
		} = &tok.kind
		{
			self.bump()?;
			Ok(Expr::Literal {
				lit: Literal::String(content.to_string()),
				span: tok.span,
			})
		} else if let TokenKind::BitString {
			is_hex,
			content,
		} = &tok.kind
		{
			self.bump()?;
			Ok(Expr::Literal {
				lit: Literal::BitString {
					is_hex: *is_hex,
					value: content.to_string(),
				},
				span: tok.span,
			})
		} else if tok.kind == TokenKind::Keyword(Keyword::True) {
			self.bump()?;
			Ok(Expr::Literal {
				lit: Literal::Boolean(true),
				span: tok.span,
			})
		} else if tok.kind == TokenKind::Keyword(Keyword::False) {
			self.bump()?;
			Ok(Expr::Literal {
				lit: Literal::Boolean(false),
				span: tok.span,
			})
		} else if tok.kind == TokenKind::Keyword(Keyword::Null) {
			self.bump()?;
			Ok(Expr::Literal {
				lit: Literal::Null,
				span: tok.span,
			})
		} else if tok.kind == TokenKind::Keyword(Keyword::Default) {
			let t = self.bump()?;
			Ok(Expr::ColumnRef(ColumnRef {
				fields: vec!["DEFAULT".to_string()],
				span: t.span,
			}))
		} else if let TokenKind::Param(id) = &tok.kind {
			self.bump()?;
			Ok(Expr::Param {
				id: *id,
				span: tok.span,
			})
		} else if tok.kind == TokenKind::LParen {
			let l_paren = self.bump()?;
			if matches!(
				self.peek().kind,
				TokenKind::Keyword(Keyword::Select | Keyword::With | Keyword::Values)
			) {
				let sub = self.parse_subquery_box()?;
				let r_paren = self.expect(&TokenKind::RParen, "')'")?;
				let span = l_paren.span.merge(r_paren.span);
				return Ok(Expr::Subquery {
					subquery: sub,
					span,
				});
			}
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
		} else if tok.kind == TokenKind::Keyword(Keyword::Array) {
			let arr_tok = self.bump()?;
			if self.eat(&TokenKind::LParen)? {
				let sub = self.parse_subquery_box()?;
				let r_paren = self.expect(&TokenKind::RParen, "')' closing ARRAY subquery")?;
				let span = arr_tok.span.merge(r_paren.span);
				return Ok(Expr::ArraySubquery {
					subquery: sub,
					span,
				});
			}
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
		} else if tok.kind == TokenKind::Keyword(Keyword::Exists) {
			let exists_tok = self.bump()?;
			self.expect(&TokenKind::LParen, "'(' after EXISTS")?;
			let sub = self.parse_subquery_box()?;
			let r_paren = self.expect(&TokenKind::RParen, "')' closing EXISTS subquery")?;
			let span = exists_tok.span.merge(r_paren.span);
			Ok(Expr::Exists {
				subquery: sub,
				span,
			})
		} else if tok.kind == TokenKind::Keyword(Keyword::Row) {
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
		} else if tok.kind == TokenKind::Keyword(Keyword::Cast) {
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
		} else if let TokenKind::Keyword(
			kw @ (Keyword::JsonObject
			| Keyword::JsonArray
			| Keyword::JsonExists
			| Keyword::JsonValue
			| Keyword::JsonQuery),
		) = &tok.kind
		{
			let kw = *kw;
			let tok = self.bump()?;
			self.parse_json_expr(kw, tok.span)
		} else if tok.kind == TokenKind::Keyword(Keyword::Case) {
			self.parse_case_expr()
		} else if matches!(tok.kind, TokenKind::Ident(_) | TokenKind::QuotedIdent(_)) {
			let qname = self.parse_qualified_name()?;
			self.parse_ident_or_func(qname)
		} else if let TokenKind::Keyword(kw) = &tok.kind
			&& kw.category() != crate::lexer::keyword::KeywordCategory::Reserved
		{
			let qname = self.parse_qualified_name()?;
			self.parse_ident_or_func(qname)
		} else if let TokenKind::Keyword(
			kw @ (Keyword::CurrentDate
			| Keyword::CurrentTime
			| Keyword::CurrentTimestamp
			| Keyword::CurrentUser
			| Keyword::CurrentRole
			| Keyword::CurrentSchema
			| Keyword::CurrentCatalog
			| Keyword::Localtime
			| Keyword::Localtimestamp
			| Keyword::User
			| Keyword::SessionUser),
		) = &tok.kind
		{
			let kw = *kw;
			let kw_tok = self.bump()?;
			let mut span = kw_tok.span;
			if self.peek().kind == TokenKind::LParen {
				self.bump()?;
				let r_paren = self.expect(&TokenKind::RParen, "')'")?;
				span = span.merge(r_paren.span);
			}
			Ok(Expr::FunctionCall {
				name: QualifiedName::single(kw.as_str().to_ascii_lowercase(), kw_tok.span),
				args: Vec::new(),
				distinct: false,
				filter: None,
				over: None,
				span,
			})
		} else {
			Err(ParseError::new(
				tok.span,
				ErrorKind::UnexpectedToken {
					expected: "expression",
					found: tok.kind.to_string(),
				},
			))
		}
	}

	/// gram.y: columnref, func_application.
	pub(crate) fn parse_ident_or_func(&mut self, qname: QualifiedName) -> ParseResult<Expr> {
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
			let mut span = qname.span.merge(r_paren.span);
			let filter = if self.eat(&TokenKind::Keyword(Keyword::Filter))? {
				self.expect(&TokenKind::LParen, "'('")?;
				self.expect(&TokenKind::Keyword(Keyword::Where), "'WHERE'")?;
				let cond = self.parse_expr()?;
				let r_filter = self.expect(&TokenKind::RParen, "')'")?;
				span = span.merge(r_filter.span);
				Some(Box::new(cond))
			} else {
				None
			};
			let over = if self.eat(&TokenKind::Keyword(Keyword::Over))? {
				let spec = self.parse_window_spec()?;
				span = span.merge(spec.span);
				Some(spec)
			} else {
				None
			};
			Ok(Expr::FunctionCall {
				name: qname,
				args,
				distinct,
				filter,
				over,
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

	#[inline(never)]
	fn parse_json_expr(&mut self, kw: Keyword, start_span: Span) -> ParseResult<Expr> {
		if kw == Keyword::JsonObject {
			self.expect(&TokenKind::LParen, "'('")?;
			let mut fields = Vec::new();
			while self.peek().kind != TokenKind::RParen {
				let key = self.parse_expr()?;
				if self.eat(&TokenKind::Colon)?
					|| self.eat(&TokenKind::Keyword(Keyword::Value))?
					|| self.eat(&TokenKind::Comma)?
				{
					let val = self.parse_expr()?;
					fields.push((key, val));
				} else {
					return Err(ParseError::new(
						self.peek().span,
						ErrorKind::UnexpectedToken {
							expected: "':' or 'VALUE' or ',' between JSON_OBJECT key and value",
							found: self.peek().kind.to_string(),
						},
					));
				}
				if !self.eat(&TokenKind::Comma)? {
					break;
				}
			}
			let end = self.expect(&TokenKind::RParen, "')'")?.span;
			Ok(Expr::JsonObject {
				fields,
				span: start_span.merge(end),
			})
		} else if kw == Keyword::JsonArray {
			self.expect(&TokenKind::LParen, "'('")?;
			let mut elements = Vec::new();
			while self.peek().kind != TokenKind::RParen {
				elements.push(self.parse_expr()?);
				if !self.eat(&TokenKind::Comma)? {
					break;
				}
			}
			let end = self.expect(&TokenKind::RParen, "')'")?.span;
			Ok(Expr::JsonArray {
				elements,
				span: start_span.merge(end),
			})
		} else if kw == Keyword::JsonExists {
			self.expect(&TokenKind::LParen, "'('")?;
			let context = Box::new(self.parse_expr()?);
			self.expect(&TokenKind::Comma, "','")?;
			let (path, _) = self.parse_string_constant()?;
			let end = self.expect(&TokenKind::RParen, "')'")?.span;
			Ok(Expr::JsonExists {
				context,
				path,
				span: start_span.merge(end),
			})
		} else if kw == Keyword::JsonValue {
			self.expect(&TokenKind::LParen, "'('")?;
			let context = Box::new(self.parse_expr()?);
			self.expect(&TokenKind::Comma, "','")?;
			let (path, _) = self.parse_string_constant()?;
			let returning = if self.eat(&TokenKind::Keyword(Keyword::Returning))? {
				Some(self.parse_type_name()?)
			} else {
				None
			};
			let end = self.expect(&TokenKind::RParen, "')'")?.span;
			Ok(Expr::JsonValue {
				context,
				path,
				returning,
				span: start_span.merge(end),
			})
		} else if kw == Keyword::JsonQuery {
			self.expect(&TokenKind::LParen, "'('")?;
			let context = Box::new(self.parse_expr()?);
			self.expect(&TokenKind::Comma, "','")?;
			let (path, _) = self.parse_string_constant()?;
			let end = self.expect(&TokenKind::RParen, "')'")?.span;
			Ok(Expr::JsonQuery {
				context,
				path,
				span: start_span.merge(end),
			})
		} else {
			Err(ParseError::new(
				start_span,
				ErrorKind::UnexpectedToken {
					expected: "JSON expression",
					found: kw.as_str().to_string(),
				},
			))
		}
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
		if let TokenKind::Ident(s) = &tok.kind {
			self.bump()?;
			Ok((s.to_string(), tok.span))
		} else if let TokenKind::QuotedIdent(s) = &tok.kind {
			self.bump()?;
			Ok((s.clone(), tok.span))
		} else if let TokenKind::Keyword(kw) = &tok.kind
			&& kw.category() != crate::lexer::keyword::KeywordCategory::Reserved
		{
			self.bump()?;
			Ok((kw.as_str().to_lowercase(), tok.span))
		} else {
			Err(ParseError::new(
				tok.span,
				ErrorKind::UnexpectedToken {
					expected: "identifier",
					found: tok.kind.to_string(),
				},
			))
		}
	}

	/// Parse the label after `AS` in a select list, where any keyword, reserved or not, names the
	/// column.
	pub fn parse_col_label(&mut self) -> ParseResult<(String, Span)> {
		let tok = self.peek().clone();
		if let TokenKind::Keyword(kw) = &tok.kind {
			self.bump()?;
			Ok((kw.as_str().to_lowercase(), tok.span))
		} else {
			self.parse_ident()
		}
	}

	/// Parse a single string constant or dollar-quoted string token.
	pub fn parse_string_constant(&mut self) -> ParseResult<(String, Span)> {
		let tok = self.peek().clone();
		if let TokenKind::StringConstant(s) = &tok.kind {
			self.bump()?;
			Ok((s.clone(), tok.span))
		} else if let TokenKind::DollarQuotedString {
			content,
			..
		} = &tok.kind
		{
			self.bump()?;
			Ok((content.to_string(), tok.span))
		} else {
			Err(ParseError::new(
				tok.span,
				ErrorKind::UnexpectedToken {
					expected: "string literal",
					found: tok.kind.to_string(),
				},
			))
		}
	}

	/// Parse a PostgreSQL data type name (e.g. `INTEGER`, `VARCHAR(255)`, `TEXT[]`).
	pub fn parse_type_name(&mut self) -> ParseResult<TypeName> {
		let mut name = self.parse_qualified_name()?;
		let mut modifiers = Vec::new();
		let mut array_bounds = Vec::new();
		let mut end_span = name.span;

		// Check for multi-word types like DOUBLE PRECISION or CHARACTER VARYING
		if let Some(last_mut) = name.parts.last_mut() {
			if last_mut.eq_ignore_ascii_case("double")
				&& let Some(tok_text) = self.peek().span.slice(self.src())
				&& tok_text.eq_ignore_ascii_case("precision")
			{
				let prec_tok = self.bump()?;
				end_span = end_span.merge(prec_tok.span);
				name.span = end_span;
				last_mut.push_str(" precision");
			} else if last_mut.eq_ignore_ascii_case("character")
				&& let Some(tok_text) = self.peek().span.slice(self.src())
				&& tok_text.eq_ignore_ascii_case("varying")
			{
				let var_tok = self.bump()?;
				end_span = end_span.merge(var_tok.span);
				name.span = end_span;
				last_mut.push_str(" varying");
			}
		}

		// Check for modifiers e.g. `(255)` or `(10, 2)`
		if self.eat(&TokenKind::LParen)? {
			while self.peek().kind != TokenKind::RParen {
				let tok = self.bump()?;
				let mod_str = tok.span.slice(self.src()).unwrap_or("");
				modifiers.push(mod_str.to_string());
				if !self.eat(&TokenKind::Comma)? {
					break;
				}
			}
			let r_paren = self.expect(&TokenKind::RParen, "')'")?;
			end_span = end_span.merge(r_paren.span);
		}

		// `TIMESTAMP(3) WITH TIME ZONE`, `TIME WITHOUT TIME ZONE`: the zone suffix follows the
		// modifiers and is part of the type's name.
		if name.parts.last().is_some_and(|last| {
			last.eq_ignore_ascii_case("timestamp") || last.eq_ignore_ascii_case("time")
		}) {
			let next = &self.peek().kind;
			let suffix = if *next == TokenKind::Keyword(Keyword::With) {
				Some(" with time zone")
			} else if *next == TokenKind::Keyword(Keyword::Without) {
				Some(" without time zone")
			} else {
				None
			};
			if let Some(suffix) = suffix {
				self.bump()?;
				for word in ["time", "zone"] {
					let tok = self.peek().clone();
					if !tok
						.span
						.slice(self.src())
						.is_some_and(|text| text.eq_ignore_ascii_case(word))
					{
						return Err(ParseError::new(
							tok.span,
							ErrorKind::UnexpectedToken {
								expected: if word == "time" {
									"TIME"
								} else {
									"ZONE"
								},
								found: tok.kind.to_string(),
							},
						));
					}
					end_span = end_span.merge(tok.span);
					self.bump()?;
				}
				name.span = end_span;
				if let Some(last) = name.parts.last_mut() {
					last.push_str(suffix);
				}
			}
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

	/// Parse items in an `ORDER BY` clause.
	pub fn parse_order_by_clause(&mut self) -> ParseResult<Vec<OrderByItem>> {
		let mut items = Vec::new();

		loop {
			let expr = self.parse_expr()?;
			let mut span = expr.span();

			let direction = if self.eat(&TokenKind::Keyword(Keyword::Asc))? {
				Some(OrderDirection::Asc)
			} else if self.eat(&TokenKind::Keyword(Keyword::Desc))? {
				Some(OrderDirection::Desc)
			} else {
				None
			};

			let nulls = if self.eat(&TokenKind::Keyword(Keyword::Nulls))? {
				if self.eat(&TokenKind::Keyword(Keyword::First))? {
					Some(NullsOrder::First)
				} else if self.eat(&TokenKind::Keyword(Keyword::Last))? {
					Some(NullsOrder::Last)
				} else {
					return Err(ParseError::new(
						self.peek().span,
						ErrorKind::UnexpectedToken {
							expected: "FIRST or LAST",
							found: self.peek().kind.to_string(),
						},
					));
				}
			} else {
				None
			};

			span = span.merge(self.peek().span);
			items.push(OrderByItem {
				expr,
				direction,
				nulls,
				span,
			});

			if !self.eat(&TokenKind::Comma)? {
				break;
			}
		}

		Ok(items)
	}

	/// Parse a window specification in an `OVER` clause or `WINDOW` definition.
	pub fn parse_window_spec(&mut self) -> ParseResult<WindowSpec> {
		if self.eat(&TokenKind::LParen)? {
			let start_span = self.peek().span;
			let mut window_name = None;
			let mut partition_by = Vec::new();
			let mut order_by = Vec::new();
			let mut frame = None;

			// Optional existing window name
			if matches!(self.peek().kind, TokenKind::Ident(_) | TokenKind::QuotedIdent(_))
				&& !matches!(
					self.peek().kind,
					TokenKind::Keyword(
						Keyword::Partition
							| Keyword::Order | Keyword::Rows
							| Keyword::Range | Keyword::Groups
					)
				) {
				let (ident, _) = self.parse_ident()?;
				window_name = Some(ident);
			}

			// Optional PARTITION BY
			if self.eat(&TokenKind::Keyword(Keyword::Partition))? {
				self.expect(&TokenKind::Keyword(Keyword::By), "'BY'")?;
				loop {
					partition_by.push(self.parse_expr()?);
					if !self.eat(&TokenKind::Comma)? {
						break;
					}
				}
			}

			// Optional ORDER BY
			if self.eat(&TokenKind::Keyword(Keyword::Order))? {
				self.expect(&TokenKind::Keyword(Keyword::By), "'BY'")?;
				order_by = self.parse_order_by_clause()?;
			}

			// Optional frame specification: ROWS | RANGE | GROUPS
			let frame_units = if self.eat(&TokenKind::Keyword(Keyword::Rows))? {
				Some(WindowFrameUnits::Rows)
			} else if self.eat(&TokenKind::Keyword(Keyword::Range))? {
				Some(WindowFrameUnits::Range)
			} else if self.eat(&TokenKind::Keyword(Keyword::Groups))? {
				Some(WindowFrameUnits::Groups)
			} else {
				None
			};

			if let Some(units) = frame_units {
				let frame_start_span = self.peek().span;
				let (start, end) = if self.eat(&TokenKind::Keyword(Keyword::Between))? {
					let start = self.parse_window_frame_bound()?;
					self.expect(&TokenKind::Keyword(Keyword::And), "'AND'")?;
					let end = self.parse_window_frame_bound()?;
					(start, Some(end))
				} else {
					let start = self.parse_window_frame_bound()?;
					(start, None)
				};

				let exclusion = if self.eat(&TokenKind::Keyword(Keyword::Exclude))? {
					if self.eat(&TokenKind::Keyword(Keyword::Current))? {
						self.expect(&TokenKind::Keyword(Keyword::Row), "'ROW'")?;
						Some(WindowFrameExclusion::CurrentRow)
					} else if self.eat(&TokenKind::Keyword(Keyword::Group))? {
						Some(WindowFrameExclusion::Group)
					} else if self.eat(&TokenKind::Keyword(Keyword::Ties))? {
						Some(WindowFrameExclusion::Ties)
					} else if self.eat(&TokenKind::Keyword(Keyword::No))? {
						self.expect(&TokenKind::Keyword(Keyword::Others), "'OTHERS'")?;
						Some(WindowFrameExclusion::NoOthers)
					} else {
						return Err(ParseError::new(
							self.peek().span,
							ErrorKind::UnexpectedToken {
								expected: "CURRENT ROW, GROUP, TIES, or NO OTHERS",
								found: self.peek().kind.to_string(),
							},
						));
					}
				} else {
					None
				};

				let frame_span = frame_start_span.merge(self.peek().span);
				frame = Some(WindowFrame {
					units,
					start,
					end,
					exclusion,
					span: frame_span,
				});
			}

			let rparen = self.expect(&TokenKind::RParen, "')'")?;
			let span = start_span.merge(rparen.span);

			Ok(WindowSpec {
				window_name,
				partition_by,
				order_by,
				frame,
				span,
			})
		} else {
			// Single window name: OVER win_name
			let tok = self.peek().clone();
			let (name, _) = self.parse_ident()?;
			Ok(WindowSpec {
				window_name: Some(name),
				partition_by: Vec::new(),
				order_by: Vec::new(),
				frame: None,
				span: tok.span,
			})
		}
	}

	fn parse_window_frame_bound(&mut self) -> ParseResult<WindowFrameBound> {
		if self.eat(&TokenKind::Keyword(Keyword::Current))? {
			self.expect(&TokenKind::Keyword(Keyword::Row), "'ROW'")?;
			Ok(WindowFrameBound::CurrentRow)
		} else if self.eat(&TokenKind::Keyword(Keyword::Unbounded))? {
			if self.eat(&TokenKind::Keyword(Keyword::Preceding))? {
				Ok(WindowFrameBound::UnboundedPreceding)
			} else if self.eat(&TokenKind::Keyword(Keyword::Following))? {
				Ok(WindowFrameBound::UnboundedFollowing)
			} else {
				Err(ParseError::new(
					self.peek().span,
					ErrorKind::UnexpectedToken {
						expected: "PRECEDING or FOLLOWING",
						found: self.peek().kind.to_string(),
					},
				))
			}
		} else {
			let expr = self.parse_expr()?;
			if self.eat(&TokenKind::Keyword(Keyword::Preceding))? {
				Ok(WindowFrameBound::Preceding(Box::new(expr)))
			} else if self.eat(&TokenKind::Keyword(Keyword::Following))? {
				Ok(WindowFrameBound::Following(Box::new(expr)))
			} else {
				Err(ParseError::new(
					self.peek().span,
					ErrorKind::UnexpectedToken {
						expected: "PRECEDING or FOLLOWING",
						found: self.peek().kind.to_string(),
					},
				))
			}
		}
	}
}

/// Helper to convert token kind to binary operator.
fn token_to_binary_op(tok: &TokenKind<'_>) -> ParseResult<BinaryOperator> {
	if tok == &TokenKind::Keyword(Keyword::Or) {
		Ok(BinaryOperator::Or)
	} else if tok == &TokenKind::Keyword(Keyword::And) {
		Ok(BinaryOperator::And)
	} else if tok == &TokenKind::Keyword(Keyword::Like) {
		Ok(BinaryOperator::Like)
	} else if tok == &TokenKind::Keyword(Keyword::Ilike) {
		Ok(BinaryOperator::ILike)
	} else if tok == &TokenKind::Keyword(Keyword::Similar) {
		Ok(BinaryOperator::SimilarTo)
	} else if tok == &TokenKind::Operator("+") {
		Ok(BinaryOperator::Add)
	} else if tok == &TokenKind::Operator("-") {
		Ok(BinaryOperator::Sub)
	} else if tok == &TokenKind::Operator("*") {
		Ok(BinaryOperator::Mul)
	} else if tok == &TokenKind::Operator("/") {
		Ok(BinaryOperator::Div)
	} else if tok == &TokenKind::Operator("%") {
		Ok(BinaryOperator::Mod)
	} else if tok == &TokenKind::Operator("^") {
		Ok(BinaryOperator::Exp)
	} else if tok == &TokenKind::Operator("=") {
		Ok(BinaryOperator::Eq)
	} else if tok == &TokenKind::Operator("<>") || tok == &TokenKind::Operator("!=") {
		Ok(BinaryOperator::NotEq)
	} else if tok == &TokenKind::Operator("<") {
		Ok(BinaryOperator::Lt)
	} else if tok == &TokenKind::Operator("<=") {
		Ok(BinaryOperator::LtEq)
	} else if tok == &TokenKind::Operator(">") {
		Ok(BinaryOperator::Gt)
	} else if tok == &TokenKind::Operator(">=") {
		Ok(BinaryOperator::GtEq)
	} else if tok == &TokenKind::Operator("||") {
		Ok(BinaryOperator::Concat)
	} else if tok == &TokenKind::Operator("&") {
		Ok(BinaryOperator::BitAnd)
	} else if tok == &TokenKind::Operator("|") {
		Ok(BinaryOperator::BitOr)
	} else if tok == &TokenKind::Operator("#") {
		Ok(BinaryOperator::BitXor)
	} else if tok == &TokenKind::Operator("<<") {
		Ok(BinaryOperator::ShiftLeft)
	} else if tok == &TokenKind::Operator(">>") {
		Ok(BinaryOperator::ShiftRight)
	} else if tok == &TokenKind::Operator("->") {
		Ok(BinaryOperator::Arrow)
	} else if tok == &TokenKind::Operator("->>") {
		Ok(BinaryOperator::LongArrow)
	} else if tok == &TokenKind::Operator("#>") {
		Ok(BinaryOperator::HashArrow)
	} else if tok == &TokenKind::Operator("#>>") {
		Ok(BinaryOperator::HashLongArrow)
	} else if tok == &TokenKind::Operator("@>") {
		Ok(BinaryOperator::Contains)
	} else if tok == &TokenKind::Operator("<@") {
		Ok(BinaryOperator::ContainedBy)
	} else if tok == &TokenKind::Operator("~") {
		Ok(BinaryOperator::RegexMatch)
	} else if tok == &TokenKind::Operator("~*") {
		Ok(BinaryOperator::RegexMatchCi)
	} else if tok == &TokenKind::Operator("!~") {
		Ok(BinaryOperator::RegexNotMatch)
	} else if tok == &TokenKind::Operator("!~*") {
		Ok(BinaryOperator::RegexNotMatchCi)
	} else if let TokenKind::Operator(s) = tok {
		Ok(BinaryOperator::Custom(s.to_string()))
	} else {
		Err(ParseError::new(
			Span::empty(0),
			ErrorKind::InvalidExpression("unsupported binary operator"),
		))
	}
}

/// The literal of an integer token that is not a plain 64-bit decimal, and `None` when it has no
/// value.
///
/// gram.y: Iconst, FCONST (scan.l: process_integer_literal). A literal with a base prefix (0x, 0o,
/// 0b) is an integer that must fit 64 bits. A decimal integer beyond 64 bits is a numeric
/// constant, which this front end reads as the float of its value, as it reads `1.5`; it has no
/// value when that float is not finite.
fn raw_integer_literal(s: &str) -> Option<Literal> {
	let clean = s.replace('_', "");
	let based = [("0x", 16), ("0X", 16), ("0o", 8), ("0O", 8), ("0b", 2), ("0B", 2)]
		.into_iter()
		.find_map(|(prefix, radix)| clean.strip_prefix(prefix).map(|digits| (digits, radix)));
	if let Some((digits, radix)) = based {
		return i64::from_str_radix(digits, radix).ok().map(Literal::Integer);
	}
	match clean.parse::<i64>() {
		Ok(value) => Some(Literal::Integer(value)),
		Err(_) => clean.parse::<f64>().ok().filter(|value| value.is_finite()).map(Literal::Float),
	}
}
