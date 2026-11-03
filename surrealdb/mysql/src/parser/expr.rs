//! Expression parser for MySQL SQL syntax using Pratt parsing.

use crate::ast::expr::{
	ColumnRef, Expr, Quantifier, TruthValue, WhenClause, WindowFrame, WindowFrameBound,
	WindowFrameExclusion, WindowFrameUnits, WindowSpec,
};
use crate::ast::literal::Literal;
use crate::ast::operator::{BinaryOperator, UnaryOperator};
use crate::lexer::keyword::Keyword;
use crate::lexer::token::TokenKind;
use crate::parser::Parser;
use crate::parser::error::{ErrorKind, ParseError, ParseResult};

// Precedence levels
const PREC_ASSIGN: u8 = 10;
const PREC_OR: u8 = 20;
const PREC_XOR: u8 = 25;
const PREC_AND: u8 = 30;
const PREC_NOT: u8 = 40;
const PREC_BETWEEN: u8 = 45;
const PREC_CMP: u8 = 50;
const PREC_BIT_OR: u8 = 60;
const PREC_BIT_AND: u8 = 70;
const PREC_SHIFT: u8 = 80;
const PREC_ADD_SUB: u8 = 90;
const PREC_MUL_DIV: u8 = 100;
const PREC_BIT_XOR: u8 = 110;
const PREC_UNARY: u8 = 120;
const PREC_COLLATE: u8 = 130;
const PREC_INTERVAL: u8 = 140;

impl<'a> Parser<'a> {
	/// Parse an expression.
	pub fn parse_expr(&mut self) -> ParseResult<Expr> {
		self.parse_expr_bp(0)
	}

	/// Pratt expression parsing with binding power.
	pub fn parse_expr_bp(&mut self, min_bp: u8) -> ParseResult<Expr> {
		self.check_recursion_limit()?;
		self.recursion_depth += 1;
		let mut loop_depth: usize = 0;
		let res = self.parse_expr_bp_inner(min_bp, &mut loop_depth);
		self.recursion_depth = self.recursion_depth.saturating_sub(1 + loop_depth);
		res
	}

	fn parse_expr_bp_inner(&mut self, min_bp: u8, loop_depth: &mut usize) -> ParseResult<Expr> {
		let mut lhs = self.parse_prefix()?;

		loop {
			// Infix operators
			if let Some(op) = self.peek_binary_op() {
				let bp = binary_op_precedence(op);
				if bp < min_bp {
					break;
				}
				self.check_recursion_limit()?;
				self.recursion_depth += 1;
				*loop_depth += 1;
				self.bump(); // consume operator

				// Check for quantified comparison: op {ANY|SOME|ALL} (SELECT ...)
				if matches!(
					op,
					BinaryOperator::Equal
						| BinaryOperator::NotEqual
						| BinaryOperator::Less
						| BinaryOperator::LessEqual
						| BinaryOperator::Greater
						| BinaryOperator::GreaterEqual
				) && matches!(
					self.peek_keyword(),
					Some(Keyword::Any | Keyword::Some | Keyword::All)
				) {
					let quantifier = match self.peek_keyword() {
						Some(Keyword::All) => Quantifier::All,
						Some(Keyword::Some) => Quantifier::Some,
						_ => Quantifier::Any,
					};
					self.bump(); // consume quantifier
					self.expect(
						&TokenKind::LParen,
						"'(' before subquery in quantified comparison",
					)?;
					let subquery = Box::new(self.parse_select_stmt()?);
					let rparen =
						self.expect(&TokenKind::RParen, "')' closing quantified subquery")?;
					let span = lhs.span().merge(rparen.span);
					lhs = Expr::Quantified {
						expr: Box::new(lhs),
						op,
						quantifier,
						subquery,
						span,
					};
					continue;
				}

				let rhs = self.parse_expr_bp(bp + 1)?;
				let span = lhs.span().merge(rhs.span());
				lhs = Expr::BinaryOp {
					left: Box::new(lhs),
					op,
					right: Box::new(rhs),
					span,
				};
				continue;
			}

			// IS [NOT] NULL / TRUE / FALSE / UNKNOWN
			if self.peek_keyword() == Some(Keyword::Is) && min_bp <= PREC_CMP {
				self.check_recursion_limit()?;
				self.recursion_depth += 1;
				*loop_depth += 1;
				let is_span = self.bump().span;
				let negated = if self.peek_keyword() == Some(Keyword::Not) {
					self.bump();
					true
				} else {
					false
				};

				match self.peek_keyword() {
					Some(Keyword::Null) => {
						let end = self.bump().span;
						lhs = Expr::IsNull {
							expr: Box::new(lhs),
							negated,
							span: is_span.merge(end),
						};
						continue;
					}
					Some(Keyword::True) => {
						let end = self.bump().span;
						lhs = Expr::IsTruthValue {
							expr: Box::new(lhs),
							negated,
							value: TruthValue::True,
							span: is_span.merge(end),
						};
						continue;
					}
					Some(Keyword::False) => {
						let end = self.bump().span;
						lhs = Expr::IsTruthValue {
							expr: Box::new(lhs),
							negated,
							value: TruthValue::False,
							span: is_span.merge(end),
						};
						continue;
					}
					Some(Keyword::Unknown) => {
						let end = self.bump().span;
						lhs = Expr::IsTruthValue {
							expr: Box::new(lhs),
							negated,
							value: TruthValue::Unknown,
							span: is_span.merge(end),
						};
						continue;
					}
					_ => {
						return Err(ParseError::new(
							self.peek().span,
							ErrorKind::UnexpectedToken {
								expected: "NULL, TRUE, FALSE, or UNKNOWN after IS",
								found: self.peek().kind.to_string(),
							},
						));
					}
				}
			}

			// [NOT] BETWEEN low AND high
			let is_between = self.peek_keyword() == Some(Keyword::Between);
			let is_not_between = self.peek_keyword() == Some(Keyword::Not)
				&& self.peek_nth_keyword(1) == Some(Keyword::Between);
			if (is_between || is_not_between) && min_bp <= PREC_BETWEEN {
				self.check_recursion_limit()?;
				self.recursion_depth += 1;
				*loop_depth += 1;
				let negated = if is_not_between {
					self.bump(); // NOT
					self.bump(); // BETWEEN
					true
				} else {
					self.bump(); // BETWEEN
					false
				};
				let low = self.parse_expr_bp(PREC_BETWEEN + 1)?;
				self.expect_keyword(Keyword::And, "AND in BETWEEN expression")?;
				let high = self.parse_expr_bp(PREC_BETWEEN + 1)?;
				let span = lhs.span().merge(high.span());
				lhs = Expr::Between {
					expr: Box::new(lhs),
					negated,
					low: Box::new(low),
					high: Box::new(high),
					span,
				};
				continue;
			}

			// [NOT] IN ( ... )
			let is_in = self.peek_keyword() == Some(Keyword::In);
			let is_not_in = self.peek_keyword() == Some(Keyword::Not)
				&& self.peek_nth_keyword(1) == Some(Keyword::In);
			if (is_in || is_not_in) && min_bp <= PREC_CMP {
				self.check_recursion_limit()?;
				self.recursion_depth += 1;
				*loop_depth += 1;
				let negated = if is_not_in {
					self.bump(); // NOT
					self.bump(); // IN
					true
				} else {
					self.bump(); // IN
					false
				};

				self.expect(&TokenKind::LParen, "'(' after IN")?;
				let lhs_span = lhs.span();
				if self.peek_keyword() == Some(Keyword::Select) {
					let sub = self.parse_select_stmt()?;
					let end = self.expect(&TokenKind::RParen, "')' closing IN subquery")?.span;
					lhs = Expr::InSubquery {
						expr: Box::new(lhs),
						negated,
						subquery: Box::new(sub),
						span: lhs_span.merge(end),
					};
				} else {
					let mut list = Vec::new();
					while self.peek().kind != TokenKind::RParen
						&& self.peek().kind != TokenKind::Eof
					{
						list.push(self.parse_expr()?);
						if self.peek().kind == TokenKind::Comma {
							self.bump();
						} else {
							break;
						}
					}
					let end = self.expect(&TokenKind::RParen, "')' closing IN list")?.span;
					lhs = Expr::InList {
						expr: Box::new(lhs),
						negated,
						list,
						span: lhs_span.merge(end),
					};
				}
				continue;
			}

			// NOT LIKE / NOT REGEXP / NOT RLIKE
			let is_not_like = self.peek_keyword() == Some(Keyword::Not)
				&& matches!(
					self.peek_nth_keyword(1),
					Some(Keyword::Like | Keyword::Regexp | Keyword::Rlike)
				);
			if is_not_like && min_bp <= PREC_CMP {
				self.check_recursion_limit()?;
				self.recursion_depth += 1;
				*loop_depth += 1;
				self.bump(); // NOT
				let bumped = &self.bump().kind;
				let op = if *bumped == TokenKind::Keyword(Keyword::Like) {
					BinaryOperator::Like
				} else if *bumped == TokenKind::Keyword(Keyword::Regexp) {
					BinaryOperator::Regexp
				} else if *bumped == TokenKind::Keyword(Keyword::Rlike) {
					BinaryOperator::Rlike
				} else {
					unreachable!()
				};
				let rhs = self.parse_expr_bp(PREC_CMP + 1)?;
				let span = lhs.span().merge(rhs.span());
				let inner = Expr::BinaryOp {
					left: Box::new(lhs),
					op,
					right: Box::new(rhs),
					span,
				};
				lhs = Expr::UnaryOp {
					op: UnaryOperator::Not,
					expr: Box::new(inner),
					span,
				};
				continue;
			}

			// COLLATE collation_name
			if self.peek_keyword() == Some(Keyword::Collate) && min_bp <= PREC_COLLATE {
				self.check_recursion_limit()?;
				self.recursion_depth += 1;
				*loop_depth += 1;
				self.bump(); // COLLATE
				let (collate_name, collate_span) = self.parse_ident()?;
				let span = lhs.span().merge(collate_span);
				lhs = Expr::Collate {
					expr: Box::new(lhs),
					collation: collate_name,
					span,
				};
				continue;
			}

			break;
		}

		Ok(lhs)
	}

	/// Whether `expr` is the literal 9223372036854775808, the magnitude of the minimum 64-bit
	/// integer, as a decimal integer literal. The token that `expr` was read from is the one that
	/// starts at its span.
	fn is_int8_minimum_magnitude(&self, expr: &Expr) -> bool {
		let Expr::Literal(Literal::Float(_), span) = expr else {
			return false;
		};
		self.tokens
			.binary_search_by_key(&span.start, |token| token.span.start)
			.ok()
			.and_then(|index| self.tokens.get(index))
			.is_some_and(|token| {
				token.span == *span
					&& matches!(
						token.kind,
						TokenKind::WideIntLiteral {
							int8_minimum_magnitude: true,
							..
						}
					)
			})
	}

	fn parse_prefix(&mut self) -> ParseResult<Expr> {
		let tok = self.peek().clone();
		// Unary operators
		if tok.kind == TokenKind::Plus {
			self.bump();
			let operand = self.parse_expr_bp(PREC_UNARY)?;
			let span = tok.span.merge(operand.span());
			Ok(Expr::UnaryOp {
				op: UnaryOperator::Plus,
				expr: Box::new(operand),
				span,
			})
		} else if tok.kind == TokenKind::Minus {
			self.bump();
			let operand = self.parse_expr_bp(PREC_UNARY)?;
			let span = tok.span.merge(operand.span());
			// A minus applied to the literal 9223372036854775808 alone is the minimum 64-bit
			// integer; the literal is a number beyond 64 bits until it is negated.
			if self.is_int8_minimum_magnitude(&operand) {
				return Ok(Expr::Literal(Literal::Integer(i64::MIN), span));
			}
			Ok(Expr::UnaryOp {
				op: UnaryOperator::Minus,
				expr: Box::new(operand),
				span,
			})
		} else if tok.kind == TokenKind::Tilde {
			self.bump();
			let operand = self.parse_expr_bp(PREC_UNARY)?;
			let span = tok.span.merge(operand.span());
			Ok(Expr::UnaryOp {
				op: UnaryOperator::BitNot,
				expr: Box::new(operand),
				span,
			})
		} else if tok.kind == TokenKind::Bang || tok.kind == TokenKind::Keyword(Keyword::Not) {
			self.bump();
			let operand = self.parse_expr_bp(PREC_NOT)?;
			let span = tok.span.merge(operand.span());
			Ok(Expr::UnaryOp {
				op: UnaryOperator::Not,
				expr: Box::new(operand),
				span,
			})
		} else if tok.kind == TokenKind::Keyword(Keyword::Binary) {
			self.bump();
			let operand = self.parse_expr_bp(PREC_UNARY)?;
			let span = tok.span.merge(operand.span());
			Ok(Expr::UnaryOp {
				op: UnaryOperator::Binary,
				expr: Box::new(operand),
				span,
			})
		} else if tok.kind == TokenKind::Keyword(Keyword::Interval) {
			self.bump();
			let expr = self.parse_expr_bp(PREC_INTERVAL)?;
			let (unit, unit_span) = self.parse_ident()?;
			let span = tok.span.merge(unit_span);
			Ok(Expr::Interval {
				expr: Box::new(expr),
				unit,
				span,
			})
		} else {
			self.parse_primary()
		}
	}

	#[inline(never)]
	fn parse_primary(&mut self) -> ParseResult<Expr> {
		let tok = self.peek().clone();
		if let TokenKind::IntLiteral(n) = tok.kind {
			self.bump();
			Ok(Expr::Literal(Literal::Integer(n), tok.span))
		} else if let TokenKind::UnsignedIntLiteral(n) = tok.kind {
			self.bump();
			Ok(Expr::Literal(Literal::Unsigned(n), tok.span))
		} else if let TokenKind::WideIntLiteral {
			value,
			..
		} = tok.kind
		{
			self.bump();
			Ok(Expr::Literal(Literal::Float(value), tok.span))
		} else if let TokenKind::FloatLiteral(fl) = tok.kind {
			self.bump();
			Ok(Expr::Literal(Literal::Float(fl), tok.span))
		} else if let TokenKind::DecimalLiteral(text) = tok.kind {
			self.bump();
			Ok(Expr::Literal(Literal::Decimal(text), tok.span))
		} else if let TokenKind::StringLiteral(s) = tok.kind {
			self.bump();
			Ok(Expr::Literal(Literal::String(s), tok.span))
		} else if let TokenKind::CharsetIntro(charset) = tok.kind {
			self.bump();
			let lit_tok = self.peek().clone();
			if let TokenKind::StringLiteral(s) = lit_tok.kind {
				self.bump();
				Ok(Expr::Literal(
					Literal::IntroducedString {
						charset,
						value: s,
					},
					tok.span.merge(lit_tok.span),
				))
			} else if let TokenKind::HexLiteral(bytes) = lit_tok.kind {
				self.bump();
				Ok(Expr::Literal(
					Literal::IntroducedHexString {
						charset,
						value: bytes,
					},
					tok.span.merge(lit_tok.span),
				))
			} else {
				Err(ParseError::new(
					lit_tok.span,
					ErrorKind::UnexpectedToken {
						expected: "string or hex literal after character set introducer",
						found: lit_tok.kind.to_string(),
					},
				))
			}
		} else if let TokenKind::HexLiteral(bytes) = tok.kind {
			self.bump();
			Ok(Expr::Literal(Literal::HexString(bytes), tok.span))
		} else if let TokenKind::BitLiteral(bits) = tok.kind {
			self.bump();
			Ok(Expr::Literal(Literal::BitString(bits), tok.span))
		} else if tok.kind == TokenKind::Keyword(Keyword::Null) {
			self.bump();
			Ok(Expr::Literal(Literal::Null, tok.span))
		} else if tok.kind == TokenKind::Keyword(Keyword::True) {
			self.bump();
			Ok(Expr::Literal(Literal::Boolean(true), tok.span))
		} else if tok.kind == TokenKind::Keyword(Keyword::False) {
			self.bump();
			Ok(Expr::Literal(Literal::Boolean(false), tok.span))
		} else if let TokenKind::Param(idx) = tok.kind {
			self.bump();
			Ok(Expr::Param(idx, tok.span))
		} else if let TokenKind::UserVar(v) = tok.kind {
			self.bump();
			Ok(Expr::UserVar(v, tok.span))
		} else if let TokenKind::SystemVar {
			name,
			is_global,
		} = tok.kind
		{
			self.bump();
			Ok(Expr::SystemVar {
				name,
				is_global,
				span: tok.span,
			})
		} else if tok.kind == TokenKind::LParen {
			self.bump(); // '('
			if self.peek_keyword() == Some(Keyword::Select) {
				let sub = self.parse_select_stmt()?;
				let end = self.expect(&TokenKind::RParen, "')' closing subquery")?.span;
				Ok(Expr::Subquery {
					subquery: Box::new(sub),
					span: tok.span.merge(end),
				})
			} else {
				let inner = self.parse_expr()?;
				self.expect(&TokenKind::RParen, "')' closing expression")?;
				Ok(inner)
			}
		} else if tok.kind == TokenKind::Keyword(Keyword::Exists) {
			self.bump(); // EXISTS
			self.expect(&TokenKind::LParen, "'(' after EXISTS")?;
			let sub = self.parse_select_stmt()?;
			let end = self.expect(&TokenKind::RParen, "')' closing EXISTS subquery")?.span;
			Ok(Expr::Exists {
				subquery: Box::new(sub),
				span: tok.span.merge(end),
			})
		} else if tok.kind == TokenKind::Keyword(Keyword::Case) {
			self.parse_case_expr()
		} else if tok.kind == TokenKind::Keyword(Keyword::Cast) {
			self.parse_cast_expr()
		} else if tok.kind == TokenKind::Keyword(Keyword::Convert) {
			self.parse_convert_expr()
		} else if tok.kind == TokenKind::Keyword(Keyword::Default) {
			self.bump();
			Ok(Expr::ColumnRef(ColumnRef {
				fields: vec!["DEFAULT".to_string()],
				span: tok.span,
			}))
		} else if matches!(
			tok.kind,
			TokenKind::Ident(_)
				| TokenKind::Keyword(
					Keyword::If
						| Keyword::Ifnull | Keyword::Nullif
						| Keyword::Coalesce
						| Keyword::Values
				)
		) || (matches!(tok.kind, TokenKind::Keyword(_))
			&& self.peek_nth(1).kind == TokenKind::LParen)
		{
			let (name, name_span) = self.parse_ident()?;

			// Check for function call: name(...)
			if self.peek().kind == TokenKind::LParen {
				self.bump(); // '('
				let distinct = if self.peek_keyword() == Some(Keyword::Distinct) {
					self.bump();
					true
				} else {
					false
				};
				let mut args = Vec::new();
				if let Some(special) = self.parse_special_arguments(&name)? {
					args = special;
				} else if self.peek().kind != TokenKind::RParen {
					if self.peek().kind == TokenKind::Star {
						let star_span = self.bump().span;
						args.push(Expr::ColumnRef(ColumnRef {
							fields: vec!["*".to_string()],
							span: star_span,
						}));
					} else {
						loop {
							args.push(self.parse_expr()?);
							if self.peek().kind == TokenKind::Comma {
								self.bump();
							} else {
								break;
							}
						}
					}
				}
				let end = self.expect(&TokenKind::RParen, "')' closing function arguments")?.span;
				let (over, span) = if self.eat_keyword(Keyword::Over) {
					let spec = self.parse_window_spec()?;
					let span = name_span.merge(spec.span);
					(Some(Box::new(spec)), span)
				} else {
					(None, name_span.merge(end))
				};
				return Ok(Expr::FunctionCall {
					name,
					distinct,
					args,
					over,
					span,
				});
			}

			// `CURRENT_TIMESTAMP` and its kin are keywords that call a function without
			// parentheses; they cannot name a column unquoted. A quoted name is a column: its span
			// covers the quotes as well as the name, so it is longer than the name.
			let unquoted = name_span.end - name_span.start == name.len();
			if unquoted
				&& self.peek().kind != TokenKind::Dot
				&& ["current_timestamp", "current_date", "current_time", "current_user"]
					.iter()
					.any(|keyword| name.eq_ignore_ascii_case(keyword))
			{
				return Ok(Expr::FunctionCall {
					name: name.to_ascii_lowercase(),
					distinct: false,
					args: Vec::new(),
					over: None,
					span: name_span,
				});
			}

			// Otherwise, it's a column reference `name` or `name.field...`
			let mut fields = vec![name];
			let mut span = name_span;
			while self.peek().kind == TokenKind::Dot {
				self.bump(); // '.'
				if self.peek().kind == TokenKind::Star {
					let star_span = self.bump().span;
					fields.push("*".to_string());
					span = span.merge(star_span);
					break;
				}
				let (field, field_span) = self.parse_ident()?;
				fields.push(field);
				span = span.merge(field_span);
			}

			Ok(Expr::ColumnRef(ColumnRef {
				fields,
				span,
			}))
		} else if tok.kind == TokenKind::Star {
			self.bump();
			Ok(Expr::ColumnRef(ColumnRef {
				fields: vec!["*".to_string()],
				span: tok.span,
			}))
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

	/// The arguments of the functions whose calls have their own keywords: `EXTRACT(unit FROM
	/// source)`, `POSITION(needle IN haystack)` and `SUBSTRING(s FROM start FOR count)`. They are
	/// the arguments of the ordinary call with the same name, `extract('unit', source)` and so on,
	/// which is also how the call prints. `None` leaves the arguments to the ordinary comma list.
	fn parse_special_arguments(&mut self, name: &str) -> ParseResult<Option<Vec<Expr>>> {
		match name.to_ascii_lowercase().as_str() {
			"extract" => {
				let unit = self.peek().clone();
				if self.peek_nth(1).kind == TokenKind::Keyword(Keyword::From) {
					let (unit_name, unit_span) = self.parse_ident()?;
					self.bump();
					let source = self.parse_expr()?;
					let unit = Expr::Literal(
						Literal::String(unit_name.to_ascii_lowercase()),
						unit_span.merge(unit.span),
					);
					return Ok(Some(vec![unit, source]));
				}
				Ok(None)
			}
			"position" => {
				let needle = self.parse_expr_bp(PREC_CMP + 1)?;
				if self.peek_keyword() == Some(Keyword::In) {
					self.bump();
					let haystack = self.parse_expr_bp(PREC_CMP + 1)?;
					return Ok(Some(vec![needle, haystack]));
				}
				let mut args = vec![needle];
				while self.peek().kind == TokenKind::Comma {
					self.bump();
					args.push(self.parse_expr()?);
				}
				Ok(Some(args))
			}
			"timestampdiff" | "timestampadd" => {
				let unit_token = self.peek().clone();
				if matches!(unit_token.kind, TokenKind::Ident(_))
					&& self.peek_nth(1).kind == TokenKind::Comma
				{
					let (unit, unit_span) = self.parse_ident()?;
					self.bump();
					let mut args =
						vec![Expr::Literal(Literal::String(unit.to_ascii_uppercase()), unit_span)];
					args.push(self.parse_expr()?);
					while self.peek().kind == TokenKind::Comma {
						self.bump();
						args.push(self.parse_expr()?);
					}
					return Ok(Some(args));
				}
				Ok(None)
			}
			"substring" | "substr" | "mid" => {
				let string = self.parse_expr()?;
				if self.peek_keyword() == Some(Keyword::From) {
					self.bump();
					let start = self.parse_expr()?;
					let mut args = vec![string, start];
					if self.peek_keyword() == Some(Keyword::For) {
						self.bump();
						args.push(self.parse_expr()?);
					}
					return Ok(Some(args));
				}
				let mut args = vec![string];
				while self.peek().kind == TokenKind::Comma {
					self.bump();
					args.push(self.parse_expr()?);
				}
				Ok(Some(args))
			}
			_ => Ok(None),
		}
	}

	fn parse_case_expr(&mut self) -> ParseResult<Expr> {
		let start = self.bump().span; // CASE
		let operand = if self.peek_keyword() != Some(Keyword::When) {
			Some(Box::new(self.parse_expr()?))
		} else {
			None
		};

		let mut branches = Vec::new();
		while self.peek_keyword() == Some(Keyword::When) {
			let when_start = self.bump().span;
			let when = self.parse_expr()?;
			self.expect_keyword(Keyword::Then, "THEN in CASE branch")?;
			let then = self.parse_expr()?;
			let span = when_start.merge(then.span());
			branches.push(WhenClause {
				when,
				then,
				span,
			});
		}

		let else_expr = if self.peek_keyword() == Some(Keyword::Else) {
			self.bump();
			Some(Box::new(self.parse_expr()?))
		} else {
			None
		};

		let end = self.expect_keyword(Keyword::End, "END in CASE expression")?.span;
		Ok(Expr::Case {
			operand,
			branches,
			else_expr,
			span: start.merge(end),
		})
	}

	fn parse_cast_expr(&mut self) -> ParseResult<Expr> {
		let start = self.bump().span; // CAST
		self.expect(&TokenKind::LParen, "'(' after CAST")?;
		let expr = self.parse_expr()?;
		self.expect_keyword(Keyword::As, "AS in CAST")?;
		let (mut target_type, _) = self.parse_ident()?;
		// A precision and scale, as in `DECIMAL(65,30)`, are kept in the type text.
		if self.eat(&TokenKind::LParen) {
			let mut modifiers = Vec::new();
			loop {
				let tok = self.peek();
				let TokenKind::IntLiteral(n) = &tok.kind else {
					return Err(ParseError::new(
						tok.span,
						ErrorKind::UnexpectedToken {
							expected: "a precision or scale in the CAST type",
							found: tok.kind.to_string(),
						},
					));
				};
				modifiers.push(n.to_string());
				self.bump();
				if !self.eat(&TokenKind::Comma) {
					break;
				}
			}
			self.expect(&TokenKind::RParen, "')' closing the CAST type modifiers")?;
			target_type = format!("{target_type}({})", modifiers.join(","));
		}
		let end = self.expect(&TokenKind::RParen, "')' closing CAST")?.span;
		Ok(Expr::Cast {
			expr: Box::new(expr),
			target_type,
			span: start.merge(end),
		})
	}

	fn parse_convert_expr(&mut self) -> ParseResult<Expr> {
		let start = self.bump().span; // CONVERT
		self.expect(&TokenKind::LParen, "'(' after CONVERT")?;
		let expr = self.parse_expr()?;
		let target_type = if self.peek_keyword() == Some(Keyword::Using) {
			self.bump();
			let (u, _) = self.parse_ident()?;
			format!("USING {u}")
		} else {
			self.expect(&TokenKind::Comma, "',' in CONVERT")?;
			let (t, _) = self.parse_ident()?;
			t
		};
		let end = self.expect(&TokenKind::RParen, "')' closing CONVERT")?.span;
		Ok(Expr::Cast {
			expr: Box::new(expr),
			target_type,
			span: start.merge(end),
		})
	}

	fn peek_binary_op(&self) -> Option<BinaryOperator> {
		let kind = &self.peek().kind;
		if kind == &TokenKind::ColonEqual {
			Some(BinaryOperator::Assign)
		} else if kind == &TokenKind::DoublePipe {
			if self.settings.sql_mode.pipes_as_concat {
				Some(BinaryOperator::Concat)
			} else {
				Some(BinaryOperator::Or)
			}
		} else if kind == &TokenKind::DoubleAmpersand {
			Some(BinaryOperator::And)
		} else if kind == &TokenKind::Equal {
			Some(BinaryOperator::Equal)
		} else if kind == &TokenKind::NullSafeEqual {
			Some(BinaryOperator::NullSafeEqual)
		} else if kind == &TokenKind::NotEqual {
			Some(BinaryOperator::NotEqual)
		} else if kind == &TokenKind::Less {
			Some(BinaryOperator::Less)
		} else if kind == &TokenKind::LessEqual {
			Some(BinaryOperator::LessEqual)
		} else if kind == &TokenKind::Greater {
			Some(BinaryOperator::Greater)
		} else if kind == &TokenKind::GreaterEqual {
			Some(BinaryOperator::GreaterEqual)
		} else if kind == &TokenKind::Plus {
			Some(BinaryOperator::Add)
		} else if kind == &TokenKind::Minus {
			Some(BinaryOperator::Sub)
		} else if kind == &TokenKind::Star {
			Some(BinaryOperator::Mul)
		} else if kind == &TokenKind::Slash {
			Some(BinaryOperator::Div)
		} else if kind == &TokenKind::Percent {
			Some(BinaryOperator::Mod)
		} else if kind == &TokenKind::ShiftLeft {
			Some(BinaryOperator::ShiftLeft)
		} else if kind == &TokenKind::ShiftRight {
			Some(BinaryOperator::ShiftRight)
		} else if kind == &TokenKind::Ampersand {
			Some(BinaryOperator::BitAnd)
		} else if kind == &TokenKind::Pipe {
			Some(BinaryOperator::BitOr)
		} else if kind == &TokenKind::Caret {
			Some(BinaryOperator::BitXor)
		} else if kind == &TokenKind::Keyword(Keyword::Or) {
			Some(BinaryOperator::Or)
		} else if kind == &TokenKind::Keyword(Keyword::Xor) {
			Some(BinaryOperator::Xor)
		} else if kind == &TokenKind::Keyword(Keyword::And) {
			Some(BinaryOperator::And)
		} else if kind == &TokenKind::Keyword(Keyword::Div) {
			Some(BinaryOperator::IntDiv)
		} else if kind == &TokenKind::Keyword(Keyword::Mod) {
			Some(BinaryOperator::Mod)
		} else if kind == &TokenKind::Keyword(Keyword::Like) {
			Some(BinaryOperator::Like)
		} else if kind == &TokenKind::Keyword(Keyword::Regexp) {
			Some(BinaryOperator::Regexp)
		} else if kind == &TokenKind::Keyword(Keyword::Rlike) {
			Some(BinaryOperator::Rlike)
		} else {
			None
		}
	}

	/// Parse a window specification in an `OVER` clause or `WINDOW` definition.
	pub fn parse_window_spec(&mut self) -> ParseResult<WindowSpec> {
		if self.eat(&TokenKind::LParen) {
			let start_span = self.peek().span;
			let mut window_name = None;
			let mut partition_by = Vec::new();
			let mut order_by = Vec::new();
			let mut frame = None;

			// Optional existing window name
			if matches!(self.peek().kind, TokenKind::Ident(_))
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
			if self.eat_keyword(Keyword::Partition) {
				self.expect_keyword(Keyword::By, "'BY' after PARTITION")?;
				loop {
					partition_by.push(self.parse_expr()?);
					if !self.eat(&TokenKind::Comma) {
						break;
					}
				}
			}

			// Optional ORDER BY
			if self.eat_keyword(Keyword::Order) {
				self.expect_keyword(Keyword::By, "'BY' after ORDER")?;
				order_by = self.parse_order_by_list()?;
			}

			// Optional frame specification: ROWS | RANGE | GROUPS
			let frame_units = if self.eat_keyword(Keyword::Rows) {
				Some(WindowFrameUnits::Rows)
			} else if self.eat_keyword(Keyword::Range) {
				Some(WindowFrameUnits::Range)
			} else if self.eat_keyword(Keyword::Groups) {
				Some(WindowFrameUnits::Groups)
			} else {
				None
			};

			if let Some(units) = frame_units {
				let frame_start_span = self.peek().span;
				let (start, end) = if self.eat_keyword(Keyword::Between) {
					let start = self.parse_window_frame_bound()?;
					self.expect_keyword(Keyword::And, "'AND' in window frame BETWEEN")?;
					let end = self.parse_window_frame_bound()?;
					(start, Some(end))
				} else {
					let start = self.parse_window_frame_bound()?;
					(start, None)
				};

				let exclusion = if self.eat_keyword(Keyword::Exclude) {
					if self.eat_keyword(Keyword::Current) {
						self.expect_keyword(Keyword::Row, "'ROW' after EXCLUDE CURRENT")?;
						Some(WindowFrameExclusion::CurrentRow)
					} else if self.eat_keyword(Keyword::Group) {
						Some(WindowFrameExclusion::Group)
					} else if self.eat_keyword(Keyword::Ties) {
						Some(WindowFrameExclusion::Ties)
					} else if self.eat_keyword(Keyword::No) {
						self.expect_keyword(Keyword::Others, "'OTHERS' after EXCLUDE NO")?;
						Some(WindowFrameExclusion::NoOthers)
					} else {
						return Err(ParseError::new(
							self.peek().span,
							ErrorKind::UnexpectedToken {
								expected: "CURRENT ROW, GROUP, TIES, or NO OTHERS after EXCLUDE",
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

			let rparen = self.expect(&TokenKind::RParen, "')' closing window specification")?;
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
		if self.eat_keyword(Keyword::Current) {
			self.expect_keyword(Keyword::Row, "'ROW' after CURRENT")?;
			Ok(WindowFrameBound::CurrentRow)
		} else if self.eat_keyword(Keyword::Unbounded) {
			if self.eat_keyword(Keyword::Preceding) {
				Ok(WindowFrameBound::UnboundedPreceding)
			} else if self.eat_keyword(Keyword::Following) {
				Ok(WindowFrameBound::UnboundedFollowing)
			} else {
				Err(ParseError::new(
					self.peek().span,
					ErrorKind::UnexpectedToken {
						expected: "PRECEDING or FOLLOWING after UNBOUNDED",
						found: self.peek().kind.to_string(),
					},
				))
			}
		} else {
			let expr = self.parse_expr()?;
			if self.eat_keyword(Keyword::Preceding) {
				Ok(WindowFrameBound::Preceding(Box::new(expr)))
			} else if self.eat_keyword(Keyword::Following) {
				Ok(WindowFrameBound::Following(Box::new(expr)))
			} else {
				Err(ParseError::new(
					self.peek().span,
					ErrorKind::UnexpectedToken {
						expected: "PRECEDING or FOLLOWING after frame offset",
						found: self.peek().kind.to_string(),
					},
				))
			}
		}
	}
}

fn binary_op_precedence(op: BinaryOperator) -> u8 {
	match op {
		BinaryOperator::Assign => PREC_ASSIGN,
		BinaryOperator::Or => PREC_OR,
		BinaryOperator::Xor => PREC_XOR,
		BinaryOperator::And => PREC_AND,
		BinaryOperator::Equal
		| BinaryOperator::NullSafeEqual
		| BinaryOperator::NotEqual
		| BinaryOperator::Less
		| BinaryOperator::LessEqual
		| BinaryOperator::Greater
		| BinaryOperator::GreaterEqual
		| BinaryOperator::Like
		| BinaryOperator::Regexp
		| BinaryOperator::Rlike => PREC_CMP,
		BinaryOperator::BitOr => PREC_BIT_OR,
		BinaryOperator::BitAnd => PREC_BIT_AND,
		BinaryOperator::ShiftLeft | BinaryOperator::ShiftRight => PREC_SHIFT,
		BinaryOperator::Add | BinaryOperator::Sub | BinaryOperator::Concat => PREC_ADD_SUB,
		BinaryOperator::Mul
		| BinaryOperator::Div
		| BinaryOperator::IntDiv
		| BinaryOperator::Mod => PREC_MUL_DIV,
		BinaryOperator::BitXor => PREC_BIT_XOR,
	}
}
