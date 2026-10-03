//! Expression parser for MySQL SQL syntax using Pratt parsing.

use crate::ast::expr::{ColumnRef, Expr, TruthValue, WhenClause};
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
		let res = self.parse_expr_bp_inner(min_bp);
		self.recursion_depth -= 1;
		res
	}

	fn parse_expr_bp_inner(&mut self, min_bp: u8) -> ParseResult<Expr> {
		let mut lhs = self.parse_prefix()?;

		loop {
			// Infix operators
			if let Some(op) = self.peek_binary_op() {
				let bp = binary_op_precedence(op);
				if bp < min_bp {
					break;
				}
				self.bump(); // consume operator
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

			// COLLATE collation_name
			if self.peek_keyword() == Some(Keyword::Collate) && min_bp <= PREC_COLLATE {
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

	fn parse_prefix(&mut self) -> ParseResult<Expr> {
		let tok = self.peek().clone();
		match tok.kind {
			// Unary operators
			TokenKind::Plus => {
				self.bump();
				let operand = self.parse_expr_bp(PREC_UNARY)?;
				let span = tok.span.merge(operand.span());
				Ok(Expr::UnaryOp {
					op: UnaryOperator::Plus,
					expr: Box::new(operand),
					span,
				})
			}
			TokenKind::Minus => {
				self.bump();
				let operand = self.parse_expr_bp(PREC_UNARY)?;
				let span = tok.span.merge(operand.span());
				Ok(Expr::UnaryOp {
					op: UnaryOperator::Minus,
					expr: Box::new(operand),
					span,
				})
			}
			TokenKind::Tilde => {
				self.bump();
				let operand = self.parse_expr_bp(PREC_UNARY)?;
				let span = tok.span.merge(operand.span());
				Ok(Expr::UnaryOp {
					op: UnaryOperator::BitNot,
					expr: Box::new(operand),
					span,
				})
			}
			TokenKind::Bang => {
				self.bump();
				let operand = self.parse_expr_bp(PREC_NOT)?;
				let span = tok.span.merge(operand.span());
				Ok(Expr::UnaryOp {
					op: UnaryOperator::Not,
					expr: Box::new(operand),
					span,
				})
			}
			TokenKind::Keyword(Keyword::Not) => {
				self.bump();
				let operand = self.parse_expr_bp(PREC_NOT)?;
				let span = tok.span.merge(operand.span());
				Ok(Expr::UnaryOp {
					op: UnaryOperator::Not,
					expr: Box::new(operand),
					span,
				})
			}
			TokenKind::Keyword(Keyword::Binary) => {
				self.bump();
				let operand = self.parse_expr_bp(PREC_UNARY)?;
				let span = tok.span.merge(operand.span());
				Ok(Expr::UnaryOp {
					op: UnaryOperator::Binary,
					expr: Box::new(operand),
					span,
				})
			}
			TokenKind::Keyword(Keyword::Interval) => {
				self.bump();
				let expr = self.parse_expr_bp(PREC_INTERVAL)?;
				let (unit, unit_span) = self.parse_ident()?;
				let span = tok.span.merge(unit_span);
				Ok(Expr::Interval {
					expr: Box::new(expr),
					unit,
					span,
				})
			}
			_ => self.parse_primary(),
		}
	}

	#[inline(never)]
	fn parse_primary(&mut self) -> ParseResult<Expr> {
		let tok = self.peek().clone();
		match tok.kind {
			// Literals
			TokenKind::IntLiteral(n) => {
				self.bump();
				Ok(Expr::Literal(Literal::Integer(n), tok.span))
			}
			TokenKind::FloatLiteral(fl) => {
				self.bump();
				Ok(Expr::Literal(Literal::Float(fl), tok.span))
			}
			TokenKind::StringLiteral(s) => {
				self.bump();
				Ok(Expr::Literal(Literal::String(s), tok.span))
			}
			TokenKind::HexLiteral(bytes) => {
				self.bump();
				Ok(Expr::Literal(Literal::HexString(bytes), tok.span))
			}
			TokenKind::BitLiteral(bits) => {
				self.bump();
				Ok(Expr::Literal(Literal::BitString(bits), tok.span))
			}
			TokenKind::Keyword(Keyword::Null) => {
				self.bump();
				Ok(Expr::Literal(Literal::Null, tok.span))
			}
			TokenKind::Keyword(Keyword::True) => {
				self.bump();
				Ok(Expr::Literal(Literal::Boolean(true), tok.span))
			}
			TokenKind::Keyword(Keyword::False) => {
				self.bump();
				Ok(Expr::Literal(Literal::Boolean(false), tok.span))
			}

			// Positional Parameter '?'
			TokenKind::Param(idx) => {
				self.bump();
				Ok(Expr::Param(idx, tok.span))
			}

			// User Variable '@var'
			TokenKind::UserVar(v) => {
				self.bump();
				Ok(Expr::UserVar(v, tok.span))
			}

			// System Variable '@@var' or '@@global.var'
			TokenKind::SystemVar {
				name,
				is_global,
			} => {
				self.bump();
				Ok(Expr::SystemVar {
					name,
					is_global,
					span: tok.span,
				})
			}

			// Parenthesized expression or Subquery
			TokenKind::LParen => {
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
			}

			// EXISTS (SELECT ...)
			TokenKind::Keyword(Keyword::Exists) => {
				self.bump(); // EXISTS
				self.expect(&TokenKind::LParen, "'(' after EXISTS")?;
				let sub = self.parse_select_stmt()?;
				let end = self.expect(&TokenKind::RParen, "')' closing EXISTS subquery")?.span;
				Ok(Expr::Exists {
					subquery: Box::new(sub),
					span: tok.span.merge(end),
				})
			}

			// CASE [expr] WHEN ... THEN ... [ELSE ...] END
			TokenKind::Keyword(Keyword::Case) => self.parse_case_expr(),

			// CAST(x AS type) or CONVERT(x, type)
			TokenKind::Keyword(Keyword::Cast) => self.parse_cast_expr(),
			TokenKind::Keyword(Keyword::Convert) => self.parse_convert_expr(),

			// Identifiers, Functions, or Column References
			TokenKind::Ident(_)
			| TokenKind::Keyword(Keyword::If)
			| TokenKind::Keyword(Keyword::Ifnull)
			| TokenKind::Keyword(Keyword::Nullif)
			| TokenKind::Keyword(Keyword::Coalesce) => {
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
					if self.peek().kind != TokenKind::RParen {
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
					let end =
						self.expect(&TokenKind::RParen, "')' closing function arguments")?.span;
					return Ok(Expr::FunctionCall {
						name,
						distinct,
						args,
						span: name_span.merge(end),
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
			}

			// Wildcard '*'
			TokenKind::Star => {
				self.bump();
				Ok(Expr::ColumnRef(ColumnRef {
					fields: vec!["*".to_string()],
					span: tok.span,
				}))
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
		let (target_type, _) = self.parse_ident()?;
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
		match &self.peek().kind {
			TokenKind::ColonEqual => Some(BinaryOperator::Assign),
			TokenKind::DoublePipe => Some(BinaryOperator::Or),
			TokenKind::DoubleAmpersand => Some(BinaryOperator::And),
			TokenKind::Equal => Some(BinaryOperator::Equal),
			TokenKind::NullSafeEqual => Some(BinaryOperator::NullSafeEqual),
			TokenKind::NotEqual => Some(BinaryOperator::NotEqual),
			TokenKind::Less => Some(BinaryOperator::Less),
			TokenKind::LessEqual => Some(BinaryOperator::LessEqual),
			TokenKind::Greater => Some(BinaryOperator::Greater),
			TokenKind::GreaterEqual => Some(BinaryOperator::GreaterEqual),
			TokenKind::Plus => Some(BinaryOperator::Add),
			TokenKind::Minus => Some(BinaryOperator::Sub),
			TokenKind::Star => Some(BinaryOperator::Mul),
			TokenKind::Slash => Some(BinaryOperator::Div),
			TokenKind::Percent => Some(BinaryOperator::Mod),
			TokenKind::ShiftLeft => Some(BinaryOperator::ShiftLeft),
			TokenKind::ShiftRight => Some(BinaryOperator::ShiftRight),
			TokenKind::Ampersand => Some(BinaryOperator::BitAnd),
			TokenKind::Pipe => Some(BinaryOperator::BitOr),
			TokenKind::Caret => Some(BinaryOperator::BitXor),
			TokenKind::Keyword(Keyword::Or) => Some(BinaryOperator::Or),
			TokenKind::Keyword(Keyword::Xor) => Some(BinaryOperator::Xor),
			TokenKind::Keyword(Keyword::And) => Some(BinaryOperator::And),
			TokenKind::Keyword(Keyword::Div) => Some(BinaryOperator::IntDiv),
			TokenKind::Keyword(Keyword::Mod) => Some(BinaryOperator::Mod),
			TokenKind::Keyword(Keyword::Like) => Some(BinaryOperator::Like),
			TokenKind::Keyword(Keyword::Regexp) => Some(BinaryOperator::Regexp),
			TokenKind::Keyword(Keyword::Rlike) => Some(BinaryOperator::Rlike),
			_ => None,
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
		BinaryOperator::Add | BinaryOperator::Sub => PREC_ADD_SUB,
		BinaryOperator::Mul
		| BinaryOperator::Div
		| BinaryOperator::IntDiv
		| BinaryOperator::Mod => PREC_MUL_DIV,
		BinaryOperator::BitXor => PREC_BIT_XOR,
	}
}
