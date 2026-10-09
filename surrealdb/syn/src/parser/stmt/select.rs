use surrealdb_sql::order::{OrderList, Ordering};
use surrealdb_sql::statements::SelectStatement;
use surrealdb_sql::{Expr, Fields, Limit, Literal, Order, Split, Splits, Start};

use super::parts::MissingKind;
use crate::parser::mac::expected;
use crate::parser::{ParseResult, Parser};
use crate::token::{Span, t};

impl Parser<'_> {
	/// expects `select` to be eaten.
	pub fn parse_select_stmt(&mut self) -> ParseResult<SelectStatement> {
		let before = self.peek().span;
		let fields = self.parse_fields()?;
		let fields_span = before.covers(self.last_span());

		let omit = if self.eat(t!("OMIT")) {
			let mut fields = Vec::new();
			loop {
				let expr = self.parse_expr_field()?;
				fields.push(expr);
				if !self.eat(t!(",")) {
					break;
				}
			}
			fields
		} else {
			vec![]
		};

		expected!(self, t!("FROM"));

		let only = self.eat(t!("ONLY"));

		let mut what = vec![self.parse_expr_table()?];
		while self.eat(t!(",")) {
			what.push(self.parse_expr_table()?);
		}

		let with = self.try_parse_with()?;
		let cond = self.try_parse_condition()?;

		let split_before = self.peek().span;
		let split = self.try_parse_split(&fields, fields_span)?;
		let split_span = split.as_ref().map(|_| split_before.covers(self.last_span()));
		let group = self.try_parse_group(&fields, fields_span, split_span)?;
		let order = self.try_parse_orders()?;
		let (limit, start) = if let t!("START") = self.peek_kind() {
			let start = self.try_parse_start()?;
			let limit = self.try_parse_limit()?;
			(limit, start)
		} else {
			let limit = self.try_parse_limit()?;
			let start = self.try_parse_start()?;
			(limit, start)
		};
		let fetch = self.try_parse_fetch()?;
		let version = if self.eat(t!("VERSION")) {
			self.parse_expr_field()?
		} else {
			Expr::Literal(Literal::None)
		};
		let timeout = self.try_parse_timeout()?;
		let for_update = if self.eat(t!("FOR")) {
			expected!(self, t!("UPDATE"));
			true
		} else {
			false
		};
		let tempfiles = self.eat(t!("TEMPFILES"));
		let explain = self.try_parse_explain()?;

		Ok(SelectStatement {
			fields,
			omit,
			only,
			what,
			with,
			cond,
			split,
			group,
			order,
			limit,
			start,
			fetch,
			version,
			timeout,
			tempfiles,
			explain,
			for_update,
		})
	}

	pub fn try_parse_split(
		&mut self,
		fields: &Fields,
		fields_span: Span,
	) -> ParseResult<Option<Splits>> {
		if !self.eat(t!("SPLIT")) {
			return Ok(None);
		}

		self.eat(t!("ON"));

		let has_all = fields.contains_all();

		let before = self.peek().span;
		let split = self.parse_basic_idiom()?;
		let split_span = before.covers(self.last_span());
		if !has_all {
			Self::check_idiom(MissingKind::Split, fields, fields_span, &split, split_span)?;
		}

		let mut res = vec![Split(split)];
		while self.eat(t!(",")) {
			let before = self.peek().span;
			let split = self.parse_basic_idiom()?;
			let split_span = before.covers(self.last_span());
			if !has_all {
				Self::check_idiom(MissingKind::Split, fields, fields_span, &split, split_span)?;
			}
			res.push(Split(split))
		}
		Ok(Some(Splits(res)))
	}

	pub fn try_parse_orders(&mut self) -> ParseResult<Option<Ordering>> {
		if !self.eat(t!("ORDER")) {
			return Ok(None);
		}

		self.eat(t!("BY"));

		if let t!("RAND") = self.peek_kind() {
			self.pop_peek();
			let start = expected!(self, t!("(")).span;
			self.expect_closing_delimiter(t!(")"), start)?;
			return Ok(Some(Ordering::Random));
		};

		// ORDER BY sorts the full record before the projection runs, so an
		// ordering idiom does not need to appear in the projection.
		let mut orders = vec![self.parse_order()?];
		while self.eat(t!(",")) {
			orders.push(self.parse_order()?)
		}

		Ok(Some(Ordering::Order(OrderList(orders))))
	}

	fn parse_order(&mut self) -> ParseResult<Order> {
		let start = self.parse_basic_idiom()?;
		let collate = self.eat(t!("COLLATE"));
		let numeric = self.eat(t!("NUMERIC"));
		let direction = match self.peek_kind() {
			t!("ASCENDING") => {
				self.pop_peek();
				true
			}
			t!("DESCENDING") => {
				self.pop_peek();
				false
			}
			_ => true,
		};
		Ok(Order {
			value: start,
			collate,
			numeric,
			direction,
		})
	}

	pub fn try_parse_limit(&mut self) -> ParseResult<Option<Limit>> {
		if !self.eat(t!("LIMIT")) {
			return Ok(None);
		}
		self.eat(t!("BY"));
		let value = self.parse_expr_field()?;
		Ok(Some(Limit(value)))
	}

	pub fn try_parse_start(&mut self) -> ParseResult<Option<Start>> {
		if !self.eat(t!("START")) {
			return Ok(None);
		}
		self.eat(t!("AT"));
		let value = self.parse_expr_field()?;
		Ok(Some(Start(value)))
	}
}
