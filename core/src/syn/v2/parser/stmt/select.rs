use crate::{
	sql::{
		statements::SelectStatement, Explain, Field, Fields, Ident, Idioms, Limit, Order, Orders,
		Split, Splits, Start, Values, Version, With,
	},
	syn::v2::{
		parser::{
			error::MissingKind,
			mac::{expected, unexpected},
			ParseResult, Parser,
		},
		token::{t, Span},
	},
};

impl Parser<'_> {
	pub(crate) fn parse_select_stmt(&mut self) -> ParseResult<SelectStatement> {
		let before = self.peek().span;
		let expr = self.parse_fields()?;
		let fields_span = before.covers(self.last_span());

		let omit = self.eat(t!("OMIT")).then(|| self.parse_idiom_list()).transpose()?.map(Idioms);

		expected!(self, t!("FROM"));

		let only = self.eat(t!("ONLY"));

		let mut what = vec![self.parse_value()?];
		while self.eat(t!(",")) {
			what.push(self.parse_value()?);
		}
		let what = Values(what);

		let with = self.try_parse_with()?;
		let cond = self.try_parse_condition()?;
		let split = self.try_parse_split(&expr, fields_span)?;
		let group = self.try_parse_group(&expr, fields_span)?;
		let order = self.try_parse_orders(&expr, fields_span)?;
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
		let version = self.try_parse_version()?;
		let timeout = self.try_parse_timeout()?;
		let parallel = self.eat(t!("PARALLEL"));
		let explain = self.eat(t!("EXPLAIN")).then(|| Explain(self.eat(t!("FULL"))));

		Ok(SelectStatement {
			expr,
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
			parallel,
			explain,
		})
	}

	fn try_parse_with(&mut self) -> ParseResult<Option<With>> {
		if !self.eat(t!("WITH")) {
			return Ok(None);
		}
		let with = match self.next().kind {
			t!("NOINDEX") => With::NoIndex,
			t!("NO") => {
				expected!(self, t!("INDEX"));
				With::NoIndex
			}
			t!("INDEX") => {
				let mut index = vec![self.next_token_value::<Ident>()?.0];
				while self.eat(t!(",")) {
					index.push(self.next_token_value::<Ident>()?.0);
				}
				With::Index(index)
			}
			x => unexpected!(self, x, "`NO`, `NOINDEX` or `INDEX`"),
		};
		Ok(Some(with))
	}

	fn try_parse_split(
		&mut self,
		fields: &Fields,
		fields_span: Span,
	) -> ParseResult<Option<Splits>> {
		if !self.eat(t!("SPLIT")) {
			return Ok(None);
		}

		self.eat(t!("ON"));

		let has_all = fields.contains(&Field::All);

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

	fn try_parse_orders(
		&mut self,
		fields: &Fields,
		fields_span: Span,
	) -> ParseResult<Option<Orders>> {
		if !self.eat(t!("ORDER")) {
			return Ok(None);
		}

		self.eat(t!("BY"));

		if let t!("RAND") = self.peek_kind() {
			self.pop_peek();
			let start = expected!(self, t!("(")).span;
			self.expect_closing_delimiter(t!(")"), start)?;
			return Ok(Some(Orders(vec![Order {
				order: Default::default(),
				random: true,
				collate: false,
				numeric: false,
				direction: true,
			}])));
		};

		let has_all = fields.contains(&Field::All);

		let before = self.recent_span();
		let order = self.parse_order()?;
		let order_span = before.covers(self.last_span());
		if !has_all {
			Self::check_idiom(MissingKind::Order, fields, fields_span, &order, order_span)?;
		}

		let mut orders = vec![order];
		while self.eat(t!(",")) {
			let before = self.recent_span();
			let order = self.parse_order()?;
			let order_span = before.covers(self.last_span());
			if !has_all {
				Self::check_idiom(MissingKind::Order, fields, fields_span, &order, order_span)?;
			}
			orders.push(order)
		}

		Ok(Some(Orders(orders)))
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
			order: start,
			random: false,
			collate,
			numeric,
			direction,
		})
	}

	fn try_parse_limit(&mut self) -> ParseResult<Option<Limit>> {
		if !self.eat(t!("LIMIT")) {
			return Ok(None);
		}
		self.eat(t!("BY"));
		let value = self.parse_value()?;
		Ok(Some(Limit(value)))
	}

	fn try_parse_start(&mut self) -> ParseResult<Option<Start>> {
		if !self.eat(t!("START")) {
			return Ok(None);
		}
		self.eat(t!("AT"));
		let value = self.parse_value()?;
		Ok(Some(Start(value)))
	}

	fn try_parse_version(&mut self) -> ParseResult<Option<Version>> {
		if !self.eat(t!("VERSION")) {
			return Ok(None);
		}
		let time = self.next_token_value()?;
		Ok(Some(Version(time)))
	}
}
