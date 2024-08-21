use reblessive::Stk;

use crate::{
	sql::{
		statements::SelectStatement, Explain, Field, Fields, Ident, Idioms, Limit, Order, Orders,
		Split, Splits, Start, Values, Version, With,
	},
	syn::{
		parser::{
			error::MissingKind,
			mac::{expected, unexpected},
			ParseResult, Parser,
		},
		token::{t, Span},
	},
};

impl Parser<'_> {
	pub(crate) async fn parse_select_stmt(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<SelectStatement> {
		let before = self.peek().span;
		let expr = self.parse_fields(stk).await?;
		let fields_span = before.covers(self.last_span());

		let omit = if self.eat(t!("OMIT")) {
			Some(Idioms(self.parse_idiom_list(stk).await?))
		} else {
			None
		};

		expected!(self, t!("FROM"));

		let only = self.eat(t!("ONLY"));

		let mut what = vec![stk.run(|ctx| self.parse_value(ctx)).await?];
		while self.eat(t!(",")) {
			what.push(stk.run(|ctx| self.parse_value(ctx)).await?);
		}
		let what = Values(what);

		let with = self.try_parse_with()?;
		let cond = self.try_parse_condition(stk).await?;
		let split = self.try_parse_split(stk, &expr, fields_span).await?;
		let group = self.try_parse_group(stk, &expr, fields_span).await?;
		let order = self.try_parse_orders(stk, &expr, fields_span).await?;
		let (limit, start) = if let t!("START") = self.peek_kind() {
			let start = self.try_parse_start(stk).await?;
			let limit = self.try_parse_limit(stk).await?;
			(limit, start)
		} else {
			let limit = self.try_parse_limit(stk).await?;
			let start = self.try_parse_start(stk).await?;
			(limit, start)
		};
		let fetch = self.try_parse_fetch(stk).await?;
		let version = self.try_parse_version()?;
		let timeout = self.try_parse_timeout()?;
		let parallel = self.eat(t!("PARALLEL"));
		let tempfiles = self.eat(t!("TEMPFILES"));
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
			tempfiles,
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

	async fn try_parse_split(
		&mut self,
		ctx: &mut Stk,
		fields: &Fields,
		fields_span: Span,
	) -> ParseResult<Option<Splits>> {
		if !self.eat(t!("SPLIT")) {
			return Ok(None);
		}

		self.eat(t!("ON"));

		let has_all = fields.contains(&Field::All);

		let before = self.peek().span;
		let split = self.parse_basic_idiom(ctx).await?;
		let split_span = before.covers(self.last_span());
		if !has_all {
			Self::check_idiom(MissingKind::Split, fields, fields_span, &split, split_span)?;
		}

		let mut res = vec![Split(split)];
		while self.eat(t!(",")) {
			let before = self.peek().span;
			let split = self.parse_basic_idiom(ctx).await?;
			let split_span = before.covers(self.last_span());
			if !has_all {
				Self::check_idiom(MissingKind::Split, fields, fields_span, &split, split_span)?;
			}
			res.push(Split(split))
		}
		Ok(Some(Splits(res)))
	}

	async fn try_parse_orders(
		&mut self,
		ctx: &mut Stk,
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
		let order = self.parse_order(ctx).await?;
		let order_span = before.covers(self.last_span());
		if !has_all {
			Self::check_idiom(MissingKind::Order, fields, fields_span, &order, order_span)?;
		}

		let mut orders = vec![order];
		while self.eat(t!(",")) {
			let before = self.recent_span();
			let order = self.parse_order(ctx).await?;
			let order_span = before.covers(self.last_span());
			if !has_all {
				Self::check_idiom(MissingKind::Order, fields, fields_span, &order, order_span)?;
			}
			orders.push(order)
		}

		Ok(Some(Orders(orders)))
	}

	async fn parse_order(&mut self, ctx: &mut Stk) -> ParseResult<Order> {
		let start = self.parse_basic_idiom(ctx).await?;
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

	async fn try_parse_limit(&mut self, ctx: &mut Stk) -> ParseResult<Option<Limit>> {
		if !self.eat(t!("LIMIT")) {
			return Ok(None);
		}
		self.eat(t!("BY"));
		let value = ctx.run(|ctx| self.parse_value(ctx)).await?;
		Ok(Some(Limit(value)))
	}

	async fn try_parse_start(&mut self, ctx: &mut Stk) -> ParseResult<Option<Start>> {
		if !self.eat(t!("START")) {
			return Ok(None);
		}
		self.eat(t!("AT"));
		let value = ctx.run(|ctx| self.parse_value(ctx)).await?;
		Ok(Some(Start(value)))
	}

	pub(crate) fn try_parse_version(&mut self) -> ParseResult<Option<Version>> {
		if !self.eat(t!("VERSION")) {
			return Ok(None);
		}
		let time = self.next_token_value()?;
		Ok(Some(Version(time)))
	}
}
