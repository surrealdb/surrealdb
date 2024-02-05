//! Contains parsing code for smaller common parts of statements.

use crate::{
	sql::{
		changefeed::ChangeFeed, index::Distance, Base, Cond, Data, Duration, Fetch, Fetchs, Group,
		Groups, Ident, Operator, Output, Permission, Permissions, Tables, Timeout, View,
	},
	syn::v2::{
		parser::{
			mac::{expected, unexpected},
			ParseResult, Parser,
		},
		token::{t, DistanceKind, TokenKind},
	},
};

impl Parser<'_> {
	/// Parses a data production if the next token is a data keyword.
	/// Otherwise returns None
	pub fn try_parse_data(&mut self) -> ParseResult<Option<Data>> {
		let res = match self.peek().kind {
			t!("SET") => {
				self.pop_peek();
				let mut set_list = Vec::new();
				loop {
					let idiom = self.parse_plain_idiom()?;
					let operator = match self.next().kind {
						t!("=") => Operator::Equal,
						t!("+=") => Operator::Inc,
						t!("-=") => Operator::Dec,
						t!("+?=") => Operator::Ext,
						x => unexpected!(self, x, "a assign operator"),
					};
					let value = self.parse_value()?;
					set_list.push((idiom, operator, value));
					if !self.eat(t!(",")) {
						break;
					}
				}
				Data::SetExpression(set_list)
			}
			t!("UNSET") => {
				self.pop_peek();
				let idiom_list = self.parse_idiom_list()?;
				Data::UnsetExpression(idiom_list)
			}
			t!("PATCH") => {
				self.pop_peek();
				Data::PatchExpression(self.parse_value()?)
			}
			t!("MERGE") => {
				self.pop_peek();
				Data::MergeExpression(self.parse_value()?)
			}
			t!("REPLACE") => {
				self.pop_peek();
				Data::ReplaceExpression(self.parse_value()?)
			}
			t!("CONTENT") => {
				self.pop_peek();
				Data::ContentExpression(self.parse_value()?)
			}
			_ => return Ok(None),
		};
		Ok(Some(res))
	}

	/// Parses a statement output if the next token is `return`.
	pub fn try_parse_output(&mut self) -> ParseResult<Option<Output>> {
		if !self.eat(t!("RETURN")) {
			return Ok(None);
		}
		let res = match self.peek_kind() {
			t!("NONE") => {
				self.pop_peek();
				Output::None
			}
			t!("NULL") => {
				self.pop_peek();
				Output::Null
			}
			t!("DIFF") => {
				self.pop_peek();
				Output::Diff
			}
			t!("AFTER") => {
				self.pop_peek();
				Output::After
			}
			t!("BEFORE") => {
				self.pop_peek();
				Output::Before
			}
			_ => Output::Fields(self.parse_fields()?),
		};
		Ok(Some(res))
	}

	/// Parses a statement timeout if the next token is `TIMEOUT`.
	pub fn try_parse_timeout(&mut self) -> ParseResult<Option<Timeout>> {
		if !self.eat(t!("TIMEOUT")) {
			return Ok(None);
		}
		let duration = self.next_token_value()?;
		Ok(Some(Timeout(duration)))
	}

	pub fn try_parse_fetch(&mut self) -> ParseResult<Option<Fetchs>> {
		if !self.eat(t!("FETCH")) {
			return Ok(None);
		}
		let v = self.parse_idiom_list()?.into_iter().map(Fetch).collect();
		Ok(Some(Fetchs(v)))
	}

	pub fn try_parse_condition(&mut self) -> ParseResult<Option<Cond>> {
		if !self.eat(t!("WHERE")) {
			return Ok(None);
		}
		let v = self.parse_value_field()?;
		Ok(Some(Cond(v)))
	}

	pub fn try_parse_group(&mut self) -> ParseResult<Option<Groups>> {
		if !self.eat(t!("GROUP")) {
			return Ok(None);
		}

		let res = match self.peek_kind() {
			t!("ALL") => {
				self.pop_peek();
				Groups(Vec::new())
			}
			t!("BY") => {
				self.pop_peek();
				let mut groups = Groups(vec![Group(self.parse_basic_idiom()?)]);
				while self.eat(t!(",")) {
					groups.0.push(Group(self.parse_basic_idiom()?));
				}
				groups
			}
			_ => {
				let mut groups = Groups(vec![Group(self.parse_basic_idiom()?)]);
				while self.eat(t!(",")) {
					groups.0.push(Group(self.parse_basic_idiom()?));
				}
				groups
			}
		};

		Ok(Some(res))
	}

	/// Parse a permissions production
	///
	/// # Parser State
	/// Expects the parser to have just eaten the `PERMISSIONS` keyword.
	pub fn parse_permission(&mut self, permissive: bool) -> ParseResult<Permissions> {
		match self.next().kind {
			t!("NONE") => Ok(Permissions::none()),
			t!("FULL") => Ok(Permissions::full()),
			t!("FOR") => {
				let mut permission = if permissive {
					Permissions::full()
				} else {
					Permissions::none()
				};
				self.parse_specific_permission(&mut permission)?;
				self.eat(t!(","));
				while self.eat(t!("FOR")) {
					self.parse_specific_permission(&mut permission)?;
					self.eat(t!(","));
				}
				Ok(permission)
			}
			x => unexpected!(self, x, "'NONE', 'FULL' or 'FOR'"),
		}
	}

	/// Parse a specific permission for a type of query
	///
	/// Sets the permission for a specific query on the given permission keyword.
	///
	/// # Parser State
	/// Expects the parser to just have eaten the `FOR` keyword.
	pub fn parse_specific_permission(&mut self, permissions: &mut Permissions) -> ParseResult<()> {
		let mut select = false;
		let mut create = false;
		let mut update = false;
		let mut delete = false;

		loop {
			match self.next().kind {
				t!("SELECT") => {
					select = true;
				}
				t!("CREATE") => {
					create = true;
				}
				t!("UPDATE") => {
					update = true;
				}
				t!("DELETE") => {
					delete = true;
				}
				x => unexpected!(self, x, "'SELECT', 'CREATE', 'UPDATE' or 'DELETE'"),
			}
			if !self.eat(t!(",")) {
				break;
			}
		}

		let permission_value = self.parse_permission_value()?;
		if select {
			permissions.select = permission_value.clone();
		}
		if create {
			permissions.create = permission_value.clone();
		}
		if update {
			permissions.update = permission_value.clone();
		}
		if delete {
			permissions.delete = permission_value
		}

		Ok(())
	}

	/// Parses a the value for a permission for a type of query
	///
	/// # Parser State
	///
	/// Expects the parser to just have eaten either `SELECT`, `CREATE`, `UPDATE` or `DELETE`.
	pub fn parse_permission_value(&mut self) -> ParseResult<Permission> {
		match self.next().kind {
			t!("NONE") => Ok(Permission::None),
			t!("FULL") => Ok(Permission::Full),
			t!("WHERE") => Ok(Permission::Specific(self.parse_value_field()?)),
			x => unexpected!(self, x, "'NONE', 'FULL', or 'WHERE'"),
		}
	}

	/// Parses a base
	///
	/// So either `NAMESPACE`, ~DATABASE`, `ROOT`, or `SCOPE` if `scope_allowed` is true.
	///
	/// # Parser state
	/// Expects the next keyword to be a base.
	pub fn parse_base(&mut self, scope_allowed: bool) -> ParseResult<Base> {
		match self.next().kind {
			t!("NAMESPACE") => Ok(Base::Ns),
			t!("DATABASE") => Ok(Base::Db),
			t!("ROOT") => Ok(Base::Root),
			t!("SCOPE") => {
				if !scope_allowed {
					unexpected!(self, t!("SCOPE"), "a scope is not allowed here");
				}
				let name = self.next_token_value()?;
				Ok(Base::Sc(name))
			}
			x => {
				if scope_allowed {
					unexpected!(self, x, "'NAMEPSPACE', 'DATABASE', 'ROOT', 'SCOPE' or 'KV'")
				} else {
					unexpected!(self, x, "'NAMEPSPACE', 'DATABASE', 'ROOT', or 'KV'")
				}
			}
		}
	}

	/// Parses a changefeed production
	///
	/// # Parser State
	/// Expects the parser to have already eating the `CHANGEFEED` keyword
	pub fn parse_changefeed(&mut self) -> ParseResult<ChangeFeed> {
		let expiry = self.next_token_value::<Duration>()?.0;
		Ok(ChangeFeed {
			expiry,
		})
	}

	/// Parses a view production
	///
	/// # Parse State
	/// Expects the parser to have already eaten the possible `(` if the view was wrapped in
	/// parens. Expects the next keyword to be `SELECT`.
	pub fn parse_view(&mut self) -> ParseResult<View> {
		expected!(self, t!("SELECT"));
		let fields = self.parse_fields()?;
		expected!(self, t!("FROM"));
		let mut from = vec![self.next_token_value()?];
		while self.eat(t!(",")) {
			from.push(self.next_token_value()?);
		}

		let cond = self.try_parse_condition()?;
		let group = self.try_parse_group()?;

		Ok(View {
			expr: fields,
			what: Tables(from),
			cond,
			group,
		})
	}

	pub fn parse_distance(&mut self) -> ParseResult<Distance> {
		let dist = match self.next().kind {
			TokenKind::Distance(x) => match x {
				DistanceKind::Euclidean => Distance::Euclidean,
				DistanceKind::Manhattan => Distance::Manhattan,
				DistanceKind::Hamming => Distance::Hamming,
				DistanceKind::Minkowski => {
					let distance = self.next_token_value()?;
					Distance::Minkowski(distance)
				}
			},
			x => unexpected!(self, x, "a distance measure"),
		};
		Ok(dist)
	}

	pub fn try_parse_distance(&mut self) -> ParseResult<Option<Distance>> {
		if !self.eat(t!("DISTANCE")) {
			return Ok(None);
		}

		self.parse_distance().map(Some)
	}

	pub fn parse_custom_function_name(&mut self) -> ParseResult<Ident> {
		expected!(self, t!("fn"));
		expected!(self, t!("::"));
		let mut name = self.next_token_value::<Ident>()?;
		while self.eat(t!("::")) {
			let part = self.next_token_value::<Ident>()?;
			name.0.push_str("::");
			name.0.push_str(part.0.as_str());
		}
		Ok(name)
	}
}
