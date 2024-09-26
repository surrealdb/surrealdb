//! Contains parsing code for smaller common parts of statements.

use reblessive::Stk;

use crate::sql::Fetch;
use crate::syn::error::bail;
use crate::{
	sql::{
		changefeed::ChangeFeed,
		index::{Distance, VectorType},
		Base, Cond, Data, Duration, Fetchs, Field, Fields, Group, Groups, Ident, Idiom, Output,
		Permission, Permissions, Tables, Timeout, Value, View,
	},
	syn::{
		parser::{
			mac::{expected, unexpected},
			ParseResult, Parser,
		},
		token::{t, DistanceKind, Span, TokenKind, VectorTypeKind},
	},
};

pub(crate) enum MissingKind {
	Split,
	Order,
	Group,
}

impl Parser<'_> {
	/// Parses a data production if the next token is a data keyword.
	/// Otherwise returns None
	pub async fn try_parse_data(&mut self, ctx: &mut Stk) -> ParseResult<Option<Data>> {
		let res = match self.peek().kind {
			t!("SET") => {
				self.pop_peek();
				let mut set_list = Vec::new();
				loop {
					let idiom = self.parse_plain_idiom(ctx).await?;
					let operator = self.parse_assigner()?;
					let value = ctx.run(|ctx| self.parse_value_field(ctx)).await?;
					set_list.push((idiom, operator, value));
					if !self.eat(t!(",")) {
						break;
					}
				}
				Data::SetExpression(set_list)
			}
			t!("UNSET") => {
				self.pop_peek();
				let idiom_list = self.parse_idiom_list(ctx).await?;
				Data::UnsetExpression(idiom_list)
			}
			t!("PATCH") => {
				self.pop_peek();
				Data::PatchExpression(ctx.run(|ctx| self.parse_value_field(ctx)).await?)
			}
			t!("MERGE") => {
				self.pop_peek();
				Data::MergeExpression(ctx.run(|ctx| self.parse_value_field(ctx)).await?)
			}
			t!("REPLACE") => {
				self.pop_peek();
				Data::ReplaceExpression(ctx.run(|ctx| self.parse_value_field(ctx)).await?)
			}
			t!("CONTENT") => {
				self.pop_peek();
				Data::ContentExpression(ctx.run(|ctx| self.parse_value_field(ctx)).await?)
			}
			_ => return Ok(None),
		};
		Ok(Some(res))
	}

	/// Parses a statement output if the next token is `return`.
	pub async fn try_parse_output(&mut self, ctx: &mut Stk) -> ParseResult<Option<Output>> {
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
			_ => Output::Fields(self.parse_fields(ctx).await?),
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

	pub async fn try_parse_fetch(&mut self, ctx: &mut Stk) -> ParseResult<Option<Fetchs>> {
		if !self.eat(t!("FETCH")) {
			return Ok(None);
		}
		let mut fetchs = self.try_parse_param_or_idiom_or_fields(ctx).await?;
		while self.eat(t!(",")) {
			fetchs.append(&mut self.try_parse_param_or_idiom_or_fields(ctx).await?);
		}
		Ok(Some(Fetchs(fetchs)))
	}

	pub async fn try_parse_param_or_idiom_or_fields(
		&mut self,
		ctx: &mut Stk,
	) -> ParseResult<Vec<Fetch>> {
		match self.peek().kind {
			t!("$param") => Ok(vec![Value::Param(self.next_token_value()?).into()]),
			t!("TYPE") => {
				let fields = self.parse_fields(ctx).await?;
				let fetches = fields
					.0
					.into_iter()
					.filter_map(|f| {
						if let Field::Single {
							expr,
							..
						} = f
						{
							Some(expr.into())
						} else {
							None
						}
					})
					.collect();
				Ok(fetches)
			}
			_ => Ok(vec![Value::Idiom(self.parse_plain_idiom(ctx).await?).into()]),
		}
	}

	pub async fn try_parse_condition(&mut self, ctx: &mut Stk) -> ParseResult<Option<Cond>> {
		if !self.eat(t!("WHERE")) {
			return Ok(None);
		}
		let v = ctx.run(|ctx| self.parse_value_field(ctx)).await?;
		Ok(Some(Cond(v)))
	}

	pub(crate) fn check_idiom<'a>(
		kind: MissingKind,
		fields: &'a Fields,
		field_span: Span,
		idiom: &Idiom,
		idiom_span: Span,
	) -> ParseResult<&'a Field> {
		let mut found = None;
		for field in fields.iter() {
			let Field::Single {
				expr,
				alias,
			} = field
			else {
				unreachable!()
			};

			if let Some(alias) = alias {
				if idiom == alias {
					found = Some(field);
					break;
				}
			}

			match expr {
				Value::Idiom(x) => {
					if idiom == x {
						found = Some(field);
						break;
					}
				}
				v => {
					if *idiom == v.to_idiom() {
						found = Some(field);
						break;
					}
				}
			}
		}

		let Some(found) = found else {
			match kind {
				MissingKind::Split => {
					bail!(
						"Missing split idiom `{idiom}` in statement selection",
						@idiom_span,
						@field_span => "Idiom missing here",
					)
				}
				MissingKind::Order => {
					bail!(
						"Missing order idiom `{idiom}` in statement selection",
						@idiom_span,
						@field_span => "Idiom missing here",
					)
				}
				MissingKind::Group => {
					bail!(
						"Missing group idiom `{idiom}` in statement selection",
						@idiom_span,
						@field_span => "Idiom missing here",
					)
				}
			};
		};

		Ok(found)
	}

	pub async fn try_parse_group(
		&mut self,
		ctx: &mut Stk,
		fields: &Fields,
		fields_span: Span,
	) -> ParseResult<Option<Groups>> {
		if !self.eat(t!("GROUP")) {
			return Ok(None);
		}

		if self.eat(t!("ALL")) {
			return Ok(Some(Groups(Vec::new())));
		}

		self.eat(t!("BY"));

		let has_all = fields.contains(&Field::All);

		let before = self.peek().span;
		let group = self.parse_basic_idiom(ctx).await?;
		let group_span = before.covers(self.last_span());
		if !has_all {
			Self::check_idiom(MissingKind::Group, fields, fields_span, &group, group_span)?;
		}

		let mut groups = Groups(vec![Group(group)]);
		while self.eat(t!(",")) {
			let before = self.peek().span;
			let group = self.parse_basic_idiom(ctx).await?;
			let group_span = before.covers(self.last_span());
			if !has_all {
				Self::check_idiom(MissingKind::Group, fields, fields_span, &group, group_span)?;
			}
			groups.0.push(Group(group));
		}

		Ok(Some(groups))
	}

	/// Parse a permissions production
	///
	/// # Parser State
	/// Expects the parser to have just eaten the `PERMISSIONS` keyword.
	pub async fn parse_permission(
		&mut self,
		stk: &mut Stk,
		field: bool,
	) -> ParseResult<Permissions> {
		let next = self.next();
		match next.kind {
			t!("NONE") => Ok(Permissions::none()),
			t!("FULL") => Ok(Permissions::full()),
			t!("FOR") => {
				let mut permission = if field {
					Permissions::full()
				} else {
					Permissions::none()
				};
				stk.run(|stk| self.parse_specific_permission(stk, &mut permission, field)).await?;
				self.eat(t!(","));
				while self.eat(t!("FOR")) {
					stk.run(|stk| self.parse_specific_permission(stk, &mut permission, field))
						.await?;
					self.eat(t!(","));
				}
				Ok(permission)
			}
			_ => unexpected!(self, next, "'NONE', 'FULL' or 'FOR'"),
		}
	}

	/// Parse a specific permission for a type of query
	///
	/// Sets the permission for a specific query on the given permission keyword.
	///
	/// # Parser State
	/// Expects the parser to just have eaten the `FOR` keyword.
	pub async fn parse_specific_permission(
		&mut self,
		stk: &mut Stk,
		permissions: &mut Permissions,
		field: bool,
	) -> ParseResult<()> {
		let mut select = false;
		let mut create = false;
		let mut update = false;
		let mut delete = false;

		loop {
			let next = self.next();
			match next.kind {
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
					// TODO(gguillemas): Return a parse error instead of logging a warning in 3.0.0.
					if field {
						warn!("The DELETE permission has no effect on fields and is deprecated, but was found in a DEFINE FIELD statement.");
					} else {
						delete = true;
					}
				}
				_ if field => unexpected!(self, next, "'SELECT', 'CREATE' or 'UPDATE'"),
				_ => unexpected!(self, next, "'SELECT', 'CREATE', 'UPDATE' or 'DELETE'"),
			}
			if !self.eat(t!(",")) {
				break;
			}
		}

		let permission_value = self.parse_permission_value(stk).await?;
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
	pub async fn parse_permission_value(&mut self, stk: &mut Stk) -> ParseResult<Permission> {
		let next = self.next();
		match next.kind {
			t!("NONE") => Ok(Permission::None),
			t!("FULL") => Ok(Permission::Full),
			t!("WHERE") => Ok(Permission::Specific(self.parse_value_field(stk).await?)),
			_ => unexpected!(self, next, "'NONE', 'FULL', or 'WHERE'"),
		}
	}

	// TODO(gguillemas): Deprecated in 2.0.0. Kept for backward compatibility. Drop it in 3.0.0.
	/// Parses a base
	///
	/// So either `NAMESPACE`, `DATABASE`, `ROOT`, or `SCOPE` if `scope_allowed` is true.
	///
	/// # Parser state
	/// Expects the next keyword to be a base.
	pub fn parse_base(&mut self, scope_allowed: bool) -> ParseResult<Base> {
		let next = self.next();
		match next.kind {
			t!("NAMESPACE") => Ok(Base::Ns),
			t!("DATABASE") => Ok(Base::Db),
			t!("ROOT") => Ok(Base::Root),
			t!("SCOPE") => {
				if !scope_allowed {
					unexpected!(self, next, "a scope is not allowed here");
				}
				let name = self.next_token_value()?;
				Ok(Base::Sc(name))
			}
			_ => {
				if scope_allowed {
					unexpected!(self, next, "'NAMEPSPACE', 'DATABASE', 'ROOT' or 'SCOPE'")
				} else {
					unexpected!(self, next, "'NAMEPSPACE', 'DATABASE' or 'ROOT'")
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
		let store_diff = if self.eat(t!("INCLUDE")) {
			expected!(self, t!("ORIGINAL"));
			true
		} else {
			false
		};

		Ok(ChangeFeed {
			expiry,
			store_diff,
		})
	}

	/// Parses a view production
	///
	/// # Parse State
	/// Expects the parser to have already eaten the possible `(` if the view was wrapped in
	/// parens. Expects the next keyword to be `SELECT`.
	pub async fn parse_view(&mut self, stk: &mut Stk) -> ParseResult<View> {
		expected!(self, t!("SELECT"));
		let before_fields = self.peek().span;
		let fields = self.parse_fields(stk).await?;
		let fields_span = before_fields.covers(self.recent_span());
		expected!(self, t!("FROM"));
		let mut from = vec![self.next_token_value()?];
		while self.eat(t!(",")) {
			from.push(self.next_token_value()?);
		}

		let cond = self.try_parse_condition(stk).await?;
		let group = self.try_parse_group(stk, &fields, fields_span).await?;

		Ok(View {
			expr: fields,
			what: Tables(from),
			cond,
			group,
		})
	}

	pub fn parse_distance(&mut self) -> ParseResult<Distance> {
		let next = self.next();
		match next.kind {
			TokenKind::Distance(k) => {
				let dist = match k {
					DistanceKind::Chebyshev => Distance::Chebyshev,
					DistanceKind::Cosine => Distance::Cosine,
					DistanceKind::Euclidean => Distance::Euclidean,
					DistanceKind::Manhattan => Distance::Manhattan,
					DistanceKind::Hamming => Distance::Hamming,
					DistanceKind::Jaccard => Distance::Jaccard,

					DistanceKind::Minkowski => {
						let distance = self.next_token_value()?;
						Distance::Minkowski(distance)
					}
					DistanceKind::Pearson => Distance::Pearson,
				};
				Ok(dist)
			}
			_ => unexpected!(self, next, "a distance measure"),
		}
	}

	pub fn parse_vector_type(&mut self) -> ParseResult<VectorType> {
		let next = self.next();
		match next.kind {
			TokenKind::VectorType(x) => Ok(match x {
				VectorTypeKind::F64 => VectorType::F64,
				VectorTypeKind::F32 => VectorType::F32,
				VectorTypeKind::I64 => VectorType::I64,
				VectorTypeKind::I32 => VectorType::I32,
				VectorTypeKind::I16 => VectorType::I16,
			}),
			_ => unexpected!(self, next, "a vector type"),
		}
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
