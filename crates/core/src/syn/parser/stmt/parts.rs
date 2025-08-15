//! Contains parsing code for smaller common parts of statements.

use reblessive::Stk;

use crate::sql::changefeed::ChangeFeed;
use crate::sql::index::{Distance, VectorType};
use crate::sql::reference::{Reference, ReferenceDeleteStrategy};
use crate::sql::{
	Base, Cond, Data, Explain, Expr, Fetch, Fetchs, Field, Fields, Group, Groups, Ident, Idiom,
	Output, Permission, Permissions, Timeout, View, With,
};
use crate::syn::error::bail;
use crate::syn::parser::mac::{expected, unexpected};
use crate::syn::parser::{ParseResult, Parser};
use crate::syn::token::{DistanceKind, Span, TokenKind, VectorTypeKind, t};
use crate::val::Duration;

pub(crate) enum MissingKind {
	Split,
	Order,
	Group,
}

impl Parser<'_> {
	/// Parses a data production if the next token is a data keyword.
	/// Otherwise returns None
	pub async fn try_parse_data(&mut self, stk: &mut Stk) -> ParseResult<Option<Data>> {
		let res = match self.peek().kind {
			t!("SET") => {
				self.pop_peek();
				let mut set_list = Vec::new();
				loop {
					let assignment = self.parse_assignment(stk).await?;
					set_list.push(assignment);
					if !self.eat(t!(",")) {
						break;
					}
				}
				Data::SetExpression(set_list)
			}
			t!("UNSET") => {
				self.pop_peek();
				let idiom_list = self.parse_idiom_list(stk).await?;
				Data::UnsetExpression(idiom_list)
			}
			t!("PATCH") => {
				self.pop_peek();
				Data::PatchExpression(stk.run(|ctx| self.parse_expr_field(ctx)).await?)
			}
			t!("MERGE") => {
				self.pop_peek();
				Data::MergeExpression(stk.run(|ctx| self.parse_expr_field(ctx)).await?)
			}
			t!("REPLACE") => {
				self.pop_peek();
				Data::ReplaceExpression(stk.run(|ctx| self.parse_expr_field(ctx)).await?)
			}
			t!("CONTENT") => {
				self.pop_peek();
				Data::ContentExpression(stk.run(|ctx| self.parse_expr_field(ctx)).await?)
			}
			_ => return Ok(None),
		};
		Ok(Some(res))
	}

	/// Parses a statement output if the next token is `return`.
	pub async fn try_parse_output(&mut self, stk: &mut Stk) -> ParseResult<Option<Output>> {
		if !self.eat(t!("RETURN")) {
			return Ok(None);
		}
		self.parse_output(stk).await.map(Some)
	}

	/// Needed because some part of the RPC needs to call into the parser for
	/// this specific part.
	pub async fn parse_output(&mut self, stk: &mut Stk) -> ParseResult<Output> {
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
			_ => Output::Fields(self.parse_fields(stk).await?),
		};
		Ok(res)
	}

	/// Parses a statement timeout if the next token is `TIMEOUT`.
	pub fn try_parse_timeout(&mut self) -> ParseResult<Option<Timeout>> {
		if !self.eat(t!("TIMEOUT")) {
			return Ok(None);
		}
		let duration = self.next_token_value()?;
		Ok(Some(Timeout(duration)))
	}

	pub async fn try_parse_fetch(&mut self, stk: &mut Stk) -> ParseResult<Option<Fetchs>> {
		if !self.eat(t!("FETCH")) {
			return Ok(None);
		}
		Ok(Some(self.parse_fetchs(stk).await?))
	}

	pub async fn parse_fetchs(&mut self, stk: &mut Stk) -> ParseResult<Fetchs> {
		let mut fetchs = self.try_parse_param_or_idiom_or_fields(stk).await?;
		while self.eat(t!(",")) {
			fetchs.append(&mut self.try_parse_param_or_idiom_or_fields(stk).await?);
		}
		Ok(Fetchs(fetchs))
	}

	pub async fn try_parse_param_or_idiom_or_fields(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<Vec<Fetch>> {
		match self.peek().kind {
			t!("$param") => Ok(vec![Fetch(Expr::Param(self.next_token_value()?))]),
			t!("TYPE") => {
				let fields = self.parse_fields(stk).await?;

				let fetches = match fields {
					Fields::Value(field) => match *field {
						Field::All => Vec::new(),
						Field::Single {
							expr,
							..
						} => vec![Fetch(expr)],
					},
					Fields::Select(fields) => fields
						.into_iter()
						.filter_map(|f| match f {
							Field::All => None,
							Field::Single {
								expr,
								..
							} => Some(Fetch(expr)),
						})
						.collect(),
				};

				Ok(fetches)
			}
			_ => Ok(vec![Fetch(Expr::Idiom(self.parse_plain_idiom(stk).await?))]),
		}
	}

	pub async fn try_parse_condition(&mut self, stk: &mut Stk) -> ParseResult<Option<Cond>> {
		if !self.eat(t!("WHERE")) {
			return Ok(None);
		}
		let v = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
		Ok(Some(Cond(v)))
	}

	/// Move this out of the parser.
	pub(crate) fn check_idiom(
		kind: MissingKind,
		fields: &Fields,
		field_span: Span,
		idiom: &Idiom,
		idiom_span: Span,
	) -> ParseResult<()> {
		let mut found = false;
		match fields {
			Fields::Value(field) => {
				let Field::Single {
					ref expr,
					ref alias,
				} = **field
				else {
					unreachable!()
				};

				if let Some(alias) = alias {
					if idiom == alias {
						found = true;
					}
				}

				match expr {
					Expr::Idiom(x) => {
						if idiom == x {
							found = true;
						}
					}
					v => {
						if *idiom == v.to_idiom() {
							found = true;
						}
					}
				}
			}
			Fields::Select(fields) => {
				for field in fields.iter() {
					let Field::Single {
						expr,
						alias,
					} = field
					else {
						// All is in the idiom so assume that the field is present.
						return Ok(());
					};

					if let Some(alias) = alias {
						if idiom == alias {
							found = true;
							break;
						}
					}

					match expr {
						Expr::Idiom(x) => {
							if idiom == x {
								found = true;
								break;
							}
						}
						v => {
							if *idiom == v.to_idiom() {
								found = true;
								break;
							}
						}
					}
				}
			}
		}

		if !found {
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
		Ok(())
	}

	pub async fn try_parse_group(
		&mut self,
		stk: &mut Stk,
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

		let has_all = fields.contains_all();

		let before = self.peek().span;
		let group = self.parse_basic_idiom(stk).await?;
		let group_span = before.covers(self.last_span());
		if !has_all {
			Self::check_idiom(MissingKind::Group, fields, fields_span, &group, group_span)?;
		}

		let mut groups = Groups(vec![Group(group)]);
		while self.eat(t!(",")) {
			let before = self.peek().span;
			let group = self.parse_basic_idiom(stk).await?;
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
	/// Sets the permission for a specific query on the given permission
	/// keyword.
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
						warn!(
							"The DELETE permission has no effect on fields and is deprecated, but was found in a DEFINE FIELD statement."
						);
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
	/// Expects the parser to just have eaten either `SELECT`, `CREATE`,
	/// `UPDATE` or `DELETE`.
	pub async fn parse_permission_value(&mut self, stk: &mut Stk) -> ParseResult<Permission> {
		let next = self.next();
		match next.kind {
			t!("NONE") => Ok(Permission::None),
			t!("FULL") => Ok(Permission::Full),
			t!("WHERE") => {
				Ok(Permission::Specific(stk.run(|stk| self.parse_expr_field(stk)).await?))
			}
			_ => unexpected!(self, next, "'NONE', 'FULL', or 'WHERE'"),
		}
	}

	// TODO(gguillemas): Deprecated in 2.0.0. Kept for backward compatibility. Drop
	// it in 3.0.0.
	/// Parses a base
	///
	/// So either `NAMESPACE`, `DATABASE`, `ROOT`, or `SCOPE` if `scope_allowed`
	/// is true.
	///
	/// # Parser state
	/// Expects the next keyword to be a base.
	pub fn parse_base(&mut self) -> ParseResult<Base> {
		let next = self.next();
		match next.kind {
			t!("NAMESPACE") => Ok(Base::Ns),
			t!("DATABASE") => Ok(Base::Db),
			t!("ROOT") => Ok(Base::Root),
			_ => {
				unexpected!(self, next, "'NAMEPSPACE', 'DATABASE' or 'ROOT'")
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

	/// Parses a reference
	///
	/// # Parser State
	/// Expects the parser to have already eating the `REFERENCE` keyword
	pub async fn parse_reference(&mut self, stk: &mut Stk) -> ParseResult<Reference> {
		let on_delete = if self.eat(t!("ON")) {
			expected!(self, t!("DELETE"));
			let next = self.next();
			match next.kind {
				t!("REJECT") => ReferenceDeleteStrategy::Reject,
				t!("CASCADE") => ReferenceDeleteStrategy::Cascade,
				t!("IGNORE") => ReferenceDeleteStrategy::Ignore,
				t!("UNSET") => ReferenceDeleteStrategy::Unset,
				t!("THEN") => ReferenceDeleteStrategy::Custom(
					stk.run(|stk| self.parse_expr_field(stk)).await?,
				),
				_ => {
					unexpected!(self, next, "`REJECT`, `CASCASE`, `IGNORE`, `UNSET` or `THEN`")
				}
			}
		} else {
			ReferenceDeleteStrategy::Ignore
		};

		Ok(Reference {
			on_delete,
		})
	}

	/// Parses a view production
	///
	/// # Parse State
	/// Expects the parser to have already eaten the possible `(` if the view
	/// was wrapped in parens. Expects the next keyword to be `SELECT`.
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
			what: from,
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
		let mut name = self.next_token_value::<Ident>()?.into_string();
		while self.eat(t!("::")) {
			let part = self.next_token_value::<Ident>()?.into_string();
			name.push_str("::");
			name.push_str(part.as_str());
		}
		// Safety: Parser guarentees no null bytes.
		Ok(unsafe { Ident::new_unchecked(name) })
	}
	pub(super) fn try_parse_explain(&mut self) -> ParseResult<Option<Explain>> {
		Ok(self.eat(t!("EXPLAIN")).then(|| Explain(self.eat(t!("FULL")))))
	}

	pub(super) fn try_parse_with(&mut self) -> ParseResult<Option<With>> {
		if !self.eat(t!("WITH")) {
			return Ok(None);
		}
		let next = self.next();
		let with = match next.kind {
			t!("NOINDEX") => With::NoIndex,
			t!("NO") => {
				expected!(self, t!("INDEX"));
				With::NoIndex
			}
			t!("INDEX") => {
				let mut index = vec![self.next_token_value::<Ident>()?.into_string()];
				while self.eat(t!(",")) {
					index.push(self.next_token_value::<Ident>()?.into_string());
				}
				With::Index(index)
			}
			_ => unexpected!(self, next, "`NO`, `NOINDEX` or `INDEX`"),
		};
		Ok(Some(with))
	}
}
