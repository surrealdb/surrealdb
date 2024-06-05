use reblessive::Stk;

use crate::sql::access_type::JwtAccessVerify;
use crate::sql::index::HnswParams;
use crate::{
	sql::{
		access_type,
		base::Base,
		filter::Filter,
		index::{Distance, VectorType},
		statements::{
			DefineAccessStatement, DefineAnalyzerStatement, DefineDatabaseStatement,
			DefineEventStatement, DefineFieldStatement, DefineFunctionStatement,
			DefineIndexStatement, DefineNamespaceStatement, DefineParamStatement, DefineStatement,
			DefineTableStatement, DefineUserStatement,
		},
		table_type,
		tokenizer::Tokenizer,
		user, AccessType, Ident, Idioms, Index, Kind, Param, Permissions, Scoring, Strand,
		TableType, Values,
	},
	syn::{
		parser::{
			mac::{expected, unexpected},
			ParseResult, Parser,
		},
		token::{t, Keyword, TokenKind},
	},
};

impl Parser<'_> {
	pub async fn parse_define_stmt(&mut self, ctx: &mut Stk) -> ParseResult<DefineStatement> {
		match self.next().kind {
			t!("NAMESPACE") => self.parse_define_namespace().map(DefineStatement::Namespace),
			t!("DATABASE") => self.parse_define_database().map(DefineStatement::Database),
			t!("FUNCTION") => self.parse_define_function(ctx).await.map(DefineStatement::Function),
			t!("USER") => self.parse_define_user().map(DefineStatement::User),
			t!("TOKEN") => self.parse_define_token().map(DefineStatement::Access),
			t!("SCOPE") => self.parse_define_scope(ctx).await.map(DefineStatement::Access),
			t!("PARAM") => self.parse_define_param(ctx).await.map(DefineStatement::Param),
			t!("TABLE") => self.parse_define_table(ctx).await.map(DefineStatement::Table),
			t!("EVENT") => {
				ctx.run(|ctx| self.parse_define_event(ctx)).await.map(DefineStatement::Event)
			}
			t!("FIELD") => {
				ctx.run(|ctx| self.parse_define_field(ctx)).await.map(DefineStatement::Field)
			}
			t!("INDEX") => self.parse_define_index().map(DefineStatement::Index),
			t!("ANALYZER") => self.parse_define_analyzer().map(DefineStatement::Analyzer),
			t!("ACCESS") => self.parse_define_access(ctx).await.map(DefineStatement::Access),
			x => unexpected!(self, x, "a define statement keyword"),
		}
	}

	pub fn parse_define_namespace(&mut self) -> ParseResult<DefineNamespaceStatement> {
		let if_not_exists = if self.eat(t!("IF")) {
			expected!(self, t!("NOT"));
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.next_token_value()?;
		let mut res = DefineNamespaceStatement {
			id: None,
			name,
			if_not_exists,
			..Default::default()
		};

		while let t!("COMMENT") = self.peek_kind() {
			self.pop_peek();
			res.comment = Some(self.next_token_value()?);
		}

		Ok(res)
	}

	pub fn parse_define_database(&mut self) -> ParseResult<DefineDatabaseStatement> {
		let if_not_exists = if self.eat(t!("IF")) {
			expected!(self, t!("NOT"));
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.next_token_value()?;
		let mut res = DefineDatabaseStatement {
			name,
			if_not_exists,
			..Default::default()
		};
		loop {
			match self.peek_kind() {
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = Some(self.next_token_value()?);
				}
				t!("CHANGEFEED") => {
					self.pop_peek();
					res.changefeed = Some(self.parse_changefeed()?);
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub async fn parse_define_function(
		&mut self,
		ctx: &mut Stk,
	) -> ParseResult<DefineFunctionStatement> {
		let if_not_exists = if self.eat(t!("IF")) {
			expected!(self, t!("NOT"));
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.parse_custom_function_name()?;
		let token = expected!(self, t!("(")).span;
		let mut args = Vec::new();
		loop {
			if self.eat(t!(")")) {
				break;
			}

			let param = self.next_token_value::<Param>()?.0;
			expected!(self, t!(":"));
			let kind = ctx.run(|ctx| self.parse_inner_kind(ctx)).await?;

			args.push((param, kind));

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!(")"), token)?;
				break;
			}
		}

		let next = expected!(self, t!("{")).span;
		let block = self.parse_block(ctx, next).await?;

		let mut res = DefineFunctionStatement {
			name,
			args,
			block,
			if_not_exists,
			..Default::default()
		};

		loop {
			match self.peek_kind() {
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = Some(self.next_token_value()?);
				}
				t!("PERMISSIONS") => {
					self.pop_peek();
					res.permissions = ctx.run(|ctx| self.parse_permission_value(ctx)).await?;
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub fn parse_define_user(&mut self) -> ParseResult<DefineUserStatement> {
		let if_not_exists = if self.eat(t!("IF")) {
			expected!(self, t!("NOT"));
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.next_token_value()?;
		expected!(self, t!("ON"));
		let base = self.parse_base(false)?;

		let mut res = DefineUserStatement::from_parsed_values(
			name,
			base,
			vec!["Viewer".into()], // New users get the viewer role by default
			user::UserDuration::default(),
		);

		if if_not_exists {
			res.if_not_exists = true;
		}

		loop {
			match self.peek_kind() {
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = Some(self.next_token_value()?);
				}
				t!("PASSWORD") => {
					self.pop_peek();
					res.set_password(&self.next_token_value::<Strand>()?.0);
				}
				t!("PASSHASH") => {
					self.pop_peek();
					res.set_passhash(self.next_token_value::<Strand>()?.0);
				}
				t!("ROLES") => {
					self.pop_peek();
					res.roles = vec![self.next_token_value()?];
					while self.eat(t!(",")) {
						res.roles.push(self.next_token_value()?);
					}
				}
				t!("DURATION") => {
					self.pop_peek();
					while self.eat(t!("FOR")) {
						match self.peek_kind() {
							t!("TOKEN") => {
								self.pop_peek();
								match self.peek_kind() {
									t!("NONE") => {
										// Currently, SurrealDB does not accept tokens without expiration.
										// For this reason, some token duration must be set.
										unexpected!(self, t!("NONE"), "a token duration");
									}
									_ => res.set_token_duration(Some(self.next_token_value()?)),
								}
							}
							t!("SESSION") => {
								self.pop_peek();
								match self.peek_kind() {
									t!("NONE") => {
										self.pop_peek();
										res.set_session_duration(None)
									}
									_ => res.set_session_duration(Some(self.next_token_value()?)),
								}
							}
							_ => break,
						}
						self.eat(t!(","));
					}
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub async fn parse_define_access(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<DefineAccessStatement> {
		let if_not_exists = if self.eat(t!("IF")) {
			expected!(self, t!("NOT"));
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.next_token_value()?;
		expected!(self, t!("ON"));
		// TODO: Parse base should no longer take an argument.
		let base = self.parse_base(false)?;

		let mut res = DefineAccessStatement {
			name,
			base,
			if_not_exists,
			..Default::default()
		};

		loop {
			match self.peek_kind() {
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = Some(self.next_token_value()?);
				}
				t!("TYPE") => {
					self.pop_peek();
					match self.peek_kind() {
						t!("JWT") => {
							self.pop_peek();
							res.kind = AccessType::Jwt(self.parse_jwt()?);
						}
						t!("RECORD") => {
							self.pop_peek();
							let mut ac = access_type::RecordAccess {
								..Default::default()
							};
							loop {
								match self.peek_kind() {
									t!("SIGNUP") => {
										self.pop_peek();
										ac.signup =
											Some(stk.run(|stk| self.parse_value(stk)).await?);
									}
									t!("SIGNIN") => {
										self.pop_peek();
										ac.signin =
											Some(stk.run(|stk| self.parse_value(stk)).await?);
									}
									_ => break,
								}
							}
							if self.eat(t!("WITH")) {
								expected!(self, t!("JWT"));
								ac.jwt = self.parse_jwt()?;
							}
							res.kind = AccessType::Record(ac);
						}
						_ => break,
					}
				}
				t!("DURATION") => {
					self.pop_peek();
					while self.eat(t!("FOR")) {
						match self.peek_kind() {
							t!("GRANT") => {
								self.pop_peek();
								match self.peek_kind() {
									t!("NONE") => {
										self.pop_peek();
										res.duration.grant = None
									}
									_ => res.duration.grant = Some(self.next_token_value()?),
								}
							}
							t!("TOKEN") => {
								self.pop_peek();
								match self.peek_kind() {
									t!("NONE") => {
										// Currently, SurrealDB does not accept tokens without expiration.
										// For this reason, some token duration must be set.
										// In the future, allowing issuing tokens without expiration may be useful.
										// Tokens issued by access methods can be consumed by third parties that support it.
										unexpected!(self, t!("NONE"), "a token duration");
									}
									_ => res.duration.token = Some(self.next_token_value()?),
								}
							}
							t!("SESSION") => {
								self.pop_peek();
								match self.peek_kind() {
									t!("NONE") => {
										self.pop_peek();
										res.duration.session = None
									}
									_ => res.duration.session = Some(self.next_token_value()?),
								}
							}
							_ => break,
						}
						self.eat(t!(","));
					}
				}
				_ => break,
			}
		}

		Ok(res)
	}

	// TODO(gguillemas): Deprecated in 2.0.0. Drop this in 3.0.0 in favor of DEFINE ACCESS
	pub fn parse_define_token(&mut self) -> ParseResult<DefineAccessStatement> {
		let if_not_exists = if self.eat(t!("IF")) {
			expected!(self, t!("NOT"));
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.next_token_value()?;
		expected!(self, t!("ON"));
		let base = self.parse_base(true)?;

		let mut res = DefineAccessStatement {
			name,
			base: base.clone(),
			if_not_exists,
			..Default::default()
		};

		match base {
			// DEFINE TOKEN ON SCOPE is now record access with JWT
			Base::Sc(_) => {
				res.base = Base::Db;
				let mut ac = access_type::RecordAccess {
					..Default::default()
				};
				ac.jwt.issue = None;
				loop {
					match self.peek_kind() {
						t!("COMMENT") => {
							self.pop_peek();
							res.comment = Some(self.next_token_value()?);
						}
						// For backward compatibility, value is always expected after type
						// This matches the display format of the legacy statement
						t!("TYPE") => {
							self.pop_peek();
							match self.next().kind {
								TokenKind::Algorithm(alg) => {
									expected!(self, t!("VALUE"));
									ac.jwt.verify = access_type::JwtAccessVerify::Key(
										access_type::JwtAccessVerifyKey {
											alg,
											key: self.next_token_value::<Strand>()?.0,
										},
									);
								}
								TokenKind::Keyword(Keyword::Jwks) => {
									expected!(self, t!("VALUE"));
									ac.jwt.verify = access_type::JwtAccessVerify::Jwks(
										access_type::JwtAccessVerifyJwks {
											url: self.next_token_value::<Strand>()?.0,
										},
									);
								}
								x => unexpected!(self, x, "a token algorithm or 'JWKS'"),
							}
						}
						_ => break,
					}
				}
				res.kind = AccessType::Record(ac);
			}
			// DEFINE TOKEN anywhere else is now JWT access
			_ => {
				let mut ac = access_type::JwtAccess {
					issue: None,
					..Default::default()
				};
				loop {
					match self.peek_kind() {
						t!("COMMENT") => {
							self.pop_peek();
							res.comment = Some(self.next_token_value()?);
						}
						// For backward compatibility, value is always expected after type
						// This matches the display format of the legacy statement
						t!("TYPE") => {
							self.pop_peek();
							match self.next().kind {
								TokenKind::Algorithm(alg) => {
									expected!(self, t!("VALUE"));
									ac.verify = access_type::JwtAccessVerify::Key(
										access_type::JwtAccessVerifyKey {
											alg,
											key: self.next_token_value::<Strand>()?.0,
										},
									);
								}
								TokenKind::Keyword(Keyword::Jwks) => {
									expected!(self, t!("VALUE"));
									ac.verify = access_type::JwtAccessVerify::Jwks(
										access_type::JwtAccessVerifyJwks {
											url: self.next_token_value::<Strand>()?.0,
										},
									);
								}
								x => unexpected!(self, x, "a token algorithm or 'JWKS'"),
							}
						}
						_ => break,
					}
				}
				res.kind = AccessType::Jwt(ac);
			}
		}

		Ok(res)
	}

	// TODO(gguillemas): Deprecated in 2.0.0. Drop this in 3.0.0 in favor of DEFINE ACCESS
	pub async fn parse_define_scope(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<DefineAccessStatement> {
		let if_not_exists = if self.eat(t!("IF")) {
			expected!(self, t!("NOT"));
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.next_token_value()?;
		let mut res = DefineAccessStatement {
			name,
			base: Base::Db,
			if_not_exists,
			..Default::default()
		};
		let mut ac = access_type::RecordAccess {
			..Default::default()
		};

		loop {
			match self.peek_kind() {
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = Some(self.next_token_value()?);
				}
				t!("SESSION") => {
					self.pop_peek();
					res.duration.session = Some(self.next_token_value()?);
				}
				t!("SIGNUP") => {
					self.pop_peek();
					ac.signup = Some(stk.run(|stk| self.parse_value(stk)).await?);
				}
				t!("SIGNIN") => {
					self.pop_peek();
					ac.signin = Some(stk.run(|stk| self.parse_value(stk)).await?);
				}
				_ => break,
			}
		}

		res.kind = AccessType::Record(ac);

		Ok(res)
	}

	pub async fn parse_define_param(&mut self, ctx: &mut Stk) -> ParseResult<DefineParamStatement> {
		let if_not_exists = if self.eat(t!("IF")) {
			expected!(self, t!("NOT"));
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.next_token_value::<Param>()?.0;

		let mut res = DefineParamStatement {
			name,
			if_not_exists,
			..Default::default()
		};

		loop {
			match self.peek_kind() {
				t!("VALUE") => {
					self.pop_peek();
					res.value = ctx.run(|ctx| self.parse_value(ctx)).await?;
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = Some(self.next_token_value()?);
				}
				t!("PERMISSIONS") => {
					self.pop_peek();
					res.permissions = ctx.run(|ctx| self.parse_permission_value(ctx)).await?;
				}
				_ => break,
			}
		}
		Ok(res)
	}

	pub async fn parse_define_table(&mut self, ctx: &mut Stk) -> ParseResult<DefineTableStatement> {
		let if_not_exists = if self.eat(t!("IF")) {
			expected!(self, t!("NOT"));
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.next_token_value()?;
		let mut res = DefineTableStatement {
			name,
			permissions: Permissions::none(),
			if_not_exists,
			..Default::default()
		};

		loop {
			match self.peek_kind() {
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = Some(self.next_token_value()?);
				}
				t!("DROP") => {
					self.pop_peek();
					res.drop = true;
				}
				t!("TYPE") => {
					self.pop_peek();
					match self.peek_kind() {
						t!("NORMAL") => {
							self.pop_peek();
							res.kind = TableType::Normal;
						}
						t!("RELATION") => {
							self.pop_peek();
							res.kind = TableType::Relation(self.parse_relation_schema()?);
						}
						t!("ANY") => {
							self.pop_peek();
							res.kind = TableType::Any;
						}
						x => unexpected!(self, x, "`NORMAL`, `RELATION`, or `ANY`"),
					}
				}
				t!("SCHEMALESS") => {
					self.pop_peek();
					res.full = false;
				}
				t!("SCHEMAFULL") => {
					self.pop_peek();
					res.full = true;
				}
				t!("PERMISSIONS") => {
					self.pop_peek();
					res.permissions = ctx.run(|ctx| self.parse_permission(ctx, false)).await?;
				}
				t!("CHANGEFEED") => {
					self.pop_peek();
					res.changefeed = Some(self.parse_changefeed()?);
				}
				t!("AS") => {
					self.pop_peek();
					match self.peek_kind() {
						t!("(") => {
							let open = self.pop_peek().span;
							res.view = Some(self.parse_view(ctx).await?);
							self.expect_closing_delimiter(t!(")"), open)?;
						}
						t!("SELECT") => {
							res.view = Some(self.parse_view(ctx).await?);
						}
						x => unexpected!(self, x, "`SELECT`"),
					}
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub async fn parse_define_event(&mut self, ctx: &mut Stk) -> ParseResult<DefineEventStatement> {
		let if_not_exists = if self.eat(t!("IF")) {
			expected!(self, t!("NOT"));
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.next_token_value()?;
		expected!(self, t!("ON"));
		self.eat(t!("TABLE"));
		let what = self.next_token_value()?;

		let mut res = DefineEventStatement {
			name,
			what,
			if_not_exists,
			..Default::default()
		};

		loop {
			match self.peek_kind() {
				t!("WHEN") => {
					self.pop_peek();
					res.when = ctx.run(|ctx| self.parse_value(ctx)).await?;
				}
				t!("THEN") => {
					self.pop_peek();
					res.then = Values(vec![ctx.run(|ctx| self.parse_value(ctx)).await?]);
					while self.eat(t!(",")) {
						res.then.0.push(ctx.run(|ctx| self.parse_value(ctx)).await?)
					}
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = Some(self.next_token_value()?);
				}
				_ => break,
			}
		}
		Ok(res)
	}

	pub async fn parse_define_field(&mut self, ctx: &mut Stk) -> ParseResult<DefineFieldStatement> {
		let if_not_exists = if self.eat(t!("IF")) {
			expected!(self, t!("NOT"));
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.parse_local_idiom()?;
		expected!(self, t!("ON"));
		self.eat(t!("TABLE"));
		let what = self.next_token_value()?;

		let mut res = DefineFieldStatement {
			name,
			what,
			if_not_exists,
			..Default::default()
		};

		loop {
			match self.peek_kind() {
				// FLEX, FLEXI and FLEXIBLE are all the same token type.
				t!("FLEXIBLE") => {
					self.pop_peek();
					res.flex = true;
				}
				t!("TYPE") => {
					self.pop_peek();
					res.kind = Some(ctx.run(|ctx| self.parse_inner_kind(ctx)).await?);
				}
				t!("READONLY") => {
					self.pop_peek();
					res.readonly = true;
				}
				t!("VALUE") => {
					self.pop_peek();
					res.value = Some(ctx.run(|ctx| self.parse_value(ctx)).await?);
				}
				t!("ASSERT") => {
					self.pop_peek();
					res.assert = Some(ctx.run(|ctx| self.parse_value(ctx)).await?);
				}
				t!("DEFAULT") => {
					self.pop_peek();
					res.default = Some(ctx.run(|ctx| self.parse_value(ctx)).await?);
				}
				t!("PERMISSIONS") => {
					self.pop_peek();
					res.permissions = ctx.run(|ctx| self.parse_permission(ctx, true)).await?;
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = Some(self.next_token_value()?);
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub fn parse_define_index(&mut self) -> ParseResult<DefineIndexStatement> {
		let if_not_exists = if self.eat(t!("IF")) {
			expected!(self, t!("NOT"));
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.next_token_value()?;
		expected!(self, t!("ON"));
		self.eat(t!("TABLE"));
		let what = self.next_token_value()?;

		let mut res = DefineIndexStatement {
			name,
			what,

			if_not_exists,
			..Default::default()
		};

		loop {
			match self.peek_kind() {
				// COLUMNS and FIELDS are the same tokenkind
				t!("FIELDS") => {
					self.pop_peek();
					res.cols = Idioms(vec![self.parse_local_idiom()?]);
					while self.eat(t!(",")) {
						res.cols.0.push(self.parse_local_idiom()?);
					}
				}
				t!("UNIQUE") => {
					self.pop_peek();
					res.index = Index::Uniq;
				}
				t!("SEARCH") => {
					self.pop_peek();
					let mut analyzer: Option<Ident> = None;
					let mut scoring = None;
					let mut doc_ids_order = 100;
					let mut doc_lengths_order = 100;
					let mut postings_order = 100;
					let mut terms_order = 100;
					let mut doc_ids_cache = 100;
					let mut doc_lengths_cache = 100;
					let mut postings_cache = 100;
					let mut terms_cache = 100;
					let mut hl = false;

					loop {
						match self.peek_kind() {
							t!("ANALYZER") => {
								self.pop_peek();
								analyzer = Some(self.next_token_value()).transpose()?;
							}
							t!("VS") => {
								self.pop_peek();
								scoring = Some(Scoring::Vs);
							}
							t!("BM25") => {
								self.pop_peek();
								if self.eat(t!("(")) {
									let open = self.last_span();
									let k1 = self.next_token_value()?;
									expected!(self, t!(","));
									let b = self.next_token_value()?;
									self.expect_closing_delimiter(t!(")"), open)?;
									scoring = Some(Scoring::Bm {
										k1,
										b,
									})
								} else {
									scoring = Some(Default::default());
								};
							}
							t!("DOC_IDS_ORDER") => {
								self.pop_peek();
								doc_ids_order = self.next_token_value()?;
							}
							t!("DOC_LENGTHS_ORDER") => {
								self.pop_peek();
								doc_lengths_order = self.next_token_value()?;
							}
							t!("POSTINGS_ORDER") => {
								self.pop_peek();
								postings_order = self.next_token_value()?;
							}
							t!("TERMS_ORDER") => {
								self.pop_peek();
								terms_order = self.next_token_value()?;
							}
							t!("DOC_IDS_CACHE") => {
								self.pop_peek();
								doc_ids_cache = self.next_token_value()?;
							}
							t!("DOC_LENGTHS_CACHE") => {
								self.pop_peek();
								doc_lengths_cache = self.next_token_value()?;
							}
							t!("POSTINGS_CACHE") => {
								self.pop_peek();
								postings_cache = self.next_token_value()?;
							}
							t!("TERMS_CACHE") => {
								self.pop_peek();
								terms_cache = self.next_token_value()?;
							}
							t!("HIGHLIGHTS") => {
								self.pop_peek();
								hl = true;
							}
							_ => break,
						}
					}

					res.index = Index::Search(crate::sql::index::SearchParams {
						az: analyzer.unwrap_or_else(|| Ident::from("like")),
						sc: scoring.unwrap_or_else(Default::default),
						hl,
						doc_ids_order,
						doc_lengths_order,
						postings_order,
						terms_order,
						doc_ids_cache,
						doc_lengths_cache,
						postings_cache,
						terms_cache,
					});
				}
				t!("MTREE") => {
					self.pop_peek();
					expected!(self, t!("DIMENSION"));
					let dimension = self.next_token_value()?;
					let mut distance = Distance::Euclidean;
					let mut vector_type = VectorType::F64;
					let mut capacity = 40;
					let mut doc_ids_cache = 100;
					let mut doc_ids_order = 100;
					let mut mtree_cache = 100;
					loop {
						match self.peek_kind() {
							t!("DISTANCE") => {
								self.pop_peek();
								distance = self.parse_distance()?
							}
							t!("TYPE") => {
								self.pop_peek();
								vector_type = self.parse_vector_type()?
							}
							t!("CAPACITY") => {
								self.pop_peek();
								capacity = self.next_token_value()?
							}
							t!("DOC_IDS_CACHE") => {
								self.pop_peek();
								doc_ids_cache = self.next_token_value()?
							}
							t!("DOC_IDS_ORDER") => {
								self.pop_peek();
								doc_ids_order = self.next_token_value()?
							}
							t!("MTREE_CACHE") => {
								self.pop_peek();
								mtree_cache = self.next_token_value()?
							}
							_ => break,
						}
					}
					res.index = Index::MTree(crate::sql::index::MTreeParams::new(
						dimension,
						distance,
						vector_type,
						capacity,
						doc_ids_order,
						doc_ids_cache,
						mtree_cache,
					))
				}
				t!("HNSW") => {
					self.pop_peek();
					expected!(self, t!("DIMENSION"));
					let dimension = self.next_token_value()?;
					let mut distance = Distance::Euclidean;
					let mut vector_type = VectorType::F64;
					let mut m = None;
					let mut m0 = None;
					let mut ml = None;
					let mut ef_construction = 150;
					let mut extend_candidates = false;
					let mut keep_pruned_connections = false;
					loop {
						match self.peek_kind() {
							t!("DISTANCE") => {
								self.pop_peek();
								distance = self.parse_distance()?;
							}
							t!("TYPE") => {
								self.pop_peek();
								vector_type = self.parse_vector_type()?;
							}
							t!("LM") => {
								self.pop_peek();
								ml = Some(self.next_token_value()?);
							}
							t!("M0") => {
								self.pop_peek();
								m0 = Some(self.next_token_value()?);
							}
							t!("M") => {
								self.pop_peek();
								m = Some(self.next_token_value()?);
							}
							t!("EFC") => {
								self.pop_peek();
								ef_construction = self.next_token_value()?;
							}
							t!("EXTEND_CANDIDATES") => {
								self.pop_peek();
								extend_candidates = true;
							}
							t!("KEEP_PRUNED_CONNECTIONS") => {
								self.pop_peek();
								keep_pruned_connections = true;
							}
							_ => {
								break;
							}
						}
					}

					let m = m.unwrap_or(12);
					let m0 = m0.unwrap_or(m * 2);
					let ml = ml.unwrap_or(1.0 / (m as f64).ln()).into();
					res.index = Index::Hnsw(HnswParams::new(
						dimension,
						distance,
						vector_type,
						m,
						m0,
						ml,
						ef_construction,
						extend_candidates,
						keep_pruned_connections,
					));
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = Some(self.next_token_value()?);
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub fn parse_define_analyzer(&mut self) -> ParseResult<DefineAnalyzerStatement> {
		let if_not_exists = if self.eat(t!("IF")) {
			expected!(self, t!("NOT"));
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.next_token_value()?;
		let mut res = DefineAnalyzerStatement {
			name,

			function: None,
			tokenizers: None,
			filters: None,
			comment: None,

			if_not_exists,
		};
		loop {
			match self.peek_kind() {
				t!("FILTERS") => {
					self.pop_peek();
					let mut filters = Vec::new();
					loop {
						match self.next().kind {
							t!("ASCII") => {
								filters.push(Filter::Ascii);
							}
							t!("LOWERCASE") => {
								filters.push(Filter::Lowercase);
							}
							t!("UPPERCASE") => {
								filters.push(Filter::Uppercase);
							}
							t!("EDGENGRAM") => {
								let open_span = expected!(self, t!("(")).span;
								let a = self.next_token_value()?;
								expected!(self, t!(","));
								let b = self.next_token_value()?;
								self.expect_closing_delimiter(t!(")"), open_span)?;
								filters.push(Filter::EdgeNgram(a, b));
							}
							t!("NGRAM") => {
								let open_span = expected!(self, t!("(")).span;
								let a = self.next_token_value()?;
								expected!(self, t!(","));
								let b = self.next_token_value()?;
								self.expect_closing_delimiter(t!(")"), open_span)?;
								filters.push(Filter::Ngram(a, b));
							}
							t!("SNOWBALL") => {
								let open_span = expected!(self, t!("(")).span;
								let language = self.next_token_value()?;
								self.expect_closing_delimiter(t!(")"), open_span)?;
								filters.push(Filter::Snowball(language))
							}
							x => unexpected!(self, x, "a filter"),
						}
						if !self.eat(t!(",")) {
							break;
						}
					}
					res.filters = Some(filters);
				}
				t!("TOKENIZERS") => {
					self.pop_peek();
					let mut tokenizers = Vec::new();

					loop {
						let tokenizer = match self.next().kind {
							t!("BLANK") => Tokenizer::Blank,
							t!("CAMEL") => Tokenizer::Camel,
							t!("CLASS") => Tokenizer::Class,
							t!("PUNCT") => Tokenizer::Punct,
							x => unexpected!(self, x, "a tokenizer"),
						};
						tokenizers.push(tokenizer);
						if !self.eat(t!(",")) {
							break;
						}
					}
					res.tokenizers = Some(tokenizers);
				}

				t!("FUNCTION") => {
					self.pop_peek();
					expected!(self, t!("fn"));
					expected!(self, t!("::"));
					let mut ident = self.next_token_value::<Ident>()?;
					while self.eat(t!("::")) {
						let value = self.next_token_value::<Ident>()?;
						ident.0.push_str("::");
						ident.0.push_str(&value);
					}
					res.function = Some(ident);
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = Some(self.next_token_value()?);
				}
				_ => break,
			}
		}
		Ok(res)
	}

	pub fn parse_relation_schema(&mut self) -> ParseResult<table_type::Relation> {
		let mut res = table_type::Relation {
			from: None,
			to: None,
		};
		loop {
			match self.peek_kind() {
				t!("FROM") | t!("IN") => {
					self.pop_peek();
					let from = self.parse_tables()?;
					res.from = Some(from);
				}
				t!("TO") | t!("OUT") => {
					self.pop_peek();
					let to = self.parse_tables()?;
					res.to = Some(to);
				}
				_ => break,
			}
		}
		Ok(res)
	}

	pub fn parse_tables(&mut self) -> ParseResult<Kind> {
		let mut names = vec![self.next_token_value()?];
		while self.eat(t!("|")) {
			names.push(self.next_token_value()?);
		}
		Ok(Kind::Record(names))
	}

	pub fn parse_jwt(&mut self) -> ParseResult<access_type::JwtAccess> {
		let mut res = access_type::JwtAccess {
			// By default, a JWT access method is only used to verify.
			issue: None,
			..Default::default()
		};

		let mut iss = access_type::JwtAccessIssue {
			..Default::default()
		};

		match self.peek_kind() {
			t!("ALGORITHM") => {
				self.pop_peek();
				match self.next().kind {
					TokenKind::Algorithm(alg) => match self.next().kind {
						t!("KEY") => {
							let key = self.next_token_value::<Strand>()?.0;
							res.verify = access_type::JwtAccessVerify::Key(
								access_type::JwtAccessVerifyKey {
									alg,
									key: key.to_owned(),
								},
							);

							// Currently, issuer and verifier must use the same algorithm.
							iss.alg = alg;

							// If the algorithm is symmetric, the issuer and verifier keys are the same.
							// For asymmetric algorithms, the key needs to be explicitly defined.
							if alg.is_symmetric() {
								iss.key = key;
								// Since all the issuer data is known, it can already be assigned.
								// Cloning allows updating the original with any explicit issuer data.
								res.issue = Some(iss.clone());
							}
						}
						x => unexpected!(self, x, "a key"),
					},
					x => unexpected!(self, x, "a valid algorithm"),
				}
			}
			t!("URL") => {
				self.pop_peek();
				let url = self.next_token_value::<Strand>()?.0;
				res.verify = access_type::JwtAccessVerify::Jwks(access_type::JwtAccessVerifyJwks {
					url,
				});
			}
			x => unexpected!(self, x, "`ALGORITHM`, or `URL`"),
		}

		if self.eat(t!("WITH")) {
			expected!(self, t!("ISSUER"));
			loop {
				match self.peek_kind() {
					t!("ALGORITHM") => {
						self.pop_peek();
						match self.next().kind {
							TokenKind::Algorithm(alg) => {
								// If an algorithm is already defined, a different value is not expected.
								if let JwtAccessVerify::Key(ref ver) = res.verify {
									if alg != ver.alg {
										unexpected!(
											self,
											t!("ALGORITHM"),
											"a compatible algorithm or no algorithm"
										);
									}
								}
								iss.alg = alg;
							}
							x => unexpected!(self, x, "a valid algorithm"),
						}
					}
					t!("KEY") => {
						self.pop_peek();
						let key = self.next_token_value::<Strand>()?.0;
						// If the algorithm is symmetric and a key is already defined, a different key is not expected.
						if let JwtAccessVerify::Key(ref ver) = res.verify {
							if ver.alg.is_symmetric() && key != ver.key {
								unexpected!(self, t!("KEY"), "a symmetric key or no key");
							}
						}
						iss.key = key;
					}
					_ => break,
				}
			}
			res.issue = Some(iss);
		}

		Ok(res)
	}
}
