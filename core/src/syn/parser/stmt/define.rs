use reblessive::Stk;

use crate::{
	sql::{
		filter::Filter,
		index::{Distance, VectorType},
		statements::{
			DefineAnalyzerStatement, DefineDatabaseStatement, DefineEventStatement,
			DefineFieldStatement, DefineFunctionStatement, DefineIndexStatement,
			DefineNamespaceStatement, DefineParamStatement, DefineScopeStatement, DefineStatement,
			DefineTableStatement, DefineTokenStatement, DefineUserStatement,
		},
		table_type,
		tokenizer::Tokenizer,
		Ident, Idioms, Index, Kind, Param, Permissions, Scoring, Strand, TableType, Values,
	},
	syn::{
		parser::{
			mac::{expected, unexpected},
			ParseResult, Parser,
		},
		token::{t, TokenKind},
	},
};

impl Parser<'_> {
	pub async fn parse_define_stmt(&mut self, ctx: &mut Stk) -> ParseResult<DefineStatement> {
		match self.next().kind {
			t!("NAMESPACE") => self.parse_define_namespace().map(DefineStatement::Namespace),
			t!("DATABASE") => self.parse_define_database().map(DefineStatement::Database),
			t!("FUNCTION") => self.parse_define_function(ctx).await.map(DefineStatement::Function),
			t!("USER") => self.parse_define_user().map(DefineStatement::User),
			t!("TOKEN") => self.parse_define_token().map(DefineStatement::Token),
			t!("SCOPE") => self.parse_define_scope(ctx).await.map(DefineStatement::Scope),
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
				_ => break,
			}
		}

		Ok(res)
	}

	pub fn parse_define_token(&mut self) -> ParseResult<DefineTokenStatement> {
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

		let mut res = DefineTokenStatement {
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
				t!("VALUE") => {
					self.pop_peek();
					res.code = self.next_token_value::<Strand>()?.0;
				}
				t!("TYPE") => {
					self.pop_peek();
					match self.next().kind {
						TokenKind::Algorithm(x) => {
							res.kind = x;
						}
						x => unexpected!(self, x, "a token algorithm"),
					}
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub async fn parse_define_scope(&mut self, stk: &mut Stk) -> ParseResult<DefineScopeStatement> {
		let if_not_exists = if self.eat(t!("IF")) {
			expected!(self, t!("NOT"));
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.next_token_value()?;
		let mut res = DefineScopeStatement {
			name,
			code: DefineScopeStatement::random_code(),
			if_not_exists,
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
					res.session = Some(self.next_token_value()?);
				}
				t!("SIGNUP") => {
					self.pop_peek();
					res.signup = Some(stk.run(|stk| self.parse_value(stk)).await?);
				}
				t!("SIGNIN") => {
					self.pop_peek();
					res.signin = Some(stk.run(|stk| self.parse_value(stk)).await?);
				}
				_ => break,
			}
		}

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
				// COLUMS and FIELDS are the same tokenkind
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
					let analyzer =
						self.eat(t!("ANALYZER")).then(|| self.next_token_value()).transpose()?;
					let scoring = match self.next().kind {
						t!("VS") => Scoring::Vs,
						t!("BM25") => {
							if self.eat(t!("(")) {
								let open = self.last_span();
								let k1 = self.next_token_value()?;
								expected!(self, t!(","));
								let b = self.next_token_value()?;
								self.expect_closing_delimiter(t!(")"), open)?;
								Scoring::Bm {
									k1,
									b,
								}
							} else {
								Scoring::bm25()
							}
						}
						x => unexpected!(self, x, "`VS` or `BM25`"),
					};

					// TODO: Propose change in how order syntax works.
					let doc_ids_order = self
						.eat(t!("DOC_IDS_ORDER"))
						.then(|| self.next_token_value())
						.transpose()?
						.unwrap_or(100);
					let doc_lengths_order = self
						.eat(t!("DOC_LENGTHS_ORDER"))
						.then(|| self.next_token_value())
						.transpose()?
						.unwrap_or(100);
					let postings_order = self
						.eat(t!("POSTINGS_ORDER"))
						.then(|| self.next_token_value())
						.transpose()?
						.unwrap_or(100);
					let terms_order = self
						.eat(t!("TERMS_ORDER"))
						.then(|| self.next_token_value())
						.transpose()?
						.unwrap_or(100);
					let doc_ids_cache = self
						.eat(t!("DOC_IDS_CACHE"))
						.then(|| self.next_token_value())
						.transpose()?
						.unwrap_or(100);
					let doc_lengths_cache = self
						.eat(t!("DOC_LENGTHS_CACHE"))
						.then(|| self.next_token_value())
						.transpose()?
						.unwrap_or(100);
					let postings_cache = self
						.eat(t!("POSTINGS_CACHE"))
						.then(|| self.next_token_value())
						.transpose()?
						.unwrap_or(100);
					let terms_cache = self
						.eat(t!("TERMS_CACHE"))
						.then(|| self.next_token_value())
						.transpose()?
						.unwrap_or(100);

					let hl = self.eat(t!("HIGHLIGHTS"));

					res.index = Index::Search(crate::sql::index::SearchParams {
						az: analyzer.unwrap_or_else(|| Ident::from("like")),
						sc: scoring,
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
					let distance = self.try_parse_distance()?.unwrap_or(Distance::Euclidean);
					let capacity = self
						.eat(t!("CAPACITY"))
						.then(|| self.next_token_value())
						.transpose()?
						.unwrap_or(40);

					let doc_ids_order = self
						.eat(t!("DOC_IDS_ORDER"))
						.then(|| self.next_token_value())
						.transpose()?
						.unwrap_or(100);

					let doc_ids_cache = self
						.eat(t!("DOC_IDS_CACHE"))
						.then(|| self.next_token_value())
						.transpose()?
						.unwrap_or(100);

					let mtree_cache = self
						.eat(t!("MTREE_CACHE"))
						.then(|| self.next_token_value())
						.transpose()?
						.unwrap_or(100);

					res.index = Index::MTree(crate::sql::index::MTreeParams {
						dimension,
						_distance: Default::default(),
						distance,
						capacity,
						doc_ids_order,
						doc_ids_cache,
						mtree_cache,
						vector_type: VectorType::F64,
					})
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
}
