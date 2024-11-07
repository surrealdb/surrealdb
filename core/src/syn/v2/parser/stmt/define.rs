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
		tokenizer::Tokenizer,
		Ident, Idioms, Index, Param, Permissions, Scoring, Strand, Values,
	},
	syn::v2::{
		parser::{
			mac::{expected, unexpected},
			ParseResult, Parser,
		},
		token::{t, TokenKind},
	},
};

impl Parser<'_> {
	pub fn parse_define_stmt(&mut self) -> ParseResult<DefineStatement> {
		match self.next().kind {
			t!("NAMESPACE") => self.parse_define_namespace().map(DefineStatement::Namespace),
			t!("DATABASE") => self.parse_define_database().map(DefineStatement::Database),
			t!("FUNCTION") => self.parse_define_function().map(DefineStatement::Function),
			t!("USER") => self.parse_define_user().map(DefineStatement::User),
			t!("TOKEN") => self.parse_define_token().map(DefineStatement::Token),
			t!("SCOPE") => self.parse_define_scope().map(DefineStatement::Scope),
			t!("PARAM") => self.parse_define_param().map(DefineStatement::Param),
			t!("TABLE") => self.parse_define_table().map(DefineStatement::Table),
			t!("EVENT") => self.parse_define_event().map(DefineStatement::Event),
			t!("FIELD") => self.parse_define_field().map(DefineStatement::Field),
			t!("INDEX") => self.parse_define_index().map(DefineStatement::Index),
			t!("ANALYZER") => self.parse_define_analyzer().map(DefineStatement::Analyzer),
			x => unexpected!(self, x, "a define statement keyword"),
		}
	}

	pub fn parse_define_namespace(&mut self) -> ParseResult<DefineNamespaceStatement> {
		let name = self.next_token_value()?;
		let mut res = DefineNamespaceStatement {
			id: None,
			name,
			..Default::default()
		};

		while let t!("COMMENT") = self.peek_kind() {
			self.pop_peek();
			res.comment = Some(self.next_token_value()?);
		}

		Ok(res)
	}

	pub fn parse_define_database(&mut self) -> ParseResult<DefineDatabaseStatement> {
		let name = self.next_token_value()?;
		let mut res = DefineDatabaseStatement {
			name,
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

	pub fn parse_define_function(&mut self) -> ParseResult<DefineFunctionStatement> {
		let name = self.parse_custom_function_name()?;
		let token = expected!(self, t!("(")).span;
		let mut args = Vec::new();
		loop {
			if self.eat(t!(")")) {
				break;
			}

			let param = self.next_token_value::<Param>()?.0;
			expected!(self, t!(":"));
			let kind = self.parse_inner_kind()?;

			args.push((param, kind));

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!(")"), token)?;
				break;
			}
		}

		let next = expected!(self, t!("{")).span;
		let block = self.parse_block(next)?;

		let mut res = DefineFunctionStatement {
			name,
			args,
			block,
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
					res.permissions = self.parse_permission_value()?;
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub fn parse_define_user(&mut self) -> ParseResult<DefineUserStatement> {
		let name = self.next_token_value()?;
		expected!(self, t!("ON"));
		let base = self.parse_base(false)?;

		let mut res = DefineUserStatement::from_parsed_values(
			name,
			base,
			vec!["Viewer".into()], // New users get the viewer role by default
		);

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
		let name = self.next_token_value()?;
		expected!(self, t!("ON"));
		let base = self.parse_base(true)?;

		let mut res = DefineTokenStatement {
			name,
			base,
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

	pub fn parse_define_scope(&mut self) -> ParseResult<DefineScopeStatement> {
		let name = self.next_token_value()?;
		let mut res = DefineScopeStatement {
			name,
			code: DefineScopeStatement::random_code(),
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
					res.signup = Some(self.parse_value()?);
				}
				t!("SIGNIN") => {
					self.pop_peek();
					res.signin = Some(self.parse_value()?);
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub fn parse_define_param(&mut self) -> ParseResult<DefineParamStatement> {
		let name = self.next_token_value::<Param>()?.0;

		let mut res = DefineParamStatement {
			name,
			..Default::default()
		};

		loop {
			match self.peek_kind() {
				t!("VALUE") => {
					self.pop_peek();
					res.value = self.parse_value()?;
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = Some(self.next_token_value()?);
				}
				t!("PERMISSIONS") => {
					self.pop_peek();
					res.permissions = self.parse_permission_value()?;
				}
				_ => break,
			}
		}
		Ok(res)
	}

	pub fn parse_define_table(&mut self) -> ParseResult<DefineTableStatement> {
		let name = self.next_token_value()?;
		let mut res = DefineTableStatement {
			name,
			permissions: Permissions::none(),
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
					res.permissions = self.parse_permission(false)?;
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
							res.view = Some(self.parse_view()?);
							self.expect_closing_delimiter(t!(")"), open)?;
						}
						t!("SELECT") => {
							res.view = Some(self.parse_view()?);
						}
						x => unexpected!(self, x, "`SELECT`"),
					}
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub fn parse_define_event(&mut self) -> ParseResult<DefineEventStatement> {
		let name = self.next_token_value()?;
		expected!(self, t!("ON"));
		self.eat(t!("TABLE"));
		let what = self.next_token_value()?;

		let mut res = DefineEventStatement {
			name,
			what,
			..Default::default()
		};

		loop {
			match self.peek_kind() {
				t!("WHEN") => {
					self.pop_peek();
					res.when = self.parse_value()?;
				}
				t!("THEN") => {
					self.pop_peek();
					res.then = Values(vec![self.parse_value()?]);
					while self.eat(t!(",")) {
						res.then.0.push(self.parse_value()?)
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

	pub fn parse_define_field(&mut self) -> ParseResult<DefineFieldStatement> {
		let name = self.parse_local_idiom()?;
		expected!(self, t!("ON"));
		self.eat(t!("TABLE"));
		let what = self.next_token_value()?;

		let mut res = DefineFieldStatement {
			name,
			what,
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
					res.kind = Some(self.parse_inner_kind()?);
				}
				t!("VALUE") => {
					self.pop_peek();
					res.value = Some(self.parse_value()?);
				}
				t!("ASSERT") => {
					self.pop_peek();
					res.assert = Some(self.parse_value()?);
				}
				t!("DEFAULT") => {
					self.pop_peek();
					res.default = Some(self.parse_value()?);
				}
				t!("PERMISSIONS") => {
					self.pop_peek();
					res.permissions = self.parse_permission(true)?;
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
		let name = self.next_token_value()?;
		expected!(self, t!("ON"));
		self.eat(t!("TABLE"));
		let what = self.next_token_value()?;

		let mut res = DefineIndexStatement {
			name,
			what,
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
		let name = self.next_token_value()?;
		let mut res = DefineAnalyzerStatement {
			name,
			tokenizers: None,
			filters: None,
			comment: None,
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
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = Some(self.next_token_value()?);
				}
				_ => break,
			}
		}
		Ok(res)
	}
}
