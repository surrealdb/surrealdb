use crate::{
	idx::ft::analyzer::Analyzers,
	sql::{
		filter::Filter,
		index::Distance,
		statements::{
			DefineAnalyzerStatement, DefineDatabaseStatement, DefineEventStatement,
			DefineFieldStatement, DefineFunctionStatement, DefineIndexStatement,
			DefineNamespaceStatement, DefineParamStatement, DefineScopeStatement, DefineStatement,
			DefineTableStatement, DefineTokenStatement, DefineUserStatement,
		},
		tokenizer::Tokenizer,
		Ident, Idioms, Index, Scoring, Values,
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
	pub fn parse_define_stmt(&mut self) -> ParseResult<DefineStatement> {
		expected!(self, "DEFINE");

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
		let name = self.parse_ident()?;
		let comment = self.eat(t!("COMMENT")).then(|| self.parse_strand()).transpose()?;
		Ok(DefineNamespaceStatement {
			id: None,
			name,
			comment,
		})
	}

	pub fn parse_define_database(&mut self) -> ParseResult<DefineDatabaseStatement> {
		let name = self.parse_ident()?;
		let mut res = DefineDatabaseStatement {
			id: None,
			name,
			comment: None,
			changefeed: None,
		};
		loop {
			match self.peek_kind() {
				t!("COMMENT") => {
					res.comment = Some(self.parse_strand()?);
				}
				t!("CHANGEFEED") => {
					res.changefeed = Some(self.parse_changefeed()?);
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub fn parse_define_function(&mut self) -> ParseResult<DefineFunctionStatement> {
		expected!(self, "fn");
		expected!(self, "::");
		let mut name = self.parse_ident()?;
		while self.eat(t!("::")) {
			let part = self.parse_ident()?;
			name.0.push(':');
			name.0.push(':');
			name.0.push_str(part.as_str());
		}
		let token = expected!(self, "(").span;
		let mut args = Vec::new();
		loop {
			if self.eat(t!(")")) {
				break;
			}

			let param = self.parse_param()?.0;
			expected!(self, ":");
			let delim = expected!(self, "<").span;
			let kind = self.parse_kind(delim)?;

			args.push((param, kind));

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!(")"), token)?;
			}
		}

		let next = expected!(self, "{").span;
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
					res.comment = Some(self.parse_strand()?);
				}
				t!("PERMISSIONS") => {
					res.permissions = self.parse_permission_value()?;
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub fn parse_define_user(&mut self) -> ParseResult<DefineUserStatement> {
		let name = self.parse_ident()?;
		expected!(self, "ON");
		let base = self.parse_base(false)?;

		let mut res = DefineUserStatement {
			name,
			base,
			..Default::default()
		};

		loop {
			match self.next().kind {
				t!("COMMENT") => {
					res.comment = Some(self.parse_strand()?);
				}
				t!("PASSWORD") => {
					res.code = self.parse_strand()?.0;
				}
				t!("PASSHASH") => {
					res.hash = self.parse_strand()?.0;
				}
				t!("ROLES") => {
					res.roles = vec![self.parse_ident()?];
					while self.eat(t!(",")) {
						res.roles.push(self.parse_ident()?);
					}
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub fn parse_define_token(&mut self) -> ParseResult<DefineTokenStatement> {
		let name = self.parse_ident()?;
		expected!(self, "ON");
		let base = self.parse_base(true)?;

		let mut res = DefineTokenStatement {
			name,
			base,
			..Default::default()
		};

		loop {
			match self.next().kind {
				t!("COMMENT") => {
					res.comment = Some(self.parse_strand()?);
				}
				t!("VALUE") => {
					res.code = self.parse_strand()?.0;
				}
				t!("TYPE") => match self.next().kind {
					TokenKind::Algorithm(x) => {
						res.kind = x;
					}
					x => unexpected!(self, x, "a token algorithm"),
				},
				_ => break,
			}
		}

		Ok(res)
	}

	pub fn parse_define_scope(&mut self) -> ParseResult<DefineScopeStatement> {
		let name = self.parse_ident()?;
		let mut res = DefineScopeStatement {
			name,
			..Default::default()
		};

		loop {
			match self.next().kind {
				t!("COMMENT") => {
					res.comment = Some(self.parse_strand()?);
				}
				t!("SESSION") => {
					res.session = Some(self.parse_duration()?);
				}
				t!("SIGNUP") => {
					res.signup = Some(self.parse_value()?);
				}
				t!("SIGNIN") => {
					res.signin = Some(self.parse_value()?);
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub fn parse_define_param(&mut self) -> ParseResult<DefineParamStatement> {
		let name = self.parse_ident()?;

		let mut res = DefineParamStatement {
			name,
			..Default::default()
		};

		loop {
			match self.next().kind {
				t!("VALUE") => {
					res.value = self.parse_value()?;
				}
				t!("COMMENT") => {
					res.comment = Some(self.parse_strand()?);
				}
				t!("PERMISSIONS") => {
					res.permissions = self.parse_permission_value()?;
				}
				_ => break,
			}
		}
		Ok(res)
	}

	pub fn parse_define_table(&mut self) -> ParseResult<DefineTableStatement> {
		let name = self.parse_ident()?;
		let mut res = DefineTableStatement {
			name,
			..Default::default()
		};

		loop {
			match self.peek_kind() {
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = Some(self.parse_strand()?);
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
					res.permissions = self.parse_permission()?;
				}
				t!("CHANGEFEED") => {
					self.pop_peek();
					res.changefeed = Some(self.parse_changefeed()?);
				}
				t!("(") => {
					let open = self.pop_peek().span;
					res.view = Some(self.parse_view()?);
					self.expect_closing_delimiter(t!(")"), open)?;
				}
				t!("SELECT") => {
					res.view = Some(self.parse_view()?);
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub fn parse_define_event(&mut self) -> ParseResult<DefineEventStatement> {
		let name = self.parse_ident()?;
		expected!(self, "ON");
		self.eat(t!("TABLE"));
		let what = self.parse_ident()?;

		let mut res = DefineEventStatement {
			name,
			what,
			..Default::default()
		};

		loop {
			match self.next().kind {
				t!("WHEN") => {
					res.when = self.parse_value()?;
				}
				t!("THEN") => {
					res.then = Values(vec![self.parse_value()?]);
					while self.eat(t!(",")) {
						res.then.0.push(self.parse_value()?)
					}
				}
				t!("COMMENT") => {
					res.comment = Some(self.parse_strand()?);
				}
				_ => break,
			}
		}
		Ok(res)
	}

	pub fn parse_define_field(&mut self) -> ParseResult<DefineFieldStatement> {
		let name = self.parse_local_idiom()?;
		expected!(self, "ON");
		self.eat(t!("TABLE"));
		let what = self.parse_ident()?;

		let mut res = DefineFieldStatement {
			name,
			what,
			..Default::default()
		};

		loop {
			match self.next().kind {
				// FLEX, FLEXI and FLEXIBLE are all the same token type.
				t!("FLEXIBLE") => {
					res.flex = true;
				}
				t!("TYPE") => {
					let delim = expected!(self, "<").span;
					res.kind = Some(self.parse_kind(delim)?);
				}
				t!("VALUE") => {
					res.value = Some(self.parse_value()?);
				}
				t!("ASSERT") => {
					res.assert = Some(self.parse_value()?);
				}
				t!("DEFAULT") => {
					res.default = Some(self.parse_value()?);
				}
				t!("COMMENT") => {
					res.comment = Some(self.parse_strand()?);
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub fn parse_define_index(&mut self) -> ParseResult<DefineIndexStatement> {
		let name = self.parse_ident()?;
		expected!(self, "ON");
		self.eat(t!("TABLE"));
		let what = self.parse_ident()?;

		let mut res = DefineIndexStatement {
			name,
			what,
			..Default::default()
		};

		loop {
			match self.next().kind {
				// COLUMS and FIELDS are the same tokenkind
				t!("FIELDS") => {
					res.cols = Idioms(vec![self.parse_local_idiom()?]);
					while self.eat(t!(",")) {
						res.cols.0.push(self.parse_local_idiom()?);
					}
				}
				t!("UNIQUE") => {
					res.index = Index::Uniq;
				}
				t!("SEARCH") => {
					let analyzer =
						self.eat(t!("ANALYZER")).then(|| self.parse_ident()).transpose()?;
					let scoring = match self.next().kind {
						t!("VS") => Scoring::Vs,
						t!("BM25") => {
							let k1 = self.parse_f32()?;
							let b = self.parse_f32()?;
							Scoring::Bm {
								k1,
								b,
							}
						}
						x => unexpected!(self, x, "`VS` or `BM25`"),
					};

					// TODO: Propose change in how order syntax works.
					let doc_ids_order = self
						.eat(t!("DOC_IDS_ORDER"))
						.then(|| self.parse_u32())
						.transpose()?
						.unwrap_or(100);
					let doc_lengths_order = self
						.eat(t!("DOC_LENGTHS_ORDER"))
						.then(|| self.parse_u32())
						.transpose()?
						.unwrap_or(100);
					let postings_order = self
						.eat(t!("POSTINGS_ORDER"))
						.then(|| self.parse_u32())
						.transpose()?
						.unwrap_or(100);
					let terms_order = self
						.eat(t!("TERMS_ORDER"))
						.then(|| self.parse_u32())
						.transpose()?
						.unwrap_or(100);

					let hl = self.eat(t!("HIGHLIGHTS"));

					res.index = Index::Search(crate::sql::index::SearchParams {
						az: analyzer.unwrap_or_else(|| Ident::from(Analyzers::LIKE)),
						sc: scoring,
						hl,
						doc_ids_order,
						doc_lengths_order,
						postings_order,
						terms_order,
					});
				}
				t!("MTREE") => {
					expected!(self, "DIMENSION");
					let dimension = self.parse_u16()?;
					let distance = self.try_parse_distance()?.unwrap_or(Distance::Euclidean);
					let capacity = self
						.eat(t!("CAPACITY"))
						.then(|| self.parse_u16())
						.transpose()?
						.unwrap_or(40);

					let doc_ids_order = self
						.eat(t!("DOC_IDS_ORDER"))
						.then(|| self.parse_u32())
						.transpose()?
						.unwrap_or(100);

					res.index = Index::MTree(crate::sql::index::MTreeParams {
						dimension,
						distance,
						capacity,
						doc_ids_order,
					})
				}
				t!("COMMENT") => {
					res.comment = Some(self.parse_strand()?);
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub fn parse_define_analyzer(&mut self) -> ParseResult<DefineAnalyzerStatement> {
		let name = self.parse_ident()?;
		let mut res = DefineAnalyzerStatement {
			name,
			tokenizers: None,
			filters: None,
			comment: None,
		};
		loop {
			match self.next().kind {
				t!("FILTERS") => {
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
								let open_span = expected!(self, "(").span;
								let a = self.parse_u16()?;
								expected!(self, ",");
								let b = self.parse_u16()?;
								self.expect_closing_delimiter(t!(")"), open_span)?;
								filters.push(Filter::EdgeNgram(a, b));
							}
							t!("NGRAM") => {
								let open_span = expected!(self, "(").span;
								let a = self.parse_u16()?;
								expected!(self, ",");
								let b = self.parse_u16()?;
								self.expect_closing_delimiter(t!(")"), open_span)?;
								filters.push(Filter::Ngram(a, b));
							}
							t!("SNOWBALL") => {
								let open_span = expected!(self, "(").span;
								let language = match self.next().kind {
									TokenKind::Language(x) => x,
									x => unexpected!(self, x, "a language"),
								};
								self.expect_closing_delimiter(t!(")"), open_span)?;
								filters.push(Filter::Snowball(language))
							}
							_ => break,
						}
					}
					res.filters = Some(filters);
				}
				t!("TOKENIZERS") => {
					let mut tokenizers = Vec::new();

					loop {
						let tokenizer = match self.next().kind {
							t!("BLANK") => Tokenizer::Blank,
							t!("CAMEL") => Tokenizer::Camel,
							t!("CLASS") => Tokenizer::Class,
							t!("PUNCT") => Tokenizer::Punct,
							_ => break,
						};
						tokenizers.push(tokenizer);
					}
					res.tokenizers = Some(tokenizers);
				}
				t!("COMMENT") => {
					res.comment = Some(self.parse_strand()?);
				}
				_ => break,
			}
		}
		Ok(res)
	}
}
