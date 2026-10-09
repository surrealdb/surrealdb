use surrealdb_sql::filter::Filter;
use surrealdb_sql::kind::kind_contains_object;
use surrealdb_sql::statements::alter::field::AlterDefault;
use surrealdb_sql::statements::alter::{
	AlterAccessStatement, AlterAnalyzerStatement, AlterApiClause, AlterApiStatement,
	AlterBucketStatement, AlterConfigStatement, AlterDatabaseStatement, AlterEventStatement,
	AlterFieldStatement, AlterFunctionStatement, AlterIndexStatement, AlterKind,
	AlterModuleStatement, AlterNamespaceStatement, AlterParamStatement, AlterSequenceStatement,
	AlterSystemStatement, AlterUserStatement,
};
use surrealdb_sql::statements::define::ApiAction;
use surrealdb_sql::statements::{AlterStatement, AlterTableStatement};
use surrealdb_sql::{ApiMethod, EventKind, TableType};

use crate::error::bail;
use crate::parser::mac::{expected, expected_whitespace, unexpected};
use crate::parser::{ParseResult, Parser};
use crate::token::{Token, TokenKind, t};

impl Parser<'_> {
	pub fn parse_alter_stmt(&mut self) -> ParseResult<AlterStatement> {
		let next = self.next();
		match next.kind {
			t!("SYSTEM") => self.parse_alter_system().map(AlterStatement::System),
			t!("NAMESPACE") => self.parse_alter_namespace().map(AlterStatement::Namespace),
			t!("DATABASE") => self.parse_alter_database().map(AlterStatement::Database),
			t!("TABLE") | t!("TB") => self.parse_alter_table().map(AlterStatement::Table),
			t!("EVENT") => self.parse_alter_event().map(AlterStatement::Event),
			t!("INDEX") => self.parse_alter_index().map(AlterStatement::Index),
			t!("FIELD") => self.parse_alter_field().map(|s| AlterStatement::Field(Box::new(s))),
			t!("PARAM") => self.parse_alter_param().map(AlterStatement::Param),
			t!("SEQUENCE") => self.parse_alter_sequence().map(AlterStatement::Sequence),
			t!("BUCKET") => self.parse_alter_bucket(next).map(AlterStatement::Bucket),
			t!("ANALYZER") => self.parse_alter_analyzer().map(AlterStatement::Analyzer),
			t!("FUNCTION") => self.parse_alter_function().map(AlterStatement::Function),
			t!("USER") => self.parse_alter_user().map(AlterStatement::User),
			t!("ACCESS") => self.parse_alter_access().map(AlterStatement::Access),
			t!("CONFIG") => self.parse_alter_config().map(AlterStatement::Config),
			t!("API") => self.parse_alter_api().map(AlterStatement::Api),
			t!("MODULE") => self.parse_alter_module().map(AlterStatement::Module),
			_ => unexpected!(self, next, "a alter statement keyword"),
		}
	}

	pub fn parse_alter_system(&mut self) -> ParseResult<AlterSystemStatement> {
		let mut res = AlterSystemStatement::default();

		loop {
			match self.peek_kind() {
				t!("DROP") => {
					self.pop_peek();
					let peek = self.peek();
					match peek.kind {
						TokenKind::Identifier => {
							let name = self.parse_ident()?.into_string();
							match name.as_str() {
								"QUERY_TIMEOUT" => {
									res.query_timeout = AlterKind::Drop;
								}
								_ => unexpected!(self, peek, "`QUERY_TIMEOUT`"),
							}
						}
						_ => {
							unexpected!(self, peek, "`QUERY_TIMEOUT`")
						}
					}
				}
				t!("COMPACT") => {
					self.pop_peek();
					res.compact = true;
				}
				TokenKind::Identifier => {
					let peek = self.peek();
					let name = self.parse_ident()?.into_string();
					match name.as_str() {
						"QUERY_TIMEOUT" => {
							let duration = self.parse_expr_field()?;
							res.query_timeout = AlterKind::Set(duration);
						}
						_ => unexpected!(self, peek, "`QUERY_TIMEOUT`"),
					}
				}
				_ => break,
			}
		}

		if !res.compact && matches!(res.query_timeout, AlterKind::None) {
			unexpected!(self, self.peek(), "`COMPACT`, `DROP` or `QUERY_TIMEOUT`")
		}
		Ok(res)
	}

	pub fn parse_alter_namespace(&mut self) -> ParseResult<AlterNamespaceStatement> {
		if !self.eat(t!("COMPACT")) {
			unexpected!(self, self.peek(), "`COMPACT`")
		}
		Ok(AlterNamespaceStatement {
			compact: true,
		})
	}

	pub fn parse_alter_database(&mut self) -> ParseResult<AlterDatabaseStatement> {
		if !self.eat(t!("COMPACT")) {
			unexpected!(self, self.peek(), "`COMPACT`")
		}
		Ok(AlterDatabaseStatement {
			compact: true,
		})
	}

	pub fn parse_alter_table(&mut self) -> ParseResult<AlterTableStatement> {
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.parse_expr_table()?;
		let mut res = AlterTableStatement {
			name,
			if_exists,
			..Default::default()
		};

		loop {
			match self.peek_kind() {
				t!("DROP") => {
					self.pop_peek();
					let peek = self.peek();
					match peek.kind {
						t!("COMMENT") => {
							self.pop_peek();
							res.comment = AlterKind::Drop;
						}
						t!("CHANGEFEED") => {
							self.pop_peek();
							res.changefeed = AlterKind::Drop;
						}
						t!("INLINE") => {
							self.pop_peek();
							let peek = self.peek();
							match peek.kind {
								t!("EDGES") => {
									self.pop_peek();
									res.inline_edges_cap = AlterKind::Drop;
								}
								t!("REFERENCES") => {
									self.pop_peek();
									res.inline_refs_cap = AlterKind::Drop;
								}
								_ => unexpected!(self, peek, "`EDGES` or `REFERENCES`"),
							}
						}
						_ => {
							unexpected!(self, peek, "`COMMENT`, `CHANGEFEED` or `INLINE`")
						}
					}
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = AlterKind::Set(self.parse_string_lit()?);
				}
				t!("COMPACT") => {
					self.pop_peek();
					res.compact = true;
				}
				t!("INLINE") => {
					self.pop_peek();
					let peek = self.peek();
					match peek.kind {
						t!("EDGES") => {
							self.pop_peek();
							res.inline_edges_cap = AlterKind::Set(self.next_token_value()?);
						}
						t!("REFERENCES") => {
							self.pop_peek();
							res.inline_refs_cap = AlterKind::Set(self.next_token_value()?);
						}
						_ => unexpected!(self, peek, "`EDGES` or `REFERENCES`"),
					}
				}
				t!("TYPE") => {
					self.pop_peek();
					let peek = self.peek();
					match peek.kind {
						t!("NORMAL") => {
							self.pop_peek();
							res.kind = Some(TableType::Normal);
						}
						t!("RELATION") => {
							self.pop_peek();
							res.kind = Some(TableType::Relation(self.parse_relation_schema()?));
						}
						t!("ANY") => {
							self.pop_peek();
							res.kind = Some(TableType::Any);
						}
						_ => unexpected!(self, peek, "`NORMAL`, `RELATION`, or `ANY`"),
					}
				}
				t!("SCHEMALESS") => {
					self.pop_peek();
					res.schemafull = AlterKind::Drop;
				}
				t!("SCHEMAFULL") => {
					self.pop_peek();
					res.schemafull = AlterKind::Set(());
				}
				t!("PERMISSIONS") => {
					self.pop_peek();
					res.permissions = Some(self.parse_permission(false)?);
				}
				t!("CHANGEFEED") => {
					self.pop_peek();
					res.changefeed = AlterKind::Set(self.parse_changefeed()?)
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub fn parse_alter_event(&mut self) -> ParseResult<AlterEventStatement> {
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.parse_expr_field()?;
		expected!(self, t!("ON"));
		self.eat(t!("TABLE"));
		let what = self.parse_expr_table()?;
		let mut res = AlterEventStatement {
			name,
			what,
			if_exists,
			..Default::default()
		};

		loop {
			match self.peek_kind() {
				t!("DROP") => {
					self.pop_peek();
					let peek = self.peek();
					match peek.kind {
						t!("WHEN") => {
							self.pop_peek();
							res.when = AlterKind::Drop;
						}
						t!("THEN") => {
							self.pop_peek();
							res.then = AlterKind::Drop;
						}
						t!("COMMENT") => {
							self.pop_peek();
							res.comment = AlterKind::Drop;
						}
						t!("ASYNC") => {
							self.pop_peek();
							res.kind = AlterKind::Drop;
						}
						_ => {
							unexpected!(self, peek, "`WHEN`, `THEN`, `COMMENT`, or `ASYNC`")
						}
					}
				}
				t!("WHEN") => {
					self.pop_peek();
					res.when = AlterKind::Set(self.parse_expr_field()?);
				}
				t!("THEN") => {
					self.pop_peek();
					let mut then = vec![self.parse_expr_field()?];
					while self.eat(t!(",")) {
						then.push(self.parse_expr_field()?);
					}
					res.then = AlterKind::Set(then);
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = AlterKind::Set(self.parse_string_lit()?);
				}
				t!("ASYNC") => {
					self.pop_peek();
					res.kind = AlterKind::Set(EventKind::Async {
						retry: EventKind::DEFAULT_RETRY,
						max_depth: EventKind::DEFAULT_MAX_DEPTH,
					});
				}
				t!("RETRY") => {
					let token = self.pop_peek();
					if let AlterKind::Set(EventKind::Async {
						ref mut retry,
						..
					}) = res.kind
					{
						*retry = self.next_token_value()?;
					} else {
						bail!("Unexpected token `RETRY`", @token.span => "RETRY must be set after ASYNC");
					}
				}
				t!("MAXDEPTH") => {
					let token = self.pop_peek();
					if let AlterKind::Set(EventKind::Async {
						ref mut max_depth,
						..
					}) = res.kind
					{
						*max_depth = self.next_token_value()?;
					} else {
						bail!("Unexpected token `MAXDEPTH`", @token.span => "MAXDEPTH must be set after ASYNC");
					}
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub fn parse_alter_index(&mut self) -> ParseResult<AlterIndexStatement> {
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.parse_expr_field()?;
		expected!(self, t!("ON"));
		self.eat(t!("TABLE"));
		let table = self.parse_expr_table()?;

		let mut res = AlterIndexStatement {
			name,
			table,
			if_exists,
			..Default::default()
		};

		loop {
			match self.peek_kind() {
				t!("DROP") => {
					self.pop_peek();
					let peek = self.peek();
					match peek.kind {
						t!("COMMENT") => {
							self.pop_peek();
							res.comment = AlterKind::Drop;
						}
						_ => {
							unexpected!(self, peek, "`COMMENT`")
						}
					}
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = AlterKind::Set(self.parse_string_lit()?);
				}
				t!("PREPARE") => {
					self.pop_peek();
					self.eat(t!("REMOVE"));
					res.prepare_remove = true;
				}
				_ => break,
			}
		}

		if !res.prepare_remove && matches!(res.comment, AlterKind::None) {
			unexpected!(self, self.peek(), "`PREPARE`, `DROP` or `COMMENT`")
		}
		Ok(res)
	}

	pub fn parse_alter_field(&mut self) -> ParseResult<AlterFieldStatement> {
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.parse_expr_field()?;
		expected!(self, t!("ON"));
		self.eat(t!("TABLE"));
		let what = self.parse_expr_table()?;
		let mut res = AlterFieldStatement {
			name,
			what,
			if_exists,
			..Default::default()
		};

		loop {
			match self.peek_kind() {
				t!("DROP") => {
					self.pop_peek();
					let peek = self.peek();
					match peek.kind {
						t!("TYPE") => {
							self.pop_peek();
							res.kind = AlterKind::Drop;
						}
						t!("FLEXIBLE") => {
							self.pop_peek();
							res.flexible = AlterKind::Drop;
						}
						t!("READONLY") => {
							self.pop_peek();
							res.readonly = AlterKind::Drop;
						}
						t!("INLINE") => {
							self.pop_peek();
							res.inline = AlterKind::Drop;
						}
						t!("VALUE") => {
							self.pop_peek();
							res.value = AlterKind::Drop;
						}
						t!("ASSERT") => {
							self.pop_peek();
							res.assert = AlterKind::Drop;
						}
						t!("DEFAULT") => {
							self.pop_peek();
							res.default = AlterDefault::Drop;
						}
						t!("COMMENT") => {
							self.pop_peek();
							res.comment = AlterKind::Drop;
						}
						t!("REFERENCE") => {
							self.pop_peek();
							res.reference = AlterKind::Drop;
						}
						_ => {
							unexpected!(
								self,
								peek,
								"`FLEXIBLE`, `TYPE`, `READONLY`, `INLINE`, `VALUE`, `ASSERT`, `DEFAULT`, `COMMENT`, or `REFERENCE`"
							)
						}
					}
				}
				t!("TYPE") => {
					self.pop_peek();
					res.kind = AlterKind::Set(self.parse_inner_kind()?);
					if self.eat(t!("FLEXIBLE")) {
						if !matches!(&res.kind, AlterKind::Set(k) if kind_contains_object(k)) {
							bail!(
								"FLEXIBLE can only be used with types containing object",
								@self.last_span
							);
						}
						res.flexible = AlterKind::Set(());
					}
				}
				t!("FLEXIBLE") => {
					self.pop_peek();
					res.flexible = AlterKind::Set(());
				}
				t!("READONLY") => {
					self.pop_peek();
					res.readonly = AlterKind::Set(());
				}
				t!("INLINE") => {
					self.pop_peek();
					res.inline = AlterKind::Set(());
				}
				t!("VALUE") => {
					self.pop_peek();
					res.value = AlterKind::Set(self.parse_expr_field()?);
				}
				t!("ASSERT") => {
					self.pop_peek();
					res.assert = AlterKind::Set(self.parse_expr_field()?);
				}
				t!("DEFAULT") => {
					self.pop_peek();
					if self.eat(t!("ALWAYS")) {
						res.default = AlterDefault::Always(self.parse_expr_field()?);
					} else {
						res.default = AlterDefault::Set(self.parse_expr_field()?);
					}
				}
				t!("PERMISSIONS") => {
					self.pop_peek();
					res.permissions = Some(self.parse_permission(false)?);
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = AlterKind::Set(self.parse_string_lit()?);
				}
				t!("REFERENCE") => {
					self.pop_peek();
					res.reference = AlterKind::Set(self.parse_reference()?);
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub fn parse_alter_param(&mut self) -> ParseResult<AlterParamStatement> {
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.next_token_value::<surrealdb_sql::Param>()?.into_strand();
		let mut res = AlterParamStatement {
			name,
			if_exists,
			..Default::default()
		};

		loop {
			match self.peek_kind() {
				t!("DROP") => {
					self.pop_peek();
					let peek = self.peek();
					match peek.kind {
						t!("COMMENT") => {
							self.pop_peek();
							res.comment = AlterKind::Drop;
						}
						_ => unexpected!(self, peek, "`COMMENT`"),
					}
				}
				t!("VALUE") => {
					self.pop_peek();
					res.value = Some(self.parse_expr_field()?);
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = AlterKind::Set(self.parse_string_lit()?);
				}
				t!("PERMISSIONS") => {
					self.pop_peek();
					res.permissions = Some(self.parse_permission_value()?);
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub fn parse_alter_bucket(&mut self, token: Token) -> ParseResult<AlterBucketStatement> {
		if !self.settings.files_enabled {
			unexpected!(self, token, "the experimental files feature to be enabled");
		}

		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.parse_expr_field()?;
		let mut res = AlterBucketStatement {
			name,
			if_exists,
			..Default::default()
		};

		loop {
			match self.peek_kind() {
				t!("DROP") => {
					self.pop_peek();
					let peek = self.peek();
					match peek.kind {
						t!("READONLY") => {
							self.pop_peek();
							res.readonly = AlterKind::Drop;
						}
						t!("BACKEND") => {
							self.pop_peek();
							res.backend = AlterKind::Drop;
						}
						t!("COMMENT") => {
							self.pop_peek();
							res.comment = AlterKind::Drop;
						}
						_ => {
							unexpected!(self, peek, "`READONLY`, `BACKEND`, or `COMMENT`")
						}
					}
				}
				t!("READONLY") => {
					self.pop_peek();
					res.readonly = AlterKind::Set(());
				}
				t!("BACKEND") => {
					self.pop_peek();
					res.backend = AlterKind::Set(self.parse_string_lit()?);
				}
				t!("PERMISSIONS") => {
					self.pop_peek();
					res.permissions = Some(self.parse_permission_value()?);
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = AlterKind::Set(self.parse_string_lit()?);
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub fn parse_alter_analyzer(&mut self) -> ParseResult<AlterAnalyzerStatement> {
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.parse_expr_field()?;
		let mut res = AlterAnalyzerStatement {
			name,
			if_exists,
			..Default::default()
		};

		loop {
			match self.peek_kind() {
				t!("DROP") => {
					self.pop_peek();
					let peek = self.peek();
					match peek.kind {
						t!("FUNCTION") => {
							self.pop_peek();
							res.function = AlterKind::Drop;
						}
						t!("TOKENIZERS") => {
							self.pop_peek();
							res.tokenizers = AlterKind::Drop;
						}
						t!("FILTERS") => {
							self.pop_peek();
							res.filters = AlterKind::Drop;
						}
						t!("COMMENT") => {
							self.pop_peek();
							res.comment = AlterKind::Drop;
						}
						_ => {
							unexpected!(
								self,
								peek,
								"`FUNCTION`, `TOKENIZERS`, `FILTERS`, or `COMMENT`"
							)
						}
					}
				}
				t!("FUNCTION") => {
					self.pop_peek();
					res.function = AlterKind::Set(self.parse_analyzer_function_name()?.into());
				}
				t!("TOKENIZERS") => {
					self.pop_peek();
					res.tokenizers = AlterKind::Set(self.parse_tokenizers()?);
				}
				t!("FILTERS") => {
					self.pop_peek();
					let mut filters = Vec::new();
					loop {
						let next = self.next();
						match next.kind {
							t!("ASCII") => filters.push(Filter::Ascii),
							t!("LOWERCASE") => filters.push(Filter::Lowercase),
							t!("UPPERCASE") => filters.push(Filter::Uppercase),
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
								filters.push(Filter::Snowball(language));
							}
							t!("MAPPER") => {
								let open_span = expected!(self, t!("(")).span;
								let path: String = self.parse_string_lit()?;
								self.expect_closing_delimiter(t!(")"), open_span)?;
								filters.push(Filter::Mapper(path));
							}
							_ => unexpected!(self, next, "a filter"),
						}
						if !self.eat(t!(",")) {
							break;
						}
					}
					res.filters = AlterKind::Set(filters);
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = AlterKind::Set(self.parse_string_lit()?);
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub fn parse_alter_function(&mut self) -> ParseResult<AlterFunctionStatement> {
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.parse_custom_function_name()?;
		let mut res = AlterFunctionStatement {
			name,
			if_exists,
			..Default::default()
		};

		loop {
			match self.peek_kind() {
				t!("DROP") => {
					self.pop_peek();
					let peek = self.peek();
					match peek.kind {
						t!("COMMENT") => {
							self.pop_peek();
							res.comment = AlterKind::Drop;
						}
						_ => {
							unexpected!(self, peek, "`COMMENT`")
						}
					}
				}
				t!("(") => {
					self.pop_peek();
					let mut args = vec![];
					if !self.eat(t!(")")) {
						loop {
							let param =
								self.next_token_value::<surrealdb_sql::Param>()?.into_string();
							expected!(self, t!(":"));
							let kind = self.parse_inner_kind()?;
							args.push((param, kind));
							if !self.eat(t!(",")) {
								break;
							}
						}
						expected!(self, t!(")"));
					}
					res.args = AlterKind::Set(args);

					if self.eat(t!("->")) {
						res.returns = AlterKind::Set(self.parse_inner_kind()?);
					}

					let span = expected!(self, t!("{")).span;
					res.block = AlterKind::Set(self.parse_block(span)?);
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = AlterKind::Set(self.parse_string_lit()?);
				}
				t!("PERMISSIONS") => {
					self.pop_peek();
					res.permissions = Some(self.parse_permission_value()?);
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub fn parse_alter_user(&mut self) -> ParseResult<AlterUserStatement> {
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.parse_expr_field()?;
		expected!(self, t!("ON"));
		let base = self.parse_base()?;
		let mut res = AlterUserStatement {
			name,
			base,
			if_exists,
			..Default::default()
		};

		loop {
			match self.peek_kind() {
				t!("DROP") => {
					self.pop_peek();
					let peek = self.peek();
					match peek.kind {
						t!("COMMENT") => {
							self.pop_peek();
							res.comment = AlterKind::Drop;
						}
						_ => unexpected!(self, peek, "`COMMENT`"),
					}
				}
				t!("PASSWORD") => {
					self.pop_peek();
					res.pass_type =
						Some(surrealdb_sql::statements::define::user::PassType::Password(
							self.parse_string_lit()?,
						));
				}
				t!("PASSHASH") => {
					self.pop_peek();
					res.pass_type = Some(surrealdb_sql::statements::define::user::PassType::Hash(
						self.parse_string_lit()?,
					));
				}
				t!("PASSSCRAM") => {
					let token = self.pop_peek();
					let verifier = self.parse_string_lit()?;
					// Validate here so downstream conversions stay infallible.
					if let Err(e) = surrealdb_iam::ScramCredential::from_verifier_string(&verifier)
					{
						bail!("Invalid SCRAM verifier: {e}", @token.span => "Expected a valid `SCRAM-SHA-256$...` verifier string");
					}
					res.scram = Some(verifier);
				}
				t!("ROLES") => {
					self.pop_peek();
					let mut roles = vec![self.parse_ident()?.into_string()];
					while self.eat(t!(",")) {
						roles.push(self.parse_ident()?.into_string());
					}
					res.roles = AlterKind::Set(roles);
				}
				t!("DURATION") => {
					self.pop_peek();
					loop {
						expected!(self, t!("FOR"));
						let peek = self.peek();
						match peek.kind {
							t!("TOKEN") => {
								self.pop_peek();
								if self.eat(t!("NONE")) {
									res.token_duration = AlterKind::Drop;
								} else {
									res.token_duration = AlterKind::Set(self.next_token_value()?);
								}
							}
							t!("SESSION") => {
								self.pop_peek();
								if self.eat(t!("NONE")) {
									res.session_duration = AlterKind::Drop;
								} else {
									res.session_duration = AlterKind::Set(self.next_token_value()?);
								}
							}
							_ => unexpected!(self, peek, "`TOKEN` or `SESSION`"),
						}
						if !self.eat(t!(",")) {
							break;
						}
					}
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = AlterKind::Set(self.parse_string_lit()?);
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub fn parse_alter_access(&mut self) -> ParseResult<AlterAccessStatement> {
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.parse_expr_field()?;
		expected!(self, t!("ON"));
		let base = self.parse_base()?;
		let mut res = AlterAccessStatement {
			name,
			base,
			if_exists,
			..Default::default()
		};

		loop {
			match self.peek_kind() {
				t!("DROP") => {
					self.pop_peek();
					let peek = self.peek();
					match peek.kind {
						t!("AUTHENTICATE") => {
							self.pop_peek();
							res.authenticate = AlterKind::Drop;
						}
						t!("CONTEXT") => {
							self.pop_peek();
							res.context = AlterKind::Drop;
						}
						t!("COMMENT") => {
							self.pop_peek();
							res.comment = AlterKind::Drop;
						}
						_ => unexpected!(self, peek, "`AUTHENTICATE`, `CONTEXT` or `COMMENT`"),
					}
				}
				t!("AUTHENTICATE") => {
					self.pop_peek();
					res.authenticate = AlterKind::Set(self.parse_expr_field()?);
				}
				t!("CONTEXT") => {
					self.pop_peek();
					res.context = AlterKind::Set(self.parse_expr_field()?);
				}
				t!("DURATION") => {
					self.pop_peek();
					loop {
						expected!(self, t!("FOR"));
						let peek = self.peek();
						match peek.kind {
							t!("GRANT") => {
								self.pop_peek();
								if self.eat(t!("NONE")) {
									res.grant_duration = AlterKind::Drop;
								} else {
									res.grant_duration = AlterKind::Set(self.next_token_value()?);
								}
							}
							t!("TOKEN") => {
								self.pop_peek();
								if self.eat(t!("NONE")) {
									res.token_duration = AlterKind::Drop;
								} else {
									res.token_duration = AlterKind::Set(self.next_token_value()?);
								}
							}
							t!("SESSION") => {
								self.pop_peek();
								if self.eat(t!("NONE")) {
									res.session_duration = AlterKind::Drop;
								} else {
									res.session_duration = AlterKind::Set(self.next_token_value()?);
								}
							}
							_ => unexpected!(self, peek, "`GRANT`, `TOKEN`, or `SESSION`"),
						}
						if !self.eat(t!(",")) {
							break;
						}
					}
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = AlterKind::Set(self.parse_string_lit()?);
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub fn parse_alter_config(&mut self) -> ParseResult<AlterConfigStatement> {
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};

		let next = self.next();
		let inner = match next.kind {
			t!("API") => self
				.parse_api_config()
				.map(surrealdb_sql::statements::define::config::ConfigInner::Api)?,
			t!("GRAPHQL") => self
				.parse_graphql_config()
				.map(surrealdb_sql::statements::define::config::ConfigInner::GraphQL)?,
			t!("DEFAULT") => self
				.parse_default_config()
				.map(surrealdb_sql::statements::define::config::ConfigInner::Default)?,
			_ => unexpected!(self, next, "a type of config"),
		};

		let mut res = AlterConfigStatement {
			if_exists,
			inner,
			..Default::default()
		};

		if self.eat(t!("COMMENT")) {
			res.comment = AlterKind::Set(self.parse_string_lit()?);
		} else if self.eat(t!("DROP")) {
			expected!(self, t!("COMMENT"));
			res.comment = AlterKind::Drop;
		}

		Ok(res)
	}

	pub fn parse_alter_api(&mut self) -> ParseResult<AlterApiStatement> {
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let path = self.parse_expr_field()?;
		let mut res = AlterApiStatement {
			path,
			if_exists,
			..Default::default()
		};

		loop {
			match self.peek_kind() {
				t!("FOR") => {
					self.pop_peek();
					let peek = self.peek();
					match peek.kind {
						t!("ANY") => {
							self.pop_peek();
							let config = self.parse_api_config()?;
							let config_is_default = config == Default::default();

							let fallback = if self.eat(t!("THEN")) {
								AlterKind::Set(self.parse_expr_field()?)
							} else if self.eat(t!("DROP")) {
								expected!(self, t!("THEN"));
								AlterKind::Drop
							} else {
								AlterKind::None
							};

							res.clauses.push(AlterApiClause::ForAny {
								config: if config_is_default {
									None
								} else {
									Some(config)
								},
								fallback,
							});
						}
						t!("DELETE")
						| t!("GET")
						| t!("PATCH")
						| t!("POST")
						| t!("PUT")
						| t!("TRACE") => {
							let mut methods: Vec<ApiMethod> = vec![];
							loop {
								let method = match self.peek().kind {
									t!("DELETE") => ApiMethod::Delete,
									t!("GET") => ApiMethod::Get,
									t!("PATCH") => ApiMethod::Patch,
									t!("POST") => ApiMethod::Post,
									t!("PUT") => ApiMethod::Put,
									t!("TRACE") => ApiMethod::Trace,
									_ => {
										unexpected!(
											self,
											peek,
											"one of `DELETE`, `GET`, `PATCH`, `POST`, `PUT` or `TRACE`"
										)
									}
								};
								self.pop_peek();
								methods.push(method);
								if !self.eat(t!(",")) {
									break;
								}
							}

							if self.eat(t!("DROP")) {
								expected!(self, t!("THEN"));
								res.clauses.push(AlterApiClause::DropAction {
									methods,
								});
							} else {
								let config = self.parse_api_config()?;
								expected!(self, t!("THEN"));
								let action = self.parse_expr_field()?;
								res.clauses.push(AlterApiClause::SetAction(ApiAction {
									methods,
									action,
									config,
								}));
							}
						}
						_ => {
							unexpected!(
								self,
								peek,
								"`any`, `DELETE`, `GET`, `PATCH`, `POST`, `PUT` or `TRACE`"
							)
						}
					}
				}
				t!("DROP") => {
					self.pop_peek();
					expected!(self, t!("COMMENT"));
					res.comment = AlterKind::Drop;
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = AlterKind::Set(self.parse_string_lit()?);
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub fn parse_alter_module(&mut self) -> ParseResult<AlterModuleStatement> {
		if !self.settings.surrealism_enabled {
			bail!(
				"Surrealism modules are not enabled",
				@self.last_span() => "Use of `ALTER MODULE` requires Surrealism modules"
			)
		}

		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};

		let peek = self.peek();
		let name = match peek.kind {
			t!("mod") => {
				self.pop_peek();
				expected_whitespace!(self, t!("::"));
				let name = self.parse_ident()?.into_string();
				surrealdb_sql::ModuleName::Module(name)
			}
			t!("silo") => {
				self.pop_peek();
				expected!(self, t!("::"));
				let organisation = self.parse_ident()?.into_string();
				expected!(self, t!("::"));
				let package = self.parse_ident()?.into_string();
				expected!(self, t!("::"));
				expected_whitespace!(self, t!("<"));
				let major = self.parse_version_digits()?;
				expected_whitespace!(self, t!("."));
				let minor = self.parse_version_digits()?;
				expected_whitespace!(self, t!("."));
				let patch = self.parse_version_digits()?;
				expected!(self, t!(">"));
				surrealdb_sql::ModuleName::Silo(organisation, package, major, minor, patch)
			}
			_ => unexpected!(self, peek, "a module name"),
		};

		let mut res = AlterModuleStatement {
			name,
			if_exists,
			comment: AlterKind::None,
			permissions: None,
		};

		loop {
			match self.peek_kind() {
				t!("DROP") => {
					self.pop_peek();
					let peek = self.peek();
					match peek.kind {
						t!("COMMENT") => {
							self.pop_peek();
							res.comment = AlterKind::Drop;
						}
						_ => unexpected!(self, peek, "`COMMENT`"),
					}
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = AlterKind::Set(self.parse_string_lit()?);
				}
				t!("PERMISSIONS") => {
					self.pop_peek();
					res.permissions = Some(self.parse_permission_value()?);
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub fn parse_alter_sequence(&mut self) -> ParseResult<AlterSequenceStatement> {
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.parse_expr_field()?;
		let mut res = AlterSequenceStatement {
			name,
			if_exists,
			..Default::default()
		};

		if self.eat(t!("TIMEOUT")) {
			res.timeout = Some(self.parse_expr_field()?);
		}

		Ok(res)
	}
}
