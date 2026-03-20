use reblessive::Stk;

use crate::catalog::{ApiMethod, EventDefinition, EventKind};
use crate::sql::TableType;
use crate::sql::filter::Filter;
use crate::sql::statements::alter::field::AlterDefault;
use crate::sql::statements::alter::{
	AlterAccessStatement, AlterAnalyzerStatement, AlterApiClause, AlterApiStatement,
	AlterBucketStatement, AlterConfigStatement, AlterDatabaseStatement, AlterEventStatement,
	AlterFieldStatement, AlterFunctionStatement, AlterIndexStatement, AlterKind,
	AlterModuleStatement, AlterNamespaceStatement, AlterParamStatement, AlterSequenceStatement,
	AlterSystemStatement, AlterUserStatement,
};
use crate::sql::statements::define::ApiAction;
use crate::sql::statements::{AlterStatement, AlterTableStatement};
use crate::sql::tokenizer::Tokenizer;
use crate::syn::error::bail;
use crate::syn::parser::mac::{expected, expected_whitespace, unexpected};
use crate::syn::parser::{ParseResult, Parser};
use crate::syn::token::{Token, TokenKind, t};

impl Parser<'_> {
	pub(crate) async fn parse_alter_stmt(&mut self, stk: &mut Stk) -> ParseResult<AlterStatement> {
		let next = self.next();
		match next.kind {
			t!("SYSTEM") => self.parse_alter_system(stk).await.map(AlterStatement::System),
			t!("NAMESPACE") => self.parse_alter_namespace().await.map(AlterStatement::Namespace),
			t!("DATABASE") => self.parse_alter_database().await.map(AlterStatement::Database),
			t!("TABLE") => self.parse_alter_table(stk).await.map(AlterStatement::Table),
			t!("EVENT") => self.parse_alter_event(stk).await.map(AlterStatement::Event),
			t!("INDEX") => self.parse_alter_index().await.map(AlterStatement::Index),
			t!("FIELD") => self.parse_alter_field(stk).await.map(AlterStatement::Field),
			t!("PARAM") => self.parse_alter_param(stk).await.map(AlterStatement::Param),
			t!("SEQUENCE") => self.parse_alter_sequence(stk).await.map(AlterStatement::Sequence),
			t!("BUCKET") => self.parse_alter_bucket(stk, next).await.map(AlterStatement::Bucket),
			t!("ANALYZER") => self.parse_alter_analyzer(stk).await.map(AlterStatement::Analyzer),
			t!("FUNCTION") => self.parse_alter_function(stk).await.map(AlterStatement::Function),
			t!("USER") => self.parse_alter_user(stk).await.map(AlterStatement::User),
			t!("ACCESS") => self.parse_alter_access(stk).await.map(AlterStatement::Access),
			t!("CONFIG") => self.parse_alter_config(stk).await.map(AlterStatement::Config),
			t!("API") => self.parse_alter_api(stk).await.map(AlterStatement::Api),
			t!("MODULE") => self.parse_alter_module(stk).await.map(AlterStatement::Module),
			_ => unexpected!(self, next, "a alter statement keyword"),
		}
	}

	pub(crate) async fn parse_alter_system(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<AlterSystemStatement> {
		let mut res = AlterSystemStatement::default();

		loop {
			match self.peek_kind() {
				t!("DROP") => {
					self.pop_peek();
					let peek = self.peek();
					match peek.kind {
						TokenKind::Identifier => {
							let name = self.parse_ident()?;
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
					let name = self.parse_ident()?;
					match name.as_str() {
						"QUERY_TIMEOUT" => {
							let duration = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
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

	pub(crate) async fn parse_alter_namespace(&mut self) -> ParseResult<AlterNamespaceStatement> {
		if !self.eat(t!("COMPACT")) {
			unexpected!(self, self.peek(), "`COMPACT`")
		}
		Ok(AlterNamespaceStatement {
			compact: true,
		})
	}

	pub(crate) async fn parse_alter_database(&mut self) -> ParseResult<AlterDatabaseStatement> {
		if !self.eat(t!("COMPACT")) {
			unexpected!(self, self.peek(), "`COMPACT`")
		}
		Ok(AlterDatabaseStatement {
			compact: true,
		})
	}

	pub(crate) async fn parse_alter_table(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<AlterTableStatement> {
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.parse_ident()?;
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
						_ => {
							unexpected!(self, peek, "`COMMENT` or `CHANGEFEED`")
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
					res.permissions = Some(stk.run(|stk| self.parse_permission(stk, false)).await?);
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

	pub(crate) async fn parse_alter_event(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<AlterEventStatement> {
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.parse_ident()?;
		expected!(self, t!("ON"));
		self.eat(t!("TABLE"));
		let what = self.parse_ident()?;
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
					res.when = AlterKind::Set(stk.run(|stk| self.parse_expr_field(stk)).await?);
				}
				t!("THEN") => {
					self.pop_peek();
					let mut then = vec![stk.run(|stk| self.parse_expr_field(stk)).await?];
					while self.eat(t!(",")) {
						then.push(stk.run(|stk| self.parse_expr_field(stk)).await?);
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
						retry: EventDefinition::DEFAULT_RETRY,
						max_depth: EventDefinition::DEFAULT_MAX_DEPTH,
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

	pub(crate) async fn parse_alter_index(&mut self) -> ParseResult<AlterIndexStatement> {
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.parse_ident()?;
		expected!(self, t!("ON"));
		self.eat(t!("TABLE"));
		let table = self.parse_ident()?;

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

	pub(crate) async fn parse_alter_field(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<AlterFieldStatement> {
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.parse_local_idiom()?;
		expected!(self, t!("ON"));
		self.eat(t!("TABLE"));
		let what = self.parse_ident()?;
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
								"`FLEXIBLE`, `TYPE`, `READONLY`, `VALUE`, `ASSERT`, `DEFAULT`, `COMMENT`, or `REFERENCE`"
							)
						}
					}
				}
				t!("TYPE") => {
					self.pop_peek();
					res.kind = AlterKind::Set(stk.run(|stk| self.parse_inner_kind(stk)).await?);
				}
				t!("FLEXIBLE") => {
					self.pop_peek();
					res.flexible = AlterKind::Set(());
				}
				t!("READONLY") => {
					self.pop_peek();
					res.readonly = AlterKind::Set(());
				}
				t!("VALUE") => {
					self.pop_peek();
					res.value = AlterKind::Set(stk.run(|stk| self.parse_expr_field(stk)).await?);
				}
				t!("ASSERT") => {
					self.pop_peek();
					res.assert = AlterKind::Set(stk.run(|stk| self.parse_expr_field(stk)).await?);
				}
				t!("DEFAULT") => {
					self.pop_peek();
					if self.eat(t!("ALWAYS")) {
						res.default =
							AlterDefault::Always(stk.run(|stk| self.parse_expr_field(stk)).await?);
					} else {
						res.default =
							AlterDefault::Set(stk.run(|stk| self.parse_expr_field(stk)).await?);
					}
				}
				t!("PERMISSIONS") => {
					self.pop_peek();
					res.permissions = Some(stk.run(|stk| self.parse_permission(stk, false)).await?);
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = AlterKind::Set(self.parse_string_lit()?);
				}
				t!("REFERENCE") => {
					self.pop_peek();
					res.reference = AlterKind::Set(self.parse_reference(stk).await?);
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub(crate) async fn parse_alter_param(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<AlterParamStatement> {
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.next_token_value::<crate::sql::Param>()?.into_string();
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
					res.value = Some(stk.run(|stk| self.parse_expr_field(stk)).await?);
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = AlterKind::Set(self.parse_string_lit()?);
				}
				t!("PERMISSIONS") => {
					self.pop_peek();
					res.permissions = Some(stk.run(|stk| self.parse_permission_value(stk)).await?);
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub(crate) async fn parse_alter_bucket(
		&mut self,
		stk: &mut Stk,
		token: Token,
	) -> ParseResult<AlterBucketStatement> {
		if !self.settings.files_enabled {
			unexpected!(self, token, "the experimental files feature to be enabled");
		}

		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.parse_ident()?;
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
					res.permissions = Some(stk.run(|stk| self.parse_permission_value(stk)).await?);
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

	pub(crate) async fn parse_alter_analyzer(
		&mut self,
		_stk: &mut Stk,
	) -> ParseResult<AlterAnalyzerStatement> {
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.parse_ident()?;
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
					expected!(self, t!("fn"));
					expected!(self, t!("::"));
					let mut ident = self.parse_ident()?;
					while self.eat(t!("::")) {
						let value = self.parse_ident()?;
						ident.push_str("::");
						ident.push_str(&value);
					}
					res.function = AlterKind::Set(ident);
				}
				t!("TOKENIZERS") => {
					self.pop_peek();
					let mut tokenizers = Vec::new();
					loop {
						let next = self.next();
						let tokenizer = match next.kind {
							t!("BLANK") => Tokenizer::Blank,
							t!("CAMEL") => Tokenizer::Camel,
							t!("CLASS") => Tokenizer::Class,
							t!("PUNCT") => Tokenizer::Punct,
							_ => unexpected!(self, next, "a tokenizer"),
						};
						tokenizers.push(tokenizer);
						if !self.eat(t!(",")) {
							break;
						}
					}
					res.tokenizers = AlterKind::Set(tokenizers);
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

	pub(crate) async fn parse_alter_function(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<AlterFunctionStatement> {
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
							let param = self.next_token_value::<crate::sql::Param>()?.into_string();
							expected!(self, t!(":"));
							let kind = stk.run(|stk| self.parse_inner_kind(stk)).await?;
							args.push((param, kind));
							if !self.eat(t!(",")) {
								break;
							}
						}
						expected!(self, t!(")"));
					}
					res.args = AlterKind::Set(args);

					if self.eat(t!("->")) {
						res.returns =
							AlterKind::Set(stk.run(|stk| self.parse_inner_kind(stk)).await?);
					}

					let span = expected!(self, t!("{")).span;
					res.block = AlterKind::Set(self.parse_block(stk, span).await?);
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = AlterKind::Set(self.parse_string_lit()?);
				}
				t!("PERMISSIONS") => {
					self.pop_peek();
					res.permissions = Some(stk.run(|stk| self.parse_permission_value(stk)).await?);
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub(crate) async fn parse_alter_user(
		&mut self,
		_stk: &mut Stk,
	) -> ParseResult<AlterUserStatement> {
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.parse_ident()?;
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
					res.pass_type = Some(crate::sql::statements::define::user::PassType::Password(
						self.parse_string_lit()?,
					));
				}
				t!("PASSHASH") => {
					self.pop_peek();
					res.pass_type = Some(crate::sql::statements::define::user::PassType::Hash(
						self.parse_string_lit()?,
					));
				}
				t!("ROLES") => {
					self.pop_peek();
					let mut roles = vec![self.parse_ident()?];
					while self.eat(t!(",")) {
						roles.push(self.parse_ident()?);
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

	pub(crate) async fn parse_alter_access(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<AlterAccessStatement> {
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.parse_ident()?;
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
						t!("COMMENT") => {
							self.pop_peek();
							res.comment = AlterKind::Drop;
						}
						_ => unexpected!(self, peek, "`AUTHENTICATE` or `COMMENT`"),
					}
				}
				t!("AUTHENTICATE") => {
					self.pop_peek();
					res.authenticate =
						AlterKind::Set(stk.run(|stk| self.parse_expr_field(stk)).await?);
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

	pub(crate) async fn parse_alter_config(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<AlterConfigStatement> {
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};

		let next = self.next();
		let inner = match next.kind {
			t!("API") => self
				.parse_api_config(stk)
				.await
				.map(crate::sql::statements::define::config::ConfigInner::Api)?,
			t!("GRAPHQL") => self
				.parse_graphql_config()
				.map(crate::sql::statements::define::config::ConfigInner::GraphQL)?,
			t!("DEFAULT") => self
				.parse_default_config(stk)
				.await
				.map(crate::sql::statements::define::config::ConfigInner::Default)?,
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

	pub(crate) async fn parse_alter_api(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<AlterApiStatement> {
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let path = stk.run(|stk| self.parse_expr_field(stk)).await?;
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
							let config = self.parse_api_config(stk).await?;
							let config_is_default = config == Default::default();

							let fallback = if self.eat(t!("THEN")) {
								AlterKind::Set(stk.run(|ctx| self.parse_expr_field(ctx)).await?)
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
								let config = self.parse_api_config(stk).await?;
								expected!(self, t!("THEN"));
								let action = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
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

	pub(crate) async fn parse_alter_module(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<AlterModuleStatement> {
		if !self.settings.surrealism_enabled {
			bail!(
				"Experimental capability `surrealism` is not enabled",
				@self.last_span() => "Use of `ALTER MODULE` is still experimental"
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
				let name = self.parse_ident()?;
				crate::sql::ModuleName::Module(name)
			}
			t!("silo") => {
				self.pop_peek();
				expected_whitespace!(self, t!("::"));
				let organisation = self.parse_ident()?;
				expected_whitespace!(self, t!("::"));
				let package = self.parse_ident()?;
				expected_whitespace!(self, t!("<"));
				let major = self.parse_version_digits()?;
				expected_whitespace!(self, t!("."));
				let minor = self.parse_version_digits()?;
				expected_whitespace!(self, t!("."));
				let patch = self.parse_version_digits()?;
				expected_whitespace!(self, t!(">"));
				crate::sql::ModuleName::Silo(organisation, package, major, minor, patch)
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
					res.permissions = Some(stk.run(|stk| self.parse_permission_value(stk)).await?);
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub(crate) async fn parse_alter_sequence(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<AlterSequenceStatement> {
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.parse_ident()?;
		let mut res = AlterSequenceStatement {
			name,
			if_exists,
			..Default::default()
		};

		if self.eat(t!("TIMEOUT")) {
			res.timeout = Some(stk.run(|stk| self.parse_expr_field(stk)).await?);
		}

		Ok(res)
	}
}
