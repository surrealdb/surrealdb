use reblessive::Stk;

use crate::catalog::ApiMethod;
use crate::sql::access::AccessDuration;
use crate::sql::access_type::JwtAccessVerify;
use crate::sql::base::Base;
use crate::sql::filter::Filter;
use crate::sql::index::{Distance, HnswParams, VectorType};
use crate::sql::kind::KindLiteral;
use crate::sql::statements::define::config::api::{ApiConfig, Middleware};
use crate::sql::statements::define::config::defaults::DefaultConfig;
use crate::sql::statements::define::config::graphql::{GraphQLConfig, TableConfig};
use crate::sql::statements::define::config::{ConfigInner, graphql};
use crate::sql::statements::define::user::PassType;
use crate::sql::statements::define::{
	ApiAction, DefineAccessStatement, DefineAnalyzerStatement, DefineApiStatement,
	DefineBucketStatement, DefineConfigStatement, DefineDatabaseStatement, DefineDefault,
	DefineEventStatement, DefineFieldStatement, DefineFunctionStatement, DefineIndexStatement,
	DefineKind, DefineNamespaceStatement, DefineParamStatement, DefineSequenceStatement,
	DefineStatement, DefineTableStatement, DefineUserStatement,
};
use crate::sql::tokenizer::Tokenizer;
use crate::sql::{
	AccessType, DefineModuleStatement, Expr, Index, Kind, Literal, Param, Permission, Permissions,
	Scoring, TableType, access_type, table_type,
};
#[cfg(feature = "surrealism")]
use crate::sql::{ModuleExecutable, SiloExecutable, SurrealismExecutable};
use crate::syn::error::bail;
use crate::syn::parser::mac::{expected, unexpected};
use crate::syn::parser::{ParseResult, Parser};
use crate::syn::token::{Token, TokenKind, t};
use crate::types::PublicDuration;
#[cfg(feature = "surrealism")]
use crate::types::PublicFile;

impl Parser<'_> {
	pub(crate) async fn parse_define_stmt(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<DefineStatement> {
		let next = self.next();
		match next.kind {
			t!("NAMESPACE") => {
				self.parse_define_namespace(stk).await.map(DefineStatement::Namespace)
			}
			t!("DATABASE") => self.parse_define_database(stk).await.map(DefineStatement::Database),
			t!("FUNCTION") => self.parse_define_function(stk).await.map(DefineStatement::Function),
			t!("USER") => self.parse_define_user(stk).await.map(DefineStatement::User),
			t!("PARAM") => self.parse_define_param(stk).await.map(DefineStatement::Param),
			t!("TABLE") => self.parse_define_table(stk).await.map(DefineStatement::Table),
			t!("API") => self.parse_define_api(stk).await.map(DefineStatement::Api),
			t!("EVENT") => {
				stk.run(|stk| self.parse_define_event(stk)).await.map(DefineStatement::Event)
			}
			t!("FIELD") => {
				stk.run(|stk| self.parse_define_field(stk)).await.map(DefineStatement::Field)
			}
			t!("INDEX") => {
				stk.run(|stk| self.parse_define_index(stk)).await.map(DefineStatement::Index)
			}
			t!("ANALYZER") => self.parse_define_analyzer(stk).await.map(DefineStatement::Analyzer),
			t!("ACCESS") => self.parse_define_access(stk).await.map(DefineStatement::Access),
			t!("CONFIG") => self.parse_define_config(stk).await.map(DefineStatement::Config),
			t!("BUCKET") => self.parse_define_bucket(stk, next).await.map(DefineStatement::Bucket),
			t!("SEQUENCE") => self.parse_define_sequence(stk).await.map(DefineStatement::Sequence),
			t!("MODULE") => self.parse_define_module(stk).await.map(DefineStatement::Module),
			_ => unexpected!(self, next, "a define statement keyword"),
		}
	}

	pub(crate) async fn parse_define_namespace(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<DefineNamespaceStatement> {
		let kind = if self.eat(t!("IF")) {
			expected!(self, t!("NOT"));
			expected!(self, t!("EXISTS"));
			DefineKind::IfNotExists
		} else if self.eat(t!("OVERWRITE")) {
			DefineKind::Overwrite
		} else {
			DefineKind::Default
		};
		let name = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
		let mut res = DefineNamespaceStatement {
			id: None,
			name,
			kind,
			..Default::default()
		};

		while let t!("COMMENT") = self.peek_kind() {
			self.pop_peek();
			res.comment = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
		}

		Ok(res)
	}

	pub(crate) async fn parse_define_database(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<DefineDatabaseStatement> {
		let kind = if self.eat(t!("IF")) {
			expected!(self, t!("NOT"));
			expected!(self, t!("EXISTS"));
			DefineKind::IfNotExists
		} else if self.eat(t!("OVERWRITE")) {
			DefineKind::Overwrite
		} else {
			DefineKind::Default
		};
		let name = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
		let mut res = DefineDatabaseStatement {
			name,
			kind,
			..Default::default()
		};
		loop {
			match self.peek_kind() {
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
				}
				t!("CHANGEFEED") => {
					self.pop_peek();
					res.changefeed = Some(self.parse_changefeed()?);
				}
				t!("STRICT") => {
					self.pop_peek();
					res.strict = true;
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub(crate) async fn parse_define_function(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<DefineFunctionStatement> {
		let kind = if self.eat(t!("IF")) {
			expected!(self, t!("NOT"));
			expected!(self, t!("EXISTS"));
			DefineKind::IfNotExists
		} else if self.eat(t!("OVERWRITE")) {
			DefineKind::Overwrite
		} else {
			DefineKind::Default
		};
		let name = self.parse_custom_function_name()?;
		let token = expected!(self, t!("(")).span;
		let mut args = Vec::new();
		loop {
			if self.eat(t!(")")) {
				break;
			}

			let param = self.next_token_value::<Param>()?.into_string();
			expected!(self, t!(":"));
			let kind = stk.run(|ctx| self.parse_inner_kind(ctx)).await?;

			args.push((param, kind));

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!(")"), token)?;
				break;
			}
		}
		let returns = if self.eat(t!("->")) {
			Some(stk.run(|ctx| self.parse_inner_kind(ctx)).await?)
		} else {
			None
		};

		let next = expected!(self, t!("{")).span;
		let block = self.parse_block(stk, next).await?;

		let mut res = DefineFunctionStatement {
			name,
			args,
			block,
			kind,
			returns,
			comment: Expr::Literal(Literal::None),
			permissions: Permission::default(),
		};

		loop {
			match self.peek_kind() {
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
				}
				t!("PERMISSIONS") => {
					self.pop_peek();
					res.permissions = stk.run(|ctx| self.parse_permission_value(ctx)).await?;
				}
				_ => break,
			}
		}

		Ok(res)
	}

	#[cfg(not(feature = "surrealism"))]
	pub(crate) async fn parse_define_module(
		&mut self,
		_stk: &mut Stk,
	) -> ParseResult<DefineModuleStatement> {
		bail!(
			"Surrealism modules are not supported in WASM environments",
			@self.last_span() => "Use of `DEFINE MODULE` is not supported in WASM environments"
		)
	}

	#[cfg(feature = "surrealism")]
	pub(crate) async fn parse_define_module(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<DefineModuleStatement> {
		if !self.settings.surrealism_enabled {
			bail!(
				"Experimental capability `surrealism` is not enabled",
				@self.last_span() => "Use of `DEFINE MODULE` is still experimental"
			)
		}

		let kind = if self.eat(t!("IF")) {
			expected!(self, t!("NOT"));
			expected!(self, t!("EXISTS"));
			DefineKind::IfNotExists
		} else if self.eat(t!("OVERWRITE")) {
			DefineKind::Overwrite
		} else {
			DefineKind::Default
		};

		let name = if self.eat(t!("mod")) {
			expected!(self, t!("::"));
			let name = self.parse_ident()?;
			expected!(self, t!("AS"));
			Some(name)
		} else {
			None
		};

		let peek = self.peek();
		let executable = match peek.kind {
			t!("silo") => {
				self.pop_peek();
				expected!(self, t!("::"));
				let organisation = self.parse_ident()?;
				expected!(self, t!("::"));
				let package = self.parse_ident()?;
				expected!(self, t!("<"));
				let major = self.next_token_value::<u32>()?;
				expected!(self, t!("."));
				let minor = self.next_token_value::<u32>()?;
				expected!(self, t!("."));
				let patch = self.next_token_value::<u32>()?;
				expected!(self, t!(">"));

				ModuleExecutable::Silo(SiloExecutable {
					organisation,
					package,
					major,
					minor,
					patch,
				})
			}
			t!("f\"") | t!("f'") => {
				let file = self.next_token_value::<PublicFile>()?;
				ModuleExecutable::Surrealism(SurrealismExecutable(file.into()))
			}
			_ => {
				unexpected!(self, peek, "a module executable");
			}
		};

		let mut definition = DefineModuleStatement {
			kind,
			name,
			executable,
			comment: Expr::Literal(Literal::None),
			permissions: Permission::default(),
		};

		loop {
			match self.peek_kind() {
				t!("COMMENT") => {
					self.pop_peek();
					definition.comment = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
				}
				t!("PERMISSIONS") => {
					self.pop_peek();
					definition.permissions =
						stk.run(|ctx| self.parse_permission_value(ctx)).await?;
				}
				_ => break,
			}
		}

		Ok(definition)
	}

	pub(crate) async fn parse_define_user(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<DefineUserStatement> {
		let kind = if self.eat(t!("IF")) {
			expected!(self, t!("NOT"));
			expected!(self, t!("EXISTS"));
			DefineKind::IfNotExists
		} else if self.eat(t!("OVERWRITE")) {
			DefineKind::Overwrite
		} else {
			DefineKind::Default
		};
		let name = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
		expected!(self, t!("ON"));
		let base = self.parse_base()?;

		let mut res = DefineUserStatement {
			kind,
			name,
			base,
			// Safety: "Viewer" does not contain a null byte
			roles: vec!["Viewer".to_owned()], /* New users get
			                                   * the viewer role
			                                   * by default */
			// TODO: Move out of the parser
			token_duration: Expr::Literal(Literal::Duration(PublicDuration::from_secs(3600))), /* defaults to 1 hour. */
			..DefineUserStatement::default()
		};

		loop {
			match self.peek_kind() {
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
				}
				t!("PASSWORD") => {
					let token = self.pop_peek();
					if let PassType::Hash(_) = res.pass_type {
						bail!("Unexpected token `PASSWORD`", @token.span => "Can't set both a passhash and a password");
					}
					res.pass_type = PassType::Password(self.parse_string_lit()?);
				}
				t!("PASSHASH") => {
					let token = self.pop_peek();
					if let PassType::Password(_) = res.pass_type {
						bail!("Unexpected token `PASSHASH`", @token.span => "Can't set both a passhash and a password");
					}
					res.pass_type = PassType::Hash(self.parse_string_lit()?);
				}
				t!("ROLES") => {
					self.pop_peek();
					let mut roles = Vec::new();
					loop {
						let token = self.peek();
						let role = self.parse_ident()?;
						// NOTE(gguillemas): This hardcoded list is a temporary fix in order
						// to avoid making breaking changes to the DefineUserStatement structure
						// while still providing parsing feedback to users referencing unexistent
						// roles. This list should be removed once arbitrary roles can be
						// defined by users.
						if !matches!(role.to_lowercase().as_str(), "viewer" | "editor" | "owner") {
							unexpected!(self, token, "an existent role");
						}
						roles.push(role);

						if !self.eat(t!(",")) {
							break;
						}
					}
					res.roles = roles;
				}
				t!("DURATION") => {
					self.pop_peek();
					expected!(self, t!("FOR"));
					loop {
						let token = self.peek();
						match token.kind {
							t!("TOKEN") => {
								self.pop_peek();
								res.token_duration =
									stk.run(|ctx| self.parse_expr_field(ctx)).await?
							}
							t!("SESSION") => {
								self.pop_peek();
								res.session_duration =
									stk.run(|ctx| self.parse_expr_field(ctx)).await?;
							}
							_ => unexpected!(self, token, "`TOKEN` or `SESSION`"),
						}

						self.eat(t!(","));

						if !self.eat(t!("FOR")) {
							break;
						}
					}
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub(crate) async fn parse_define_access(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<DefineAccessStatement> {
		let kind = if self.eat(t!("IF")) {
			expected!(self, t!("NOT"));
			expected!(self, t!("EXISTS"));
			DefineKind::IfNotExists
		} else if self.eat(t!("OVERWRITE")) {
			DefineKind::Overwrite
		} else {
			DefineKind::Default
		};
		let name = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
		expected!(self, t!("ON"));
		let base = self.parse_base()?;

		let mut res = DefineAccessStatement {
			name,
			base,
			kind,
			authenticate: None,
			access_type: AccessType::default(),
			duration: AccessDuration::default(),
			comment: Expr::Literal(Literal::None),
		};

		loop {
			match self.peek_kind() {
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
				}
				t!("TYPE") => {
					self.pop_peek();
					let peek = self.peek();
					match peek.kind {
						t!("JWT") => {
							self.pop_peek();
							res.access_type = AccessType::Jwt(self.parse_jwt(stk).await?);
						}
						t!("RECORD") => {
							let token = self.pop_peek();
							// The record access type can only be defined at the database level
							if !matches!(res.base, Base::Db) {
								unexpected!(self, token, "a valid access type at this level");
							}
							let mut ac = access_type::RecordAccess::default();
							loop {
								match self.peek_kind() {
									t!("SIGNUP") => {
										self.pop_peek();
										ac.signup =
											Some(stk.run(|stk| self.parse_expr_field(stk)).await?);
									}
									t!("SIGNIN") => {
										self.pop_peek();
										ac.signin =
											Some(stk.run(|stk| self.parse_expr_field(stk)).await?);
									}
									_ => break,
								}
							}

							while self.eat(t!("WITH")) {
								let token = self.peek();
								match token.kind {
									t!("JWT") => {
										self.pop_peek();
										let jwt = self.parse_jwt(stk).await?;
										ac.jwt = jwt.clone();
										// Use same issuer for refreshed tokens.
										if let Some(mut bearer) = ac.bearer {
											bearer.jwt = jwt;
											ac.bearer = Some(bearer);
										}
									}
									t!("REFRESH") => {
										self.pop_peek();
										ac.bearer = Some(access_type::BearerAccess {
											kind: access_type::BearerAccessType::Refresh,
											subject: access_type::BearerAccessSubject::Record,
											// Use same issuer for refreshed tokens.
											jwt: ac.jwt.clone(),
										});
									}
									_ => {
										unexpected!(self, token, "JWT or REFRESH")
									}
								}
								self.eat(t!(","));
							}
							res.access_type = AccessType::Record(ac);
						}
						t!("BEARER") => {
							self.pop_peek();
							let mut ac = access_type::BearerAccess {
								..Default::default()
							};
							expected!(self, t!("FOR"));
							let peek = self.peek();
							match peek.kind {
								t!("USER") => {
									self.pop_peek();
									ac.subject = access_type::BearerAccessSubject::User;
								}
								t!("RECORD") => {
									match &res.base {
										Base::Db => (),
										_ => {
											unexpected!(self, peek, "USER", => "`RECORD` bearer can only be defined on a database")
										}
									}
									self.pop_peek();
									ac.subject = access_type::BearerAccessSubject::Record;
								}
								_ => match &res.base {
									Base::Db => unexpected!(self, peek, "USER or RECORD"),
									_ => unexpected!(self, peek, "USER"),
								},
							}
							if self.eat(t!("WITH")) {
								expected!(self, t!("JWT"));
								ac.jwt = self.parse_jwt(stk).await?;
							}
							res.access_type = AccessType::Bearer(ac);
						}
						_ => break,
					}
				}
				t!("AUTHENTICATE") => {
					self.pop_peek();
					res.authenticate = Some(stk.run(|stk| self.parse_expr_field(stk)).await?);
				}
				t!("DURATION") => {
					self.pop_peek();
					while self.eat(t!("FOR")) {
						let peek = self.peek();
						match peek.kind {
							t!("GRANT") => {
								self.pop_peek();
								res.duration.grant =
									stk.run(|ctx| self.parse_expr_field(ctx)).await?;
							}
							t!("TOKEN") => {
								self.pop_peek();
								res.duration.token =
									stk.run(|ctx| self.parse_expr_field(ctx)).await?;
							}
							t!("SESSION") => {
								self.pop_peek();
								res.duration.session =
									stk.run(|ctx| self.parse_expr_field(ctx)).await?;
							}
							_ => unexpected!(self, peek, "GRANT, TOKEN or SESSIONS"),
						}
						self.eat(t!(","));
					}
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub(crate) async fn parse_define_param(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<DefineParamStatement> {
		let kind = if self.eat(t!("IF")) {
			expected!(self, t!("NOT"));
			expected!(self, t!("EXISTS"));
			DefineKind::IfNotExists
		} else if self.eat(t!("OVERWRITE")) {
			DefineKind::Overwrite
		} else {
			DefineKind::Default
		};
		let name = self.next_token_value::<Param>()?.into_string();

		let mut res = DefineParamStatement {
			name,
			kind,
			value: Expr::Literal(Literal::None),
			comment: Expr::Literal(Literal::None),
			permissions: Permission::default(),
		};

		loop {
			match self.peek_kind() {
				t!("VALUE") => {
					self.pop_peek();
					res.value = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
				}
				t!("PERMISSIONS") => {
					self.pop_peek();
					res.permissions = stk.run(|ctx| self.parse_permission_value(ctx)).await?;
				}
				_ => break,
			}
		}
		Ok(res)
	}

	pub(crate) async fn parse_define_table(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<DefineTableStatement> {
		let kind = if self.eat(t!("IF")) {
			expected!(self, t!("NOT"));
			expected!(self, t!("EXISTS"));
			DefineKind::IfNotExists
		} else if self.eat(t!("OVERWRITE")) {
			DefineKind::Overwrite
		} else {
			DefineKind::Default
		};
		let name = stk.run(|ctx| self.parse_expr_table(ctx)).await?;
		let mut res = DefineTableStatement {
			name,
			permissions: Permissions::none(),
			kind,
			..Default::default()
		};

		let mut set_table_type = false;
		loop {
			match self.peek_kind() {
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
				}
				t!("DROP") => {
					self.pop_peek();
					res.drop = true;
				}
				t!("TYPE") => {
					self.pop_peek();
					let peek = self.peek();
					match peek.kind {
						t!("NORMAL") => {
							self.pop_peek();
							res.table_type = TableType::Normal;
						}
						t!("RELATION") => {
							self.pop_peek();
							res.table_type = TableType::Relation(self.parse_relation_schema()?);
						}
						t!("ANY") => {
							self.pop_peek();
							res.table_type = TableType::Any;
						}
						_ => unexpected!(self, peek, "`NORMAL`, `RELATION`, or `ANY`"),
					}
					set_table_type = true;
				}
				t!("SCHEMALESS") => {
					self.pop_peek();
					res.full = false;
				}
				t!("SCHEMAFULL") => {
					self.pop_peek();
					res.full = true;
					// TODO: Move logic out of parser.
					if !set_table_type {
						res.table_type = TableType::Normal;
					}
				}
				t!("PERMISSIONS") => {
					self.pop_peek();
					res.permissions = stk.run(|stk| self.parse_permission(stk, false)).await?;
				}
				t!("CHANGEFEED") => {
					self.pop_peek();
					res.changefeed = Some(self.parse_changefeed()?);
				}
				t!("AS") => {
					self.pop_peek();
					let peek = self.peek();
					match peek.kind {
						t!("(") => {
							let open = self.pop_peek().span;
							res.view = Some(self.parse_view(stk).await?);
							self.expect_closing_delimiter(t!(")"), open)?;
						}
						t!("SELECT") => {
							res.view = Some(self.parse_view(stk).await?);
						}
						_ => unexpected!(self, peek, "`SELECT`"),
					}
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub(crate) async fn parse_define_api(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<DefineApiStatement> {
		if !self.settings.define_api_enabled {
			bail!("Cannot define an API, as the experimental define api capability is not enabled", @self.last_span);
		}

		let kind = if self.eat(t!("IF")) {
			expected!(self, t!("NOT"));
			expected!(self, t!("EXISTS"));
			DefineKind::IfNotExists
		} else if self.eat(t!("OVERWRITE")) {
			DefineKind::Overwrite
		} else {
			DefineKind::Default
		};

		let path = stk.run(|ctx| self.parse_expr_field(ctx)).await?;

		let mut res = DefineApiStatement {
			path,
			kind,
			actions: Vec::new(),
			fallback: None,
			config: ApiConfig {
				middleware: Vec::new(),
				permissions: Permission::Full,
			},
			comment: Expr::Literal(Literal::None),
		};

		loop {
			if !self.eat(t!("FOR")) {
				break;
			}

			let peek = self.peek();
			match peek.kind {
				t!("ANY") => {
					self.pop_peek();
					res.config = self.parse_api_config(stk).await?;

					if self.eat(t!("THEN")) {
						res.fallback = Some(stk.run(|ctx| self.parse_expr_field(ctx)).await?);
					}
				}
				t!("DELETE") | t!("GET") | t!("PATCH") | t!("POST") | t!("PUT") | t!("TRACE") => {
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

					let config = self.parse_api_config(stk).await?;

					expected!(self, t!("THEN"));
					let action = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
					res.actions.push(ApiAction {
						methods,
						action,
						config,
					});
				}
				_ => {
					unexpected!(
						self,
						peek,
						"one of `DELETE`, `GET`, `PATCH`, `POST`, `PUT` or `TRACE`"
					)
				}
			}
		}

		if self.eat(t!("COMMENT")) {
			res.comment = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
		}

		Ok(res)
	}

	pub(crate) async fn parse_define_event(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<DefineEventStatement> {
		let kind = if self.eat(t!("IF")) {
			expected!(self, t!("NOT"));
			expected!(self, t!("EXISTS"));
			DefineKind::IfNotExists
		} else if self.eat(t!("OVERWRITE")) {
			DefineKind::Overwrite
		} else {
			DefineKind::Default
		};

		let name = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
		expected!(self, t!("ON"));
		self.eat(t!("TABLE"));
		let what = stk.run(|ctx| self.parse_expr_table(ctx)).await?;

		let mut res = DefineEventStatement {
			kind,
			name,
			target_table: what,
			when: Expr::Literal(Literal::Bool(true)),
			then: Vec::new(),
			comment: Expr::Literal(Literal::None),
		};

		loop {
			match self.peek_kind() {
				t!("WHEN") => {
					self.pop_peek();
					res.when = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
				}
				t!("THEN") => {
					self.pop_peek();
					res.then = vec![stk.run(|ctx| self.parse_expr_field(ctx)).await?];
					while self.eat(t!(",")) {
						res.then.push(stk.run(|ctx| self.parse_expr_field(ctx)).await?)
					}
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
				}
				_ => break,
			}
		}
		Ok(res)
	}

	pub(crate) async fn parse_define_field(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<DefineFieldStatement> {
		let kind = if self.eat(t!("IF")) {
			expected!(self, t!("NOT"));
			expected!(self, t!("EXISTS"));
			DefineKind::IfNotExists
		} else if self.eat(t!("OVERWRITE")) {
			DefineKind::Overwrite
		} else {
			DefineKind::Default
		};
		let name = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
		expected!(self, t!("ON"));
		self.eat(t!("TABLE"));
		let what = stk.run(|ctx| self.parse_expr_table(ctx)).await?;

		let mut res = DefineFieldStatement {
			name,
			what,
			kind,
			..Default::default()
		};

		loop {
			match self.peek_kind() {
				// FLEX, FLEXI and FLEXIBLE are all the same token type.
				t!("FLEXIBLE") => {
					self.pop_peek();
					bail!("FLEXIBLE must be specified after TYPE", @self.last_span);
				}
				t!("TYPE") => {
					self.pop_peek();
					res.field_kind = Some(stk.run(|ctx| self.parse_inner_kind(ctx)).await?);

					// Check if FLEXIBLE follows TYPE
					if self.eat(t!("FLEXIBLE")) {
						// Validate that the field_kind contains an object
						fn kind_contains_object(kind: &Kind) -> bool {
							match kind {
								Kind::Object => true,
								Kind::Either(kinds) => kinds.iter().any(kind_contains_object),
								Kind::Array(inner, _) | Kind::Set(inner, _) => {
									kind_contains_object(inner)
								}
								Kind::Literal(KindLiteral::Object(_)) => true,
								Kind::Literal(KindLiteral::Array(x)) => {
									x.iter().any(kind_contains_object)
								}
								_ => false,
							}
						}

						let is_valid_for_flexible =
							res.field_kind.as_ref().is_some_and(kind_contains_object);

						if !is_valid_for_flexible {
							bail!("FLEXIBLE can only be used with types containing object", @self.last_span);
						}

						res.flexible = true;
					}
				}
				t!("READONLY") => {
					self.pop_peek();
					res.readonly = true;
				}
				t!("VALUE") => {
					self.pop_peek();
					res.value = Some(stk.run(|ctx| self.parse_expr_field(ctx)).await?);
				}
				t!("ASSERT") => {
					self.pop_peek();
					res.assert = Some(stk.run(|ctx| self.parse_expr_field(ctx)).await?);
				}
				t!("DEFAULT") => {
					self.pop_peek();
					if self.eat(t!("ALWAYS")) {
						res.default =
							DefineDefault::Always(stk.run(|ctx| self.parse_expr_field(ctx)).await?);
					} else {
						res.default =
							DefineDefault::Set(stk.run(|ctx| self.parse_expr_field(ctx)).await?);
					}
				}
				t!("PERMISSIONS") => {
					self.pop_peek();
					res.permissions = stk.run(|ctx| self.parse_permission(ctx, true)).await?;
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
				}
				t!("REFERENCE") => {
					self.pop_peek();
					res.reference = Some(self.parse_reference(stk).await?);
				}
				t!("COMPUTED") => {
					self.pop_peek();
					res.computed = Some(stk.run(|stk| self.parse_expr_field(stk)).await?);
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub(crate) async fn parse_define_index(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<DefineIndexStatement> {
		let kind = if self.eat(t!("IF")) {
			expected!(self, t!("NOT"));
			expected!(self, t!("EXISTS"));
			DefineKind::IfNotExists
		} else if self.eat(t!("OVERWRITE")) {
			DefineKind::Overwrite
		} else {
			DefineKind::Default
		};
		let name = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
		expected!(self, t!("ON"));
		self.eat(t!("TABLE"));
		let what = stk.run(|ctx| self.parse_expr_table(ctx)).await?;

		let mut res = DefineIndexStatement {
			name,
			what,
			kind,
			cols: Vec::new(),
			index: Index::Idx,
			comment: Expr::Literal(Literal::None),
			concurrently: false,
		};

		let mut field_span = None;

		loop {
			match self.peek_kind() {
				// COLUMNS and FIELDS are the same tokenkind
				t!("FIELDS") => {
					self.pop_peek();
					res.cols = vec![stk.run(|ctx| self.parse_expr_field(ctx)).await?];
					while self.eat(t!(",")) {
						res.cols.push(stk.run(|ctx| self.parse_expr_field(ctx)).await?);
					}
					field_span = Some(self.last_span);
				}
				t!("UNIQUE") => {
					self.pop_peek();
					res.index = Index::Uniq;
				}
				t!("COUNT") => {
					self.pop_peek();
					let cond = self.try_parse_condition(stk).await?;
					res.index = Index::Count(cond);
				}
				t!("FULLTEXT") => {
					self.pop_peek();
					let mut analyzer: Option<String> = None;
					let mut scoring = None;
					let mut hl = false;
					loop {
						match self.peek_kind() {
							t!("ANALYZER") => {
								self.pop_peek();
								analyzer = Some(self.parse_ident()).transpose()?;
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
							t!("HIGHLIGHTS") => {
								self.pop_peek();
								hl = true;
							}
							_ => break,
						}
					}
					res.index = Index::FullText(crate::sql::index::FullTextParams {
						az: analyzer.unwrap_or_else(|| "like".to_owned()),
						sc: scoring.unwrap_or_else(Default::default),
						hl,
					});
				}
				t!("HNSW") => {
					self.pop_peek();
					expected!(self, t!("DIMENSION"));
					let dimension = self.next_token_value()?;
					let mut distance = Distance::Euclidean;
					let mut vector_type = VectorType::F32;
					let mut m = None;
					let mut m0 = None;
					let mut ml = None;
					let mut ef_construction = 150;
					let mut extend_candidates = false;
					let mut keep_pruned_connections = false;
					let mut use_hashed_vector = false;
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
								let v = self.next_token_value()?;
								let span = self.last_span();
								m = Some((v, span));
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
							t!("HASHED_VECTOR") => {
								self.pop_peek();
								use_hashed_vector = true;
							}
							_ => {
								break;
							}
						}
					}

					let m = if let Some((m, span)) = m {
						if m > 127 {
							bail!("Invalid value for HNSW parameter `M`", @span => "`M` cannot be larger then 127")
						}
						m
					} else {
						12u8
					};

					let m0 = m0.unwrap_or(m.saturating_mul(2));
					let ml = ml.unwrap_or((1.0 / (m as f64).ln()).into());
					res.index = Index::Hnsw(HnswParams {
						dimension,
						distance,
						vector_type,
						m,
						m0,
						ml,
						ef_construction,
						extend_candidates,
						keep_pruned_connections,
						use_hashed_vector,
					});
				}
				t!("CONCURRENTLY") => {
					self.pop_peek();
					res.concurrently = true;
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
				}
				_ => break,
			}
		}
		match (field_span, &res.index) {
			(Some(field_span), Index::Count(_)) => {
				if !res.cols.is_empty() {
					bail!("Cannot create a count index with fields", @field_span);
				}
			}
			(field_span, Index::FullText(_) | Index::Hnsw(_)) => {
				if res.cols.len() != 1 {
					if let Some(field_span) = field_span {
						bail!("Expected one column, found {}", res.cols.len(), @field_span);
					} else {
						bail!("Expected one column, found none", @self.recent_span());
					}
				}
			}
			(None, Index::Uniq | Index::Idx) => {
				if res.cols.is_empty() {
					bail!("Expected at least one column - Use FIELDS to define columns", @self.recent_span());
				}
			}
			(_, _) => {}
		}
		Ok(res)
	}

	pub(crate) async fn parse_define_analyzer(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<DefineAnalyzerStatement> {
		let kind = if self.eat(t!("IF")) {
			expected!(self, t!("NOT"));
			expected!(self, t!("EXISTS"));
			DefineKind::IfNotExists
		} else if self.eat(t!("OVERWRITE")) {
			DefineKind::Overwrite
		} else {
			DefineKind::Default
		};
		let name = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
		let mut res = DefineAnalyzerStatement {
			name,

			function: None,
			tokenizers: None,
			filters: None,
			comment: Expr::Literal(Literal::None),

			kind,
		};
		loop {
			match self.peek_kind() {
				t!("FILTERS") => {
					self.pop_peek();
					let mut filters = Vec::new();
					loop {
						let next = self.next();
						match next.kind {
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
							t!("MAPPER") => {
								let open_span = expected!(self, t!("(")).span;
								let path: String = self.parse_string_lit()?;
								self.expect_closing_delimiter(t!(")"), open_span)?;
								filters.push(Filter::Mapper(path))
							}
							_ => unexpected!(self, next, "a filter"),
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
					res.tokenizers = Some(tokenizers);
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
					res.function = Some(ident);
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
				}
				_ => break,
			}
		}
		Ok(res)
	}

	pub(crate) async fn parse_define_bucket(
		&mut self,
		stk: &mut Stk,
		token: Token,
	) -> ParseResult<DefineBucketStatement> {
		if !self.settings.files_enabled {
			unexpected!(self, token, "the experimental files feature to be enabled");
		}

		let kind = if self.eat(t!("IF")) {
			expected!(self, t!("NOT"));
			expected!(self, t!("EXISTS"));
			DefineKind::IfNotExists
		} else if self.eat(t!("OVERWRITE")) {
			DefineKind::Overwrite
		} else {
			DefineKind::Default
		};

		let name = stk.run(|ctx| self.parse_expr_field(ctx)).await?;

		let mut res = DefineBucketStatement {
			name,
			kind,
			..Default::default()
		};

		loop {
			match self.peek_kind() {
				t!("BACKEND") => {
					self.pop_peek();
					res.backend = Some(stk.run(|stk| self.parse_expr_field(stk)).await?);
				}
				t!("PERMISSIONS") => {
					self.pop_peek();
					res.permissions = stk.run(|stk| self.parse_permission_value(stk)).await?;
				}
				t!("READONLY") => {
					self.pop_peek();
					res.readonly = true;
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
				}
				_ => {
					break;
				}
			}
		}

		Ok(res)
	}

	pub(crate) async fn parse_define_sequence(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<DefineSequenceStatement> {
		let kind = if self.eat(t!("IF")) {
			expected!(self, t!("NOT"));
			expected!(self, t!("EXISTS"));
			DefineKind::IfNotExists
		} else if self.eat(t!("OVERWRITE")) {
			DefineKind::Overwrite
		} else {
			DefineKind::Default
		};
		let name = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
		let batch = if self.eat(t!("BATCH")) {
			stk.run(|ctx| self.parse_expr_field(ctx)).await?
		} else {
			Expr::Literal(Literal::Integer(1000))
		};
		let start = if self.eat(t!("START")) {
			stk.run(|ctx| self.parse_expr_field(ctx)).await?
		} else {
			Expr::Literal(Literal::Integer(0))
		};
		let timeout = self.try_parse_timeout(stk).await?;
		Ok(DefineSequenceStatement {
			name,
			kind,
			batch,
			start,
			timeout,
		})
	}

	pub(crate) async fn parse_define_config(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<DefineConfigStatement> {
		let kind = if self.eat(t!("IF")) {
			expected!(self, t!("NOT"));
			expected!(self, t!("EXISTS"));
			DefineKind::IfNotExists
		} else if self.eat(t!("OVERWRITE")) {
			DefineKind::Overwrite
		} else {
			DefineKind::Default
		};

		let next = self.next();
		let inner = match next.kind {
			t!("API") => self.parse_api_config(stk).await.map(ConfigInner::Api)?,
			t!("GRAPHQL") => self.parse_graphql_config().map(ConfigInner::GraphQL)?,
			t!("DEFAULT") => self.parse_default_config(stk).await.map(ConfigInner::Default)?,
			_ => unexpected!(self, next, "a type of config"),
		};

		Ok(DefineConfigStatement {
			inner,
			kind,
		})
	}

	pub(crate) async fn parse_default_config(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<DefaultConfig> {
		let mut config = DefaultConfig::default();

		let peek = self.peek();
		if !matches!(peek.kind, t!("NAMESPACE") | t!("DATABASE")) {
			unexpected!(self, peek, "a namespace or database name");
		}

		loop {
			match self.peek_kind() {
				t!("NAMESPACE") => {
					self.pop_peek();
					config.namespace = stk.run(|stk| self.parse_expr_field(stk)).await?;
				}
				t!("DATABASE") => {
					self.pop_peek();
					config.database = stk.run(|stk| self.parse_expr_field(stk)).await?;
				}
				_ => break,
			}
		}
		Ok(config)
	}

	pub(crate) async fn parse_api_config(&mut self, stk: &mut Stk) -> ParseResult<ApiConfig> {
		let mut config = ApiConfig::default();
		loop {
			match self.peek_kind() {
				t!("PERMISSIONS") => {
					self.pop_peek();
					config.permissions = self.parse_permission_value(stk).await?;
				}
				t!("MIDDLEWARE") => {
					self.pop_peek();

					let mut middleware = Vec::new();

					loop {
						let name = self.parse_function_name().await?.to_string();

						expected!(self, t!("("));
						let args = self.parse_function_args(stk).await?;

						middleware.push(Middleware {
							name,
							args,
						});

						if !self.eat(t!(",")) {
							break;
						}
					}

					config.middleware = middleware;
				}
				_ => {
					break;
				}
			}
		}
		Ok(config)
	}

	fn parse_graphql_config(&mut self) -> ParseResult<GraphQLConfig> {
		use graphql::{FunctionsConfig, TablesConfig};
		let mut tmp_tables = Option::<TablesConfig>::None;
		let mut tmp_fncs = Option::<FunctionsConfig>::None;
		loop {
			match self.peek_kind() {
				t!("NONE") => {
					self.pop_peek();
					tmp_tables = Some(TablesConfig::None);
					tmp_fncs = Some(FunctionsConfig::None);
				}
				t!("AUTO") => {
					self.pop_peek();
					tmp_tables = Some(TablesConfig::Auto);
					tmp_fncs = Some(FunctionsConfig::Auto);
				}
				t!("TABLES") => {
					self.pop_peek();

					let next = self.next();
					match next.kind {
						t!("INCLUDE") => {
							tmp_tables =
								Some(TablesConfig::Include(self.parse_graphql_table_configs()?))
						}
						t!("EXCLUDE") => {
							tmp_tables =
								Some(TablesConfig::Exclude(self.parse_graphql_table_configs()?))
						}
						t!("NONE") => {
							tmp_tables = Some(TablesConfig::None);
						}
						t!("AUTO") => {
							tmp_tables = Some(TablesConfig::Auto);
						}
						_ => unexpected!(self, next, "`NONE`, `AUTO`, `INCLUDE` or `EXCLUDE`"),
					}
				}
				t!("FUNCTIONS") => {
					self.pop_peek();

					let next = self.next();
					match next.kind {
						t!("NONE") => {
							tmp_fncs = Some(FunctionsConfig::None);
						}
						t!("AUTO") => {
							tmp_fncs = Some(FunctionsConfig::Auto);
						}
						//TODO: Actually implement INCLUDE and EXCLUDE
						_ => unexpected!(self, next, "`NONE`, `AUTO`"),
					}
				}
				_ => break,
			}
		}

		Ok(GraphQLConfig {
			tables: tmp_tables.unwrap_or_default(),
			functions: tmp_fncs.unwrap_or_default(),
		})
	}

	fn parse_graphql_table_configs(&mut self) -> ParseResult<Vec<graphql::TableConfig>> {
		let mut acc = vec![];
		loop {
			match self.peek_kind() {
				x if Self::kind_is_identifier(x) => {
					let name = self.parse_ident()?;
					acc.push(TableConfig {
						name,
					});
				}
				_ => unexpected!(self, self.next(), "a table config"),
			}
			if !self.eat(t!(",")) {
				break;
			}
		}
		Ok(acc)
	}

	pub fn parse_relation_schema(&mut self) -> ParseResult<table_type::Relation> {
		let mut res = table_type::Relation {
			from: Vec::new(),
			to: Vec::new(),
			enforced: false,
		};
		loop {
			match self.peek_kind() {
				t!("FROM") | t!("IN") => {
					self.pop_peek();
					res.from = self.parse_tables()?;
				}
				t!("TO") | t!("OUT") => {
					self.pop_peek();
					res.to = self.parse_tables()?;
				}
				_ => break,
			}
		}
		if self.eat(t!("ENFORCED")) {
			res.enforced = true;
		}
		Ok(res)
	}

	pub fn parse_tables(&mut self) -> ParseResult<Vec<String>> {
		let mut names = vec![self.parse_ident()?];
		while self.eat(t!("|")) {
			names.push(self.parse_ident()?);
		}
		Ok(names)
	}

	async fn parse_jwt(&mut self, stk: &mut Stk) -> ParseResult<access_type::JwtAccess> {
		let mut res = access_type::JwtAccess {
			// By default, a JWT access method is only used to verify.
			issue: None,
			..Default::default()
		};

		let mut iss = access_type::JwtAccessIssue::default();

		let peek = self.peek();
		match peek.kind {
			t!("ALGORITHM") => {
				self.pop_peek();
				let next = self.next();
				match next.kind {
					TokenKind::Algorithm(alg) => {
						let next = self.next();
						match next.kind {
							t!("KEY") => {
								let key = stk.run(|stk| self.parse_expr_field(stk)).await?;
								res.verify = access_type::JwtAccessVerify::Key(
									access_type::JwtAccessVerifyKey {
										alg,
										key: key.clone(),
									},
								);

								// Currently, issuer and verifier must use the same algorithm.
								iss.alg = alg;

								// If the algorithm is symmetric, the issuer and verifier keys are
								// the same. For asymmetric algorithms, the key needs to be
								// explicitly defined.
								if alg.is_symmetric() {
									iss.key = key;
									// Since all the issuer data is known, it can already be
									// assigned. Cloning allows updating the original with
									// any explicit issuer data.
									res.issue = Some(iss.clone());
								}
							}
							_ => unexpected!(self, next, "a key"),
						}
					}
					_ => unexpected!(self, next, "a valid algorithm"),
				}
			}
			t!("URL") => {
				self.pop_peek();
				let url = stk.run(|stk| self.parse_expr_field(stk)).await?;
				res.verify = access_type::JwtAccessVerify::Jwks(access_type::JwtAccessVerifyJwks {
					url,
				});
			}
			_ => unexpected!(self, peek, "`ALGORITHM`, or `URL`"),
		}

		if self.eat(t!("WITH")) {
			expected!(self, t!("ISSUER"));
			loop {
				let peek = self.peek();
				match peek.kind {
					t!("ALGORITHM") => {
						self.pop_peek();
						let next = self.next();
						match next.kind {
							TokenKind::Algorithm(alg) => {
								// If an algorithm is already defined, a different value is not
								// expected.
								if let JwtAccessVerify::Key(ref ver) = res.verify
									&& alg != ver.alg
								{
									unexpected!(
										self,
										next,
										"a compatible algorithm or no algorithm"
									);
								}
								iss.alg = alg;
							}
							_ => unexpected!(self, next, "a valid algorithm"),
						}
					}
					t!("KEY") => {
						self.pop_peek();
						iss.key = stk.run(|stk| self.parse_expr_field(stk)).await?;
					}
					_ => break,
				}
			}
			res.issue = Some(iss);
		}

		Ok(res)
	}
}
