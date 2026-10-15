//! Lowering of `DEFINE` statements: replays each definition's clauses in
//! source order onto the runtime statement, and makes the parse-time
//! checks on combinations of clauses.

use std::time::Duration;

use surrealdb_parser_next::ast::{
	ALTER_SYSTEM, Ast, Cl, DefineKind as K, NONE, Node, NodeId, Tag, define, jwt, vector,
};
use surrealdb_parser_next::lex::decode_duration;
use surrealdb_strand::{Strand as Ident, Strand, TableName};

use super::{FILES_DISABLED, Lower, R, Reject, SURREALISM_DISABLED};
use crate::expr::access::AccessDuration;
use crate::expr::access_type::{
	BearerAccess, BearerAccessSubject, BearerAccessType, JwtAccess, JwtAccessIssue,
	JwtAccessVerify, JwtAccessVerifyJwks, JwtAccessVerifyKey, RecordAccess,
};
use crate::expr::changefeed::ChangeFeed;
use crate::expr::filter::Filter;
use crate::expr::graphql_config::{
	GraphQLConfig, GraphQLFunctionsConfig as FunctionsConfig,
	GraphQLIntrospectionConfig as IntrospectionConfig, GraphQLTablesConfig as TablesConfig,
};
use crate::expr::index::{DiskAnnParams, Distance, FullTextParams, HnswParams, VectorType};
use crate::expr::index_kind::Index;
use crate::expr::language::Language;
use crate::expr::module::ModuleName;
use crate::expr::permission::{Permission, Permissions};
use crate::expr::reference::{Reference, ReferenceDeleteStrategy};
use crate::expr::statements::alter::field::AlterDefault;
use crate::expr::statements::alter::{
	AlterAccessStatement, AlterAnalyzerStatement, AlterApiClause, AlterApiStatement,
	AlterBucketStatement, AlterConfigStatement, AlterDatabaseStatement, AlterEventStatement,
	AlterFieldStatement, AlterFunctionStatement, AlterIndexStatement, AlterKind,
	AlterModuleStatement, AlterNamespaceStatement, AlterParamStatement, AlterSequenceStatement,
	AlterStatement, AlterSystemStatement, AlterTableStatement, AlterUserStatement,
};
use crate::expr::statements::define::config::ConfigInner;
use crate::expr::statements::define::config::api::{ApiConfig, Middleware};
use crate::expr::statements::define::config::defaults::DefaultConfig;
use crate::expr::statements::define::{
	ApiAction, DefineAccessStatement, DefineAnalyzerStatement, DefineApiStatement,
	DefineBucketStatement, DefineConfigStatement, DefineDatabaseStatement, DefineDefault,
	DefineEventStatement, DefineFieldStatement, DefineFunctionStatement, DefineIndexStatement,
	DefineKind, DefineNamespaceStatement, DefineParamStatement, DefineSequenceStatement,
	DefineStatement, DefineTableStatement, kind_contains_object,
};
use crate::expr::statements::remove::{
	RemoveAccessStatement, RemoveAnalyzerStatement, RemoveApiStatement, RemoveBucketStatement,
	RemoveConfigStatement, RemoveDatabaseStatement, RemoveEventStatement, RemoveFieldStatement,
	RemoveFunctionStatement, RemoveIndexStatement, RemoveModuleStatement, RemoveNamespaceStatement,
	RemoveParamStatement, RemoveSequenceStatement, RemoveStatement, RemoveTableStatement,
	RemoveUserStatement,
};
use crate::expr::statements::{DefineModuleStatement, DefineUserStatement};
use crate::expr::table_type::TableType;
use crate::expr::tokenizer::{SegmentLanguage, Tokenizer};
use crate::expr::user::PendingPassword;
use crate::expr::{
	AccessType, Algorithm, ApiMethod, Base, Cond, EventKind, Expr, Fields, Literal,
	ModuleExecutable, Scoring, SiloExecutable, SurrealismExecutable, View, table_type,
};
use crate::iam::ScramCredential;

fn table(s: String) -> TableName {
	TableName::new(Strand::new(s))
}

impl Lower<'_> {
	fn rec(&self, idx: u32, len: usize) -> &[u32] {
		&self.ast.extra[idx as usize..idx as usize + len]
	}

	fn string(&self, id: NodeId) -> R<String> {
		match self.expr(id)? {
			Expr::Literal(Literal::String(s)) => Ok(s.into_string()),
			_ => Err(Reject::new("not a string")),
		}
	}

	fn uint<T: TryFrom<u64>>(&self, id: NodeId) -> R<T> {
		T::try_from(Ast::uint(self.n(id))).map_err(|_| Reject::new("integer out of range"))
	}

	fn float(&self, id: NodeId) -> f64 {
		let n = self.n(id);
		match n.tag {
			Tag::Int => Ast::int(n) as f64,
			Tag::NaN => f64::NAN,
			Tag::Infinity => f64::INFINITY,
			_ => Ast::float(n),
		}
	}

	fn number(&self, id: NodeId) -> R<surrealdb_types::Number> {
		Ok(match self.expr(id)? {
			Expr::Literal(Literal::Integer(i)) => surrealdb_types::Number::Int(i),
			Expr::Literal(Literal::Float(f)) => surrealdb_types::Number::Float(f),
			Expr::Literal(Literal::Decimal(d)) => surrealdb_types::Number::Decimal(d),
			_ => return Err(Reject::new("not a number")),
		})
	}

	fn duration(&self, id: NodeId) -> R<Duration> {
		let (s, ns) =
			decode_duration(self.text(id).as_bytes()).ok_or(Reject::new("duration overflow"))?;
		Ok(Duration::new(s, ns))
	}

	fn perm(&self, id: NodeId) -> R<Permission> {
		let n = self.n(id);
		Ok(match n.a {
			0 => Permission::None,
			1 => Permission::Full,
			_ => Permission::Specific(self.expr(n.b)?),
		})
	}

	fn permissions(&self, c: &Node) -> R<Permissions> {
		let mut p = if c.b == 1 {
			Permissions::default()
		} else {
			Permissions {
				select: Permission::None,
				create: Permission::None,
				update: Permission::None,
				delete: Permission::None,
			}
		};
		let list = self.list(c.a);
		// A bare `NONE` or `FULL` is the whole set, whatever the default.
		for &e in list {
			let e = self.n(e);
			let v = self.perm(e.b)?;
			if e.a & 1 != 0 {
				p.select = v.clone();
			}
			if e.a & 2 != 0 {
				p.create = v.clone();
			}
			if e.a & 4 != 0 {
				p.update = v.clone();
			}
			if e.a & 8 != 0 {
				p.delete = v;
			}
		}
		Ok(p)
	}

	fn scram_credential(&self, value: &str, start: u32) -> R<ScramCredential> {
		ScramCredential::from_verifier_string(value).map_err(|e| {
			Reject::new(format!("Invalid SCRAM verifier: {e}"))
				.at(start, start + "PASSSCRAM".len() as u32)
				.with_label("Expected a valid `SCRAM-SHA-256$...` verifier string")
		})
	}

	fn changefeed(&self, c: &Node) -> R<ChangeFeed> {
		Ok(ChangeFeed {
			expiry: self.duration(c.a)?.into(),
			store_diff: c.b == 1,
		})
	}

	/// The `fn::` path of a `Name`, without `fn::`, its escaped segments
	/// decoded.
	fn path(&self, id: NodeId) -> R<String> {
		self.path_name(id)
	}

	pub fn define(&self, n: &Node) -> R<DefineStatement> {
		let rec = self.rec(n.a, define::LEN);
		let kind = match rec[define::EXIST] {
			1 => DefineKind::IfNotExists,
			2 => DefineKind::Overwrite,
			_ => DefineKind::Default,
		};
		let clauses: Vec<&Node> =
			self.list(rec[define::CLAUSES]).iter().map(|&c| self.n(c)).collect();
		let cl = |c: &Node| -> Cl { unsafe_cl(c.op) };
		let name = || self.expr(rec[define::NAME]);
		let kd = n.op;
		self.gate_kind(kd)?;
		Ok(match kd {
			x if x == K::Namespace as u8 => {
				let mut s = DefineNamespaceStatement {
					kind,
					name: name()?,
					..Default::default()
				};
				for c in clauses {
					s.comment = self.expr(c.a)?;
				}
				DefineStatement::Namespace(s)
			}
			x if x == K::Database as u8 => {
				let mut s = DefineDatabaseStatement {
					kind,
					name: name()?,
					..Default::default()
				};
				for c in clauses {
					match cl(c) {
						Cl::Comment => s.comment = self.expr(c.a)?,
						Cl::Changefeed => s.changefeed = Some(self.changefeed(c)?),
						_ => s.strict = true,
					}
				}
				DefineStatement::Database(s)
			}
			x if x == K::Function as u8 => {
				let args = self
					.list(rec[define::X1])
					.chunks(2)
					.map(|pk| Ok((self.param_name(pk[0])?, self.kind(pk[1])?)))
					.collect::<R<_>>()?;
				let block = match self.expr(rec[define::X3])? {
					Expr::Block(b) => *b,
					_ => return Err(Reject::new("not a block")),
				};
				let mut s = DefineFunctionStatement {
					name: Strand::new(self.path(rec[define::NAME])?),
					args,
					block,
					kind,
					returns: if rec[define::X2] == NONE {
						None
					} else {
						Some(self.kind(rec[define::X2])?)
					},
					comment: Expr::Literal(Literal::None),
					permissions: Permission::default(),
					graphql_alias: None,
					graphql_deprecated: None,
				};
				for c in clauses {
					match cl(c) {
						Cl::Comment => s.comment = self.expr(c.a)?,
						Cl::PermValue => s.permissions = self.perm(c.a)?,
						Cl::GraphqlAlias => s.graphql_alias = Some(self.string(c.a)?),
						_ => s.graphql_deprecated = Some(self.string(c.a)?),
					}
				}
				DefineStatement::Function(s)
			}

			x if x == K::User as u8 => {
				let mut s = DefineUserStatement {
					kind,
					name: name()?,
					base: Self::base(rec[define::ON]),
					roles: vec!["Viewer".to_owned()],
					..Default::default()
				};
				let mut pass_kind = 0;
				for c in clauses {
					match cl(c) {
						Cl::Comment => s.comment = self.expr(c.a)?,
						Cl::Password => {
							if pass_kind == 2 {
								return Err(Reject::new("both a passhash and a password"));
							}
							pass_kind = 1;
							s.pending_password = Some(PendingPassword(self.string(c.a)?));
						}
						Cl::Passhash => {
							if pass_kind == 1 {
								return Err(Reject::new("both a passhash and a password"));
							}
							pass_kind = 2;
							s.hash = self.string(c.a)?;
							s.hash_is_set = true;
						}
						Cl::Passscram => {
							s.scram = Some(self.scram_credential(&self.string(c.a)?, c.start)?)
						}
						Cl::Roles => {
							let mut roles = Vec::new();
							for &r in self.list(c.a) {
								let role = self.name(r)?;
								if !matches!(
									role.to_lowercase().as_str(),
									"viewer" | "editor" | "owner"
								) {
									return Err(Reject::new("not an existent role"));
								}
								roles.push(role);
							}
							s.roles = roles;
						}
						Cl::DurToken => s.duration.token = self.expr(c.a)?,
						_ => s.duration.session = self.expr(c.a)?,
					}
				}
				DefineStatement::User(s)
			}
			x if x == K::Access as u8 => DefineStatement::Access(self.access(kind, rec, &clauses)?),
			x if x == K::Param as u8 => {
				let mut s = DefineParamStatement {
					name: Ident::new(Strand::new(self.param_name(rec[define::NAME])?)),
					kind,
					value: Expr::Literal(Literal::None),
					comment: Expr::Literal(Literal::None),
					permissions: Permission::default(),
				};
				for c in clauses {
					match cl(c) {
						Cl::Value => s.value = self.expr(c.a)?,
						Cl::Comment => s.comment = self.expr(c.a)?,
						_ => s.permissions = self.perm(c.a)?,
					}
				}
				DefineStatement::Param(s)
			}
			x if x == K::Table as u8 => {
				let mut s = DefineTableStatement {
					name: name()?,
					permissions: Permissions {
						select: Permission::None,
						create: Permission::None,
						update: Permission::None,
						delete: Permission::None,
					},
					kind,
					..Default::default()
				};
				let mut set_type = false;
				for c in clauses {
					match cl(c) {
						Cl::Comment => s.comment = self.expr(c.a)?,
						Cl::Drop => s.drop = true,
						Cl::TypeNormal => {
							s.table_type = TableType::Normal;
							set_type = true;
						}
						Cl::TypeAny => {
							s.table_type = TableType::Any;
							set_type = true;
						}
						Cl::TypeRelation => {
							let r = self.rec(c.a, 3);
							let names = |l: u32| -> R<Vec<TableName>> {
								if l == NONE {
									return Ok(Vec::new());
								}
								self.list(l).iter().map(|&t| self.name(t).map(table)).collect()
							};
							s.table_type = TableType::Relation(table_type::Relation {
								from: names(r[0])?,
								to: names(r[1])?,
								enforced: r[2] & 1 != 0,
								lightweight: r[2] & 2 != 0,
							});
							set_type = true;
						}
						Cl::Schemaless => s.full = false,
						Cl::Schemafull => {
							s.full = true;
							if !set_type {
								s.table_type = TableType::Normal;
							}
						}
						Cl::Permissions => s.permissions = self.permissions(c)?,
						Cl::Changefeed => s.changefeed = Some(self.changefeed(c)?),
						Cl::View => s.view = Some(self.view(c.a)?),
						Cl::GraphqlAlias => s.graphql_alias = Some(self.string(c.a)?),
						Cl::GraphqlDeprecated => s.graphql_deprecated = Some(self.string(c.a)?),
						Cl::InlineEdges => s.inline_edges_cap = Some(self.uint(c.a)?),
						_ => s.inline_refs_cap = Some(self.uint(c.a)?),
					}
				}
				DefineStatement::Table(s)
			}
			x if x == K::Api as u8 => {
				let mut s = DefineApiStatement {
					path: name()?,
					kind,
					actions: Vec::new(),
					fallback: None,
					config: ApiConfig {
						middleware: Vec::new(),
						permissions: Permission::Full,
					},
					comment: Expr::Literal(Literal::None),
				};
				for c in clauses {
					match cl(c) {
						Cl::ApiAny => {
							s.config = self.api_config(c.a)?;
							if c.b != NONE {
								s.fallback = Some(self.expr(c.b)?);
							}
						}
						Cl::ApiMethods => {
							let r = self.rec(c.a, 3);
							let methods = self
								.list(r[0])
								.iter()
								.map(|&m| match self.text(m).to_ascii_uppercase().as_str() {
									"DELETE" => ApiMethod::Delete,
									"GET" => ApiMethod::Get,
									"PATCH" => ApiMethod::Patch,
									"POST" => ApiMethod::Post,
									"PUT" => ApiMethod::Put,
									_ => ApiMethod::Trace,
								})
								.collect();
							s.actions.push(ApiAction {
								methods,
								config: self.api_config(r[1])?,
								action: self.expr(r[2])?,
							});
						}
						_ => s.comment = self.expr(c.a)?,
					}
				}
				DefineStatement::Api(s)
			}
			x if x == K::Event as u8 => {
				let mut s = DefineEventStatement {
					kind,
					name: name()?,
					target_table: self.expr(rec[define::ON])?,
					when: Expr::Literal(Literal::Bool(true)),
					then: Vec::new(),
					comment: Expr::Literal(Literal::None),
					event_kind: EventKind::Sync,
				};
				for c in clauses {
					match cl(c) {
						Cl::When => s.when = self.expr(c.a)?,
						Cl::Then => s.then = self.exprs(c.a)?,
						Cl::Comment => s.comment = self.expr(c.a)?,
						Cl::Async => {
							s.event_kind = EventKind::Async {
								retry: EventKind::DEFAULT_RETRY,
								max_depth: EventKind::DEFAULT_MAX_DEPTH,
							}
						}
						Cl::Retry => match &mut s.event_kind {
							EventKind::Async {
								retry,
								..
							} => *retry = self.uint(c.a)?,
							_ => return Err(after_async(c, "RETRY")),
						},
						_ => match &mut s.event_kind {
							EventKind::Async {
								max_depth,
								..
							} => *max_depth = self.uint(c.a)?,
							_ => return Err(after_async(c, "MAXDEPTH")),
						},
					}
				}
				DefineStatement::Event(s)
			}
			x if x == K::Field as u8 => {
				let mut s = DefineFieldStatement {
					name: name()?,
					what: self.expr(rec[define::ON])?,
					kind,
					..Default::default()
				};
				// Only the field `id` may name the components of its tuple type.
				let is_id = matches!(&s.name, Expr::Idiom(i) if i.is_id());
				for c in clauses {
					match cl(c) {
						Cl::FieldType => {
							let (k, names) = self.field_type(c.a, is_id)?;
							if c.b == 1 {
								if !kind_contains_object(&k) {
									let (a, b) = self.last_tok(c.start, c.end);
									return Err(Reject::new(
										"FLEXIBLE can only be used with types containing object",
									)
									.at(a, b));
								}
								s.flexible = true;
							}
							// Each `TYPE` clause replaces the names along with the kind.
							s.set_type(Some(k), names)
								.map_err(|e| Reject::new(e.to_string()).at(c.start, c.end))?;
						}
						Cl::Readonly => s.readonly = true,
						Cl::Inline => s.inline = true,
						Cl::Value => s.value = Some(self.expr(c.a)?),
						Cl::Assert => s.assert = Some(self.expr(c.a)?),
						Cl::Default => {
							let e = self.expr(c.a)?;
							s.default = if c.b == 1 {
								DefineDefault::Always(e)
							} else {
								DefineDefault::Set(e)
							};
						}
						Cl::Permissions => s.permissions = self.permissions(c)?,
						Cl::Comment => s.comment = self.expr(c.a)?,
						Cl::Reference => {
							s.reference = Some(Reference {
								on_delete: match c.a {
									0 => ReferenceDeleteStrategy::Reject,
									1 => ReferenceDeleteStrategy::Cascade,
									3 => ReferenceDeleteStrategy::Unset,
									4 => ReferenceDeleteStrategy::Custom(self.expr(c.b)?),
									_ => ReferenceDeleteStrategy::Ignore,
								},
							})
						}
						Cl::Computed => s.computed = Some(self.expr(c.a)?),
						Cl::GraphqlAlias => s.graphql_alias = Some(self.string(c.a)?),
						_ => s.graphql_deprecated = Some(self.string(c.a)?),
					}
				}
				DefineStatement::Field(Box::new(s))
			}
			x if x == K::Index as u8 => DefineStatement::Index(self.index(kind, rec, &clauses)?),
			x if x == K::Analyzer as u8 => {
				let mut s = DefineAnalyzerStatement {
					name: name()?,
					kind,
					..Default::default()
				};
				for c in clauses {
					match cl(c) {
						Cl::Filters => s.filters = Some(self.filters(c)?),
						Cl::Tokenizers => s.tokenizers = Some(self.tokenizers(c)),
						Cl::Function => s.function = Some(Strand::new(self.analyzer_function(c)?)),
						_ => s.comment = self.expr(c.a)?,
					}
				}
				DefineStatement::Analyzer(s)
			}
			x if x == K::Config as u8 => {
				let inner = match rec[define::ON] {
					0 => ConfigInner::Api(self.api_config(self.list(rec[define::CLAUSES])[0])?),
					1 => ConfigInner::GraphQL(self.graphql(&clauses)?),
					_ => {
						let mut d = DefaultConfig::default();
						for c in clauses {
							match cl(c) {
								Cl::DefaultNs => d.namespace = self.expr(c.a)?,
								_ => d.database = self.expr(c.a)?,
							}
						}
						ConfigInner::Default(d)
					}
				};
				DefineStatement::Config(DefineConfigStatement {
					inner,
					kind,
				})
			}
			x if x == K::Bucket as u8 => {
				let mut s = DefineBucketStatement {
					name: name()?,
					kind,
					..Default::default()
				};
				for c in clauses {
					match cl(c) {
						Cl::Backend => s.backend = Some(self.expr(c.a)?),
						Cl::PermValue => s.permissions = self.perm(c.a)?,
						Cl::Readonly => s.readonly = true,
						_ => s.comment = self.expr(c.a)?,
					}
				}
				DefineStatement::Bucket(s)
			}
			x if x == K::Sequence as u8 => {
				let mut s = DefineSequenceStatement {
					name: name()?,
					kind,
					batch: Expr::Literal(Literal::Integer(1000)),
					start: Expr::Literal(Literal::Integer(0)),
					timeout: Expr::Literal(Literal::None),
				};
				for c in clauses {
					match cl(c) {
						Cl::Batch => s.batch = self.expr(c.a)?,
						Cl::Start => s.start = self.expr(c.a)?,
						_ => s.timeout = self.expr(c.a)?,
					}
				}
				DefineStatement::Sequence(s)
			}
			_ => {
				self.warn_deprecated_extension(n.b, n.start);
				let exe = rec[define::X1];
				// `X1` is a list index for a silo package, a node otherwise.
				let executable = if rec[define::X2] != 1 {
					match self.expr(exe)? {
						Expr::Literal(Literal::File(f)) => {
							ModuleExecutable::Surrealism(SurrealismExecutable(f))
						}
						_ => return Err(Reject::new("not a file")),
					}
				} else {
					let l = self.list(exe);
					ModuleExecutable::Silo(SiloExecutable {
						organisation: self.name(l[0])?,
						package: self.name(l[1])?,
						major: self.uint(l[2])?,
						minor: self.uint(l[3])?,
						patch: self.uint(l[4])?,
					})
				};
				let mut s = DefineModuleStatement {
					kind,
					// An unnamed silo definition, which only an import reads.
					name: if rec[define::NAME] == NONE {
						None
					} else {
						Some(self.name(rec[define::NAME])?)
					},
					executable,
					unsigned: false,
					comment: Expr::Literal(Literal::None),
					permissions: Permission::default(),
				};
				for c in clauses {
					match cl(c) {
						Cl::Unsigned => s.unsigned = true,
						Cl::Comment => s.comment = self.expr(c.a)?,
						_ => s.permissions = self.perm(c.a)?,
					}
				}
				DefineStatement::Module(s)
			}
		})
	}

	fn view(&self, id: NodeId) -> R<View> {
		let n = self.n(id);
		use surrealdb_parser_next::ast::select as s;
		let rec = self.rec(n.a, s::LEN);
		let expr = if rec[s::VALUE] != NONE {
			Fields::Value(Box::new(self.selector(self.n(rec[s::VALUE]))?))
		} else {
			self.fields(rec[s::FIELDS])?
		};
		let (_, group) = self.split_group(&expr, rec, n.start, true)?;
		Ok(View {
			what: self
				.list(rec[s::WHAT])
				.iter()
				.map(|&t| self.name(t).map(table))
				.collect::<R<_>>()?,
			cond: self.opt(rec[s::COND])?.map(Cond),
			group,
			expr,
		})
	}

	fn api_config(&self, id: NodeId) -> R<ApiConfig> {
		let mut cfg = ApiConfig::default();
		for &c in self.list(self.n(id).a) {
			let c = self.n(c);
			if unsafe_cl(c.op) == Cl::PermValue {
				cfg.permissions = self.perm(c.a)?;
				continue;
			}
			let mut mw = Vec::new();
			for &call in self.list(c.a) {
				let call = self.n(call);
				let name = self.path(call.a)?;
				let name = if self.n(call.a).op == 1 {
					format!("fn::{name}")
				} else {
					name
				};
				mw.push(Middleware {
					name: Strand::new(name),
					args: self.exprs(call.b)?,
				});
			}
			cfg.middleware = mw;
		}
		Ok(cfg)
	}

	fn graphql(&self, clauses: &[&Node]) -> R<GraphQLConfig> {
		let mut tables = None;
		let mut fncs = None;
		let mut depth = None;
		let mut complexity = None;
		let mut introspection = None;
		for c in clauses {
			match unsafe_cl(c.op) {
				Cl::GqlBoth => {
					if c.a == 1 {
						tables = Some(TablesConfig::Auto);
						fncs = Some(FunctionsConfig::Auto);
					} else {
						tables = Some(TablesConfig::None);
						fncs = Some(FunctionsConfig::None);
					}
				}
				Cl::GqlTables => {
					let names = || -> R<Vec<TableName>> {
						self.list(c.b).iter().map(|&t| Ok(table(self.name(t)?))).collect()
					};
					tables = Some(match c.a {
						0 => TablesConfig::None,
						1 => TablesConfig::Auto,
						2 => TablesConfig::Include(names()?),
						_ => TablesConfig::Exclude(names()?),
					});
				}
				Cl::GqlFunctions => {
					let names = || -> R<Vec<Strand>> {
						self.list(c.b).iter().map(|&t| Ok(Strand::new(self.path(t)?))).collect()
					};
					fncs = Some(match c.a {
						0 => FunctionsConfig::None,
						1 => FunctionsConfig::Auto,
						2 => FunctionsConfig::Include(names()?),
						_ => FunctionsConfig::Exclude(names()?),
					});
				}
				Cl::GqlDepth => depth = Some(self.uint(c.a)?),
				Cl::GqlComplexity => complexity = Some(self.uint(c.a)?),
				_ => {
					introspection = Some(if c.a == 1 {
						IntrospectionConfig::Auto
					} else {
						IntrospectionConfig::None
					})
				}
			}
		}
		Ok(GraphQLConfig {
			tables: tables.unwrap_or_default(),
			functions: fncs.unwrap_or_default(),
			depth_limit: depth,
			complexity_limit: complexity,
			introspection: introspection.unwrap_or_default(),
		})
	}

	fn index(&self, kind: DefineKind, rec: &[u32], clauses: &[&Node]) -> R<DefineIndexStatement> {
		let mut s = DefineIndexStatement {
			name: self.expr(rec[define::NAME])?,
			what: self.expr(rec[define::ON])?,
			kind,
			cols: Vec::new(),
			index: Index::Idx,
			comment: Expr::Literal(Literal::None),
			concurrently: false,
		};
		// The last token of the column list, where a column count that does
		// not fit the index is reported.
		let mut fields = None;
		for c in clauses {
			match unsafe_cl(c.op) {
				Cl::Fields => {
					s.cols = self.exprs(c.a)?;
					fields = Some(self.last_tok(c.start, c.end));
				}
				Cl::Unique => s.index = Index::Uniq,
				Cl::Count => s.index = Index::Count(self.opt(c.a)?.map(Cond)),
				Cl::Fulltext => {
					let sc = if c.b == NONE || c.b == NONE - 1 {
						Scoring::default()
					} else {
						let r = self.rec(c.b, 2);
						Scoring::Bm {
							k1: self.float(r[0]) as f32,
							b: self.float(r[1]) as f32,
						}
					};
					s.index = Index::FullText(FullTextParams {
						az: Strand::new(if c.a == NONE {
							"like".to_owned()
						} else {
							self.name(c.a)?
						}),
						sc,
						hl: false,
					});
				}
				Cl::Highlights => {
					if let Index::FullText(p) = &mut s.index {
						p.hl = true;
					}
				}
				Cl::Hnsw => s.index = Index::Hnsw(self.hnsw(c.a)?),
				Cl::Diskann => s.index = Index::DiskAnn(self.diskann(c.a)?),
				Cl::Concurrently => s.concurrently = true,
				_ => s.comment = self.expr(c.a)?,
			}
		}
		// An index without a `FIELDS` clause that needs columns is rejected
		// while parsing.
		match (&s.index, fields) {
			(Index::Count(_), Some((a, b))) if !s.cols.is_empty() => {
				return Err(Reject::new("Cannot create a count index with fields").at(a, b));
			}
			(Index::FullText(_) | Index::Hnsw(_) | Index::DiskAnn(_), Some((a, b)))
				if s.cols.len() != 1 =>
			{
				return Err(
					Reject::new(format!("Expected one column, found {}", s.cols.len())).at(a, b)
				);
			}
			_ => {}
		}
		Ok(s)
	}

	fn distance(&self, r: &[u32]) -> R<Distance> {
		if r[vector::DISTANCE] == NONE {
			return Ok(Distance::Euclidean);
		}
		self.distance_of(r[vector::DISTANCE], r[vector::MINKOWSKI])
	}

	pub fn distance_of(&self, name: NodeId, order: NodeId) -> R<Distance> {
		Ok(match self.text(name).to_ascii_uppercase().as_str() {
			"CHEBYSHEV" => Distance::Chebyshev,
			"COSINE" => Distance::Cosine,
			"COSINE_NORMALIZED" => Distance::CosineNormalized,
			"EUCLIDEAN" => Distance::Euclidean,
			"HAMMING" => Distance::Hamming,
			"INNER_PRODUCT" => Distance::InnerProduct,
			"JACCARD" => Distance::Jaccard,
			"MANHATTAN" => Distance::Manhattan,
			"MINKOWSKI" => Distance::Minkowski(self.number(order)?),
			_ => Distance::Pearson,
		})
	}

	fn vector_type(&self, r: &[u32]) -> VectorType {
		if r[vector::TYPE] == NONE {
			return VectorType::F32;
		}
		match self.text(r[vector::TYPE]).to_ascii_uppercase().as_str() {
			"F64" => VectorType::F64,
			"F16" => VectorType::F16,
			"I64" => VectorType::I64,
			"I32" => VectorType::I32,
			"I16" => VectorType::I16,
			"I8" => VectorType::I8,
			"U8" => VectorType::U8,
			_ => VectorType::F32,
		}
	}

	fn hnsw(&self, idx: u32) -> R<HnswParams> {
		let r = self.rec(idx, vector::LEN);
		let m: u8 = if r[vector::M] == NONE {
			12
		} else {
			self.uint(r[vector::M])?
		};
		if m > 127 {
			return Err(Reject::new("`M` cannot be larger then 127"));
		}
		Ok(HnswParams {
			dimension: self.uint(r[vector::DIMENSION])?,
			distance: self.distance(r)?,
			vector_type: self.vector_type(r),
			m,
			m0: if r[vector::M0] == NONE {
				m.saturating_mul(2)
			} else {
				self.uint(r[vector::M0])?
			},
			ml: if r[vector::LM] == NONE {
				(1.0 / (m as f64).ln()).into()
			} else {
				self.number(r[vector::LM])?
			},
			ef_construction: if r[vector::EFC] == NONE {
				150
			} else {
				self.uint(r[vector::EFC])?
			},
			extend_candidates: r[vector::FLAGS] & 1 != 0,
			keep_pruned_connections: r[vector::FLAGS] & 2 != 0,
			use_hashed_vector: r[vector::FLAGS] & 4 != 0,
		})
	}

	fn diskann(&self, idx: u32) -> R<DiskAnnParams> {
		let r = self.rec(idx, vector::LEN);
		let distance = self.distance(r)?;
		let vector_type = self.vector_type(r);
		if !matches!(
			distance,
			Distance::Euclidean
				| Distance::Cosine
				| Distance::InnerProduct
				| Distance::CosineNormalized
		) {
			return Err(Reject::new("invalid DISTANCE for DISKANN"));
		}
		if !matches!(
			vector_type,
			VectorType::F32 | VectorType::F16 | VectorType::I8 | VectorType::U8
		) {
			return Err(Reject::new("invalid TYPE for DISKANN"));
		}
		if matches!(distance, Distance::CosineNormalized)
			&& matches!(vector_type, VectorType::I8 | VectorType::U8)
		{
			return Err(Reject::new("invalid TYPE for DISKANN COSINE_NORMALIZED"));
		}
		let degree: u16 = if r[vector::M] == NONE {
			64
		} else {
			self.uint(r[vector::M])?
		};
		let l_build: u16 = if r[vector::M0] == NONE {
			100
		} else {
			self.uint(r[vector::M0])?
		};
		if degree == 0 || l_build == 0 {
			return Err(Reject::new("DISKANN parameter must be greater than 0"));
		}
		Ok(DiskAnnParams {
			dimension: self.uint(r[vector::DIMENSION])?,
			distance,
			vector_type,
			degree,
			l_build,
			alpha: if r[vector::LM] == NONE {
				1.2.into()
			} else {
				self.number(r[vector::LM])?
			},
			use_hashed_vector: r[vector::FLAGS] & 4 != 0,
		})
	}

	fn algorithm(&self, id: NodeId) -> Algorithm {
		match self.text(id).to_ascii_uppercase().as_str() {
			"EDDSA" => Algorithm::EdDSA,
			"ES256" => Algorithm::Es256,
			"ES384" => Algorithm::Es384,
			"ES512" => Algorithm::Es512,
			"HS256" => Algorithm::Hs256,
			"HS384" => Algorithm::Hs384,
			"HS512" => Algorithm::Hs512,
			"PS256" => Algorithm::Ps256,
			"PS384" => Algorithm::Ps384,
			"PS512" => Algorithm::Ps512,
			"RS256" => Algorithm::Rs256,
			"RS384" => Algorithm::Rs384,
			_ => Algorithm::Rs512,
		}
	}

	/// A JWT definition, as earlier releases build it: a symmetric verification key
	/// doubles as the issuer key.
	fn jwt(&self, id: NodeId) -> R<JwtAccess> {
		let r = self.rec(self.n(id).a, jwt::LEN);
		let mut res = JwtAccess {
			issue: None,
			..Default::default()
		};
		let mut iss = JwtAccessIssue::default();
		if r[jwt::ALG] != NONE {
			let alg = self.algorithm(r[jwt::ALG]);
			let key = self.expr(r[jwt::KEY])?;
			res.verify = JwtAccessVerify::Key(JwtAccessVerifyKey {
				alg,
				key: key.clone(),
			});
			iss.alg = alg;
			if alg.is_symmetric() {
				iss.key = key;
				res.issue = Some(iss.clone());
			}
		} else {
			res.verify = JwtAccessVerify::Jwks(JwtAccessVerifyJwks {
				url: self.expr(r[jwt::KEY])?,
			});
		}
		if r[jwt::AUDIENCE] != NONE {
			res.audience = Some(self.exprs(r[jwt::AUDIENCE])?);
		}
		if r[jwt::ISSUER] == 1 {
			if r[jwt::ISSUER_ALG] != NONE {
				let alg = self.algorithm(r[jwt::ISSUER_ALG]);
				if let JwtAccessVerify::Key(ref ver) = res.verify
					&& alg != ver.alg
				{
					return Err(Reject::new("incompatible issuer algorithm"));
				}
				iss.alg = alg;
			}
			if r[jwt::ISSUER_KEY] != NONE {
				iss.key = self.expr(r[jwt::ISSUER_KEY])?;
			}
			res.issue = Some(iss);
		}
		Ok(res)
	}

	fn access(&self, kind: DefineKind, rec: &[u32], clauses: &[&Node]) -> R<DefineAccessStatement> {
		let base = Self::base(rec[define::ON]);
		let mut s = DefineAccessStatement {
			name: self.expr(rec[define::NAME])?,
			base,
			kind,
			authenticate: None,
			context: None,
			access_type: AccessType::default(),
			duration: AccessDuration::default(),
			comment: Expr::Literal(Literal::None),
		};
		for c in clauses {
			match unsafe_cl(c.op) {
				Cl::Comment => s.comment = self.expr(c.a)?,
				Cl::AccessJwt => s.access_type = AccessType::Jwt(self.jwt(c.a)?),
				Cl::AccessRecord => {
					if !matches!(s.base, Base::Db) {
						return Err(Reject::new("record access is only valid on a database"));
					}
					let mut ac = RecordAccess::default();
					for &rc in self.list(c.a) {
						let rc = self.n(rc);
						match unsafe_cl(rc.op) {
							Cl::Signup => ac.signup = Some(self.expr(rc.a)?),
							Cl::Signin => ac.signin = Some(self.expr(rc.a)?),
							Cl::WithJwt => {
								let jwt = self.jwt(rc.a)?;
								ac.jwt = jwt.clone();
								if let Some(mut bearer) = ac.bearer {
									bearer.jwt = jwt;
									ac.bearer = Some(bearer);
								}
							}
							_ => {
								ac.bearer = Some(BearerAccess {
									kind: BearerAccessType::Refresh,
									subject: BearerAccessSubject::Record,
									jwt: ac.jwt.clone(),
								})
							}
						}
					}
					s.access_type = AccessType::Record(Box::new(ac));
				}
				Cl::AccessBearer => {
					let mut ac = BearerAccess {
						..Default::default()
					};
					if c.a == 1 {
						if !matches!(s.base, Base::Db) {
							return Err(Reject::new(
								"`RECORD` bearer can only be defined on a database",
							));
						}
						ac.subject = BearerAccessSubject::Record;
					} else {
						ac.subject = BearerAccessSubject::User;
					}
					if c.b != NONE {
						ac.jwt = self.jwt(c.b)?;
					}
					s.access_type = AccessType::Bearer(ac);
				}
				Cl::Authenticate => s.authenticate = Some(self.expr(c.a)?),
				Cl::Context => s.context = Some(self.expr(c.a)?),
				Cl::DurGrant => s.duration.grant = self.expr(c.a)?,
				Cl::DurToken => s.duration.token = self.expr(c.a)?,
				_ => s.duration.session = self.expr(c.a)?,
			}
		}
		if !matches!(s.base, Base::Db) && matches!(s.access_type, AccessType::Record(_)) {
			// The base keyword, after `ON`.
			let on = self.tok_at(self.n(rec[define::NAME]).end);
			let b = self.tok_at(on.end);
			return Err(Reject::new("Missing access type")
				.at(b.start, b.end)
				.with_label("An access defined at this level must specify a TYPE"));
		}
		Ok(s)
	}
}

/// The established error for a `RETRY` or `MAXDEPTH` clause `c` before `ASYNC`.
fn after_async(c: &Node, kw: &'static str) -> Reject {
	let end = c.start + kw.len() as u32;
	Reject::new(format!("Unexpected token `{kw}`"))
		.at(c.start, end)
		.with_label(format!("{kw} must be set after ASYNC"))
}

fn language(text: &str) -> Language {
	match text.to_ascii_uppercase().as_str() {
		"AR" | "ARA" | "ARABIC" => Language::Arabic,
		"DA" | "DAN" | "DANISH" => Language::Danish,
		"NL" | "NLD" | "DUTCH" => Language::Dutch,
		"EN" | "ENG" | "ENGLISH" => Language::English,
		"FI" | "FIN" | "FINNISH" => Language::Finnish,
		"FR" | "FRA" | "FRENCH" => Language::French,
		"DE" | "DEU" | "GERMAN" => Language::German,
		"EL" | "ELL" | "GREEK" => Language::Greek,
		"HU" | "HUN" | "HUNGARIAN" => Language::Hungarian,
		"IT" | "ITA" | "ITALIAN" => Language::Italian,
		"NO" | "NOR" | "NORWEGIAN" => Language::Norwegian,
		"PT" | "POR" | "PORTUGUESE" => Language::Portuguese,
		"RO" | "RON" | "ROMANIAN" => Language::Romanian,
		"RU" | "RUS" | "RUSSIAN" => Language::Russian,
		"ES" | "SPA" | "SPANISH" => Language::Spanish,
		"SV" | "SWE" | "SWEDISH" => Language::Swedish,
		"TA" | "TAM" | "TAMIL" => Language::Tamil,
		_ => Language::Turkish,
	}
}

/// The clause code of a clause node.
fn unsafe_cl(op: u8) -> Cl {
	// SAFETY: `Cl` is `repr(u8)` with contiguous discriminants, and every
	// clause node's `op` was written from a `Cl`.
	unsafe { core::mem::transmute::<u8, Cl>(op) }
}

impl Lower<'_> {
	pub fn remove(&self, n: &Node) -> R<RemoveStatement> {
		use surrealdb_parser_next::ast::remove as rm;
		let rec = self.rec(n.a, rm::LEN);
		let if_exists = rec[rm::FLAGS] & 1 != 0;
		let expunge = rec[rm::FLAGS] & 2 != 0;
		let name = || self.expr(rec[rm::NAME]);
		let on = || self.expr(rec[rm::ON]);
		if n.op == K::Module as u8 {
			self.gate_kind(n.op)?;
		}
		Ok(match n.op {
			x if x == K::Namespace as u8 => RemoveStatement::Namespace(RemoveNamespaceStatement {
				name: name()?,
				if_exists,
				expunge,
			}),
			x if x == K::Database as u8 => RemoveStatement::Database(RemoveDatabaseStatement {
				name: name()?,
				if_exists,
				expunge,
			}),
			x if x == K::Table as u8 => RemoveStatement::Table(RemoveTableStatement {
				name: name()?,
				if_exists,
				expunge,
			}),
			x if x == K::Function as u8 => RemoveStatement::Function(RemoveFunctionStatement {
				name: Strand::new(self.path(rec[rm::NAME])?),
				if_exists,
			}),
			x if x == K::Module as u8 => {
				self.warn_deprecated_extension(n.b, n.start);
				RemoveStatement::Module(RemoveModuleStatement {
					name: self.module_name(rec)?,
					if_exists,
				})
			}
			x if x == K::Access as u8 => RemoveStatement::Access(RemoveAccessStatement {
				name: name()?,
				base: Self::base(rec[rm::ON]),
				if_exists,
			}),
			x if x == K::User as u8 => RemoveStatement::User(RemoveUserStatement {
				name: name()?,
				base: Self::base(rec[rm::ON]),
				if_exists,
			}),
			x if x == K::Param as u8 => RemoveStatement::Param(RemoveParamStatement {
				name: Ident::new(Strand::new(self.param_name(rec[rm::NAME])?)),
				if_exists,
			}),
			x if x == K::Event as u8 => RemoveStatement::Event(RemoveEventStatement {
				name: name()?,
				table_name: on()?,
				if_exists,
			}),
			x if x == K::Field as u8 => RemoveStatement::Field(RemoveFieldStatement {
				name: name()?,
				table_name: on()?,
				if_exists,
			}),
			x if x == K::Index as u8 => RemoveStatement::Index(RemoveIndexStatement {
				name: name()?,
				what: on()?,
				if_exists,
			}),
			x if x == K::Analyzer as u8 => RemoveStatement::Analyzer(RemoveAnalyzerStatement {
				name: name()?,
				if_exists,
			}),
			x if x == K::Sequence as u8 => RemoveStatement::Sequence(RemoveSequenceStatement {
				name: name()?,
				if_exists,
			}),
			x if x == K::Api as u8 => RemoveStatement::Api(RemoveApiStatement {
				name: name()?,
				if_exists,
			}),
			x if x == K::Bucket as u8 => RemoveStatement::Bucket(RemoveBucketStatement {
				name: name()?,
				if_exists,
			}),
			_ => RemoveStatement::Config(RemoveConfigStatement {
				kind: match rec[rm::ON] {
					0 => surrealdb_iam::ConfigKind::GraphQL,
					1 => surrealdb_iam::ConfigKind::Api,
					_ => surrealdb_iam::ConfigKind::Default,
				},
				if_exists,
			}),
		})
	}
}

impl Lower<'_> {
	fn filters(&self, c: &Node) -> R<Vec<Filter>> {
		let mut fs = Vec::new();
		for &f in self.list(c.a) {
			let f = self.n(f);
			fs.push(match f.a {
				0 => Filter::Ascii,
				1 => Filter::Lowercase,
				2 => Filter::Uppercase,
				3 | 4 => {
					let r = self.rec(f.b, 2);
					let (x, y) = (self.uint(r[0])?, self.uint(r[1])?);
					if f.a == 3 {
						Filter::EdgeNgram(x, y)
					} else {
						Filter::Ngram(x, y)
					}
				}
				5 => Filter::Snowball(language(self.text(f.b))),
				_ => Filter::Mapper(self.string(f.b)?),
			});
		}
		Ok(fs)
	}

	fn tokenizers(&self, c: &Node) -> Vec<Tokenizer> {
		self.list(c.a)
			.iter()
			.map(|&t| {
				let tn = self.n(t);
				if tn.op == 1 {
					return Tokenizer::Segment(
						match self.text(tn.a).to_ascii_uppercase().as_str() {
							"CHINESE" => SegmentLanguage::Chinese,
							"JAPANESE" => SegmentLanguage::Japanese,
							_ => SegmentLanguage::Korean,
						},
					);
				}
				match self.text(t).to_ascii_uppercase().as_str() {
					"BLANK" => Tokenizer::Blank,
					"CAMEL" => Tokenizer::Camel,
					"CLASS" => Tokenizer::Class,
					_ => Tokenizer::Punct,
				}
			})
			.collect()
	}

	/// The stored form of an analyzer's `FUNCTION` reference: the path of a
	/// `fn::` function, or `mod::` and the path of an extension function,
	/// whichever prefix the source spelled.
	fn analyzer_function(&self, c: &Node) -> R<String> {
		let f = self.n(c.a);
		let path = self.path(c.a)?;
		if f.op == 1 {
			Ok(path)
		} else {
			if !self.opts.surrealism {
				return Err(Reject::new(SURREALISM_DISABLED));
			}
			self.warn_deprecated_extension(f.b, f.start);
			let path: String = path.split_whitespace().collect();
			let rest = path.split_once("::").map_or(path.as_str(), |(_, rest)| rest);
			Ok(format!("mod::{rest}"))
		}
	}

	/// Rejects a `DEFINE`, `ALTER` or `REMOVE` of a kind whose feature is off.
	fn gate_kind(&self, kind: u8) -> R<()> {
		if kind == K::Bucket as u8 && !self.opts.files {
			return Err(Reject::new(FILES_DISABLED));
		}
		if kind == K::Module as u8 && !self.opts.surrealism {
			return Err(Reject::new(SURREALISM_DISABLED));
		}
		Ok(())
	}
}

impl Lower<'_> {
	fn str_alter(&self, c: &Node) -> R<AlterKind<String>> {
		Ok(AlterKind::Set(self.string(c.a)?))
	}

	fn dur_alter(&self, c: &Node) -> R<AlterKind<Option<Duration>>> {
		Ok(if c.a == NONE {
			AlterKind::Set(None)
		} else {
			AlterKind::Set(Some(self.duration(c.a)?))
		})
	}

	/// The extension name in the `NAME` and `ON` slots shared by the `ALTER`
	/// and `REMOVE` records.
	fn module_name(&self, rec: &[u32]) -> R<ModuleName> {
		Ok(if rec[2] == 1 {
			let l = self.list(rec[1]);
			ModuleName::Silo(
				self.name(l[0])?,
				self.name(l[1])?,
				self.uint(l[2])?,
				self.uint(l[3])?,
				self.uint(l[4])?,
			)
		} else {
			ModuleName::Module(self.name(rec[1])?)
		})
	}

	pub fn alter(&self, n: &Node) -> R<AlterStatement> {
		let rec = self.rec(n.a, 4);
		let if_exists = rec[0] == 1;
		let clauses: Vec<&Node> = self.list(rec[3]).iter().map(|&c| self.n(c)).collect();
		let drop = |c: &Node| (unsafe_cl(c.op) == Cl::AlterDrop).then(|| unsafe_cl(c.a as u8));
		let name = || self.expr(rec[1]);
		let on = || self.expr(rec[2]);
		let kd = n.op;
		if kd != ALTER_SYSTEM {
			self.gate_kind(kd)?;
		}
		Ok(match kd {
			ALTER_SYSTEM => {
				let mut s = AlterSystemStatement::default();
				for c in clauses {
					match (unsafe_cl(c.op), drop(c)) {
						(_, Some(_)) => s.query_timeout = AlterKind::Drop,
						(Cl::Compact, _) => s.compact = true,
						_ => s.query_timeout = AlterKind::Set(self.expr(c.a)?),
					}
				}
				AlterStatement::System(s)
			}
			x if x == K::Namespace as u8 => AlterStatement::Namespace(AlterNamespaceStatement {
				compact: true,
			}),
			x if x == K::Database as u8 => AlterStatement::Database(AlterDatabaseStatement {
				compact: true,
			}),
			x if x == K::Table as u8 => {
				let mut s = AlterTableStatement {
					name: name()?,
					if_exists,
					..Default::default()
				};
				for c in clauses {
					match (unsafe_cl(c.op), drop(c)) {
						(_, Some(Cl::Comment)) => s.comment = AlterKind::Drop,
						(_, Some(Cl::Changefeed)) => s.changefeed = AlterKind::Drop,
						(_, Some(Cl::InlineEdges)) => s.inline_edges_cap = AlterKind::Drop,
						(_, Some(_)) => s.inline_refs_cap = AlterKind::Drop,
						(Cl::Comment, _) => s.comment = self.str_alter(c)?,
						(Cl::Compact, _) => s.compact = true,
						(Cl::InlineEdges, _) => {
							s.inline_edges_cap = AlterKind::Set(self.uint(c.a)?)
						}
						(Cl::InlineRefs, _) => s.inline_refs_cap = AlterKind::Set(self.uint(c.a)?),
						(Cl::TypeNormal, _) => s.kind = Some(TableType::Normal),
						(Cl::TypeAny, _) => s.kind = Some(TableType::Any),
						(Cl::TypeRelation, _) => {
							let r = self.rec(c.a, 3);
							let names = |l: u32| -> R<Vec<TableName>> {
								if l == NONE {
									return Ok(Vec::new());
								}
								self.list(l).iter().map(|&t| self.name(t).map(table)).collect()
							};
							s.kind = Some(TableType::Relation(table_type::Relation {
								from: names(r[0])?,
								to: names(r[1])?,
								enforced: r[2] & 1 != 0,
								lightweight: r[2] & 2 != 0,
							}));
						}
						(Cl::Schemaless, _) => s.schemafull = AlterKind::Drop,
						(Cl::Schemafull, _) => s.schemafull = AlterKind::Set(()),
						(Cl::Permissions, _) => s.permissions = Some(self.permissions(c)?),
						_ => s.changefeed = AlterKind::Set(self.changefeed(c)?),
					}
				}
				AlterStatement::Table(s)
			}
			x if x == K::Event as u8 => {
				let mut s = AlterEventStatement {
					name: name()?,
					what: on()?,
					if_exists,
					..Default::default()
				};
				for c in clauses {
					match (unsafe_cl(c.op), drop(c)) {
						(_, Some(Cl::When)) => s.when = AlterKind::Drop,
						(_, Some(Cl::Then)) => s.then = AlterKind::Drop,
						(_, Some(Cl::Comment)) => s.comment = AlterKind::Drop,
						(_, Some(_)) => s.kind = AlterKind::Drop,
						(Cl::When, _) => s.when = AlterKind::Set(self.expr(c.a)?),
						(Cl::Then, _) => s.then = AlterKind::Set(self.exprs(c.a)?),
						(Cl::Comment, _) => s.comment = self.str_alter(c)?,
						(Cl::Async, _) => {
							s.kind = AlterKind::Set(EventKind::Async {
								retry: EventKind::DEFAULT_RETRY,
								max_depth: EventKind::DEFAULT_MAX_DEPTH,
							})
						}
						(Cl::Retry, _) => match &mut s.kind {
							AlterKind::Set(EventKind::Async {
								retry,
								..
							}) => *retry = self.uint(c.a)?,
							_ => return Err(after_async(c, "RETRY")),
						},
						_ => match &mut s.kind {
							AlterKind::Set(EventKind::Async {
								max_depth,
								..
							}) => *max_depth = self.uint(c.a)?,
							_ => return Err(after_async(c, "MAXDEPTH")),
						},
					}
				}
				AlterStatement::Event(s)
			}
			x if x == K::Index as u8 => {
				let mut s = AlterIndexStatement {
					name: name()?,
					table: on()?,
					if_exists,
					..Default::default()
				};
				for c in clauses {
					match (unsafe_cl(c.op), drop(c)) {
						(_, Some(_)) => s.comment = AlterKind::Drop,
						(Cl::Comment, _) => s.comment = self.str_alter(c)?,
						_ => s.prepare_remove = true,
					}
				}
				if !s.prepare_remove && matches!(s.comment, AlterKind::None) {
					return Err(self.unexpected_after(n.end, "`PREPARE`, `DROP` or `COMMENT`"));
				}
				AlterStatement::Index(s)
			}
			x if x == K::Field as u8 => {
				let mut s = AlterFieldStatement {
					name: name()?,
					what: on()?,
					if_exists,
					..Default::default()
				};
				let is_id = matches!(&s.name, Expr::Idiom(i) if i.is_id());
				for c in clauses {
					match (unsafe_cl(c.op), drop(c)) {
						(_, Some(Cl::FieldType)) => {
							s.kind = AlterKind::Drop;
							s.key_names = AlterKind::Drop;
						}
						(_, Some(Cl::Flexible)) => s.flexible = AlterKind::Drop,
						(_, Some(Cl::Readonly)) => s.readonly = AlterKind::Drop,
						(_, Some(Cl::Inline)) => s.inline = AlterKind::Drop,
						(_, Some(Cl::Value)) => s.value = AlterKind::Drop,
						(_, Some(Cl::Assert)) => s.assert = AlterKind::Drop,
						(_, Some(Cl::Default)) => s.default = AlterDefault::Drop,
						(_, Some(Cl::Comment)) => s.comment = AlterKind::Drop,
						(_, Some(_)) => s.reference = AlterKind::Drop,
						(Cl::FieldType, _) => {
							let (k, names) = self.field_type(c.a, is_id)?;
							if c.b == 1 {
								if !kind_contains_object(&k) {
									let (a, b) = self.last_tok(c.start, c.end);
									return Err(Reject::new(
										"FLEXIBLE can only be used with types containing object",
									)
									.at(a, b));
								}
								s.flexible = AlterKind::Set(());
							}
							let names = names.map_or(AlterKind::Drop, AlterKind::Set);
							s.set_type(AlterKind::Set(k), names)
								.map_err(|e| Reject::new(e.to_string()).at(c.start, c.end))?;
						}
						(Cl::Flexible, _) => s.flexible = AlterKind::Set(()),
						(Cl::Readonly, _) => s.readonly = AlterKind::Set(()),
						(Cl::Inline, _) => s.inline = AlterKind::Set(()),
						(Cl::Value, _) => s.value = AlterKind::Set(self.expr(c.a)?),
						(Cl::Assert, _) => s.assert = AlterKind::Set(self.expr(c.a)?),
						(Cl::Default, _) => {
							let e = self.expr(c.a)?;
							s.default = if c.b == 1 {
								AlterDefault::Always(e)
							} else {
								AlterDefault::Set(e)
							};
						}
						(Cl::Permissions, _) => s.permissions = Some(self.permissions(c)?),
						(Cl::Comment, _) => s.comment = self.str_alter(c)?,
						_ => {
							s.reference = AlterKind::Set(Reference {
								on_delete: match c.a {
									0 => ReferenceDeleteStrategy::Reject,
									1 => ReferenceDeleteStrategy::Cascade,
									3 => ReferenceDeleteStrategy::Unset,
									4 => ReferenceDeleteStrategy::Custom(self.expr(c.b)?),
									_ => ReferenceDeleteStrategy::Ignore,
								},
							})
						}
					}
				}
				AlterStatement::Field(Box::new(s))
			}
			x if x == K::Param as u8 => {
				let mut s = AlterParamStatement {
					name: Strand::new(self.param_name(rec[1])?),
					if_exists,
					..Default::default()
				};
				for c in clauses {
					match (unsafe_cl(c.op), drop(c)) {
						(_, Some(_)) => s.comment = AlterKind::Drop,
						(Cl::Value, _) => s.value = Some(self.expr(c.a)?),
						(Cl::Comment, _) => s.comment = self.str_alter(c)?,
						_ => s.permissions = Some(self.perm(c.a)?),
					}
				}
				AlterStatement::Param(s)
			}
			x if x == K::Sequence as u8 => {
				let mut s = AlterSequenceStatement {
					name: name()?,
					if_exists,
					..Default::default()
				};
				for c in clauses {
					s.timeout = Some(self.expr(c.a)?);
				}
				AlterStatement::Sequence(s)
			}
			x if x == K::Bucket as u8 => {
				let mut s = AlterBucketStatement {
					name: name()?,
					if_exists,
					..Default::default()
				};
				for c in clauses {
					match (unsafe_cl(c.op), drop(c)) {
						(_, Some(Cl::Readonly)) => s.readonly = AlterKind::Drop,
						(_, Some(Cl::Backend)) => s.backend = AlterKind::Drop,
						(_, Some(_)) => s.comment = AlterKind::Drop,
						(Cl::Readonly, _) => s.readonly = AlterKind::Set(()),
						(Cl::Backend, _) => s.backend = self.str_alter(c)?,
						(Cl::PermValue, _) => s.permissions = Some(self.perm(c.a)?),
						_ => s.comment = self.str_alter(c)?,
					}
				}
				AlterStatement::Bucket(s)
			}
			x if x == K::Analyzer as u8 => {
				let mut s = AlterAnalyzerStatement {
					name: name()?,
					if_exists,
					..Default::default()
				};
				for c in clauses {
					match (unsafe_cl(c.op), drop(c)) {
						(_, Some(Cl::Function)) => s.function = AlterKind::Drop,
						(_, Some(Cl::Tokenizers)) => s.tokenizers = AlterKind::Drop,
						(_, Some(Cl::Filters)) => s.filters = AlterKind::Drop,
						(_, Some(_)) => s.comment = AlterKind::Drop,
						(Cl::Function, _) => {
							s.function = AlterKind::Set(self.analyzer_function(c)?)
						}
						(Cl::Tokenizers, _) => s.tokenizers = AlterKind::Set(self.tokenizers(c)),
						(Cl::Filters, _) => s.filters = AlterKind::Set(self.filters(c)?),
						_ => s.comment = self.str_alter(c)?,
					}
				}
				AlterStatement::Analyzer(s)
			}
			x if x == K::Function as u8 => {
				let mut s = AlterFunctionStatement {
					name: Strand::new(self.path(rec[1])?),
					if_exists,
					..Default::default()
				};
				for c in clauses {
					match (unsafe_cl(c.op), drop(c)) {
						(_, Some(_)) => s.comment = AlterKind::Drop,
						(Cl::Signature, _) => {
							let r = self.rec(c.a, 3);
							s.args = AlterKind::Set(
								self.list(r[0])
									.chunks(2)
									.map(|pk| Ok((self.param_name(pk[0])?, self.kind(pk[1])?)))
									.collect::<R<_>>()?,
							);
							if r[1] != NONE {
								s.returns = AlterKind::Set(self.kind(r[1])?);
							}
							s.block = AlterKind::Set(match self.expr(r[2])? {
								Expr::Block(b) => *b,
								_ => return Err(Reject::new("not a block")),
							});
						}
						(Cl::Comment, _) => s.comment = self.str_alter(c)?,
						_ => s.permissions = Some(self.perm(c.a)?),
					}
				}
				AlterStatement::Function(s)
			}

			x if x == K::User as u8 => {
				let mut s = AlterUserStatement {
					name: name()?,
					base: Self::base(rec[2]),
					if_exists,
					..Default::default()
				};
				let mut explicit_scram = None;
				for c in clauses {
					match (unsafe_cl(c.op), drop(c)) {
						(_, Some(_)) => s.comment = AlterKind::Drop,
						(Cl::Password, _) => {
							s.pending_password = Some(PendingPassword(self.string(c.a)?));
							s.hash = None;
						}
						(Cl::Passhash, _) => {
							s.hash = Some(self.string(c.a)?);
							s.pending_password = None;
						}
						(Cl::Passscram, _) => {
							explicit_scram =
								Some(self.scram_credential(&self.string(c.a)?, c.start)?)
						}
						(Cl::Roles, _) => {
							s.roles = AlterKind::Set(
								self.list(c.a).iter().map(|&r| self.name(r)).collect::<R<_>>()?,
							)
						}
						(Cl::DurToken, _) => s.token_duration = self.dur_alter(c)?,
						(Cl::DurSession, _) => s.session_duration = self.dur_alter(c)?,
						_ => s.comment = self.str_alter(c)?,
					}
				}
				s.scram = if let Some(scram) = explicit_scram {
					Some(Some(scram))
				} else if s.hash.is_some() {
					Some(None)
				} else {
					None
				};
				AlterStatement::User(s)
			}
			x if x == K::Access as u8 => {
				let mut s = AlterAccessStatement {
					name: name()?,
					base: Self::base(rec[2]),
					if_exists,
					..Default::default()
				};
				for c in clauses {
					match (unsafe_cl(c.op), drop(c)) {
						(_, Some(Cl::Authenticate)) => s.authenticate = AlterKind::Drop,
						(_, Some(Cl::Context)) => s.context = AlterKind::Drop,
						(_, Some(_)) => s.comment = AlterKind::Drop,
						(Cl::Authenticate, _) => s.authenticate = AlterKind::Set(self.expr(c.a)?),
						(Cl::Context, _) => s.context = AlterKind::Set(self.expr(c.a)?),
						(Cl::DurGrant, _) => s.grant_duration = self.dur_alter(c)?,
						(Cl::DurToken, _) => s.token_duration = self.dur_alter(c)?,
						(Cl::DurSession, _) => s.session_duration = self.dur_alter(c)?,
						_ => s.comment = self.str_alter(c)?,
					}
				}
				AlterStatement::Access(s)
			}
			x if x == K::Config as u8 => {
				let mut comment = AlterKind::None;
				let mut rest = Vec::new();
				for c in &clauses {
					match (unsafe_cl(c.op), drop(c)) {
						(_, Some(_)) => comment = AlterKind::Drop,
						(Cl::Comment, _) => comment = self.str_alter(c)?,
						_ => rest.push(*c),
					}
				}
				let inner = match rec[2] {
					0 => ConfigInner::Api(self.api_config(self.list(rec[3])[0])?),
					1 => {
						let gql = rest.first().map(|c| self.list(c.a)).unwrap_or(&[]);
						let gql: Vec<&Node> = gql.iter().map(|&c| self.n(c)).collect();
						ConfigInner::GraphQL(self.graphql(&gql)?)
					}
					_ => {
						let mut d = DefaultConfig::default();
						for c in rest {
							match unsafe_cl(c.op) {
								Cl::DefaultNs => d.namespace = self.expr(c.a)?,
								_ => d.database = self.expr(c.a)?,
							}
						}
						ConfigInner::Default(d)
					}
				};
				AlterStatement::Config(AlterConfigStatement {
					if_exists,
					inner,
					comment,
				})
			}
			x if x == K::Api as u8 => {
				let mut s = AlterApiStatement {
					path: name()?,
					if_exists,
					..Default::default()
				};
				let methods = |l: u32| -> Vec<ApiMethod> {
					self.list(l)
						.iter()
						.map(|&m| match self.text(m).to_ascii_uppercase().as_str() {
							"DELETE" => ApiMethod::Delete,
							"GET" => ApiMethod::Get,
							"PATCH" => ApiMethod::Patch,
							"POST" => ApiMethod::Post,
							"PUT" => ApiMethod::Put,
							_ => ApiMethod::Trace,
						})
						.collect()
				};
				for c in clauses {
					match (unsafe_cl(c.op), drop(c)) {
						(_, Some(_)) => s.comment = AlterKind::Drop,
						(Cl::Comment, _) => s.comment = self.str_alter(c)?,
						(Cl::ApiAny, _) => {
							let config = self.api_config(c.a)?;
							let is_default = config == ApiConfig::default();
							s.clauses.push(AlterApiClause::ForAny {
								config: (!is_default).then_some(config),
								fallback: if c.b == NONE {
									AlterKind::None
								} else if c.b == NONE - 1 {
									AlterKind::Drop
								} else {
									AlterKind::Set(self.expr(c.b)?)
								},
							});
						}
						(Cl::ApiDropAction, _) => s.clauses.push(AlterApiClause::DropAction {
							methods: methods(c.a),
						}),
						_ => {
							let r = self.rec(c.a, 3);
							s.clauses.push(AlterApiClause::SetAction(ApiAction {
								methods: methods(r[0]),
								config: self.api_config(r[1])?,
								action: self.expr(r[2])?,
							}));
						}
					}
				}
				AlterStatement::Api(s)
			}
			_ => {
				self.warn_deprecated_extension(n.b, n.start);
				let mut s = AlterModuleStatement {
					name: self.module_name(rec)?,
					if_exists,
					comment: AlterKind::None,
					permissions: None,
				};
				for c in clauses {
					match (unsafe_cl(c.op), drop(c)) {
						(_, Some(_)) => s.comment = AlterKind::Drop,
						(Cl::Comment, _) => s.comment = self.str_alter(c)?,
						_ => s.permissions = Some(self.perm(c.a)?),
					}
				}
				AlterStatement::Module(s)
			}
		})
	}
}
