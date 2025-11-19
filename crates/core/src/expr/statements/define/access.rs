use std::fmt::{self, Display};

use anyhow::{Result, bail};
use rand::Rng;
use rand::distributions::Alphanumeric;
use reblessive::tree::Stk;
use surrealdb_types::{ToSql, write_sql};

use super::DefineKind;
use crate::catalog::providers::{AuthorisationProvider, NamespaceProvider};
use crate::catalog::{self, AccessDefinition};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::access::AccessDuration;
use crate::expr::access_type::{
	BearerAccess, BearerAccessSubject, BearerAccessType, JwtAccessIssue, JwtAccessVerify,
	JwtAccessVerifyJwks, JwtAccessVerifyKey,
};
use crate::expr::parameterize::expr_to_ident;
use crate::expr::{AccessType, Algorithm, Base, Expr, Idiom, JwtAccess, Literal, RecordAccess};
use crate::iam::{Action, ResourceKind};
use crate::val::{self, Value};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct DefineAccessStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub base: Base,
	pub access_type: AccessType,
	pub authenticate: Option<Expr>,
	pub duration: AccessDuration,
	pub comment: Option<Expr>,
}

impl Default for DefineAccessStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			name: Expr::Literal(Literal::None),
			base: Base::Root,
			access_type: AccessType::default(),
			authenticate: None,
			duration: AccessDuration::default(),
			comment: None,
		}
	}
}

impl DefineAccessStatement {
	/// Generate a random key to be used to sign session tokens
	/// This key will be used to sign tokens issued with this access method
	/// This value is used by default in every access method other than JWT
	pub(crate) fn random_key() -> String {
		rand::thread_rng().sample_iter(&Alphanumeric).take(128).map(char::from).collect::<String>()
	}

	pub fn from_definition(base: Base, def: &AccessDefinition) -> Self {
		fn convert_algorithm(access: &catalog::Algorithm) -> Algorithm {
			match access {
				catalog::Algorithm::EdDSA => Algorithm::EdDSA,
				catalog::Algorithm::Es256 => Algorithm::Es256,
				catalog::Algorithm::Es384 => Algorithm::Es384,
				catalog::Algorithm::Es512 => Algorithm::Es512,
				catalog::Algorithm::Hs256 => Algorithm::Hs256,
				catalog::Algorithm::Hs384 => Algorithm::Hs384,
				catalog::Algorithm::Hs512 => Algorithm::Hs512,
				catalog::Algorithm::Ps256 => Algorithm::Ps256,
				catalog::Algorithm::Ps384 => Algorithm::Ps384,
				catalog::Algorithm::Ps512 => Algorithm::Ps512,
				catalog::Algorithm::Rs256 => Algorithm::Rs256,
				catalog::Algorithm::Rs384 => Algorithm::Rs384,
				catalog::Algorithm::Rs512 => Algorithm::Rs512,
			}
		}

		fn convert_jwt_access(access: &catalog::JwtAccess) -> JwtAccess {
			JwtAccess {
				verify: match &access.verify {
					catalog::JwtAccessVerify::Key(k) => JwtAccessVerify::Key(JwtAccessVerifyKey {
						alg: convert_algorithm(&k.alg),
						key: Expr::Literal(Literal::String(k.key.clone())),
					}),
					catalog::JwtAccessVerify::Jwks(j) => {
						JwtAccessVerify::Jwks(JwtAccessVerifyJwks {
							url: Expr::Literal(Literal::String(j.url.clone())),
						})
					}
				},
				issue: access.issue.as_ref().map(|x| JwtAccessIssue {
					alg: convert_algorithm(&x.alg),
					key: Expr::Literal(Literal::String(x.key.clone())),
				}),
			}
		}

		fn convert_bearer_access(access: &catalog::BearerAccess) -> BearerAccess {
			BearerAccess {
				kind: match access.kind {
					catalog::BearerAccessType::Bearer => BearerAccessType::Bearer,
					catalog::BearerAccessType::Refresh => BearerAccessType::Refresh,
				},
				subject: match access.subject {
					catalog::BearerAccessSubject::Record => BearerAccessSubject::Record,
					catalog::BearerAccessSubject::User => BearerAccessSubject::User,
				},
				jwt: convert_jwt_access(&access.jwt),
			}
		}

		DefineAccessStatement {
			kind: DefineKind::Default,
			base,
			name: Expr::Idiom(Idiom::field(def.name.clone())),
			duration: AccessDuration {
				grant: def
					.grant_duration
					.map(|v| Expr::Literal(Literal::Duration(val::Duration(v)))),
				token: def
					.token_duration
					.map(|v| Expr::Literal(Literal::Duration(val::Duration(v)))),
				session: def
					.session_duration
					.map(|v| Expr::Literal(Literal::Duration(val::Duration(v)))),
			},
			comment: def.comment.clone().map(|x| Expr::Literal(Literal::String(x))),
			authenticate: def.authenticate.clone(),
			access_type: match &def.access_type {
				catalog::AccessType::Record(record_access) => AccessType::Record(RecordAccess {
					signup: record_access.signup.clone(),
					signin: record_access.signin.clone(),
					jwt: convert_jwt_access(&record_access.jwt),
					bearer: record_access.bearer.as_ref().map(convert_bearer_access),
				}),
				catalog::AccessType::Jwt(jwt_access) => {
					AccessType::Jwt(convert_jwt_access(jwt_access))
				}
				catalog::AccessType::Bearer(bearer_access) => {
					AccessType::Bearer(convert_bearer_access(bearer_access))
				}
			},
		}
	}

	async fn to_definition(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<AccessDefinition> {
		fn convert_algorithm(access: &Algorithm) -> catalog::Algorithm {
			match access {
				Algorithm::EdDSA => catalog::Algorithm::EdDSA,
				Algorithm::Es256 => catalog::Algorithm::Es256,
				Algorithm::Es384 => catalog::Algorithm::Es384,
				Algorithm::Es512 => catalog::Algorithm::Es512,
				Algorithm::Hs256 => catalog::Algorithm::Hs256,
				Algorithm::Hs384 => catalog::Algorithm::Hs384,
				Algorithm::Hs512 => catalog::Algorithm::Hs512,
				Algorithm::Ps256 => catalog::Algorithm::Ps256,
				Algorithm::Ps384 => catalog::Algorithm::Ps384,
				Algorithm::Ps512 => catalog::Algorithm::Ps512,
				Algorithm::Rs256 => catalog::Algorithm::Rs256,
				Algorithm::Rs384 => catalog::Algorithm::Rs384,
				Algorithm::Rs512 => catalog::Algorithm::Rs512,
			}
		}

		async fn convert_jwt_access(
			stk: &mut Stk,
			ctx: &Context,
			opt: &Options,
			doc: Option<&CursorDoc>,
			access: &JwtAccess,
		) -> Result<catalog::JwtAccess> {
			Ok(catalog::JwtAccess {
				verify: match &access.verify {
					JwtAccessVerify::Key(k) => {
						catalog::JwtAccessVerify::Key(catalog::JwtAccessVerifyKey {
							alg: convert_algorithm(&k.alg),
							key: compute_to!(stk, ctx, opt, doc, k.key => String),
						})
					}
					JwtAccessVerify::Jwks(j) => {
						catalog::JwtAccessVerify::Jwks(catalog::JwtAccessVerifyJwks {
							url: compute_to!(stk, ctx, opt, doc, j.url => String),
						})
					}
				},
				issue: map_opt!(x as &access.issue => catalog::JwtAccessIssue {
					alg: convert_algorithm(&x.alg),
					key: compute_to!(stk, ctx, opt, doc, x.key => String),
				}),
			})
		}

		async fn convert_bearer_access(
			stk: &mut Stk,
			ctx: &Context,
			opt: &Options,
			doc: Option<&CursorDoc>,
			access: &BearerAccess,
		) -> Result<catalog::BearerAccess> {
			Ok(catalog::BearerAccess {
				kind: match access.kind {
					BearerAccessType::Bearer => catalog::BearerAccessType::Bearer,
					BearerAccessType::Refresh => catalog::BearerAccessType::Refresh,
				},
				subject: match access.subject {
					BearerAccessSubject::Record => catalog::BearerAccessSubject::Record,
					BearerAccessSubject::User => catalog::BearerAccessSubject::User,
				},
				jwt: convert_jwt_access(stk, ctx, opt, doc, &access.jwt).await?,
			})
		}

		Ok(AccessDefinition {
			name: expr_to_ident(stk, ctx, opt, doc, &self.name, "access name").await?,
			base: self.base.into(),
			grant_duration: map_opt!(x as &self.duration.grant => compute_to!(stk, ctx, opt, doc, x => val::Duration).0),
			token_duration: map_opt!(x as &self.duration.token => compute_to!(stk, ctx, opt, doc, x => val::Duration).0),
			session_duration: map_opt!(x as &self.duration.session => compute_to!(stk, ctx, opt, doc, x => val::Duration).0),
			comment: map_opt!(x as &self.comment => compute_to!(stk, ctx, opt, doc, x => String)),
			authenticate: self.authenticate.clone(),
			access_type: match &self.access_type {
				AccessType::Record(record_access) => {
					catalog::AccessType::Record(catalog::RecordAccess {
						signup: record_access.signup.clone(),
						signin: record_access.signin.clone(),
						jwt: convert_jwt_access(stk, ctx, opt, doc, &record_access.jwt).await?,
						bearer: map_opt!(x as &record_access.bearer => convert_bearer_access(stk, ctx, opt, doc, x).await?),
					})
				}
				AccessType::Jwt(jwt_access) => catalog::AccessType::Jwt(
					convert_jwt_access(stk, ctx, opt, doc, jwt_access).await?,
				),
				AccessType::Bearer(bearer_access) => catalog::AccessType::Bearer(
					convert_bearer_access(stk, ctx, opt, doc, bearer_access).await?,
				),
			},
		})
	}
}

impl DefineAccessStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Actor, &self.base)?;
		// Compute the definition
		let definition = self.to_definition(stk, ctx, opt, doc).await?;
		// Check the statement type
		match &self.base {
			Base::Root => {
				// Fetch the transaction
				let txn = ctx.tx();
				// Check if access method already exists
				if let Some(access) = txn.get_root_access(&definition.name).await? {
					match self.kind {
						DefineKind::Default => {
							if !opt.import {
								bail!(Error::AccessRootAlreadyExists {
									ac: access.name.clone(),
								});
							}
						}
						DefineKind::Overwrite => {}
						DefineKind::IfNotExists => return Ok(Value::None),
					}
				}
				// Process the statement
				let key = crate::key::root::ac::new(&definition.name);
				txn.set(&key, &definition, None).await?;
				// Clear the cache
				txn.clear_cache();
				// Ok all good
				Ok(Value::None)
			}
			Base::Ns => {
				// Fetch the transaction
				let txn = ctx.tx();
				// Check if the definition exists
				let ns = ctx.get_ns_id(opt).await?;
				if let Some(access) = txn.get_ns_access(ns, &definition.name).await? {
					match self.kind {
						DefineKind::Default => {
							if !opt.import {
								bail!(Error::AccessNsAlreadyExists {
									ns: opt.ns()?.to_string(),
									ac: access.name.clone(),
								});
							}
						}
						DefineKind::Overwrite => {}
						DefineKind::IfNotExists => return Ok(Value::None),
					}
				}
				// Process the statement
				let key = crate::key::namespace::ac::new(ns, &definition.name);
				txn.get_or_add_ns(Some(ctx), opt.ns()?).await?;
				txn.set(&key, &definition, None).await?;
				// Clear the cache
				txn.clear_cache();
				// Ok all good
				Ok(Value::None)
			}
			Base::Db => {
				// Fetch the transaction
				let txn = ctx.tx();
				// Check if the definition exists
				let (ns, db) = ctx.get_ns_db_ids(opt).await?;
				if let Some(access) = txn.get_db_access(ns, db, &definition.name).await? {
					match self.kind {
						DefineKind::Default => {
							if !opt.import {
								bail!(Error::AccessDbAlreadyExists {
									ns: opt.ns()?.to_string(),
									db: opt.db()?.to_string(),
									ac: access.name.clone(),
								});
							}
						}
						DefineKind::Overwrite => {}
						DefineKind::IfNotExists => return Ok(Value::None),
					}
				}
				// Process the statement
				let key = crate::key::database::ac::new(ns, db, &definition.name);
				txn.set(&key, &definition, None).await?;
				// Clear the cache
				txn.clear_cache();
				// Ok all good
				Ok(Value::None)
			}
		}
	}

	/// Remove information from the access definition which should not be displayed.
	pub fn redact(mut self) -> Self {
		fn redact_jwt_access(acc: &mut JwtAccess) {
			if let JwtAccessVerify::Key(ref mut v) = acc.verify
				&& v.alg.is_symmetric()
			{
				v.key = Expr::Literal(Literal::String("[REDACTED]".to_string()));
			}
			if let Some(ref mut s) = acc.issue {
				s.key = Expr::Literal(Literal::String("[REDACTED]".to_string()));
			}
		}

		match self.access_type {
			AccessType::Jwt(ref mut key) => {
				redact_jwt_access(key);
			}
			AccessType::Bearer(ref mut b) => {
				redact_jwt_access(&mut b.jwt);
			}
			AccessType::Record(ref mut r) => {
				redact_jwt_access(&mut r.jwt);
				if let Some(ref mut b) = r.bearer {
					redact_jwt_access(&mut b.jwt);
				}
			}
		}
		self
	}
}

impl Display for DefineAccessStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE ACCESS",)?;
		match self.kind {
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::Default => {}
		}
		// The specific access method definition is displayed by AccessType
		write!(f, " {} ON {} TYPE {}", self.name, self.base, self.access_type)?;
		// The additional authentication clause
		if let Some(ref v) = self.authenticate {
			write!(f, " AUTHENTICATE {v}")?
		}
		// Always print relevant durations so defaults can be changed in the future
		// If default values were not printed, exports would not be forward compatible
		// None values need to be printed, as they are different from the default values
		write!(f, " DURATION")?;
		if self.access_type.can_issue_grants() {
			write!(
				f,
				" FOR GRANT {},",
				match self.duration.grant {
					Some(ref dur) => format!("{}", dur),
					None => "NONE".to_string(),
				}
			)?;
		}
		if self.access_type.can_issue_tokens() {
			write!(
				f,
				" FOR TOKEN {},",
				match self.duration.token {
					Some(ref dur) => format!("{}", dur),
					None => "NONE".to_string(),
				}
			)?;
		}
		write!(
			f,
			" FOR SESSION {}",
			match self.duration.session {
				Some(ref dur) => format!("{}", dur),
				None => "NONE".to_string(),
			}
		)?;
		if let Some(ref comment) = self.comment {
			write!(f, " COMMENT {}", comment)?
		}
		Ok(())
	}
}

impl ToSql for DefineAccessStatement {
	fn fmt_sql(&self, f: &mut String) {
		write_sql!(f, "{}", self)
	}
}
