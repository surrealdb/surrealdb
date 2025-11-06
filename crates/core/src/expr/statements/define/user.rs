use std::fmt::{self, Display};

use anyhow::{Result, bail};
use argon2::Argon2;
use argon2::password_hash::{PasswordHasher, SaltString};
use rand::Rng as _;
use rand::distributions::Alphanumeric;
use rand::rngs::OsRng;
use reblessive::tree::Stk;
use surrealdb_types::{ToSql, write_sql};

use super::DefineKind;
use crate::catalog::providers::{CatalogProvider, NamespaceProvider, UserProvider};
use crate::catalog::{self, UserDefinition};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::expression::VisitExpression;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::user::UserDuration;
use crate::expr::{Base, Expr, Idiom, Literal};
use crate::fmt::{Fmt, QuoteStr};
use crate::iam::{Action, ResourceKind};
use crate::val::{self, Duration, Value};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct DefineUserStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub base: Base,
	pub hash: String,
	pub code: String,
	pub roles: Vec<String>,
	pub duration: UserDuration,
	pub comment: Option<Expr>,
}

impl VisitExpression for DefineUserStatement {
	fn visit<F>(&self, visitor: &mut F)
	where
		F: FnMut(&Expr),
	{
		self.name.visit(visitor);
		self.comment.iter().for_each(|expr| expr.visit(visitor));
	}
}

impl Default for DefineUserStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			name: Expr::Literal(Literal::None),
			base: Base::Root,
			hash: String::new(),
			code: String::new(),
			roles: vec![],
			duration: UserDuration::default(),
			comment: None,
		}
	}
}

impl DefineUserStatement {
	pub(crate) fn new_with_password(base: Base, user: String, pass: &str, role: String) -> Self {
		DefineUserStatement {
			kind: DefineKind::Default,
			base,
			name: Expr::Idiom(Idiom::field(user)),
			hash: Argon2::default()
				.hash_password(pass.as_ref(), &SaltString::generate(&mut OsRng))
				.unwrap()
				.to_string(),
			code: rand::thread_rng()
				.sample_iter(&Alphanumeric)
				.take(128)
				.map(char::from)
				.collect::<String>(),
			roles: vec![role],
			duration: UserDuration::default(),
			comment: None,
		}
	}

	pub(crate) async fn to_definition(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<catalog::UserDefinition> {
		Ok(UserDefinition {
			name: expr_to_ident(stk, ctx, opt, doc, &self.name, "user name").await?,
			hash: self.hash.clone(),
			code: self.code.clone(),
			roles: self.roles.clone(),
			token_duration: map_opt!(x as &self.duration.token => compute_to!(stk, ctx, opt, doc, x => Duration).0),
			session_duration: map_opt!(x as &self.duration.session => compute_to!(stk, ctx, opt, doc, x => Duration).0),
			comment: map_opt!(x as &self.comment => compute_to!(stk, ctx, opt, doc, x => String)),
			base: self.base.into(),
		})
	}

	pub fn from_definition(base: Base, def: &catalog::UserDefinition) -> Self {
		Self {
			kind: DefineKind::Default,
			base,
			name: Expr::Idiom(Idiom::field(def.name.clone())),
			hash: def.hash.clone(),
			code: def.code.clone(),
			roles: def.roles.clone(),
			duration: UserDuration {
				token: def
					.token_duration
					.map(|x| Expr::Literal(Literal::Duration(val::Duration(x)))),
				session: def
					.session_duration
					.map(|x| Expr::Literal(Literal::Duration(val::Duration(x)))),
			},
			comment: def.comment.as_ref().map(|x| Expr::Idiom(Idiom::field(x.clone()))),
		}
	}

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
		// Compute definition
		let definition = self.to_definition(stk, ctx, opt, doc).await?;
		// Check the statement type
		match self.base {
			Base::Root => {
				// Fetch the transaction
				let txn = ctx.tx();
				// Check if the definition exists
				if let Some(user) = txn.get_root_user(&definition.name).await? {
					match self.kind {
						DefineKind::Default => {
							if !opt.import {
								bail!(Error::UserRootAlreadyExists {
									name: user.name.to_string(),
								});
							}
						}
						DefineKind::Overwrite => {}
						DefineKind::IfNotExists => return Ok(Value::None),
					}
				}
				// Process the statement
				txn.put_root_user(&definition).await?;
				// Clear the cache
				txn.clear_cache();
				// Ok all good
				Ok(Value::None)
			}
			Base::Ns => {
				// Fetch the transaction
				let txn = ctx.tx();
				let ns = ctx.get_ns_id(opt).await?;
				// Check if the definition exists
				if let Some(user) = txn.get_ns_user(ns, &definition.name).await? {
					match self.kind {
						DefineKind::Default => {
							if !opt.import {
								bail!(Error::UserNsAlreadyExists {
									name: user.name.to_string(),
									ns: opt.ns()?.into(),
								});
							}
						}
						DefineKind::Overwrite => {}
						DefineKind::IfNotExists => return Ok(Value::None),
					}
				}

				let ns = {
					let ns = opt.ns()?;
					txn.get_or_add_ns(Some(ctx), ns, opt.strict).await?
				};

				// Process the statement
				txn.put_ns_user(ns.namespace_id, &definition).await?;
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
				if let Some(user) = txn.get_db_user(ns, db, &definition.name).await? {
					match self.kind {
						DefineKind::Default => {
							if !opt.import {
								bail!(Error::UserDbAlreadyExists {
									name: user.name.to_string(),
									ns: opt.ns()?.to_string(),
									db: opt.db()?.to_string(),
								});
							}
						}
						DefineKind::Overwrite => {}
						DefineKind::IfNotExists => return Ok(Value::None),
					}
				}

				let db = {
					let (ns, db) = opt.ns_db()?;
					txn.get_or_add_db(Some(ctx), ns, db, opt.strict).await?
				};

				// Process the statement
				txn.put_db_user(db.namespace_id, db.database_id, &definition).await?;
				// Clear the cache
				txn.clear_cache();
				// Ok all good
				Ok(Value::None)
			}
		}
	}
}

impl Display for DefineUserStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE USER")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(
			f,
			" {} ON {} PASSHASH {} ROLES {}",
			self.name,
			self.base,
			QuoteStr(&self.hash),
			Fmt::comma_separated(
				&self.roles.iter().map(|r| r.to_string().to_uppercase()).collect::<Vec<_>>()
			),
		)?;
		// Always print relevant durations so defaults can be changed in the future
		// If default values were not printed, exports would not be forward compatible
		// None values need to be printed, as they are different from the default values
		write!(f, " DURATION")?;
		write!(
			f,
			" FOR TOKEN {},",
			match self.duration.token {
				Some(ref dur) => format!("{}", dur),
				None => "NONE".to_string(),
			}
		)?;
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

impl ToSql for DefineUserStatement {
	fn fmt_sql(&self, f: &mut String) {
		write_sql!(f, "{}", self)
	}
}
