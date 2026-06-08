use std::ops::Deref;
use std::time::Duration;

use anyhow::Result;
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};
use tracing::instrument;

use super::AlterKind;
use crate::catalog;
use crate::catalog::providers::UserProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::{Base, Expr, Literal};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct AlterUserStatement {
	pub name: Expr,
	pub base: Base,
	pub if_exists: bool,
	pub hash: Option<String>,
	pub roles: AlterKind<Vec<String>>,
	pub token_duration: AlterKind<Option<Duration>>,
	pub session_duration: AlterKind<Option<Duration>>,
	pub comment: AlterKind<String>,
}

impl Default for AlterUserStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			base: Base::Root,
			if_exists: false,
			hash: None,
			roles: AlterKind::None,
			token_duration: AlterKind::None,
			session_duration: AlterKind::None,
			comment: AlterKind::None,
		}
	}
}

impl AlterUserStatement {
	#[instrument(level = "trace", name = "AlterUserStatement::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		ctx.is_allowed(opt, Action::Edit, ResourceKind::Actor, self.base)?;
		let name = expr_to_ident(stk, ctx, opt, doc, &self.name, "user name").await?;

		match self.base {
			Base::Root => self.compute_root(ctx, &name).await,
			Base::Ns => self.compute_ns(ctx, opt, &name).await,
			Base::Db => self.compute_db(ctx, opt, &name).await,
		}
	}

	fn apply(&self, user: &mut catalog::UserDefinition) {
		if let Some(ref h) = self.hash {
			user.hash.clone_from(h);
		}
		match self.roles {
			AlterKind::Set(ref v) => user.roles.clone_from(v),
			AlterKind::Drop => user.roles = vec![],
			AlterKind::None => {}
		}
		match self.token_duration {
			AlterKind::Set(v) => user.token_duration = v,
			AlterKind::Drop => user.token_duration = None,
			AlterKind::None => {}
		}
		match self.session_duration {
			AlterKind::Set(v) => user.session_duration = v,
			AlterKind::Drop => user.session_duration = None,
			AlterKind::None => {}
		}
		match self.comment {
			AlterKind::Set(ref v) => user.comment = Some(v.clone()),
			AlterKind::Drop => user.comment = None,
			AlterKind::None => {}
		}
	}

	async fn compute_root(&self, ctx: &FrozenContext, name: &str) -> Result<Value> {
		let txn = ctx.tx();
		let mut user = match txn.get_root_user(name, None).await? {
			Some(v) => v.deref().clone(),
			None => {
				if self.if_exists {
					return Ok(Value::None);
				}
				return Err(Error::UserRootNotFound {
					name: name.to_owned(),
				}
				.into());
			}
		};
		self.apply(&mut user);
		txn.put_root_user(&user).await?;
		txn.clear_cache();
		Ok(Value::None)
	}

	async fn compute_ns(&self, ctx: &FrozenContext, opt: &Options, name: &str) -> Result<Value> {
		let txn = ctx.tx();
		let ns = ctx.get_ns_id(opt).await?;
		let ns_name = opt.ns()?;
		let mut user = match txn.get_ns_user(ns, name, None).await? {
			Some(v) => v.deref().clone(),
			None => {
				if self.if_exists {
					return Ok(Value::None);
				}
				return Err(Error::UserNsNotFound {
					name: name.to_owned(),
					ns: ns_name.to_string(),
				}
				.into());
			}
		};
		self.apply(&mut user);
		txn.put_ns_user(ns, &user).await?;
		txn.clear_cache();
		Ok(Value::None)
	}

	async fn compute_db(&self, ctx: &FrozenContext, opt: &Options, name: &str) -> Result<Value> {
		let txn = ctx.tx();
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		let (ns_name, db_name) = opt.ns_db()?;
		let mut user = match txn.get_db_user(ns, db, name, None).await? {
			Some(v) => v.deref().clone(),
			None => {
				if self.if_exists {
					return Ok(Value::None);
				}
				return Err(Error::UserDbNotFound {
					name: name.to_owned(),
					ns: ns_name.to_string(),
					db: db_name.to_string(),
				}
				.into());
			}
		};
		self.apply(&mut user);
		txn.put_db_user(ns, db, &user).await?;
		txn.clear_cache();
		Ok(Value::None)
	}
}

impl ToSql for AlterUserStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterUserStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
