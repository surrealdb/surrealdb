use std::ops::Deref;
use std::time::Duration;

use anyhow::Result;
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};
use tracing::instrument;

use super::AlterKind;
use crate::catalog;
use crate::catalog::providers::AuthorisationProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::{Base, Expr, Literal};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct AlterAccessStatement {
	pub name: Expr,
	pub base: Base,
	pub if_exists: bool,
	pub authenticate: AlterKind<Expr>,
	pub grant_duration: AlterKind<Option<Duration>>,
	pub token_duration: AlterKind<Option<Duration>>,
	pub session_duration: AlterKind<Option<Duration>>,
	pub comment: AlterKind<String>,
}

impl Default for AlterAccessStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			base: Base::Root,
			if_exists: false,
			authenticate: AlterKind::None,
			grant_duration: AlterKind::None,
			token_duration: AlterKind::None,
			session_duration: AlterKind::None,
			comment: AlterKind::None,
		}
	}
}

impl AlterAccessStatement {
	#[instrument(level = "trace", name = "AlterAccessStatement::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		ctx.is_allowed(opt, Action::Edit, ResourceKind::Access, self.base)?;
		let name = expr_to_ident(stk, ctx, opt, doc, &self.name, "access name").await?;

		match self.base {
			Base::Root => self.compute_root(ctx, &name).await,
			Base::Ns => self.compute_ns(ctx, opt, &name).await,
			Base::Db => self.compute_db(ctx, opt, &name).await,
		}
	}

	fn apply(&self, ac: &mut catalog::AccessDefinition) {
		match self.authenticate {
			AlterKind::Set(ref v) => ac.authenticate = Some(v.clone()),
			AlterKind::Drop => ac.authenticate = None,
			AlterKind::None => {}
		}
		match self.grant_duration {
			AlterKind::Set(v) => ac.grant_duration = v,
			AlterKind::Drop => ac.grant_duration = None,
			AlterKind::None => {}
		}
		match self.token_duration {
			AlterKind::Set(v) => ac.token_duration = v,
			AlterKind::Drop => ac.token_duration = None,
			AlterKind::None => {}
		}
		match self.session_duration {
			AlterKind::Set(v) => ac.session_duration = v,
			AlterKind::Drop => ac.session_duration = None,
			AlterKind::None => {}
		}
		match self.comment {
			AlterKind::Set(ref v) => ac.comment = Some(v.clone()),
			AlterKind::Drop => ac.comment = None,
			AlterKind::None => {}
		}
	}

	async fn compute_root(&self, ctx: &FrozenContext, name: &str) -> Result<Value> {
		let txn = ctx.tx();
		let mut ac = match txn.get_root_access(name, None).await? {
			Some(v) => v.deref().clone(),
			None => {
				if self.if_exists {
					return Ok(Value::None);
				}
				return Err(Error::AccessRootNotFound {
					ac: name.to_owned(),
				}
				.into());
			}
		};
		self.apply(&mut ac);
		let key = crate::key::root::ac::new(name);
		txn.set(&key, &ac).await?;
		txn.clear_cache();
		Ok(Value::None)
	}

	async fn compute_ns(&self, ctx: &FrozenContext, opt: &Options, name: &str) -> Result<Value> {
		let txn = ctx.tx();
		let ns = ctx.get_ns_id(opt).await?;
		let mut ac = match txn.get_ns_access(ns, name, None).await? {
			Some(v) => v.deref().clone(),
			None => {
				if self.if_exists {
					return Ok(Value::None);
				}
				return Err(Error::AccessNsNotFound {
					ac: name.to_owned(),
					ns: opt.ns()?.to_string(),
				}
				.into());
			}
		};
		self.apply(&mut ac);
		let key = crate::key::namespace::ac::new(ns, name);
		txn.set(&key, &ac).await?;
		txn.clear_cache();
		Ok(Value::None)
	}

	async fn compute_db(&self, ctx: &FrozenContext, opt: &Options, name: &str) -> Result<Value> {
		let txn = ctx.tx();
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		let mut ac = match txn.get_db_access(ns, db, name, None).await? {
			Some(v) => v.deref().clone(),
			None => {
				if self.if_exists {
					return Ok(Value::None);
				}
				let (ns_name, db_name) = opt.ns_db()?;
				return Err(Error::AccessDbNotFound {
					ac: name.to_owned(),
					ns: ns_name.to_string(),
					db: db_name.to_string(),
				}
				.into());
			}
		};
		self.apply(&mut ac);
		let key = crate::key::database::ac::new(ns, db, name);
		txn.set(&key, &ac).await?;
		txn.clear_cache();
		Ok(Value::None)
	}
}

impl ToSql for AlterAccessStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterAccessStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
