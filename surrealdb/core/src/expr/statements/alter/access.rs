use std::ops::Deref;
use std::time::Duration;

use anyhow::Result;
use surrealdb_types::{SqlFormat, ToSql};

use super::AlterKind;
use crate::catalog;
use crate::catalog::providers::AuthorisationProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::{Base, Expr};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct AlterAccessStatement {
	pub name: String,
	pub base: Base,
	pub if_exists: bool,
	pub authenticate: AlterKind<Expr>,
	pub grant_duration: AlterKind<Option<Duration>>,
	pub token_duration: AlterKind<Option<Duration>>,
	pub session_duration: AlterKind<Option<Duration>>,
	pub comment: AlterKind<String>,
}

impl AlterAccessStatement {
	#[instrument(level = "trace", name = "AlterAccessStatement::compute", skip_all)]
	pub(crate) async fn compute(&self, ctx: &FrozenContext, opt: &Options) -> Result<Value> {
		opt.is_allowed(Action::Edit, ResourceKind::Access, &self.base)?;

		match self.base {
			Base::Root => self.compute_root(ctx).await,
			Base::Ns => self.compute_ns(ctx, opt).await,
			Base::Db => self.compute_db(ctx, opt).await,
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

	async fn compute_root(&self, ctx: &FrozenContext) -> Result<Value> {
		let txn = ctx.tx();
		let mut ac = match txn.get_root_access(&self.name, None).await? {
			Some(v) => v.deref().clone(),
			None => {
				if self.if_exists {
					return Ok(Value::None);
				}
				return Err(Error::AccessRootNotFound {
					ac: self.name.clone(),
				}
				.into());
			}
		};
		self.apply(&mut ac);
		let key = crate::key::root::ac::new(&self.name);
		txn.set(&key, &ac).await?;
		txn.clear_cache();
		Ok(Value::None)
	}

	async fn compute_ns(&self, ctx: &FrozenContext, opt: &Options) -> Result<Value> {
		let txn = ctx.tx();
		let ns = ctx.get_ns_id(opt).await?;
		let mut ac = match txn.get_ns_access(ns, &self.name, None).await? {
			Some(v) => v.deref().clone(),
			None => {
				if self.if_exists {
					return Ok(Value::None);
				}
				return Err(Error::AccessNsNotFound {
					ac: self.name.clone(),
					ns: opt.ns()?.to_string(),
				}
				.into());
			}
		};
		self.apply(&mut ac);
		let key = crate::key::namespace::ac::new(ns, &self.name);
		txn.set(&key, &ac).await?;
		txn.clear_cache();
		Ok(Value::None)
	}

	async fn compute_db(&self, ctx: &FrozenContext, opt: &Options) -> Result<Value> {
		let txn = ctx.tx();
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		let mut ac = match txn.get_db_access(ns, db, &self.name, None).await? {
			Some(v) => v.deref().clone(),
			None => {
				if self.if_exists {
					return Ok(Value::None);
				}
				let (ns_name, db_name) = opt.ns_db()?;
				return Err(Error::AccessDbNotFound {
					ac: self.name.clone(),
					ns: ns_name.to_string(),
					db: db_name.to_string(),
				}
				.into());
			}
		};
		self.apply(&mut ac);
		let key = crate::key::database::ac::new(ns, db, &self.name);
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
