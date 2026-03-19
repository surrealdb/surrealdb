use std::ops::Deref;

use anyhow::Result;
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};

use super::AlterKind;
use crate::catalog::ApiActionDefinition;
use crate::catalog::providers::ApiProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::statements::define::ApiAction;
use crate::expr::statements::define::config::api::ApiConfig;
use crate::expr::{Base, Expr, Literal};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct AlterApiStatement {
	pub path: Expr,
	pub if_exists: bool,
	pub actions: Option<Vec<ApiAction>>,
	pub fallback: AlterKind<Expr>,
	pub config: Option<ApiConfig>,
	pub comment: AlterKind<String>,
}

impl Default for AlterApiStatement {
	fn default() -> Self {
		Self {
			path: Expr::Literal(Literal::None),
			if_exists: false,
			actions: None,
			fallback: AlterKind::None,
			config: None,
			comment: AlterKind::None,
		}
	}
}

impl AlterApiStatement {
	#[instrument(level = "trace", name = "AlterApiStatement::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		opt.is_allowed(Action::Edit, ResourceKind::Api, &Base::Db)?;
		let (_, _) = opt.ns_db()?;
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		let txn = ctx.tx();

		let path_name = expr_to_ident(stk, ctx, opt, doc, &self.path, "api path").await?;
		let mut ap = match txn.get_db_api(ns, db, &path_name).await? {
			Some(v) => v.deref().clone(),
			None => {
				if self.if_exists {
					return Ok(Value::None);
				}
				return Err(Error::ApNotFound {
					value: path_name,
				}
				.into());
			}
		};

		if let Some(ref actions) = self.actions {
			let mut new_actions = Vec::new();
			for action in actions {
				new_actions.push(ApiActionDefinition {
					methods: action.methods.clone(),
					action: action.action.clone(),
					config: action.config.compute(stk, ctx, opt, doc).await?,
				});
			}
			ap.actions = new_actions;
		}

		match self.fallback {
			AlterKind::Set(ref v) => ap.fallback = Some(v.clone()),
			AlterKind::Drop => ap.fallback = None,
			AlterKind::None => {}
		}

		if let Some(ref config) = self.config {
			ap.config = config.compute(stk, ctx, opt, doc).await?;
		}

		match self.comment {
			AlterKind::Set(ref v) => ap.comment = Some(v.clone()),
			AlterKind::Drop => ap.comment = None,
			AlterKind::None => {}
		}

		let key = crate::key::database::ap::new(ns, db, &path_name);
		txn.set(&key, &ap, None).await?;
		txn.clear_cache();
		Ok(Value::None)
	}
}

impl ToSql for AlterApiStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterApiStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
