use std::ops::Deref;

use anyhow::Result;
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};

use super::AlterKind;
use crate::catalog::providers::ApiProvider;
use crate::catalog::{ApiActionDefinition, ApiMethod};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::statements::define::ApiAction;
use crate::expr::statements::define::config::api::ApiConfig;
use crate::expr::{Base, Expr, Literal};
use crate::iam::{Action, AuthLimit, ResourceKind};
use crate::val::Value;

/// A single `FOR` clause within an `ALTER API` statement.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum AlterApiClause {
	/// `FOR any [config] [THEN expr | DROP THEN]`
	ForAny {
		config: Option<ApiConfig>,
		fallback: AlterKind<Expr>,
	},
	/// `FOR method1, method2 [config] THEN expr`
	SetAction(ApiAction),
	/// `FOR method1, method2 DROP THEN`
	DropAction {
		methods: Vec<ApiMethod>,
	},
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct AlterApiStatement {
	pub path: Expr,
	pub if_exists: bool,
	pub clauses: Vec<AlterApiClause>,
	pub comment: AlterKind<String>,
}

impl Default for AlterApiStatement {
	fn default() -> Self {
		Self {
			path: Expr::Literal(Literal::None),
			if_exists: false,
			clauses: Vec::new(),
			comment: AlterKind::None,
		}
	}
}

/// Remove the given methods from existing action entries, splitting entries
/// that partially overlap and removing entries that are fully consumed.
fn remove_methods_from_actions(actions: &mut Vec<ApiActionDefinition>, drop_methods: &[ApiMethod]) {
	let mut i = 0;
	while i < actions.len() {
		actions[i].methods.retain(|m| !drop_methods.contains(m));
		if actions[i].methods.is_empty() {
			actions.swap_remove(i);
		} else {
			i += 1;
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
		let mut ap = match txn.get_db_api(ns, db, &path_name, None).await? {
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

		for clause in &self.clauses {
			match clause {
				AlterApiClause::ForAny {
					config,
					fallback,
				} => {
					if let Some(c) = config {
						ap.config = c.compute(stk, ctx, opt, doc).await?;
					}
					match fallback {
						AlterKind::Set(v) => ap.fallback = Some(v.clone()),
						AlterKind::Drop => ap.fallback = None,
						AlterKind::None => {}
					}
				}
				AlterApiClause::SetAction(action) => {
					remove_methods_from_actions(&mut ap.actions, &action.methods);
					ap.actions.push(ApiActionDefinition {
						methods: action.methods.clone(),
						action: action.action.clone(),
						config: action.config.compute(stk, ctx, opt, doc).await?,
					});
				}
				AlterApiClause::DropAction {
					methods,
				} => {
					remove_methods_from_actions(&mut ap.actions, methods);
				}
			}
		}

		match self.comment {
			AlterKind::Set(ref v) => ap.comment = Some(v.clone()),
			AlterKind::Drop => ap.comment = None,
			AlterKind::None => {}
		}

		// Recompute auth_limit from the current principal to prevent privilege escalation
		ap.auth_limit = AuthLimit::new_from_auth(opt.auth.as_ref()).into();

		let key = crate::key::database::ap::new(ns, db, &path_name);
		txn.set(&key, &ap).await?;
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
