use std::borrow::Cow;
use std::ops::Deref;

use anyhow::Result;
use reblessive::tree::Stk;
use tracing::instrument;

use crate::catalog::providers::DatabaseProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::Base;
use crate::expr::statements::alter::AlterKind;
use crate::expr::statements::alter::analyzer::AlterAnalyzerStatement;
use crate::iam::{Action, ResourceKind};
use crate::key::schema::AnalyzerKey;
use crate::legacy::expr_to_ident;
use crate::val::Value;

#[instrument(level = "trace", name = "AlterAnalyzerStatement::compute", skip_all)]
pub(crate) async fn alter_analyzer_statement_compute(
	this: &AlterAnalyzerStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Analyzer, Base::Db)?;
	let (_, _) = opt.ns_db()?;
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	let txn = ctx.tx();
	let name = expr_to_ident(stk, ctx, opt, doc, &this.name, "analyzer name").await?;

	let mut az = match txn.get_db_analyzer(ns, db, &name, None).await {
		Ok(v) => v.deref().clone(),
		Err(e) => {
			if this.if_exists {
				return Ok(Value::None);
			}
			return Err(e);
		}
	};

	match this.function {
		AlterKind::Set(ref v) => az.function = Some(v.clone().into()),
		AlterKind::Drop => az.function = None,
		AlterKind::None => {}
	}

	match this.tokenizers {
		AlterKind::Set(ref v) => az.tokenizers = Some(v.clone()),
		AlterKind::Drop => az.tokenizers = None,
		AlterKind::None => {}
	}

	match this.filters {
		AlterKind::Set(ref v) => az.filters = Some(v.clone()),
		AlterKind::Drop => az.filters = None,
		AlterKind::None => {}
	}

	match this.comment {
		AlterKind::Set(ref v) => az.comment = Some(v.clone()),
		AlterKind::Drop => az.comment = None,
		AlterKind::None => {}
	}

	let key = AnalyzerKey {
		ns,
		db,
		az: Cow::Borrowed(&name),
	};
	txn.set_key(&key, &az).await?;
	txn.clear_cache();
	Ok(Value::None)
}
