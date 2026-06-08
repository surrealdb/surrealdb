use std::ops::Deref;

use anyhow::Result;
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};
use tracing::instrument;

use super::AlterKind;
use crate::catalog::providers::DatabaseProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::{Base, Expr, Filter, Literal, Tokenizer};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct AlterAnalyzerStatement {
	pub name: Expr,
	pub if_exists: bool,
	pub function: AlterKind<String>,
	pub tokenizers: AlterKind<Vec<Tokenizer>>,
	pub filters: AlterKind<Vec<Filter>>,
	pub comment: AlterKind<String>,
}

impl Default for AlterAnalyzerStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			if_exists: false,
			function: AlterKind::None,
			tokenizers: AlterKind::None,
			filters: AlterKind::None,
			comment: AlterKind::None,
		}
	}
}

impl AlterAnalyzerStatement {
	#[instrument(level = "trace", name = "AlterAnalyzerStatement::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		ctx.is_allowed(opt, Action::Edit, ResourceKind::Analyzer, Base::Db)?;
		let (_, _) = opt.ns_db()?;
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		let txn = ctx.tx();
		let name = expr_to_ident(stk, ctx, opt, doc, &self.name, "analyzer name").await?;

		let mut az = match txn.get_db_analyzer(ns, db, &name, None).await {
			Ok(v) => v.deref().clone(),
			Err(e) => {
				if self.if_exists {
					return Ok(Value::None);
				}
				return Err(e);
			}
		};

		match self.function {
			AlterKind::Set(ref v) => az.function = Some(v.clone().into()),
			AlterKind::Drop => az.function = None,
			AlterKind::None => {}
		}

		match self.tokenizers {
			AlterKind::Set(ref v) => az.tokenizers = Some(v.clone()),
			AlterKind::Drop => az.tokenizers = None,
			AlterKind::None => {}
		}

		match self.filters {
			AlterKind::Set(ref v) => az.filters = Some(v.clone()),
			AlterKind::Drop => az.filters = None,
			AlterKind::None => {}
		}

		match self.comment {
			AlterKind::Set(ref v) => az.comment = Some(v.clone()),
			AlterKind::Drop => az.comment = None,
			AlterKind::None => {}
		}

		let key = crate::key::database::az::new(ns, db, &name);
		txn.set(&key, &az).await?;
		txn.clear_cache();
		Ok(Value::None)
	}
}

impl ToSql for AlterAnalyzerStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterAnalyzerStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
