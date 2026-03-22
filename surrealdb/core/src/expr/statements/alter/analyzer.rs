use std::ops::Deref;

use anyhow::Result;
use surrealdb_types::{SqlFormat, ToSql};

use super::AlterKind;
use crate::catalog::providers::DatabaseProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::expr::{Base, Filter, Tokenizer};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct AlterAnalyzerStatement {
	pub name: String,
	pub if_exists: bool,
	pub function: AlterKind<String>,
	pub tokenizers: AlterKind<Vec<Tokenizer>>,
	pub filters: AlterKind<Vec<Filter>>,
	pub comment: AlterKind<String>,
}

impl AlterAnalyzerStatement {
	#[instrument(level = "trace", name = "AlterAnalyzerStatement::compute", skip_all)]
	pub(crate) async fn compute(&self, ctx: &FrozenContext, opt: &Options) -> Result<Value> {
		opt.is_allowed(Action::Edit, ResourceKind::Analyzer, &Base::Db)?;
		let (_, _) = opt.ns_db()?;
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		let txn = ctx.tx();

		let mut az = match txn.get_db_analyzer(ns, db, &self.name).await {
			Ok(v) => v.deref().clone(),
			Err(e) => {
				if self.if_exists {
					return Ok(Value::None);
				}
				return Err(e);
			}
		};

		match self.function {
			AlterKind::Set(ref v) => az.function = Some(v.clone()),
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

		let key = crate::key::database::az::new(ns, db, &self.name);
		txn.set(&key, &az, None).await?;
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
