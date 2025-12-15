use anyhow::{Result, bail};
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};

use super::DefineKind;
use crate::catalog;
use crate::catalog::providers::DatabaseProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::filter::Filter;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::tokenizer::Tokenizer;
use crate::expr::{Base, Expr, FlowResultExt, Idiom, Literal, Value};
use crate::iam::{Action, ResourceKind};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct DefineAnalyzerStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub function: Option<String>,
	pub tokenizers: Option<Vec<Tokenizer>>,
	pub filters: Option<Vec<Filter>>,
	pub comment: Expr,
}

impl Default for DefineAnalyzerStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			name: Expr::Literal(Literal::None),
			function: None,
			tokenizers: None,
			filters: None,
			comment: Expr::Literal(Literal::None),
		}
	}
}

impl DefineAnalyzerStatement {
	pub(crate) async fn to_definition(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<catalog::AnalyzerDefinition> {
		let comment = stk
			.run(|stk| self.comment.compute(stk, ctx, opt, doc))
			.await
			.catch_return()?
			.cast_to()?;

		Ok(catalog::AnalyzerDefinition {
			name: expr_to_ident(stk, ctx, opt, doc, &self.name, "analyzer name").await?,
			function: self.function.clone(),
			tokenizers: self.tokenizers.clone(),
			filters: self.filters.clone(),
			comment,
		})
	}

	pub fn from_definition(def: &catalog::AnalyzerDefinition) -> Self {
		Self {
			kind: DefineKind::Default,
			name: Expr::Idiom(Idiom::field(def.name.clone())),
			function: def.function.clone(),
			tokenizers: def.tokenizers.clone(),
			filters: def.filters.clone(),
			comment: def
				.comment
				.as_ref()
				.map(|x| Expr::Literal(Literal::String(x.clone())))
				.unwrap_or(Expr::Literal(Literal::None)),
		}
	}

	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Analyzer, &Base::Db)?;
		// Compute the definition
		let definition = self.to_definition(stk, ctx, opt, doc).await?;
		// Fetch the transaction
		let txn = ctx.tx();
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;
		// Check if the definition exists
		if txn.get_db_analyzer(ns, db, &definition.name).await.is_ok() {
			match self.kind {
				DefineKind::Default => {
					if !opt.import {
						bail!(Error::AzAlreadyExists {
							name: definition.name.clone(),
						});
					}
				}
				DefineKind::Overwrite => {}
				DefineKind::IfNotExists => return Ok(Value::None),
			}
		}
		// Process the statement
		let key = crate::key::database::az::new(ns, db, &definition.name);
		ctx.get_index_stores().mappers().load(&definition).await?;
		txn.set(&key, &definition, None).await?;
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}
impl ToSql for DefineAnalyzerStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let sql_stmt: crate::sql::statements::define::DefineAnalyzerStatement = self.clone().into();
		sql_stmt.fmt_sql(f, fmt);
	}
}
