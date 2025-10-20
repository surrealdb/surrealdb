use anyhow::{Result, bail};
use reblessive::tree::Stk;
use surrealdb_types::{write_sql, PrettyMode, ToSql};

use super::DefineKind;
use crate::catalog;
use crate::catalog::providers::DatabaseProvider;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::expression::VisitExpression;
use crate::expr::filter::Filter;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::tokenizer::Tokenizer;
use crate::expr::{Base, Expr, Idiom, Literal, Value};
use crate::iam::{Action, ResourceKind};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct DefineAnalyzerStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub function: Option<String>,
	pub tokenizers: Option<Vec<Tokenizer>>,
	pub filters: Option<Vec<Filter>>,
	pub comment: Option<Expr>,
}

impl VisitExpression for DefineAnalyzerStatement {
	fn visit<F>(&self, visitor: &mut F)
	where
		F: FnMut(&Expr),
	{
		self.name.visit(visitor);
		self.comment.iter().for_each(|comment| comment.visit(visitor));
	}
}

impl Default for DefineAnalyzerStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			name: Expr::Literal(Literal::None),
			function: None,
			tokenizers: None,
			filters: None,
			comment: None,
		}
	}
}

impl DefineAnalyzerStatement {
	pub(crate) async fn to_definition(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<catalog::AnalyzerDefinition> {
		Ok(catalog::AnalyzerDefinition {
			name: expr_to_ident(stk, ctx, opt, doc, &self.name, "analyzer name").await?,
			function: self.function.clone(),
			tokenizers: self.tokenizers.clone(),
			filters: self.filters.clone(),
			comment: map_opt!(x as &self.comment => compute_to!(stk, ctx, opt, doc, x => String)),
		})
	}

	pub fn from_definition(def: &catalog::AnalyzerDefinition) -> Self {
		Self {
			kind: DefineKind::Default,
			name: Expr::Idiom(Idiom::field(def.name.clone())),
			function: def.function.clone(),
			tokenizers: def.tokenizers.clone(),
			filters: def.filters.clone(),
			comment: def.comment.as_ref().map(|x| Expr::Literal(Literal::String(x.clone()))),
		}
	}

	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
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
							name: definition.name.to_string(),
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
	fn fmt_sql(&self, f: &mut String, pretty: PrettyMode) {
		write_sql!(f, "DEFINE ANALYZER");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write_sql!(f, " OVERWRITE"),
			DefineKind::IfNotExists => write_sql!(f, " IF NOT EXISTS"),
		}
		write_sql!(f, " ");
		self.name.fmt_sql(f, pretty);
		if let Some(ref v) = self.function {
			write_sql!(f, " FUNCTION fn::{v}");
		}
		if let Some(v) = &self.tokenizers {
			let tokens: Vec<String> = v.iter().map(|f| f.to_string()).collect();
			write_sql!(f, " TOKENIZERS {}", tokens.join(","));
		}
		if let Some(v) = &self.filters {
			let tokens: Vec<String> = v.iter().map(|f| f.to_string()).collect();
			write_sql!(f, " FILTERS {}", tokens.join(","));
		}
		if let Some(ref v) = self.comment {
			write_sql!(f, " COMMENT ");
			v.fmt_sql(f, pretty);
		}
	}
}
