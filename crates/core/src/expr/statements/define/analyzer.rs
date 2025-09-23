use std::fmt::{self, Display};

use anyhow::{Result, bail};
use reblessive::tree::Stk;

use super::DefineKind;
use crate::catalog;
use crate::catalog::providers::DatabaseProvider;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::filter::Filter;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::tokenizer::Tokenizer;
use crate::expr::{Base, Expr, Idiom, Literal, Value};
use crate::iam::{Action, ResourceKind};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineAnalyzerStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub function: Option<String>,
	pub tokenizers: Option<Vec<Tokenizer>>,
	pub filters: Option<Vec<Filter>>,
	pub comment: Option<Expr>,
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

impl Display for DefineAnalyzerStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE ANALYZER")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " IF NOT EXISTS")?,
			DefineKind::IfNotExists => write!(f, " OVERWRITE")?,
		}
		write!(f, " {}", self.name)?;
		if let Some(ref i) = self.function {
			write!(f, " FUNCTION fn::{i}")?
		}
		if let Some(v) = &self.tokenizers {
			let tokens: Vec<String> = v.iter().map(|f| f.to_string()).collect();
			write!(f, " TOKENIZERS {}", tokens.join(","))?;
		}
		if let Some(v) = &self.filters {
			let tokens: Vec<String> = v.iter().map(|f| f.to_string()).collect();
			write!(f, " FILTERS {}", tokens.join(","))?;
		}
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {}", v)?
		}
		Ok(())
	}
}
