use std::fmt::{self, Display};

use anyhow::{Result, bail};

use super::DefineKind;
use crate::catalog;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::filter::Filter;
use crate::expr::statements::info::InfoStructure;
use crate::expr::tokenizer::Tokenizer;
use crate::expr::{Base, Ident, Value};
use crate::iam::{Action, ResourceKind};
use crate::val::{Array, Strand};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct DefineAnalyzerStatement {
	pub kind: DefineKind,
	pub name: Ident,
	pub function: Option<String>,
	pub tokenizers: Option<Vec<Tokenizer>>,
	pub filters: Option<Vec<Filter>>,
	pub comment: Option<Strand>,
}

impl DefineAnalyzerStatement {
	pub(crate) fn to_definition(&self) -> catalog::AnalyzerDefinition {
		catalog::AnalyzerDefinition {
			name: self.name.clone().into_string(),
			function: self.function.clone(),
			tokenizers: self.tokenizers.clone(),
			filters: self.filters.clone(),
			comment: self.comment.as_ref().map(|x| x.clone().into_string()),
		}
	}

	pub fn from_definition(def: &catalog::AnalyzerDefinition) -> Self {
		Self {
			kind: DefineKind::Default,
			name: Ident::new(def.name.clone()).unwrap(),
			function: def.function.clone(),
			tokenizers: def.tokenizers.clone(),
			filters: def.filters.clone(),
			comment: def.comment.as_ref().map(|x| Strand::new(x.clone()).unwrap()),
		}
	}

	pub(crate) async fn compute(
		&self,
		ctx: &Context,
		opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Analyzer, &Base::Db)?;
		// Fetch the transaction
		let txn = ctx.tx();
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;
		// Check if the definition exists
		if txn.get_db_analyzer(ns, db, &self.name).await.is_ok() {
			match self.kind {
				DefineKind::Default => {
					if !opt.import {
						bail!(Error::AzAlreadyExists {
							name: self.name.to_string(),
						});
					}
				}
				DefineKind::Overwrite => {}
				DefineKind::IfNotExists => return Ok(Value::None),
			}
		}
		// Process the statement
		let key = crate::key::database::az::new(ns, db, &self.name);
		let az = self.to_definition();
		ctx.get_index_stores().mappers().load(&az).await?;
		txn.set(&key, &az, None).await?;
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
			write!(f, " COMMENT {v}")?
		}
		Ok(())
	}
}

impl InfoStructure for DefineAnalyzerStatement {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => Value::from(self.name.clone().into_strand()),
			// TODO: Null byte validity
			"function".to_string(), if let Some(v) = self.function => Value::from(Strand::new(v.clone()).unwrap()),
			"tokenizers".to_string(), if let Some(v) = self.tokenizers =>
				v.into_iter().map(|v| v.to_string().into()).collect::<Array>().into(),
			"filters".to_string(), if let Some(v) = self.filters =>
				v.into_iter().map(|v| v.to_string().into()).collect::<Array>().into(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}
