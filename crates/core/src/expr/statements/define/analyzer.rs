use std::fmt::{self, Display};

use anyhow::{Result, bail};
use revision::revisioned;
use serde::{Deserialize, Serialize};

use super::DefineKind;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::filter::Filter;
use crate::expr::statements::info::InfoStructure;
use crate::expr::tokenizer::Tokenizer;
use crate::expr::{Base, Ident, Value};
use crate::iam::{Action, ResourceKind};
use crate::kvs::impl_kv_value_revisioned;
use crate::val::{Array, Strand};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct DefineAnalyzerStatement {
	pub kind: DefineKind,
	pub name: Ident,
	pub function: Option<String>,
	pub tokenizers: Option<Vec<Tokenizer>>,
	pub filters: Option<Vec<Filter>>,
	pub comment: Option<Strand>,
}

impl_kv_value_revisioned!(DefineAnalyzerStatement);

impl DefineAnalyzerStatement {
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
		let az = DefineAnalyzerStatement {
			// Don't persist the `IF NOT EXISTS` clause to schema
			kind: DefineKind::Default,
			..self.clone()
		};
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
