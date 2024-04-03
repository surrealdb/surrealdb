use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::{filter::Filter, tokenizer::Tokenizer, Base, Ident, Strand, Value};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[revisioned(revision = 3)]
#[non_exhaustive]
pub struct DefineAnalyzerStatement {
	pub name: Ident,
	#[revision(start = 2)]
	pub function: Option<Ident>,
	pub tokenizers: Option<Vec<Tokenizer>>,
	pub filters: Option<Vec<Filter>>,
	pub comment: Option<Strand>,
	#[revision(start = 3)]
	pub if_not_exists: bool,
}

impl DefineAnalyzerStatement {
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Analyzer, &Base::Db)?;
		// Claim transaction
		let mut run = txn.lock().await;
		// Clear the cache
		run.clear_cache();
		// Check if analyzer already exists
		if self.if_not_exists && run.get_db_analyzer(opt.ns(), opt.db(), &self.name).await.is_ok() {
			return Err(Error::AzAlreadyExists {
				value: self.name.to_string(),
			});
		}
		// Process the statement
		let key = crate::key::database::az::new(opt.ns(), opt.db(), &self.name);
		run.add_ns(opt.ns(), opt.strict).await?;
		run.add_db(opt.ns(), opt.db(), opt.strict).await?;
		// Persist the definition
		run.set(
			key,
			DefineAnalyzerStatement {
				// Don't persist the "IF NOT EXISTS" clause to schema
				if_not_exists: false,
				..self.clone()
			},
		)
		.await?;
		// Release the transaction
		drop(run); // Do we really need this?
		   // Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineAnalyzerStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE ANALYZER")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
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
