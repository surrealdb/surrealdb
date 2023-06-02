use crate::ctx::Context;
use crate::dbs::Level;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use crate::sql::ident::{ident, Ident};
use crate::sql::index::Index;
use crate::sql::object::Object;
use crate::sql::value::Value;
use derive::Store;
use nom::bytes::complete::tag_no_case;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Display, Formatter};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Store, Hash)]
pub enum AnalyzeStatement {
	Idx(Ident, Ident),
}

impl AnalyzeStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&Value>,
	) -> Result<Value, Error> {
		match self {
			AnalyzeStatement::Idx(tb, idx) => {
				// Selected DB?
				opt.needs(Level::Db)?;
				// Allowed to run?
				opt.check(Level::Db)?;
				// Clone transaction
				let run = txn.clone();
				// Claim transaction
				let mut run = run.lock().await;
				// Read the index
				let ix = run.get_ix(opt.ns(), opt.db(), tb.as_str(), idx.as_str()).await?;
				// Index operation dispatching
				match &ix.index {
					Index::Uniq => todo!(),
					Index::Idx => todo!(),
					Index::Search {
						az,
						sc,
						hl,
						order,
					} => {}
				};
				// Create the result set
				let res = Object::default();
				// Ok all good
				Value::from(res).ok()
			}
		}
	}
}

pub fn analyze(i: &str) -> IResult<&str, AnalyzeStatement> {
	let (i, _) = tag_no_case("ANALYZE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("INDEX")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, idx) = ident(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("ON")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, tb) = ident(i)?;
	Ok((i, AnalyzeStatement::Idx(tb, idx)))
}

impl Display for AnalyzeStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Idx(tb, idx) => write!(f, "ANALYZE INDEX {idx} ON {tb}"),
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn analyze_index() {
		let sql = "ANALYZE INDEX my_index ON my_table";
		let res = analyze(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, AnalyzeStatement::Idx(Ident::from("my_table"), Ident::from("my_index")));
		assert_eq!("ANALYZE INDEX my_index ON my_table", format!("{}", out));
	}
}
