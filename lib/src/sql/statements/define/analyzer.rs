use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::Action;
use crate::iam::ResourceKind;
use crate::sql::base::Base;
use crate::sql::comment::shouldbespace;
use crate::sql::ending;
use crate::sql::error::expected;
use crate::sql::error::IResult;
use crate::sql::filter::{filters, Filter};
use crate::sql::ident::{ident, Ident};
use crate::sql::strand::{strand, Strand};
use crate::sql::tokenizer::{tokenizers, Tokenizer};
use crate::sql::value::Value;
use derive::Store;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::cut;
use nom::multi::many0;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct DefineAnalyzerStatement {
	pub name: Ident,
	pub tokenizers: Option<Vec<Tokenizer>>,
	pub filters: Option<Vec<Filter>>,
	pub comment: Option<Strand>,
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
		// Process the statement
		let key = crate::key::database::az::new(opt.ns(), opt.db(), &self.name);
		run.add_ns(opt.ns(), opt.strict).await?;
		run.add_db(opt.ns(), opt.db(), opt.strict).await?;
		run.set(key, self).await?;
		// Release the transaction
		drop(run); // Do we really need this?
		   // Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineAnalyzerStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE ANALYZER {}", self.name)?;
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

pub fn analyzer(i: &str) -> IResult<&str, DefineAnalyzerStatement> {
	let (i, _) = tag_no_case("ANALYZER")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = cut(ident)(i)?;
	let (i, opts) = many0(analyzer_opts)(i)?;
	let (i, _) = expected("one of FILTERS, TOKENIZERS, or COMMENT", ending::query)(i)?;
	// Create the base statement
	let mut res = DefineAnalyzerStatement {
		name,
		..Default::default()
	};
	// Assign any defined options
	for opt in opts {
		match opt {
			DefineAnalyzerOption::Comment(v) => {
				res.comment = Some(v);
			}
			DefineAnalyzerOption::Filters(v) => {
				res.filters = Some(v);
			}
			DefineAnalyzerOption::Tokenizers(v) => {
				res.tokenizers = Some(v);
			}
		}
	}
	// Return the statement
	Ok((i, res))
}

enum DefineAnalyzerOption {
	Comment(Strand),
	Filters(Vec<Filter>),
	Tokenizers(Vec<Tokenizer>),
}

fn analyzer_opts(i: &str) -> IResult<&str, DefineAnalyzerOption> {
	alt((analyzer_comment, analyzer_filters, analyzer_tokenizers))(i)
}

fn analyzer_comment(i: &str) -> IResult<&str, DefineAnalyzerOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("COMMENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(strand)(i)?;
	Ok((i, DefineAnalyzerOption::Comment(v)))
}

fn analyzer_filters(i: &str) -> IResult<&str, DefineAnalyzerOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("FILTERS")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(filters)(i)?;
	Ok((i, DefineAnalyzerOption::Filters(v)))
}

fn analyzer_tokenizers(i: &str) -> IResult<&str, DefineAnalyzerOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("TOKENIZERS")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(tokenizers)(i)?;
	Ok((i, DefineAnalyzerOption::Tokenizers(v)))
}
