use crate::sql::{Ident, Strand, ToSql, filter::Filter, tokenizer::Tokenizer};

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 4)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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
	#[revision(start = 4)]
	pub overwrite: bool,
}

impl Display for DefineAnalyzerStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE ANALYZER")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		if self.overwrite {
			write!(f, " OVERWRITE")?
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
			write!(f, " COMMENT {}", v.to_sql())?
		}
		Ok(())
	}
}

impl From<DefineAnalyzerStatement> for crate::expr::statements::DefineAnalyzerStatement {
	fn from(v: DefineAnalyzerStatement) -> Self {
		crate::expr::statements::DefineAnalyzerStatement {
			name: v.name.into(),
			function: v.function.map(Into::into),
			tokenizers: v.tokenizers.map(|v| v.into_iter().map(Into::into).collect()),
			filters: v.filters.map(|v| v.into_iter().map(Into::into).collect()),
			comment: v.comment.map(Into::into),
			if_not_exists: v.if_not_exists,
			overwrite: v.overwrite,
		}
	}
}

impl From<crate::expr::statements::DefineAnalyzerStatement> for DefineAnalyzerStatement {
	fn from(v: crate::expr::statements::DefineAnalyzerStatement) -> Self {
		DefineAnalyzerStatement {
			name: v.name.into(),
			function: v.function.map(Into::into),
			tokenizers: v.tokenizers.map(|v| v.into_iter().map(Into::into).collect()),
			filters: v.filters.map(|v| v.into_iter().map(Into::into).collect()),
			comment: v.comment.map(Into::into),
			if_not_exists: v.if_not_exists,
			overwrite: v.overwrite,
		}
	}
}
