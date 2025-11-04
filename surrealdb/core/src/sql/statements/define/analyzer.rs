use std::fmt::{self, Display};

use super::DefineKind;
use crate::sql::filter::Filter;
use crate::sql::tokenizer::Tokenizer;
use crate::sql::{Expr, Literal};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct DefineAnalyzerStatement {
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

impl From<DefineAnalyzerStatement> for crate::expr::statements::DefineAnalyzerStatement {
	fn from(v: DefineAnalyzerStatement) -> Self {
		crate::expr::statements::DefineAnalyzerStatement {
			kind: v.kind.into(),
			name: v.name.into(),
			function: v.function,
			tokenizers: v.tokenizers.map(|v| v.into_iter().map(Into::into).collect()),
			filters: v.filters.map(|v| v.into_iter().map(Into::into).collect()),
			comment: v.comment.map(|x| x.into()),
		}
	}
}

impl From<crate::expr::statements::DefineAnalyzerStatement> for DefineAnalyzerStatement {
	fn from(v: crate::expr::statements::DefineAnalyzerStatement) -> Self {
		DefineAnalyzerStatement {
			kind: v.kind.into(),
			name: v.name.into(),
			function: v.function,
			tokenizers: v.tokenizers.map(|v| v.into_iter().map(Into::into).collect()),
			filters: v.filters.map(|v| v.into_iter().map(Into::into).collect()),
			comment: v.comment.map(|x| x.into()),
		}
	}
}
