use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::Base;
use crate::sql::ident::Ident;
use crate::sql::index::Index;
use crate::sql::value::SqlValue;
use crate::iam::{Action, ResourceKind};
use crate::idx::IndexKeyBase;
use crate::idx::ft::FtIndex;
use crate::idx::trees::mtree::MTreeIndex;
use crate::kvs::TransactionType;
use anyhow::{Result, bail};

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Display, Formatter};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum AnalyzeStatement {
	Idx(Ident, Ident),
}

impl From<AnalyzeStatement> for crate::expr::statements::analyze::AnalyzeStatement {
	fn from(value: AnalyzeStatement) -> Self {
		match value {
			AnalyzeStatement::Idx(tb, idx) => Self::Idx(tb.into(), idx.into()),
		}
	}
}

impl From<crate::expr::statements::analyze::AnalyzeStatement> for AnalyzeStatement {
	fn from(value: crate::expr::statements::analyze::AnalyzeStatement) -> Self {
		match value {
			crate::expr::statements::analyze::AnalyzeStatement::Idx(tb, idx) => {
				Self::Idx(tb.into(), idx.into())
			}
		}
	}
}

impl Display for AnalyzeStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Idx(tb, idx) => write!(f, "ANALYZE INDEX {idx} ON {tb}"),
		}
	}
}
