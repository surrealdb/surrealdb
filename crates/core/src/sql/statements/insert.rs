use std::fmt;

use crate::fmt::CoverStmts;
use crate::sql::{Data, Expr, Literal, Output};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InsertStatement {
	pub into: Option<Expr>,
	pub data: Data,
	/// Does the statement have the ignore clause.
	pub ignore: bool,
	pub update: Option<Data>,
	pub output: Option<Output>,
	pub timeout: Expr,
	pub parallel: bool,
	pub relation: bool,
	pub version: Expr,
}

impl Default for InsertStatement {
	fn default() -> Self {
		Self {
			into: Default::default(),
			data: Default::default(),
			ignore: Default::default(),
			update: Default::default(),
			output: Default::default(),
			timeout: Expr::Literal(Literal::None),
			parallel: Default::default(),
			relation: Default::default(),
			version: Expr::Literal(Literal::None),
		}
	}
}

impl fmt::Display for InsertStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("INSERT")?;
		if self.relation {
			f.write_str(" RELATION")?
		}
		if self.ignore {
			f.write_str(" IGNORE")?
		}
		if let Some(into) = &self.into {
			write!(f, " INTO {}", CoverStmts(into))?;
		}
		write!(f, " {}", self.data)?;
		if let Some(ref v) = self.update {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.output {
			write!(f, " {v}")?
		}
		if !matches!(self.version, Expr::Literal(Literal::None)) {
			write!(f, "VERSION {}", CoverStmts(&self.version))?;
		}
		if !matches!(self.timeout, Expr::Literal(Literal::None)) {
			write!(f, " TIMEOUT {}", CoverStmts(&self.timeout))?;
		}
		if self.parallel {
			f.write_str(" PARALLEL")?
		}
		Ok(())
	}
}

impl From<InsertStatement> for crate::expr::statements::InsertStatement {
	fn from(v: InsertStatement) -> Self {
		crate::expr::statements::InsertStatement {
			into: v.into.map(Into::into),
			data: v.data.into(),
			ignore: v.ignore,
			update: v.update.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.into(),
			parallel: v.parallel,
			relation: v.relation,
			version: v.version.into(),
		}
	}
}

impl From<crate::expr::statements::InsertStatement> for InsertStatement {
	fn from(v: crate::expr::statements::InsertStatement) -> Self {
		InsertStatement {
			into: v.into.map(Into::into),
			data: v.data.into(),
			ignore: v.ignore,
			update: v.update.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.into(),
			parallel: v.parallel,
			relation: v.relation,
			version: v.version.into(),
		}
	}
}
