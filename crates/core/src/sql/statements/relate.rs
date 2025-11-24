use std::fmt;

use crate::sql::{Data, Expr, Literal, Output, RecordIdKeyLit, RecordIdLit, Timeout};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct RelateStatement {
	pub only: bool,
	/// The expression through which we create a relation
	pub through: Expr,
	/// The expression the relation is from
	pub from: Expr,
	/// The expression the relation targets.
	pub to: Expr,
	pub uniq: bool,
	pub data: Option<Data>,
	pub output: Option<Output>,
	pub timeout: Option<Timeout>,
	pub parallel: bool,
}

impl fmt::Display for RelateStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "RELATE")?;
		if self.only {
			f.write_str(" ONLY")?
		}
		f.write_str(" ")?;

		// Only array's, params, and record-id's that are not a range can be expressed without
		// surrounding parens
		if matches!(
			self.from,
			Expr::Literal(
				Literal::Array(_)
					| Literal::RecordId(RecordIdLit {
						key: RecordIdKeyLit::Number(_)
							| RecordIdKeyLit::String(_)
							| RecordIdKeyLit::Generate(_)
							| RecordIdKeyLit::Array(_)
							| RecordIdKeyLit::Object(_)
							| RecordIdKeyLit::Uuid(_),
						..
					})
			) | Expr::Param(_)
		) {
			self.from.fmt(f)?;
		} else {
			f.write_str("(")?;
			self.from.fmt(f)?;
			f.write_str(")")?;
		}
		f.write_str(" -> ")?;

		if matches!(self.through, Expr::Param(_) | Expr::Table(_)) {
			self.through.fmt(f)?;
		} else {
			f.write_str("(")?;
			self.through.fmt(f)?;
			f.write_str(")")?;
		}

		f.write_str(" -> ")?;

		// Only array's, params, and record-id's that are not a range can be expressed without
		// surrounding parens
		if matches!(
			self.to,
			Expr::Literal(
				Literal::Array(_)
					| Literal::RecordId(RecordIdLit {
						key: RecordIdKeyLit::Number(_)
							| RecordIdKeyLit::String(_)
							| RecordIdKeyLit::Generate(_)
							| RecordIdKeyLit::Array(_)
							| RecordIdKeyLit::Object(_)
							| RecordIdKeyLit::Uuid(_),
						..
					})
			) | Expr::Param(_)
		) {
			self.to.fmt(f)?;
		} else {
			f.write_str("(")?;
			self.to.fmt(f)?;
			f.write_str(")")?;
		}

		if self.uniq {
			f.write_str(" UNIQUE")?
		}
		if let Some(ref v) = self.data {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.output {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.timeout {
			write!(f, " {v}")?
		}
		if self.parallel {
			f.write_str(" PARALLEL")?
		}
		Ok(())
	}
}

impl From<RelateStatement> for crate::expr::statements::RelateStatement {
	fn from(v: RelateStatement) -> Self {
		crate::expr::statements::RelateStatement {
			only: v.only,
			through: v.through.into(),
			from: v.from.into(),
			to: v.to.into(),
			uniq: v.uniq,
			data: v.data.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.map(Into::into),
			parallel: v.parallel,
		}
	}
}

impl From<crate::expr::statements::RelateStatement> for RelateStatement {
	fn from(v: crate::expr::statements::RelateStatement) -> Self {
		RelateStatement {
			only: v.only,
			through: v.through.into(),
			from: v.from.into(),
			to: v.to.into(),
			uniq: v.uniq,
			data: v.data.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.map(Into::into),
			parallel: v.parallel,
		}
	}
}
