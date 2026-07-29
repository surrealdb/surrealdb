use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::{CoverStmts, Data, Expr, Literal, Output, RecordIdKeyLit, RecordIdLit};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RelateStatement {
	pub only: bool,
	/// When true, update an existing edge record with the same explicit id.
	pub or_update: bool,
	/// The expression through which we create a relation
	pub through: Expr,
	/// The expression the relation is from
	pub from: Expr,
	/// The expression the relation targets.
	pub to: Expr,
	/// The data associated with the relation being created
	pub data: Option<Data>,
	/// What the result of the statement should resemble (i.e. Diff or no result etc).
	pub output: Option<Output>,
	/// The timeout for the statement
	pub timeout: Expr,
}

impl ToSql for RelateStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "RELATE");
		if self.only {
			write_sql!(f, fmt, " ONLY");
		}
		if self.or_update {
			write_sql!(f, fmt, " OR UPDATE");
		}
		write_sql!(f, fmt, " ");

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
			self.from.fmt_sql(f, fmt);
		} else {
			write_sql!(f, fmt, "(");
			self.from.fmt_sql(f, fmt);
			write_sql!(f, fmt, ")");
		}
		write_sql!(f, fmt, " -> ");

		if matches!(self.through, Expr::Param(_) | Expr::Table(_)) {
			self.through.fmt_sql(f, fmt);
		} else {
			write_sql!(f, fmt, "(");
			self.through.fmt_sql(f, fmt);
			write_sql!(f, fmt, ")");
		}

		write_sql!(f, fmt, " -> ");

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
			self.to.fmt_sql(f, fmt);
		} else {
			write_sql!(f, fmt, "(");
			self.to.fmt_sql(f, fmt);
			write_sql!(f, fmt, ")");
		}

		if let Some(ref v) = self.data {
			write_sql!(f, fmt, " {v}");
		}
		if let Some(ref v) = self.output {
			write_sql!(f, fmt, " {v}");
		}
		if !matches!(self.timeout, Expr::Literal(Literal::None)) {
			write_sql!(f, fmt, " TIMEOUT {}", CoverStmts(&self.timeout));
		}
	}
}
