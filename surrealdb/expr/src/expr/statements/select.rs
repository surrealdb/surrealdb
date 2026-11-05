use common::fmt::Fmt;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::order::Ordering;
use crate::expr::{
	Cond, CoverStmts, Explain, Expr, Fetchs, Fields, Groups, Limit, Literal, Splits, Start, With,
};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct SelectStatement {
	/// The fields to extract from the records.
	///
	/// The foo,bar part in `SELECT foo,bar FROM baz`.
	pub fields: Fields,
	/// The fields to omit from the records. This is used to exclude fields from wildcard
	/// selection.
	///
	/// The OMIT foo,bar part in `SELECT foo,bar OMIT baz,qux FROM baz`.
	pub omit: Vec<Expr>,
	/// Whether to only return a single record.
	///
	/// The ONLY part in `SELECT * FROM ONLY foo`.
	pub only: bool,
	/// The expressions (tables, record IDs, arrays, etc) to select from.
	///
	/// The baz part in SELECT foo,bar FROM baz.
	pub what: Vec<Expr>,
	pub with: Option<With>,
	/// The WHERE clause.
	pub cond: Option<Cond>,
	/// The SPLIT clause.
	///
	/// This is used to produce the cartesian product of the values in split fields.
	pub split: Option<Splits>,
	pub group: Option<Groups>,
	pub order: Option<Ordering>,
	pub limit: Option<Limit>,
	pub start: Option<Start>,
	pub fetch: Option<Fetchs>,
	pub version: Expr,
	pub timeout: Expr,
	pub explain: Option<Explain>,
	pub tempfiles: bool,
	/// Whether the read locks the selected records for the duration of
	/// the transaction.
	///
	/// The FOR UPDATE part in `SELECT * FROM foo FOR UPDATE`.
	pub for_update: bool,
}

impl SelectStatement {
	/// Check if computing this type can be done on a read only transaction.
	/// Whether computing this statement can be done on a read-only
	/// transaction.
	///
	/// Every clause that can carry a user expression is inspected. The
	/// executor picks the transaction type from this before the statement is
	/// planned, so a clause omitted here would let a mutation reach a
	/// read-only transaction and fail partway through the statement.
	/// A `FOR UPDATE` read always requires a write transaction: the locked
	/// reads register the fetched keys for commit-time conflict detection,
	/// which storage engines only support on writeable transactions.
	pub fn read_only(&self) -> bool {
		!self.for_update
			&& self.fields.read_only()
			&& self.omit.iter().all(|v| v.read_only())
			&& self.what.iter().all(|v| v.read_only())
			&& self.cond.as_ref().map(|x| x.0.read_only()).unwrap_or(true)
			&& self.split.as_ref().map(|x| x.read_only()).unwrap_or(true)
			&& self.group.as_ref().map(|x| x.read_only()).unwrap_or(true)
			&& self.order.as_ref().map(|x| x.read_only()).unwrap_or(true)
			&& self.limit.as_ref().map(|x| x.read_only()).unwrap_or(true)
			&& self.start.as_ref().map(|x| x.read_only()).unwrap_or(true)
			&& self.fetch.as_ref().map(|x| x.read_only()).unwrap_or(true)
			&& self.version.read_only()
			&& self.timeout.read_only()
	}
}

impl ToSql for SelectStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "SELECT {}", self.fields);
		if !self.omit.is_empty() {
			write_sql!(f, fmt, " OMIT {}", Fmt::comma_separated(self.omit.iter().map(CoverStmts)));
		}
		write_sql!(f, fmt, " FROM");
		if self.only {
			write_sql!(f, fmt, " ONLY");
		}
		write_sql!(f, fmt, " {}", Fmt::comma_separated(self.what.iter().map(CoverStmts)));
		if let Some(ref v) = self.with {
			write_sql!(f, fmt, " {v}");
		}
		if let Some(ref v) = self.cond {
			write_sql!(f, fmt, " {v}");
		}
		if let Some(ref v) = self.split {
			write_sql!(f, fmt, " {v}");
		}
		if let Some(ref v) = self.group {
			write_sql!(f, fmt, " {v}");
		}
		if let Some(ref v) = self.order {
			write_sql!(f, fmt, " {v}");
		}
		if let Some(ref v) = self.limit {
			write_sql!(f, fmt, " {v}");
		}
		if let Some(ref v) = self.start {
			write_sql!(f, fmt, " {v}");
		}
		if let Some(ref v) = self.fetch {
			write_sql!(f, fmt, " {v}");
		}
		if !matches!(self.version, Expr::Literal(Literal::None)) {
			write_sql!(f, fmt, " VERSION {}", CoverStmts(&self.version));
		}
		if !matches!(self.timeout, Expr::Literal(Literal::None)) {
			write_sql!(f, fmt, " TIMEOUT {}", CoverStmts(&self.timeout));
		}
		if self.for_update {
			write_sql!(f, fmt, " FOR UPDATE");
		}
		if self.tempfiles {
			write_sql!(f, fmt, " TEMPFILES");
		}
		if let Some(ref v) = self.explain {
			write_sql!(f, fmt, " {v}");
		}
	}
}
