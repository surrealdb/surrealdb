use crate::expr::order::Ordering;
use crate::expr::{Cond, Explain, Expr, Fetchs, Fields, Groups, Limit, Splits, Start, With};

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
}

impl SelectStatement {
	/// Check if computing this type can be done on a read only transaction.
	pub fn read_only(&self) -> bool {
		self.fields.read_only()
			&& self.what.iter().all(|v| v.read_only())
			&& self.cond.as_ref().map(|x| x.0.read_only()).unwrap_or(true)
	}
}
