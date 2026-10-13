use crate::val::{Datetime, TableName};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum ShowSince {
	Timestamp(Datetime),
	Versionstamp(u64),
}

/// A SHOW CHANGES statement for displaying changes made to a table or database.

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ShowStatement {
	pub table: Option<TableName>,
	pub since: ShowSince,
	pub limit: Option<u32>,
}
