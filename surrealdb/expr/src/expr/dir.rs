use serde::{Deserialize, Serialize};
use storekey::{BorrowDecode, Encode};
use surrealdb_types::{SqlFormat, ToSql};

#[derive(
	Copy,
	Clone,
	Debug,
	Default,
	Eq,
	PartialEq,
	Serialize,
	PartialOrd,
	Deserialize,
	Hash,
	Encode,
	BorrowDecode,
)]
pub enum Dir {
	/// `<-`
	In,
	/// `->`
	Out,
	/// `<->`
	#[default]
	Both,
}

impl ToSql for Dir {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		let dir: crate::sql::Dir = (*self).into();
		dir.fmt_sql(f, sql_fmt);
	}
}
