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
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str(match self {
			Self::In => "<-",
			Self::Out => "->",
			Self::Both => "<->",
		})
	}
}
