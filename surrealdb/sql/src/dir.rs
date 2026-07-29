use serde::{Deserialize, Serialize};
use surrealdb_types::{SqlFormat, ToSql};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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
