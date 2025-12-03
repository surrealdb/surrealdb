use revision::revisioned;
use serde::{Deserialize, Serialize};
use surrealdb_types::{SqlFormat, ToSql};

#[revisioned(revision = 1)]
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

impl From<Dir> for crate::expr::Dir {
	fn from(v: Dir) -> Self {
		match v {
			Dir::In => Self::In,
			Dir::Out => Self::Out,
			Dir::Both => Self::Both,
		}
	}
}

impl From<crate::expr::Dir> for Dir {
	fn from(v: crate::expr::Dir) -> Self {
		match v {
			crate::expr::Dir::In => Self::In,
			crate::expr::Dir::Out => Self::Out,
			crate::expr::Dir::Both => Self::Both,
		}
	}
}
