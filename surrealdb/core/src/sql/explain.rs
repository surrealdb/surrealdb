use surrealdb_types::{SqlFormat, ToSql};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Explain(pub bool);

impl ToSql for Explain {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str("EXPLAIN");
		if self.0 {
			f.push_str(" FULL");
		}
	}
}

impl From<Explain> for crate::expr::Explain {
	fn from(v: Explain) -> Self {
		Self(v.0)
	}
}
impl From<crate::expr::Explain> for Explain {
	fn from(v: crate::expr::Explain) -> Self {
		Self(v.0)
	}
}
