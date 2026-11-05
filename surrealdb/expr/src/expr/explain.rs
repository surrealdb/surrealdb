use surrealdb_types::{SqlFormat, ToSql};

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct Explain(pub bool);

impl ToSql for Explain {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str("EXPLAIN");
		if self.0 {
			f.push_str(" FULL");
		}
	}
}
