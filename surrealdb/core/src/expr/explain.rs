use surrealdb_types::{SqlFormat, ToSql};

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Hash, priority_lfu::DeepSizeOf)]
pub(crate) struct Explain(pub bool);

impl ToSql for Explain {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		let explain: crate::sql::Explain = (*self).into();
		explain.fmt_sql(f, sql_fmt);
	}
}
