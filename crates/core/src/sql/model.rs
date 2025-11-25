use surrealdb_types::{SqlFormat, ToSql, write_sql};

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Model {
	pub name: String,
	pub version: String,
}

impl ToSql for Model {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "ml::{}<{}>", self.name, self.version)
	}
}

impl From<Model> for crate::expr::Model {
	fn from(v: Model) -> Self {
		Self {
			name: v.name,
			version: v.version,
		}
	}
}
impl From<crate::expr::Model> for Model {
	fn from(v: crate::expr::Model) -> Self {
		Self {
			name: v.name,
			version: v.version,
		}
	}
}
