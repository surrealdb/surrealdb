use crate::sql::idiom::Idiom;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Groups(pub Vec<Group>);

impl surrealdb_types::ToSql for Groups {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		use surrealdb_types::ToSql;
		if self.0.is_empty() {
			f.push_str("GROUP ALL");
		} else {
			f.push_str("GROUP BY ");
			for (i, item) in self.0.iter().enumerate() {
				if i > 0 {
					fmt.write_separator(f);
				}
				item.fmt_sql(f, fmt);
			}
		}
	}
}

impl From<Groups> for crate::expr::Groups {
	fn from(v: Groups) -> Self {
		Self(v.0.into_iter().map(Into::into).collect())
	}
}

impl From<crate::expr::Groups> for Groups {
	fn from(v: crate::expr::Groups) -> Self {
		Self(v.0.into_iter().map(Into::into).collect())
	}
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct Group(pub(crate) Idiom);

impl surrealdb_types::ToSql for Group {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		self.0.fmt_sql(f, fmt);
	}
}

impl From<Group> for crate::expr::Group {
	fn from(v: Group) -> Self {
		Self(v.0.into())
	}
}

impl From<crate::expr::Group> for Group {
	fn from(v: crate::expr::Group) -> Self {
		Self(v.0.into())
	}
}
