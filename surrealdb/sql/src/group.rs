use crate::idiom::Idiom;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Groups(pub Vec<Group>);

impl surrealdb_types::ToSql for Groups {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
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

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Group(
	#[cfg_attr(feature = "arbitrary", arbitrary(with = crate::arbitrary::basic_idiom))] pub Idiom,
);

impl surrealdb_types::ToSql for Group {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		self.0.fmt_sql(f, fmt);
	}
}
