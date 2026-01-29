use std::fmt::Debug;
use std::ops::Deref;

use priority_lfu::DeepSizeOf;
use revision::revisioned;

use crate::expr::idiom::Idiom;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash, DeepSizeOf)]
pub(crate) struct Groups(pub(crate) Vec<Group>);

impl Groups {
	pub(crate) fn is_group_all_only(&self) -> bool {
		self.0.is_empty()
	}

	pub(crate) fn len(&self) -> usize {
		self.0.len()
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash, DeepSizeOf)]
pub(crate) struct Group(pub(crate) Idiom);

impl Deref for Group {
	type Target = Idiom;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl surrealdb_types::ToSql for Groups {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		let sql_groups: crate::sql::Groups = self.clone().into();
		sql_groups.fmt_sql(f, fmt);
	}
}

impl surrealdb_types::ToSql for Group {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		let sql_group: crate::sql::Group = self.clone().into();
		sql_group.fmt_sql(f, fmt);
	}
}
