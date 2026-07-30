use revision::revisioned;

use crate::expr::Expr;
use crate::expr::statements::info::InfoStructure;
use crate::val::Value;

/// A list of fetches to be applied to the result of a query.
///
/// Fetches are applied to the result of a query in the order they are specified.
/// For this reason, the list of fetches is always sorted to ensure that parent fetches are
/// applied before child fetches.
///
/// For example:
/// `FETCH a.b, a` is sorted to `FETCH a, a.b`.
/// `FETCH a.b, a.b.c, d, a, b` is sorted to `FETCH a, a.b, a.b.c, b, d`.
///
/// This prevents confusing behaviour like `FETCH a.b, a` only returning `a` because `a.b` gets
/// clobbered by `a`.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Fetchs(Vec<Fetch>);

impl Fetchs {
	pub fn new(fetches: Vec<Fetch>) -> Self {
		Self(fetches)
	}

	pub fn len(&self) -> usize {
		self.0.len()
	}

	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	pub fn iter(&self) -> impl Iterator<Item = &Fetch> {
		self.0.iter()
	}

	pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut Fetch> {
		self.0.iter_mut()
	}
}

impl IntoIterator for Fetchs {
	type Item = Fetch;
	type IntoIter = std::vec::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl surrealdb_types::ToSql for Fetchs {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		let sql_fetchs: crate::sql::Fetchs = self.clone().into();
		sql_fetchs.fmt_sql(f, fmt);
	}
}

impl InfoStructure for Fetchs {
	fn structure(self) -> Value {
		self.into_iter().map(Fetch::structure).collect::<Vec<_>>().into()
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Fetch(pub Expr);

impl surrealdb_types::ToSql for Fetch {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		let sql_fetch: crate::sql::Fetch = self.clone().into();
		sql_fetch.fmt_sql(f, fmt);
	}
}

impl InfoStructure for Fetch {
	fn structure(self) -> Value {
		use surrealdb_types::ToSql;
		self.to_sql().into()
	}
}
