use std::slice::Iter;

use revision::revisioned;
use surrealdb_types::{SqlFormat, ToSql};

use super::paths::ID;
use crate::expr::{Expr, Function, Idiom};

/// The `foo,bar,*` part of statements like `SELECT foo,bar.* FROM faz`.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Fields {
	/// Fields had the `VALUE` clause and should only return the given selector
	///
	/// This variant should not contain Field::All
	/// TODO: Encode the above variant into the type.
	Value(Box<Selector>),
	/// Normal fields where an object with the selected fields is expected
	Select(Vec<Field>),
}

impl ToSql for Fields {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let fields: crate::sql::field::Fields = self.clone().into();
		fields.fmt_sql(f, fmt);
	}
}

impl Fields {
	/// Returns true if computing this value can be done on a read only
	/// transaction.
	pub fn read_only(&self) -> bool {
		match self {
			Fields::Value(field) => field.read_only(),
			Fields::Select(fields) => fields.iter().all(|x| x.read_only()),
		}
	}

	/// Create a new `*` field projection
	pub fn all() -> Self {
		Fields::Select(vec![Field::All])
	}

	/// Check to see if this field is a `*` projection
	pub fn has_all_selection(&self) -> bool {
		match self {
			Fields::Select(x) => x.iter().any(|x| matches!(x, Field::All)),
			Fields::Value(_) => false,
		}
	}
	/// Create a new `VALUE id` field projection
	pub fn value_id() -> Self {
		Fields::Value(Box::new(Selector {
			expr: Expr::Idiom(Idiom(ID.to_vec())),
			alias: None,
		}))
	}

	/// Returns an iterator which returns all fields which are not `Field::All`.
	pub fn iter_non_all_fields(&self) -> FieldsIter<'_> {
		match self {
			Fields::Value(selector) => FieldsIter::Single(Some(selector)),
			Fields::Select(fields) => FieldsIter::Multiple(fields.iter()),
		}
	}

	/// Check to see if this field is a single VALUE clause
	pub fn is_single(&self) -> bool {
		matches!(self, Fields::Value(_))
	}
	/// Check if the fields are only about counting
	pub fn is_count_all_only(&self) -> bool {
		fn field_is_count(f: &Field) -> bool {
			match f {
				Field::All => false,
				Field::Single(selector) => selector_is_count(selector),
			}
		}
		fn selector_is_count(f: &Selector) -> bool {
			let Expr::FunctionCall(x) = &f.expr else {
				return false;
			};
			if !x.arguments.is_empty() {
				return false;
			}
			let Function::Normal(name) = &x.receiver else {
				return false;
			};
			name == "count"
		}

		match self {
			Fields::Value(field) => selector_is_count(field),
			Fields::Select(fields) => !fields.is_empty() && fields.iter().all(field_is_count),
		}
	}
}

pub enum FieldsIter<'a> {
	Single(Option<&'a Selector>),
	Multiple(Iter<'a, Field>),
}

impl<'a> Iterator for FieldsIter<'a> {
	type Item = &'a Selector;

	fn next(&mut self) -> Option<Self::Item> {
		match self {
			FieldsIter::Single(field) => field.take(),
			FieldsIter::Multiple(iter) => loop {
				if let Field::Single(x) = iter.next()? {
					return Some(x);
				}
			},
		}
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		match self {
			FieldsIter::Single(field) => {
				if field.is_some() {
					(1, Some(1))
				} else {
					(0, Some(0))
				}
			}
			FieldsIter::Multiple(iter) => iter.size_hint(),
		}
	}
}
impl ExactSizeIterator for FieldsIter<'_> {}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum Field {
	/// The `*` in `SELECT * FROM ...`
	#[default]
	All,
	/// The 'rating' in `SELECT rating FROM ...`
	Single(Selector),
}

impl Field {
	/// Check if computing this type can be done on a read only transaction.
	pub fn read_only(&self) -> bool {
		match self {
			Field::All => true,
			Field::Single(x) => x.read_only(),
		}
	}
}

impl ToSql for Field {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::All => f.push('*'),
			Self::Single(s) => s.fmt_sql(f, fmt),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Selector {
	pub expr: Expr,
	/// The `quality` in `SELECT rating AS quality FROM ...`
	pub alias: Option<Idiom>,
}

impl Selector {
	pub fn read_only(&self) -> bool {
		self.expr.read_only()
	}
}

impl ToSql for Selector {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.expr.fmt_sql(f, fmt);
		if let Some(alias) = &self.alias {
			f.push_str(" AS ");
			alias.fmt_sql(f, fmt);
		}
	}
}
