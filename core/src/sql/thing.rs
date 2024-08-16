use super::id::range::IdRange;
use super::id::value::IdValue;
use super::{Cond, Expression, Ident, Idiom, Operator, Part, Table};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::{escape::escape_rid, id::Id, Strand, Value};
use crate::syn;
use derive::Store;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Bound;
use std::str::FromStr;

const ID: &str = "id";
pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Thing";

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[serde(rename = "$surrealdb::private::sql::Thing")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Thing {
	/// Table name
	pub tb: String,
	pub id: Id,
}

impl Thing {
	/// Convert `Thing` to `Cond`
	pub fn to_cond(self) -> Option<Cond> {
		match &self.id {
			Id::Range(r) => match (&r.beg, &r.end) {
				(Bound::Unbounded, Bound::Unbounded) => None,
				(Bound::Unbounded, Bound::Excluded(id)) => {
					Some(Cond(Value::Expression(Box::new(Expression::new(
						Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
						Operator::LessThan,
						Thing::from((self.tb, Id::try_from(id.to_owned()).unwrap())).into(),
					)))))
				}
				(Bound::Unbounded, Bound::Included(id)) => {
					Some(Cond(Value::Expression(Box::new(Expression::new(
						Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
						Operator::LessThanOrEqual,
						Thing::from((self.tb, Id::try_from(id.to_owned()).unwrap())).into(),
					)))))
				}
				(Bound::Excluded(id), Bound::Unbounded) => {
					Some(Cond(Value::Expression(Box::new(Expression::new(
						Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
						Operator::MoreThan,
						Thing::from((self.tb, Id::try_from(id.to_owned()).unwrap())).into(),
					)))))
				}
				(Bound::Included(id), Bound::Unbounded) => {
					Some(Cond(Value::Expression(Box::new(Expression::new(
						Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
						Operator::MoreThanOrEqual,
						Thing::from((self.tb, Id::try_from(id.to_owned()).unwrap())).into(),
					)))))
				}
				(Bound::Included(lid), Bound::Included(rid)) => {
					Some(Cond(Value::Expression(Box::new(Expression::new(
						Value::Expression(Box::new(Expression::new(
							Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
							Operator::MoreThanOrEqual,
							Thing::from((self.tb.clone(), Id::try_from(lid.to_owned()).unwrap()))
								.into(),
						))),
						Operator::And,
						Value::Expression(Box::new(Expression::new(
							Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
							Operator::LessThanOrEqual,
							Thing::from((self.tb, Id::try_from(rid.to_owned()).unwrap())).into(),
						))),
					)))))
				}
				(Bound::Included(lid), Bound::Excluded(rid)) => {
					Some(Cond(Value::Expression(Box::new(Expression::new(
						Value::Expression(Box::new(Expression::new(
							Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
							Operator::MoreThanOrEqual,
							Thing::from((self.tb.clone(), Id::try_from(lid.to_owned()).unwrap()))
								.into(),
						))),
						Operator::And,
						Value::Expression(Box::new(Expression::new(
							Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
							Operator::LessThan,
							Thing::from((self.tb, Id::try_from(rid.to_owned()).unwrap())).into(),
						))),
					)))))
				}
				(Bound::Excluded(lid), Bound::Included(rid)) => {
					Some(Cond(Value::Expression(Box::new(Expression::new(
						Value::Expression(Box::new(Expression::new(
							Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
							Operator::MoreThan,
							Thing::from((self.tb.clone(), Id::try_from(lid.to_owned()).unwrap()))
								.into(),
						))),
						Operator::And,
						Value::Expression(Box::new(Expression::new(
							Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
							Operator::LessThanOrEqual,
							Thing::from((self.tb, Id::try_from(rid.to_owned()).unwrap())).into(),
						))),
					)))))
				}
				(Bound::Excluded(lid), Bound::Excluded(rid)) => {
					Some(Cond(Value::Expression(Box::new(Expression::new(
						Value::Expression(Box::new(Expression::new(
							Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
							Operator::MoreThan,
							Thing::from((self.tb.clone(), Id::try_from(lid.to_owned()).unwrap()))
								.into(),
						))),
						Operator::And,
						Value::Expression(Box::new(Expression::new(
							Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
							Operator::LessThan,
							Thing::from((self.tb, Id::try_from(rid.to_owned()).unwrap())).into(),
						))),
					)))))
				}
			},
			_ => Some(Cond(Value::Expression(Box::new(Expression::new(
				Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
				Operator::Equal,
				Thing::from((self.tb, self.id)).into(),
			))))),
		}
	}
}

impl From<(&str, Id)> for Thing {
	fn from((tb, id): (&str, Id)) -> Self {
		Self {
			tb: tb.to_owned(),
			id,
		}
	}
}

impl From<(String, Id)> for Thing {
	fn from((tb, id): (String, Id)) -> Self {
		Self {
			tb,
			id,
		}
	}
}

impl From<(&str, IdValue)> for Thing {
	fn from((tb, id): (&str, IdValue)) -> Self {
		Self {
			tb: tb.to_owned(),
			id: id.into(),
		}
	}
}

impl From<(String, IdValue)> for Thing {
	fn from((tb, id): (String, IdValue)) -> Self {
		Self {
			tb,
			id: id.into(),
		}
	}
}

impl From<(&str, IdRange)> for Thing {
	fn from((tb, id): (&str, IdRange)) -> Self {
		Self {
			tb: tb.to_owned(),
			id: id.into(),
		}
	}
}

impl From<(String, IdRange)> for Thing {
	fn from((tb, id): (String, IdRange)) -> Self {
		Self {
			tb,
			id: id.into(),
		}
	}
}

impl From<(String, String)> for Thing {
	fn from((tb, id): (String, String)) -> Self {
		Self::from((tb, Id::from(id)))
	}
}

impl From<(&str, &str)> for Thing {
	fn from((tb, id): (&str, &str)) -> Self {
		Self::from((tb.to_owned(), Id::from(id)))
	}
}

impl FromStr for Thing {
	type Err = ();
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		Self::try_from(s)
	}
}

impl TryFrom<String> for Thing {
	type Error = ();
	fn try_from(v: String) -> Result<Self, Self::Error> {
		Self::try_from(v.as_str())
	}
}

impl TryFrom<Strand> for Thing {
	type Error = ();
	fn try_from(v: Strand) -> Result<Self, Self::Error> {
		Self::try_from(v.as_str())
	}
}

impl TryFrom<&str> for Thing {
	type Error = ();
	fn try_from(v: &str) -> Result<Self, Self::Error> {
		match syn::thing(v) {
			Ok(v) => Ok(v),
			_ => Err(()),
		}
	}
}

impl Thing {
	/// Convert the Thing to a raw String
	pub fn to_raw(&self) -> String {
		self.to_string()
	}

	pub fn is_record_type(&self, types: &[Table]) -> bool {
		types.is_empty() || types.iter().any(|tb| tb.0 == self.tb)
	}
}

impl fmt::Display for Thing {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}:{}", escape_rid(&self.tb), self.id)
	}
}

impl Thing {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		Ok(Value::Thing(Thing {
			tb: self.tb.clone(),
			id: self.id.compute(stk, ctx, opt, doc).await?,
		}))
	}
}
