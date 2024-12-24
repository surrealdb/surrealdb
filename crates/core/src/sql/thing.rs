use super::id::range::IdRange;
use super::{Cond, Expression, Ident, Idiom, Operator, Part, Table};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::key::r#ref::Ref;
use crate::sql::{escape::escape_rid, id::Id, Strand, Value};
use crate::syn;
use derive::Store;
use futures::StreamExt;
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
						Thing::from((self.tb, id.to_owned())).into(),
					)))))
				}
				(Bound::Unbounded, Bound::Included(id)) => {
					Some(Cond(Value::Expression(Box::new(Expression::new(
						Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
						Operator::LessThanOrEqual,
						Thing::from((self.tb, id.to_owned())).into(),
					)))))
				}
				(Bound::Excluded(id), Bound::Unbounded) => {
					Some(Cond(Value::Expression(Box::new(Expression::new(
						Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
						Operator::MoreThan,
						Thing::from((self.tb, id.to_owned())).into(),
					)))))
				}
				(Bound::Included(id), Bound::Unbounded) => {
					Some(Cond(Value::Expression(Box::new(Expression::new(
						Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
						Operator::MoreThanOrEqual,
						Thing::from((self.tb, id.to_owned())).into(),
					)))))
				}
				(Bound::Included(lid), Bound::Included(rid)) => {
					Some(Cond(Value::Expression(Box::new(Expression::new(
						Value::Expression(Box::new(Expression::new(
							Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
							Operator::MoreThanOrEqual,
							Thing::from((self.tb.clone(), lid.to_owned())).into(),
						))),
						Operator::And,
						Value::Expression(Box::new(Expression::new(
							Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
							Operator::LessThanOrEqual,
							Thing::from((self.tb, rid.to_owned())).into(),
						))),
					)))))
				}
				(Bound::Included(lid), Bound::Excluded(rid)) => {
					Some(Cond(Value::Expression(Box::new(Expression::new(
						Value::Expression(Box::new(Expression::new(
							Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
							Operator::MoreThanOrEqual,
							Thing::from((self.tb.clone(), lid.to_owned())).into(),
						))),
						Operator::And,
						Value::Expression(Box::new(Expression::new(
							Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
							Operator::LessThan,
							Thing::from((self.tb, rid.to_owned())).into(),
						))),
					)))))
				}
				(Bound::Excluded(lid), Bound::Included(rid)) => {
					Some(Cond(Value::Expression(Box::new(Expression::new(
						Value::Expression(Box::new(Expression::new(
							Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
							Operator::MoreThan,
							Thing::from((self.tb.clone(), lid.to_owned())).into(),
						))),
						Operator::And,
						Value::Expression(Box::new(Expression::new(
							Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
							Operator::LessThanOrEqual,
							Thing::from((self.tb, rid.to_owned())).into(),
						))),
					)))))
				}
				(Bound::Excluded(lid), Bound::Excluded(rid)) => {
					Some(Cond(Value::Expression(Box::new(Expression::new(
						Value::Expression(Box::new(Expression::new(
							Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
							Operator::MoreThan,
							Thing::from((self.tb.clone(), lid.to_owned())).into(),
						))),
						Operator::And,
						Value::Expression(Box::new(Expression::new(
							Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
							Operator::LessThan,
							Thing::from((self.tb, rid.to_owned())).into(),
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

	pub(crate) async fn refs(
		&self,
		ctx: &Context,
		opt: &Options,
		ft: Option<&Table>,
		ff: Option<&Idiom>,
	) -> Result<Vec<Thing>, Error> {
		let ns = opt.ns()?;
		let db = opt.db()?;

		let (prefix, suffix) = match (ft, ff) {
			(Some(ft), Some(ff)) => {
				let ff = ff.to_string();

				(
					crate::key::r#ref::ffprefix(ns, db, &self.tb, &self.id, ft, &ff),
					crate::key::r#ref::ffsuffix(ns, db, &self.tb, &self.id, ft, &ff),
				)
			}
			(Some(ft), None) => (
				crate::key::r#ref::ftprefix(ns, db, &self.tb, &self.id, ft),
				crate::key::r#ref::ftsuffix(ns, db, &self.tb, &self.id, ft),
			),
			(None, None) => (
				crate::key::r#ref::prefix(ns, db, &self.tb, &self.id),
				crate::key::r#ref::suffix(ns, db, &self.tb, &self.id),
			),
			(None, Some(_)) => return Err(Error::Unreachable("A foreign field was passed without a foreign table".into())),
		};

		let txn = ctx.tx();
		let range = prefix..suffix;
		let mut stream = txn.stream_keys(range);

		// Collect the keys from the stream into a vec
		let mut keys: Vec<Vec<u8>> = vec![];
		while let Some(res) = stream.next().await {
			keys.push(res?);
		}

		let ids = keys
			.iter()
			.filter_map(|x| {
				let key = Ref::from(x);

				Some(Thing {
					tb: key.ft.to_string(),
					id: key.fk,
				})
			})
			.collect();

		Ok(ids)
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
		match syn::thing_with_range(v) {
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
	/// Check if this Thing is a range
	pub fn is_range(&self) -> bool {
		matches!(self.id, Id::Range(_))
	}
	/// Check if this Thing is of a certain table type
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

#[cfg(test)]
mod test {
	use std::{ops::Bound, str::FromStr};

	use crate::sql::{Array, Id, IdRange, Object, Value};

	use super::Thing;

	#[test]
	fn from() {
		{
			let string = "foo:bar";
			let thing = Thing {
				tb: "foo".into(),
				id: Id::String("bar".into()),
			};
			assert_eq!(thing, Thing::from_str(string).unwrap());
		}
		{
			let string = "foo:1";
			let thing = Thing {
				tb: "foo".into(),
				id: Id::Number(1),
			};
			assert_eq!(thing, Thing::from_str(string).unwrap());
		}
		{
			let string = "foo:[1, 'bar']";
			let thing = Thing {
				tb: "foo".into(),
				id: Id::Array(Array(vec![1i64.into(), "bar".into()])),
			};
			assert_eq!(thing, Thing::from_str(string).unwrap());
		}
		{
			let string = "foo:{bar: 1}";
			let thing = Thing {
				tb: "foo".into(),
				id: Id::Object(Object([("bar".to_string(), Value::from(1))].into_iter().collect())),
			};
			assert_eq!(thing, Thing::from_str(string).unwrap());
		}
		{
			let string = "foo:1..=2";
			let thing = Thing {
				tb: "foo".into(),
				id: Id::Range(Box::new(
					IdRange::try_from((
						Bound::Included(Id::Number(1)),
						Bound::Included(Id::Number(2)),
					))
					.unwrap(),
				)),
			};
			assert_eq!(thing, Thing::from_str(string).unwrap());
		}
	}
}
