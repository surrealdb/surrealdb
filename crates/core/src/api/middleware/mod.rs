use std::ops::Deref;

pub mod api;
pub(super) mod invoke;

use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::{
	err::Error,
	sql::{statements::info::InfoStructure, Object, Value},
};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RequestMiddleware(
	#[cfg_attr(feature = "arbitrary", arbitrary(with = crate::sql::arbitrary::atleast_one))]
	pub  Vec<(String, Vec<Value>)>,
);

impl InfoStructure for RequestMiddleware {
	fn structure(self) -> Value {
		Value::Object(Object(self.0.into_iter().map(|(k, v)| (k, Value::from(v))).collect()))
	}
}

impl Deref for RequestMiddleware {
	type Target = Vec<(String, Vec<Value>)>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

pub type CollectedMiddleware<'a> = Vec<(&'a String, &'a Vec<Value>)>;

pub trait CollectMiddleware<'a> {
	fn collect(&'a self) -> Result<CollectedMiddleware<'a>, Error>;
}

impl<'a> CollectMiddleware<'a> for Vec<&'a RequestMiddleware> {
	fn collect(&'a self) -> Result<CollectedMiddleware<'a>, Error> {
		let mut middleware: CollectedMiddleware<'a> = Vec::new();

		for map in self.iter() {
			for (k, v) in map.iter() {
				match k.split_once("::") {
					Some(("api", _)) => middleware.push((k, v)),
					Some(("fn", _)) => {
						return Err(Error::Unreachable(
							"Custom middleware are not yet supported".into(),
						))
					}
					_ => {
						return Err(Error::Unreachable(
							"Found a middleware which is unparsable".into(),
						))
					}
				}
			}
		}

		Ok(middleware)
	}
}
