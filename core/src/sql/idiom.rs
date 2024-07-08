use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::statements::info::InfoStructure;
use crate::sql::{
	fmt::{fmt_separated_by, Fmt},
	part::Next,
	paths::{ID, IN, META, OUT},
	Part, Value,
};
use md5::{Digest, Md5};
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::str;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Idiom";

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Idioms(pub Vec<Idiom>);

impl Deref for Idioms {
	type Target = Vec<Idiom>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl IntoIterator for Idioms {
	type Item = Idiom;
	type IntoIter = std::vec::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl Display for Idioms {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&Fmt::comma_separated(&self.0), f)
	}
}

impl InfoStructure for Idioms {
	fn structure(self) -> Value {
		self.to_string().into()
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Idiom")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Idiom(pub Vec<Part>);

impl Deref for Idiom {
	type Target = [Part];
	fn deref(&self) -> &Self::Target {
		self.0.as_slice()
	}
}

impl From<String> for Idiom {
	fn from(v: String) -> Self {
		Self(vec![Part::from(v)])
	}
}

impl From<&str> for Idiom {
	fn from(v: &str) -> Self {
		Self(vec![Part::from(v)])
	}
}

impl From<Vec<Part>> for Idiom {
	fn from(v: Vec<Part>) -> Self {
		Self(v)
	}
}

impl From<&[Part]> for Idiom {
	fn from(v: &[Part]) -> Self {
		Self(v.to_vec())
	}
}

impl From<Part> for Idiom {
	fn from(v: Part) -> Self {
		Self(vec![v])
	}
}

impl Idiom {
	/// Appends a part to the end of this Idiom
	pub(crate) fn push(mut self, n: Part) -> Idiom {
		self.0.push(n);
		self
	}
	/// Convert this Idiom to a unique hash
	pub(crate) fn to_hash(&self) -> String {
		let mut hasher = Md5::new();
		hasher.update(self.to_string().as_str());
		format!("{:x}", hasher.finalize())
	}
	/// Convert this Idiom to a JSON Path string
	pub(crate) fn to_path(&self) -> String {
		format!("/{self}").replace(']', "").replace(&['.', '['][..], "/")
	}
	/// Simplifies this Idiom for use in object keys
	pub(crate) fn simplify(&self) -> Idiom {
		self.0
			.iter()
			.filter(|&p| {
				matches!(p, Part::Field(_) | Part::Start(_) | Part::Value(_) | Part::Graph(_))
			})
			.cloned()
			.collect::<Vec<_>>()
			.into()
	}
	/// Check if this Idiom is an 'id' field
	pub(crate) fn is_id(&self) -> bool {
		self.0.len() == 1 && self.0[0].eq(&ID[0])
	}
	/// Check if this Idiom is an 'in' field
	pub(crate) fn is_in(&self) -> bool {
		self.0.len() == 1 && self.0[0].eq(&IN[0])
	}
	/// Check if this Idiom is an 'out' field
	pub(crate) fn is_out(&self) -> bool {
		self.0.len() == 1 && self.0[0].eq(&OUT[0])
	}
	/// Check if this Idiom is a 'meta' field
	pub(crate) fn is_meta(&self) -> bool {
		self.0.len() == 1 && self.0[0].eq(&META[0])
	}
	/// Check if this is an expression with multiple yields
	pub(crate) fn is_multi_yield(&self) -> bool {
		self.iter().any(Self::split_multi_yield)
	}
	/// Check if the path part is a yield in a multi-yield expression
	pub(crate) fn split_multi_yield(v: &Part) -> bool {
		matches!(v, Part::Graph(g) if g.alias.is_some())
	}
	/// Check if the path part is a yield in a multi-yield expression
	pub(crate) fn remove_trailing_all(&mut self) {
		if self.ends_with(&[Part::All]) {
			self.0.truncate(self.len() - 1);
		}
	}
}

impl Idiom {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		self.0.iter().any(|v| v.writeable())
	}
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		match self.first() {
			// The starting part is a value
			Some(Part::Start(v)) => {
				v.compute(stk, ctx, opt, doc)
					.await?
					.get(stk, ctx, opt, doc, self.as_ref().next())
					.await?
					.compute(stk, ctx, opt, doc)
					.await
			}
			// Otherwise use the current document
			_ => match doc {
				// There is a current document
				Some(v) => {
					v.doc.get(stk, ctx, opt, doc, self).await?.compute(stk, ctx, opt, doc).await
				}
				// There isn't any document
				None => Ok(Value::None),
			},
		}
	}
}

impl Display for Idiom {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		Display::fmt(
			&Fmt::new(
				self.0.iter().enumerate().map(|args| {
					Fmt::new(args, |(i, p), f| match (i, p) {
						(0, Part::Field(v)) => Display::fmt(v, f),
						_ => Display::fmt(p, f),
					})
				}),
				fmt_separated_by(""),
			),
			f,
		)
	}
}

impl InfoStructure for Idiom {
	fn structure(self) -> Value {
		self.to_string().into()
	}
}
