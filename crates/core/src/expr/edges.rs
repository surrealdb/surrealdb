use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::table::Tables;
use crate::expr::thing::Thing;
use crate::{ctx::Context, expr::dir::Dir};
use anyhow::Result;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

use super::Value;
use super::graph::GraphSubjects;

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Edges")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Edges {
	pub dir: Dir,
	pub from: Thing,
	#[revision(end = 2, convert_fn = "convert_old_what")]
	pub _what: Tables,
	#[revision(start = 2)]
	pub what: GraphSubjects,
}

impl Edges {
	pub fn new(dir: Dir, from: Thing, what: GraphSubjects) -> Self {
		Edges {
			dir,
			from,
			what,
		}
	}

	fn convert_old_what(&mut self, _rev: u16, old: Tables) -> Result<(), revision::Error> {
		self.what = old.into();
		Ok(())
	}

	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		Ok(Value::Edges(Box::new(Self {
			dir: self.dir.clone(),
			from: self.from.clone(),
			what: self.what.clone().compute(stk, ctx, opt, doc).await?,
		})))
	}
}

impl fmt::Display for Edges {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self.what.len() {
			0 => write!(f, "{}{}?", self.from, self.dir,),
			1 => write!(f, "{}{}{}", self.from, self.dir, self.what),
			_ => write!(f, "{}{}({})", self.from, self.dir, self.what),
		}
	}
}
