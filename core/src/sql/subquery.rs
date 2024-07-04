use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::statements::rebuild::RebuildStatement;
use crate::sql::statements::{
	CreateStatement, DefineStatement, DeleteStatement, IfelseStatement, InsertStatement,
	OutputStatement, RelateStatement, RemoveStatement, SelectStatement, UpdateStatement,
	UpsertStatement,
};
use crate::sql::value::Value;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::{self, Display, Formatter};

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Subquery";

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Subquery")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Subquery {
	Value(Value),
	Ifelse(IfelseStatement),
	Output(OutputStatement),
	Select(SelectStatement),
	Create(CreateStatement),
	Update(UpdateStatement),
	Delete(DeleteStatement),
	Relate(RelateStatement),
	Insert(InsertStatement),
	Define(DefineStatement),
	Remove(RemoveStatement),
	#[revision(start = 2)]
	Rebuild(RebuildStatement),
	#[revision(start = 3)]
	Upsert(UpsertStatement),
}

impl PartialOrd for Subquery {
	#[inline]
	fn partial_cmp(&self, _: &Self) -> Option<Ordering> {
		None
	}
}

impl Subquery {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		match self {
			Self::Value(v) => v.writeable(),
			Self::Ifelse(v) => v.writeable(),
			Self::Output(v) => v.writeable(),
			Self::Select(v) => v.writeable(),
			Self::Create(v) => v.writeable(),
			Self::Upsert(v) => v.writeable(),
			Self::Update(v) => v.writeable(),
			Self::Delete(v) => v.writeable(),
			Self::Relate(v) => v.writeable(),
			Self::Insert(v) => v.writeable(),
			Self::Define(v) => v.writeable(),
			Self::Remove(v) => v.writeable(),
			Self::Rebuild(v) => v.writeable(),
		}
	}
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Duplicate context
		let mut ctx = Context::new(ctx);
		// Add parent document
		if let Some(doc) = doc {
			ctx.add_value("parent", doc.doc.as_ref());
		}
		// Process the subquery
		match self {
			Self::Value(ref v) => v.compute(stk, &ctx, opt, doc).await,
			Self::Ifelse(ref v) => v.compute(stk, &ctx, opt, doc).await,
			Self::Output(ref v) => v.compute(stk, &ctx, opt, doc).await,
			Self::Define(ref v) => v.compute(stk, &ctx, opt, doc).await,
			Self::Rebuild(ref v) => v.compute(stk, &ctx, opt, doc).await,
			Self::Remove(ref v) => v.compute(&ctx, opt, doc).await,
			Self::Select(ref v) => v.compute(stk, &ctx, opt, doc).await,
			Self::Create(ref v) => v.compute(stk, &ctx, opt, doc).await,
			Self::Upsert(ref v) => v.compute(stk, &ctx, opt, doc).await,
			Self::Update(ref v) => v.compute(stk, &ctx, opt, doc).await,
			Self::Delete(ref v) => v.compute(stk, &ctx, opt, doc).await,
			Self::Relate(ref v) => v.compute(stk, &ctx, opt, doc).await,
			Self::Insert(ref v) => v.compute(stk, &ctx, opt, doc).await,
		}
	}
}

impl Display for Subquery {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Value(v) => write!(f, "({v})"),
			Self::Output(v) => write!(f, "({v})"),
			Self::Select(v) => write!(f, "({v})"),
			Self::Create(v) => write!(f, "({v})"),
			Self::Upsert(v) => write!(f, "({v})"),
			Self::Update(v) => write!(f, "({v})"),
			Self::Delete(v) => write!(f, "({v})"),
			Self::Relate(v) => write!(f, "({v})"),
			Self::Insert(v) => write!(f, "({v})"),
			Self::Define(v) => write!(f, "({v})"),
			Self::Remove(v) => write!(f, "({v})"),
			Self::Rebuild(v) => write!(f, "({v})"),
			Self::Ifelse(v) => Display::fmt(v, f),
		}
	}
}
