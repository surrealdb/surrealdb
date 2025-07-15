use crate::ctx::{Context, MutableContext};
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::statements::rebuild::RebuildStatement;
use crate::expr::statements::{
	AlterStatement, CreateStatement, DefineStatement, DeleteStatement, IfelseStatement,
	InsertStatement, OutputStatement, RelateStatement, RemoveStatement, SelectStatement,
	UpdateStatement, UpsertStatement,
};
use crate::expr::value::Value;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::{self, Display, Formatter};

use super::FlowResult;
use super::statements::InfoStatement;

#[revisioned(revision = 5)]
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
	#[revision(start = 4)]
	Alter(AlterStatement),
	#[revision(start = 5)]
	Info(InfoStatement),
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
			Self::Alter(v) => v.writeable(),
			Self::Info(v) => v.writeable(),
		}
	}

	/// Process this type returning a computed simple Value, without catching errors
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> FlowResult<Value> {
		// Duplicate context
		let mut ctx = MutableContext::new(ctx);
		// Add parent document
		if let Some(doc) = doc {
			ctx.add_value("parent", doc.doc.as_ref().clone().into());
		}
		let ctx = ctx.freeze();
		// Process the subquery
		let res = match self {
			Self::Value(v) => return v.compute(stk, &ctx, opt, doc).await,
			Self::Ifelse(v) => return v.compute(stk, &ctx, opt, doc).await,
			Self::Output(v) => return v.compute(stk, &ctx, opt, doc).await,
			Self::Define(v) => v.compute(stk, &ctx, opt, doc).await?,
			Self::Rebuild(v) => v.compute(stk, &ctx, opt, doc).await?,
			Self::Remove(v) => v.compute(&ctx, opt, doc).await?,
			Self::Select(v) => v.compute(stk, &ctx, opt, doc).await?,
			Self::Create(v) => v.compute(stk, &ctx, opt, doc).await?,
			Self::Upsert(v) => v.compute(stk, &ctx, opt, doc).await?,
			Self::Update(v) => v.compute(stk, &ctx, opt, doc).await?,
			Self::Delete(v) => v.compute(stk, &ctx, opt, doc).await?,
			Self::Relate(v) => v.compute(stk, &ctx, opt, doc).await?,
			Self::Insert(v) => v.compute(stk, &ctx, opt, doc).await?,
			Self::Alter(v) => v.compute(stk, &ctx, opt, doc).await?,
			Self::Info(v) => v.compute(stk, &ctx, opt, doc).await?,
		};

		Ok(res)
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
			Self::Alter(v) => write!(f, "({v})"),
			Self::Info(v) => write!(f, "({v})"),
			Self::Ifelse(v) => Display::fmt(v, f),
		}
	}
}
