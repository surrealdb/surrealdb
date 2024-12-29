use crate::ctx::MutableContext;
use crate::iam::{Action, Auth, ResourceKind};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::{Base, Block, Value};
use derive::Store;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct ImpersonateStatement {
	pub target: ImpersonationTarget,
	pub then: Block,
}

impl ImpersonateStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		self.then.writeable()
	}
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		let base = match (opt.ns(), opt.db()) {
			(Ok(_), Ok(_)) => Base::Db,
			(Ok(_), Err(_)) => Base::Ns,
			_ => Base::Root,
		};

		opt.is_allowed(Action::Impersonate, ResourceKind::Actor, &base)?;

		let (ctx, opt) = match &self.target {
			ImpersonationTarget::Record(thing, ac) => {
				let thing = match thing.compute(stk, ctx, opt, doc).await? {
					Value::Thing(v) => v,
					_ => return Err(Error::Thrown("expected a record id".into())),
				};

				let ns = opt.ns()?;
				let db = opt.db()?;
				let auth = Auth::for_record(thing.to_string(), ns, db, &ac);
				let opt = opt.new_with_auth(auth.into());

				let mut ctx = MutableContext::new(ctx);
				ctx.add_value("auth", Value::from(thing).into());
				ctx.add_value("access", Value::from(ac.to_owned()).into());
				let ctx = ctx.freeze();

				(ctx, opt)
			}
		};

		stk.run(|stk| self.then.compute(stk, &ctx, &opt, doc)).await
	}
}

impl fmt::Display for ImpersonateStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "IMPERSONATE {} {}", self.target, self.then)
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum ImpersonationTarget {
	Record(Value, String)
}

impl fmt::Display for ImpersonationTarget {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Record(thing, access) => write!(f, "RECORD {thing} VIA {access}")
		}
	}
}