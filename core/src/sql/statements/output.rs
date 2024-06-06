use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::fetch::Fetchs;
use crate::sql::value::Value;
use derive::Store;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct OutputStatement {
	pub what: Value,
	pub fetch: Option<Fetchs>,
}

impl OutputStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		self.what.writeable()
	}
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Ensure futures are processed
		let opt = &opt.new_with_futures(true);
		// Process the output value
		let mut val = self.what.compute(stk, ctx, opt, doc).await?;
		// Fetch any
		if let Some(fetchs) = &self.fetch {
			let fields = fetchs.fields();
			for fetch in fetchs.iter() {
				val.fetch(stk, ctx, opt, fetch, &fields).await?;
			}
		}
		//
		Ok(val)
	}
}

impl fmt::Display for OutputStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "RETURN {}", self.what)?;
		if let Some(ref v) = self.fetch {
			write!(f, " {v}")?
		}
		Ok(())
	}
}
