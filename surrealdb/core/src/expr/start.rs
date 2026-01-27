use anyhow::Result;
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};

use super::FlowResultExt as _;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::Expr;
use crate::val::{Number, Value};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct Start(pub(crate) Expr);

impl Start {
	pub(crate) async fn process(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<u64> {
		match stk.run(|stk| self.0.compute(stk, ctx, opt, doc)).await.catch_return() {
			// This is a valid starting number
			Ok(Value::Number(Number::Int(v))) if v >= 0 => Ok(v as u64),
			// An invalid value was specified
			Ok(v) => Err(anyhow::Error::new(Error::InvalidStart {
				value: v.into_raw_string(),
			})),
			// A different error occurred
			Err(e) => Err(e),
		}
	}
}

impl ToSql for Start {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let sql_start: crate::sql::Start = self.clone().into();
		sql_start.fmt_sql(f, fmt);
	}
}
