use crate::ctx::Context;
use crate::dbs::Level;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::key::lq::Lq;
use crate::key::lv::Lv;
use crate::sql::comment::shouldbespace;
use crate::sql::cond::{cond, Cond};
use crate::sql::error::IResult;
use crate::sql::fetch::{fetch, Fetchs};
use crate::sql::field::{fields, Fields};
use crate::sql::param::param;
use crate::sql::table::table;
use crate::sql::value::Value;
use derive::Store;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::map;
use nom::combinator::opt;
use nom::sequence::preceded;
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Store, Hash)]
pub struct LiveStatement {
	pub id: Uuid,
	pub node: Uuid,
	pub expr: Fields,
	pub what: Value,
	pub cond: Option<Cond>,
	pub fetch: Option<Fetchs>,

	// Non-query properties

	// When a live query is archived, this should be the node ID that archived the query.
	pub archived: Option<Uuid>,
}

impl LiveStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&Value>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.realtime()?;
		// Selected DB?
		opt.needs(Level::Db)?;
		// Allowed to run?
		opt.check(Level::No)?;
		// Clone transaction
		let run = txn.clone();
		// Claim transaction
		let mut run = run.lock().await;
		// Process the live query table
		match self.what.compute(ctx, opt, txn, doc).await? {
			Value::Table(tb) => {
				// Clone the current statement
				let mut stm = self.clone();
				trace!("The statement is: {:?}", stm);
				// Store the current Node ID
				stm.node = opt.id.clone();
				// Insert the node live query
				let key = crate::key::lq::new(opt.id(), opt.ns(), opt.db(), &self.id);
				let key_enc = Lq::encode(&key)?;
				trace!("Inserting node live query: {}", crate::key::debug::sprint_key(&key_enc));
				run.putc(key_enc, tb.as_str(), None).await?;
				// Insert the table live query
				let key = crate::key::lv::new(opt.ns(), opt.db(), &tb, &self.id);
				let key_enc = Lv::encode(&key)?;
				trace!("Inserting table live query: {:?}", crate::key::debug::sprint_key(&key_enc));
				run.putc(key_enc, stm, None).await?;
			}
			v => {
				return Err(Error::LiveStatement {
					value: v.to_string(),
				})
			}
		};
		// Return the query id
		Ok(self.id.into())
	}

	pub(crate) fn archive(self, node_id: Uuid) -> LiveStatement {
		LiveStatement {
			id: self.id,
			node: self.node,
			expr: self.expr,
			what: self.what,
			cond: self.cond,
			fetch: self.fetch,
			archived: Some(node_id),
		}
	}
}

impl fmt::Display for LiveStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "LIVE SELECT {} FROM {}", self.expr, self.what)?;
		if let Some(ref v) = self.cond {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.fetch {
			write!(f, " {v}")?
		}
		Ok(())
	}
}

pub fn live(i: &str) -> IResult<&str, LiveStatement> {
	let (i, _) = tag_no_case("LIVE SELECT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, expr) = alt((map(tag_no_case("DIFF"), |_| Fields::default()), fields))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("FROM")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, what) = alt((map(param, Value::from), map(table, Value::from)))(i)?;
	let (i, cond) = opt(preceded(shouldbespace, cond))(i)?;
	let (i, fetch) = opt(preceded(shouldbespace, fetch))(i)?;
	Ok((
		i,
		LiveStatement {
			id: Uuid::new_v4(),
			node: Uuid::default(),
			expr,
			what,
			cond,
			fetch,
			archived: None,
		},
	))
}
