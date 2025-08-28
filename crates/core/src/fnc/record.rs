use anyhow::Result;
use reblessive::tree::Stk;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::FlowResultExt as _;
use crate::expr::paths::ID;
use crate::val::{RecordId, Value};

pub async fn exists(
	(stk, ctx, opt, doc): (&mut Stk, &Context, Option<&Options>, Option<&CursorDoc>),
	(arg,): (RecordId,),
) -> Result<Value> {
	if let Some(opt) = opt {
		let v = Value::RecordId(arg).get(stk, ctx, opt, doc, ID.as_ref()).await.catch_return()?;
		Ok(Value::Bool(!v.is_none()))
	} else {
		Ok(Value::None)
	}
}

pub fn id((arg,): (RecordId,)) -> Result<Value> {
	Ok(arg.key.into_value())
}

pub fn tb((arg,): (RecordId,)) -> Result<Value> {
	Ok(arg.table.into())
}

pub mod is {
	use anyhow::Result;
	use reblessive::tree::Stk;

	use crate::ctx::Context;
	use crate::dbs::Options;
	use crate::doc::CursorDoc;
	use crate::err::Error;
	use crate::expr::Base;
	use crate::iam::{Action, ResourceKind};
	use crate::val::value::Cast;
	use crate::val::{RecordId, Value};

	/// Checks if a record is an edge in a graph
	///
	/// This function checks if the given record represents an edge in a graph
	/// by examining its metadata for the Edge record type.
	///
	/// # Arguments
	///
	/// * `arg` - The record to check
	///
	/// # Returns
	///
	/// Returns `true` if the record is an edge, `false` otherwise
	pub async fn edge(
		(_stk, ctx, opt, doc): (&mut Stk, &Context, Option<&Options>, Option<&CursorDoc>),
		(arg,): (Value,),
	) -> Result<Value> {
		match opt {
			Some(opt) => {
				// Cast the input value to a RecordId, returning an error if the cast fails
				let rid = RecordId::cast(arg).map_err(|_| Error::InvalidArguments {
					name: "record::is::edge".to_owned(),
					message: "Expected a record ID".to_owned(),
				})?;

				// We may already know if the record is an edge based on the current document
				// As an example, we may use this function inside a select predicate or filter
				// get_record() can potentially do a new fetch on the KV store, which at scale can
				// be expensive Let's short circuit if the rid matches the current document
				if let Some(doc) = doc {
					if doc.rid.as_ref().is_some_and(|x| x.as_ref() == &rid) {
						return Ok(Value::Bool(doc.doc.is_edge()));
					}
				}

				// Ensure we have a valid database context (namespace and database must be set)
				opt.valid_for_db()?;

				// Check if the user has permission to view records at the database level
				opt.is_allowed(Action::View, ResourceKind::Record, &Base::Db)?;

				// Get the namespace and database IDs
				let (ns, db) = ctx.expect_ns_db_ids(opt).await?;

				// Get the transaction
				let txn = ctx.tx();

				// Fetch the actual record from the database
				let record = txn.get_record(ns, db, &rid.table, &rid.key, opt.version).await?;

				// Check if the record is an edge using the is_edge() method
				Ok(Value::Bool(record.is_edge()))
			}
			None => Ok(Value::None),
		}
	}
}
