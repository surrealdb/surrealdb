use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::{Error, LiveQueryCause};
use crate::fflags::FFLAGS;
use crate::iam::Auth;
use crate::kvs::lq_structs::{LqEntry, TrackedResult};
use crate::sql::statements::info::InfoStructure;
use crate::sql::{Cond, Fetchs, Fields, Table, Uuid, Value};
use derive::Store;
use futures::lock::MutexGuard;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct LiveStatement {
	pub id: Uuid,
	pub node: Uuid,
	pub expr: Fields,
	pub what: Value,
	pub cond: Option<Cond>,
	pub fetch: Option<Fetchs>,
	// When a live query is marked for archiving, this will
	// be set to the node ID that archived the query. This
	// is an internal property, set by the database runtime.
	// This is optional, and is only set when archived.
	//
	// This is deprecated from 2.0
	pub(crate) archived: Option<Uuid>,
	// When a live query is created, we must also store the
	// authenticated session of the user who made the query,
	// so we can check it later when sending notifications.
	// This is optional as it is only set by the database
	// runtime when storing the live query to storage.
	#[revision(start = 2)]
	pub(crate) session: Option<Value>,
	// When a live query is created, we must also store the
	// authenticated session of the user who made the query,
	// so we can check it later when sending notifications.
	// This is optional as it is only set by the database
	// runtime when storing the live query to storage.
	pub(crate) auth: Option<Auth>,
}

impl LiveStatement {
	#[doc(hidden)]
	pub fn new(expr: Fields) -> Self {
		LiveStatement {
			id: Uuid::new_v4(),
			node: Uuid::new_v4(),
			expr,
			..Default::default()
		}
	}

	/// Creates a live statement from parts that can be set during a query.
	pub(crate) fn from_source_parts(
		expr: Fields,
		what: Value,
		cond: Option<Cond>,
		fetch: Option<Fetchs>,
	) -> Self {
		LiveStatement {
			id: Uuid::new_v4(),
			node: Uuid::new_v4(),
			expr,
			what,
			cond,
			fetch,
			..Default::default()
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
		// Is realtime enabled?
		opt.realtime()?;
		// Valid options?
		opt.valid_for_db()?;
		// Get the Node ID
		let nid = opt.id()?;
		// Check that auth has been set
		let mut stm = LiveStatement {
			// Use the current session authentication
			// for when we store the LIVE Statement
			session: ctx.value("session").cloned(),
			// Use the current session authentication
			// for when we store the LIVE Statement
			auth: Some(opt.auth.as_ref().clone()),
			// Clone the rest of the original fields
			// from the LIVE statement to the new one
			..self.clone()
		};
		let id = stm.id.0;
		match FFLAGS.change_feed_live_queries.enabled() {
			true => {
				let mut run = ctx.tx_lock().await;
				match stm.what.compute(stk, ctx, opt, doc).await? {
					Value::Table(tb) => {
						// We modify the table as it can be a $PARAM and the compute evaluates that
						let mut stm = stm;
						stm.what = Value::Table(tb.clone());

						let ns = opt.ns()?.to_string();
						let db = opt.db()?.to_string();
						self.validate_change_feed_valid(&mut run, &ns, &db, &tb).await?;
						// Send the live query registration hook to the transaction pre-commit channel
						run.pre_commit_register_async_event(TrackedResult::LiveQuery(LqEntry {
							live_id: stm.id,
							ns,
							db,
							stm,
						}))?;
					}
					v => {
						return Err(Error::LiveStatement {
							value: v.to_string(),
						});
					}
				}
				Ok(id.into())
			}
			false => {
				// Claim transaction
				let mut run = ctx.tx_lock().await;
				// Process the condition params
				let condition = match stm.cond.as_mut() {
					None => None,
					Some(cond) => Some(Cond(cond.partially_compute(stk, ctx, opt, doc).await?)),
				};
				// Overwrite the condition params
				stm.cond = condition;
				// Process the live query table
				match stm.what.compute(stk, ctx, opt, doc).await? {
					Value::Table(tb) => {
						// Store the current Node ID
						stm.node = nid.into();
						// Insert the node live query
						run.putc_ndlq(nid, id, opt.ns()?, opt.db()?, tb.as_str(), None).await?;
						// Insert the table live query
						run.putc_tblq(opt.ns()?, opt.db()?, &tb, stm, None).await?;
					}
					v => {
						return Err(Error::LiveStatement {
							value: v.to_string(),
						});
					}
				};
				// Return the query id
				Ok(id.into())
			}
		}
	}

	async fn validate_change_feed_valid(
		&self,
		tx: &mut MutexGuard<'_, crate::kvs::Transaction>,
		ns: &str,
		db: &str,
		tb: &Table,
	) -> Result<(), Error> {
		// Find the table definition
		let tb_definition = tx.get_and_cache_tb(ns, db, tb).await.map_err(|e| match e {
			Error::TbNotFound {
				value: _tb,
			} => Error::LiveQueryError(LiveQueryCause::MissingChangeFeed),
			_ => e,
		})?;
		// check it has a change feed
		let cf = tb_definition
			.changefeed
			.ok_or(Error::LiveQueryError(LiveQueryCause::MissingChangeFeed))?;
		// check the change feed includes the original - required for differentiating between CREATE and UPDATE
		if !cf.store_diff {
			return Err(Error::LiveQueryError(LiveQueryCause::ChangeFeedNoOriginal));
		}
		Ok(())
	}

	pub(crate) fn archive(mut self, node_id: Uuid) -> LiveStatement {
		self.archived = Some(node_id);
		self
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

impl InfoStructure for LiveStatement {
	fn structure(self) -> Value {
		Value::from(map! {
			"expr".to_string() => self.expr.structure(),
			"what".to_string() => self.what.structure(),
			"cond".to_string(), if let Some(v) = self.cond => v.structure(),
			"fetch".to_string(), if let Some(v) = self.fetch => v.structure(),
		})
	}
}
