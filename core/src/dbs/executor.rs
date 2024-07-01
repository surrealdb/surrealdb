use std::sync::Arc;

use channel::Receiver;
use futures::lock::Mutex;
use futures::StreamExt;
use reblessive::TreeStack;
#[cfg(not(target_arch = "wasm32"))]
use tokio::spawn;
use tracing::instrument;
use trice::Instant;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::spawn_local as spawn;

use crate::ctx::Context;
use crate::dbs::response::Response;
use crate::dbs::Force;
use crate::dbs::Notification;
use crate::dbs::Options;
use crate::dbs::QueryType;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::iam::Action;
use crate::iam::ResourceKind;
use crate::kvs::lq_structs::TrackedResult;
use crate::kvs::TransactionType;
use crate::kvs::{Datastore, LockType::*, TransactionType::*};
use crate::sql::paths::DB;
use crate::sql::paths::NS;
use crate::sql::query::Query;
use crate::sql::statement::Statement;
use crate::sql::value::Value;
use crate::sql::Base;

pub(crate) struct Executor<'a> {
	err: bool,
	kvs: &'a Datastore,
	txn: Option<Transaction>,
}

impl<'a> Executor<'a> {
	pub fn new(kvs: &'a Datastore) -> Executor<'a> {
		Executor {
			kvs,
			txn: None,
			err: false,
		}
	}

	fn txn(&self) -> Transaction {
		self.txn.clone().expect("unreachable: txn was None after successful begin")
	}

	/// # Return
	/// - true if a new transaction has begun
	/// - false if
	///   - couldn't create transaction (sets err flag)
	///   - a transaction has already begun
	async fn begin(&mut self, write: TransactionType) -> bool {
		match self.txn.as_ref() {
			Some(_) => false,
			None => match self.kvs.transaction(write, Optimistic).await {
				Ok(v) => {
					self.txn = Some(Arc::new(Mutex::new(v)));
					true
				}
				Err(_) => {
					self.err = true;
					false
				}
			},
		}
	}

	/// Commits the transaction if it is local.
	///
	/// # Return
	///
	/// An `Err` if the transaction could not be committed;
	/// otherwise returns `Ok`.
	async fn commit(&mut self, local: bool) -> Result<(), Error> {
		if local {
			// Extract the transaction
			if let Some(txn) = self.txn.take() {
				let mut txn = txn.lock().await;
				if self.err {
					// Cancel and ignore any error because the error flag was
					// already set
					let _ = txn.cancel().await;
				} else {
					let r = match txn.complete_changes(false).await {
						Ok(_) => {
							match txn.commit().await {
								Ok(()) => {
									// Commit succeeded, do post commit operations that do not matter to the tx
									let lqs: Vec<TrackedResult> =
										txn.consume_pending_live_queries();
									// Track the live queries in the data store
									self.kvs.handle_postprocessing_of_statements(&lqs).await?;
									Ok(())
								}
								Err(e) => Err(e),
							}
						}
						r => r,
					};
					if let Err(e) = r {
						// Transaction failed to commit
						//
						// TODO: Not all commit errors definitively mean
						// the transaction didn't commit. Detect that and tell
						// the user.
						self.err = true;
						return Err(e);
					}
				}
			}
		}
		Ok(())
	}

	async fn cancel(&mut self, local: bool) {
		if local {
			// Extract the transaction
			if let Some(txn) = self.txn.take() {
				let mut txn = txn.lock().await;
				if txn.cancel().await.is_err() {
					self.err = true;
				}
			}
		}
	}

	fn buf_cancel(&self, v: Response) -> Response {
		Response {
			time: v.time,
			result: Err(Error::QueryCancelled),
			query_type: QueryType::Other,
		}
	}

	fn buf_commit(&self, v: Response, commit_error: &Option<Error>) -> Response {
		match &self.err {
			true => Response {
				time: v.time,
				result: match v.result {
					Ok(_) => Err(commit_error
						.as_ref()
						.map(|e| Error::QueryNotExecutedDetail {
							message: e.to_string(),
						})
						.unwrap_or(Error::QueryNotExecuted)),
					Err(e) => Err(e),
				},
				query_type: QueryType::Other,
			},
			_ => v,
		}
	}

	/// Consume the live query notifications
	async fn clear(&self, _: &Context<'_>, mut rcv: Receiver<Notification>) {
		spawn(async move {
			while rcv.next().await.is_some() {
				// Ignore notification
			}
		});
	}

	/// Flush notifications from a buffer channel (live queries) to the committed notification channel.
	/// This is because we don't want to broadcast notifications to the user for failed transactions.
	/// TODO we can delete this once we migrate to lq v2
	async fn flush(&self, ctx: &Context<'_>, mut rcv: Receiver<Notification>) {
		let sender = ctx.notifications();
		spawn(async move {
			while let Some(notification) = rcv.next().await {
				if let Some(chn) = &sender {
					if chn.send(notification).await.is_err() {
						break;
					}
				}
			}
		});
	}

	/// A transaction collects created live queries which can then be consumed when a transaction is committed
	/// We use this function to get these transactions and send them to the invoker without channels
	async fn consume_committed_live_query_registrations(&self) -> Option<Vec<TrackedResult>> {
		if let Some(txn) = self.txn.as_ref() {
			let txn = txn.lock().await;
			Some(txn.consume_pending_live_queries())
		} else {
			None
		}
	}

	async fn set_ns(&self, ctx: &mut Context<'_>, opt: &mut Options, ns: &str) {
		let mut session = ctx.value("session").unwrap_or(&Value::None).clone();
		session.put(NS.as_ref(), ns.to_owned().into());
		ctx.add_value("session", session);
		opt.set_ns(Some(ns.into()));
	}

	async fn set_db(&self, ctx: &mut Context<'_>, opt: &mut Options, db: &str) {
		let mut session = ctx.value("session").unwrap_or(&Value::None).clone();
		session.put(DB.as_ref(), db.to_owned().into());
		ctx.add_value("session", session);
		opt.set_db(Some(db.into()));
	}

	#[instrument(level = "debug", name = "executor", skip_all)]
	pub async fn execute(
		&mut self,
		mut ctx: Context<'_>,
		opt: Options,
		qry: Query,
	) -> Result<(Vec<Response>, Vec<TrackedResult>), Error> {
		// The stack to run the executor in.
		let mut stack = TreeStack::new();

		// Create a notification channel
		let (send, recv) = channel::unbounded();
		// Set the notification channel
		let mut opt = opt.new_with_sender(send);
		// Initialise buffer of responses
		let mut buf: Vec<Response> = vec![];
		// Initialise array of responses
		let mut out: Vec<Response> = vec![];
		let mut live_queries: Vec<TrackedResult> = vec![];
		// Process all statements in query
		for stm in qry.into_iter() {
			// Log the statement
			debug!("Executing: {}", stm);
			// Reset errors
			if self.txn.is_none() {
				self.err = false;
			}
			// Get the statement start time
			let now = Instant::now();
			// Check if this is a LIVE statement
			let is_stm_live = matches!(stm, Statement::Live(_));
			// Check if this is a KILL statement
			let is_stm_kill = matches!(stm, Statement::Kill(_));
			// Check if this is a RETURN statement
			let is_stm_output = matches!(stm, Statement::Output(_));
			// Process a single statement
			let res = match stm {
				// Specify runtime options
				Statement::Option(mut stm) => {
					// Allowed to run?
					opt.is_allowed(Action::Edit, ResourceKind::Option, &Base::Db)?;
					// Convert to uppercase
					stm.name.0.make_ascii_uppercase();
					// Process the option
					opt = match stm.name.0.as_str() {
						"IMPORT" => opt.with_import(stm.what),
						"FORCE" => opt.with_force(if stm.what {
							Force::All
						} else {
							Force::None
						}),
						_ => break,
					};
					// Continue
					continue;
				}
				// Begin a new transaction
				Statement::Begin(_) => {
					self.begin(Write).await;
					continue;
				}
				// Cancel a running transaction
				Statement::Cancel(_) => {
					self.cancel(true).await;
					self.clear(&ctx, recv.clone()).await;
					buf = buf.into_iter().map(|v| self.buf_cancel(v)).collect();
					out.append(&mut buf);
					debug_assert!(self.txn.is_none(), "cancel(true) should have unset txn");
					self.txn = None;
					continue;
				}
				// Commit a running transaction
				Statement::Commit(_) => {
					let commit_error = self.commit(true).await.err();
					buf = buf.into_iter().map(|v| self.buf_commit(v, &commit_error)).collect();
					self.flush(&ctx, recv.clone()).await;
					if let Some(lqs) = self.consume_committed_live_query_registrations().await {
						live_queries.extend(lqs);
					}
					out.append(&mut buf);
					debug_assert!(self.txn.is_none(), "commit(true) should have unset txn");
					self.txn = None;
					continue;
				}
				// Switch to a different NS or DB
				Statement::Use(stm) => {
					if let Some(ref ns) = stm.ns {
						self.set_ns(&mut ctx, &mut opt, ns).await;
					}
					if let Some(ref db) = stm.db {
						self.set_db(&mut ctx, &mut opt, db).await;
					}
					Ok(Value::None)
				}
				// Process param definition statements
				Statement::Set(stm) => {
					// Create a transaction
					let loc = self.begin(stm.writeable().into()).await;
					// Check the transaction
					match self.err {
						// We failed to create a transaction
						true => Err(Error::TxFailure),
						// The transaction began successfully
						false => {
							ctx.set_transaction_mut(self.txn());
							// Check the statement
							match stack
								.enter(|stk| stm.compute(stk, &ctx, &opt, None))
								.finish()
								.await
							{
								Ok(val) => {
									// Check if writeable
									let writeable = stm.writeable();
									// Set the parameter
									ctx.add_value(stm.name, val);
									// Finalise transaction, returning nothing unless it couldn't commit
									if writeable {
										match self.commit(loc).await {
											Err(e) => {
												// Clear live query notifications
												self.clear(&ctx, recv.clone()).await;
												Err(Error::QueryNotExecutedDetail {
													message: e.to_string(),
												})
											}
											Ok(_) => {
												// Flush live query notifications
												self.flush(&ctx, recv.clone()).await;
												if let Some(lqs) = self
													.consume_committed_live_query_registrations()
													.await
												{
													live_queries.extend(lqs);
												}
												Ok(Value::None)
											}
										}
									} else {
										self.cancel(loc).await;
										self.clear(&ctx, recv.clone()).await;
										Ok(Value::None)
									}
								}
								Err(err) => {
									// Cancel transaction
									self.cancel(loc).await;
									// Return error
									Err(err)
								}
							}
						}
					}
				}
				// Process all other normal statements
				_ => match self.err {
					// This transaction has failed
					true => Err(Error::QueryNotExecuted),
					// Compute the statement normally
					false => {
						// Create a transaction
						let loc = self.begin(stm.writeable().into()).await;
						// Check the transaction
						match self.err {
							// We failed to create a transaction
							true => Err(Error::TxFailure),
							// The transaction began successfully
							false => {
								let mut ctx = Context::new(&ctx);
								// Process the statement
								let res = match stm.timeout() {
									// There is a timeout clause
									Some(timeout) => {
										// Set statement timeout or propagate the error
										if let Err(err) = ctx.add_timeout(timeout) {
											Err(err)
										} else {
											ctx.set_transaction_mut(self.txn());
											// Process the statement
											let res = stack
												.enter(|stk| stm.compute(stk, &ctx, &opt, None))
												.finish()
												.await;
											// Catch statement timeout
											match ctx.is_timedout() {
												true => Err(Error::QueryTimedout),
												false => res,
											}
										}
									}
									// There is no timeout clause
									None => {
										ctx.set_transaction_mut(self.txn());
										stack
											.enter(|stk| stm.compute(stk, &ctx, &opt, None))
											.finish()
											.await
									}
								};
								// Catch global timeout
								let res = match ctx.is_timedout() {
									true => Err(Error::QueryTimedout),
									false => res,
								};
								// Finalise transaction and return the result.
								if res.is_ok() && stm.writeable() {
									if let Err(e) = self.commit(loc).await {
										// Clear live query notification details
										self.clear(&ctx, recv.clone()).await;
										// The commit failed
										Err(Error::QueryNotExecutedDetail {
											message: e.to_string(),
										})
									} else {
										// Flush the live query change notifications
										self.flush(&ctx, recv.clone()).await;
										if let Some(lqs) =
											self.consume_committed_live_query_registrations().await
										{
											live_queries.extend(lqs);
										}
										res
									}
								} else {
									self.cancel(loc).await;
									// Clear live query notification details
									self.clear(&ctx, recv.clone()).await;
									// Return an error
									res
								}
							}
						}
					}
				},
			};
			// Produce the response
			let res = Response {
				// Get the statement end time
				time: now.elapsed(),
				result: res.inspect_err(|&e| {
					// Mark the error.
					self.err = true;
					e
				}),
				query_type: match (is_stm_live, is_stm_kill) {
					(true, _) => {
						if let Some(lqs) = self.consume_committed_live_query_registrations().await {
							live_queries.extend(lqs);
						}
						QueryType::Live
					}
					(_, true) => {
						if let Some(lqs) = self.consume_committed_live_query_registrations().await {
							live_queries.extend(lqs);
						}
						QueryType::Kill
					}
					_ => QueryType::Other,
				},
			};
			// Output the response
			if self.txn.is_some() {
				if is_stm_output {
					buf.clear();
				}
				buf.push(res);
			} else {
				out.push(res)
			}
		}
		// Return responses
		Ok((out, live_queries))
	}
}

#[cfg(test)]
mod tests {
	use crate::{dbs::Session, iam::Role, kvs::Datastore};

	#[tokio::test]
	async fn check_execute_option_permissions() {
		let tests = vec![
            // Root level
            (Session::for_level(().into(), Role::Owner).with_ns("NS").with_db("DB"), true, "owner at root level should be able to set options"),
            (Session::for_level(().into(), Role::Editor).with_ns("NS").with_db("DB"), true, "editor at root level should be able to set options"),
            (Session::for_level(().into(), Role::Viewer).with_ns("NS").with_db("DB"), false, "viewer at root level should not be able to set options"),

            // Namespace level
            (Session::for_level(("NS", ).into(), Role::Owner).with_ns("NS").with_db("DB"), true, "owner at namespace level should be able to set options on its namespace"),
            (Session::for_level(("NS", ).into(), Role::Owner).with_ns("OTHER_NS").with_db("DB"), false, "owner at namespace level should not be able to set options on another namespace"),
            (Session::for_level(("NS", ).into(), Role::Editor).with_ns("NS").with_db("DB"), true, "editor at namespace level should be able to set options on its namespace"),
            (Session::for_level(("NS", ).into(), Role::Editor).with_ns("OTHER_NS").with_db("DB"), false, "editor at namespace level should not be able to set options on another namespace"),
            (Session::for_level(("NS", ).into(), Role::Viewer).with_ns("NS").with_db("DB"), false, "viewer at namespace level should not be able to set options on its namespace"),

            // Database level
            (Session::for_level(("NS", "DB").into(), Role::Owner).with_ns("NS").with_db("DB"), true, "owner at database level should be able to set options on its database"),
            (Session::for_level(("NS", "DB").into(), Role::Owner).with_ns("NS").with_db("OTHER_DB"), false, "owner at database level should not be able to set options on another database"),
            (Session::for_level(("NS", "DB").into(), Role::Owner).with_ns("OTHER_NS").with_db("DB"), false, "owner at database level should not be able to set options on another namespace even if the database name matches"),
            (Session::for_level(("NS", "DB").into(), Role::Editor).with_ns("NS").with_db("DB"), true, "editor at database level should be able to set options on its database"),
            (Session::for_level(("NS", "DB").into(), Role::Editor).with_ns("NS").with_db("OTHER_DB"), false, "editor at database level should not be able to set options on another database"),
            (Session::for_level(("NS", "DB").into(), Role::Editor).with_ns("OTHER_NS").with_db("DB"), false, "editor at database level should not be able to set options on another namespace even if the database name matches"),
            (Session::for_level(("NS", "DB").into(), Role::Viewer).with_ns("NS").with_db("DB"), false, "viewer at database level should not be able to set options on its database"),
        ];
		let statement = "OPTION IMPORT = false";

		for test in tests.iter() {
			let (session, should_succeed, msg) = test;

			{
				let ds = Datastore::new("memory").await.unwrap().with_auth_enabled(true);

				let res = ds.execute(statement, session, None).await;

				if *should_succeed {
					assert!(res.is_ok(), "{}: {:?}", msg, res);
				} else {
					let err = res.unwrap_err().to_string();
					assert!(
						err.contains("Not enough permissions to perform this action"),
						"{}: {}",
						msg,
						err
					)
				}
			}
		}

		// Anonymous with auth enabled
		{
			let ds = Datastore::new("memory").await.unwrap().with_auth_enabled(true);

			let res =
				ds.execute(statement, &Session::default().with_ns("NS").with_db("DB"), None).await;

			let err = res.unwrap_err().to_string();
			assert!(
				err.contains("Not enough permissions to perform this action"),
				"anonymous user should not be able to set options: {}",
				err
			)
		}

		// Anonymous with auth disabled
		{
			let ds = Datastore::new("memory").await.unwrap().with_auth_enabled(false);

			let res =
				ds.execute(statement, &Session::default().with_ns("NS").with_db("DB"), None).await;

			assert!(
				res.is_ok(),
				"anonymous user should be able to set options when auth is disabled: {:?}",
				res
			)
		}
	}

	#[tokio::test]
	async fn check_execute_timeout() {
		// With small timeout
		{
			let ds = Datastore::new("memory").await.unwrap();
			let stmt = "UPDATE test TIMEOUT 2s";
			let res = ds.execute(stmt, &Session::default().with_ns("NS").with_db("DB"), None).await;
			assert!(res.is_ok(), "Failed to execute statement with small timeout: {:?}", res);
		}
		// With large timeout
		{
			let ds = Datastore::new("memory").await.unwrap();
			let stmt = "UPDATE test TIMEOUT 31540000s"; // 1 year
			let res = ds.execute(stmt, &Session::default().with_ns("NS").with_db("DB"), None).await;
			assert!(res.is_ok(), "Failed to execute statement with large timeout: {:?}", res);
		}
		// With very large timeout
		{
			let ds = Datastore::new("memory").await.unwrap();
			let stmt = "UPDATE test TIMEOUT 9460800000000000000s"; // 300 billion years
			let res = ds.execute(stmt, &Session::default().with_ns("NS").with_db("DB"), None).await;
			assert!(res.is_ok(), "Failed to execute statement with very large timeout: {:?}", res);
			let err = res.unwrap()[0].result.as_ref().unwrap_err().to_string();
			assert!(
				err.contains("Invalid timeout"),
				"Expected to find invalid timeout error: {:?}",
				err
			);
		}
	}
}
