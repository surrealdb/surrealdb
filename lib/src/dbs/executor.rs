use crate::cnf::PROTECTED_PARAM_NAMES;
use crate::ctx::Context;
use crate::dbs::response::Response;
use crate::dbs::Level;
use crate::dbs::Notification;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::dbs::{Auth, QueryType};
use crate::err::Error;
use crate::kvs::Datastore;
use crate::sql::paths::DB;
use crate::sql::paths::NS;
use crate::sql::query::Query;
use crate::sql::statement::Statement;
use crate::sql::value::Value;
use channel::Receiver;
use futures::lock::Mutex;
use std::sync::Arc;
use tracing::instrument;
use trice::Instant;

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
	async fn begin(&mut self, write: bool) -> bool {
		match self.txn.as_ref() {
			Some(_) => false,
			None => match self.kvs.transaction(write, false).await {
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

	/// # Return
	///
	/// An `Err` if the transaction could not be commited;
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
				} else if let Err(e) = txn.commit().await {
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
	async fn clear(&self, _: &Context<'_>, rcv: Receiver<Notification>) {
		while rcv.try_recv().is_ok() {
			// Ignore notification
		}
	}

	/// Flush notifications from a buffer channel (live queries) to the committed notification channel.
	/// This is because we don't want to broadcast notifications to the user for failed transactions.
	async fn flush(&self, ctx: &Context<'_>, rcv: Receiver<Notification>) {
		if let Some(chn) = ctx.notifications() {
			while let Ok(v) = rcv.try_recv() {
				let _ = chn.send(v).await;
			}
		} else {
			while rcv.try_recv().is_ok() {
				// Ignore notification
			}
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

	#[instrument(name = "executor", skip_all)]
	pub async fn execute(
		&mut self,
		mut ctx: Context<'_>,
		opt: Options,
		qry: Query,
	) -> Result<Vec<Response>, Error> {
		// Create a notification channel
		let (send, recv) = channel::unbounded();
		// Set the notification channel
		let mut opt = opt.new_with_sender(send);
		// Initialise buffer of responses
		let mut buf: Vec<Response> = vec![];
		// Initialise array of responses
		let mut out: Vec<Response> = vec![];
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
					// Selected DB?
					opt.needs(Level::Db)?;
					// Allowed to run?
					opt.check(Level::Db)?;
					// Convert to uppercase
					stm.name.0.make_ascii_uppercase();
					// Process the option
					opt = match stm.name.0.as_str() {
						"FIELDS" => opt.with_fields(stm.what),
						"EVENTS" => opt.with_events(stm.what),
						"TABLES" => opt.with_tables(stm.what),
						"IMPORT" => opt.with_import(stm.what),
						"FORCE" => opt.with_force(stm.what),
						_ => break,
					};
					// Continue
					continue;
				}
				// Begin a new transaction
				Statement::Begin(_) => {
					self.begin(true).await;
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
					out.append(&mut buf);
					debug_assert!(self.txn.is_none(), "commit(true) should have unset txn");
					self.txn = None;
					continue;
				}
				// Switch to a different NS or DB
				Statement::Use(stm) => {
					if let Some(ref ns) = stm.ns {
						match &*opt.auth {
							Auth::No => self.set_ns(&mut ctx, &mut opt, ns).await,
							Auth::Kv => self.set_ns(&mut ctx, &mut opt, ns).await,
							Auth::Ns(v) if v == ns => self.set_ns(&mut ctx, &mut opt, ns).await,
							Auth::Db(v, _) if v == ns => self.set_ns(&mut ctx, &mut opt, ns).await,
							_ => {
								opt.set_ns(None);
								return Err(Error::NsNotAllowed {
									ns: ns.to_owned(),
								});
							}
						}
					}
					if let Some(ref db) = stm.db {
						match &*opt.auth {
							Auth::No => self.set_db(&mut ctx, &mut opt, db).await,
							Auth::Kv => self.set_db(&mut ctx, &mut opt, db).await,
							Auth::Ns(_) => self.set_db(&mut ctx, &mut opt, db).await,
							Auth::Db(_, v) if v == db => self.set_db(&mut ctx, &mut opt, db).await,
							_ => {
								opt.set_db(None);
								return Err(Error::DbNotAllowed {
									db: db.to_owned(),
								});
							}
						}
					}
					Ok(Value::None)
				}
				// Process param definition statements
				Statement::Set(mut stm) => {
					// Create a transaction
					let loc = self.begin(stm.writeable()).await;
					// Check the transaction
					match self.err {
						// We failed to create a transaction
						true => Err(Error::TxFailure),
						// The transaction began successfully
						false => {
							// Check if the variable is a protected variable
							let res = match PROTECTED_PARAM_NAMES.contains(&stm.name.as_str()) {
								// The variable isn't protected and can be stored
								false => stm.compute(&ctx, &opt, &self.txn(), None).await,
								// The user tried to set a protected variable
								true => Err(Error::InvalidParam {
									// Move the parameter name, as we no longer need it
									name: std::mem::take(&mut stm.name),
								}),
							};
							// Check the statement
							match res {
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
						let loc = self.begin(stm.writeable()).await;
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
										// Set statement timeout
										ctx.add_timeout(timeout);
										// Process the statement
										let res = stm.compute(&ctx, &opt, &self.txn(), None).await;
										// Catch statement timeout
										match ctx.is_timedout() {
											true => Err(Error::QueryTimedout),
											false => res,
										}
									}
									// There is no timeout clause
									None => stm.compute(&ctx, &opt, &self.txn(), None).await,
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
										// Successful, committed result
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
				// TODO: Replace with `inspect_err` once stable.
				result: res.map_err(|e| {
					// Mark the error.
					self.err = true;
					e
				}),
				query_type: match (is_stm_live, is_stm_kill) {
					(true, _) => QueryType::Live,
					(_, true) => QueryType::Kill,
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
		Ok(out)
	}
}
