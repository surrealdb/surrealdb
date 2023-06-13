use crate::cnf::PROTECTED_PARAM_NAMES;
use crate::ctx::Context;
use crate::dbs::response::Response;
use crate::dbs::Auth;
use crate::dbs::Level;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::dbs::LOG;
use crate::err::Error;
use crate::kvs::Datastore;
use crate::sql::paths::DB;
use crate::sql::paths::NS;
use crate::sql::query::Query;
use crate::sql::statement::Statement;
use crate::sql::value::Value;
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
			},
			_ => v,
		}
	}

	async fn set_ns(&self, ctx: &mut Context<'_>, opt: &mut Options, ns: &str) {
		let mut session = ctx.value("session").unwrap_or(&Value::None).clone();
		session.put(NS.as_ref(), ns.to_owned().into());
		ctx.add_value("session", session);
		opt.ns = Some(ns.into());
	}

	async fn set_db(&self, ctx: &mut Context<'_>, opt: &mut Options, db: &str) {
		let mut session = ctx.value("session").unwrap_or(&Value::None).clone();
		session.put(DB.as_ref(), db.to_owned().into());
		ctx.add_value("session", session);
		opt.db = Some(db.into());
	}

	#[instrument(name = "executor", skip_all)]
	pub async fn execute(
		&mut self,
		mut ctx: Context<'_>,
		mut opt: Options,
		qry: Query,
	) -> Result<Vec<Response>, Error> {
		// Initialise buffer of responses
		let mut buf: Vec<Response> = vec![];
		// Initialise array of responses
		let mut out: Vec<Response> = vec![];
		// Process all statements in query
		for stm in qry.into_iter() {
			// Log the statement
			debug!(target: LOG, "Executing: {}", stm);
			// Reset errors
			if self.txn.is_none() {
				self.err = false;
			}
			// Get the statement start time
			let now = Instant::now();
			// Check if this is a RETURN statement
			let clr = matches!(stm, Statement::Output(_));
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
						"FIELDS" => opt.fields(stm.what),
						"EVENTS" => opt.events(stm.what),
						"TABLES" => opt.tables(stm.what),
						"IMPORT" => opt.import(stm.what),
						"FORCE" => opt.force(stm.what),
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
					buf = buf.into_iter().map(|v| self.buf_cancel(v)).collect();
					out.append(&mut buf);
					debug_assert!(self.txn.is_none(), "cancel(true) should have unset txn");
					continue;
				}
				// Commit a running transaction
				Statement::Commit(_) => {
					let commit_error = self.commit(true).await.err();
					buf = buf.into_iter().map(|v| self.buf_commit(v, &commit_error)).collect();
					out.append(&mut buf);
					debug_assert!(self.txn.is_none(), "commit(true) should have unset txn");
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
								opt.ns = None;
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
								opt.db = None;
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
								false => {
									ctx.add_transaction(self.txn.as_ref());
									stm.compute(&ctx, &opt).await
								}
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
											Err(e) => Err(Error::QueryNotExecutedDetail {
												message: e.to_string(),
											}),
											Ok(_) => Ok(Value::None),
										}
									} else {
										self.cancel(loc).await;
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
								// Process the statement
								let res = match stm.timeout() {
									// There is a timeout clause
									Some(timeout) => {
										// Set statement timeout
										let mut ctx = Context::new(&ctx);
										ctx.add_timeout(timeout);
										ctx.add_transaction(self.txn.as_ref());
										// Process the statement
										let res = stm.compute(&ctx, &opt).await;
										// Catch statement timeout
										match ctx.is_timedout() {
											true => Err(Error::QueryTimedout),
											false => res,
										}
									}
									// There is no timeout clause
									None => {
										ctx.add_transaction(self.txn.as_ref());
										stm.compute(&ctx, &opt).await
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
										// The commit failed
										Err(Error::QueryNotExecutedDetail {
											message: e.to_string(),
										})
									} else {
										// Successful, committed result
										res
									}
								} else {
									self.cancel(loc).await;

									// An error
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
			};
			// Output the response
			if self.txn.is_some() {
				if clr {
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
