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

	fn txn(&self) -> Transaction {
		match self.txn.as_ref() {
			Some(txn) => txn.clone(),
			None => unreachable!(),
		}
	}

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

	async fn commit(&mut self, local: bool) {
		if local {
			if let Some(txn) = self.txn.as_ref() {
				match &self.err {
					true => {
						let txn = txn.clone();
						let mut txn = txn.lock().await;
						if txn.cancel().await.is_err() {
							self.err = true;
						}
						self.txn = None;
					}
					false => {
						let txn = txn.clone();
						let mut txn = txn.lock().await;
						if txn.commit().await.is_err() {
							self.err = true;
						}
						self.txn = None;
					}
				}
			}
		}
	}

	async fn cancel(&mut self, local: bool) {
		if local {
			if let Some(txn) = self.txn.as_ref() {
				let txn = txn.clone();
				let mut txn = txn.lock().await;
				if txn.cancel().await.is_err() {
					self.err = true;
				}
				self.txn = None;
			}
		}
	}

	fn buf_cancel(&self, v: Response) -> Response {
		Response {
			time: v.time,
			result: Err(Error::QueryCancelled),
		}
	}

	fn buf_commit(&self, v: Response) -> Response {
		match &self.err {
			true => Response {
				time: v.time,
				result: match v.result {
					Ok(_) => Err(Error::QueryNotExecuted),
					Err(e) => Err(e),
				},
			},
			_ => v,
		}
	}

	async fn set_ns(&self, ctx: &mut Context<'_>, opt: &mut Options, ns: &str) {
		let mut session = ctx.value("session").unwrap_or(&Value::None).clone();
		session.put(NS.as_ref(), ns.to_owned().into());
		ctx.add_value(String::from("session"), session);
		opt.ns = Some(ns.into());
	}

	async fn set_db(&self, ctx: &mut Context<'_>, opt: &mut Options, db: &str) {
		let mut session = ctx.value("session").unwrap_or(&Value::None).clone();
		session.put(DB.as_ref(), db.to_owned().into());
		ctx.add_value(String::from("session"), session);
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
		for stm in qry.iter() {
			// Log the statement
			debug!(target: LOG, "Executing: {}", stm);
			// Reset errors
			if self.txn.is_none() {
				self.err = false;
			}
			// Get the statement start time
			let now = Instant::now();
			// Process a single statement
			let res = match stm {
				// Specify runtime options
				Statement::Option(stm) => {
					// Selected DB?
					opt.needs(Level::Db)?;
					// Allowed to run?
					opt.check(Level::Db)?;
					// Process the option
					match &stm.name.to_uppercase()[..] {
						"FIELDS" => opt = opt.fields(stm.what),
						"EVENTS" => opt = opt.events(stm.what),
						"TABLES" => opt = opt.tables(stm.what),
						"IMPORT" => opt = opt.import(stm.what),
						"FORCE" => opt = opt.force(stm.what),
						_ => break,
					}
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
					self.txn = None;
					continue;
				}
				// Commit a running transaction
				Statement::Commit(_) => {
					self.commit(true).await;
					buf = buf.into_iter().map(|v| self.buf_commit(v)).collect();
					out.append(&mut buf);
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
				Statement::Set(stm) => {
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
									name: stm.name.to_owned(),
								}),
							};
							// Check the statement
							match res {
								Ok(val) => {
									// Set the parameter
									ctx.add_value(stm.name.to_owned(), val);
									// Finalise transaction
									match stm.writeable() {
										true => self.commit(loc).await,
										false => self.cancel(loc).await,
									}
									// Return nothing
									Ok(Value::None)
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
								// Finalise transaction
								match &res {
									Ok(_) => match stm.writeable() {
										true => self.commit(loc).await,
										false => self.cancel(loc).await,
									},
									Err(_) => self.cancel(loc).await,
								};
								// Return the result
								res
							}
						}
					}
				},
			};
			// Get the statement end time
			let dur = now.elapsed();
			// Produce the response
			let res = match res {
				Ok(v) => Response {
					time: dur,
					result: Ok(v),
				},
				Err(e) => {
					// Produce the response
					let res = Response {
						time: dur,
						result: Err(e),
					};
					// Mark the error
					self.err = true;
					// Return
					res
				}
			};
			// Output the response
			match self.txn {
				Some(_) => match stm {
					Statement::Output(_) => {
						buf.clear();
						buf.push(res);
					}
					_ => buf.push(res),
				},
				None => out.push(res),
			}
		}
		// Return responses
		Ok(out)
	}
}
