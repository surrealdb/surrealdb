use crate::ctx::Context;
use crate::dbs::response::{Response, Responses, Status};
use crate::dbs::Auth;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::kvs::Store;
use crate::sql::query::Query;
use crate::sql::statement::Statement;
use crate::sql::value::Value;
use futures::lock::Mutex;
use std::sync::Arc;
use trice::Instant;

pub struct Executor {
	kvs: Store,
	err: Option<Error>,
	txn: Option<Transaction>,
}

impl Executor {
	pub fn new(kvs: Store) -> Executor {
		Executor {
			kvs,
			txn: None,
			err: None,
		}
	}

	fn txn(&self) -> Transaction {
		match self.txn.as_ref() {
			Some(txn) => txn.clone(),
			None => unreachable!(),
		}
	}

	async fn begin(&mut self) -> bool {
		match self.txn.as_ref() {
			Some(_) => false,
			None => match self.kvs.transaction(true, false).await {
				Ok(v) => {
					self.txn = Some(Arc::new(Mutex::new(v)));
					true
				}
				Err(e) => {
					self.err = Some(e);
					false
				}
			},
		}
	}

	async fn commit(&mut self, local: bool) {
		if local {
			match self.txn.as_ref() {
				Some(txn) => match &self.err {
					Some(_) => {
						let txn = txn.clone();
						let mut txn = txn.lock().await;
						if let Err(e) = txn.cancel().await {
							self.err = Some(e);
						}
						self.txn = None;
					}
					None => {
						let txn = txn.clone();
						let mut txn = txn.lock().await;
						if let Err(e) = txn.commit().await {
							self.err = Some(e);
						}
						self.txn = None;
					}
				},
				None => (),
			}
		}
	}

	async fn cancel(&mut self, local: bool) {
		if local {
			match self.txn.as_ref() {
				Some(txn) => {
					let txn = txn.clone();
					let mut txn = txn.lock().await;
					if let Err(e) = txn.cancel().await {
						self.err = Some(e);
					}
					self.txn = None;
				}
				None => unreachable!(),
			}
		}
	}

	fn buf_cancel(&self, v: Response) -> Response {
		Response {
			sql: v.sql,
			time: v.time,
			status: Status::Err,
			detail: Some(format!("{}", Error::QueryCancelledError)),
			result: None,
		}
	}

	fn buf_commit(&self, v: Response) -> Response {
		match &self.err {
			Some(_) => Response {
				sql: v.sql,
				time: v.time,
				status: Status::Err,
				detail: match v.status {
					Status::Ok => Some(format!("{}", Error::QueryExecutionError)),
					Status::Err => v.detail,
				},
				result: None,
			},
			_ => v,
		}
	}

	pub async fn execute(
		&mut self,
		mut ctx: Runtime,
		mut opt: Options,
		qry: Query,
	) -> Result<Responses, Error> {
		// Initialise buffer of responses
		let mut buf: Vec<Response> = vec![];
		// Initialise array of responses
		let mut out: Vec<Response> = vec![];
		// Process all statements in query
		for stm in qry.statements().iter() {
			// Log the statement
			debug!("Executing: {}", stm);
			// Reset errors
			if self.txn.is_none() {
				self.err = None;
			}
			// Get the statement start time
			let now = Instant::now();
			// Process a single statement
			let res = match stm {
				// Specify runtime options
				Statement::Option(stm) => {
					match &stm.name.name.to_uppercase()[..] {
						"FIELD_QUERIES" => opt = opt.fields(stm.what),
						"EVENT_QUERIES" => opt = opt.events(stm.what),
						"TABLE_QUERIES" => opt = opt.tables(stm.what),
						"IMPORT" => opt = opt.import(stm.what),
						"DEBUG" => opt = opt.debug(stm.what),
						_ => break,
					}
					continue;
				}
				// Begin a new transaction
				Statement::Begin(_) => {
					self.begin().await;
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
							Auth::No => opt.ns = Some(Arc::new(ns.to_owned())),
							Auth::Kv => opt.ns = Some(Arc::new(ns.to_owned())),
							Auth::Ns(v) if v == ns => opt.ns = Some(Arc::new(ns.to_owned())),
							_ => {
								opt.ns = None;
								return Err(Error::NsAuthenticationError {
									ns: ns.to_owned(),
								});
							}
						}
					}
					if let Some(ref db) = stm.db {
						match &*opt.auth {
							Auth::No => opt.db = Some(Arc::new(db.to_owned())),
							Auth::Kv => opt.db = Some(Arc::new(db.to_owned())),
							Auth::Ns(_) => opt.db = Some(Arc::new(db.to_owned())),
							Auth::Db(_, v) if v == db => opt.db = Some(Arc::new(db.to_owned())),
							_ => {
								opt.db = None;
								return Err(Error::DbAuthenticationError {
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
					let loc = self.begin().await;
					// Process the statement
					match stm.compute(&ctx, &opt, &self.txn(), None).await {
						Ok(val) => {
							let mut new = Context::new(&ctx);
							let key = stm.name.to_owned();
							new.add_value(key, val);
							ctx = new.freeze();
						}
						_ => break,
					}
					// Cancel transaction
					self.cancel(loc).await;
					// Return nothing
					Ok(Value::None)
				}
				// Process all other normal statements
				_ => match self.err {
					// This transaction has failed
					Some(_) => Err(Error::QueryExecutionError),
					// Compute the statement normally
					None => {
						// Create a transaction
						let loc = self.begin().await;
						// Enable context override
						let mut ctx = Context::new(&ctx).freeze();
						// Specify statement timeout
						if let Some(timeout) = stm.timeout() {
							let mut new = Context::new(&ctx);
							new.add_timeout(timeout);
							ctx = new.freeze();
						}
						// Process the statement
						let res = stm.compute(&ctx, &opt, &self.txn(), None).await;
						// Catch statement timeout
						let res = match stm.timeout() {
							Some(timeout) => match ctx.is_timedout() {
								true => Err(Error::QueryTimeoutError {
									timer: timeout,
								}),
								false => res,
							},
							None => res,
						};
						// Finalise transaction
						match &res {
							Ok(_) => self.commit(loc).await,
							Err(_) => self.cancel(loc).await,
						};
						// Return the result
						res
					}
				},
			};
			// Get the statement end time
			let dur = now.elapsed();
			// Produce the response
			let res = match res {
				Ok(v) => Response {
					sql: match opt.debug {
						true => Some(format!("{}", stm)),
						false => None,
					},
					time: format!("{:?}", dur),
					status: Status::Ok,
					detail: None,
					result: v.output(),
				},
				Err(e) => {
					// Produce the response
					let res = Response {
						sql: match opt.debug {
							true => Some(format!("{}", stm)),
							false => None,
						},
						time: format!("{:?}", dur),
						status: Status::Err,
						detail: Some(format!("{}", e)),
						result: None,
					};
					// Keep the error
					self.err = Some(e);
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
		Ok(Responses(out))
	}
}
