use crate::ctx::Context;
use crate::dbs::response::{Response, Responses, Status};
use crate::dbs::Auth;
use crate::dbs::Level;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::err::Error;
use crate::kvs::transaction;
use crate::kvs::Transaction;
use crate::sql::query::Query;
use crate::sql::statement::Statement;
use crate::sql::value::Value;
use futures::lock::Mutex;
use std::sync::Arc;
use std::time::Instant;

#[derive(Default)]
pub struct Executor<'a> {
	pub id: Option<String>,
	pub ns: Option<String>,
	pub db: Option<String>,
	pub txn: Option<Arc<Mutex<Transaction<'a>>>>,
	pub err: Option<Error>,
}

impl<'a> Executor<'a> {
	pub fn new() -> Executor<'a> {
		Executor {
			id: None,
			ns: None,
			db: None,
			..Executor::default()
		}
	}

	pub fn check(&self, opt: &Options, level: Level) -> Result<(), Error> {
		if opt.auth.check(level) == false {
			return Err(Error::QueryPermissionsError);
		}
		if self.ns.is_none() {
			return Err(Error::NsError);
		}
		if self.db.is_none() {
			return Err(Error::DbError);
		}
		Ok(())
	}

	pub fn export(&mut self, ctx: Runtime) -> Result<String, Error> {
		todo!()
	}

	async fn begin(&mut self) -> bool {
		match self.txn {
			Some(_) => false,
			None => match transaction(true, false).await {
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
			match &self.txn {
				Some(txn) => {
					let txn = txn.clone();
					let mut txn = txn.lock().await;
					if let Err(e) = txn.commit().await {
						self.err = Some(e);
					}
					self.txn = None;
				}
				None => unreachable!(),
			}
		}
	}

	async fn cancel(&mut self, local: bool) {
		if local {
			match &self.txn {
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

	async fn finish(&mut self, res: &Result<Value, Error>, local: bool) {
		match res {
			Ok(_) => self.commit(local).await,
			Err(_) => self.cancel(local).await,
		}
	}

	pub fn buf_cancel(&self, v: Response) -> Response {
		Response {
			sql: v.sql,
			time: v.time,
			status: Status::Err,
			detail: Some(format!("{}", Error::QueryCancelledError)),
			result: None,
		}
	}

	pub fn buf_commit(&self, v: Response) -> Response {
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

	pub async fn execute(&mut self, mut ctx: Runtime, qry: Query) -> Result<Responses, Error> {
		// Initialise buffer of responses
		let mut buf: Vec<Response> = vec![];
		// Initialise array of responses
		let mut out: Vec<Response> = vec![];
		// Create a new options
		let mut opt = Options::new(&Auth::No);
		// Process all statements in query
		for stm in qry.statements().iter() {
			// Log the statement
			debug!("{}", stm);
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
				Statement::Begin(stm) => {
					let res = stm.compute(&ctx, &opt, self, None).await;
					if res.is_err() {
						self.err = res.err()
					};
					continue;
				}
				// Cancel a running transaction
				Statement::Cancel(stm) => {
					let res = stm.compute(&ctx, &opt, self, None).await;
					if res.is_err() {
						self.err = res.err()
					};
					buf = buf.into_iter().map(|v| self.buf_cancel(v)).collect();
					out.append(&mut buf);
					self.txn = None;
					continue;
				}
				// Commit a running transaction
				Statement::Commit(stm) => {
					let res = stm.compute(&ctx, &opt, self, None).await;
					if res.is_err() {
						self.err = res.err()
					};
					buf = buf.into_iter().map(|v| self.buf_commit(v)).collect();
					out.append(&mut buf);
					self.txn = None;
					continue;
				}
				// Commit a running transaction
				Statement::Use(stm) => {
					let res = stm.compute(&ctx, &opt, self, None).await;
					res
				}
				// Process param definition statements
				Statement::Set(stm) => {
					match stm.compute(&ctx, &opt, self, None).await {
						Ok(val) => {
							let mut new = Context::new(&ctx);
							let key = stm.name.to_owned();
							new.add_value(key, val);
							ctx = new.freeze();
						}
						_ => break,
					}
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
						let res = stm.compute(&ctx, &opt, self, None).await;
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
						self.finish(&res, loc).await;
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
