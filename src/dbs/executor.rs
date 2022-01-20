use crate::ctx::Context;
use crate::dbs::response::{Response, Responses};
use crate::dbs::Auth;
use crate::dbs::Level;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::err::Error;
use crate::sql::query::Query;
use crate::sql::statement::Statement;
use crate::sql::value::Value;
use std::time::Instant;

const NAME: &'static str = "surreal::exe";

#[derive(Debug, Default)]
pub struct Executor {
	pub id: Option<String>,
	pub ns: Option<String>,
	pub db: Option<String>,
	pub txn: Option<()>,
	pub err: Option<Error>,
}

impl Executor {
	pub fn new() -> Executor {
		Executor {
			id: None,
			ns: None,
			db: None,
			..Executor::default()
		}
	}

	pub fn check(&mut self, opt: &Options, level: Level) -> Result<(), Error> {
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

	pub async fn execute(&mut self, mut ctx: Runtime, qry: Query) -> Result<Responses, Error> {
		// Initialise array of responses
		let mut out: Vec<Response> = vec![];
		// Create a new options
		let mut opt = Options::new(&Auth::No);
		// Process all statements in query
		for stm in qry.statements().iter() {
			// Log the statement
			debug!(target: NAME, "{}", stm);
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
					match &stm.name.name[..] {
						"FIELD_QUERIES" => opt = opt.fields(stm.what),
						"EVENT_QUERIES" => opt = opt.events(stm.what),
						"TABLE_QUERIES" => opt = opt.tables(stm.what),
						"IMPORT" => opt = opt.import(stm.what),
						_ => break,
					}
					continue;
				}
				// Begin a new transaction
				Statement::Begin(stm) => {
					let res = stm.compute(&ctx, &opt, self, None).await;
					self.err = res.err();
					continue;
				}
				// Cancel a running transaction
				Statement::Cancel(stm) => {
					let res = stm.compute(&ctx, &opt, self, None).await;
					self.err = res.err();
					continue;
				}
				// Commit a running transaction
				Statement::Commit(stm) => {
					let res = stm.compute(&ctx, &opt, self, None).await;
					self.err = res.err();
					continue;
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
				_ => {
					// Enable context override
					let mut ctx = Context::new(&ctx).freeze();
					// Specify statement timeout
					if let Some(timeout) = stm.timeout() {
						let mut new = Context::new(&ctx);
						new.add_timeout(timeout);
						ctx = new.freeze();
					}
					// Process statement
					let res = stm.compute(&ctx, &opt, self, None).await;
					// Catch statement timeout
					if let Some(timeout) = stm.timeout() {
						if ctx.is_timedout() {
							self.err = Some(Error::QueryTimeoutError {
								timer: timeout,
							});
						}
					}
					// Continue with result
					res
				}
			};
			// Get the statement end time
			let dur = now.elapsed();
			// Check transaction errors
			match &self.err {
				Some(e) => out.push(Response {
					time: format!("{:?}", dur),
					status: String::from("ERR"),
					detail: Some(format!("{}", e)),
					result: None,
				}),
				None => {
					// Format responses
					match res {
						Ok(v) => out.push(Response {
							time: format!("{:?}", dur),
							status: String::from("OK"),
							detail: None,
							result: v.output(),
						}),
						Err(e) => out.push(Response {
							time: format!("{:?}", dur),
							status: String::from("ERR"),
							detail: Some(format!("{}", e)),
							result: None,
						}),
					}
				}
			}
		}
		// Return responses
		Ok(Responses(out))
	}
}
