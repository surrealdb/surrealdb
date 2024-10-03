use crate::ctx::reason::Reason;
use crate::ctx::Context;
use crate::dbs::response::Response;
use crate::dbs::Force;
use crate::dbs::Options;
use crate::dbs::QueryType;
use crate::err::Error;
use crate::iam::Action;
use crate::iam::ResourceKind;
use crate::kvs::Datastore;
use crate::kvs::TransactionType;
use crate::kvs::{LockType, Transaction};
use crate::sql::paths::DB;
use crate::sql::paths::NS;
use crate::sql::query::Query;
use crate::sql::statement::Statement;
use crate::sql::statements::{OptionStatement, UseStatement};
use crate::sql::value::Value;
use crate::sql::Base;
use futures::{Stream, StreamExt};
use reblessive::TreeStack;
use std::pin::{pin, Pin};
use std::sync::Arc;
use std::time::Duration;
#[cfg(not(target_arch = "wasm32"))]
use tokio::spawn;
use tracing::instrument;
use trice::Instant;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::spawn_local as spawn;

const TARGET: &str = "surrealdb::core::dbs";

pub struct Executor {
	stack: TreeStack,
	results: Vec<Response>,
	opt: Options,
	ctx: Context,
}

impl Executor {
	pub fn new(ctx: Context, opt: Options) -> Self {
		Executor {
			stack: TreeStack::new(),
			results: Vec::new(),
			opt,
			ctx,
		}
	}

	fn execute_use_statement(&mut self, stmt: UseStatement) -> Result<(), Error> {
		let ctx_ref = Arc::get_mut(&mut self.ctx)
			.ok_or_else(|| fail!("Tried to unfreeze a Context with multiple references"))?;

		if let Some(ns) = stmt.ns {
			let mut session = ctx_ref.value("session").unwrap_or(&Value::None).clone();
			self.opt.set_ns(Some(ns.as_str().into()));
			session.put(NS.as_ref(), ns.into());
			ctx_ref.add_value("session", session.into());
		}
		if let Some(db) = stmt.db {
			let mut session = ctx_ref.value("session").unwrap_or(&Value::None).clone();
			self.opt.set_db(Some(db.as_str().into()));
			session.put(DB.as_ref(), db.into());
			ctx_ref.add_value("session", session.into());
		}
		Ok(())
	}

	fn execute_option_statement(&mut self, stmt: OptionStatement) -> Result<(), Error> {
		// Allowed to run?
		self.opt.is_allowed(Action::Edit, ResourceKind::Option, &Base::Db)?;
		// Convert to uppercase
		let mut name = stmt.name.0;
		name.make_ascii_uppercase();
		// Process the option
		match name.as_str() {
			"IMPORT" => {
				self.opt.set_import(stmt.what);
			}
			"FORCE" => {
				let force = if stmt.what {
					Force::All
				} else {
					Force::None
				};
				self.opt.force = force;
			}
			"FUTURES" => {
				if stmt.what {
					self.opt.set_futures(true);
				} else {
					self.opt.set_futures_never();
				}
			}
			_ => {}
		};
		Ok(())
	}

	/// Executes a statement which needs a transaction with the supplied transaction.
	#[instrument(level = "debug", name = "executor", target = "surrealdb::core::dbs", skip_all)]
	async fn execute_transaction_statement(
		&mut self,
		txn: Arc<Transaction>,
		stmt: Statement,
	) -> Result<Value, Error> {
		let res = match stmt {
			Statement::Set(stm) => {
				// Avoid moving in and out of the context via Arc::get_mut
				Arc::get_mut(&mut self.ctx)
					.ok_or_else(|| fail!("Tried to unfreeze a Context with multiple references"))?
					.set_transaction(txn);
				// Run the statement
				match self
					.stack
					.enter(|stk| stm.compute(stk, &self.ctx, &self.opt, None))
					.finish()
					.await
				{
					// TODO: Maybe catch Error::Return?
					// Currently unsure of if that should be handled here.
					Ok(val) => {
						// Set the parameter
						Arc::get_mut(&mut self.ctx)
							.ok_or_else(|| {
								fail!("Tried to unfreeze a Context with multiple references")
							})?
							.add_value(stm.name, val.into());
						// Finalise transaction, returning nothing unless it couldn't commit
						Ok(Value::None)
					}
					Err(err) => Err(err),
				}
			}
			// Process all other normal statements
			stmt => {
				// The transaction began successfully
				// Create a new context for this statement
				Arc::get_mut(&mut self.ctx)
					.ok_or_else(|| fail!("Tried to unfreeze a Context with multiple references"))?
					.set_transaction(txn);
				// Process the statement
				self.stack.enter(|stk| stmt.compute(stk, &self.ctx, &self.opt, None)).finish().await
			}
		};

		// Catch cancelation during running.
		match self.ctx.done() {
			None => {}
			Some(Reason::Timedout) => {
				return Err(Error::QueryTimedout);
			}
			Some(Reason::Canceled) => {
				return Err(Error::QueryCancelled);
			}
		}

		return res;
	}

	/// Execute a query not wrapped in a transaction block.
	async fn execute_bare_statement(
		&mut self,
		kvs: &Datastore,
		stmt: Statement,
	) -> Result<Value, Error> {
		// Don't even try to run if the query should already be finished.
		match self.ctx.done() {
			None => {}
			Some(Reason::Timedout) => {
				return Err(Error::QueryTimedout);
			}
			Some(Reason::Canceled) => {
				return Err(Error::QueryCancelled);
			}
		}

		match stmt {
			// These statements don't need a transaction.
			Statement::Option(stmt) => {
				self.execute_option_statement(stmt)?;
			}
			Statement::Use(stmt) => {
				self.execute_use_statement(stmt)?;
			}
			stmt => {
				let writeable = stmt.writeable();
				let Ok(txn) = kvs.transaction(writeable.into(), LockType::Optimistic).await else {
					return Err(Error::TxFailure);
				};
				let txn = Arc::new(txn);

				let receiver = self.ctx.has_notifications().then(|| {
					let (send, recv) = channel::unbounded();
					self.opt.sender = Some(send);
					recv
				});

				match self.execute_transaction_statement(txn.clone(), stmt).await {
					Ok(value)
					| Err(Error::Return {
						value,
					}) => {
						let mut lock = txn.lock().await;

						// non-writable transactions might return an error on commit.
						// So cancel them instead. This is fine since a non-writable transaction
						// has nothing to commit anyway.
						if !writeable {
							let _ = lock.cancel().await;
							return Ok(value);
						}

						if let Err(e) = lock.complete_changes(false).await {
							let _ = lock.cancel().await;

							return Err(Error::QueryNotExecutedDetail {
								message: e.to_string(),
							});
						}

						if let Err(e) = lock.commit().await {
							return Err(Error::QueryNotExecutedDetail {
								message: e.to_string(),
							});
						}

						// flush notifications.
						if let Some(recv) = receiver {
							self.opt.sender = None;
							if let Some(sink) = self.ctx.notifications() {
								spawn(async move {
									while let Ok(x) = recv.recv().await {
										if sink.send(x).await.is_err() {
											break;
										}
									}
								});
							}
						}

						return Ok(value);
					}
					Err(e) => {
						let _ = txn.cancel().await;
						return Err(e);
					}
				}
			}
		}
		Ok(Value::None)
	}

	/// Execute the begin statement and all statements after which are within a transaction block.
	async fn execute_begin_statement<S>(
		&mut self,
		kvs: &Datastore,
		mut stream: Pin<&mut S>,
	) -> Result<(), Error>
	where
		S: Stream<Item = Result<Statement, Error>>,
	{
		let Ok(txn) = kvs.transaction(TransactionType::Write, LockType::Optimistic).await else {
			// couldn't create a transaction.
			// Fast forward until we hit CANCEL or COMMIT
			while let Some(stmt) = stream.next().await {
				let stmt = stmt?;
				if let Statement::Cancel(_) | Statement::Commit(_) = stmt {
					return Ok(());
				}

				self.results.push(Response {
					time: Duration::ZERO,
					result: Err(Error::QueryNotExecuted),
					query_type: QueryType::Other,
				});
			}

			// Ran out of statements but still didn't hit a COMMIT or CANCEL
			// Just break as we can't do anything else since the query is already
			// effectively canceled.
			return Ok(());
		};

		// Create a sender for this transaction.
		let receiver = self.ctx.has_notifications().then(|| {
			let (send, recv) = channel::unbounded();
			self.opt.sender = Some(send);
			recv
		});

		let txn = Arc::new(txn);
		let start_results = self.results.len();
		let mut skip_remaining = false;

		// loop over the statements until we hit a cancel or a commit statement.
		while let Some(stmt) = stream.next().await {
			let stmt = match stmt {
				Ok(x) => x,
				Err(e) => {
					// make sure the transaction is properly canceled.
					let _ = txn.cancel().await;
					return Err(e);
				}
			};

			// check for timeout and cancelation.
			if let Some(done) = self.ctx.done() {
				// a cancelation happend. Cancel the transaction, fast forward the remaining
				// results and then return.
				let _ = txn.cancel().await;

				for res in &mut self.results[start_results..] {
					res.query_type = QueryType::Other;
					res.result = Err(Error::QueryCancelled);
				}

				while let Some(stmt) = stream.next().await {
					let stmt = stmt?;
					if let Statement::Cancel(_) | Statement::Commit(_) = stmt {
						return Ok(());
					}

					self.results.push(Response {
						time: Duration::ZERO,
						result: Err(match done {
							Reason::Timedout => Error::QueryTimedout,
							Reason::Canceled => Error::QueryCancelled,
						}),
						query_type: QueryType::Other,
					});
				}

				return Ok(());
			}

			trace!(target: TARGET, statement = %stmt, "Executing statement");

			// Fast forward if we hit a return statement.
			if skip_remaining && !matches!(stmt, Statement::Cancel(_) | Statement::Commit(_)) {
				continue;
			}

			let query_type = match stmt {
				Statement::Live(_) => QueryType::Live,
				Statement::Kill(_) => QueryType::Kill,
				_ => QueryType::Other,
			};

			let before = Instant::now();
			let value = match stmt {
				Statement::Option(stmt) => self.execute_option_statement(stmt).map(|_| Value::None),
				Statement::Use(stmt) => self.execute_use_statement(stmt).map(|_| Value::None),
				Statement::Begin(_) => {
					let _ = txn.cancel().await;
					// tried to begin a transaction within a transaction.
					for res in &mut self.results[start_results..] {
						res.query_type = QueryType::Other;
						res.result = Err(Error::QueryCancelled);
					}

					self.results.push(Response {
						time: Duration::ZERO,
						result: Err(Error::QueryNotExecutedDetail {
							message:
								"Tried to start a transaction while another transaction was open"
									.to_string(),
						}),
						query_type: QueryType::Other,
					});

					while let Some(stmt) = stream.next().await {
						let stmt = stmt?;
						if let Statement::Cancel(_) | Statement::Commit(_) = stmt {
							return Ok(());
						}

						self.results.push(Response {
							time: Duration::ZERO,
							result: Err(Error::QueryNotExecuted),
							query_type: QueryType::Other,
						});
					}

					return Ok(());
				}
				Statement::Cancel(_) => {
					let _ = txn.cancel().await;

					for res in &mut self.results[start_results..] {
						res.query_type = QueryType::Other;
						res.result = Err(Error::QueryCancelled);
					}

					self.opt.sender = None;

					return Ok(());
				}
				Statement::Commit(_) => {
					let mut lock = txn.lock().await;

					// complete_changes and then commit.
					// If either error undo results.
					let e = if let Err(e) = lock.complete_changes(false).await {
						let _ = lock.cancel().await;
						e
					} else if let Err(e) = lock.commit().await {
						e
					} else {
						// Successfully commited. everything is fine.

						// flush notifications.
						if let Some(recv) = receiver {
							self.opt.sender = None;
							if let Some(sink) = self.ctx.notifications() {
								spawn(async move {
									while let Ok(x) = recv.recv().await {
										if sink.send(x).await.is_err() {
											break;
										}
									}
								});
							}
						}

						return Ok(());
					};

					// failed to commit
					for res in &mut self.results[start_results..] {
						res.query_type = QueryType::Other;
						res.result = Err(Error::QueryNotExecutedDetail {
							message: e.to_string(),
						});
					}

					self.opt.sender = None;

					return Ok(());
				}
				stmt => {
					skip_remaining = matches!(stmt, Statement::Output(_));

					match self.execute_transaction_statement(txn.clone(), stmt).await {
						Ok(x) => Ok(x),
						Err(Error::Return {
							value,
						}) => {
							skip_remaining = true;
							self.results.truncate(start_results);
							Ok(value)
						}
						Err(e) => {
							for res in &mut self.results[start_results..] {
								res.query_type = QueryType::Other;
								res.result = Err(Error::QueryCancelled);
							}

							// statement return an error. Consume all the other statement until we hit a cancel or commit.
							self.results.push(Response {
								time: before.elapsed(),
								result: Err(e),
								query_type,
							});

							let _ = txn.cancel().await;

							while let Some(stmt) = stream.next().await {
								let stmt = stmt?;
								if let Statement::Cancel(_) | Statement::Commit(_) = stmt {
									return Ok(());
								}

								self.results.push(Response {
									time: Duration::ZERO,
									result: Err(Error::QueryNotExecuted),
									query_type: QueryType::Other,
								});
							}

							self.opt.sender = None;

							// ran out of statements before the transaction ended.
							// Just break as we have nothing else we can do.
							return Ok(());
						}
					}
				}
			};

			self.results.push(Response {
				time: before.elapsed(),
				result: value,
				query_type,
			});
		}

		// we ran out of query but we still have an open transaction.
		// Be reserved and treat this essentially as a CANCEL statement.

		let _ = txn.cancel().await;

		for res in &mut self.results[start_results..] {
			res.query_type = QueryType::Other;
			res.result = Err(Error::QueryCancelled);
		}

		self.opt.sender = None;

		Ok(())
	}

	#[instrument(level = "debug", name = "executor", target = "surrealdb::core::dbs", skip_all)]
	pub async fn execute(
		kvs: &Datastore,
		ctx: Context,
		opt: Options,
		qry: Query,
	) -> Result<Vec<Response>, Error> {
		let stream = futures::stream::iter(qry.into_iter().map(Ok));
		Self::execute_stream(kvs, ctx, opt, stream).await
	}

	#[instrument(level = "debug", name = "executor", target = "surrealdb::core::dbs", skip_all)]
	pub async fn execute_stream<S>(
		kvs: &Datastore,
		ctx: Context,
		opt: Options,
		stream: S,
	) -> Result<Vec<Response>, Error>
	where
		S: Stream<Item = Result<Statement, Error>>,
	{
		let mut this = Executor::new(ctx, opt);
		let mut stream = pin!(stream);

		while let Some(stmt) = stream.next().await {
			let stmt = match stmt {
				Ok(x) => x,
				Err(e) => {
					this.results.push(Response {
						time: Duration::ZERO,
						result: Err(e),
						query_type: QueryType::Other,
					});

					return Ok(this.results);
				}
			};

			match stmt {
				// handle option here because it doesn't produce a result.
				Statement::Begin(_) => {
					if let Err(e) = this.execute_begin_statement(kvs, stream.as_mut()).await {
						this.results.push(Response {
							time: Duration::ZERO,
							result: Err(e),
							query_type: QueryType::Other,
						});

						return Ok(this.results);
					}
				}
				stmt => {
					let query_type = match stmt {
						Statement::Live(_) => QueryType::Live,
						Statement::Kill(_) => QueryType::Kill,
						_ => QueryType::Other,
					};

					let now = Instant::now();
					let result = this.execute_bare_statement(kvs, stmt).await;
					this.results.push(Response {
						time: now.elapsed(),
						result,
						query_type,
					});
				}
			}
		}
		Ok(this.results)
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
