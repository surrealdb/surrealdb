use crate::ctx::{Context, MutableContext};
use crate::dbs::response::Response;
use crate::dbs::Force;
use crate::dbs::Notification;
use crate::dbs::Options;
use crate::dbs::QueryType;
use crate::err::Error;
use crate::iam::Action;
use crate::iam::ResourceKind;
use crate::kvs::Transaction;
use crate::kvs::TransactionType;
use crate::kvs::{Datastore, LockType::*, TransactionType::*};
use crate::sql::paths::DB;
use crate::sql::paths::NS;
use crate::sql::query::Query;
use crate::sql::statement::Statement;
use crate::sql::value::Value;
use crate::sql::Base;
use async_graphql::Response;
use channel::Receiver;
use futures::{Stream, StreamExt};
use reblessive::TreeStack;
use std::pin::pin;
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use tokio::spawn;
use tracing::instrument;
use trice::Instant;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::spawn_local as spawn;

const TARGET: &str = "surrealdb::core::dbs";

pub struct Executor2 {
	err: bool,
	txn: Option<Arc<Transaction>>,
	stack: TreeStack,
	results: Vec<Response>,
	notification: Receiver<Notification>,
}

impl Executor2 {
	pub fn new() -> Self {
		Executor2 {
			err: false,
			txn: None,
			stack: TreeStack::new(),
			results: Vec::new(),
			notification: Receiver::new(),
		}
	}
}

enum ControlFlow {
	Continue,
	Break,
	Return,
}

pub struct ExecutionState {
	stack: TreeStack,
	/// A buffer for the results of a query.
	results: Vec<Response>,
	/// When the current running transaction started.
	transaction_start: Option<usize>,

	notifications: Receiver<Notification>,
}

pub(crate) struct Executor<'a> {
	err: bool,
	kvs: &'a Datastore,
	txn: Option<Arc<Transaction>>,
}

impl<'a> Executor<'a> {
	pub fn new(kvs: &'a Datastore) -> Executor<'a> {
		Executor {
			kvs,
			txn: None,
			err: false,
		}
	}

	fn txn(&self) -> Result<Arc<Transaction>, Error> {
		self.txn.clone().ok_or_else(|| fail!("txn was None after successful begin"))
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
					self.txn = Some(Arc::new(v));
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
				// Lock the transaction
				let mut txn = txn.lock().await;
				// Check for any errors
				if self.err {
					let _ = txn.cancel().await;
				} else {
					//
					if let Err(e) = txn.complete_changes(false).await {
						// Rollback the transaction
						let _ = txn.cancel().await;
						// Return the error message
						self.err = true;
						return Err(e);
					}
					if let Err(e) = txn.commit().await {
						// Rollback the transaction
						let _ = txn.cancel().await;
						// Return the error message
						self.err = true;
						return Err(e);
					};
				}
			}
		}
		Ok(())
	}

	async fn cancel(&mut self, local: bool) {
		if local {
			// Extract the transaction
			if let Some(txn) = self.txn.take() {
				if txn.cancel().await.is_err() {
					self.err = true;
				}
			}
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
	async fn clear(&self, _: &Context, mut rcv: Receiver<Notification>) {
		spawn(async move {
			while rcv.next().await.is_some() {
				// Ignore notification
			}
		});
	}

	/// Flush notifications from a buffer channel (live queries) to the committed notification channel.
	/// This is because we don't want to broadcast notifications to the user for failed transactions.
	async fn flush(&self, ctx: &Context, mut rcv: Receiver<Notification>) {
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

	async fn set_ns(&self, ctx: Context, opt: &mut Options, ns: &str) -> Result<Context, Error> {
		let mut ctx = MutableContext::unfreeze(ctx)?;
		let mut session = ctx.value("session").unwrap_or(&Value::None).clone();
		session.put(NS.as_ref(), ns.to_owned().into());
		ctx.add_value("session", session.into());
		opt.set_ns(Some(ns.into()));
		Ok(ctx.freeze())
	}

	async fn set_db(&self, ctx: Context, opt: &mut Options, db: &str) -> Result<Context, Error> {
		let mut ctx = MutableContext::unfreeze(ctx)?;
		let mut session = ctx.value("session").unwrap_or(&Value::None).clone();
		session.put(DB.as_ref(), db.to_owned().into());
		ctx.add_value("session", session.into());
		opt.set_db(Some(db.into()));
		Ok(ctx.freeze())
	}

	#[instrument(level = "debug", name = "executor", target = "surrealdb::core::dbs", skip_all)]
	pub async fn execute(
		&mut self,
		mut ctx: Context,
		opt: Options,
		qry: Query,
	) -> Result<Vec<Response>, Error> {
		// Create a notification channel
		let (send, recv) = channel::unbounded();

		let mut state = ExecutionState {
			stack: TreeStack::new(),
			notifications: recv,
			results: Vec::new(),
			pending_results: Vec::new(),
		};
		// Do we fast-forward a transaction?
		// Set to true when we encounter a return statement in a transaction
		let mut ff_txn = false;
		// Process all statements in query
		for stm in qry.into_iter() {
			if ff_txn && !matches!(stm, Statement::Cancel(_) | Statement::Commit(_)) {
				continue;
			}

			ff_txn = false;

			match self.execute_statement(&mut ctx, &mut opt, &mut state, stm).await? {
				ControlFlow::Continue => {}
				ControlFlow::Break => break,
				ControlFlow::Return => {
					ff_txn = true;
				}
			}
		}
		// Return responses
		Ok(state.results)
	}

	#[instrument(level = "debug", name = "executor", target = "surrealdb::core::dbs", skip_all)]
	pub async fn execute_stream<S>(
		&mut self,
		mut ctx: Context,
		opt: Options,
		queries: S,
	) -> Result<Vec<Response>, Error>
	where
		S: Stream<Item = Result<Statement, Error>>,
	{
		// Create a notification channel
		let (send, recv) = channel::unbounded();

		let mut state = ExecutionState {
			stack: TreeStack::new(),
			notifications: recv,
			results: Vec::new(),
			transaction_start: None,
		};
		// Set the notification channel
		// Do we fast-forward a transaction?
		// Set to true when we encounter a return statement in a transaction
		let mut ff_txn = false;

		let mut queries = pin!(queries);

		while let Some(stm) = queries.next().await {
			let stm = stm?;

			if ff_txn && !matches!(stm, Statement::Cancel(_) | Statement::Commit(_)) {
				continue;
			}

			ff_txn = false;

			match self.execute_statement(&mut ctx, &mut opt, &mut state, stm).await? {
				ControlFlow::Continue => {}
				ControlFlow::Break => break,
				ControlFlow::Return => {
					ff_txn = true;
				}
			}
		}

		// Return responses
		Ok(state.results)
	}

	#[instrument(level = "debug", name = "executor", target = "surrealdb::core::dbs", skip_all)]
	pub async fn execute_statement(
		&mut self,
		ctx: &mut Context,
		opt: &mut Options,
		state: &mut ExecutionState,
		stm: Statement,
	) -> Result<ControlFlow, Error> {
		// Log the statement
		trace!(target: TARGET, statement = %stm, "Executing statement");
		// Reset errors
		if self.txn.is_none() {
			self.err = false;
		}
		// Get the statement start time
		let now = Instant::now();

		let query_type = match stm {
			Statement::Live(_) => QueryType::Live,
			Statement::Kill(_) => QueryType::Kill,
			_ => QueryType::Other,
		};
		// Check if this is a RETURN statement
		let is_stm_output = matches!(stm, Statement::Output(_));
		// Has this statement returned a value
		let mut has_returned = false;
		// Process a single statement
		let res = match stm {
			// Specify runtime options
			Statement::Option(mut stm) => {
				// Allowed to run?
				opt.is_allowed(Action::Edit, ResourceKind::Option, &Base::Db)?;
				// Convert to uppercase
				stm.name.0.make_ascii_uppercase();
				// Process the option
				match stm.name.0.as_str() {
					"IMPORT" => {
						opt.set_import(stm.what);
					}
					"FORCE" => {
						let force = if stm.what {
							Force::All
						} else {
							Force::None
						};
						opt.force = force;
					}
					"FUTURES" => {
						if stm.what {
							opt.set_futures(true);
						} else {
							opt.set_futures_never();
						}
					}
					_ => return Ok(ControlFlow::Break),
				};
				// Continue
				return Ok(ControlFlow::Continue);
			}
			// Begin a new transaction
			Statement::Begin(_) => {
				if opt.import {
					return Ok(ControlFlow::Continue);
				}
				self.begin(Write).await;
				state.transaction_start = Some(state.results.len());
				return Ok(ControlFlow::Continue);
			}
			// Cancel a running transaction
			Statement::Cancel(_) => {
				if opt.import {
					return Ok(ControlFlow::Continue);
				}

				let update_results =
					state.transaction_start.take().and_then(|x| state.results.get_mut(x..));

				self.cancel(true).await;
				self.clear(ctx, state.notifications.clone()).await;
				debug_assert!(self.txn.is_none(), "cancel(true) should have unset txn");

				if let Some(update_results) = update_results {
					for r in update_results {
						r.result = Err(Error::QueryCancelled);
						r.query_type = QueryType::Other;
					}
				}

				return Ok(ControlFlow::Continue);
			}
			// Commit a running transaction
			Statement::Commit(_) => {
				if opt.import {
					return Ok(ControlFlow::Continue);
				}
				let update_results =
					state.transaction_start.take().and_then(|x| state.results.get_mut(x..));

				// Check for any error that happend during the transaction.
				let error = if self.err {
					self.cancel(true).await;
					Err(None)
				} else if let Err(error) = self.commit(true).await {
					Err(Some(error.to_string()))
				} else {
					Ok(())
				};

				self.err = false;

				// If an error happend we need to update the results to reflect that.
				if let Err(commit_error) = error {
					// if this is none there where no results to update.
					if let Some(update_results) = update_results {
						for r in update_results {
							r.query_type = QueryType::Other;
							if r.result.is_ok() {
								r.result = match commit_error {
									Some(x) => Err(Error::QueryNotExecutedDetail {
										message: x.clone(),
									}),
									None => Err(Error::QueryNotExecuted),
								};
							}
						}
					}
				}
				self.flush(ctx, state.notifications.clone()).await;
				debug_assert!(self.txn.is_none(), "commit(true) should have unset txn");
				return Ok(ControlFlow::Continue);
			}
			// Switch to a different NS or DB
			Statement::Use(stm) => {
				if let Some(ref ns) = stm.ns {
					*ctx = self.set_ns(ctx.clone(), opt, ns).await?;
				}
				if let Some(ref db) = stm.db {
					*ctx = self.set_db(ctx.clone(), opt, db).await?;
				}
				Ok(Value::None)
			}
			// Process param definition statements
			Statement::Set(stm) => {
				// Create a transaction
				let loc = self.begin(stm.writeable().into()).await;
				// Check the transaction
				if self.err {
					// We failed to create a transaction
					return Err(Error::TxFailure);
				}
				// The transaction began successfully
				// ctx.set_transaction(txn)
				let mut c = MutableContext::unfreeze(ctx.clone())?;
				c.set_transaction(self.txn()?);
				*ctx = c.freeze();
				// Check the statement
				match state.stack.enter(|stk| stm.compute(stk, ctx, opt, None)).finish().await {
					Ok(val) => {
						// Check if writeable
						let writeable = stm.writeable();
						// Set the parameter
						let mut c = MutableContext::unfreeze(ctx.clone())?;
						c.add_value(stm.name, val.into());
						*ctx = c.freeze();
						// Finalise transaction, returning nothing unless it couldn't commit
						if writeable {
							match self.commit(loc).await {
								Err(e) => {
									// Clear live query notifications
									self.clear(ctx, state.notifications.clone()).await;
									Err(Error::QueryNotExecutedDetail {
										message: e.to_string(),
									})
								}
								Ok(_) => {
									// Flush live query notifications
									self.flush(ctx, state.notifications.clone()).await;
									Ok(Value::None)
								}
							}
						} else {
							self.cancel(loc).await;
							self.clear(ctx, state.notifications.clone()).await;
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
			// Process all other normal statements
			_ => {
				if self.err {
					// This transaction has failed
					return Err(Error::QueryNotExecuted);
				}
				// Compute the statement normally
				// Create a transaction
				let loc = self.begin(stm.writeable().into()).await;
				// Check the transaction
				if self.err {
					// We failed to create a transaction
					return Err(Error::TxFailure);
				}

				// The transaction began successfully
				// Create a new context for this statement
				let mut ctx = MutableContext::new(ctx);
				// Set the transaction on the context
				ctx.set_transaction(self.txn()?);
				let c = ctx.freeze();
				// Process the statement
				let res = state.stack.enter(|stk| stm.compute(stk, &c, opt, None)).finish().await;
				ctx = MutableContext::unfreeze(c)?;
				// Check if this is a RETURN statement
				let can_return = matches!(
					stm,
					Statement::Output(_)
						| Statement::Value(_)
						| Statement::Ifelse(_)
						| Statement::Foreach(_)
				);
				// Catch global timeout
				if ctx.is_timedout() {
					return Err(Error::QueryTimedout);
				}

				let res = match res {
					Err(Error::Return {
						value,
					}) if can_return => {
						has_returned = true;
						Ok(value)
					}
					res => res,
				};

				let ctx = ctx.freeze();
				// Finalise transaction and return the result.
				if res.is_ok() && stm.writeable() {
					if let Err(e) = self.commit(loc).await {
						// Clear live query notification details
						self.clear(&ctx, state.notifications.clone()).await;
						// The commit failed
						Err(Error::QueryNotExecutedDetail {
							message: e.to_string(),
						})
					} else {
						// Flush the live query change notifications
						self.flush(&ctx, state.notifications.clone()).await;
						res
					}
				} else {
					self.cancel(loc).await;
					// Clear live query notification details
					self.clear(&ctx, state.notifications.clone()).await;
					// Return an error
					res
				}
			}
		};

		self.err = res.is_err();
		// Produce the response
		let res = Response {
			// Get the statement end time
			time: now.elapsed(),
			result: res,
			query_type,
		};

		state.results.push(res);

		// Output the response
		if let Some(x) = state.transaction_start {
			if is_stm_output || has_returned {
				state.results.truncate(x);
				state.results.push(res);
				return Ok(ControlFlow::Return);
			}
			buf.push(res);
		} else {
			out.push(res)
		}

		Ok(ControlFlow::Continue)
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
