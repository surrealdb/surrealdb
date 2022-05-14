use super::tx::Transaction;
use crate::ctx::Context;
use crate::dbs::Attach;
use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Response;
use crate::dbs::Session;
use crate::dbs::Variables;
use crate::err::Error;
use crate::key::thing;
use crate::sql;
use crate::sql::query::Query;
use crate::sql::thing::Thing;
use bytes::Bytes;
use channel::Receiver;

/// The underlying datastore instance which stores the dataset.
pub struct Datastore {
	pub(super) inner: Inner,
}

#[allow(clippy::large_enum_variant)]
pub(super) enum Inner {
	#[cfg(feature = "kv-echodb")]
	Mem(super::mem::Datastore),
	#[cfg(feature = "kv-indxdb")]
	IxDB(super::ixdb::Datastore),
	#[cfg(feature = "kv-yokudb")]
	File(super::file::Datastore),
	#[cfg(feature = "kv-tikv")]
	TiKV(super::tikv::Datastore),
}

impl Datastore {
	/// Creates a new datastore instance
	///
	/// # Examples
	///
	/// ```rust,no_run
	/// # use surrealdb::Datastore;
	/// # use surrealdb::Error;
	/// # #[tokio::main]
	/// # async fn main() -> Result<(), Error> {
	/// let ds = Datastore::new("memory").await?;
	/// # Ok(())
	/// # }
	/// ```
	///
	/// Or to create a file-backed store:
	///
	/// ```rust,no_run
	/// # use surrealdb::Datastore;
	/// # use surrealdb::Error;
	/// # #[tokio::main]
	/// # async fn main() -> Result<(), Error> {
	/// let ds = Datastore::new("file://temp.db").await?;
	/// # Ok(())
	/// # }
	/// ```
	///
	/// Or to connect to a tikv-backed distributed store:
	///
	/// ```rust,no_run
	/// # use surrealdb::Datastore;
	/// # use surrealdb::Error;
	/// # #[tokio::main]
	/// # async fn main() -> Result<(), Error> {
	/// let ds = Datastore::new("tikv://127.0.0.1:2379").await?;
	/// # Ok(())
	/// # }
	/// ```
	pub async fn new(path: &str) -> Result<Datastore, Error> {
		match path {
			#[cfg(feature = "kv-echodb")]
			"memory" => {
				info!("Starting kvs store in {}", path);
				super::mem::Datastore::new().await.map(|v| Datastore {
					inner: Inner::Mem(v),
				})
			}
			// Parse and initiate an IxDB database
			#[cfg(feature = "kv-indxdb")]
			s if s.starts_with("ixdb:") => {
				info!("Starting kvs store at {}", path);
				let s = s.trim_start_matches("ixdb://");
				super::ixdb::Datastore::new(s).await.map(|v| Datastore {
					inner: Inner::IxDB(v),
				})
			}
			// Parse and initiate an File database
			#[cfg(feature = "kv-yokudb")]
			s if s.starts_with("file:") => {
				info!("Starting kvs store at {}", path);
				let s = s.trim_start_matches("file://");
				super::file::Datastore::new(s).await.map(|v| Datastore {
					inner: Inner::File(v),
				})
			}
			// Parse and initiate an TiKV database
			#[cfg(feature = "kv-tikv")]
			s if s.starts_with("tikv:") => {
				info!("Starting kvs store at {}", path);
				let s = s.trim_start_matches("tikv://");
				super::tikv::Datastore::new(s).await.map(|v| Datastore {
					inner: Inner::TiKV(v),
				})
			}
			// The datastore path is not valid
			_ => unreachable!(),
		}
	}

	/// Create a new transaction on this datastore
	pub async fn transaction(&self, write: bool, lock: bool) -> Result<Transaction, Error> {
		match &self.inner {
			#[cfg(feature = "kv-echodb")]
			Inner::Mem(v) => {
				let tx = v.transaction(write, lock).await?;
				Ok(Transaction {
					inner: super::tx::Inner::Mem(tx),
				})
			}
			#[cfg(feature = "kv-indxdb")]
			Inner::IxDB(v) => {
				let tx = v.transaction(write, lock).await?;
				Ok(Transaction {
					inner: super::tx::Inner::IxDB(tx),
				})
			}
			#[cfg(feature = "kv-yokudb")]
			Inner::File(v) => {
				let tx = v.transaction(write, lock).await?;
				Ok(Transaction {
					inner: super::tx::Inner::File(tx),
				})
			}
			#[cfg(feature = "kv-tikv")]
			Inner::TiKV(v) => {
				let tx = v.transaction(write, lock).await?;
				Ok(Transaction {
					inner: super::tx::Inner::TiKV(tx),
				})
			}
		}
	}

	/// Parse and execute an SQL query
	pub async fn execute(
		&self,
		txt: &str,
		sess: &Session,
		vars: Variables,
	) -> Result<Vec<Response>, Error> {
		// Create a new query options
		let mut opt = Options::default();
		// Create a new query executor
		let mut exe = Executor::new(self);
		// Create a default context
		let ctx = Context::default();
		// Start an execution context
		let ctx = sess.context(ctx);
		// Store the query variables
		let ctx = vars.attach(ctx);
		// Parse the SQL query text
		let ast = sql::parse(txt)?;
		// Process all statements
		opt.auth = sess.au.clone();
		opt.ns = sess.ns();
		opt.db = sess.db();
		exe.execute(ctx, opt, ast).await
	}

	/// Execute a pre-parsed SQL query
	pub async fn process(
		&self,
		ast: Query,
		sess: &Session,
		vars: Variables,
	) -> Result<Vec<Response>, Error> {
		// Create a new query options
		let mut opt = Options::default();
		// Create a new query executor
		let mut exe = Executor::new(self);
		// Create a default context
		let ctx = Context::default();
		// Start an execution context
		let ctx = sess.context(ctx);
		// Store the query variables
		let ctx = vars.attach(ctx);
		// Process all statements
		opt.auth = sess.au.clone();
		opt.ns = sess.ns();
		opt.db = sess.db();
		exe.execute(ctx, opt, ast).await
	}

	/// Performs a full database export as SQL
	pub async fn export(&self, ns: String, db: String) -> Result<Receiver<Bytes>, Error> {
		// Start a new transaction
		let mut txn = self.transaction(false, false).await?;
		// Create a new channel
		let (chn, rcv) = channel::bounded(10);
		// Spawn the export
		crate::exe::spawn(async move {
			// Output OPTIONS
			{
				chn.send(output!("-- ------------------------------")).await?;
				chn.send(output!("-- OPTION")).await?;
				chn.send(output!("-- ------------------------------")).await?;
				chn.send(output!("")).await?;
				chn.send(output!("OPTION IMPORT;")).await?;
				chn.send(output!("")).await?;
			}
			// Output LOGINS
			{
				let dls = txn.all_dl(&ns, &db).await?;
				if !dls.is_empty() {
					chn.send(output!("-- ------------------------------")).await?;
					chn.send(output!("-- LOGINS")).await?;
					chn.send(output!("-- ------------------------------")).await?;
					chn.send(output!("")).await?;
					for dl in dls {
						chn.send(output!(format!("{};", dl))).await?;
					}
					chn.send(output!("")).await?;
				}
			}
			// Output TOKENS
			{
				let dts = txn.all_dt(&ns, &db).await?;
				if !dts.is_empty() {
					chn.send(output!("-- ------------------------------")).await?;
					chn.send(output!("-- TOKENS")).await?;
					chn.send(output!("-- ------------------------------")).await?;
					chn.send(output!("")).await?;
					for dt in dts {
						chn.send(output!(format!("{};", dt))).await?;
					}
					chn.send(output!("")).await?;
				}
			}
			// Output SCOPES
			{
				let scs = txn.all_sc(&ns, &db).await?;
				if !scs.is_empty() {
					chn.send(output!("-- ------------------------------")).await?;
					chn.send(output!("-- SCOPES")).await?;
					chn.send(output!("-- ------------------------------")).await?;
					chn.send(output!("")).await?;
					for sc in scs {
						chn.send(output!(format!("{};", sc))).await?;
					}
					chn.send(output!("")).await?;
				}
			}
			// Output TABLES
			{
				let tbs = txn.all_tb(&ns, &db).await?;
				if !tbs.is_empty() {
					for tb in &tbs {
						// Output TABLE
						chn.send(output!("-- ------------------------------")).await?;
						chn.send(output!(format!("-- TABLE: {}", tb.name))).await?;
						chn.send(output!("-- ------------------------------")).await?;
						chn.send(output!("")).await?;
						chn.send(output!(format!("{};", tb))).await?;
						chn.send(output!("")).await?;
						// Output FIELDS
						{
							let fds = txn.all_fd(&ns, &db, &tb.name).await?;
							if !fds.is_empty() {
								for fd in &fds {
									chn.send(output!(format!("{};", fd))).await?;
								}
								chn.send(output!("")).await?;
							}
						}
						// Output INDEXS
						let ixs = txn.all_fd(&ns, &db, &tb.name).await?;
						if !ixs.is_empty() {
							for ix in &ixs {
								chn.send(output!(format!("{};", ix))).await?;
							}
							chn.send(output!("")).await?;
						}
						// Output EVENTS
						let evs = txn.all_ev(&ns, &db, &tb.name).await?;
						if !evs.is_empty() {
							for ev in &evs {
								chn.send(output!(format!("{};", ev))).await?;
							}
							chn.send(output!("")).await?;
						}
					}
					// Start transaction
					chn.send(output!("-- ------------------------------")).await?;
					chn.send(output!("-- TRANSACTION")).await?;
					chn.send(output!("-- ------------------------------")).await?;
					chn.send(output!("")).await?;
					chn.send(output!("BEGIN TRANSACTION;")).await?;
					chn.send(output!("")).await?;
					// Output TABLE data
					for tb in &tbs {
						chn.send(output!("-- ------------------------------")).await?;
						chn.send(output!(format!("-- TABLE DATA: {}", tb.name))).await?;
						chn.send(output!("-- ------------------------------")).await?;
						chn.send(output!("")).await?;
						// Fetch records
						let beg = thing::prefix(&ns, &db, &tb.name);
						let end = thing::suffix(&ns, &db, &tb.name);
						let mut nxt: Option<Vec<u8>> = None;
						loop {
							let res = match nxt {
								None => {
									let min = beg.clone();
									let max = end.clone();
									txn.scan(min..max, 1000).await?
								}
								Some(ref mut beg) => {
									beg.push(0x00);
									let min = beg.clone();
									let max = end.clone();
									txn.scan(min..max, 1000).await?
								}
							};
							if !res.is_empty() {
								// Get total results
								let n = res.len();
								// Exit when settled
								if n == 0 {
									break;
								}
								// Loop over results
								for (i, (k, v)) in res.into_iter().enumerate() {
									// Ready the next
									if n == i + 1 {
										nxt = Some(k.clone());
									}
									// Parse the key-value
									let k: crate::key::thing::Thing = (&k).into();
									let v: crate::sql::value::Value = (&v).into();
									let t = Thing::from((k.tb, k.id));
									// Write record
									chn.send(output!(format!("UPDATE {} CONTENT {};", t, v)))
										.await?;
								}
								continue;
							}
							break;
						}
						chn.send(output!("")).await?;
					}
					// Commit transaction
					chn.send(output!("-- ------------------------------")).await?;
					chn.send(output!("-- TRANSACTION")).await?;
					chn.send(output!("-- ------------------------------")).await?;
					chn.send(output!("")).await?;
					chn.send(output!("COMMIT TRANSACTION;")).await?;
					chn.send(output!("")).await?;
				}
			};
			// Everything exported
			Ok::<(), Error>(())
			// Task done
		})
		.detach();
		// Send back the receiver
		Ok(rcv)
	}
}
