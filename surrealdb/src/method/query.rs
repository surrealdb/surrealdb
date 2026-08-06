use std::borrow::Cow;
use std::collections::HashMap;
use std::future::IntoFuture;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::StreamExt;
use futures::future::Either;
use futures::stream::SelectAll;
use indexmap::IndexMap;
use surrealdb_engine_api::QUERY_STREAM_BUFFER;
use surrealdb_rpc::{DbResultStats, QueryStreamItem, QueryType};
use surrealdb_types::Error as TypesError;
use uuid::Uuid;

use super::transaction::WithTransaction;
use crate::conn::ctx_txn;
use crate::method::live::{Stream, spawn};
use crate::method::{BoxFuture, OnceLockExt, Stats, WithStats};
use crate::notification::Notification;
use crate::types::{SurrealValue, Value, Variables};
use crate::{Connection, Error, Result, Surreal, opt};

/// Returned by [`Surreal::query`](crate::Surreal::query), resolving to [`IndexedResults`]
/// (optionally via [`Query::with_stats`](Self::with_stats)).
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Query<'r, C: Connection> {
	pub(crate) txn: Option<Uuid>,
	pub(crate) client: Cow<'r, Surreal<C>>,
	pub(crate) queries: Vec<Cow<'r, str>>,
	pub(crate) variables: Result<Variables>,
}

impl<C> WithTransaction for Query<'_, C>
where
	C: Connection,
{
	fn with_transaction(mut self, id: Uuid) -> Self {
		self.txn = Some(id);
		self
	}
}

pub trait IntoVariables {
	fn into_variables(self) -> Result<Variables>;
}

impl<T: SurrealValue> IntoVariables for T {
	fn into_variables(self) -> Result<Variables> {
		let value = self.into_value();
		match value {
			Value::Object(obj) => Ok(Variables::from(obj)),
			Value::Array(arr) => {
				let mut vars = Variables::new();
				for v in arr.chunks(2) {
					let key = v[0].clone().into_string().map_err(|_| {
						Error::validation("Variable key must be a string".to_string(), None)
					})?;
					let value = v[1].clone();
					vars.insert(key, value);
				}
				Ok(vars)
			}
			unexpected => {
				Err(Error::validation(format!("Invalid variables type: {unexpected:?}"), None))
			}
		}
	}
}

impl<'r, C> Query<'r, C>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different
	/// thread
	pub fn into_owned(self) -> Query<'static, C> {
		Query {
			txn: self.txn,
			client: Cow::Owned(self.client.into_owned()),
			queries: self.queries.into_iter().map(|q| Cow::Owned(q.into_owned())).collect(),
			variables: self.variables,
		}
	}

	/// Chains an additional query statement onto this query builder
	///
	/// This allows multiple queries to be built up and sent together, with
	/// results accessible by index via `.take(0)`, `.take(1)`, etc.
	///
	/// # Examples
	///
	/// ```no_run
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// use surrealdb::RecordId;
	///
	/// let id = RecordId::from_table_key("user", "john");
	/// let mut response = db
	///     .query("SELECT * FROM $id<-knows.*")
	///     .query("SELECT * FROM $id->knows.*")
	///     .bind(("id", id))
	///     .await?;
	///
	/// let followers = response.take::<Vec<_>>(0)?;
	/// let following = response.take::<Vec<_>>(1)?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn query(mut self, query: impl Into<Cow<'r, str>>) -> Self {
		self.queries.push(query.into());
		self
	}

	/// Run the query and receive its results as they are produced, rather than
	/// waiting for all of them.
	///
	/// Awaiting a [`Query`] gives [`IndexedResults`], which means holding the
	/// whole result set in memory and seeing none of it until the last row is
	/// read. This yields [`StreamItem`]s instead, so a large `SELECT` can be
	/// processed — or forwarded — while the server is still producing it.
	///
	/// # Rows are provisional until their statement finishes
	///
	/// A statement's rows arrive before its outcome is known: it can still fail
	/// on a later row, and a `BEGIN … COMMIT` block can still roll back. The
	/// [`StatementEnd`](StreamItem::StatementEnd) item is what confirms them,
	/// and one carrying an error retracts every row that preceded it. Anything
	/// that acts on rows as they arrive has to be able to undo that.
	///
	/// Only some connections stream for real. The others answer from the
	/// buffered path and replay the results, so this is always correct but only
	/// sometimes earlier.
	///
	/// # `LIVE SELECT` is not for this
	///
	/// Awaiting a [`Query`] registers each `LIVE SELECT` it ran and hands back a
	/// notification stream for it. This does not: a live query's id arrives as
	/// an ordinary row, and nothing subscribes to it. Use the awaited form for
	/// live queries, and this for reading rows.
	///
	/// # Examples
	///
	/// ```no_run
	/// # use futures::StreamExt;
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// use surrealdb::method::StreamItem;
	///
	/// let mut rows = db.query("SELECT * FROM person").stream_items()?;
	/// while let Some(item) = rows.next().await {
	///     match item? {
	///         StreamItem::Row { value, .. } => println!("{value:?}"),
	///         StreamItem::StatementEnd { result, .. } => result?,
	///     }
	/// }
	/// # Ok(())
	/// # }
	/// ```
	pub fn stream_items(self) -> Result<ItemStream> {
		let Self {
			txn,
			client,
			queries,
			variables,
		} = self;
		let engine = Arc::clone(&client.inner.router.extract()?.engine);
		let query = join_queries(&queries);
		let variables = variables?;
		let ctx = ctx_txn(client.session_id, txn);

		let (out_tx, out_rx) = crate::channel::bounded(STREAM_ITEM_BUFFER);
		let (raw_tx, raw_rx) = crate::channel::bounded(QUERY_STREAM_BUFFER);
		// Closing this channel is what stops the execution, and a `Receiver`
		// clone closes it for every holder — so the returned stream can stop the
		// execution without also being the thing that drains it.
		let stop = StopHandle {
			items: Box::new(raw_rx.clone()),
		};
		// The execution and the drain have to make progress together -- the
		// channel between them is bounded, so each blocks on the other -- and
		// the caller only drains the far end. Both therefore run on a task of
		// their own.
		spawn(async move {
			let run = engine.query_stream(ctx, Cow::Owned(query), variables, raw_tx);
			let forward = async {
				while let Ok(item) = raw_rx.recv().await {
					for item in stream_items_for(item) {
						if out_tx.send(Ok(item)).await.is_err() {
							// The caller stopped reading. Closing the execution's
							// channel is what unparks it: it is bounded, and this
							// loop was the only thing draining it.
							raw_rx.close();
							return;
						}
					}
				}
			};
			// Joined, not selected: the execution owns an open transaction and
			// finalises it as it completes, so it is driven to the end even once
			// nothing is left to receive its results.
			let (outcome, ()) = futures::future::join(run, forward).await;
			// A failure belonging to no single statement ends the stream, as
			// distinct from a statement reporting its own.
			if let Err(error) = outcome {
				let _ = out_tx.send(Err(error)).await;
			}
		});

		Ok(ItemStream {
			items: Box::pin(out_rx),
			stop,
		})
	}
}

/// How far a streaming query may run ahead of the caller reading it.
///
/// Each item is one row, so this is deliberately larger than the batch-sized
/// buffer the engine uses underneath. It still bounds the query by the caller's
/// read speed, which is what stops an abandoned stream from running a scan to
/// completion into memory nobody drains.
const STREAM_ITEM_BUFFER: usize = 256;

/// Rewrites one execution item as the items a caller sees.
///
/// Rows are flattened: whether a statement's value arrived as a batch of rows
/// or as a single value is how the buffered API decides between an array and a
/// bare value, but a caller reading rows one at a time has already made that
/// choice.
fn stream_items_for(item: QueryStreamItem) -> Vec<StreamItem> {
	match item {
		QueryStreamItem::Rows {
			index,
			values,
		} => values
			.into_iter()
			.map(|value| StreamItem::Row {
				statement: index,
				value,
			})
			.collect(),
		QueryStreamItem::Value {
			index,
			value,
		} => vec![StreamItem::Row {
			statement: index,
			value,
		}],
		QueryStreamItem::Finished {
			index,
			time,
			query_type,
			error,
		} => vec![StreamItem::StatementEnd {
			statement: index,
			stats: DbResultStats::default().with_execution_time(time).with_query_type(query_type),
			result: match error {
				Some(error) => Err(error),
				None => Ok(()),
			},
		}],
	}
}

/// One item of a query's results, as they are produced.
///
/// See [`Query::stream_items`], and note that a [`Row`](Self::Row) is
/// provisional until its statement's [`StatementEnd`](Self::StatementEnd)
/// confirms it.
///
/// Deliberately exhaustive, unlike most types here: a caller matches on every
/// item a query produces, so a wildcard arm would silently discard any kind
/// added later. Failing to compile is the right way to learn about one.
#[derive(Debug)]
pub enum StreamItem {
	/// One row of a statement's results.
	Row {
		/// Which statement produced it, counting from zero.
		statement: usize,
		/// The row.
		value: Value,
	},
	/// A statement is finished. No further item carries it.
	StatementEnd {
		/// Which statement, counting from zero.
		statement: usize,
		/// What the statement cost.
		stats: DbResultStats,
		/// Whether it succeeded. An error retracts every row it emitted.
		result: Result<()>,
	},
}

/// Stops the execution behind an [`ItemStream`] by closing the channel it sends
/// its results into.
///
/// The execution's sends then fail and it reads the channel as closed, which is
/// both what unparks one blocked on a full channel and what tells it to stop. It
/// is observed only where the execution sends, so a statement in a phase that
/// emits nothing runs on until it next produces something — reaching its own end,
/// and finalising its transaction there, rather than stopping early.
///
/// The receiver is boxed only to keep it `Unpin`, which [`ItemStream`]'s own
/// `Stream` impl needs; nothing here polls it.
#[derive(Debug)]
struct StopHandle {
	items: Box<crate::channel::Receiver<QueryStreamItem>>,
}

impl StopHandle {
	fn stop(&self) {
		self.items.close();
	}
}

/// A stream of a query's results, from [`Query::stream_items`].
///
/// Dropping this stops the query. The execution behind it holds an open
/// transaction that it finalises as it completes, so it is stopped rather than
/// abandoned: an execution left parked on a channel nobody drains would hold
/// that transaction open for the lifetime of the process.
#[derive(Debug)]
#[must_use = "streams do nothing unless you poll them"]
pub struct ItemStream {
	items: Pin<Box<crate::channel::Receiver<Result<StreamItem>>>>,
	stop: StopHandle,
}

impl futures::Stream for ItemStream {
	type Item = Result<StreamItem>;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		self.items.as_mut().poll_next(cx)
	}
}

impl Drop for ItemStream {
	/// Stops the execution as the stream goes away.
	///
	/// Dropping the receiving end of the caller-facing channel is not enough on
	/// its own: the execution sends into a channel further back, and that one
	/// closing is what it observes. Left open, an execution that had already
	/// filled it would stay parked on a send with nothing to drain it.
	fn drop(&mut self) {
		self.stop.stop();
	}
}

/// Joins a builder's queries into one script.
fn join_queries(queries: &[Cow<'_, str>]) -> String {
	queries
		.iter()
		.map(|q| q.trim_end_matches(|c: char| c == ';' || c.is_whitespace()))
		.collect::<Vec<_>>()
		.join("; ")
}

impl<'r, Client> IntoFuture for Query<'r, Client>
where
	Client: Connection,
{
	type Output = Result<IndexedResults>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		let Self {
			txn,
			client,
			queries,
			variables,
		} = self;

		Box::pin(async move {
			// Extract the router from the client
			let router = client.inner.router.extract()?;
			let query = join_queries(&queries);

			let results = router
				.query_results(ctx_txn(client.session_id, txn), Cow::Owned(query), variables?)
				.await?;

			let mut indexed_results = IndexedResults::new();

			for (index, result) in results.into_iter().enumerate() {
				let stats = DbResultStats::default()
					.with_execution_time(result.time)
					.with_query_type(result.query_type);

				match result.query_type {
					QueryType::Other => {
						indexed_results.results.insert(index, (stats, result.result));
					}
					QueryType::Live => {
						let value = result.result?;
						let live_query_id =
							value.into_uuid().map_err(|e| Error::internal(e.to_string()))?;
						let live_stream = crate::method::live::register(
							router,
							live_query_id.into(),
							client.session_id,
						)
						.await
						.map(|rx| {
							Stream::new(
								Arc::clone(&client.inner).into(),
								live_query_id.into(),
								Some(rx),
							)
						});
						indexed_results.live_queries.insert(index, live_stream);
						indexed_results
							.results
							.insert(index, (stats, Ok(Value::Uuid(live_query_id))));
					}
					QueryType::Kill => {}
				}
			}

			Ok(indexed_results)
		})
	}
}

impl<'r, Client> IntoFuture for WithStats<Query<'r, Client>>
where
	Client: Connection,
{
	type Output = Result<WithStats<IndexedResults>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let response = self.0.await?;
			Ok(WithStats(response))
		})
	}
}

impl<'r, C> Query<'r, C>
where
	C: Connection,
{
	/// Return query statistics along with its results
	pub const fn with_stats(self) -> WithStats<Self> {
		WithStats(self)
	}

	/// Binds a parameter or parameters to a query
	///
	/// # Examples
	///
	/// Binding a key/value tuple
	///
	/// ```no_run
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// let response = db.query("CREATE user SET name = $name")
	///     .bind(("name", "John Doe"))
	///     .await?;
	/// # Ok(())
	/// # }
	/// ```
	///
	/// Binding an object
	///
	/// ```no_run
	/// use serde::Serialize;
	///
	/// #[derive(Serialize)]
	/// struct User<'a> {
	///     name: &'a str,
	/// }
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// let response = db.query("CREATE user SET name = $name")
	///     .bind(User {
	///         name: "John Doe",
	///     })
	///     .await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn bind(self, vars: impl IntoVariables) -> Self {
		let variables = match (self.variables, vars.into_variables()) {
			(Ok(mut a), Ok(b)) => {
				a.extend(b);
				Ok(a)
			}
			(Ok(_a), Err(e)) => Err(e),
			(Err(e), Ok(_b)) => Err(e),
			(Err(e), Err(_f)) => Err(e),
		};

		Query {
			txn: self.txn,
			client: self.client,
			queries: self.queries,
			variables,
		}
	}
}

/// Map of per-statement results from [`Surreal::query`](crate::Surreal::query); read rows with
/// [`IndexedResults::take`](IndexedResults::take).
#[derive(Debug)]
pub struct IndexedResults {
	pub(crate) results: IndexMap<usize, (DbResultStats, std::result::Result<Value, TypesError>)>,
	pub(crate) live_queries: IndexMap<usize, Result<Stream<Value>>>,
}

/// A `LIVE SELECT` stream from the `query` method
#[derive(Debug)]
#[must_use = "streams do nothing unless you poll them"]
pub struct QueryStream<R>(pub(crate) Either<Stream<R>, SelectAll<Stream<R>>>);

impl futures::Stream for QueryStream<Value> {
	type Item = Result<Notification<Value>>;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		self.as_mut().0.poll_next_unpin(cx)
	}
}

impl<R> futures::Stream for QueryStream<Notification<R>>
where
	R: SurrealValue + Unpin,
{
	type Item = Result<Notification<R>>;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		self.as_mut().0.poll_next_unpin(cx)
	}
}

impl IndexedResults {
	pub(crate) fn new() -> Self {
		Self {
			results: Default::default(),
			live_queries: Default::default(),
		}
	}

	/// Returns a mutable reference to the `Ok` value at the given index.
	/// If the result is an error, the entry is removed and the error is returned.
	/// Returns `Ok(None)` if no entry exists at the index.
	pub(crate) fn try_get_value_mut(&mut self, index: usize) -> Result<Option<&mut Value>> {
		if matches!(self.results.get(&index), Some((_, Err(_)))) {
			let Some((_, Err(err))) = self.results.swap_remove(&index) else {
				unreachable!()
			};
			return Err(err);
		}
		match self.results.get_mut(&index) {
			Some((_, Ok(val))) => Ok(Some(val)),
			_ => Ok(None),
		}
	}

	/// Takes and returns records returned from the database
	///
	/// A query that only returns one result can be deserialized into an
	/// `Option<T>`, while those that return multiple results should be
	/// deserialized into a `Vec<T>`, `LinkedList<T>` or `HashSet<T>`.
	///
	/// # Examples
	///
	/// ```no_run
	/// use serde::Deserialize;
	/// use surrealdb::RecordId;
	///
	/// #[derive(Debug, Deserialize)]
	/// struct User {
	///     id: RecordId,
	///     balance: String
	/// }
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// #
	/// // Run multiple queries in a single request
	/// let mut response = db.query("
	///     SELECT * FROM user:john;
	///     SELECT * FROM user WHERE name.first = 'John';
	///     SELECT address FROM user:john;
	///     SELECT address FROM user;
	/// ").await?;
	///
	/// // Get the first (and only) user from the first query
	/// let user: Option<User> = response.take(0)?;
	///
	/// // Get all users from the second query
	/// let users: Vec<User> = response.take(1)?;
	///
	/// // Retrieve John's address without making a special struct for it
	/// let address: Option<String> = response.take((2, "address"))?;
	///
	/// // Get all users' addresses
	/// let addresses: Vec<String> = response.take((3, "address"))?;
	///
	/// // You can continue taking more fields on the same response
	/// // object when extracting individual fields
	/// let mut response = db.query("SELECT * FROM user").await?;
	///
	/// // Since the query we want to access is at index 0, we can use
	/// // a shortcut instead of `response.take((0, "field"))`
	/// let ids: Vec<String> = response.take("id")?;
	/// let names: Vec<String> = response.take("name")?;
	/// let addresses: Vec<String> = response.take("address")?;
	/// #
	/// # Ok(())
	/// # }
	/// ```
	///
	/// The indices are stable. Taking one index doesn't affect the numbering
	/// of the other indices, so you can take them in any order you see fit.
	pub fn take<R>(&mut self, index: impl opt::QueryResult<R>) -> Result<R>
	where
		R: SurrealValue,
	{
		index.query_result(self)
	}

	/// Takes and streams records returned from a `LIVE SELECT` query
	///
	/// This is the counterpart to [IndexedResults::take] used to stream the results
	/// of a live query.
	///
	/// # Examples
	///
	/// ```no_run
	/// use serde::Deserialize;
	/// use surrealdb::Notification;
	/// use surrealdb::RecordId;
	/// use surrealdb::Value;
	///
	/// #[derive(Debug, Deserialize)]
	/// struct User {
	///     id: RecordId,
	///     balance: String
	/// }
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// #
	/// let mut response = db
	///     // Stream all changes to the user table
	///     .query("LIVE SELECT * FROM user")
	///     .await?;
	///
	/// // Stream the result of the live query at the given index
	/// // while deserialising into the User type
	/// let mut stream = response.stream::<Notification<User>>(0)?;
	///
	/// // Stream raw values instead
	/// let mut stream = response.stream::<Value>(0)?;
	///
	/// // Combine and stream all `LIVE SELECT` statements in this query
	/// let mut stream = response.stream::<Value>(())?;
	/// #
	/// # Ok(())
	/// # }
	/// ```
	///
	/// Consume the stream the same way you would any other type that implements
	/// `futures::Stream`.
	pub fn stream<R>(&mut self, index: impl opt::QueryStream<R>) -> Result<QueryStream<R>> {
		index.query_stream(self)
	}

	/// Take all errors from the query response
	///
	/// The errors are keyed by the corresponding index of the statement that
	/// failed. Afterwards the response is left with only statements that did
	/// not produce any errors.
	///
	/// # Examples
	///
	/// ```no_run
	/// 
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// # let mut response = db.query("SELECT * FROM user").await?;
	/// let errors = response.take_errors();
	/// # Ok(())
	/// # }
	/// ```
	pub fn take_errors(&mut self) -> HashMap<usize, Error> {
		let mut keys = Vec::new();
		for (key, result) in &self.results {
			if result.1.is_err() {
				keys.push(*key);
			}
		}
		let mut errors = HashMap::with_capacity(keys.len());
		for key in keys {
			if let Some((_, Err(error))) = self.results.swap_remove(&key) {
				errors.insert(key, error);
			}
		}
		errors
	}

	/// Check query response for errors and return the first error, if any, or
	/// the response
	///
	/// # Examples
	///
	/// ```no_run
	/// 
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// # let response = db.query("SELECT * FROM user").await?;
	/// response.check()?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn check(mut self) -> Result<Self> {
		let mut first_error = None;
		for (key, result) in &self.results {
			if result.1.is_err() {
				first_error = Some(*key);
				break;
			}
		}
		if let Some(key) = first_error
			&& let Some((_, Err(error))) = self.results.swap_remove(&key)
		{
			return Err(error);
		}
		Ok(self)
	}

	/// Returns the number of statements in the query
	///
	/// # Examples
	///
	/// ```no_run
	/// 
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// let response = db.query("SELECT * FROM user:john; SELECT * FROM user;").await?;
	///
	/// assert_eq!(response.num_statements(), 2);
	/// #
	/// # Ok(())
	/// # }
	pub fn num_statements(&self) -> usize {
		self.results.len()
	}
}

impl WithStats<IndexedResults> {
	/// Takes and returns records returned from the database
	///
	/// Similar to [IndexedResults::take] but this method returns `None` when
	/// you try taking an index that doesn't correspond to a query
	/// statement.
	///
	/// # Examples
	///
	/// ```no_run
	/// use serde::Deserialize;
	/// use surrealdb::RecordId;
	///
	/// #[derive(Debug, Deserialize)]
	/// struct User {
	///     id: RecordId,
	///     balance: String
	/// }
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// #
	/// // Run multiple queries in a single request with stats
	/// let mut response = db.query("
	///     SELECT * FROM user:john;
	///     SELECT * FROM user WHERE name.first = 'John';
	///     SELECT address FROM user:john;
	///     SELECT address FROM user;
	/// ")
	///     .with_stats()
	///     .await?;
	///
	/// // Get the first (and only) user from the first query
	/// if let Some((stats, result)) = response.take(0) {
	///     let execution_time = stats.execution_time;
	///     let user: Option<User> = result?;
	/// }
	///
	/// // Get all users from the second query
	/// if let Some((stats, result)) = response.take(1) {
	///     let execution_time = stats.execution_time;
	///     let users: Vec<User> = result?;
	/// }
	///
	/// // Retrieve John's address without making a special struct for it
	/// if let Some((stats, result)) = response.take((2, "address")) {
	///     let execution_time = stats.execution_time;
	///     let address: Option<String> = result?;
	/// }
	///
	/// // Get all users' addresses
	/// if let Some((stats, result)) = response.take((3, "address")) {
	///     let execution_time = stats.execution_time;
	///     let addresses: Vec<String> = result?;
	/// }
	/// #
	/// # Ok(())
	/// # }
	/// ```
	pub fn take<R>(&mut self, index: impl opt::QueryResult<R>) -> Option<(Stats, Result<R>)>
	where
		R: SurrealValue,
	{
		let db_stats = index.stats(&self.0)?;
		let stats = Stats {
			execution_time: db_stats.execution_time,
		};
		let result = index.query_result(&mut self.0);
		Some((stats, result))
	}

	/// Take all errors from the query response
	///
	/// The errors are keyed by the corresponding index of the statement that
	/// failed. Afterwards the response is left with only statements that did
	/// not produce any errors.
	///
	/// # Examples
	///
	/// ```no_run
	/// 
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// # let mut response = db.query("SELECT * FROM user").await?;
	/// let errors = response.take_errors();
	/// # Ok(())
	/// # }
	/// ```
	pub fn take_errors(&mut self) -> HashMap<usize, (Stats, Error)> {
		let mut keys = Vec::new();
		for (key, result) in &self.0.results {
			if result.1.is_err() {
				keys.push(*key);
			}
		}
		let mut errors = HashMap::with_capacity(keys.len());
		for key in keys {
			if let Some((db_stats, Err(error))) = self.0.results.swap_remove(&key) {
				let stats = Stats {
					execution_time: db_stats.execution_time,
				};
				errors.insert(key, (stats, error));
			}
		}
		errors
	}

	/// Check query response for errors and return the first error, if any, or
	/// the response
	///
	/// # Examples
	///
	/// ```no_run
	/// 
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// # let response = db.query("SELECT * FROM user").await?;
	/// response.check()?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn check(self) -> Result<Self> {
		let response = self.0.check()?;
		Ok(Self(response))
	}

	/// Returns the number of statements in the query
	///
	/// # Examples
	///
	/// ```no_run
	/// 
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// let response = db.query("SELECT * FROM user:john; SELECT * FROM user;").await?;
	///
	/// assert_eq!(response.num_statements(), 2);
	/// #
	/// # Ok(())
	/// # }
	pub fn num_statements(&self) -> usize {
		self.0.num_statements()
	}

	/// Returns the unwrapped response
	pub fn into_inner(self) -> IndexedResults {
		self.0
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[derive(Debug, Clone, SurrealValue)]
	#[surreal(crate = "crate::types")]
	struct Summary {
		title: String,
	}

	#[derive(Debug, Clone, SurrealValue)]
	#[surreal(crate = "crate::types")]
	struct Article {
		title: String,
		body: String,
	}

	fn to_map(
		vec: Vec<std::result::Result<Value, TypesError>>,
	) -> IndexMap<usize, (DbResultStats, std::result::Result<Value, TypesError>)> {
		vec.into_iter()
			.map(|result| match result {
				Ok(result) => {
					let stats = DbResultStats::default();
					(stats, Ok(result))
				}
				Err(error) => {
					let stats = DbResultStats::default();
					(stats, Err(error))
				}
			})
			.enumerate()
			.collect()
	}

	#[test]
	fn take_from_an_empty_response() {
		let mut response = IndexedResults::new();
		let value: Value = response.take(0).unwrap();
		assert!(value.is_none());

		let mut response = IndexedResults::new();
		let option: Option<String> = response.take(0).unwrap();
		assert!(option.is_none());

		let mut response = IndexedResults::new();
		let vec: Vec<String> = response.take(0).unwrap();
		assert!(vec.is_empty());
	}

	#[test]
	fn take_from_an_errored_query() {
		let mut response = IndexedResults {
			results: to_map(vec![Err(TypesError::internal(
				"Unimportant error message".to_string(),
			))]),
			..IndexedResults::new()
		};
		response.take::<Option<()>>(0).unwrap_err();
	}

	#[test]
	fn take_from_empty_records() {
		let mut response = IndexedResults {
			results: to_map(vec![]),
			..IndexedResults::new()
		};
		let value: Value = response.take(0).unwrap();
		assert_eq!(value, Value::None);

		let mut response = IndexedResults {
			results: to_map(vec![]),
			..IndexedResults::new()
		};
		let option: Option<String> = response.take(0).unwrap();
		assert!(option.is_none());

		let mut response = IndexedResults {
			results: to_map(vec![]),
			..IndexedResults::new()
		};
		let vec: Vec<String> = response.take(0).unwrap();
		assert!(vec.is_empty());
	}

	#[test]
	fn take_from_a_scalar_response() {
		let scalar = 265;

		let mut response = IndexedResults {
			results: to_map(vec![Ok(Value::from_int(scalar))]),
			..IndexedResults::new()
		};
		let value: Value = response.take(0).unwrap();
		assert_eq!(value, Value::from_t(scalar));

		let mut response = IndexedResults {
			results: to_map(vec![Ok(Value::from_int(scalar))]),
			..IndexedResults::new()
		};
		let option: Option<_> = response.take(0).unwrap();
		assert_eq!(option, Some(scalar));

		let mut response = IndexedResults {
			results: to_map(vec![Ok(Value::from_int(scalar))]),
			..IndexedResults::new()
		};
		let vec: Vec<i64> = response.take(0).unwrap();
		assert_eq!(vec, vec![scalar]);

		let scalar = true;

		let mut response = IndexedResults {
			results: to_map(vec![Ok(Value::from_bool(scalar))]),
			..IndexedResults::new()
		};
		let value: Value = response.take(0).unwrap();
		assert_eq!(value, Value::from_t(scalar));

		let mut response = IndexedResults {
			results: to_map(vec![Ok(Value::from_bool(scalar))]),
			..IndexedResults::new()
		};
		let option: Option<_> = response.take(0).unwrap();
		assert_eq!(option, Some(scalar));

		let mut response = IndexedResults {
			results: to_map(vec![Ok(Value::from_bool(scalar))]),
			..IndexedResults::new()
		};
		let vec: Vec<bool> = response.take(0).unwrap();
		assert_eq!(vec, vec![scalar]);
	}

	#[test]
	fn take_preserves_order() {
		let mut response = IndexedResults {
			results: to_map(vec![
				Ok(Value::from_int(0)),
				Ok(Value::from_int(1)),
				Ok(Value::from_int(2)),
				Ok(Value::from_int(3)),
				Ok(Value::from_int(4)),
				Ok(Value::from_int(5)),
				Ok(Value::from_int(6)),
				Ok(Value::from_int(7)),
			]),
			..IndexedResults::new()
		};
		let Some(four): Option<i32> = response.take(4).unwrap() else {
			panic!("query not found");
		};
		assert_eq!(four, 4);
		let Some(six): Option<i32> = response.take(6).unwrap() else {
			panic!("query not found");
		};
		assert_eq!(six, 6);
		let Some(zero): Option<i32> = response.take(0).unwrap() else {
			panic!("query not found");
		};
		assert_eq!(zero, 0);
		let one: Value = response.take(1).unwrap();
		assert_eq!(one, Value::from_int(1));
	}

	#[test]
	fn take_key() {
		let summary = Summary {
			title: "Lorem Ipsum".to_owned(),
		};
		let value = summary.clone().into_value();

		let mut response = IndexedResults {
			results: to_map(vec![Ok(value.clone())]),
			..IndexedResults::new()
		};
		let title: Value = response.take("title").unwrap();
		assert_eq!(title, Value::String(summary.title.clone()));

		let mut response = IndexedResults {
			results: to_map(vec![Ok(value.clone())]),
			..IndexedResults::new()
		};
		let Some(title): Option<String> = response.take("title").unwrap() else {
			panic!("title not found");
		};
		assert_eq!(title, summary.title);

		let mut response = IndexedResults {
			results: to_map(vec![Ok(value)]),
			..IndexedResults::new()
		};
		let vec: Vec<String> = response.take("title").unwrap();
		assert_eq!(vec, vec![summary.title]);

		let article = Article {
			title: "Lorem Ipsum".to_owned(),
			body: "Lorem Ipsum Lorem Ipsum".to_owned(),
		};
		let value = article.clone().into_value();

		let mut response = IndexedResults {
			results: to_map(vec![Ok(value.clone())]),
			..IndexedResults::new()
		};
		let Some(title): Option<String> = response.take("title").unwrap() else {
			panic!("title not found");
		};
		assert_eq!(title, article.title);
		let Some(body): Option<String> = response.take("body").unwrap() else {
			panic!("body not found");
		};
		assert_eq!(body, article.body);

		let mut response = IndexedResults {
			results: to_map(vec![Ok(value.clone())]),
			..IndexedResults::new()
		};
		let vec: Vec<String> = response.take("title").unwrap();
		assert_eq!(vec, vec![article.title.clone()]);

		let mut response = IndexedResults {
			results: to_map(vec![Ok(value)]),
			..IndexedResults::new()
		};
		let value: Value = response.take("title").unwrap();
		assert_eq!(value, Value::String(article.title));
	}

	#[test]
	fn take_key_multi() {
		let article = Article {
			title: "Lorem Ipsum".to_owned(),
			body: "Lorem Ipsum Lorem Ipsum".to_owned(),
		};
		let value = article.clone().into_value();

		let mut response = IndexedResults {
			results: to_map(vec![Ok(value.clone())]),
			..IndexedResults::new()
		};
		let title: Vec<String> = response.take("title").unwrap();
		assert_eq!(title, vec![article.title.clone()]);
		let body: Vec<String> = response.take("body").unwrap();
		assert_eq!(body, vec![article.body]);

		let mut response = IndexedResults {
			results: to_map(vec![Ok(value)]),
			..IndexedResults::new()
		};
		let vec: Vec<String> = response.take("title").unwrap();
		assert_eq!(vec, vec![article.title]);
	}

	#[test]
	fn take_partial_records() {
		let mut response = IndexedResults {
			results: to_map(vec![Ok(Value::from_vec(vec![
				Value::from_bool(true),
				Value::from_bool(false),
			]))]),
			..IndexedResults::new()
		};
		let value: Value = response.take(0).unwrap();
		assert_eq!(value, Value::from_vec(vec![Value::from_bool(true), Value::from_bool(false)]));

		let mut response = IndexedResults {
			results: to_map(vec![Ok(Value::from_vec(vec![
				Value::from_bool(true),
				Value::from_bool(false),
			]))]),
			..IndexedResults::new()
		};
		let vec: Vec<bool> = response.take(0).unwrap();
		assert_eq!(vec, vec![true, false]);

		let mut response = IndexedResults {
			results: to_map(vec![Ok(Value::from_vec(vec![
				Value::from_bool(true),
				Value::from_bool(false),
			]))]),
			..IndexedResults::new()
		};

		let Err(e) = response.take::<Option<bool>>(0) else {
			panic!("silently dropping records not allowed");
		};
		assert!(
			e.message().contains("Tried to take only a single result"),
			"expected lossy take error, got: {}",
			e.message()
		);
	}

	#[test]
	fn check_returns_the_first_error() {
		let response = vec![
			Ok(Value::from_int(0)),
			Ok(Value::from_int(1)),
			Ok(Value::from_int(2)),
			Err(TypesError::internal("test".to_string())),
			Ok(Value::from_int(3)),
			Ok(Value::from_int(4)),
			Ok(Value::from_int(5)),
			Err(TypesError::internal("test".to_string())),
			Ok(Value::from_int(6)),
			Ok(Value::from_int(7)),
			Err(TypesError::internal("test".to_string())),
		];
		let response = IndexedResults {
			results: to_map(response),
			..IndexedResults::new()
		};
		let err = response.check().unwrap_err();

		assert_eq!(err.message(), "test");
	}

	#[test]
	fn take_errors() {
		let response = vec![
			Ok(Value::from_int(0)),
			Ok(Value::from_int(1)),
			Ok(Value::from_int(2)),
			Err(TypesError::internal("test".to_string())),
			Ok(Value::from_int(3)),
			Ok(Value::from_int(4)),
			Ok(Value::from_int(5)),
			Err(TypesError::internal("test".to_string())),
			Ok(Value::from_int(6)),
			Ok(Value::from_int(7)),
			Err(TypesError::internal("test".to_string())),
		];
		let mut response = IndexedResults {
			results: to_map(response),
			..IndexedResults::new()
		};
		let errors = response.take_errors();
		assert_eq!(response.num_statements(), 8);
		assert_eq!(errors.len(), 3);
		assert_eq!(errors[&10].message(), "test");
		assert_eq!(errors[&7].message(), "test");
		assert_eq!(errors[&3].message(), "test");
		let Some(value): Option<i32> = response.take(2).unwrap() else {
			panic!("statement not found");
		};
		assert_eq!(value, 2);
		let value: Value = response.take(4).unwrap();
		assert_eq!(value, Value::from_int(3));
	}

	#[test]
	fn query_chaining_indexes_results_correctly() {
		// Simulate what happens when multiple queries are chained:
		// db.query("SELECT * FROM a").query("SELECT * FROM b").query("SELECT * FROM c")
		// Each statement should be accessible by its own index.
		let mut response = IndexedResults {
			results: to_map(vec![
				Ok(Value::from_int(0)), // index 0: first chained query
				Ok(Value::from_int(1)), // index 1: second chained query
				Ok(Value::from_int(2)), // index 2: third chained query
			]),
			..IndexedResults::new()
		};

		// Each index is independently accessible
		let first: Value = response.take(0).unwrap();
		assert_eq!(first, Value::from_int(0));

		let second: Value = response.take(1).unwrap();
		assert_eq!(second, Value::from_int(1));

		let third: Value = response.take(2).unwrap();
		assert_eq!(third, Value::from_int(2));

		// After taking all three, takes return Value::None
		let none: Value = response.take(0).unwrap();
		assert_eq!(none, Value::None);
	}
}
