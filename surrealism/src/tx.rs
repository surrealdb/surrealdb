//! The `transaction` capability handle.
//!
//! A `#[surrealism]` function reaches the datastore only through a value of
//! this type, which the host lends for the duration of one call. A function
//! that declares no transaction parameter is lent nothing and has no route to
//! the host at all — the canonical ABI revokes the handle when `invoke`
//! returns, so a retained one traps rather than working.
//!
//! ```ignore
//! use surrealism::{ReadWrite, Transaction, surrealism};
//!
//! // Read-only is the default mode.
//! #[surrealism]
//! async fn top_customers(tx: Transaction, limit: i64) -> anyhow::Result<Value> {
//!     tx.sql_with_vars("SELECT * FROM customer LIMIT $limit", [("limit", limit)]).await
//! }
//!
//! // Writes are opted into through the type, not an attribute.
//! #[surrealism]
//! async fn add_customer(tx: Transaction<ReadWrite>, name: String) -> anyhow::Result<Value> {
//!     tx.sql_with_vars("CREATE customer SET name = $name", [("name", name)]).await
//! }
//!
//! // No transaction parameter: provably cannot query.
//! #[surrealism]
//! fn add(a: i64, b: i64) -> i64 {
//!     a + b
//! }
//! ```

use core::marker::PhantomData;
use core::mem::ManuallyDrop;

use anyhow::Result;
use surrealdb_types::{Object, SurrealValue, Value};
use surrealism_types::args::Args;

use crate::bindings::surrealism::extension::host;

/// Read-only access. The default mode.
///
/// Named `ReadOnly` rather than `Read` because `std::io::Read` is imported in
/// most plugins that touch files, and two unqualified `Read`s in scope is a
/// collision every author would have to work around.
pub struct ReadOnly;

/// Read-write access. Opted into by writing `Transaction<ReadWrite>`.
pub struct ReadWrite;

/// The access declared in the [`FunctionMeta`](crate::FunctionMeta) of a
/// function added with [`register`](crate::register).
///
/// A registered function has no Rust signature, so its mode is the
/// `writeable` flag it was registered with rather than a type. The host
/// enforces that flag exactly as it does for the other two modes. This mode
/// has no [`TxMode`] impl, so `#[surrealism]` rejects it in a signature.
pub struct Declared;

mod sealed {
	/// Sealed so a plugin cannot invent a mode. Implemented only for
	/// [`ReadOnly`](super::ReadOnly), [`ReadWrite`](super::ReadWrite) and
	/// [`Declared`](super::Declared).
	pub trait Mode {}
	impl Mode for super::ReadOnly {}
	impl Mode for super::ReadWrite {}
	impl Mode for super::Declared {}
}

pub use sealed::Mode;

/// Whether a transaction type grants writes.
///
/// The `#[surrealism]` macro reads this instead of parsing the mode out of the
/// parameter's tokens, so a type alias works and a bogus mode is a compile
/// error at the author's signature rather than a wrong `exports.toml` entry.
pub trait TxMode {
	const WRITEABLE: bool;
}

impl TxMode for Transaction<ReadOnly> {
	const WRITEABLE: bool = false;
}

impl TxMode for Transaction<ReadWrite> {
	const WRITEABLE: bool = true;
}

/// A borrowed capability to act inside the calling query's transaction.
///
/// `ManuallyDrop` keeps this wrapper from invoking the resource's drop
/// intrinsic on a handle the host still owns. Holding the handle by value
/// rather than by reference also avoids a lifetime parameter, so both
/// `Transaction` and `Transaction<ReadWrite>` read naturally in a signature.
#[repr(transparent)]
pub struct Transaction<M: Mode = ReadOnly> {
	inner: ManuallyDrop<host::Transaction>,
	_mode: PhantomData<M>,
}

impl<M: Mode> Transaction<M> {
	/// Wrap the borrowed handle the host passed to `invoke`.
	///
	/// # Safety
	///
	/// `handle` must be a handle the host lent for the invocation currently
	/// running, and the returned value must not outlive it. Called only by the
	/// `#[surrealism]` macro and the dispatch of registered functions.
	#[doc(hidden)]
	pub unsafe fn __from_borrow(handle: &host::Transaction) -> Self {
		Self {
			// Copies the handle index. `ManuallyDrop` ensures the copy is never
			// dropped, so the host's ownership is not disturbed.
			inner: ManuallyDrop::new(unsafe {
				core::ptr::read(handle as *const host::Transaction)
			}),
			_mode: PhantomData,
		}
	}

	/// Execute a SurrealQL query with no bind variables.
	///
	/// Requires the definition to grant `CAPABILITIES { queries: true }`.
	///
	/// This takes a string, so the mode marker cannot reject a write by itself.
	/// The host does: a function that declared a read-only transaction runs
	/// under a no-write frame, and a data-modifying statement is refused where
	/// it is reached. Declare `Transaction<ReadWrite>` to write.
	pub async fn sql<R: SurrealValue>(&self, query: impl Into<String>) -> Result<R> {
		self.sql_with_vars(query, Object::new()).await
	}

	/// Execute a SurrealQL query with bind variables.
	///
	/// `vars` is anything that iterates key/value pairs, so the natural
	/// spellings all work: an array of tuples, a `Vec`, a `BTreeMap`, or an
	/// [`Object`].
	///
	/// ```ignore
	/// tx.sql_with_vars("SELECT * FROM customer LIMIT $limit", [("limit", limit)]).await
	/// ```
	pub async fn sql_with_vars<R, K, V, I>(&self, query: impl Into<String>, vars: I) -> Result<R>
	where
		R: SurrealValue,
		K: Into<String>,
		V: SurrealValue,
		I: IntoIterator<Item = (K, V)>,
	{
		let query = query.into();
		// Refused here rather than at the host: an empty query is a guest bug
		// (an unset `format!`, an empty branch), and it costs a round trip and
		// a parse error to find that out from the other side.
		if query.trim().is_empty() {
			anyhow::bail!("transaction.sql: the query is empty");
		}
		let vars: Vec<(String, Value)> =
			vars.into_iter().map(|(k, v)| (k.into(), v.into_value())).collect();
		let vars_bytes = surrealdb_types::encode_string_key_values(&vars)?;
		let result_bytes =
			self.inner.sql(query, vars_bytes).await.map_err(|e| host_err("transaction.sql", &e))?;
		let value: Value = surrealdb_types::decode(&result_bytes)?;
		Ok(R::from_value(value)?)
	}

	/// Call another surrealism function by fully-qualified name.
	///
	/// Requires the definition's `CAPABILITIES { functions: [...] }` to grant
	/// the function.
	pub async fn run<R: SurrealValue>(&self, fnc: impl Into<String>, args: impl Args) -> Result<R> {
		self.run_versioned(fnc, None, args).await
	}

	/// As [`run`](Self::run), pinning a specific module version.
	pub async fn run_versioned<R: SurrealValue>(
		&self,
		fnc: impl Into<String>,
		version: Option<String>,
		args: impl Args,
	) -> Result<R> {
		let args_bytes = surrealdb_types::encode_value_list(&args.to_values())?;
		let result_bytes = self
			.inner
			.run(fnc.into(), version, args_bytes)
			.await
			.map_err(|e| host_err("transaction.run", &e))?;
		let value: Value = surrealdb_types::decode(&result_bytes)?;
		Ok(R::from_value(value)?)
	}
}

/// Convert a WIT `host-error` into an `anyhow::Error`, preserving the class.
///
/// The variant is kept in the message so a plugin that only logs still sees
/// whether the call was denied, malformed, or refused by the datastore. Plugins
/// that need to branch can match [`host::HostError`] directly.
fn host_err(op: &str, e: &host::HostError) -> anyhow::Error {
	match e {
		host::HostError::PermissionDenied(m) => {
			anyhow::anyhow!("host {op}: permission denied: {m}")
		}
		host::HostError::InvalidRequest(m) => anyhow::anyhow!("host {op}: invalid request: {m}"),
		host::HostError::Datastore(m) => anyhow::anyhow!("host {op}: datastore error: {m}"),
		host::HostError::DeadlineExceeded => anyhow::anyhow!("host {op}: deadline exceeded"),
		host::HostError::Other(m) => anyhow::anyhow!("host {op}: {m}"),
	}
}
