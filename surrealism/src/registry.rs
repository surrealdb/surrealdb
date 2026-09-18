use core::future::Future;
use core::pin::Pin;

use crate::bindings::surrealism::plugin::host;

/// A boxed future returned by a registered function's invoke thunk.
///
/// The thunk borrows both the lent handle and the argument buffer, so the
/// future is tied to their lifetime.
pub type InvokeFuture<'a> = Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + 'a>>;

/// One registered callable function (name + invoke/args/returns).
/// Submitted by `#[surrealism]`; collected in the surrealism crate.
///
/// `name` is `None` for the default export, `Some("foo")` for a named export.
#[doc(hidden)]
pub struct SurrealismEntry {
	pub name: Option<&'static str>,
	pub comment: Option<&'static str>,
	/// `tx` is `None` when the function declared no transaction parameter. A
	/// function that wants one and is passed `None` returns an error rather
	/// than reaching the host.
	pub invoke: for<'a> fn(Option<&'a host::Transaction>, &'a [u8]) -> InvokeFuture<'a>,
	pub args: fn() -> Result<Vec<u8>, String>,
	pub returns: fn() -> Result<Vec<u8>, String>,
	pub writeable: bool,
	/// Whether the function declared a transaction parameter at all.
	///
	/// Reported to the host so it can lend a handle only to a function that
	/// asked for one. `writeable` cannot stand in: it is `false` both for a
	/// read-only `Transaction` and for no parameter, and those want different
	/// handles — one read-only, one none.
	pub declares_transaction: bool,
}

/// Registered init function. Submitted by `#[surrealism(init)]`.
///
/// Synchronous: `init` is sync-lifted and is lent no transaction, so it has
/// nothing to await.
#[doc(hidden)]
pub struct SurrealismInit(pub fn() -> Result<(), String>);
