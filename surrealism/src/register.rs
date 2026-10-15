//! Functions registered at run time, from `init`.
//!
//! `#[surrealism]` builds its function table at compile time. A module that
//! only learns its functions while running — an engine that compiles scripts
//! from the attached filesystem, say — calls [`register`] from its
//! `#[surrealism(init)]` function instead. The exports see one table: the
//! macro's entries first, then the registered ones in the order they were
//! registered.
//!
//! ```ignore
//! use surrealism::{FunctionMeta, register, surrealism};
//! use surrealdb_types::{Kind, Value};
//!
//! #[surrealism(init)]
//! fn init() -> anyhow::Result<()> {
//!     register(
//!         Some("shout"),
//!         FunctionMeta {
//!             args: vec![("text".into(), Kind::String)],
//!             returns: Kind::String,
//!             ..Default::default()
//!         },
//!         |_tx, args| async move {
//!             let [Value::String(text)] = args.as_slice() else {
//!                 anyhow::bail!("expected one string");
//!             };
//!             Ok(Value::String(text.to_uppercase()))
//!         },
//!     )
//! }
//! ```
//!
//! The table lives in linear memory and nowhere else. The host holds no copy,
//! so a snapshot of the instance taken after `init` returns carries every
//! registered function, and an instance that never ran `init` has none.
//!
//! [`register`] succeeds only while `init` runs. Before it, or once it has
//! returned, the table is fixed: the build reads it into `exports.toml` after
//! `init`, and the host plans every call from that manifest.

use core::cell::RefCell;
use core::future::Future;
use core::panic::Location;
use core::pin::Pin;

use anyhow::{Result, bail};
use surrealdb_types::{Kind, Value};

use crate::bindings::surrealism::extension::host;
use crate::registry::{InvokeFuture, SurrealismEntry};
use crate::tx::{Declared, Transaction};

/// The signature of a function passed to [`register`].
///
/// Everything the exports report about the function comes from here, because a
/// registered handler has no Rust signature for the SDK to read. The host
/// trusts it: it checks arguments against `args` before invoking, lends a
/// handle only when `declares_transaction` is set, and refuses writes through
/// that handle unless `writeable` is set.
#[derive(Clone, Debug, Default)]
pub struct FunctionMeta {
	/// Argument names and kinds, in call order.
	pub args: Vec<(String, Kind)>,
	/// The kind of the returned value.
	pub returns: Kind,
	/// Whether the function may write through its transaction. Ignored unless
	/// `declares_transaction` is set.
	pub writeable: bool,
	/// Whether the function takes a transaction. When `false` the handler is
	/// always passed `None`.
	pub declares_transaction: bool,
	/// Shown by `INFO` and `surreal module info`.
	pub comment: Option<String>,
}

type HandlerFuture = Pin<Box<dyn Future<Output = Result<Value, String>>>>;
type Handler = Box<dyn Fn(Option<Transaction<Declared>>, Vec<Value>) -> HandlerFuture>;

/// One function added by [`register`].
pub(crate) struct Registered {
	pub(crate) name: Option<String>,
	pub(crate) meta: FunctionMeta,
	/// Where [`register`] was called, for the duplicate-name error.
	location: &'static Location<'static>,
	handler: Handler,
}

impl Registered {
	/// Decode the arguments, call the handler and encode its result.
	///
	/// The handle is passed through only when the function declared one, so a
	/// handler registered without a transaction cannot reach the host even if
	/// the host lends one.
	pub(crate) fn invoke<'a>(
		&'static self,
		tx: Option<&'a host::Transaction>,
		raw_args: &'a [u8],
	) -> InvokeFuture<'a> {
		Box::pin(async move {
			let args = surrealdb_types::decode_value_list(raw_args).map_err(|e| e.to_string())?;
			let tx = match (self.meta.declares_transaction, tx) {
				// SAFETY: `handle` is the one lent to the `invoke` running now,
				// and the future holding the wrapper completes inside it.
				(true, Some(handle)) => Some(unsafe { Transaction::__from_borrow(handle) }),
				(true, None) => {
					return Err(
						"function requires a transaction but the host lent none".to_string()
					);
				}
				(false, _) => None,
			};
			let value = (self.handler)(tx, args).await?;
			surrealdb_types::encode(&value).map_err(|e| e.to_string())
		})
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
	Before,
	Running,
	Finished,
}

/// The run-time half of the function table.
///
/// Entries are leaked so that a lookup hands out `&'static` references and no
/// borrow of the table is held across an `invoke` await. Nothing is ever
/// removed, and `init` runs once per instance.
pub(crate) struct Registry {
	phase: Phase,
	entries: Vec<&'static Registered>,
}

impl Registry {
	pub(crate) const fn new() -> Self {
		Self {
			phase: Phase::Before,
			entries: Vec::new(),
		}
	}

	/// Open the table for [`register`]. Fails if `init` already ran.
	pub(crate) fn begin_init(&mut self) -> Result<(), String> {
		if self.phase != Phase::Before {
			return Err("init has already run on this instance".to_string());
		}
		self.phase = Phase::Running;
		Ok(())
	}

	/// Close the table, whether or not `init` succeeded.
	pub(crate) fn end_init(&mut self) {
		self.phase = Phase::Finished;
	}

	pub(crate) fn entries(&self) -> &[&'static Registered] {
		&self.entries
	}

	pub(crate) fn find(&self, name: Option<&str>) -> Option<&'static Registered> {
		self.entries.iter().copied().find(|e| e.name.as_deref() == name)
	}

	/// Add a function, checking it against `statics` (the macro's entries) and
	/// every function registered before it.
	fn add<'s>(
		&mut self,
		statics: impl IntoIterator<Item = &'s SurrealismEntry>,
		name: Option<&str>,
		meta: FunctionMeta,
		handler: Handler,
		location: &'static Location<'static>,
	) -> Result<()> {
		let display = display_name(name);
		match self.phase {
			Phase::Running => {}
			Phase::Before => bail!(
				"surrealism::register(\"{display}\") at {location}: \
				 functions can only be registered from #[surrealism(init)]"
			),
			Phase::Finished => bail!(
				"surrealism::register(\"{display}\") at {location}: \
				 init has returned, so the function table is fixed"
			),
		}
		if let Some(name) = name {
			validate_name(name)
				.map_err(|e| anyhow::anyhow!("surrealism::register at {location}: {e}"))?;
		}
		if let Some(existing) = statics.into_iter().find(|e| e.name == name) {
			bail!(
				"duplicate function \"{display}\": registered at {location}, \
				 and defined by #[surrealism] at {}",
				existing.location
			);
		}
		if let Some(existing) = self.find(name) {
			bail!(
				"duplicate function \"{display}\": registered at {location}, \
				 and registered before at {}",
				existing.location
			);
		}
		self.entries.push(Box::leak(Box::new(Registered {
			name: name.map(String::from),
			meta,
			location,
			handler,
		})));
		Ok(())
	}
}

std::thread_local! {
	/// The instance's registry. A guest is single-threaded, so this is one
	/// plain static in linear memory.
	static REGISTRY: RefCell<Registry> = const { RefCell::new(Registry::new()) };
}

/// Run `f` with the instance's registry.
pub(crate) fn with_registry<R>(f: impl FnOnce(&mut Registry) -> R) -> R {
	REGISTRY.with(|r| f(&mut r.borrow_mut()))
}

/// Add a function to the module's exports at run time.
///
/// `name` is `None` for the default export, and otherwise `::`-separated
/// segments of ASCII letters, digits and underscores, as for
/// `#[surrealism(name = "...")]`. The handler receives the decoded arguments
/// and, when `meta.declares_transaction` is set, the transaction the host lent
/// for the call.
///
/// # Errors
///
/// - called anywhere but inside the `#[surrealism(init)]` function;
/// - `name` is malformed;
/// - `name` is already taken by a `#[surrealism]` function or an earlier registration. The message
///   names both definitions.
///
/// Return the error from `init`: a failed `init` leaves the module unhealthy,
/// and the build reports it.
#[track_caller]
pub fn register<F, Fut>(name: Option<&str>, meta: FunctionMeta, handler: F) -> Result<()>
where
	F: Fn(Option<Transaction<Declared>>, Vec<Value>) -> Fut + 'static,
	Fut: Future<Output = Result<Value>> + 'static,
{
	let location = Location::caller();
	let handler: Handler = Box::new(move |tx, args| {
		let fut = handler(tx, args);
		Box::pin(async move { fut.await.map_err(|e| e.to_string()) })
	});
	with_registry(|r| r.add(inventory::iter::<SurrealismEntry>(), name, meta, handler, location))
}

fn display_name(name: Option<&str>) -> &str {
	name.unwrap_or("<default>")
}

/// The rule `#[surrealism(name = "...")]` enforces at compile time.
fn validate_name(name: &str) -> Result<(), String> {
	for segment in name.split("::") {
		if segment.is_empty() || !segment.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
			return Err(format!(
				"invalid function name \"{name}\": segments must use only ASCII letters, \
				 digits, and underscores, separated by `::`"
			));
		}
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	fn noop() -> Handler {
		Box::new(|_, _| Box::pin(async { Ok(Value::None) }))
	}

	fn invoke_none<'a>(_: Option<&'a host::Transaction>, _: &'a [u8]) -> InvokeFuture<'a> {
		Box::pin(async { Ok(Vec::new()) })
	}

	fn empty() -> Result<Vec<u8>, String> {
		Ok(Vec::new())
	}

	fn static_entry(name: Option<&'static str>) -> SurrealismEntry {
		SurrealismEntry {
			name,
			comment: None,
			location: "src/lib.rs:12",
			invoke: invoke_none,
			args: empty,
			returns: empty,
			writeable: false,
			declares_transaction: false,
		}
	}

	fn in_init() -> Registry {
		let mut r = Registry::new();
		r.begin_init().unwrap();
		r
	}

	#[test]
	fn register_only() {
		let mut r = in_init();
		let meta = FunctionMeta {
			args: vec![("x".into(), Kind::Int)],
			returns: Kind::Int,
			comment: Some("doubles".into()),
			..Default::default()
		};
		r.add([], Some("double"), meta, noop(), Location::caller()).unwrap();
		r.add([], None, FunctionMeta::default(), noop(), Location::caller()).unwrap();
		r.end_init();

		let names: Vec<_> = r.entries().iter().map(|e| e.name.clone()).collect();
		assert_eq!(names, [Some("double".to_string()), None]);
		let double = r.find(Some("double")).unwrap();
		assert_eq!(double.meta.args, [("x".to_string(), Kind::Int)]);
		assert_eq!(double.meta.comment.as_deref(), Some("doubles"));
		assert!(r.find(None).is_some());
		assert!(r.find(Some("missing")).is_none());
	}

	#[test]
	fn register_beside_macro_entries() {
		let statics = [static_entry(Some("macro_fn")), static_entry(None)];
		let mut r = in_init();
		r.add(&statics, Some("registered"), FunctionMeta::default(), noop(), Location::caller())
			.unwrap();
		assert!(r.find(Some("registered")).is_some());
		assert!(r.find(Some("macro_fn")).is_none(), "macro entries stay in the static table");
	}

	#[test]
	fn duplicate_of_macro_entry_names_both() {
		let statics = [static_entry(Some("taken"))];
		let mut r = in_init();
		let here = Location::caller();
		let err =
			r.add(&statics, Some("taken"), FunctionMeta::default(), noop(), here).unwrap_err();
		let msg = err.to_string();
		assert!(msg.contains("duplicate function \"taken\""), "{msg}");
		assert!(msg.contains(&here.to_string()), "{msg}");
		assert!(msg.contains("#[surrealism] at src/lib.rs:12"), "{msg}");
	}

	#[test]
	fn duplicate_default_of_macro_entry() {
		let statics = [static_entry(None)];
		let mut r = in_init();
		let err =
			r.add(&statics, None, FunctionMeta::default(), noop(), Location::caller()).unwrap_err();
		assert!(err.to_string().contains("duplicate function \"<default>\""), "{err}");
	}

	#[test]
	fn duplicate_of_registered_entry_names_both() {
		let mut r = in_init();
		let first = Location::caller();
		r.add([], Some("twice"), FunctionMeta::default(), noop(), first).unwrap();
		let second = Location::caller();
		let err = r.add([], Some("twice"), FunctionMeta::default(), noop(), second).unwrap_err();
		let msg = err.to_string();
		assert!(msg.contains(&format!("registered before at {first}")), "{msg}");
		assert_eq!(r.entries().len(), 1);
	}

	#[test]
	fn register_after_init_fails() {
		let mut r = in_init();
		r.end_init();
		let err = r
			.add([], Some("late"), FunctionMeta::default(), noop(), Location::caller())
			.unwrap_err();
		assert!(err.to_string().contains("init has returned"), "{err}");
		assert!(r.entries().is_empty());
	}

	#[test]
	fn register_before_init_fails() {
		let mut r = Registry::new();
		let err = r
			.add([], Some("early"), FunctionMeta::default(), noop(), Location::caller())
			.unwrap_err();
		assert!(err.to_string().contains("only be registered from #[surrealism(init)]"), "{err}");
	}

	#[test]
	fn init_runs_once() {
		let mut r = in_init();
		r.end_init();
		assert!(r.begin_init().is_err());
	}

	#[test]
	fn malformed_name_rejected() {
		let mut r = in_init();
		for bad in ["", "a::", "a-b", "a::::b"] {
			assert!(
				r.add([], Some(bad), FunctionMeta::default(), noop(), Location::caller()).is_err(),
				"{bad:?} should be rejected"
			);
		}
	}

	#[test]
	fn public_register_outside_init_fails() {
		// The thread-local registry on a test thread never enters `init`.
		let err =
			register(Some("outside"), FunctionMeta::default(), |_, _| async { Ok(Value::None) })
				.unwrap_err();
		assert!(err.to_string().contains("only be registered from #[surrealism(init)]"), "{err}");
	}
}
