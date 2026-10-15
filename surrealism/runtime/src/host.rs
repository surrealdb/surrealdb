//! `InvocationContext` trait and WIT host implementation.
//!
//! Implementations supply `sql`/`run` and the WASI stdio callbacks per call.
//! Host functions decode FlatBuffers, call the context, and encode results.
//!
//! Host access is not ambient: `sql` and `run` are methods on a `transaction`
//! resource that the host lends for the duration of one `invoke` call and
//! revokes on return. The context therefore lives in the store's
//! [`ResourceTable`](wasmtime::component::ResourceTable) rather than in a
//! swappable field, and an export lent no handle has no path to the datastore.
//!
//! A guest may hold several calls through one handle in flight at once, which
//! is what the async ABI is for. This module bounds how many; which of them may
//! run at the same time is decided by the [`InvocationContext`], because it is
//! a question about the statements they carry.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use surrealism_types::err::{HostCallError, HostCallResult, PrefixErr, SurrealismResult};
use wasmtime::component::{Accessor, Resource};

use crate::config::SurrealismConfig;
use crate::store::StoreData;

// ============================================================================
// InvocationContext trait
// ============================================================================

/// Context provided for each WASM function invocation.
///
/// Methods take `&self` because a guest may hold several host calls in flight
/// at once: each one is a component-model subtask, and the store is handed back
/// to the guest while any of them is pending. Implementations are shared as
/// `Arc<dyn InvocationContext>` and must therefore be internally mutable where
/// they need per-call scratch state.
#[async_trait]
pub trait InvocationContext: Send + Sync {
	async fn sql(
		&self,
		config: &SurrealismConfig,
		query: String,
		vars: surrealdb_types::Object,
	) -> HostCallResult<surrealdb_types::Value>;

	async fn run(
		&self,
		config: &SurrealismConfig,
		fnc: String,
		version: Option<String>,
		args: Vec<surrealdb_types::Value>,
	) -> HostCallResult<surrealdb_types::Value>;

	/// Returns a self-contained callback for forwarding WASI stdout output.
	///
	/// This is used by the WASI output stream to route guest `println!` / C `printf`
	/// output through a structured logging path. Override this to capture context
	/// (module name, namespace, database, etc.) inside the closure so the callback
	/// can be invoked independently of the context itself.
	///
	/// The returned `Arc` is cheap to clone and allows the WASI stream to
	/// snapshot the callback without holding a lock during invocation.
	fn stdout_callback(&self) -> Arc<dyn Fn(&str) + Send + Sync> {
		Arc::new(|output| print!("{}", output))
	}

	/// Same as [`stdout_callback`](Self::stdout_callback) but for stderr.
	fn stderr_callback(&self) -> Arc<dyn Fn(&str) + Send + Sync> {
		Arc::new(|output| eprint!("{}", output))
	}
}

// ============================================================================
// Wire form of `host-error`
// ============================================================================

/// Lowers onto the WIT `host-error` variant.
///
/// Hand-derived rather than generated: the linker in this module is built by
/// hand, so there is no bindgen output to borrow the type from. The case names
/// must stay in step with `surrealism/wit/surrealism.wit`.
#[derive(wasmtime::component::ComponentType, wasmtime::component::Lower)]
#[component(variant)]
enum HostErrorWire {
	#[component(name = "permission-denied")]
	PermissionDenied(String),
	#[component(name = "invalid-request")]
	InvalidRequest(String),
	#[component(name = "datastore")]
	Datastore(String),
	#[component(name = "deadline-exceeded")]
	DeadlineExceeded,
	#[component(name = "other")]
	Other(String),
}

// ============================================================================
// The `transaction` resource
// ============================================================================

/// What a guest's `transaction` handle points at, host-side.
///
/// The host pushes one of these into the store's resource table before calling
/// `invoke` and deletes it afterwards, so a handle retained past the end of an
/// invocation refers to nothing and traps on use.
pub(crate) struct Transaction {
	pub(crate) context: Arc<dyn InvocationContext>,
	/// Host calls admitted through this handle and not yet finished.
	///
	/// Each one is a component-model subtask the host holds open: a decoded
	/// argument list, a datastore call and whatever that call allocates. The
	/// guest can create them far faster than they retire, and nothing else
	/// bounds that — `join_all` over a large range barely moves linear memory,
	/// so `StoreLimits::memory_size` never bites, and the epoch deadline
	/// interrupts guest execution rather than a pending host subtask. So the
	/// count is kept and a call past [`MAX_IN_FLIGHT_HOST_CALLS`] is refused.
	///
	/// This bounds the fan-out; it does not order it. Whether two calls may run
	/// at the same time is a question about the statements they carry, which
	/// only the [`InvocationContext`] can read — see the write gate in
	/// `surrealdb/core/src/surrealism/host.rs`.
	pub(crate) in_flight: Arc<AtomicUsize>,
}

/// The most host calls one lent handle may have in flight at once.
///
/// Far above what a module doing legitimate concurrent work holds — the point
/// is to bound a guest that spawns subtasks faster than the host retires them,
/// not to ration ordinary use.
const MAX_IN_FLIGHT_HOST_CALLS: usize = 64;

/// One admitted host call. Releases its place on drop, on every path.
struct CallSlot(Arc<AtomicUsize>);

impl Drop for CallSlot {
	fn drop(&mut self) {
		self.0.fetch_sub(1, Ordering::AcqRel);
	}
}

/// Take a place among the calls in flight through one handle, or refuse.
fn admit(counter: &Arc<AtomicUsize>) -> HostCallResult<CallSlot> {
	let in_flight = counter.fetch_add(1, Ordering::AcqRel) + 1;
	// Constructed before the check so the place is released whichever way this
	// returns.
	let slot = CallSlot(Arc::clone(counter));
	if in_flight > MAX_IN_FLIGHT_HOST_CALLS {
		return Err(HostCallError::InvalidRequest(format!(
			"too many host calls in flight through one transaction handle: {in_flight}, \
			 limit {MAX_IN_FLIGHT_HOST_CALLS}"
		)));
	}
	Ok(slot)
}

// ============================================================================
// Helper
// ============================================================================

fn stringify<E: std::fmt::Display>(e: E) -> String {
	e.to_string()
}

/// Lower a classified host failure onto the wire.
fn lower(e: HostCallError) -> HostErrorWire {
	match e {
		HostCallError::PermissionDenied(m) => HostErrorWire::PermissionDenied(m),
		HostCallError::InvalidRequest(m) => HostErrorWire::InvalidRequest(m),
		HostCallError::Datastore(m) => HostErrorWire::Datastore(m),
		HostCallError::DeadlineExceeded => HostErrorWire::DeadlineExceeded,
		HostCallError::Other(m) => HostErrorWire::Other(m),
	}
}

/// Resolve a borrowed handle into the context and config it grants access to.
///
/// [`Accessor::with`] is synchronous and cannot span an await, so everything a
/// method needs from the store is cloned out here, before the first one.
fn resolve(accessor: &Accessor<StoreData>, this: &Resource<Transaction>) -> HostCallResult<Lent> {
	accessor.with(|mut access| {
		let data = access.get();
		// A handle the host has already revoked lands here. It is the guest
		// reaching outside the invocation it was lent to, not a datastore fault.
		let tx = data.table.get(this).map_err(|e| HostCallError::PermissionDenied(stringify(e)))?;
		let slot = admit(&tx.in_flight)?;
		Ok(Lent {
			context: Arc::clone(&tx.context),
			config: Arc::clone(&data.config),
			_slot: slot,
		})
	})
}

/// What one host call needs out of the store, taken before the first await.
struct Lent {
	context: Arc<dyn InvocationContext>,
	config: Arc<SurrealismConfig>,
	/// Held for the rest of the call, so the place it took among the calls in
	/// flight is released however the call ends.
	_slot: CallSlot,
}

// ============================================================================
// Component model host functions (FlatBuffers serialization)
// ============================================================================

pub fn implement_host_functions(
	linker: &mut wasmtime::component::Linker<StoreData>,
) -> SurrealismResult<()> {
	let mut root = linker.root();
	let mut host = root
		.instance("surrealism:extension/host")
		.prefix_err(|| "failed to define host instance")?;

	// The guest only ever holds `borrow<transaction>`, which the canonical ABI
	// revokes at return, so this destructor is not reached in practice. It is
	// required to define the resource type, and deleting the table entry is the
	// correct behaviour should the guest ever be given an owned handle.
	host.resource(
		"transaction",
		wasmtime::component::ResourceType::host::<Transaction>(),
		|mut store, rep| {
			store.data_mut().table.delete(Resource::<Transaction>::new_own(rep))?;
			Ok(())
		},
	)
	.prefix_err(|| "failed to register transaction resource")?;

	host.func_wrap_concurrent(
		"[method]transaction.sql",
		|accessor: &Accessor<StoreData>,
		 (this, query, vars_bytes): (Resource<Transaction>, String, Vec<u8>)| {
			let resolved = resolve(accessor, &this);
			Box::pin(async move {
				let inner: HostCallResult<Vec<u8>> = async {
					let lent = resolved?;
					let vars_vec = surrealdb_types::decode_string_key_values(&vars_bytes)
						.map_err(|e| HostCallError::InvalidRequest(stringify(e)))?;
					let vars = surrealdb_types::Object::from_iter(vars_vec);
					let val = lent.context.sql(&lent.config, query, vars).await?;
					surrealdb_types::encode(&val).map_err(|e| HostCallError::Other(stringify(e)))
				}
				.await;
				Ok((inner.map_err(lower),))
			})
		},
	)
	.prefix_err(|| "failed to register transaction.sql")?;

	host.func_wrap_concurrent(
		"[method]transaction.run",
		|accessor: &Accessor<StoreData>,
		 (this, fnc, version, args_bytes): (
			Resource<Transaction>,
			String,
			Option<String>,
			Vec<u8>,
		)| {
			let resolved = resolve(accessor, &this);
			Box::pin(async move {
				let inner: HostCallResult<Vec<u8>> = async {
					let lent = resolved?;
					let args = surrealdb_types::decode_value_list(&args_bytes)
						.map_err(|e| HostCallError::InvalidRequest(stringify(e)))?;
					let val = lent.context.run(&lent.config, fnc, version, args).await?;
					surrealdb_types::encode(&val).map_err(|e| HostCallError::Other(stringify(e)))
				}
				.await;
				Ok((inner.map_err(lower),))
			})
		},
	)
	.prefix_err(|| "failed to register transaction.run")?;

	Ok(())
}
