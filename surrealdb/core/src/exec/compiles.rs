//! A count of the compilations this thread has made while a VM program runs,
//! for tests that must know a running program compiled nothing.
//!
//! Every entry point that binds, maps, lowers or compiles a plan or a scalar
//! notes itself here: [`Planner::bind_scoped`], [`Planner::plan_logical`],
//! [`Planner::physical_expr`], the VM's mapping and lowering, and the lowering
//! of a closure body. A note made while a program's stream is being polled
//! ([`while_running`]) is a compilation the running program made; one made
//! while the statement is planned is not. A test runs a query and reads
//! [`count`] before and after.
//!
//! The one compilation a running program may cause is a fault: an
//! instruction handing a callee a closure its run holds no unit for yields
//! to the host with the request, and so does a read of a table only the data
//! names that its run has not linked ([`crate::exec::vm::fault`]); the host
//! links or compiles what it asks for. An invocation inside the call such an
//! instruction made whose linked unit stopped standing takes the same fault.
//! The host serves the fault inside [`serving_fault`], which counts it,
//! exactly once, by [`faults`]; the compilations beneath it are the fault's
//! and are not noted as ones the program made, so a running program that
//! compiles only in faults leaves [`count`] unchanged.
//!
//! Every closure invocation runs a unit; no closure body is evaluated in the
//! operator tree's form. A body the host lowers no unit of is compiled to that
//! form once, in the fault that finds it, only for the error compiling it
//! reports ([`crate::exec::vm::closure`]). Each such compilation is noted by
//! [`note_tree_body`] and counted by [`tree_bodies`], whether or not a
//! program is running, so a test can tell exactly how many bodies took it.
//!
//! The counts are per thread, so tests running in parallel do not see one
//! another's compilations, and a test that asks them runs on a current-thread
//! runtime, which runs every task a plan spawns on the same thread. Outside
//! a test build none of this is compiled in.
//!
//! [`Planner::bind_scoped`]: crate::exec::planner::Planner::bind_scoped
//! [`Planner::plan_logical`]: crate::exec::planner::Planner::plan_logical
//! [`Planner::physical_expr`]: crate::exec::planner::Planner::physical_expr

use crate::exec::ValueBatchStream;

#[cfg(test)]
thread_local! {
	/// How many program streams this thread is polling, nested.
	static RUNNING: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	/// The compilations noted while one was.
	static COMPILES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	/// How many faults this thread is serving, nested.
	static SERVING: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	/// The faults this thread has served.
	static FAULTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	/// The closure bodies this thread has compiled to the operator tree's
	/// form for the error a body no unit of which is lowered reports.
	static TREE_BODIES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	/// The program streams this thread has started ([`while_running`]).
	static PROGRAMS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Note one compilation of a closure body to the operator tree's form for the
/// error it reports, on this thread.
#[inline]
pub(crate) fn note_tree_body() {
	#[cfg(test)]
	TREE_BODIES.with(|count| count.set(count.get() + 1));
}

/// How many closure bodies this thread has compiled to the operator tree's
/// form for the error it reports.
#[cfg(test)]
pub(crate) fn tree_bodies() -> usize {
	TREE_BODIES.with(std::cell::Cell::get)
}

/// Note one compilation on this thread.
#[inline]
pub(crate) fn note() {
	#[cfg(test)]
	if RUNNING.with(std::cell::Cell::get) > 0 && SERVING.with(std::cell::Cell::get) == 0 {
		COMPILES.with(|count| count.set(count.get() + 1));
		// With `SURREAL_TRACE_COMPILES` set, each such compilation prints the
		// frames that reached it.
		if std::env::var_os("SURREAL_TRACE_COMPILES").is_some() {
			let trace = std::backtrace::Backtrace::force_capture().to_string();
			let frames: Vec<&str> = trace
				.lines()
				.filter(|l| l.contains("surrealdb_core::") && !l.contains("compiles::"))
				.take(8)
				.collect();
			eprintln!("compiled while running:\n{}", frames.join("\n"));
		}
	}
}

/// How many compilations this thread has noted while a program ran.
#[cfg(test)]
pub(crate) fn count() -> usize {
	COMPILES.with(std::cell::Cell::get)
}

/// How many program streams this thread has started: one per statement the
/// executor runs as a program, whatever it nests.
#[cfg(test)]
pub(crate) fn programs() -> usize {
	PROGRAMS.with(std::cell::Cell::get)
}

/// How many faults this thread has served.
#[cfg(test)]
pub(crate) fn faults() -> usize {
	FAULTS.with(std::cell::Cell::get)
}

/// `future`, the host serving one fault: counted once by [`faults`] when it
/// is first polled, with the compilations beneath it not noted as ones the
/// running program made. A fault is a seam of the run-time seal ([`seam`]):
/// what it compiles may read the catalog and build planners.
pub(crate) fn serving_fault<'a, F: std::future::Future + Send + 'a>(
	future: F,
) -> impl std::future::Future<Output = F::Output> + Send + 'a {
	#[cfg(test)]
	{
		struct Serving<F> {
			inner: std::pin::Pin<Box<F>>,
			counted: bool,
		}
		impl<F: std::future::Future> std::future::Future for Serving<F> {
			type Output = F::Output;
			fn poll(
				mut self: std::pin::Pin<&mut Self>,
				cx: &mut std::task::Context<'_>,
			) -> std::task::Poll<F::Output> {
				if !self.counted {
					self.counted = true;
					FAULTS.with(|count| count.set(count.get() + 1));
				}
				SERVING.with(|depth| depth.set(depth.get() + 1));
				let polled = self.inner.as_mut().poll(cx);
				SERVING.with(|depth| depth.set(depth.get() - 1));
				polled
			}
		}
		seam(Serving {
			inner: Box::pin(future),
			counted: false,
		})
	}
	#[cfg(not(test))]
	seam(future)
}

/// `stream`, a program's rows, marking this thread as running the program
/// while it is polled, under the run-time seal ([`sealed_stream`]).
pub(crate) fn while_running(stream: ValueBatchStream) -> ValueBatchStream {
	#[cfg(test)]
	let stream: ValueBatchStream = {
		PROGRAMS.with(|count| count.set(count.get() + 1));
		struct Running(ValueBatchStream);
		impl futures::Stream for Running {
			type Item = <ValueBatchStream as futures::Stream>::Item;
			fn poll_next(
				mut self: std::pin::Pin<&mut Self>,
				cx: &mut std::task::Context<'_>,
			) -> std::task::Poll<Option<Self::Item>> {
				RUNNING.with(|running| running.set(running.get() + 1));
				let polled = self.0.as_mut().poll_next(cx);
				RUNNING.with(|running| running.set(running.get() - 1));
				polled
			}
		}
		Box::pin(Running(stream))
	};
	sealed_stream(stream)
}

/// `future`, work a running program hands to another task, marking this
/// thread as running the program while it is polled, under the run-time seal
/// ([`sealed`]). A program hands no work to another task on wasm.
#[cfg(not(target_family = "wasm"))]
pub(crate) fn while_running_task<'a, F: std::future::Future + Send + 'a>(
	future: F,
) -> impl std::future::Future<Output = F::Output> + Send + 'a {
	#[cfg(test)]
	{
		struct Running<F>(std::pin::Pin<Box<F>>);
		impl<F: std::future::Future> std::future::Future for Running<F> {
			type Output = F::Output;
			fn poll(
				mut self: std::pin::Pin<&mut Self>,
				cx: &mut std::task::Context<'_>,
			) -> std::task::Poll<F::Output> {
				RUNNING.with(|running| running.set(running.get() + 1));
				let polled = self.0.as_mut().poll(cx);
				RUNNING.with(|running| running.set(running.get() - 1));
				polled
			}
		}
		sealed(Running(Box::pin(future)))
	}
	#[cfg(not(test))]
	sealed(future)
}

// The run-time seal.
//
// A running program reads no catalog definition and builds no planner. The
// program holds no catalog assumption: every catalog fact it acts on is
// loaded when it is needed through the descriptor API
// (`crate::exec::vm::descriptor`), which reads through the transaction's
// cache. Every poll of a program — its stream, and each task it hands work
// to — is declared sealed, which a build with debug assertions checks: a
// catalog read panics ([`crate::kvs::catalog_reads::sealed`]), and so does
// building a [`Planner`](crate::exec::planner::Planner) ([`assert_may_plan`]).
//
// A seam is work entered with [`seam`] that may do both. The seams are:
// - the descriptor API, the one way a running program learns a catalog fact;
// - a load from the compile cache (`crate::exec::vm::compile_cache`), by name or by text: a table's
//   read unit, its field state and `SELECT` permission, the stages a write that may reach any
//   table, or a late-bound write, applies to a table, a closure body's unit, the code reached
//   through the operator tree's expressions — a function body a compiled expression calls, an
//   analyzer's function, an API's middleware and handler, an expression reached through
//   `crate::exec::entry`, a GQL mutation step's write (`crate::exec::vm::serve`) — and for a
//   late-bound statement a function's call unit, the body of a function that reads an argument
//   where only a value answers compiled for the call's values, and a parameter's permission
//   predicate. A load reads the object's version and checks what else the unit depends on, and
//   compiles nothing;
// - a compile-cache miss ([`serving_fault`]): the unit a load found none of, compiled once per key
//   and frame, the one kind of compile a running program causes;
// - the closure links a run makes and their checks that a held unit still stands;
// - a nested query ([`nested_query`]): `eval::surql` and `eval::gql` text and embedded JavaScript's
//   `surrealdb.query()`, compiled as a query and program of its own.
//
// An entry boundary is not a seam: a request that arrives from outside a
// running program — an HTTP request to an API route, a query — compiles its
// program before that program runs, unsealed.
//
// A program a seam runs is sealed again by its own stream. Outside a build
// with debug assertions none of this is compiled in.

#[cfg(debug_assertions)]
thread_local! {
	/// Whether the work polled on this thread is a running program outside
	/// any seam.
	static RUN_SEALED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Declares the work polled on this thread sealed or unsealed, catalog and
/// planner alike, until dropped.
struct Declared {
	#[cfg(debug_assertions)]
	_catalog: crate::kvs::catalog_reads::Declaration,
	#[cfg(debug_assertions)]
	planner: bool,
}

impl Declared {
	#[inline]
	fn new(sealed: bool) -> Self {
		#[cfg(debug_assertions)]
		{
			Self {
				_catalog: crate::kvs::catalog_reads::declare(sealed),
				planner: RUN_SEALED.with(|cell| cell.replace(sealed)),
			}
		}
		#[cfg(not(debug_assertions))]
		{
			let _ = sealed;
			Self {}
		}
	}
}

#[cfg(debug_assertions)]
impl Drop for Declared {
	fn drop(&mut self) {
		RUN_SEALED.with(|cell| cell.set(self.planner));
	}
}

/// A future polled under one declaration of the run-time seal. The future is
/// held as a trait object, so wrapping it adds no depth to the type of the
/// work around it.
#[cfg(debug_assertions)]
struct Scoped<'a, T> {
	inner: crate::exec::BoxFut<'a, T>,
	sealed: bool,
}

#[cfg(debug_assertions)]
impl<T> std::future::Future for Scoped<'_, T> {
	type Output = T;
	fn poll(
		mut self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<T> {
		let _declared = Declared::new(self.sealed);
		self.inner.as_mut().poll(cx)
	}
}

/// `future`, polled under the run-time seal. Only work handed to another
/// task is sealed this way, and a program hands none on wasm.
#[cfg(not(target_family = "wasm"))]
pub(crate) fn sealed<'a, F: std::future::Future + Send + 'a>(
	future: F,
) -> impl std::future::Future<Output = F::Output> + Send + 'a {
	#[cfg(debug_assertions)]
	{
		Scoped {
			inner: Box::pin(future),
			sealed: true,
		}
	}
	#[cfg(not(debug_assertions))]
	future
}

/// `stream`, polled under the run-time seal.
pub(crate) fn sealed_stream(stream: ValueBatchStream) -> ValueBatchStream {
	#[cfg(debug_assertions)]
	{
		struct SealedStream(ValueBatchStream);
		impl futures::Stream for SealedStream {
			type Item = <ValueBatchStream as futures::Stream>::Item;
			fn poll_next(
				mut self: std::pin::Pin<&mut Self>,
				cx: &mut std::task::Context<'_>,
			) -> std::task::Poll<Option<Self::Item>> {
				let _declared = Declared::new(true);
				self.0.as_mut().poll_next(cx)
			}
		}
		Box::pin(SealedStream(stream))
	}
	#[cfg(not(debug_assertions))]
	stream
}

/// `future`, a seam of the run-time seal: polled unsealed, whatever the work
/// around it. Only the seams the seal names enter one.
pub(crate) fn seam<'a, F: std::future::Future + Send + 'a>(
	future: F,
) -> impl std::future::Future<Output = F::Output> + Send + 'a {
	#[cfg(debug_assertions)]
	{
		Scoped {
			inner: Box::pin(future),
			sealed: false,
		}
	}
	#[cfg(not(debug_assertions))]
	future
}

/// `future`, a nested query: query text that exists only once a running
/// program reaches it — `eval::surql` / `eval::gql` text and an embedded
/// script's `surrealdb.query()` and `surrealdb.value()` — compiled by the
/// executor as a query of its own, in the caller's transaction, and run to its
/// value.
///
/// The one sanctioned way a running program has query text compiled for it;
/// the only other compile a run causes is a compile-cache miss
/// ([`serving_fault`]). The program itself is never recompiled, and what the
/// nested query compiles runs as a program of its own, sealed again by its own
/// stream. A [`seam`] by
/// another name, so every entry is found by it. A script's work is polled on
/// the thread running the script, so the future need not be `Send`; the
/// returned future is `Send` exactly when `future` is.
pub(crate) fn nested_query<'a, F: std::future::Future + 'a>(
	future: F,
) -> impl std::future::Future<Output = F::Output> + 'a {
	#[cfg(debug_assertions)]
	{
		NestedQuery {
			inner: Box::pin(future),
		}
	}
	#[cfg(not(debug_assertions))]
	future
}

/// [`nested_query`]'s work, polled unsealed. Boxed so wrapping it adds no
/// depth to the type of the work around it; `Send` exactly when the work is.
#[cfg(debug_assertions)]
struct NestedQuery<F> {
	inner: std::pin::Pin<Box<F>>,
}

#[cfg(debug_assertions)]
impl<F: std::future::Future> std::future::Future for NestedQuery<F> {
	type Output = F::Output;
	fn poll(
		mut self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<F::Output> {
		let _declared = Declared::new(false);
		self.inner.as_mut().poll(cx)
	}
}

/// [`seam`] for synchronous work.
#[inline]
pub(crate) fn seam_sync<R>(work: impl FnOnce() -> R) -> R {
	let _declared = Declared::new(false);
	work()
}

/// Panics, in a build with debug assertions, when a running program outside
/// every seam builds a planner.
#[inline]
#[track_caller]
pub(crate) fn assert_may_plan() {
	#[cfg(debug_assertions)]
	if RUN_SEALED.with(std::cell::Cell::get) {
		panic!("a planner was built by a running program outside every seam");
	}
}

#[cfg(all(test, debug_assertions))]
mod tests {
	use super::{sealed, seam};
	use crate::exec::planner::Planner;

	fn plan() {
		let ctx = crate::ctx::Context::new_test().freeze();
		let registry = crate::exec::function::FunctionRegistry::with_builtins();
		let _ = Planner::new(&ctx, &registry);
	}

	#[tokio::test]
	#[should_panic(expected = "outside every seam")]
	async fn a_running_program_that_builds_a_planner_panics() {
		sealed(async { plan() }).await;
	}

	#[tokio::test]
	async fn a_nested_query_may_build_a_planner() {
		sealed(super::nested_query(async { plan() })).await;
	}

	#[tokio::test]
	#[should_panic(expected = "outside every seam")]
	async fn a_running_program_is_sealed_again_after_a_nested_query() {
		sealed(async {
			super::nested_query(async { plan() }).await;
			plan();
		})
		.await;
	}

	#[tokio::test]
	async fn a_seam_of_a_running_program_may_build_a_planner() {
		sealed(seam(async { plan() })).await;
		// Once the program's poll returns, the work around it plans freely.
		plan();
	}
}
