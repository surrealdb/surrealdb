//! Build-time pre-initialisation of an extension.
//!
//! [`snapshot`] runs the component's `init` export once under `wasmtime-wizer`
//! and writes the memory and globals it leaves behind back into the component
//! as initial state. The result exports no `init`, so every instance the
//! runtime creates starts from the initialised image and nothing runs before
//! `invoke`.
//!
//! `init` runs against a stub WASI that makes the snapshot reproducible from
//! the archive alone:
//!
//! - the attached `fs/` is preopened read-only at `/`, as at run time;
//! - every `wasi:sockets` entry point traps, and the build fails;
//! - a WASI resource still open when `init` returns fails the build, because the handle index left
//!   in linear memory names nothing in the instance that later runs the snapshot;
//! - randomness and the clock work and are recorded. Their values are frozen into the image, which
//!   is the author's choice, so the build lists them for information and does not fail.
//!
//! Imports the snapshot store has no use for trap if called: the guest's
//! `transaction` resource, which `init` is never lent, and `binding`, whose
//! values belong to a definition that does not exist at build time.

use std::collections::BTreeSet;
use std::fmt;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use rand::RngCore;
use wasm_encoder::{
	Component as ComponentEncoder, ComponentSectionId, Encode, RawSection, Section,
};
use wasmparser::{Chunk, Parser, Payload};
use wasmtime::component::{
	Accessor, Component, Instance, Lift, Linker, ResourceTable, ResourceType, Val, WasmList,
};
use wasmtime::{Config, Engine, Store, StoreLimits, StoreLimitsBuilder, Trap};
use wasmtime_wasi::clocks::{MonotonicClock, WallClock};
use wasmtime_wasi::p2::{DynInputStream, DynOutputStream, DynPollable};
use wasmtime_wasi::sockets::{TcpSocket, UdpSocket};
use wasmtime_wasi::{
	FsPerms, HostMonotonicClock, HostWallClock, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView,
};
use wasmtime_wizer::{ComponentInstanceState, Wizer};

/// The export the snapshot runs and then removes.
const INIT: &str = "init";

/// The largest buffer one `get-random-bytes` call may ask for, matching
/// wasmtime-wasi's own limit.
const MAX_RANDOM_BYTES: u64 = 64 << 20;

/// A pre-initialised component and what its `init` read while it ran.
pub struct Snapshot {
	/// The component with `init`'s state as its initial data and no `init`
	/// export.
	pub wasm: Vec<u8>,
	/// Non-deterministic inputs `init` read, frozen into `wasm`.
	pub reads: BTreeSet<FrozenRead>,
}

/// A non-deterministic input whose value `init` captured into the image.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum FrozenRead {
	SecureRandom,
	InsecureRandom,
	InsecureSeed,
	WallClock,
	MonotonicClock,
}

impl fmt::Display for FrozenRead {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(match self {
			Self::SecureRandom => "randomness (wasi:random/random)",
			Self::InsecureRandom => "insecure randomness (wasi:random/insecure)",
			Self::InsecureSeed => {
				"the insecure seed (wasi:random/insecure-seed), e.g. for HashMap keys"
			}
			Self::WallClock => "the wall clock (wasi:clocks/wall-clock)",
			Self::MonotonicClock => "the monotonic clock (wasi:clocks/monotonic-clock)",
		})
	}
}

/// What `init` did that the build must report, shared between the store and
/// the host functions and clocks that observe it.
#[derive(Default)]
struct InitLog {
	reads: BTreeSet<FrozenRead>,
	/// The first `wasi:sockets` function `init` called.
	network: Option<String>,
}

type SharedLog = Arc<Mutex<InitLog>>;

fn record(log: &SharedLog, read: FrozenRead) {
	log.lock().unwrap_or_else(PoisonError::into_inner).reads.insert(read);
}

struct InitStore {
	wasi: WasiCtx,
	table: ResourceTable,
	limits: StoreLimits,
}

/// How long `init` may run and how much linear memory it may grow to while
/// the build snapshots it. Past either, the build fails rather than hang or
/// exhaust the build host's memory; a snapshot that needs more would also
/// ship that memory in every archive.
#[derive(Clone, Copy, Debug)]
pub struct InitLimits {
	pub time: Duration,
	pub memory: usize,
}

impl Default for InitLimits {
	fn default() -> Self {
		Self {
			time: Duration::from_secs(60),
			memory: 1 << 30,
		}
	}
}

/// How often the snapshot engine's epoch advances while `init` runs.
const EPOCH_TICK: Duration = Duration::from_millis(10);

impl WasiView for InitStore {
	fn ctx(&mut self) -> WasiCtxView<'_> {
		WasiCtxView {
			ctx: &mut self.wasi,
			table: &mut self.table,
		}
	}
}

struct RecordingWallClock {
	inner: WallClock,
	log: SharedLog,
}

impl HostWallClock for RecordingWallClock {
	fn resolution(&self) -> Duration {
		self.inner.resolution()
	}

	fn now(&self) -> Duration {
		record(&self.log, FrozenRead::WallClock);
		self.inner.now()
	}
}

struct RecordingMonotonicClock {
	inner: MonotonicClock,
	log: SharedLog,
}

impl HostMonotonicClock for RecordingMonotonicClock {
	fn resolution(&self) -> u64 {
		self.inner.resolution()
	}

	fn now(&self) -> u64 {
		record(&self.log, FrozenRead::MonotonicClock);
		self.inner.now()
	}
}

/// Pre-initialise `wasm` by running its `init` export with `fs` preopened.
///
/// Returns `None` for a component that exports no `init`; it is already in
/// the state every instance starts from.
///
/// Fails if `init` returns an error, traps, calls into `wasi:sockets`, or
/// leaves a WASI resource open, and if Wizer cannot snapshot the component
/// (for example because the guest mutates a table).
pub async fn snapshot(wasm: &[u8], fs: Option<&Path>) -> Result<Option<Snapshot>> {
	snapshot_with(wasm, fs, InitLimits::default()).await
}

/// As [`snapshot`], with `init` bounded by `limits`.
pub async fn snapshot_with(
	wasm: &[u8],
	fs: Option<&Path>,
	limits: InitLimits,
) -> Result<Option<Snapshot>> {
	let mut config = Config::new();
	config.wasm_component_model_async(true);
	config.concurrency_support(true);
	config.epoch_interruption(true);
	let engine = Engine::new(&config).map_err(wasmtime_err)?;
	// Advances the epoch until the engine is dropped with this function.
	let ticker = engine.weak();
	std::thread::Builder::new()
		.name("surrealism-snapshot-ticker".into())
		.spawn(move || {
			while let Some(engine) = ticker.upgrade() {
				engine.increment_epoch();
				drop(engine);
				std::thread::sleep(EPOCH_TICK);
			}
		})
		.context("Failed to start the timer that bounds `init`")?;

	let mut wizer = Wizer::new();
	// `keep_init_func(false)` would also drop the core module's `init` export
	// while leaving the alias that lifts it, which produces an invalid
	// component. The component-level export is removed by `strip_export`
	// instead, and the lifted function stays behind unexported.
	wizer.init_func(INIT).keep_init_func(true);
	let (cx, instrumented) = wizer.instrument_component(wasm).map_err(wasmtime_err)?;
	let component = Component::new(&engine, &instrumented).map_err(wasmtime_err)?;
	if component.get_export_index(None, INIT).is_none() {
		return Ok(None);
	}

	surrealism_runtime::runtime::refuse_unbound_imports(&engine, &component)?;
	let log = SharedLog::default();
	let mut linker = Linker::new(&engine);
	wasmtime_wasi::p2::add_to_linker_async(&mut linker).map_err(wasmtime_err)?;
	shadow_imports(&engine, &component, &mut linker, &log)?;

	let mut builder = WasiCtxBuilder::new();
	builder
		.inherit_stdout()
		.inherit_stderr()
		.allow_tcp(false)
		.allow_udp(false)
		.allow_ip_name_lookup(false)
		.wall_clock(RecordingWallClock {
			inner: WallClock::new(),
			log: Arc::clone(&log),
		})
		.monotonic_clock(RecordingMonotonicClock {
			inner: MonotonicClock::new(),
			log: Arc::clone(&log),
		});
	if let Some(fs) = fs {
		builder.preopened_dir(fs, "/", FsPerms::ReadOnly).map_err(wasmtime_err)?;
	}
	let mut store = Store::new(
		&engine,
		InitStore {
			wasi: builder.build(),
			table: ResourceTable::new(),
			limits: StoreLimitsBuilder::new().memory_size(limits.memory).build(),
		},
	);
	store.limiter(|state| &mut state.limits);
	let ticks = (limits.time.as_millis() / EPOCH_TICK.as_millis()).max(1);
	store.set_epoch_deadline(u64::try_from(ticks).unwrap_or(u64::MAX));

	let instance = linker.instantiate_async(&mut store, &component).await.map_err(wasmtime_err)?;
	let init =
		instance.get_typed_func::<(), (Result<(), String>,)>(&mut store, INIT).map_err(|e| {
			wasmtime_err(e).context("`init` does not have the type the Surrealism SDK exports")
		})?;
	let outcome = init.call_async(&mut store, ()).await;

	let log = std::mem::take(&mut *log.lock().unwrap_or_else(PoisonError::into_inner));
	if let Some(call) = log.network {
		bail!(
			"`init` called `{call}`. No network exists while the build snapshots `init`; \
			 move network access into a function the database invokes."
		);
	}
	match outcome {
		Ok((Ok(()),)) => {}
		Ok((Err(e),)) => bail!("`init` returned an error: {e}"),
		Err(e) if e.downcast_ref::<Trap>() == Some(&Trap::Interrupt) => {
			bail!("`init` ran longer than {:?}, the most the build lets it run", limits.time)
		}
		Err(e) => bail!(
			"`init` trapped: {:#}. It may grow linear memory to at most {} bytes while the \
			 build snapshots it.",
			wasmtime_err(e),
			limits.memory
		),
	}

	let open = open_resources(&mut store.data_mut().table);
	if !open.is_empty() {
		bail!(
			"`init` returned with WASI resources still open: {}. A handle does not survive \
			 the snapshot: its index in memory would name nothing in the instance that later \
			 runs it. Close everything `init` opens. Writing to stdout or stderr from `init` \
			 also leaves its stream open.",
			open.join(", ")
		);
	}

	let snapshot = wizer
		.snapshot_component(
			&cx,
			&mut SnapshotState {
				store: &mut store,
				instance,
			},
		)
		.await
		.map_err(wasmtime_err)?;
	Ok(Some(Snapshot {
		wasm: strip_export(&snapshot, INIT)?,
		reads: log.reads,
	}))
}

fn wasmtime_err(e: wasmtime::Error) -> anyhow::Error {
	anyhow::Error::from_boxed(e.into_boxed_dyn_error())
}

/// Describe each entry left in the store's resource table.
fn open_resources(table: &mut ResourceTable) -> Vec<&'static str> {
	table
		.iter_mut()
		.map(|entry| {
			if entry.is::<DynOutputStream>() {
				"output-stream"
			} else if entry.is::<DynInputStream>() {
				"input-stream"
			} else if entry.is::<DynPollable>() {
				"pollable"
			} else if entry.is::<TcpSocket>() || entry.is::<UdpSocket>() {
				"socket"
			} else {
				"resource"
			}
		})
		.collect()
}

/// Replace the imports `init` must not use, or whose use the build reports,
/// with the snapshot's own definitions.
///
/// Each interface is defined under the exact name the component imports, so it
/// shadows the semver-compatible WASI definition, and must therefore define
/// every function of that interface.
fn shadow_imports(
	engine: &Engine,
	component: &Component,
	linker: &mut Linker<InitStore>,
	log: &SharedLog,
) -> Result<()> {
	let imports: Vec<String> =
		component.component_type().imports(engine).map(|(name, _)| name.to_owned()).collect();
	linker.allow_shadowing(true);
	for name in &imports {
		let interface = name.split_once('@').map_or(name.as_str(), |(base, _)| base);
		match interface {
			"wasi:sockets/instance-network" => deny(linker, name, &["instance-network"], log)?,
			"wasi:sockets/tcp-create-socket" => deny(linker, name, &["create-tcp-socket"], log)?,
			"wasi:sockets/udp-create-socket" => deny(linker, name, &["create-udp-socket"], log)?,
			"wasi:random/random" => random(
				linker,
				name,
				["get-random-bytes", "get-random-u64"],
				FrozenRead::SecureRandom,
				log,
			)?,
			"wasi:random/insecure" => random(
				linker,
				name,
				["get-insecure-random-bytes", "get-insecure-random-u64"],
				FrozenRead::InsecureRandom,
				log,
			)?,
			"wasi:random/insecure-seed" => insecure_seed(linker, name, log)?,
			_ if interface.starts_with("surrealism:") && interface.ends_with("/host") => {
				transaction(linker, name)?;
			}
			_ => {}
		}
	}
	linker.allow_shadowing(false);
	Ok(())
}

/// Define `funcs` of `interface` to record the call and trap.
fn deny(
	linker: &mut Linker<InitStore>,
	interface: &str,
	funcs: &[&str],
	log: &SharedLog,
) -> Result<()> {
	let mut instance = linker.instance(interface).map_err(wasmtime_err)?;
	for func in funcs {
		let call = format!("{interface}#{func}");
		let log = Arc::clone(log);
		instance
			.func_new(func, move |_, _, _, _| {
				log.lock()
					.unwrap_or_else(PoisonError::into_inner)
					.network
					.get_or_insert_with(|| call.clone());
				Err(wasmtime::Error::msg("network access is not available during the build"))
			})
			.map_err(wasmtime_err)?;
	}
	Ok(())
}

/// Define a `wasi:random` interface whose `[bytes, u64]` functions record the
/// read and draw from the build host's thread RNG.
fn random(
	linker: &mut Linker<InitStore>,
	interface: &str,
	[bytes, word]: [&str; 2],
	read: FrozenRead,
	log: &SharedLog,
) -> Result<()> {
	let mut instance = linker.instance(interface).map_err(wasmtime_err)?;
	let bytes_log = Arc::clone(log);
	instance
		.func_new(bytes, move |_, _, params, results| {
			record(&bytes_log, read);
			let Some(Val::U64(len)) = params.first() else {
				return Err(wasmtime::Error::msg("expected a length"));
			};
			if *len > MAX_RANDOM_BYTES {
				return Err(wasmtime::Error::msg(format!(
					"requested {len} random bytes; at most {MAX_RANDOM_BYTES} are allowed"
				)));
			}
			let mut buf = vec![0u8; *len as usize];
			rand::rng().fill_bytes(&mut buf);
			results[0] = Val::List(buf.into_iter().map(Val::U8).collect());
			Ok(())
		})
		.map_err(wasmtime_err)?;
	let word_log = Arc::clone(log);
	instance
		.func_new(word, move |_, _, _, results| {
			record(&word_log, read);
			results[0] = Val::U64(rand::rng().next_u64());
			Ok(())
		})
		.map_err(wasmtime_err)?;
	Ok(())
}

fn insecure_seed(linker: &mut Linker<InitStore>, interface: &str, log: &SharedLog) -> Result<()> {
	let log = Arc::clone(log);
	linker
		.instance(interface)
		.map_err(wasmtime_err)?
		.func_new("insecure-seed", move |_, _, _, results| {
			record(&log, FrozenRead::InsecureSeed);
			let mut rng = rand::rng();
			results[0] = Val::Tuple(vec![Val::U64(rng.next_u64()), Val::U64(rng.next_u64())]);
			Ok(())
		})
		.map_err(wasmtime_err)?;
	Ok(())
}

/// The host's `transaction` resource. `init` is lent no handle, so its methods
/// are unreachable during the snapshot; they trap should that ever change.
struct NoTransaction;

fn transaction(linker: &mut Linker<InitStore>, interface: &str) -> Result<()> {
	let mut instance = linker.instance(interface).map_err(wasmtime_err)?;
	instance
		.resource("transaction", ResourceType::host::<NoTransaction>(), |_, _| Ok(()))
		.map_err(wasmtime_err)?;
	for method in ["[method]transaction.sql", "[method]transaction.run"] {
		instance
			.func_new_concurrent(method, |_: &Accessor<InitStore>, _, _, _| {
				Box::pin(async { Err(wasmtime::Error::msg("`init` is lent no transaction")) })
			})
			.map_err(wasmtime_err)?;
	}
	instance
		.func_new_concurrent("binding", |_: &Accessor<InitStore>, _, _, _| {
			Box::pin(async {
				Err(wasmtime::Error::msg(
					"`init` cannot read a binding: bindings belong to a definition, which does not exist at build time",
				))
			})
		})
		.map_err(wasmtime_err)?;
	Ok(())
}

/// Wizer's view of the instance `init` ran in, through the accessor exports
/// that instrumentation added.
struct SnapshotState<'a> {
	store: &'a mut Store<InitStore>,
	instance: Instance,
}

impl SnapshotState<'_> {
	async fn call<R: Lift + Send + Sync + 'static, O>(
		&mut self,
		instance: &str,
		func: &str,
		read: impl FnOnce(&mut Store<InitStore>, R) -> O,
	) -> O {
		// Instrumentation added these exports and gave them these types, so a
		// failure here is a fault in Wizer, not in the extension.
		let (_, parent) = self
			.instance
			.get_export(&mut *self.store, None, instance)
			.expect("instrumented component exports the wizer accessor instance");
		let (_, export) = self
			.instance
			.get_export(&mut *self.store, Some(&parent), func)
			.expect("wizer accessor instance exports the requested function");
		let typed = self
			.instance
			.get_typed_func::<(), (R,)>(&mut *self.store, export)
			.expect("wizer accessor has the type Wizer requested");
		let (value,) = typed
			.call_async(&mut *self.store, ())
			.await
			.expect("wizer accessor reads instance state without trapping");
		read(&mut *self.store, value)
	}
}

impl ComponentInstanceState for SnapshotState<'_> {
	async fn call_func_ret_list_u8(
		&mut self,
		instance: &str,
		func: &str,
		contents: impl FnOnce(&[u8]) + Send,
	) {
		self.call(instance, func, |store, list: WasmList<u8>| contents(list.as_le_slice(&store)))
			.await
	}

	async fn call_func_ret_s32(&mut self, instance: &str, func: &str) -> i32 {
		self.call(instance, func, |_, v| v).await
	}

	async fn call_func_ret_s64(&mut self, instance: &str, func: &str) -> i64 {
		self.call(instance, func, |_, v| v).await
	}

	async fn call_func_ret_f32(&mut self, instance: &str, func: &str) -> u32 {
		self.call(instance, func, |_, v: f32| v.to_bits()).await
	}

	async fn call_func_ret_f64(&mut self, instance: &str, func: &str) -> u64 {
		self.call(instance, func, |_, v: f64| v.to_bits()).await
	}
}

/// Remove the top-level component export `name`.
///
/// wit-component emits each export after everything it refers to, and `init`
/// last, so dropping its entry shifts no index that any later item uses. The
/// function insists on that shape — `name` must be the final non-custom item —
/// rather than renumbering, and validates the result.
fn strip_export(wasm: &[u8], name: &str) -> Result<Vec<u8>> {
	let mut parser = Parser::new(0);
	let mut offset = 0;
	let mut out = ComponentEncoder::HEADER.to_vec();
	let mut stripped = false;
	let not_last = || anyhow::anyhow!("the `{name}` export is not the component's last item");
	loop {
		let Chunk::Parsed {
			consumed,
			payload,
		} = parser.parse(&wasm[offset..], true)?
		else {
			bail!("the component is truncated");
		};
		offset += consumed;
		if matches!(payload, Payload::Version { .. }) {
			continue;
		} else if matches!(payload, Payload::End(_)) {
			break;
		} else if matches!(payload, Payload::CustomSection(_)) {
			// Custom sections are allowed after the stripped export.
		} else if let Payload::ModuleSection {
			ref unchecked_range,
			..
		}
		| Payload::ComponentSection {
			ref unchecked_range,
			..
		} = payload
		{
			// A nested module or component is a single section whose body the
			// outer parser does not descend into when driven by `parse`.
			if stripped {
				return Err(not_last());
			}
			let id = if matches!(payload, Payload::ModuleSection { .. }) {
				ComponentSectionId::CoreModule
			} else {
				ComponentSectionId::Component
			};
			RawSection {
				id: id as u8,
				data: &wasm[unchecked_range.clone()],
			}
			.append_to(&mut out);
			offset += unchecked_range.len();
			continue;
		} else if let Payload::ComponentExportSection(ref reader) = payload {
			if stripped {
				return Err(not_last());
			}
			let entries = reader.clone().into_iter_with_offsets().collect::<Result<Vec<_>, _>>()?;
			if let Some(pos) = entries.iter().position(|(_, export)| export.name.name == name) {
				if pos + 1 != entries.len() {
					return Err(not_last());
				}
				if pos > 0 {
					let mut data = Vec::new();
					(pos as u32).encode(&mut data);
					data.extend_from_slice(&wasm[entries[0].0..entries[pos].0]);
					RawSection {
						id: ComponentSectionId::Export as u8,
						data: &data,
					}
					.append_to(&mut out);
				}
				stripped = true;
				continue;
			}
		} else if stripped {
			return Err(not_last());
		}
		if let Some((id, range)) = payload.as_section() {
			RawSection {
				id,
				data: &wasm[range],
			}
			.append_to(&mut out);
		}
	}
	if !stripped {
		bail!("the component has no `{name}` export");
	}
	wasmparser::Validator::new_with_features(wasmparser::WasmFeatures::all())
		.validate_all(&out)
		.map_err(|e| {
			anyhow::anyhow!("removing the `{name}` export left an invalid component: {e}")
		})?;
	Ok(out)
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use wasmtime::component::{Component, Linker};
	use wasmtime::{Config, Engine, Store};

	use super::{FrozenRead, InitLimits, snapshot, snapshot_with};

	/// A component whose core module exports `init`, lifted as
	/// `func() -> result<_, string>` like the SDK's, and `get`, which returns
	/// the `i32` at address 64.
	///
	/// The module imports `f0: () -> i32` and `f1`, `f2: () -> i64` from `host`.
	/// `imports` declares component imports and lowers them into core
	/// functions; `f0`..`f2` name the core function behind each import, with a
	/// do-nothing `$stubs` instance available for any the test does not use.
	fn component(imports: &str, [f0, f1, f2]: [&str; 3], init: &str) -> Vec<u8> {
		wat::parse_str(format!(
			r#"(component $C
				{imports}
				(core module $stub
					(func (export "f0") (result i32) (i32.const 0))
					(func (export "f1") (result i64) (i64.const 0))
					(func (export "f2") (result i64) (i64.const 0)))
				(core instance $stubs (instantiate $stub))
				(alias core export $stubs "f0" (core func $s0))
				(alias core export $stubs "f1" (core func $s1))
				(alias core export $stubs "f2" (core func $s2))
				(core instance $host
					(export "f0" (func {f0}))
					(export "f1" (func {f1}))
					(export "f2" (func {f2})))
				(core module $m
					(import "host" "f0" (func $f0 (result i32)))
					(import "host" "f1" (func $f1 (result i64)))
					(import "host" "f2" (func $f2 (result i64)))
					(memory (export "memory") 1)
					(data (i32.const 32) "boom")
					(func (export "init") (result i32) {init})
					(func (export "get") (result i32) (i32.load (i32.const 64))))
				(core instance $i (instantiate $m (with "host" (instance $host))))
				(alias core export $i "get" (core func $get))
				(alias core export $i "init" (core func $init))
				(alias core export $i "memory" (core memory $memory))
				(func (export "get") (result u32) (canon lift (core func $get)))
				(func (export "init") (result (result (error string)))
					(canon lift (core func $init) (memory $memory))))"#
		))
		.expect("test component is valid WAT")
	}

	const STUBS: [&str; 3] = ["$s0", "$s1", "$s2"];

	/// Stores 42 at address 64 and returns `ok`: the return area at 0 is zero.
	const SET_STATIC: &str = "(i32.store (i32.const 64) (i32.const 42)) (i32.const 0)";

	/// Whether `wasm` exports `init`, and what `get` returns in a fresh instance
	/// that runs nothing else.
	async fn inspect(wasm: &[u8]) -> (bool, u32) {
		let mut config = Config::new();
		config.wasm_component_model_async(true);
		let engine = Engine::new(&config).unwrap();
		let component = Component::new(&engine, wasm).unwrap();
		let has_init = component.get_export_index(None, "init").is_some();
		let mut store = Store::new(&engine, ());
		// `get` calls no import, so a trapping stub satisfies each one.
		let mut linker = Linker::new(&engine);
		linker.define_unknown_imports_as_traps(&component).unwrap();
		let instance = linker.instantiate_async(&mut store, &component).await.unwrap();
		let get = instance.get_typed_func::<(), (u32,)>(&mut store, "get").unwrap();
		(has_init, get.call_async(&mut store, ()).await.unwrap().0)
	}

	async fn build_error(wasm: &[u8]) -> String {
		format!("{:#}", snapshot(wasm, None).await.err().expect("the snapshot must fail"))
	}

	#[tokio::test]
	async fn statics_are_set_without_running_init() {
		let wasm = component("", STUBS, SET_STATIC);
		assert_eq!(inspect(&wasm).await, (true, 0), "the input starts unset");

		let snapshot = snapshot(&wasm, None).await.unwrap().expect("the component exports init");
		assert_eq!(inspect(&snapshot.wasm).await, (false, 42), "set by init, which is gone");
		assert!(snapshot.reads.is_empty());
	}

	#[tokio::test]
	async fn without_init_there_is_nothing_to_snapshot() {
		let wasm = wat::parse_str(
			r#"(component
				(core module $m (func (export "get") (result i32) (i32.const 7)))
				(core instance $i (instantiate $m))
				(alias core export $i "get" (core func $get))
				(func (export "get") (result u32) (canon lift (core func $get))))"#,
		)
		.unwrap();
		assert!(snapshot(&wasm, None).await.unwrap().is_none());
	}

	#[tokio::test]
	async fn init_error_fails_the_build() {
		// `err("boom")`: discriminant 1, then the string's pointer and length.
		let init = "(i32.store8 (i32.const 0) (i32.const 1))
			(i32.store (i32.const 4) (i32.const 32))
			(i32.store (i32.const 8) (i32.const 4))
			(i32.const 0)";
		let err = build_error(&component("", STUBS, init)).await;
		assert!(err.contains("`init` returned an error: boom"), "{err}");
	}

	/// An `init` that never returns fails the build at its time limit.
	#[tokio::test]
	async fn a_looping_init_fails_the_build() {
		let limits = InitLimits {
			time: Duration::from_millis(200),
			..InitLimits::default()
		};
		let wasm = component("", STUBS, "(loop br 0) (i32.const 0)");
		let start = std::time::Instant::now();
		let err = format!("{:#}", snapshot_with(&wasm, None, limits).await.err().expect("refused"));
		assert!(err.contains("ran longer than"), "{err}");
		assert!(start.elapsed() < Duration::from_secs(10));
	}

	/// An `init` that grows memory past the limit is refused the growth.
	#[tokio::test]
	async fn an_allocating_init_fails_the_build() {
		let limits = InitLimits {
			memory: 1 << 20,
			..InitLimits::default()
		};
		// 64 pages is 4 MiB; a refused grow returns -1.
		let init = "(if (i32.eq (memory.grow (i32.const 64)) (i32.const -1)) (then unreachable))
			(i32.const 0)";
		let err = format!(
			"{:#}",
			snapshot_with(&component("", STUBS, init), None, limits).await.err().expect("refused")
		);
		assert!(err.contains("at most 1048576 bytes"), "{err}");
	}

	#[tokio::test]
	async fn init_trap_fails_the_build() {
		let err = build_error(&component("", STUBS, "unreachable")).await;
		assert!(err.contains("`init` trapped"), "{err}");
	}

	#[tokio::test]
	async fn network_access_fails_the_build() {
		let imports = r#"
			(import "wasi:sockets/network@0.2.0"
				(instance $net (export "network" (type (sub resource)))))
			(alias export $net "network" (type $network))
			(import "wasi:sockets/instance-network@0.2.0" (instance $inet
				(alias outer $C $network (type $n))
				(export "network" (type $exported (eq $n)))
				(export "instance-network" (func (result (own $exported))))))
			(alias export $inet "instance-network" (func $instance_network_f))
			(core func $instance_network (canon lower (func $instance_network_f)))"#;
		let wasm = component(
			imports,
			["$instance_network", STUBS[1], STUBS[2]],
			"(drop (call $f0)) (i32.const 0)",
		);
		let err = build_error(&wasm).await;
		assert!(
			err.contains("`init` called `wasi:sockets/instance-network@0.2.0#instance-network`"),
			"{err}"
		);
	}

	#[tokio::test]
	async fn open_resource_fails_the_build() {
		let imports = r#"
			(import "wasi:io/streams@0.2.0"
				(instance $streams (export "output-stream" (type (sub resource)))))
			(alias export $streams "output-stream" (type $output_stream))
			(import "wasi:cli/stdout@0.2.0" (instance $stdout
				(alias outer $C $output_stream (type $s))
				(export "output-stream" (type $exported (eq $s)))
				(export "get-stdout" (func (result (own $exported))))))
			(alias export $stdout "get-stdout" (func $get_stdout_f))
			(core func $get_stdout (canon lower (func $get_stdout_f)))"#;
		// Takes the stream and never drops the handle.
		let wasm = component(
			imports,
			["$get_stdout", STUBS[1], STUBS[2]],
			"(drop (call $f0)) (i32.const 0)",
		);
		let err = build_error(&wasm).await;
		assert!(err.contains("WASI resources still open: output-stream."), "{err}");
	}

	#[tokio::test]
	async fn clock_and_randomness_reads_are_reported() {
		let imports = r#"
			(import "wasi:clocks/monotonic-clock@0.2.0"
				(instance $clock (export "now" (func (result u64)))))
			(import "wasi:random/random@0.2.0"
				(instance $random (export "get-random-u64" (func (result u64)))))
			(alias export $clock "now" (func $now_f))
			(alias export $random "get-random-u64" (func $random_f))
			(core func $now (canon lower (func $now_f)))
			(core func $random (canon lower (func $random_f)))"#;
		let init = format!("(drop (call $f1)) (drop (call $f2)) {SET_STATIC}");
		let wasm = component(imports, [STUBS[0], "$now", "$random"], &init);
		let snapshot = snapshot(&wasm, None).await.unwrap().expect("the component exports init");
		assert_eq!(
			snapshot.reads.into_iter().collect::<Vec<_>>(),
			[FrozenRead::SecureRandom, FrozenRead::MonotonicClock]
		);
		assert_eq!(inspect(&snapshot.wasm).await, (false, 42));
	}
}
