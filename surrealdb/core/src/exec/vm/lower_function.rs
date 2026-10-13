//! Lowering calls of user-defined functions and reads of parameters.
//!
//! A call of `fn::name` is lowered with everything it runs: the body, a
//! program bound for the call's principal capped by the definition's `AUTH
//! LIMIT`; the body bound for that principal in a frame that waives
//! permission checks, for a call evaluated where reads are unchecked; the
//! predicate of the definition's `PERMISSIONS` clause; and
//! the hash of the definition they were all lowered from. A body that is one
//! expression over its arguments is inlined into the caller instead
//! ([`super::super::inline`]), and has no unit.
//!
//! Bodies are units of an arena. A call lowered at the top of a call path —
//! from a statement, not from a body — makes a bundle: an arena holding the
//! body and every body it calls, a recursive edge being an id that refers to
//! a unit of the same arena. The bundle is kept by the node
//! ([`super::super::units`]) and the caller holds it through a unit that owns
//! the arena ([`UnitHead::entry`]). A call lowered inside a bundle is a unit
//! of that bundle's arena, found by the function's name and the frame its
//! body is lowered for, so recursion and mutual recursion are cycles of ids.
//!
//! A read of `$name` is [`Instr::LoadParam`] of a [`ParamSpec`], which holds
//! the predicate of the `DEFINE PARAM` the lowering found, for the run to judge
//! the definition it reads.

use std::sync::Arc;

use super::{Lowered, Lowering, NotCovered, lend};
use crate::exec::planner::{FunctionLookup, PlanTimeVersion, Planner, PreparedBody};
use crate::exec::vm::program::{
	DefHash, FunctionSpec, Instr, Operand, ParamSpec, Program, UnitFrame, UnitHead, UnitId, UnitSig,
};
use crate::exec::vm::units::{Bundle, FunctionUnit};
use crate::exec::{AccessMode, ContextLevel};
use crate::expr::logical::{LogicalCall, LogicalExpr, LogicalPermissionRef};

/// A function body linked into an arena: the function's name, the frame its
/// body was lowered for, and its unit.
pub(super) struct FnLink {
	name: String,
	frame: UnitFrame,
	unit: UnitId,
}

/// The units of the bundle being lowered, and the functions among them.
#[derive(Default)]
pub(super) struct Arena<'ctx> {
	pub(super) units: Vec<Arc<Program>>,
	pub(super) links: Vec<FnLink>,
	pub(super) cascades: Vec<super::write_cascade::CascadeLink>,
	pub(super) observers: Vec<super::write_observe::ObserverLink>,
	pub(super) events: super::write_observe::EventsQueue<'ctx>,
	pub(super) closures: Vec<super::closure::ClosureLink>,
}

impl<'ctx> Arena<'ctx> {
	/// The same units and links, for a lowering under another planner: what
	/// the arena keeps between two statements' lowerings, whose queue of event
	/// lowerings each drains before it ends.
	pub(super) fn moved<'other>(self) -> Arena<'other> {
		Arena {
			units: self.units,
			links: self.links,
			cascades: self.cascades,
			observers: self.observers,
			events: Default::default(),
			closures: self.closures,
		}
	}

	/// How many units the arena holds.
	pub(super) fn len(&self) -> usize {
		self.units.len()
	}

	/// Drop the units from `len` on, and the links that name them.
	pub(super) fn rewind(&mut self, len: usize) {
		self.units.truncate(len);
		self.links.retain(|link| (link.unit.0 as usize) < len);
		self.cascades.retain(|link| link.names_before(len));
		self.observers.retain(|link| link.names_before(len));
		self.closures.retain(|link| link.names_before(len));
	}
}

/// What a call needs of the function it calls, as lowered.
struct Callee {
	/// Whether the function was defined when the call was lowered; a call of
	/// one that was not fails as entering it does.
	defined: bool,
	hash: DefHash,
	checked: bool,
	inline: Option<(Arc<LogicalExpr>, Vec<String>)>,
	guard: Option<(Arc<LogicalExpr>, crate::iam::AuthLimit)>,
	body: Option<UnitId>,
	unchecked_body: Option<UnitId>,
}

impl Callee {
	/// A call of a function that is undefined or refused by the capabilities:
	/// entering it raises the error that says so.
	fn absent() -> Self {
		Self {
			defined: false,
			hash: DefHash(0),
			checked: false,
			inline: None,
			guard: None,
			body: None,
			unchecked_body: None,
		}
	}
}

/// The predicate of `definition`'s `PERMISSIONS` clause and the `AUTH LIMIT`
/// it is judged under; `None` for a clause that is `FULL`, `NONE` or whose
/// limit does not parse, which the run reports as it enters.
fn guard_of(
	definition: &crate::catalog::FunctionDefinition,
) -> Lowered<Option<(Arc<LogicalExpr>, crate::iam::AuthLimit)>> {
	let permission = definition.permissions_body.logical_ref().map_err(|e| {
		NotCovered::internal(format!("a function permission with no logical form: {e}"))
	})?;
	let LogicalPermissionRef::Specific(guard) = permission else {
		return Ok(None);
	};
	Ok(crate::iam::AuthLimit::try_from(&definition.auth_limit)
		.ok()
		.map(|limit| (Arc::new(guard.clone()), limit)))
}

/// The frame the body prepared as `prepared` is lowered for: the principal it
/// is bound for, and whether it assumes permissions are checked.
fn body_frame(prepared_planner: &Planner<'_>, unchecked_variant: bool) -> UnitFrame {
	UnitFrame {
		class_digest: prepared_planner.class_digest(prepared_planner.auth.as_deref()),
		ns: prepared_planner.ns.clone(),
		db: prepared_planner.db.clone(),
		perms_suppressed: prepared_planner.perms_suppressed,
		checked: !unchecked_variant,
	}
}

/// The signature of a body: its arguments, bound by name, over the calling
/// row.
fn body_sig(definition: &crate::catalog::FunctionDefinition) -> UnitSig {
	UnitSig {
		params: definition.args.iter().map(|(name, _)| name.as_str().into()).collect(),
		takes_row: true,
	}
}

/// Lower `prepared`, the body of function `name`, as a unit of `arena`, and
/// answer its id; the unit already there when the body was lowered for the
/// same frame before, which is how a recursion closes.
///
/// The body is lowered into `arena`, and everything it reaches is linked
/// there, once. A body whose lowering linked no unit beyond its own and whose
/// program is a small leaf is lowered again as a light body ([`light_body`]),
/// which then is its unit; a light body reaches no unit, so that lowering
/// lowers nothing beyond the body, and no body is lowered outside the arena
/// its call is linked into.
async fn link_function<'c>(
	arena: &mut Arena<'c>,
	name: &str,
	prepared: &PreparedBody<'c>,
	frame: UnitFrame,
) -> Lowered<UnitId> {
	if let Some(link) = arena.links.iter().find(|l| l.name == name && l.frame == frame) {
		return Ok(link.unit);
	}
	let planner = &prepared.planner;
	let id = u32::try_from(arena.units.len())
		.map_err(|_| NotCovered::internal("more units than a program holds"))?;
	// The slot is held while the body lowers, so a call of the function from
	// inside it finds it.
	arena.units.push(Arc::new(Lowering::new(planner).finish()?));
	arena.links.push(FnLink {
		name: name.to_owned(),
		frame: frame.clone(),
		unit: UnitId(id),
	});
	let mut program = match prepared.specialised {
		// A body that reads an argument where only a value answers runs the
		// body compiled for the values each call's arguments hold.
		true => {
			let mut l = Lowering::new(planner);
			l.specialised_body(name)?;
			l.finish()?
		}
		false => {
			let physical = super::super::optimize::optimize(
				super::super::map::map(planner, &prepared.bound).await?,
			);
			let program = crate::exec::vm::host::out_of_line(|| {
				super::lower_in(planner, &physical, Some(&mut *arena), None)
			})
			.await?;
			let reached = arena.units.len() > id as usize + 1;
			if reached || !program.small_leaf() {
				program
			} else {
				crate::exec::vm::host::out_of_line(|| light_body(planner, prepared, &physical))
					.await?
					.unwrap_or(program)
			}
		}
	};
	program.unit = Some(UnitHead {
		sig: body_sig(&prepared.frame.compiled_from),
		frame,
		entry: None,
		closure: None,
	});
	arena.units[id as usize] = Arc::new(program);
	Ok(UnitId(id))
}

/// `physical`, the plan of the body prepared as `prepared`, lowered as a
/// light body where it is one: a small leaf ([`Program::small_leaf`]) that
/// reads its arguments from slots `0..` in declaration order, and reads
/// nothing else of the frame it runs in — no parameter, no name from a
/// pipeline of its own — and runs no external or invoked code. A call runs
/// such a body with no frame of its own ([`FunctionSpec::light`]). `None`
/// for any other body, whose unit is its lowering reading its arguments as
/// parameters.
///
/// The lowering is one of its own, with an arena of its own, so it is made
/// only for a body that [`link_function`] lowered as a small leaf reaching no
/// unit: one that reached a unit would lower it again here, and every unit
/// that unit reaches, outside the arena that links and bounds them.
async fn light_body(
	planner: &Planner<'_>,
	prepared: &PreparedBody<'_>,
	physical: &super::super::plan::PhysicalPlan,
) -> Lowered<Option<Program>> {
	let Some(mut l) = light_lowering(planner, prepared, physical) else {
		return Ok(None);
	};
	if crate::kvs::catalog_reads::sealed(
		true,
		crate::exec::vm::host::out_of_line(|| l.root(physical)),
	)
	.await
	.is_err()
	{
		return Ok(None);
	}
	light_program(l)
}

/// The lowering a light body is lowered into, with its arguments as the
/// slots `0..`. Built out of line, and boxed, so that the poll of
/// [`light_body`] holds a pointer where the lowering's whole size would
/// otherwise sit in its frame.
#[inline(never)]
fn light_lowering<'p, 'ctx>(
	planner: &'p Planner<'ctx>,
	prepared: &PreparedBody<'_>,
	physical: &super::super::plan::PhysicalPlan,
) -> Option<Box<Lowering<'p, 'ctx>>> {
	if physical.tail != super::super::plan::Tail::default() {
		return None;
	}
	let args = &prepared.frame.compiled_from.args;
	let slots = u16::try_from(args.len()).ok()?;
	let mut l = Box::new(Lowering::new(planner));
	l.ahead = true;
	l.stage_sets = true;
	l.slot_names = args
		.iter()
		.zip(0..slots)
		.map(|((name, _), slot)| (name.as_str().to_owned(), slot))
		.collect();
	l.slots = slots;
	Some(l)
}

/// The program of a light body whose lowering has run, or `None` where the
/// body is not one: it reads a frame, a parameter, an external or invoked
/// code, or its program is not a small leaf. Out of line, for the same reason
/// as [`light_lowering`].
#[inline(never)]
fn light_program(l: Box<Lowering<'_, '_>>) -> Lowered<Option<Program>> {
	let frameless = l.frame_reads == 0
		&& l.params.is_empty()
		&& l.externals.is_empty()
		&& l.invokes == 0
		&& l.closures.is_empty()
		&& l.closure_values.is_empty();
	if !frameless {
		return Ok(None);
	}
	let program = match (*l).finish() {
		Ok(program) => program,
		Err(refused) if refused.internal => return Err(refused),
		Err(_) => return Ok(None),
	};
	Ok(program.small_leaf().then_some(program))
}

impl<'ctx> Lowering<'_, 'ctx> {
	/// The units of the bundle this lowering is part of, taken to be lent to
	/// a lowering of a body and put back afterwards.
	pub(super) fn take_arena(&mut self) -> Arena<'ctx> {
		Arena {
			units: std::mem::take(&mut self.units),
			links: std::mem::take(&mut self.fn_links),
			cascades: std::mem::take(&mut self.cascade_links),
			observers: std::mem::take(&mut self.observer_links),
			events: std::mem::take(&mut self.events_queue),
			closures: std::mem::take(&mut self.closure_links),
		}
	}

	/// Drop the units of the arena from `len` on, and the links that name them.
	pub(super) fn rewind_arena(&mut self, len: usize) {
		self.units.truncate(len);
		self.fn_links.retain(|link| (link.unit.0 as usize) < len);
		self.cascade_links.retain(|link| link.names_before(len));
		self.observer_links.retain(|link| link.names_before(len));
		self.events_queue.rewind(len);
		self.closure_links.retain(|link| link.names_before(len));
	}

	pub(super) fn put_arena(&mut self, arena: Arena<'ctx>) {
		self.units = arena.units;
		self.fn_links = arena.links;
		self.cascade_links = arena.cascades;
		self.observer_links = arena.observers;
		self.events_queue = arena.events;
		self.closure_links = arena.closures;
	}

	/// The frame a call is lowered for, which admits any principal: what
	/// confines a call is the frame of the body it runs.
	fn caller_frame(&self) -> UnitFrame {
		UnitFrame {
			class_digest: 0,
			ns: self.planner.ns.clone(),
			db: self.planner.db.clone(),
			perms_suppressed: self.planner.perms_suppressed,
			checked: true,
		}
	}

	/// The frame a call is kept under: the principal and the permission
	/// contract the call is lowered for.
	fn key_frame(&self, unchecked: bool) -> UnitFrame {
		UnitFrame {
			class_digest: self.planner.class_digest(self.planner.auth.as_deref()),
			ns: self.planner.ns.clone(),
			db: self.planner.db.clone(),
			perms_suppressed: self.planner.perms_suppressed,
			checked: !unchecked,
		}
	}

	/// Whether the `AUTH LIMIT` of `fn::name`, as the plan read it, leaves the
	/// principal the plan is lowered for as it is: a record user's or an
	/// anonymous principal's always, and a system user's where the limit
	/// narrows neither its level nor its roles.
	fn limit_keeps_principal(&self, name: &str) -> bool {
		let Some(auth) = &self.planner.auth else {
			return false;
		};
		let Ok(FunctionLookup::Found(snapshot)) = self.function_lookup(name) else {
			return false;
		};
		match crate::iam::AuthLimit::try_from(&snapshot.definition().auth_limit) {
			Ok(limit) => auth.new_limited(&limit) == **auth,
			Err(_) => false,
		}
	}

	/// `call`, a call of the user-defined function `name`.
	pub(super) async fn function_call(
		&mut self,
		call: &LogicalCall,
		name: &str,
	) -> Lowered<Operand> {
		// A call under `VERSION` runs the definition stored at the stamp, which
		// a program is lowered against only where the stamp is known to it.
		let stamped = match self.planner.plan_time_version {
			PlanTimeVersion::Current => !self.versioned,
			PlanTimeVersion::At(_) => true,
			PlanTimeVersion::Unknown => false,
		};
		// One whose stamp only the run knows, or whose definition the plan
		// could not read, is called through the definition the call reads
		// when it runs.
		if !stamped || matches!(self.function_lookup(name)?, FunctionLookup::Unavailable) {
			return Box::pin(self.external_call(&LogicalExpr::Call(Box::new(call.clone())), call))
				.await;
		}
		let unchecked = self.reads_unchecked();
		let callee = crate::exec::vm::host::out_of_line(|| self.callee(name, unchecked)).await?;
		let guard = match &callee.guard {
			Some((expr, limit)) if !unchecked => {
				let mut planner = self.planner.shadowing(std::iter::empty());
				if let Some(auth) = &self.planner.auth {
					planner = planner.with_auth(Arc::new(auth.new_limited(limit)));
				}
				let planner = planner.with_depth(self.planner.current_depth() + 1);
				let scope = crate::exec::guard_verdicts::GuardScope::of(
					expr,
					self.planner.function_registry(),
				);
				Some((
					callee.hash,
					crate::exec::vm::host::out_of_line(|| self.guard_pipeline_in(&planner, expr))
						.await?,
					scope,
				))
			}
			_ => None,
		};
		let body_access_mode: AccessMode = self.catalog.function_access(name)?;
		if self.catalog.function_keeps_row_order(name)? {
			self.row_ordered_calls += 1;
		}
		let light = callee.inline.is_none()
			&& callee.body.is_some_and(|unit| self.light(unit))
			&& callee.unchecked_body.is_none_or(|unit| self.light(unit));
		self.functions.push(FunctionSpec {
			name: name.to_owned(),
			qualified: format!("fn::{name}"),
			plan_depth: self.planner.current_depth(),
			body_access_mode,
			defined: callee.defined,
			def_hash: callee.hash,
			frame: self.caller_frame(),
			checked: callee.checked,
			body: callee.body,
			unchecked_body: callee.unchecked_body,
			guard,
			depth: self.call_depth(name)?,
			light,
		});
		let function = (self.functions.len() - 1) as u32;
		let frame = super::super::program::CALL_FRAME
			.checked_add(self.call_frames)
			.ok_or_else(|| NotCovered::internal("more calls than a program holds"))?;
		self.call_frames += 1;
		let outer = self.eval_frame;
		// The arguments are evaluated under the definition's `AUTH LIMIT`, in
		// the call's frame. Where the limit leaves the caller's principal as
		// it is, the call's frame differs from the caller's in nothing, and
		// the arguments are evaluated in the caller's.
		let args_frame = match self.limit_keeps_principal(name) {
			true => outer,
			false => Some(frame),
		};
		self.emit(Instr::FunctionEnter {
			function,
			frame,
			outer,
			elem: self.element,
		});
		let dst = self.register()?;
		match callee.inline {
			Some((expr, params)) => {
				let args =
					crate::exec::vm::host::out_of_line(|| self.call_args(&call.args, args_frame))
						.await?;
				let first = self.registers(params.len())?;
				self.emit(Instr::FunctionBind {
					function,
					frame,
					args,
					params: first,
				});
				// The body reads its arguments from the registers they are
				// bound into, and runs in the call's frame, as a body called
				// would run under the definer's auth.
				let bindings = params
					.into_iter()
					.enumerate()
					.map(|(i, p)| (p, first + i as super::Reg))
					.collect();
				let outer_params = std::mem::replace(&mut self.inline_params, bindings);
				let outer_unchecked = std::mem::replace(&mut self.unchecked_reads, unchecked);
				self.eval_frame = Some(frame);
				let value = crate::exec::vm::host::out_of_line(|| self.value(&expr)).await;
				self.eval_frame = outer;
				self.unchecked_reads = outer_unchecked;
				self.inline_params = outer_params;
				let value = value?;
				self.emit(Instr::FunctionReturn {
					function,
					frame,
					src: value,
					dst,
				});
			}
			None => {
				let args =
					crate::exec::vm::host::out_of_line(|| self.call_args(&call.args, args_frame))
						.await?;
				self.emit(Instr::FunctionCall {
					function,
					frame,
					args,
					dst,
					outer,
					elem: self.element,
				});
			}
		}
		self.delegate_context = self.delegate_context.max(ContextLevel::Database);
		self.uses_eval = true;
		self.may_fail = true;
		Ok(Operand::Reg(dst))
	}

	/// Whether the body unit `unit` of this lowering's arena runs — the
	/// body a bundle holds, for a unit that holds one — is a light body
	/// ([`light_body`]): a small leaf that reads no parameter, its arguments
	/// being in its slots.
	fn light(&self, unit: UnitId) -> bool {
		let Some(held) = self.units.get(unit.0 as usize) else {
			return false;
		};
		let light = |body: &Program| body.small_leaf() && body.params.is_empty();
		match held.unit.as_ref().and_then(|head| head.entry) {
			Some(entry) => held.units.get(entry.0 as usize).is_some_and(|body| light(body)),
			None => light(held),
		}
	}

	/// What a call of `name` needs of the function, lowered.
	async fn callee(&mut self, name: &str, unchecked: bool) -> Lowered<Callee> {
		if self.bundle {
			crate::exec::vm::host::out_of_line(|| self.callee_in_arena(name, unchecked)).await
		} else {
			crate::exec::vm::host::out_of_line(|| self.callee_at_root(name, unchecked)).await
		}
	}

	/// A call inside a bundle: its bodies are units of the bundle's arena.
	async fn callee_in_arena(&mut self, name: &str, unchecked: bool) -> Lowered<Callee> {
		let planner = self.planner;
		let snapshot = match self.function_lookup(name)? {
			FunctionLookup::Found(snapshot) => snapshot,
			FunctionLookup::Absent => return Ok(Callee::absent()),
			FunctionLookup::Unavailable => {
				return Err(NotCovered::new(format!("fn::{name} could not be read")));
			}
		};
		let definition = snapshot.definition();
		let hash = DefHash(snapshot.content_hash());
		let guard = guard_of(definition)?;
		let Some(principal) = planner.function_principal(definition) else {
			return Err(NotCovered::internal(format!(
				"fn::{name} has an AUTH LIMIT that does not parse"
			)));
		};
		let checked = planner.body_is_checked(principal.as_ref());
		let wants_unchecked = unchecked && checked;
		let frame = UnitFrame {
			class_digest: planner.class_digest(principal.as_deref()),
			ns: planner.ns.clone(),
			db: planner.db.clone(),
			perms_suppressed: planner.perms_suppressed,
			checked: true,
		};
		let linked =
			self.fn_links.iter().find(|l| l.name == name && l.frame == frame).map(|l| l.unit);
		let (inline, body) = match linked {
			Some(unit) => (None, Some(unit)),
			None => {
				let Some(prepared) = prepared_body(planner, name, &snapshot).await else {
					return Err(NotCovered::internal(format!("fn::{name} did not bind")));
				};
				match inline_of(&prepared, planner).filter(|_| !wants_unchecked) {
					Some(inline) => (Some(inline), None),
					None if too_deep(planner) => (None, None),
					None => {
						let frame = body_frame(&prepared.planner, false);
						let mut arena = self.take_arena();
						let linked =
							Box::pin(link_function(&mut arena, name, &prepared, frame)).await;
						self.put_arena(arena);
						(None, Some(linked?))
					}
				}
			}
		};
		let unchecked_body = if wants_unchecked && !too_deep(planner) {
			let unchecked_planner = planner.waiving_permissions();
			let frame = UnitFrame {
				class_digest: planner.class_digest(principal.as_deref()),
				ns: planner.ns.clone(),
				db: planner.db.clone(),
				perms_suppressed: true,
				checked: false,
			};
			let linked =
				self.fn_links.iter().find(|l| l.name == name && l.frame == frame).map(|l| l.unit);
			match linked {
				Some(unit) => Some(unit),
				None => {
					let Some(prepared) = prepared_body(&unchecked_planner, name, &snapshot).await
					else {
						return Err(NotCovered::internal(format!("fn::{name} did not bind")));
					};
					let frame = body_frame(&prepared.planner, true);
					let mut arena = self.take_arena();
					let linked = Box::pin(link_function(&mut arena, name, &prepared, frame)).await;
					self.put_arena(arena);
					Some(linked?)
				}
			}
		} else {
			None
		};
		Ok(Callee {
			defined: true,
			hash,
			checked,
			inline,
			guard,
			body,
			unchecked_body,
		})
	}

	/// What the plan read of `fn::name`.
	/// What a call of `fn::name` lowered here spends of the computation-depth
	/// budget beyond its own re-entry.
	fn call_depth(&self, name: &str) -> Lowered<crate::exec::vm::program::CallDepth> {
		let clamp = |n: u32| u8::try_from(n).unwrap_or(u8::MAX);
		let site = self.planner.depth_sites().map_or(0, |sites| sites.call_extra(name));
		let (writes, body, deepest) = match self.function_lookup(name)? {
			FunctionLookup::Found(snapshot) => {
				let block = &snapshot.definition().block;
				let depth = crate::exec::vm::depth::ExprDepth::of_body(block);
				(block.function_facts().direct_writes, depth.charged(), depth.deepest)
			}
			_ => (false, 0, 0),
		};
		Ok(crate::exec::vm::program::CallDepth {
			site: clamp(site),
			writes,
			body: clamp(body),
			deepest: clamp(deepest),
		})
	}

	fn function_lookup(&self, name: &str) -> Lowered<FunctionLookup> {
		match self.planner.txn() {
			Some(txn) => self.catalog.function(txn, name),
			None => Ok(FunctionLookup::Unavailable),
		}
	}

	/// A call at the top of a call path: its bodies are bundles the compile
	/// cache keeps ([`super::super::compile_cache`]), which the program holds
	/// through a unit of its own arena.
	///
	/// The unit is compiled at the depth of a statement, binding only the
	/// function's own arguments ([`Planner::unit_root`]), and served to every
	/// call of the frame `self` links it into: the class of the caller's
	/// principal and the permission contract. What the cache reads to decide
	/// whether a kept unit stands, and what a unit compiled here is kept
	/// against, is the cache's, not the lowering's, and is read unsealed
	/// ([`crate::kvs::catalog_reads::sealed`]); the compilation itself is
	/// sealed as any lowering.
	async fn callee_at_root(&mut self, name: &str, unchecked: bool) -> Lowered<Callee> {
		use crate::kvs::catalog_reads::sealed;
		let planner = self.planner;
		let catalog = Arc::clone(&self.catalog);
		let compile = |record: Option<Arc<crate::kvs::Transaction>>| {
			let catalog = Arc::clone(&catalog);
			async move {
				let root = planner.unit_root();
				let mut unit = Lowering::new(&root);
				unit.catalog = catalog;
				let recording = record.as_ref().map(|txn| txn.record_catalog_reads());
				let lowered = sealed(true, Box::pin(unit.function_unit(name, unchecked))).await;
				let recorded = recording.map(|recording| recording.finish());
				Ok::<_, NotCovered>((lowered?, recorded))
			}
		};
		let cached = planner.unit_cache();
		let key = match &cached {
			Some(_) => sealed(false, planner.function_unit_key(name)).await,
			None => None,
		};
		let function = match (cached, key) {
			(Some((units, txn)), Some(key)) => {
				let frame = self.key_frame(unchecked);
				let compiled = std::sync::atomic::AtomicBool::new(false);
				let served = sealed(
					false,
					units.functions().get_or_compile(
						&key,
						&frame,
						&txn,
						planner.frozen(),
						|| async {
							compiled.store(true, std::sync::atomic::Ordering::Relaxed);
							let (function, recorded) = compile(Some(Arc::clone(&txn))).await?;
							let deps = match recorded {
								Some(recorded)
									if !planner
										.unit_root()
										.folds_context(name, &txn, key.ids)
										.await =>
								{
									crate::exec::catalog_deps::CatalogDeps::resolve(&txn, &recorded)
										.await
										.map(|deps| {
											(deps, super::super::units::ContextDeps::default())
										})
								}
								_ => None,
							};
							Ok::<_, NotCovered>((function, deps))
						},
					),
				)
				.await?;
				#[cfg(test)]
				super::super::units::BODIES.with(|bodies| {
					let (c, l) = bodies.get();
					bodies.set(if compiled.load(std::sync::atomic::Ordering::Relaxed) {
						(c + 1, l)
					} else {
						(c, l + 1)
					});
				});
				served
			}
			_ => compile(None).await?.0,
		};
		match function {
			Some(function) => self.callee_of(&function),
			None => Ok(Callee::absent()),
		}
	}

	/// The function `name` as a call lowers it, for the node to keep; `None`
	/// for a function the call cannot enter.
	async fn function_unit(&self, name: &str, unchecked: bool) -> Lowered<Option<FunctionUnit>> {
		let planner = self.planner;
		if planner.on_function_path(name) {
			return Err(NotCovered::internal(format!("fn::{name} calls itself outside its unit")));
		}
		let snapshot = match self.function_lookup(name)? {
			FunctionLookup::Found(snapshot) => snapshot,
			FunctionLookup::Absent => return Ok(None),
			FunctionLookup::Unavailable => {
				return Err(NotCovered::new(format!("fn::{name} could not be read")));
			}
		};
		let definition = snapshot.definition();
		let Some(prepared) = prepared_body(planner, name, &snapshot).await else {
			return Err(NotCovered::internal(format!("fn::{name} did not bind")));
		};
		let checked = prepared.frame.checked;
		let wants_unchecked = unchecked && checked;
		let inline = inline_of(&prepared, planner).filter(|_| !wants_unchecked);
		let body = if inline.is_none() && !too_deep(planner) {
			Some(Box::pin(bundle_of(name, &prepared, false)).await?)
		} else {
			None
		};
		let unchecked_body = if wants_unchecked && !too_deep(planner) {
			let unchecked_planner = planner.waiving_permissions();
			let Some(prepared) = prepared_body(&unchecked_planner, name, &snapshot).await else {
				return Err(NotCovered::internal(format!("fn::{name} did not bind")));
			};
			Some(Box::pin(bundle_of(name, &prepared, true)).await?)
		} else {
			None
		};
		let reach = [&body, &unchecked_body]
			.into_iter()
			.flatten()
			.flat_map(|bundle| bundle.units.iter())
			.fold(super::super::dispatch::Reach::none(), |reach, unit| {
				reach.union(super::super::dispatch::reach_of(unit))
			});
		let mut footprint = super::super::footprint::Footprint::of(
			&prepared.bound,
			planner.function_registry(),
			reach,
		);
		if prepared.specialised {
			// A body compiled for each call's arguments reads and writes what
			// its compilation for them names.
			footprint.reads = super::super::dispatch::Reach::Any;
			footprint.writes = super::super::dispatch::Reach::Any;
		}
		footprint.calls.remove(name);
		Ok(Some(FunctionUnit {
			hash: DefHash(snapshot.content_hash()),
			checked,
			inline,
			guard: guard_of(definition)?,
			body,
			unchecked_body,
			footprint: Arc::new(footprint),
		}))
	}

	/// `function`, kept by the node or just lowered, linked into this
	/// program: each bundle through a unit that owns its arena.
	fn callee_of(&mut self, function: &FunctionUnit) -> Lowered<Callee> {
		let body = function.body.as_ref().map(|bundle| self.wrapper(bundle)).transpose()?;
		let unchecked_body =
			function.unchecked_body.as_ref().map(|bundle| self.wrapper(bundle)).transpose()?;
		Ok(Callee {
			defined: true,
			hash: function.hash,
			checked: function.checked,
			inline: function.inline.clone(),
			guard: function.guard.clone(),
			body,
			unchecked_body,
		})
	}

	/// A unit of this program's arena that holds `bundle`'s.
	fn wrapper(&mut self, bundle: &Bundle) -> Lowered<UnitId> {
		let mut holder = Lowering::new(self.planner);
		holder.catalog = Arc::clone(&self.catalog);
		holder.units = bundle.units.to_vec();
		let mut program = holder.finish()?;
		program.unit = Some(bundle.head.clone());
		let id = u32::try_from(self.units.len())
			.map_err(|_| NotCovered::internal("more units than a program holds"))?;
		self.units.push(Arc::new(program));
		Ok(UnitId(id))
	}

	/// A parameter's `PERMISSIONS` predicate `guard` as a program of its own
	/// whose one host pipeline evaluates it, lowered with `planner`: the
	/// parameter's unit ([`super::super::param_unit`]).
	pub(super) async fn param_guard_unit(
		planner: &Planner<'_>,
		guard: &LogicalExpr,
		catalog: Arc<super::super::plan_catalog::PlanCatalog>,
	) -> Lowered<Program> {
		let mut l = Lowering::new(planner);
		l.catalog = catalog;
		let pipeline =
			crate::exec::vm::host::out_of_line(|| l.guard_pipeline_in(planner, guard)).await?;
		if pipeline != 0 {
			return Err(NotCovered::internal("a parameter's predicate is not its unit's pipeline"));
		}
		l.finish()
	}

	/// `guard`, a definition's `PERMISSIONS` predicate — a function's, a
	/// `DEFINE PARAM`'s, a model's or a module's — as a pipeline of
	/// [`Program::host_code`] lowered with `planner` over an isolated view of
	/// its context ([`Planner::privileged`]), which the host evaluates once,
	/// over the current row, in a frame that does not check permissions. The
	/// guard is definer code, which
	/// the host judges in a privileged frame
	/// ([`crate::exec::operators::permission_frame`]), so it is lowered as one
	/// whoever calls it: no plan-time step reads a caller's binding into it,
	/// and a statement it holds is planned against the session's own values
	/// and the database's parameters alone.
	pub(super) async fn guard_pipeline_in(
		&mut self,
		planner: &Planner<'ctx>,
		guard: &LogicalExpr,
	) -> Lowered<u16> {
		let planner = planner.privileged();
		let mut child = Lowering::new(&planner);
		child.catalog = Arc::clone(&self.catalog);
		lend(self, &mut child);
		child.ahead = self.ahead;
		child.bundle = self.bundle;
		child.versioned = self.versioned;
		child.privileged = true;
		child.unchecked_reads = true;
		child.rows_are_objects = false;
		// The pipeline runs over a state of its own, which holds none of the
		// names the block binds.
		child.slot_hidden = 1;
		let built = Box::pin(child.host_pipeline(guard)).await;
		let index = match built {
			Ok(pipeline) => match u16::try_from(child.host_code.len()) {
				Ok(index) => {
					child.host_code.push(pipeline);
					Ok(index)
				}
				Err(_) => Err(NotCovered::internal("more host pipelines than a program holds")),
			},
			Err(e) => Err(e),
		};
		let context = child.delegate_context;
		lend(self, &mut child);
		self.delegate_context = self.delegate_context.max(context);
		index
	}

	/// The [`ParamSpec`] of a read of `name`, as the index of
	/// [`Program::params`].
	pub(super) async fn param_spec(&mut self, name: &crate::expr::Param) -> Lowered<u32> {
		let guard = crate::exec::vm::host::out_of_line(|| self.param_guard(name.as_str())).await?;
		self.params.push(ParamSpec {
			name: name.clone().into_strand(),
			guard,
		});
		Ok((self.params.len() - 1) as u32)
	}

	/// The predicate of the `DEFINE PARAM $name` the transaction holds, when
	/// the clause is one and a read of the name may reach the definition and
	/// check it: the name is not one the frame answers, and the read is not
	/// in a frame that skips permissions.
	async fn param_guard(
		&mut self,
		name: &str,
	) -> Lowered<Option<(DefHash, u16, crate::exec::guard_verdicts::GuardScope)>> {
		if self.reads_unchecked()
			|| matches!(name, "this" | "self" | "parent")
			|| surrealdb_cnf::PROTECTED_PARAM_NAMES.contains(&name)
			|| (!self.planner.param_is_shadowed(name) && self.caller_value(name).is_some())
		{
			return Ok(None);
		}
		let Some(txn) = self.planner.txn() else {
			return Ok(None);
		};
		let Some(definition) = self.catalog.param(txn, name)?.cloned() else {
			return Ok(None);
		};
		let permission = definition.permissions_body.logical_ref().map_err(|e| {
			NotCovered::internal(format!("a parameter permission with no logical form: {e}"))
		})?;
		let LogicalPermissionRef::Specific(guard) = permission else {
			return Ok(None);
		};
		let guard_planner =
			self.planner.shadowing(std::iter::empty()).with_depth(self.planner.current_depth() + 1);
		let pipeline =
			crate::exec::vm::host::out_of_line(|| self.guard_pipeline_in(&guard_planner, guard))
				.await?;
		let scope =
			crate::exec::guard_verdicts::GuardScope::of(guard, self.planner.function_registry());
		Ok(Some((DefHash::of_param(&definition), pipeline, scope)))
	}
}

/// Whether a body called from `planner` would be lowered one re-entry past
/// the computation-depth budget. Such a call is lowered with no body: the
/// call raises the error the budget names.
fn too_deep(planner: &Planner<'_>) -> bool {
	planner.current_depth() + 1 > planner.frozen().config.exec.max_computation_depth
}

/// The body of the function `snapshot` reads, bound for a call from
/// `planner`, or left as written for each call to bind with its arguments in
/// force where it cannot be bound for every call at once.
async fn prepared_body<'a>(
	planner: &Planner<'a>,
	name: &str,
	snapshot: &Arc<crate::kvs::cache::ds::FunctionSnapshot>,
) -> Option<PreparedBody<'a>> {
	// Binding the body is the planning of a statement of its own, which reads
	// the catalog as any binding does.
	let prepared = crate::kvs::catalog_reads::sealed(
		false,
		Box::pin(planner.prepare_snapshot(name, snapshot)),
	);
	match prepared.await {
		Some(prepared) => Some(prepared),
		None => planner.specialised_snapshot(name, snapshot),
	}
}

/// The expression a call of the function prepared as `prepared` inlines,
/// with the names of the parameters it reads.
fn inline_of(
	prepared: &PreparedBody<'_>,
	planner: &Planner<'_>,
) -> Option<(Arc<LogicalExpr>, Vec<String>)> {
	if prepared.specialised {
		return None;
	}
	let params: Vec<&str> =
		prepared.frame.compiled_from.args.iter().map(|(arg, _)| arg.as_str()).collect();
	let expr =
		super::super::inline::inlinable(&prepared.bound, &params, planner.function_registry())?;
	Some((Arc::new(expr.clone()), params.into_iter().map(str::to_owned).collect()))
}

/// The body prepared as `prepared`, and every body it calls, lowered into an
/// arena of their own.
async fn bundle_of(
	name: &str,
	prepared: &PreparedBody<'_>,
	unchecked_variant: bool,
) -> Lowered<Bundle> {
	let mut arena = Arena::default();
	let frame = body_frame(&prepared.planner, unchecked_variant);
	let entry = Box::pin(link_function(&mut arena, name, prepared, frame.clone())).await?;
	debug_assert_eq!(entry, UnitId(0));
	Ok(Bundle {
		units: arena.units.into(),
		head: UnitHead {
			sig: body_sig(&prepared.frame.compiled_from),
			frame,
			entry: Some(entry),
			closure: None,
		},
	})
}
