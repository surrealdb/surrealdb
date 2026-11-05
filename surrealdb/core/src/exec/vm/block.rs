//! The host's side of the block instructions: running a block's statements
//! in the frame it has reached, binding parameters, iterating `FOR` ranges,
//! and the signals that leave a block.
//!
//! A block is compiled in full when the query arrives: every statement it
//! runs is a group of its program ([`Program::groups`]), lowered against the
//! catalog the statement meets when it runs. Running a block plans, binds and
//! lowers nothing: a statement is a run of its group's pipelines over the
//! program's state.

use anyhow::{Context as _, anyhow};

use super::ledger::Site;
use super::program::{BlockInstr, Collect, Program};
use super::scalar::{Held, Scalar};
use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
use crate::exec::context::ContextLevel;
use crate::exec::{Error as ExecError, ExecutionContext, FlowResult};
use crate::expr::ControlFlow;
use crate::val::Value;

/// A `FOR` loop's iterator: an array's elements or a range's integers, taken
/// one at a time as the loop reaches them.
pub(super) enum ForIter {
	Array(std::vec::IntoIter<Value>),
	Range(common::range::IntegerRangeIter),
}

fn read(regs: &[Scalar<'_>], reg: u16) -> Value {
	regs[reg as usize].to_value()
}

fn write(regs: &mut [Scalar<'_>], reg: u16, value: Value) {
	regs[reg as usize] = Scalar::Value(Held::Owned(value));
}

/// Take block instruction `instr`, answering the instruction the program
/// jumps to, or `None` to continue with the next.
pub(super) async fn step(
	program: &Program,
	instr: &BlockInstr,
	regs: &mut [Scalar<'_>],
	state: &mut super::interp::State,
	ctx: &ExecutionContext,
) -> FlowResult<Option<usize>> {
	match instr {
		BlockInstr::RunEmbedded {
			statement,
			dst,
			collect,
			on_break,
			on_continue,
			resolve,
		} => {
			let frame = state.frame.clone().unwrap_or_else(|| ctx.clone());
			if frame.cancellation().is_cancelled() {
				return Err(ControlFlow::Err(anyhow!(crate::err::EngineError::QueryCancelled)));
			}
			let frame = match resolve {
				Some(required) => resolve_names(&frame, *required).await?,
				None => frame,
			};
			// The group's pipelines are this program's, run over this state.
			let running = state
				.program
				.clone()
				.ok_or_else(|| ControlFlow::Err(anyhow!("a block statement with no program")))?;
			let scalar = running.groups[*statement as usize].scalar;
			state.hand_on = *collect == Collect::Let;
			let (rows, ended) = super::host::run_group(&running, *statement, state, &frame).await;
			match collected(rows, ended, scalar, *collect) {
				Ok(value) => write(regs, *dst, value),
				Err(ControlFlow::Break) if on_break.is_some() => {
					return Ok(on_break.map(|at| at as usize));
				}
				Err(ControlFlow::Continue) if on_continue.is_some() => {
					return Ok(on_continue.map(|at| at as usize));
				}
				Err(signal) => return Err(signal),
			}
		}
		BlockInstr::RunServed {
			served,
			dst,
			collect,
			on_break,
			on_continue,
			resolve,
		} => {
			let frame = state.frame.clone().unwrap_or_else(|| ctx.clone());
			if frame.cancellation().is_cancelled() {
				return Err(ControlFlow::Err(anyhow!(crate::err::EngineError::QueryCancelled)));
			}
			let frame = match resolve {
				Some(required) => resolve_names(&frame, *required).await?,
				None => frame,
			};
			let held =
				|| ControlFlow::Err(anyhow!("a late-bound statement the program does not hold"));
			let spec = program.served.get(*served as usize).ok_or_else(held)?;
			let op = frame
				.vm_served()
				.and_then(|ops| ops.get(*served as usize))
				.cloned()
				.ok_or_else(held)?;
			let (rows, ended, scalar) = super::serve::run_served(&frame, &op, spec).await;
			match collected(rows, ended, scalar, *collect) {
				Ok(value) => write(regs, *dst, value),
				Err(ControlFlow::Break) if on_break.is_some() => {
					return Ok(on_break.map(|at| at as usize));
				}
				Err(ControlFlow::Continue) if on_continue.is_some() => {
					return Ok(on_continue.map(|at| at as usize));
				}
				Err(signal) => return Err(signal),
			}
		}
		BlockInstr::RunAnalyzed {
			statement,
			analysis,
			dst,
			collect,
			resolve,
		} => {
			let frame = state.frame.clone().unwrap_or_else(|| ctx.clone());
			if frame.cancellation().is_cancelled() {
				return Err(ControlFlow::Err(anyhow!(crate::err::EngineError::QueryCancelled)));
			}
			let frame = match resolve {
				Some(required) => resolve_names(&frame, *required).await?,
				None => frame,
			};
			let running = state
				.program
				.clone()
				.ok_or_else(|| ControlFlow::Err(anyhow!("a block statement with no program")))?;
			let analysis = &program.analyses[*analysis as usize];
			let rendered = super::explain::analysed_run(analysis, &frame, |frame| async move {
				let (rows, ended) =
					super::host::run_group(&running, *statement, state, &frame).await;
				(rows.len() as u64, ended)
			})
			.await?;
			write(regs, *dst, value_of(vec![rendered], true, *collect));
		}
		// The host runs a nested statement over the row it is evaluated for.
		BlockInstr::Nested {
			..
		}
		| BlockInstr::Member {
			..
		} => {
			return Err(ControlFlow::Err(anyhow!("a nested statement reached as a block step")));
		}
		BlockInstr::Bind {
			name,
			value,
			slot,
			frame: in_frame,
		} => {
			// The register is the binding's own: lowering loads the value into
			// a register no later instruction reads, so the value is moved
			// into the slot rather than copied. A value the register shares
			// with a binding stays shared; one borrowed from the row or the
			// program is copied, since a slot outlives both.
			let held = match std::mem::replace(&mut regs[*value as usize], Scalar::None) {
				Scalar::Value(Held::Shared(shared)) => Held::Shared(shared),
				Scalar::Value(Held::Owned(value)) => Held::Owned(value),
				other @ (Scalar::None
				| Scalar::Null
				| Scalar::Bool(_)
				| Scalar::Number(_)
				| Scalar::Str(_)
				| Scalar::Duration(_)
				| Scalar::Datetime(_)
				| Scalar::Uuid(_)
				| Scalar::Bytes(_)
				| Scalar::Value(Held::Borrowed(_))) => Held::Owned(other.into_value()),
			};
			let slot = *slot as usize;
			if state.slots.len() <= slot {
				state.slots.resize(slot + 1, Held::Owned(Value::None));
			}
			let kept = crate::exec::memory::size::kept_bytes(&held);
			if *in_frame {
				let frame = state.frame.take().unwrap_or_else(|| ctx.clone());
				let name = program.fields[*name as usize].as_str().to_owned();
				let value = Value::clone(&held);
				state.slots[slot] = held;
				state.frame = Some(frame.with_param(name, value));
			} else {
				state.slots[slot] = held;
			}
			if kept != 0 || state.values.is_some() || state.handed.is_some() {
				state.keep(Site::Slot(slot), kept, ctx).await?;
				// The frame's copy of a parameter weighs what the value does, and
				// stays held with the copies of the earlier bindings of the name
				// until the block ends.
				if *in_frame {
					state.keep_more(Site::Frame(slot), kept, ctx).await?;
				}
			}
		}
		BlockInstr::CoerceKind {
			value,
			kind,
			name,
		} => {
			let kind = &program.kinds[*kind as usize];
			let set_coerce = |e| {
				ControlFlow::Err(anyhow::Error::new(crate::exec::Error::SetCoerce {
					name: program.fields[*name as usize].as_str().to_owned(),
					error: Box::new(e),
				}))
			};
			let coerced = read(regs, *value).coerce_to_kind(kind).map_err(set_coerce)?;
			let coerced = super::schema_host::check_types_held(
				state.frame.as_ref().unwrap_or(ctx),
				kind,
				coerced,
			)
			.await
			.map_err(set_coerce)?;
			write(regs, *value, coerced);
		}
		BlockInstr::CheckDepth {
			value,
		} => crate::val::check_value_depth(&read(regs, *value))?,
		// The host counted the block as a nested statement before it took the step.
		BlockInstr::Enter => {}
		BlockInstr::Save => {
			state.saved.push(state.frame.clone());
			state.save_frames(state.saved.len());
		}
		BlockInstr::Clock => {
			let frame = state.frame.take().unwrap_or_else(|| ctx.clone());
			state.frame = Some(frame.with_statement_clock(crate::val::Datetime::now()));
		}
		BlockInstr::Resolve => {
			let current = state.frame.clone().unwrap_or_else(|| ctx.clone());
			state.frame = Some(resolve_names(&current, ContextLevel::Root).await?);
			for saved in &mut state.saved {
				let frame = saved.clone().unwrap_or_else(|| ctx.clone());
				*saved = Some(resolve_names(&frame, ContextLevel::Root).await?);
			}
		}
		BlockInstr::Restore => {
			let depth = state.saved.len();
			state.frame = state
				.saved
				.pop()
				.ok_or_else(|| ControlFlow::Err(anyhow!("a frame restored that was not saved")))?;
			state.restore_frames(depth, ctx).await?;
		}
		BlockInstr::Return {
			value,
		} => return Err(ControlFlow::Return(read(regs, *value))),
		BlockInstr::IterStart {
			src,
			slot,
		} => {
			let source = read(regs, *src);
			// The iterator holds the array it walks until the loop ends.
			let kept = if let Value::Array(_) = &source {
				crate::exec::memory::size::kept_bytes(&source)
			} else {
				0
			};
			let iter = match source {
				Value::Array(array) => ForIter::Array(array.0.into_iter()),
				Value::Range(range) => {
					let range = range
						.coerce_to_typed::<i64>()
						.map_err(crate::expr::Error::from)
						.context("Invalid FOR range")
						.map_err(ControlFlow::Err)?;
					ForIter::Range(range.iter())
				}
				value @ (Value::None
				| Value::Null
				| Value::Bool(_)
				| Value::Number(_)
				| Value::String(_)
				| Value::Duration(_)
				| Value::Datetime(_)
				| Value::Date(_)
				| Value::Time(_)
				| Value::Timestamp(_)
				| Value::Interval(_)
				| Value::Uuid(_)
				| Value::Set(_)
				| Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::Table(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Regex(_)
				| Value::Closure(_)) => {
					return Err(ControlFlow::Err(anyhow::Error::new(
						crate::exec::Error::InvalidStatementTarget {
							value: value.to_raw_string(),
						},
					)));
				}
			};
			// What the loop keeps across its iterations is read afresh each
			// time it starts.
			for (at, nested) in program.nested.iter().enumerate() {
				if nested.hoist == Some(*slot) {
					state.reset_once(at);
				}
			}
			let slot = *slot as usize;
			if state.iters.len() <= slot {
				state.iters.resize_with(slot + 1, || None);
			}
			state.iters[slot] = Some(iter);
			if kept != 0 || state.values.is_some() {
				state.keep(Site::Iter(slot), kept, ctx).await?;
			}
		}
		BlockInstr::IterNext {
			slot,
			dst,
			done,
		} => {
			let next = match state.iters.get_mut(*slot as usize).and_then(Option::as_mut) {
				Some(ForIter::Array(values)) => values.next(),
				Some(ForIter::Range(values)) => values.next().map(Value::from),
				None => return Err(ControlFlow::Err(anyhow!("a FOR iterator never started"))),
			};
			match next {
				Some(value) => write(regs, *dst, value),
				None => {
					state.iters[*slot as usize] = None;
					state.release(Site::Iter(*slot as usize));
					return Ok(Some(*done as usize));
				}
			}
		}
		BlockInstr::Sleep {
			duration,
		} => {
			let frame = state.frame.as_ref().unwrap_or(ctx);
			// A schema clause must not block.
			if let Some(refusing) = frame.root().no_write
				&& refusing.clause().is_some()
			{
				return Err(ControlFlow::Err(anyhow!(
					refusing.side_effect_error(crate::exec::clause_effects::SLEEP_TEXT)
				)));
			}
			frame
				.is_allowed(
					crate::iam::Action::Edit,
					crate::iam::ResourceKind::Table,
					crate::expr::Base::Root,
				)
				.map_err(ControlFlow::Err)?;
			let Value::Duration(duration) = read(regs, *duration) else {
				return Err(ControlFlow::Err(anyhow!("SLEEP for a value that is not a duration")));
			};
			let sleep = async {
				tokio::select! {
					_ = common::time::sleep(duration.0) => {},
					_ = frame.cancellation().cancelled() => {},
				}
			};
			// A sleep the context's deadline cuts short fails with the
			// deadline's error, as the SLEEP operator's does.
			match frame.ctx().timeout() {
				Some(remaining) => common::time::timeout(remaining, sleep)
					.await
					.map_err(|elapsed| ControlFlow::Err(anyhow::Error::new(elapsed)))?,
				None => sleep.await,
			}
		}
	}
	Ok(None)
}

/// The value of a block statement from the rows its group produced and how
/// it ended, as the operator it stands in for gathers it: a `RETURN` raised
/// inside is the last value of a `RETURN` and the value of a `LET`; any other
/// signal leaves.
fn collected(
	mut values: Vec<Value>,
	ended: FlowResult<()>,
	scalar: bool,
	collect: Collect,
) -> FlowResult<Value> {
	match ended {
		Ok(()) => {}
		Err(ControlFlow::Return(value)) if collect == Collect::Return => values.push(value),
		Err(ControlFlow::Return(value)) if collect == Collect::Let => return Ok(value),
		Err(signal) => return Err(signal),
	}
	Ok(value_of(values, scalar, collect))
}

/// A statement's value from its rows, as `collect` says.
fn value_of(mut values: Vec<Value>, scalar: bool, collect: Collect) -> Value {
	match collect {
		Collect::Discard => Value::None,
		Collect::Block if scalar => values.pop().unwrap_or(Value::None),
		Collect::Let if scalar => values.into_iter().next().unwrap_or(Value::None),
		Collect::Return if scalar && values.len() == 1 => values.pop().expect("one value"),
		Collect::Return if scalar && values.is_empty() => Value::None,
		Collect::Block | Collect::Return | Collect::Let => Value::Array(crate::val::Array(values)),
	}
}

/// `frame` with the session's namespace and database resolved again, as far
/// as `required` asks and the catalog allows: a statement before the one
/// being run may have created, removed or replaced either, and the
/// definitions the frame holds are those the block started with. The
/// catalog is read through the transaction's cache, a seam of the run-time
/// seal ([`crate::exec::compiles::seam`]).
///
/// Above `required` the resolution is opportunistic, as the executor's is: a
/// session naming a namespace or database that is not there leaves the frame
/// at the level below it.
async fn resolve_names(
	frame: &ExecutionContext,
	required: ContextLevel,
) -> FlowResult<ExecutionContext> {
	crate::exec::compiles::seam(resolve_names_unsealed(frame, required)).await
}

async fn resolve_names_unsealed(
	frame: &ExecutionContext,
	required: ContextLevel,
) -> FlowResult<ExecutionContext> {
	let txn = frame.txn();
	let (ns, db) = match frame.options() {
		Some(options) => {
			(options.ns().ok().map(str::to_owned), options.db().ok().map(str::to_owned))
		}
		None => match frame.database() {
			Ok(db) => (Some(db.ns_name().to_owned()), Some(db.db_name().to_owned())),
			Err(_) => (None, None),
		},
	};
	let ns_def = if required >= ContextLevel::Namespace {
		let ns = ns.as_deref().ok_or_else(|| failed(ExecError::NsEmpty))?;
		Some(txn.expect_ns_by_name(ns).await.map_err(failed)?)
	} else if let Some(ns) = ns.as_deref() {
		txn.expect_ns_by_name(ns).await.ok()
	} else {
		None
	};
	let db_def = match (&ns_def, ns.as_deref()) {
		(Some(_), Some(ns)) if required >= ContextLevel::Database => {
			let db = db.as_deref().ok_or_else(|| failed(ExecError::DbEmpty))?;
			Some(txn.expect_db_by_name(ns, db).await.map_err(failed)?)
		}
		(Some(_), Some(ns)) => match db.as_deref() {
			Some(db) => txn.expect_db_by_name(ns, db).await.ok(),
			None => None,
		},
		_ => None,
	};
	Ok(match (ns_def, db_def) {
		(Some(ns), Some(db)) => frame.with_database(ns, db),
		(Some(ns), None) => frame.with_namespace(ns),
		(None, _) => ExecutionContext::Root(frame.root().clone()),
	})
}

fn failed(error: impl Into<anyhow::Error>) -> ControlFlow {
	ControlFlow::Err(error.into())
}
