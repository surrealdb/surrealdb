//! The host's side of the block instructions: running a block's statements
//! in the frame it has reached, binding parameters, iterating `FOR` ranges,
//! and the signals that leave a block.
//!
//! A block is compiled in full when the query arrives: every statement it
//! runs is a group of its program ([`Program::groups`]), lowered against the
//! catalog the statement meets when it runs. Running a block plans, binds and
//! lowers nothing: a statement is a run of its group's pipelines over the
//! program's state.

use std::borrow::Cow;

use anyhow::{Context as _, anyhow};

use super::program::{BlockInstr, Collect, Program};
use super::scalar::Scalar;
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
	regs[reg as usize] = Scalar::Value(Cow::Owned(value));
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
			on_return,
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
			let (rows, ended) = super::host::run_group(&running, *statement, state, &frame).await;
			match collected(rows, ended, scalar, *collect) {
				Ok(value) => write(regs, *dst, value),
				Err(ControlFlow::Break) if on_break.is_some() => {
					return Ok(on_break.map(|at| at as usize));
				}
				Err(ControlFlow::Continue) if on_continue.is_some() => {
					return Ok(on_continue.map(|at| at as usize));
				}
				Err(ControlFlow::Return(value)) if on_return.is_some() => {
					let catch = on_return.expect("checked above");
					write(regs, catch.value, value);
					return Ok(Some(catch.label as usize));
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
			on_return,
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
				Err(ControlFlow::Return(value)) if on_return.is_some() => {
					let catch = on_return.expect("checked above");
					write(regs, catch.value, value);
					return Ok(Some(catch.label as usize));
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
			let started = web_time::Instant::now();
			let (rows, ended) = super::host::run_group(&running, *statement, state, &frame).await;
			let elapsed = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
			// A signal ends the statement analysed, as it ends the drain of
			// the plan `EXPLAIN ANALYZE` measures; an error is raised.
			match ended {
				Ok(())
				| Err(ControlFlow::Break | ControlFlow::Continue | ControlFlow::Return(_)) => {}
				Err(error @ ControlFlow::Err(_)) => return Err(error),
			}
			let measured = crate::exec::OperatorMetrics::new();
			let total = rows.len() as u64;
			match total {
				0 => measured.record_elapsed(elapsed),
				rows => measured.record_batch(rows, elapsed),
			}
			let analysis = &program.analyses[*analysis as usize];
			let rendered = match &analysis.json {
				Some(template) => match &**template {
					Value::Object(template) => crate::exec::operators::render_analyze_json(
						template,
						&measured,
						analysis.redact,
						total,
					),
					_ => {
						return Err(ControlFlow::Err(anyhow!(
							"an analysed plan that is no object"
						)));
					}
				},
				None => crate::exec::operators::render_analyze_text(
					&analysis.pieces,
					&measured,
					analysis.redact,
					total,
				),
			};
			write(regs, *dst, value_of(vec![rendered], true, *collect));
		}
		// The host runs a nested statement over the row it is evaluated for.
		BlockInstr::Nested {
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
			let value = read(regs, *value);
			let slot = *slot as usize;
			if state.slots.len() <= slot {
				state.slots.resize(slot + 1, Value::None);
			}
			if *in_frame {
				let frame = state.frame.take().unwrap_or_else(|| ctx.clone());
				let name = program.fields[*name as usize].as_str().to_owned();
				state.slots[slot] = value.clone();
				state.frame = Some(frame.with_param(name, value));
			} else {
				state.slots[slot] = value;
			}
		}
		BlockInstr::CoerceKind {
			value,
			kind,
			name,
		} => {
			let coerced =
				read(regs, *value).coerce_to_kind(&program.kinds[*kind as usize]).map_err(|e| {
					ControlFlow::Err(anyhow::Error::new(crate::exec::Error::SetCoerce {
						name: program.fields[*name as usize].as_str().to_owned(),
						error: Box::new(e),
					}))
				})?;
			write(regs, *value, coerced);
		}
		BlockInstr::CheckDepth {
			value,
		} => crate::val::check_value_depth(&read(regs, *value))?,
		BlockInstr::Save => state.saved.push(state.frame.clone()),
		BlockInstr::Resolve => {
			let current = state.frame.clone().unwrap_or_else(|| ctx.clone());
			state.frame = Some(resolve_names(&current, ContextLevel::Root).await?);
			for saved in &mut state.saved {
				let frame = saved.clone().unwrap_or_else(|| ctx.clone());
				*saved = Some(resolve_names(&frame, ContextLevel::Root).await?);
			}
		}
		BlockInstr::Restore => {
			state.frame = state
				.saved
				.pop()
				.ok_or_else(|| ControlFlow::Err(anyhow!("a frame restored that was not saved")))?;
		}
		BlockInstr::Return {
			value,
		} => return Err(ControlFlow::Return(read(regs, *value))),
		BlockInstr::IterStart {
			src,
			slot,
		} => {
			let iter = match read(regs, *src) {
				Value::Array(array) => ForIter::Array(array.0.into_iter()),
				Value::Range(range) => {
					let range = range
						.coerce_to_typed::<i64>()
						.map_err(crate::expr::Error::from)
						.context("Invalid FOR range")
						.map_err(ControlFlow::Err)?;
					ForIter::Range(range.iter())
				}
				value => {
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
					state.once[at] = None;
				}
			}
			let slot = *slot as usize;
			if state.iters.len() <= slot {
				state.iters.resize_with(slot + 1, || None);
			}
			state.iters[slot] = Some(iter);
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
					return Ok(Some(*done as usize));
				}
			}
		}
		BlockInstr::Sleep {
			duration,
		} => {
			let frame = state.frame.as_ref().unwrap_or(ctx);
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
		_ => Value::Array(crate::val::Array(values)),
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
