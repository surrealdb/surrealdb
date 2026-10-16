//! The restricted host constant folding runs programs through.
//!
//! A folded program runs with no storage, no session, no clock and no network: every service a
//! program can reach answers that it is unavailable, so a program that needs one is not
//! foldable and keeps its run-time form. Folding is bounded by the fuel and byte limits of the
//! IR's fold bounds, and a program that exceeds either is left unfolded.
//!
//! The host serves the calls of synchronous, pure, deterministic builtins and nothing else. It
//! runs a program of one pipeline for one row with no fields, under a fuel budget of its own,
//! and restores the thread's meter when the run ends by any path. The dependency runs from this
//! module to [`crate::exec::ir::fold::evaluate`] only: the folder reaches the host through the
//! [`FoldRunner`] it implements.

#![deny(clippy::wildcard_enum_match_arm)]

use std::borrow::Cow;
use std::future::Future;
use std::pin::Pin;

use super::fuel;
use super::interp::{self, State, Step};
use super::ir_lower::IrExprLowering;
use super::program::{Instr, Pipeline, Program};
use super::row::{FieldName, RowReader};
use super::scalar::Scalar;
use crate::err::EngineError;
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::FoldedError;
use crate::exec::ir::fold::evaluate::{CapabilityPolicy, FoldRun, FoldRunner};
use crate::exec::ir::iface::bounds::FOLD_FUEL;
use crate::exec::ir::iface::fold::NotFoldable;
use crate::exec::ir::iface::ids::ValueId;
use crate::kvs::DatastoreError;
use crate::val::Value;

/// A lowered constant expression: a program of one pipeline and no groups, whose body computes
/// the value into register `result` for one empty row and ends the row.
pub(crate) struct FoldProgram {
	pub(crate) program: Program,
	pub(crate) result: u16,
}

/// Lowers one expression value of a container into a fold program.
pub(crate) trait ExprLowering: Send + Sync {
	/// The program that computes `root`, with the callees resolved against `registry`, or the
	/// reason it cannot be lowered. Every error means the value is not folded;
	/// [`IrError::NotYet`] means no lowering is available for it.
	fn lower(
		&self,
		ir: &Ir,
		registry: &FunctionRegistry,
		root: ValueId,
	) -> Result<FoldProgram, IrError>;
}

/// A row with no fields: every field is `NONE` and the record is `NONE`.
pub(crate) struct EmptyRow;

impl RowReader for EmptyRow {
	fn field(&self, _name: &FieldName) -> anyhow::Result<Scalar<'_>> {
		Ok(Scalar::None)
	}

	fn record(&self) -> anyhow::Result<Value> {
		Ok(Value::None)
	}
}

/// The definitions a fold may consult when it reads the catalog. The host installs none, so a
/// fold reads no catalog.
pub(crate) trait FoldCatalog: Send + Sync {}

/// The restricted host: a lowering, and no catalog.
pub(crate) struct FoldHost {
	lowering: &'static dyn ExprLowering,
	catalog: Option<&'static dyn FoldCatalog>,
}

/// The host that the request folder runs every fold through.
pub(crate) static HOST_RUNNER: FoldHost = FoldHost {
	lowering: &IrExprLowering,
	catalog: None,
};

impl FoldHost {
	/// Runs `program` for one empty row under a fuel budget of [`FOLD_FUEL`], serving only the
	/// steps a pure builtin of `registry` needs, and answers the value, a value-class raise or the
	/// refusal.
	///
	/// The thread's fuel meter and quantum are the same after the run as before it, whatever the
	/// answer. The run reads no data: it holds no transaction, no datastore and no session.
	pub(crate) fn run_program(
		&self,
		program: &FoldProgram,
		registry: &FunctionRegistry,
		capabilities: &CapabilityPolicy,
	) -> FoldRun {
		let _meter = Meter::enter(u32::try_from(FOLD_FUEL).unwrap_or(u32::MAX));
		run_body(program, registry, capabilities)
	}
}

impl FoldRunner for FoldHost {
	fn run<'a>(
		&'a self,
		ir: &'a Ir,
		registry: &'a FunctionRegistry,
		value: ValueId,
		capabilities: &'a CapabilityPolicy,
	) -> Pin<Box<dyn Future<Output = FoldRun> + Send + 'a>> {
		Box::pin(async move {
			match self.lowering.lower(ir, registry, value) {
				Ok(program) => self.run_program(&program, registry, capabilities),
				Err(IrError::NotYet {
					..
				}) => FoldRun::Refused(NotFoldable::NotYet),
				Err(
					IrError::Stale(_)
					| IrError::Refused(_)
					| IrError::HasUses(_)
					| IrError::TagChanged(_)
					| IrError::Capacity(_)
					| IrError::Verify(_)
					| IrError::Rejected(_)
					| IrError::Unsupported {
						..
					}
					| IrError::NotLoaded {
						..
					}
					| IrError::LateBind {
						..
					}
					| IrError::Invariant {
						..
					},
				) => FoldRun::Refused(NotFoldable::HostRefused("lowering")),
			}
		})
	}
}

/// The thread's fuel meter as it stood when a fold began, put back when the fold ends.
struct Meter {
	left: u32,
	quantum: u32,
}

impl Meter {
	/// Saves the meter and sets it to `budget` units.
	fn enter(budget: u32) -> Self {
		let saved = Self {
			left: fuel::left(),
			quantum: surrealdb_runtime::fuel::quantum(),
		};
		fuel::refill(budget);
		saved
	}
}

impl Drop for Meter {
	fn drop(&mut self) {
		let quantum = self.quantum.max(self.left).max(1);
		fuel::refill(quantum);
		fuel::charge(quantum - self.left);
	}
}

/// Runs the one pipeline of `fold` for an empty row, serving calls, until the row ends.
fn run_body(
	fold: &FoldProgram,
	registry: &FunctionRegistry,
	capabilities: &CapabilityPolicy,
) -> FoldRun {
	let program = &fold.program;
	let [pipeline] = program.pipelines.as_slice() else {
		return refused("lowering");
	};
	let result = usize::from(fold.result);
	if !program.groups.is_empty() || result >= pipeline.registers {
		return refused("lowering");
	}
	let row = EmptyRow;
	let mut regs: Vec<Scalar<'_>> = vec![Scalar::None; pipeline.registers];
	let mut state = State::vacant();
	let mut pc = 0usize;
	loop {
		let step = match interp::run_row(program, pipeline, &row, &mut regs, &mut state, pc) {
			Ok(step) => step,
			Err(error) => return classify(&error),
		};
		match step {
			Step::Next => return FoldRun::Value(regs[result].to_value(), 0),
			Step::CallAllowed {
				call,
				frame: _,
				resume,
			} => {
				let Some(spec) = program.calls.get(call as usize) else {
					return refused("host step");
				};
				if !capabilities.allows_call(&spec.name) {
					return FoldRun::Refused(NotFoldable::CapabilityDenied);
				}
				pc = resume;
			}
			Step::Call {
				at,
			} => match serve_call(program, pipeline, &row, &mut regs, registry, capabilities, at) {
				Ok(resume) => pc = resume,
				Err(run) => return run,
			},
			Step::Yield {
				..
			} => return FoldRun::Refused(NotFoldable::FuelExhausted),
			Step::Stop
			| Step::FetchRow {
				..
			}
			| Step::Fetch {
				..
			}
			| Step::LandRow {
				..
			}
			| Step::BindRow {
				..
			}
			| Step::WriteProjection {
				..
			}
			| Step::Omit {
				..
			}
			| Step::Access {
				..
			}
			| Step::Ir {
				..
			}
			| Step::Each {
				..
			}
			| Step::Method {
				..
			}
			| Step::Subquery {
				..
			}
			| Step::Lookup {
				..
			}
			| Step::Signal {
				..
			}
			| Step::Recurse {
				..
			}
			| Step::Destructure {
				..
			}
			| Step::KnnQuery {
				..
			}
			| Step::JudgeCopy {
				..
			}
			| Step::Closure {
				..
			}
			| Step::NeedUnit {
				..
			}
			| Step::Function {
				..
			}
			| Step::Param {
				..
			}
			| Step::Block {
				..
			}
			| Step::Store {
				..
			}
			| Step::Write {
				..
			}
			| Step::IndexFn {
				..
			}
			| Step::Matches {
				..
			}
			| Step::External {
				..
			}
			| Step::Degree {
				..
			} => return refused("host step"),
		}
	}
}

/// Serves the call instruction at `at`: reads its arguments, invokes the builtin and stores the
/// result. Answers where the row resumes, or the refusal.
///
/// A builtin is served only when it is registered as a scalar function that is pure,
/// deterministic and synchronous and runs no closure it is handed. The arguments and the result
/// are charged to the fold's fuel by their size.
fn serve_call<'r>(
	program: &'r Program,
	pipeline: &'r Pipeline,
	row: &dyn RowReader,
	regs: &mut [Scalar<'r>],
	registry: &FunctionRegistry,
	capabilities: &CapabilityPolicy,
	at: usize,
) -> Result<usize, FoldRun> {
	let Some(Instr::Call {
		call,
		args,
		dst,
		..
	}) = pipeline.code.get(at)
	else {
		return Err(refused("host step"));
	};
	let Some(spec) = program.calls.get(*call as usize) else {
		return Err(refused("host step"));
	};
	if !capabilities.allows_call(&spec.name) {
		return Err(FoldRun::Refused(NotFoldable::CapabilityDenied));
	}
	let Some(function) = registry.get(&spec.name) else {
		return Err(refused("impure builtin"));
	};
	if !function.is_pure()
		|| !function.is_deterministic()
		|| function.is_async()
		|| !spec.closure_args.is_empty()
		|| !function.closure_args().is_empty()
	{
		return Err(refused("impure builtin"));
	}
	let mut values = Vec::with_capacity(args.len());
	for arg in args {
		values.push(
			interp::read_operand(regs, row, program, *arg).map_err(|error| classify(&error))?,
		);
	}
	let cost = values.iter().map(fuel::weight).fold(0, u32::saturating_add);
	if cost > 0 && fuel::charge(cost) {
		return Err(FoldRun::Refused(NotFoldable::FuelExhausted));
	}
	let value = function.invoke(values).map_err(|error| classify(&error))?;
	let weight = fuel::weight(&value);
	if weight > 0 && fuel::charge(weight) {
		return Err(FoldRun::Refused(NotFoldable::FuelExhausted));
	}
	let Some(slot) = regs.get_mut(usize::from(*dst)) else {
		return Err(refused("lowering"));
	};
	*slot = Scalar::Value(Cow::Owned(value));
	Ok(at + 1)
}

fn refused(what: &'static str) -> FoldRun {
	FoldRun::Refused(NotFoldable::HostRefused(what))
}

/// Maps an error of the run to the outcome of the fold. A resource error refuses the fold, a
/// value-class error is the value's raise with the same text the expression evaluator gives,
/// and any other error refuses the fold.
fn classify(error: &anyhow::Error) -> FoldRun {
	if is_resource_error(error) {
		return FoldRun::Refused(NotFoldable::ResourceError);
	}
	if let Some(raised) = error.downcast_ref::<crate::expr::Error>()
		&& is_value_class(raised)
	{
		return FoldRun::Raised(FoldedError {
			text: raised.to_string(),
		});
	}
	refused("error class")
}

/// Whether `error` is a failure of the machinery rather than of the value: depth, memory,
/// timeout or cancellation.
fn is_resource_error(error: &anyhow::Error) -> bool {
	matches!(
		crate::err::engine_error(error),
		Some(EngineError::QueryCancelled | EngineError::QueryTimedout(_))
	) || matches!(crate::err::exec_error(error), Some(crate::exec::Error::ComputationDepthExceeded))
		|| matches!(
			error.downcast_ref::<DatastoreError>(),
			Some(DatastoreError::QueryBeyondMemoryThreshold)
		)
}

/// Whether `error` is raised as a function of the operands alone: the data-shape errors an
/// expression may resolve to `NONE`, and the argument, arithmetic and conversion errors of the
/// builtin table's value class.
fn is_value_class(error: &crate::expr::Error) -> bool {
	use crate::expr::Error;
	error.is_ignorable()
		|| matches!(
			error,
			Error::InvalidMethodArguments { .. }
				| Error::InvalidRegex(_)
				| Error::TryRem(..)
				| Error::TryFrom(..)
				| Error::TimestampOverflow(_)
				| Error::ArithmeticOverflow(_)
				| Error::ArithmeticNegativeOverflow(_)
				| Error::InvalidBytesize
				| Error::ValueTooDeep { .. }
				| Error::IdInvalid { .. }
				| Error::TbInvalid { .. }
		)
}

#[cfg(test)]
pub(in crate::exec::vm) mod tests {
	use std::sync::Arc;

	use super::*;
	use crate::exec::ir::core::ir::IrScope;
	use crate::exec::ir::iface::region_kind::RegionKind;
	use crate::exec::vm::plain::ValueData;
	use crate::exec::vm::program::{CallSpec, Operand, Source};
	use crate::exec::{AccessMode, ContextLevel};

	/// A program of one pipeline over `code`, with `registers` registers.
	pub(in crate::exec::vm) fn program_with(
		consts: Vec<Value>,
		calls: Vec<CallSpec>,
		code: Vec<Instr>,
		registers: usize,
	) -> Program {
		Program {
			pipelines: vec![Pipeline {
				source: Source::Once,
				code,
				registers,
				in_place: false,
				stored_rows: false,
				reads_row: false,
				enforce: None,
				limits: Vec::new(),
				parallel: Default::default(),
				group: None,
				deref_windows: false,
				frontier_rows: None,
				read_ahead: false,
				rows_ahead: None,
			}],
			consts: ValueData::new(consts, "the constant pool").expect("the pool holds no closure"),
			fields: Vec::new(),
			paths: Vec::new(),
			limits: Vec::new(),
			sorts: Vec::new(),
			splits: Vec::new(),
			joins: Vec::new(),
			distincts: Vec::new(),
			verdicts: Vec::new(),
			fetches: Vec::new(),
			aggregates: Vec::new(),
			permissions: Vec::new(),
			computed_names: Vec::new(),
			table: None,
			write: None,
			scalar: true,
			rows: false,
			groups: Vec::new(),
			analyses: Vec::new(),
			served: Vec::new(),
			kinds: Vec::new(),
			params: Vec::new(),
			calls,
			matches: Vec::new(),
			externals: Vec::new(),
			degrees: Vec::new(),
			methods: Vec::new(),
			record_ids: Vec::new(),
			subqueries: Vec::new(),
			nested: Vec::new(),
			probes: Vec::new(),
			lookups: Vec::new(),
			functions: Vec::new(),
			closures: Vec::new(),
			closure_values: Vec::new(),
			host_code: Vec::new(),
			reservations: Vec::new(),
			implicit_tables: Vec::new(),
			units: Vec::<Arc<Program>>::new().into(),
			reach: None,
			dispatch: None,
			unit: None,
			tail: None,
			events: None,
			access_mode: AccessMode::ReadOnly,
			required_context: ContextLevel::Root,
			keeps_row_order: false,
			request: None,
		}
	}

	fn call_spec(name: &str) -> CallSpec {
		CallSpec {
			name: name.to_owned(),
			plan_depth: 0,
			index: None,
			closure_args: Vec::new(),
		}
	}

	/// A program that calls builtin `name` on the constants `args` into register 1.
	fn call_program(name: &str, args: Vec<Value>) -> FoldProgram {
		let operands = (0..args.len()).map(|at| Operand::Const(at as u32)).collect();
		FoldProgram {
			program: program_with(
				args,
				vec![call_spec(name)],
				vec![
					Instr::CallAllowed {
						call: 0,
						frame: None,
					},
					Instr::Call {
						call: 0,
						args: operands,
						dst: 1,
						frame: None,
						elem: None,
					},
					Instr::EndRow,
				],
				2,
			),
			result: 1,
		}
	}

	#[test]
	fn host_maps_steps_and_restores_fuel() {
		fuel::refill(1_234);
		fuel::charge(34);
		let before = (fuel::left(), surrealdb_runtime::fuel::quantum());

		let constant = FoldProgram {
			program: program_with(
				vec![Value::from(3i64)],
				Vec::new(),
				vec![
					Instr::Load {
						dst: 1,
						src: Operand::Const(0),
					},
					Instr::EndRow,
				],
				2,
			),
			result: 1,
		};
		match HOST_RUNNER.run_program(
			&constant,
			crate::exec::ir::analysis::cx::builtin_registry(),
			&CapabilityPolicy::AllowAll,
		) {
			FoldRun::Value(value, depth) => {
				assert_eq!(value, Value::from(3i64));
				assert_eq!(depth, 0);
			}
			other @ (FoldRun::Raised(_) | FoldRun::Refused(_)) => {
				panic!("a constant program does not answer its value: {other:?}")
			}
		}
		assert_eq!((fuel::left(), surrealdb_runtime::fuel::quantum()), before);

		let called = call_program("math::abs", vec![Value::from(-4i64)]);
		match HOST_RUNNER.run_program(
			&called,
			crate::exec::ir::analysis::cx::builtin_registry(),
			&CapabilityPolicy::AllowAll,
		) {
			FoldRun::Value(value, _) => assert_eq!(value, Value::from(4i64)),
			other @ (FoldRun::Raised(_) | FoldRun::Refused(_)) => {
				panic!("a pure call does not answer its value: {other:?}")
			}
		}
		assert_eq!((fuel::left(), surrealdb_runtime::fuel::quantum()), before);

		let spinning = FoldProgram {
			program: program_with(
				Vec::new(),
				Vec::new(),
				vec![Instr::Jump {
					target: 0,
				}],
				2,
			),
			result: 1,
		};
		assert!(matches!(
			HOST_RUNNER.run_program(
				&spinning,
				crate::exec::ir::analysis::cx::builtin_registry(),
				&CapabilityPolicy::AllowAll,
			),
			FoldRun::Refused(NotFoldable::FuelExhausted)
		));
		assert_eq!((fuel::left(), surrealdb_runtime::fuel::quantum()), before);
	}

	/// A lowering that has none for any value.
	struct NoLowering;

	impl ExprLowering for NoLowering {
		fn lower(
			&self,
			_ir: &Ir,
			_registry: &FunctionRegistry,
			_root: ValueId,
		) -> Result<FoldProgram, IrError> {
			Err(IrError::NotYet {
				what: "fold_host::ExprLowering::lower",
			})
		}
	}

	#[tokio::test]
	async fn no_lowering_answers_not_yet() {
		let ir = Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		);
		let host = FoldHost {
			lowering: &NoLowering,
			catalog: None,
		};
		let run = host
			.run(
				&ir,
				crate::exec::ir::analysis::cx::builtin_registry(),
				ValueId::default(),
				&CapabilityPolicy::AllowAll,
			)
			.await;
		assert!(matches!(run, FoldRun::Refused(NotFoldable::NotYet)), "{run:?}");
	}

	#[test]
	fn host_raises_value_errors_with_main_text() {
		let program =
			call_program("array::repeat", vec![Value::from(0i64), Value::from(100_000_000i64)]);
		match HOST_RUNNER.run_program(
			&program,
			crate::exec::ir::analysis::cx::builtin_registry(),
			&CapabilityPolicy::AllowAll,
		) {
			FoldRun::Raised(error) => {
				assert!(error.text.contains("Output must not exceed"), "{}", error.text)
			}
			other @ (FoldRun::Value(..) | FoldRun::Refused(_)) => {
				panic!("the repeat does not raise: {other:?}")
			}
		}
	}

	#[test]
	fn host_refuses_what_a_pure_builtin_does_not_need() {
		let draw = call_program("rand::float", Vec::new());
		assert!(matches!(
			HOST_RUNNER.run_program(
				&draw,
				crate::exec::ir::analysis::cx::builtin_registry(),
				&CapabilityPolicy::AllowAll,
			),
			FoldRun::Refused(NotFoldable::HostRefused("impure builtin"))
		));
		let unknown = call_program("no::such::function", Vec::new());
		assert!(matches!(
			HOST_RUNNER.run_program(
				&unknown,
				crate::exec::ir::analysis::cx::builtin_registry(),
				&CapabilityPolicy::AllowAll,
			),
			FoldRun::Refused(NotFoldable::HostRefused("impure builtin"))
		));
		let reads_a_row = FoldProgram {
			program: program_with(
				Vec::new(),
				Vec::new(),
				vec![Instr::Fetch {
					fetch: 0,
					src: 0,
					dst: 1,
				}],
				2,
			),
			result: 1,
		};
		assert!(matches!(
			HOST_RUNNER.run_program(
				&reads_a_row,
				crate::exec::ir::analysis::cx::builtin_registry(),
				&CapabilityPolicy::AllowAll,
			),
			FoldRun::Refused(NotFoldable::HostRefused("host step"))
		));
	}
}
