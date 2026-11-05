//! The steps of a request, assembled from the container that holds it.
//!
//! A request container's top region holds its statements, each a `phys.stmt`, and the transactions
//! that group them, each a `phys.txn` whose region holds `phys.stmt`s; a `phys.request` op holds
//! the same in its region. [`request_slots`] reads them in the order they were written, one
//! [`Slot`] per statement, and [`request_steps`] turns the slots into the
//! [`RequestStep`]s a request program is driven by.
//!
//! # What a statement is
//!
//! The statement of a `phys.stmt` is the last physical op in its region other than the yield.
//!
//! - `phys.option` is [`Slot::Option`], `phys.use` is [`Slot::Use`] and a `phys.let` that binds a
//!   session parameter is [`Slot::Let`]. Each carries what planning the statements after it reads
//!   (the option, the names a `USE` selects, the name a `LET` binds) and the region of the
//!   statement, which the caller lowers to the program that does the work: the session effects of
//!   [`super::info`] record what the executor applies to the session when the program ends.
//! - `phys.access`, `phys.show`, `phys.kill` and `phys.live` are [`Slot::Run`]: each is a program
//!   of its own ([`super::access`], [`super::info::lower_kill`] and [`super::info::lower_live`]).
//! - A transaction control that cannot act where it stands (`expr.raise` of a misplaced control) is
//!   the step of the statement it was written as: [`Slot::Begin`], [`Slot::Commit`] or
//!   [`Slot::Cancel`]. The planner plans every `BEGIN`, `COMMIT` and `CANCEL` as that step wherever
//!   it stands, and the driver decides at run time, from the position the statement holds, whether
//!   it acts or raises the error of the position.
//! - Any other statement is [`Slot::Run`], whose program the caller lowers from the statement's
//!   region.
//!
//! # Transaction controls
//!
//! The first statement of an explicit transaction that begins in the request is its `BEGIN`, and
//! the last is its `COMMIT` or `CANCEL` when the transaction ends in one. A transaction that is
//! unterminated, or that stays open for the next batch of its stream, has no terminal step, and a
//! session transaction has no step at either end. A statement at one of those positions is the
//! control only when its body is bare: no statement op and no expression other than the `NONE` it
//! yields. A body that raises, as a control that cannot act where it stands does, is not the
//! control and is classified by what it holds.
//!
//! # What the caller decides
//!
//! Whether the program of a statement is compiled with the request or served when the driver
//! reaches it depends on the catalog and the session the statements before it leave, which a
//! container does not hold. [`request_steps`] asks for the program of every [`Slot::Option`],
//! [`Slot::Use`], [`Slot::Let`] and [`Slot::Run`] through a callback that answers its index among
//! the request's statement programs, or `None` for a statement served when reached.

use super::super::program::RequestStep;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ExprOp, MisplacedControl, RaiseKind};
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::logical::ops::{LetBinding, TxnRegionEnd, TxnRegionMode, TxnStart, UseTarget};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::PhysOp;
use crate::expr::Kind;
use crate::val::Value;

/// What planning knows of one operand of a `USE` before the run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UseOperand {
	/// The statement has no operand at this level.
	Absent,
	/// A name written as a string.
	Written(String),
	/// A parameter, which a session that holds its value resolves.
	Param(String),
	/// Any other expression, which only the run evaluates.
	Computed,
}

/// The two operands of a `USE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UseOperands {
	pub(crate) ns: UseOperand,
	pub(crate) db: UseOperand,
}

/// One statement of a request, as the driver takes it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Slot {
	/// `BEGIN`.
	Begin,
	/// `COMMIT`.
	Commit,
	/// `CANCEL`.
	Cancel,
	/// `OPTION name = what`; `statement` is its region.
	Option {
		name: String,
		what: bool,
		statement: RegionId,
	},
	/// `USE`: the names it writes as literals, or `computed` where the run evaluates them.
	/// `operands` says, for each level the statement names, what planning knows of it before the
	/// run; `statement` is its region.
	Use {
		ns: Option<String>,
		db: Option<String>,
		computed: bool,
		operands: UseOperands,
		statement: RegionId,
	},
	/// A session `LET`: `statement` is the region whose program is the value, and `kind` the kind
	/// the value is coerced to.
	Let {
		name: String,
		kind: Option<Kind>,
		statement: RegionId,
	},
	/// A statement the engine runs: `statement` is its region.
	Run {
		statement: RegionId,
	},
}

/// The statements of the request `ir` holds, in the order they were written.
///
/// A top-level op that is not a statement, a transaction or a request, and a transaction that
/// continues one a previous batch began, answer [`IrError::Stale`]: no producer builds either.
pub(crate) fn request_slots(ir: &Ir) -> Result<Vec<Slot>, IrError> {
	let mut slots = Vec::new();
	collect(ir, ir.top(), &mut slots)?;
	Ok(slots)
}

/// The steps of the request `ir` holds, in the order the statements were written.
///
/// `statement` is asked once for each statement with a program, in order, and answers the
/// index of that statement's program among the request's, or `None` for a statement served when
/// the driver reaches it.
pub(crate) fn request_steps(
	ir: &Ir,
	mut statement: impl FnMut(&Slot) -> Result<Option<u32>, IrError>,
) -> Result<Vec<RequestStep>, IrError> {
	let slots = request_slots(ir)?;
	let mut steps = Vec::with_capacity(slots.len());
	for slot in &slots {
		steps.push(match slot {
			Slot::Begin => RequestStep::Begin,
			Slot::Commit => RequestStep::Commit,
			Slot::Cancel => RequestStep::Cancel,
			// The three statements that change the session are programs like any other: the
			// session effects they record are applied when the program ends.
			Slot::Option {
				..
			}
			| Slot::Use {
				..
			}
			| Slot::Let {
				..
			}
			| Slot::Run {
				..
			} => RequestStep::Run {
				statement: statement(slot)?,
			},
		});
	}
	Ok(steps)
}

/// Appends the slots of the statements and transactions in `region`.
fn collect(ir: &Ir, region: RegionId, slots: &mut Vec<Slot>) -> Result<(), IrError> {
	for op in ops_in(ir, region) {
		let data = ir.op(op).ok_or(IrError::Stale("ir_lower::control: op"))?;
		let OpKind::Phys(phys) = &data.kind else {
			return Err(IrError::Stale("ir_lower::control: a top-level op that is not physical"));
		};
		match phys {
			PhysOp::Request {
				..
			} => collect(ir, only_region(ir, op)?, slots)?,
			PhysOp::Txn {
				mode,
			} => transaction(ir, only_region(ir, op)?, *mode, slots)?,
			PhysOp::Stmt {
				..
			} => slots.push(statement(ir, only_region(ir, op)?)?),
			PhysOp::TableScan(_)
			| PhysOp::IndexScan(_)
			| PhysOp::IndexUnion(_)
			| PhysOp::Bitmap(_)
			| PhysOp::IndexLookup(_)
			| PhysOp::FullTextScan(_)
			| PhysOp::KeyRangeScan(_)
			| PhysOp::KnnScan(_)
			| PhysOp::KnnTopK(_)
			| PhysOp::Count(_)
			| PhysOp::RecordScan(_)
			| PhysOp::Vacant {
				..
			}
			| PhysOp::Enforced {
				..
			}
			| PhysOp::Values {
				..
			}
			| PhysOp::Subquery
			| PhysOp::Probe {
				..
			}
			| PhysOp::Union
			| PhysOp::Join(_)
			| PhysOp::Distinct {
				..
			}
			| PhysOp::PermissionFilter(_)
			| PhysOp::Layer(_)
			| PhysOp::Filter
			| PhysOp::Fetch
			| PhysOp::Split {
				..
			}
			| PhysOp::Sort(_)
			| PhysOp::Window(_)
			| PhysOp::Aggregate(_)
			| PhysOp::Limit {
				..
			}
			| PhysOp::Project(_)
			| PhysOp::Timeout
			| PhysOp::VersionScope
			| PhysOp::Only
			| PhysOp::Explain {
				..
			}
			| PhysOp::Eval
			| PhysOp::Statement {
				..
			}
			| PhysOp::Match {
				..
			}
			| PhysOp::MatchStep(_)
			| PhysOp::DdlGuard {
				..
			}
			| PhysOp::Define(_)
			| PhysOp::Remove(_)
			| PhysOp::Alter(_)
			| PhysOp::Rebuild(_)
			| PhysOp::Info {
				..
			}
			| PhysOp::Use {
				..
			}
			| PhysOp::Option {
				..
			}
			| PhysOp::Access(_)
			| PhysOp::Kill
			| PhysOp::Live(_)
			| PhysOp::Subscriber(_)
			| PhysOp::Show(_)
			| PhysOp::Write(_)
			| PhysOp::WriteStage {
				..
			}
			| PhysOp::Events(_)
			| PhysOp::Assign(_)
			| PhysOp::Unset {
				..
			}
			| PhysOp::Content {
				..
			}
			| PhysOp::Block
			| PhysOp::Let(_)
			| PhysOp::Sleep
			| PhysOp::If {
				..
			}
			| PhysOp::Loop {
				..
			}
			| PhysOp::Pipeline(_)
			| PhysOp::Choose(_)
			| PhysOp::Prefetch(_)
			| PhysOp::Materialize {
				..
			}
			| PhysOp::LoadUnit(_)
			| PhysOp::Yield
			| PhysOp::Return
			| PhysOp::Break
			| PhysOp::Next => {
				return Err(IrError::Stale(
					"ir_lower::control: a top-level op that is not a statement",
				));
			}
		}
	}
	Ok(())
}

/// The one region `op` owns.
fn only_region(ir: &Ir, op: OpId) -> Result<RegionId, IrError> {
	let data = ir.op(op).ok_or(IrError::Stale("ir_lower::control: op"))?;
	match data.regions.as_slice() {
		[region] => Ok(*region),
		_ => Err(IrError::Stale("ir_lower::control: region arity")),
	}
}

/// Appends the slots of the transaction whose region is `region`.
fn transaction(
	ir: &Ir,
	region: RegionId,
	mode: TxnRegionMode,
	slots: &mut Vec<Slot>,
) -> Result<(), IrError> {
	let end = match mode {
		TxnRegionMode::Explicit {
			start: TxnStart::Begin,
			end,
			..
		} => Some(end),
		TxnRegionMode::Explicit {
			start: TxnStart::Continued,
			..
		} => return Err(IrError::Stale("ir_lower::control: a continued transaction")),
		TxnRegionMode::Session {
			..
		} => None,
	};
	let statements: Vec<OpId> = ops_in(ir, region).collect();
	for (at, stmt) in statements.iter().enumerate() {
		let data = ir.op(*stmt).ok_or(IrError::Stale("ir_lower::control: op"))?;
		if !matches!(&data.kind, OpKind::Phys(PhysOp::Stmt { .. })) {
			return Err(IrError::Stale("ir_lower::control: a transaction holds a non-statement"));
		}
		let body = only_region(ir, *stmt)?;
		let control = match end {
			Some(_) if at == 0 => Some(Slot::Begin),
			Some(TxnRegionEnd::Commit) if at + 1 == statements.len() => Some(Slot::Commit),
			Some(TxnRegionEnd::Cancel) if at + 1 == statements.len() => Some(Slot::Cancel),
			Some(_) | None => None,
		};
		match control {
			Some(control) if is_bare(ir, body) => slots.push(control),
			Some(_) | None => slots.push(statement(ir, body)?),
		}
	}
	Ok(())
}

/// Whether the body of a statement holds nothing but the `NONE` a transaction control yields.
fn is_bare(ir: &Ir, region: RegionId) -> bool {
	ops_in(ir, region).all(|op| {
		matches!(
			ir.op(op).map(|data| &data.kind),
			Some(OpKind::Phys(PhysOp::Yield) | OpKind::Expr(ExprOp::Yield | ExprOp::Literal(_)))
		)
	})
}

/// The slot of the statement whose body is `region`.
fn statement(ir: &Ir, region: RegionId) -> Result<Slot, IrError> {
	let mut last = None;
	for op in ops_in(ir, region) {
		let data = ir.op(op).ok_or(IrError::Stale("ir_lower::control: op"))?;
		if let OpKind::Expr(ExprOp::Raise(kind)) = &data.kind
			&& let RaiseKind::Misplaced(control) = &**kind
		{
			return Ok(match control {
				MisplacedControl::NestedBegin | MisplacedControl::BeginInSession => Slot::Begin,
				MisplacedControl::CommitOutsideBlock => Slot::Commit,
				MisplacedControl::CancelOutsideBlock => Slot::Cancel,
			});
		}
		if let OpKind::Phys(phys) = &data.kind
			&& !matches!(phys, PhysOp::Yield)
		{
			last = Some((op, phys));
		}
	}
	Ok(match last {
		Some((
			_,
			PhysOp::Option {
				name,
				value,
			},
		)) => Slot::Option {
			name: name.as_str().to_owned(),
			what: *value,
			statement: region,
		},
		Some((
			op,
			PhysOp::Use {
				target,
			},
		)) => use_slot(ir, op, *target, region)?,
		Some((_, PhysOp::Let(attrs))) if attrs.binding == LetBinding::Session => Slot::Let {
			name: attrs.name.as_str().to_owned(),
			kind: attrs.ty.clone(),
			statement: region,
		},
		Some(_) | None => Slot::Run {
			statement: region,
		},
	})
}

/// The slot of the `phys.use` `op`, the statement of `region`: its names as written when every
/// operand is a string literal, and otherwise a `USE` whose run evaluates them. `USE DEFAULT`
/// names nothing, so it is computed.
fn use_slot(ir: &Ir, op: OpId, target: UseTarget, region: RegionId) -> Result<Slot, IrError> {
	let data = ir.op(op).ok_or(IrError::Stale("ir_lower::control: op"))?;
	let expected = match target {
		UseTarget::Ns | UseTarget::Db => 1,
		UseTarget::NsDb => 2,
		UseTarget::Default => 0,
	};
	if data.operands.len() != expected {
		return Err(IrError::Stale("ir_lower::control: phys.use operand arity"));
	}
	let operand = |at: usize| match data.operands.get(at) {
		Some(value) => use_operand(ir, *value),
		None => UseOperand::Absent,
	};
	let (ns, db) = match target {
		UseTarget::Ns => (operand(0), UseOperand::Absent),
		UseTarget::Db => (UseOperand::Absent, operand(0)),
		UseTarget::NsDb => (operand(0), operand(1)),
		UseTarget::Default => (UseOperand::Absent, UseOperand::Absent),
	};
	let written = |operand: &UseOperand| match operand {
		UseOperand::Written(name) => Some(name.clone()),
		UseOperand::Absent | UseOperand::Param(_) | UseOperand::Computed => None,
	};
	let stated =
		|operand: &UseOperand| matches!(operand, UseOperand::Written(_) | UseOperand::Absent);
	let computed = target == UseTarget::Default || !(stated(&ns) && stated(&db));
	Ok(Slot::Use {
		ns: if computed {
			None
		} else {
			written(&ns)
		},
		db: if computed {
			None
		} else {
			written(&db)
		},
		computed,
		operands: UseOperands {
			ns,
			db,
		},
		statement: region,
	})
}

/// What planning knows of the `USE` operand `value`.
fn use_operand(ir: &Ir, value: ValueId) -> UseOperand {
	if let Some(name) = written_name(ir, value) {
		return UseOperand::Written(name);
	}
	let Some(ValueDef::OpResult {
		op,
		index: 0,
	}) = ir.value(value).map(|data| data.def)
	else {
		return UseOperand::Computed;
	};
	match ir.op(op).map(|data| &data.kind) {
		Some(OpKind::Expr(ExprOp::FreeParam {
			name,
		})) => UseOperand::Param(name.to_string()),
		_ => UseOperand::Computed,
	}
}

/// The name `value` is, when it is a string literal.
fn written_name(ir: &Ir, value: ValueId) -> Option<String> {
	let ValueDef::OpResult {
		op,
		index: 0,
	} = ir.value(value)?.def
	else {
		return None;
	};
	if let OpKind::Expr(ExprOp::Literal(literal)) = &ir.op(op)?.kind
		&& let Value::String(name) = literal.as_ref()
	{
		return Some(name.as_str().to_owned());
	}
	None
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::{Strand, TableName};

	use super::*;
	use crate::exec::ir::expr::ops::MisplacedControl;
	use crate::exec::ir::iface::region_kind::{RegionKind, StatementCommit, TxnKind};
	use crate::exec::ir::iface::statements::AccessOp;
	use crate::exec::ir::iface::types::TypeKnowledge;
	use crate::exec::ir::logical::ops::{AccessAttrs, Emission, LetAttrs, ShowAttrs};
	use crate::exec::vm::ir_lower::tests::Builder;
	use crate::expr::statements::access::PurgeKind;
	use crate::expr::statements::show::ShowSince;

	/// A `phys.stmt` committed as `commit` whose body `body` builds, and the region of the body.
	fn stmt(b: &mut Builder, commit: StatementCommit, body: impl FnOnce(&mut Builder)) -> RegionId {
		let op = b.add_phys(
			PhysOp::Stmt {
				commit,
				control: None,
			},
			&[],
		);
		b.open(op, RegionKind::Statement(commit), &[TypeKnowledge::Unknown]);
		body(b);
		b.close();
		only_region(&b.ir, op).expect("the statement holds one region")
	}

	/// The body of a transaction control: the `NONE` it yields.
	fn control(b: &mut Builder) {
		let none = b.literal(Value::None);
		b.add_phys(PhysOp::Yield, &[none]);
	}

	/// The body of an expression statement.
	fn eval(b: &mut Builder) {
		let one = b.literal(Value::from(1_i64));
		b.add_phys(PhysOp::Eval, &[one]);
		b.add_phys(PhysOp::Yield, &[one]);
	}

	/// A `phys.txn` of `mode` whose statements `body` builds.
	fn txn(b: &mut Builder, mode: TxnRegionMode, kind: TxnKind, body: impl FnOnce(&mut Builder)) {
		let op = b.add_phys(
			PhysOp::Txn {
				mode,
			},
			&[],
		);
		b.open(op, RegionKind::Transaction(kind), &[]);
		body(b);
		b.close();
	}

	fn explicit(end: TxnRegionEnd) -> TxnRegionMode {
		TxnRegionMode::Explicit {
			start: TxnStart::Begin,
			end,
			emission: Emission::default(),
		}
	}

	/// The kinds of the slots of `b`, with the region of a statement left out.
	fn shape(b: &Builder) -> Vec<&'static str> {
		request_slots(&b.ir)
			.expect("the request holds statements")
			.iter()
			.map(|slot| match slot {
				Slot::Begin => "begin",
				Slot::Commit => "commit",
				Slot::Cancel => "cancel",
				Slot::Option {
					..
				} => "option",
				Slot::Use {
					..
				} => "use",
				Slot::Let {
					..
				} => "let",
				Slot::Run {
					..
				} => "run",
			})
			.collect()
	}

	/// The `USE` slot of a statement that names `names` as literals, or the value `computed`
	/// computes.
	fn use_of(target: UseTarget, names: &[Value], computed: bool) -> Slot {
		let mut b = Builder::new();
		stmt(&mut b, StatementCommit::Auto, |b| {
			let mut operands: Vec<ValueId> =
				names.iter().map(|name| b.literal(name.clone())).collect();
			if computed {
				operands.push(b.call("rand::float", &[]));
			}
			b.add_phys(
				PhysOp::Use {
					target,
				},
				&operands,
			);
			let none = b.literal(Value::None);
			b.add_phys(PhysOp::Yield, &[none]);
		});
		request_slots(&b.ir).expect("the request holds a USE").remove(0)
	}

	fn string(name: &str) -> Value {
		Value::String(Strand::new(name))
	}

	#[test]
	fn statements_outside_a_transaction_are_slots_of_their_own() {
		let mut b = Builder::new();
		stmt(&mut b, StatementCommit::Auto, |b| {
			b.add_phys(
				PhysOp::Option {
					name: Strand::new("IMPORT"),
					value: true,
				},
				&[],
			);
			control(b);
		});
		stmt(&mut b, StatementCommit::Auto, |b| {
			b.add_phys(
				PhysOp::Show(Box::new(ShowAttrs {
					table: None,
					since: crate::expr::statements::show::ShowSince::Versionstamp(1),
					limit: None,
				})),
				&[],
			);
			control(b);
		});
		let eval_region = stmt(&mut b, StatementCommit::Auto, eval);
		assert_eq!(shape(&b), vec!["option", "run", "run"]);
		let slots = request_slots(&b.ir).expect("the request holds statements");
		assert!(matches!(
			&slots[0],
			Slot::Option { name, what: true, .. } if name == "IMPORT"
		));
		assert_eq!(
			slots[2],
			Slot::Run {
				statement: eval_region,
			}
		);
	}

	#[test]
	fn access_and_show_changes_are_programs_of_their_own() {
		let mut b = Builder::new();
		let show = stmt(&mut b, StatementCommit::Auto, |b| {
			let changes = b.add_phys_value(
				PhysOp::Show(Box::new(ShowAttrs {
					table: Some(TableName::new("t")),
					since: ShowSince::Versionstamp(0),
					limit: None,
				})),
				&[],
			);
			let value = b.result_of(changes);
			b.add_phys(PhysOp::Yield, &[value]);
		});
		let access = stmt(&mut b, StatementCommit::Auto, |b| {
			let purged = b.add_phys_value(
				PhysOp::Access(Box::new(AccessAttrs {
					ac: Strand::new("api"),
					base: None,
					op: AccessOp::Purge {
						kind: PurgeKind::Expired,
						grace: std::time::Duration::ZERO,
					},
				})),
				&[],
			);
			let value = b.result_of(purged);
			b.add_phys(PhysOp::Yield, &[value]);
		});
		stmt(&mut b, StatementCommit::Auto, |b| {
			let id = b.literal(Value::from(1_i64));
			b.add_phys(PhysOp::Kill, &[id]);
			control(b);
		});
		assert_eq!(shape(&b), vec!["run", "run", "run"]);
		let slots = request_slots(&b.ir).expect("the request holds statements");
		assert_eq!(
			slots[..2],
			[
				Slot::Run {
					statement: show,
				},
				Slot::Run {
					statement: access,
				},
			]
		);
	}

	#[test]
	fn an_explicit_transaction_begins_and_ends_with_its_controls() {
		for (end, last) in [(TxnRegionEnd::Commit, "commit"), (TxnRegionEnd::Cancel, "cancel")] {
			let mut b = Builder::new();
			txn(&mut b, explicit(end), TxnKind::Explicit, |b| {
				stmt(b, StatementCommit::InTxn, control);
				stmt(b, StatementCommit::InTxn, eval);
				stmt(b, StatementCommit::InTxn, control);
			});
			assert_eq!(shape(&b), vec!["begin", "run", last]);
		}
	}

	#[test]
	fn an_unterminated_or_open_transaction_has_no_terminal_step() {
		for end in [TxnRegionEnd::Unterminated, TxnRegionEnd::Open] {
			let mut b = Builder::new();
			txn(&mut b, explicit(end), TxnKind::Explicit, |b| {
				stmt(b, StatementCommit::InTxn, control);
				stmt(b, StatementCommit::InTxn, eval);
			});
			assert_eq!(shape(&b), vec!["begin", "run"]);
		}
	}

	#[test]
	fn a_session_transaction_has_no_control_steps() {
		let mut b = Builder::new();
		txn(
			&mut b,
			TxnRegionMode::Session {
				id: 7,
			},
			TxnKind::Session,
			|b| {
				stmt(b, StatementCommit::InTxn, eval);
				stmt(b, StatementCommit::InTxn, eval);
			},
		);
		assert_eq!(shape(&b), vec!["run", "run"]);
	}

	#[test]
	fn a_control_whose_body_is_not_bare_is_a_statement() {
		let mut b = Builder::new();
		txn(&mut b, explicit(TxnRegionEnd::Commit), TxnKind::Explicit, |b| {
			stmt(b, StatementCommit::InTxn, control);
			stmt(b, StatementCommit::InTxn, eval);
			// A `COMMIT` that cannot act where it stands holds the call that raises its error.
			stmt(b, StatementCommit::InTxn, |b| {
				let raised = b.call("rand::float", &[]);
				b.add_phys(PhysOp::Yield, &[raised]);
			});
		});
		assert_eq!(shape(&b), vec!["begin", "run", "run"]);
	}

	#[test]
	fn a_misplaced_control_keeps_the_step_of_the_statement_it_was_written_as() {
		let mut b = Builder::new();
		txn(&mut b, explicit(TxnRegionEnd::Commit), TxnKind::Explicit, |b| {
			stmt(b, StatementCommit::InTxn, control);
			stmt(b, StatementCommit::InTxn, |b| {
				b.terminate(
					ExprOp::Raise(Box::new(RaiseKind::Misplaced(MisplacedControl::NestedBegin))),
					&[],
				);
			});
			stmt(b, StatementCommit::InTxn, control);
		});
		assert_eq!(shape(&b), vec!["begin", "begin", "commit"]);
	}

	#[test]
	fn a_continued_transaction_is_refused() {
		let mut b = Builder::new();
		txn(
			&mut b,
			TxnRegionMode::Explicit {
				start: TxnStart::Continued,
				end: TxnRegionEnd::Open,
				emission: Emission::default(),
			},
			TxnKind::Explicit,
			|b| {
				stmt(b, StatementCommit::InTxn, eval);
			},
		);
		assert!(matches!(request_slots(&b.ir), Err(IrError::Stale(_))));
	}

	#[test]
	fn a_request_op_holds_the_statements_of_its_region() {
		let mut b = Builder::new();
		let request = b.add_phys(
			PhysOp::Request {
				fragment: None,
				nested: false,
			},
			&[],
		);
		b.open(request, RegionKind::Request, &[]);
		stmt(&mut b, StatementCommit::Auto, eval);
		b.close();
		assert_eq!(shape(&b), vec!["run"]);
	}

	#[test]
	fn an_op_that_is_not_a_statement_at_the_top_is_refused() {
		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		b.add_phys(PhysOp::Eval, &[one]);
		assert!(matches!(request_slots(&b.ir), Err(IrError::Stale(_))));
	}

	/// The names, the `computed` flag and the operands of a `USE` slot.
	fn use_parts(slot: Slot) -> (Option<String>, Option<String>, bool, UseOperands) {
		let Slot::Use {
			ns,
			db,
			computed,
			operands,
			..
		} = slot
		else {
			panic!("expected a USE, got {slot:?}");
		};
		(ns, db, computed, operands)
	}

	#[test]
	fn use_names_are_carried_when_every_operand_is_a_literal() {
		let (ns, db, computed, operands) = use_parts(use_of(UseTarget::Ns, &[string("a")], false));
		assert_eq!((ns, db, computed), (Some("a".to_owned()), None, false));
		assert_eq!(operands.ns, UseOperand::Written("a".to_owned()));
		assert_eq!(operands.db, UseOperand::Absent);
		let (ns, db, computed, _) = use_parts(use_of(UseTarget::Db, &[string("b")], false));
		assert_eq!((ns, db, computed), (None, Some("b".to_owned()), false));
		let (ns, db, computed, _) =
			use_parts(use_of(UseTarget::NsDb, &[string("a"), string("b")], false));
		assert_eq!((ns, db, computed), (Some("a".to_owned()), Some("b".to_owned()), false));
	}

	#[test]
	fn a_use_with_an_evaluated_or_non_string_name_is_computed() {
		for slot in [
			use_of(UseTarget::Default, &[], false),
			use_of(UseTarget::Ns, &[Value::from(1_i64)], false),
			// One written name and one evaluated: the pair is computed, as the executor takes it.
			use_of(UseTarget::NsDb, &[string("a")], true),
		] {
			let (ns, db, computed, _) = use_parts(slot);
			assert_eq!((ns, db, computed), (None, None, true));
		}
		let (.., operands) = use_parts(use_of(UseTarget::NsDb, &[string("a")], true));
		assert_eq!(operands.ns, UseOperand::Written("a".to_owned()));
		assert_eq!(operands.db, UseOperand::Computed);
		let (.., operands) = use_parts(use_of(UseTarget::Default, &[], false));
		assert_eq!((operands.ns, operands.db), (UseOperand::Absent, UseOperand::Absent));
	}

	#[test]
	fn a_use_of_a_parameter_names_it_for_a_session_that_holds_its_value() {
		let mut b = Builder::new();
		stmt(&mut b, StatementCommit::Auto, |b| {
			let target = b.add(
				ExprOp::FreeParam {
					name: "target".into(),
				},
				&[],
			);
			b.add_phys(
				PhysOp::Use {
					target: UseTarget::Ns,
				},
				&[target],
			);
			let none = b.literal(Value::None);
			b.add_phys(PhysOp::Yield, &[none]);
		});
		let slot = request_slots(&b.ir).expect("the request holds a USE").remove(0);
		let (ns, db, computed, operands) = use_parts(slot);
		assert_eq!((ns, db, computed), (None, None, true));
		assert_eq!(operands.ns, UseOperand::Param("target".to_owned()));
	}

	#[test]
	fn option_use_and_let_are_programs_of_the_request() {
		let mut b = Builder::new();
		let option = stmt(&mut b, StatementCommit::Auto, |b| {
			b.add_phys(
				PhysOp::Option {
					name: Strand::new("IMPORT"),
					value: true,
				},
				&[],
			);
			control(b);
		});
		let used = stmt(&mut b, StatementCommit::Auto, |b| {
			let ns = b.literal(string("a"));
			b.add_phys(
				PhysOp::Use {
					target: UseTarget::Ns,
				},
				&[ns],
			);
			control(b);
		});
		let bound = stmt(&mut b, StatementCommit::Auto, |b| {
			let one = b.literal(Value::from(1_i64));
			b.add_phys(
				PhysOp::Let(Box::new(LetAttrs {
					name: Strand::new("a"),
					binding: LetBinding::Session,
					ty: None,
				})),
				&[one],
			);
			control(b);
		});
		let mut asked = Vec::new();
		let steps = request_steps(&b.ir, |slot| {
			asked.push(slot.clone());
			Ok(Some(asked.len() as u32 - 1))
		})
		.expect("the steps assemble");
		assert_eq!(
			steps,
			vec![
				RequestStep::Run {
					statement: Some(0)
				},
				RequestStep::Run {
					statement: Some(1)
				},
				RequestStep::Run {
					statement: Some(2)
				},
			]
		);
		let regions: Vec<RegionId> = asked
			.iter()
			.map(|slot| match slot {
				Slot::Option {
					statement,
					..
				}
				| Slot::Use {
					statement,
					..
				}
				| Slot::Let {
					statement,
					..
				} => *statement,
				other @ (Slot::Begin
				| Slot::Commit
				| Slot::Cancel
				| Slot::Run {
					..
				}) => {
					panic!("unexpected slot {other:?}")
				}
			})
			.collect();
		assert_eq!(regions, vec![option, used, bound]);
	}

	#[test]
	fn a_session_let_carries_its_name_kind_and_value_region() {
		let mut b = Builder::new();
		let region = stmt(&mut b, StatementCommit::Auto, |b| {
			let one = b.literal(Value::from(1_i64));
			b.add_phys(
				PhysOp::Let(Box::new(LetAttrs {
					name: Strand::new("a"),
					binding: LetBinding::Session,
					ty: Some(Kind::Int),
				})),
				&[one],
			);
			control(b);
		});
		assert_eq!(
			request_slots(&b.ir).expect("the request holds a LET"),
			vec![Slot::Let {
				name: "a".to_owned(),
				kind: Some(Kind::Int),
				statement: region,
			}]
		);
	}

	#[test]
	fn a_local_let_is_not_a_request_step() {
		let mut b = Builder::new();
		let region = stmt(&mut b, StatementCommit::Auto, |b| {
			let one = b.literal(Value::from(1_i64));
			b.add_phys(
				PhysOp::Let(Box::new(LetAttrs {
					name: Strand::new("a"),
					binding: LetBinding::Local,
					ty: None,
				})),
				&[one],
			);
			control(b);
		});
		assert_eq!(
			request_slots(&b.ir).expect("the request holds a statement"),
			vec![Slot::Run {
				statement: region,
			}]
		);
	}

	#[test]
	fn steps_ask_the_caller_for_the_statements_it_lowers() {
		let mut b = Builder::new();
		txn(&mut b, explicit(TxnRegionEnd::Commit), TxnKind::Explicit, |b| {
			stmt(b, StatementCommit::InTxn, control);
			stmt(b, StatementCommit::InTxn, eval);
			stmt(b, StatementCommit::InTxn, |b| {
				b.add_phys(
					PhysOp::Option {
						name: Strand::new("FORCE"),
						value: false,
					},
					&[],
				);
				control(b);
			});
			stmt(b, StatementCommit::InTxn, eval);
			stmt(b, StatementCommit::InTxn, control);
		});
		let mut asked = Vec::new();
		let steps = request_steps(&b.ir, |slot| {
			asked.push(slot.clone());
			// The first statement is compiled with the request and the others are served.
			Ok((asked.len() == 1).then_some(0))
		})
		.expect("the steps assemble");
		assert_eq!(asked.len(), 3);
		assert_eq!(
			steps,
			vec![
				RequestStep::Begin,
				RequestStep::Run {
					statement: Some(0),
				},
				RequestStep::Run {
					statement: None,
				},
				RequestStep::Run {
					statement: None,
				},
				RequestStep::Commit,
			]
		);
	}

	#[test]
	fn the_callback_error_ends_the_assembly() {
		let mut b = Builder::new();
		stmt(&mut b, StatementCommit::Auto, eval);
		let refused = request_steps(&b.ir, |_| Err(IrError::Stale("ir_lower::control: test")));
		assert!(matches!(refused, Err(IrError::Stale("ir_lower::control: test"))));
	}
}
