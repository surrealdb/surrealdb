//! The instruction forms the IR's lowering adds to the VM.
//!
//! Every new instruction form is a variant of [`IrForm`], so the instruction set grows without
//! touching [`Instr`](super::program::Instr) beyond its one `Ir` variant. An [`IrInstr`] is one
//! box over a form however many forms there are, so `Instr` is never wider for a form it
//! carries.
//!
//! What the rest of the VM needs to know of a form is a method of [`IrInstr`], answered here:
//! the leaf classifier, the interpreter's fallback and a pass that moves code call it, so adding a
//! form edits this file and nothing that matches on `Instr`.
//!
//! The forms fall into families, each executed by an executor other than the one that emits it,
//! and `ir_exec::run` answers [`IrInstr::unexecuted`] for a form that has no executor:
//! - **Kernel**: [`IrForm::Kernel`] runs the per-element loop of an eligible site a batch at a
//!   time. Its executor is the IR executor.
//! - **Batched read**: [`IrForm::BatchRead`] fills the statement's read buffer for the positions a
//!   loop is about to visit, and [`IrForm::BatchReset`] starts a loop site's coverage anew. Both
//!   are executed by the IR executor.
//! - **Skip**: [`IrForm::SkipRow`] marks the position at which a skipped row's caught failure is
//!   consumed. It is executed by the IR executor.
//! - **Memo**: [`IrForm::MemoGet`], [`IrForm::MemoPut`], [`IrForm::MemoReset`] and
//!   [`IrForm::MemoTruthy`] read and fill the cells that hold a hoisted, row-independent value.
//!   They are executed by the IR executor.
//! - **Write fan-out**: [`IrForm::WriteResume`], [`IrForm::WriteCut`], [`IrForm::WriteSkip`] and
//!   [`IrForm::WriteOffPath`] bound the part of a write body that runs ahead of the ordered apply.
//!   They are executed by the IR executor with the write fan-out state.
//!
//! Every payload is boxed or empty, and every type and field here is visible to `vm`, as the
//! program's are.

#![deny(clippy::wildcard_enum_match_arm)]

use smallvec::SmallVec;

use super::leaf::{Bar, Class, Seam};
use super::plain::ValueData;
use super::program::{ArithOp, CompareOp, Label, Operand, Reg};
use crate::exec::ir::phys::ops::Batch;
use crate::val::{RecordIdKey, TableName};

/// An instruction form emitted by the IR's lowering.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct IrInstr(Box<IrForm>);

/// The forms of [`IrInstr`].
///
/// A form may stand unconstructed outside tests while no producer emits it. Each family's
/// emitter is gated on the producer named in its own docs.
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(super) enum IrForm {
	/// The per-element loop of an eligible site, run a batch at a time. It reads nothing and calls
	/// nothing, so a leaf runs it.
	Kernel(Box<KernelSpec>),
	/// Reads the keys of the positions a loop is about to visit in one call and keeps the records
	/// in the statement's read buffer. It raises nothing and writes no register.
	BatchRead(Box<BatchRead>),
	/// Marks loop site `site` as uncovered and clears its buffered records. It is emitted
	/// immediately before the loop's first instruction, so every entry to the loop starts
	/// uncovered.
	BatchReset {
		site: u32,
	},
	/// The position at which a `SkipRow` loop's caught failure is consumed.
	SkipRow,
	/// If cell [`MemoGet::cell`] holds a value, copies it into [`MemoGet::dst`] and continues at
	/// [`MemoGet::hit`]; otherwise falls through into the code that computes the value.
	MemoGet(Box<MemoGet>),
	/// Offers a computed value to a memo cell. The cell may refuse it, and the program's answer
	/// does not depend on that.
	MemoPut(Box<MemoPut>),
	/// Empties the cells of one `FOR` scope.
	MemoReset(Box<MemoReset>),
	/// For a memo consumed only through its truthiness: tests the kept value in place and
	/// continues at [`MemoTruthy::then`] or [`MemoTruthy::otherwise`], or falls through into the
	/// code that computes the value when the cell is empty.
	MemoTruthy(Box<MemoTruthy>),
	/// The first instruction of a write body: installs the pending candidate row, restores its
	/// live registers and continues at [`WriteResumeSpec::cut`] or [`WriteResumeSpec::skip`], or
	/// falls through when no candidate is pending.
	WriteResume(Box<WriteResumeSpec>),
	/// The end of the part of a write body that runs ahead of the ordered apply: the position at
	/// which the coordinator resumes. Where nothing runs ahead it is a no-op.
	WriteCut(Box<WriteCutSpec>),
	/// The end of a row that a write body skips, immediately before its `End { skipped: true }`.
	WriteSkip,
	/// The first instruction of an arm that is not the cut path: a row that reaches it is run
	/// from the start of the body on the coordinator.
	WriteOffPath,
}

/// What a batch kernel runs: the typed arm of one `array::map`, `array::filter` or element
/// filter site.
///
/// The instructions before the kernel are the site's own, so `items` and `dst` hold what they
/// left. The kernel visits the positions `0..len(items)` and ignores the loop's position,
/// element and index registers.
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(super) struct KernelSpec {
	/// The register holding the array whose elements the kernel visits.
	pub(super) items: Reg,
	/// The register holding the array or set the kernel appends to.
	pub(super) dst: Reg,
	/// How the kernel's output reaches `dst`.
	pub(super) sink: KernelSink,
	/// What each [`KernelOp::Input`] reads.
	pub(super) inputs: SmallVec<[KernelInput; 4]>,
	/// The body in the order the serial body evaluates it.
	pub(super) ops: SmallVec<[KernelOp; 16]>,
	/// The register whose value the sink consumes.
	pub(super) out: KReg,
	/// The representation of each kernel register, by register number.
	pub(super) regs: SmallVec<[KRegRepr; 16]>,
	/// The one column kind every element-derived input shares.
	pub(super) col: ColKind,
	/// The positions one run of the kernel covers.
	pub(super) batch: Batch,
	/// The labels of both arms of the site.
	pub(super) explain: KernelExplain,
}

/// How a kernel's output reaches its destination register.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(super) enum KernelSink {
	/// `array::map`: appends `out[p]` with `ArrayPush` semantics.
	Map,
	/// `array::filter` and an element filter: appends the element when `out[p]` is truthy. The
	/// append has `CollectPush` semantics when `collect` is set (the destination is an array or a
	/// set) and `ArrayPush` semantics otherwise.
	FilterElem {
		collect: bool,
	},
}

/// What a kernel input reads.
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(super) enum KernelInput {
	/// The element.
	Elem,
	/// The field of the element named by [`Program::fields`](super::program::Program::fields)
	/// at this index.
	ElemField(u32),
	/// A value the loop body does not change between positions.
	Invariant(Operand),
}

/// The kind of value a column holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(super) enum ColKind {
	Int,
	Float,
	Bool,
	Str,
}

/// How a kernel register stores its values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(super) enum KRegRepr {
	/// One unboxed value per position, of the column kind.
	Col(ColKind),
	/// One bit per position: whether the position is live.
	Mask,
	/// One `Value` per position.
	Boxed,
}

/// A kernel register number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(super) struct KReg(pub(super) u8);

/// One operation of a kernel body.
///
/// An operation runs only over the positions live in its `under` mask, and a position whose
/// operation fails is live in no later mask.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(super) enum KernelOp {
	/// Loads input number `input` into `dst`.
	Input {
		input: u8,
		dst: KReg,
	},
	Cmp {
		op: CompareOp,
		a: KReg,
		b: KReg,
		dst: KReg,
		under: KReg,
	},
	Arith {
		op: ArithOp,
		a: KReg,
		b: KReg,
		dst: KReg,
		under: KReg,
	},
	Neg {
		a: KReg,
		dst: KReg,
		under: KReg,
	},
	Not {
		a: KReg,
		dst: KReg,
		under: KReg,
	},
	/// The truthiness of `a`, as a mask.
	Truthy {
		a: KReg,
		dst: KReg,
		under: KReg,
	},
	MaskAnd {
		a: KReg,
		b: KReg,
		dst: KReg,
	},
	MaskAndNot {
		a: KReg,
		b: KReg,
		dst: KReg,
	},
	/// `dst[p] = if mask[p] { then[p] } else { else_[p] }`, the value of `AND`, `OR` and `IF`:
	/// `0 AND false` is `0`, the operand's own value.
	Blend {
		mask: KReg,
		then: KReg,
		else_: KReg,
		dst: KReg,
	},
}

/// The labels of both arms of a kernel site, for `EXPLAIN`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(super) struct KernelExplain {
	/// The label of the typed arm.
	pub(super) typed: Box<str>,
	/// The label of the generic arm.
	pub(super) generic: Box<str>,
}

/// A statement's or a loop's batched read.
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(super) struct BatchRead {
	/// The buffer partition the read fills.
	pub(super) scope: BufferScope,
	/// Where the keys of the positions ahead come from.
	pub(super) ahead: LookAhead,
	/// The keys read, one entry per site, in the order of the physical plan's prefetch.
	pub(super) keys: Box<[SiteKey]>,
	/// The most positions one execution covers.
	pub(super) max_positions: u32,
	/// The most bytes one execution buffers.
	pub(super) max_bytes: u32,
}

/// The read buffer partition a [`BatchRead`] fills. The site number is unique within one program.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(super) enum BufferScope {
	/// The partition of one loop, cleared by the [`IrForm::BatchReset`] naming the same site.
	Loop {
		site: u32,
	},
	/// The partition of a record list read at statement scope.
	Statement {
		site: u32,
	},
}

/// Where a [`BatchRead`] finds the keys of the positions ahead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(super) enum LookAhead {
	/// A `FOR` loop: the current element is in `elem`, and the elements after it are the
	/// remaining elements of iterator slot `slot`.
	Iter {
		slot: u16,
		elem: Reg,
	},
	/// An element filter: the array is in `array`, the current position in `index` and the
	/// current element in `elem`. Reserved: no lowering emits it.
	Elems {
		array: Reg,
		index: Reg,
		elem: Reg,
	},
	/// No positions ahead: the keys are literals.
	None,
}

/// One key a [`BatchRead`] reads.
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(super) enum SiteKey {
	/// The current element, when it is a record id that is not a range.
	Elem,
	/// Record ids written as literals.
	Literal(Box<[LiteralKey]>),
}

/// A record id written as a literal. Its key is held as [`ValueData`], as every program's value
/// is, so the program stays plain data.
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(super) struct LiteralKey {
	pub(super) table: TableName,
	pub(super) key: ValueData<RecordIdKey>,
}

/// The payload of [`IrForm::MemoGet`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(super) struct MemoGet {
	/// The memo cell read.
	pub(super) cell: u16,
	/// The register the kept value is copied into on a hit.
	pub(super) dst: Reg,
	/// Where execution continues on a hit, past the code that computes the value.
	pub(super) hit: Label,
}

/// The payload of [`IrForm::MemoPut`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(super) struct MemoPut {
	/// The memo cell offered the value.
	pub(super) cell: u16,
	/// The register holding the value offered.
	pub(super) src: Reg,
}

/// The payload of [`IrForm::MemoReset`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(super) struct MemoReset {
	/// The first cell emptied.
	pub(super) first: u16,
	/// How many cells, from `first`, are emptied.
	pub(super) count: u16,
}

/// The payload of [`IrForm::MemoTruthy`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(super) struct MemoTruthy {
	/// The memo cell tested.
	pub(super) cell: u16,
	/// Where execution continues when the kept value is truthy.
	pub(super) then: Label,
	/// Where execution continues when the kept value is not truthy.
	pub(super) otherwise: Label,
}

/// The payload of [`IrForm::WriteResume`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(super) struct WriteResumeSpec {
	/// Where a candidate that stopped at the cut continues.
	pub(super) cut: Label,
	/// Where a candidate that stopped at the skip continues.
	pub(super) skip: Label,
	/// The registers the part ahead of the cut wrote and the part after it reads.
	pub(super) live: SmallVec<[u16; 8]>,
}

/// The payload of [`IrForm::WriteCut`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(super) struct WriteCutSpec {
	/// The registers the part ahead of the cut wrote and the part after it reads.
	pub(super) live: SmallVec<[u16; 8]>,
	/// Whether the part ahead of the cut encodes the record's after-image for the part after it.
	pub(super) encode: bool,
}

impl IrInstr {
	/// Wraps `form`.
	#[allow(dead_code)]
	pub(super) fn new(form: IrForm) -> Self {
		Self(Box::new(form))
	}

	/// The form this instruction is.
	#[allow(dead_code)]
	pub(super) fn form(&self) -> &IrForm {
		&self.0
	}

	/// A [`IrForm::Kernel`].
	#[allow(dead_code)]
	pub(super) fn kernel(spec: KernelSpec) -> Self {
		Self::new(IrForm::Kernel(Box::new(spec)))
	}

	/// A [`IrForm::BatchRead`].
	#[allow(dead_code)]
	pub(super) fn batch_read(read: BatchRead) -> Self {
		Self::new(IrForm::BatchRead(Box::new(read)))
	}

	/// A [`IrForm::BatchReset`] of loop site `site`.
	#[allow(dead_code)]
	pub(super) fn batch_reset(site: u32) -> Self {
		Self::new(IrForm::BatchReset {
			site,
		})
	}

	/// A [`IrForm::SkipRow`].
	#[allow(dead_code)]
	pub(super) fn skip_row() -> Self {
		Self::new(IrForm::SkipRow)
	}

	/// A [`IrForm::MemoGet`].
	#[allow(dead_code)]
	pub(super) fn memo_get(get: MemoGet) -> Self {
		Self::new(IrForm::MemoGet(Box::new(get)))
	}

	/// A [`IrForm::MemoPut`].
	#[allow(dead_code)]
	pub(super) fn memo_put(put: MemoPut) -> Self {
		Self::new(IrForm::MemoPut(Box::new(put)))
	}

	/// A [`IrForm::MemoReset`].
	#[allow(dead_code)]
	pub(super) fn memo_reset(reset: MemoReset) -> Self {
		Self::new(IrForm::MemoReset(Box::new(reset)))
	}

	/// A [`IrForm::MemoTruthy`].
	#[allow(dead_code)]
	pub(super) fn memo_truthy(truthy: MemoTruthy) -> Self {
		Self::new(IrForm::MemoTruthy(Box::new(truthy)))
	}

	/// A [`IrForm::WriteResume`].
	#[allow(dead_code)]
	pub(super) fn write_resume(spec: WriteResumeSpec) -> Self {
		Self::new(IrForm::WriteResume(Box::new(spec)))
	}

	/// A [`IrForm::WriteCut`].
	#[allow(dead_code)]
	pub(super) fn write_cut(spec: WriteCutSpec) -> Self {
		Self::new(IrForm::WriteCut(Box::new(spec)))
	}

	/// A [`IrForm::WriteSkip`].
	#[allow(dead_code)]
	pub(super) fn write_skip() -> Self {
		Self::new(IrForm::WriteSkip)
	}

	/// A [`IrForm::WriteOffPath`].
	#[allow(dead_code)]
	pub(super) fn write_off_path() -> Self {
		Self::new(IrForm::WriteOffPath)
	}

	/// What a leaf makes of this instruction. Every form answers, and a form that cannot name a
	/// narrower class answers the class that keeps the item off the leaf.
	pub(super) fn leaf_class(&self) -> Class {
		match &*self.0 {
			// A kernel reads nothing and calls nothing: it computes over registers.
			IrForm::Kernel(_)
			| IrForm::BatchReset {
				..
			}
			| IrForm::SkipRow
			| IrForm::MemoGet(_)
			| IrForm::MemoPut(_)
			| IrForm::MemoReset(_)
			| IrForm::MemoTruthy(_) => Class::Leaf,
			// The one form that reaches the snapshot.
			IrForm::BatchRead(_) => Class::Kernel(&[Seam::Storage]),
			IrForm::WriteResume(_)
			| IrForm::WriteCut(_)
			| IrForm::WriteSkip
			| IrForm::WriteOffPath => Class::NonLeaf(Bar::Write),
		}
	}

	/// Every code position the instruction may continue at other than the next, for a pass that
	/// moves instructions to rewrite. A form with a jump target lists it here: a target left out
	/// goes stale when a pass moves code, and nothing else reports it.
	pub(super) fn labels_mut(&mut self) -> Vec<&mut Label> {
		match &mut *self.0 {
			IrForm::MemoGet(get) => vec![&mut get.hit],
			IrForm::MemoTruthy(truthy) => vec![&mut truthy.then, &mut truthy.otherwise],
			IrForm::WriteResume(spec) => vec![&mut spec.cut, &mut spec.skip],
			IrForm::Kernel(_)
			| IrForm::BatchRead(_)
			| IrForm::BatchReset {
				..
			}
			| IrForm::SkipRow
			| IrForm::MemoPut(_)
			| IrForm::MemoReset(_)
			| IrForm::WriteCut(_)
			| IrForm::WriteSkip
			| IrForm::WriteOffPath => Vec::new(),
		}
	}

	/// The failure of an instruction whose executor does not yet exist: an internal error naming
	/// the form, never a skipped instruction.
	pub(super) fn unexecuted(&self) -> anyhow::Error {
		crate::err::EngineError::Unreachable(format!("no executor for IR instruction {self:?}"))
			.into()
	}
}

// `Instr::Ir` holds an `IrInstr` inline, so no form may widen it past one pointer.
const _: () = assert!(::core::mem::size_of::<IrInstr>() <= ::core::mem::size_of::<usize>());

#[cfg(test)]
mod tests {
	use super::super::program::Instr;
	use super::*;

	/// `Instr` is the size it is without the IR's variant, so carrying an `IrInstr` costs the
	/// interpreter's instructions nothing. Pinned for 64-bit targets.
	#[test]
	fn instr_is_not_widened_by_the_ir_variant() {
		#[cfg(target_pointer_width = "64")]
		assert_eq!(::core::mem::size_of::<Instr>(), 104);
		#[cfg(not(target_pointer_width = "64"))]
		assert!(::core::mem::size_of::<Instr>() <= 104);
	}

	fn kernel() -> KernelSpec {
		KernelSpec {
			items: 1,
			dst: 2,
			sink: KernelSink::Map,
			inputs: SmallVec::new(),
			ops: SmallVec::new(),
			out: KReg(0),
			regs: SmallVec::new(),
			col: ColKind::Int,
			batch: Batch {
				max_iterations: 64,
				max_bytes: 4096,
			},
			explain: KernelExplain {
				typed: "typed".into(),
				generic: "generic".into(),
			},
		}
	}

	fn batch_read() -> BatchRead {
		BatchRead {
			scope: BufferScope::Loop {
				site: 0,
			},
			ahead: LookAhead::Iter {
				slot: 0,
				elem: 3,
			},
			keys: Box::new([SiteKey::Elem]),
			max_positions: 64,
			max_bytes: 4096,
		}
	}

	/// One instruction of every form, with the jump targets 10 and 11 where a form has them.
	fn every_form() -> Vec<IrInstr> {
		vec![
			IrInstr::kernel(kernel()),
			IrInstr::batch_read(batch_read()),
			IrInstr::batch_reset(0),
			IrInstr::skip_row(),
			IrInstr::memo_get(MemoGet {
				cell: 0,
				dst: 1,
				hit: 10,
			}),
			IrInstr::memo_put(MemoPut {
				cell: 0,
				src: 1,
			}),
			IrInstr::memo_reset(MemoReset {
				first: 0,
				count: 2,
			}),
			IrInstr::memo_truthy(MemoTruthy {
				cell: 0,
				then: 10,
				otherwise: 11,
			}),
			IrInstr::write_resume(WriteResumeSpec {
				cut: 10,
				skip: 11,
				live: SmallVec::from_slice(&[1, 2]),
			}),
			IrInstr::write_cut(WriteCutSpec {
				live: SmallVec::from_slice(&[1, 2]),
				encode: false,
			}),
			IrInstr::write_skip(),
			IrInstr::write_off_path(),
		]
	}

	/// Every form answers the class of its family: the kernel and the memos read nothing, a
	/// batched read needs storage, and the write forms are never a leaf's.
	#[test]
	fn every_form_has_a_leaf_class() {
		let classes: Vec<Class> = every_form().iter().map(IrInstr::leaf_class).collect();
		assert_eq!(
			classes,
			vec![
				Class::Leaf,
				Class::Kernel(&[Seam::Storage]),
				Class::Leaf,
				Class::Leaf,
				Class::Leaf,
				Class::Leaf,
				Class::Leaf,
				Class::Leaf,
				Class::NonLeaf(Bar::Write),
				Class::NonLeaf(Bar::Write),
				Class::NonLeaf(Bar::Write),
				Class::NonLeaf(Bar::Write),
			]
		);
	}

	/// A form lists every label it carries, in field order, and a form with none lists none.
	#[test]
	fn labels_mut_lists_every_target() {
		let targets: Vec<Vec<Label>> = every_form()
			.iter_mut()
			.map(|instr| instr.labels_mut().into_iter().map(|label| *label).collect())
			.collect();
		assert_eq!(
			targets,
			vec![
				vec![],
				vec![],
				vec![],
				vec![],
				vec![10],
				vec![],
				vec![],
				vec![10, 11],
				vec![10, 11],
				vec![],
				vec![],
				vec![],
			]
		);

		// A rewrite through the returned references reaches the form's own fields.
		let mut instr = IrInstr::write_resume(WriteResumeSpec {
			cut: 10,
			skip: 11,
			live: SmallVec::new(),
		});
		for label in instr.labels_mut() {
			*label += 5;
		}
		let IrForm::WriteResume(spec) = instr.form() else {
			panic!("the form changed variant");
		};
		assert_eq!((spec.cut, spec.skip), (15, 16));
	}

	/// The error of an unexecuted form names the form, so a missing executor is found from the
	/// failure alone.
	#[test]
	fn unexecuted_names_the_form() {
		for (instr, name) in every_form().iter().zip([
			"Kernel",
			"BatchRead",
			"BatchReset",
			"SkipRow",
			"MemoGet",
			"MemoPut",
			"MemoReset",
			"MemoTruthy",
			"WriteResume",
			"WriteCut",
			"WriteSkip",
			"WriteOffPath",
		]) {
			let text = format!("{:?}", instr.unexecuted());
			assert!(text.contains(name), "`{text}` does not name {name}");
		}
	}
}
