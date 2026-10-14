//! Batch kernels: the specification a `Kernel` instruction carries and the instruction sequences
//! that lead into it.
//!
//! What the physical plan fixed for a kernel site (head form, sink, column kind, batch bounds,
//! the eligible body and its inputs) is copied. The register representation of each kernel
//! register comes from a fixed table over the operation and its operands' representations, and
//! involves no cost or legality decision.
//!
//! # Head sequences
//!
//! [`emit_head`] emits the instructions that lead into one `Kernel`, in the order the loop
//! evaluates them, so every check, initialisation and charge for the site still runs before the
//! kernel does. It emits `ElemNext` and `Dive` as the loop head does, and never `ClosureBind` or
//! `ClosureReturn`, because the kernel replaces the loop body. The head ends at the label the
//! loop's exits jump to, which [`emit_head`] binds after the kernel.

#![deny(clippy::wildcard_enum_match_arm)]

use smallvec::SmallVec;

use super::super::ir_instr::{
	ColKind, IrInstr, KReg, KRegRepr, KernelExplain, KernelInput, KernelOp, KernelSink, KernelSpec,
};
use super::super::program::{ArithOp, Instr, Label, Operand, Reg};
use super::emit::Emitter;
use crate::exec::ir::error::IrError;
use crate::exec::ir::phys::ops::Batch;
use crate::val::Value;

/// The most element-derived inputs one kernel reads.
///
/// A second guard behind the physical plan's eligibility bound, never a decision of its own.
const KERNEL_MAX_INPUTS: usize = 4;

/// The most operations one kernel body holds.
///
/// A second guard behind the physical plan's eligibility bound, never a decision of its own.
const KERNEL_MAX_OPS: usize = 32;

/// One operation of an eligible kernel body, as the physical plan fixed it.
pub(super) type KernelBodyOp = KernelOp;

/// The instructions that lead into a kernel, by the site's shape.
///
/// No variant carries an operand evaluation: the operand the head reads is passed to
/// [`emit_head`], already evaluated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // The producer is request L10-5; until it lands only tests construct a head.
pub(super) enum KernelHead {
	/// `array::map` and `array::filter` written as a call: `call` is the index of the builtin's
	/// entry in the program's call pool.
	FunctionForm {
		call: u32,
	},
	/// `.map(..)` and `.filter(..)` written as a method call: `method` is the index of the
	/// method's entry in the program's method pool, `call` the index of the builtin's entry in the
	/// call pool, and `other` the label a receiver that is not an array continues at, where the
	/// caller emits the ordinary method call.
	MethodForm {
		method: u32,
		call: u32,
		other: Label,
	},
	/// An element filter, `array[WHERE ..]`.
	ElementFilter,
}

/// What the physical plan fixed for one kernel site.
///
/// Lowering copies every field.
#[allow(dead_code)] // The producer is request L10-5; until it lands only tests construct a site.
pub(super) struct KernelSite<'a> {
	/// The instructions that lead into the kernel.
	pub(super) head: KernelHead,
	/// How the kernel's output reaches the site's destination.
	pub(super) sink: KernelSink,
	/// The one column kind every element-derived input shares.
	pub(super) col: ColKind,
	/// The loop's batch bounds.
	pub(super) batch: Batch,
	/// The eligible body, in the order the serial body evaluates it. The last operation computes
	/// the value the sink consumes.
	pub(super) body: &'a [KernelBodyOp],
	/// What each [`KernelOp::Input`] reads.
	pub(super) inputs: &'a [KernelInput],
}

/// The registers `op` names, destination and operands alike.
fn registers(op: KernelOp) -> SmallVec<[KReg; 5]> {
	match op {
		KernelOp::Input {
			dst,
			..
		} => SmallVec::from_slice(&[dst]),
		KernelOp::Cmp {
			a,
			b,
			dst,
			under,
			..
		}
		| KernelOp::Arith {
			a,
			b,
			dst,
			under,
			..
		} => SmallVec::from_slice(&[a, b, dst, under]),
		KernelOp::Neg {
			a,
			dst,
			under,
		}
		| KernelOp::Not {
			a,
			dst,
			under,
		}
		| KernelOp::Truthy {
			a,
			dst,
			under,
		} => SmallVec::from_slice(&[a, dst, under]),
		KernelOp::MaskAnd {
			a,
			b,
			dst,
		}
		| KernelOp::MaskAndNot {
			a,
			b,
			dst,
		} => SmallVec::from_slice(&[a, b, dst]),
		KernelOp::Blend {
			mask,
			then,
			else_,
			dst,
		} => SmallVec::from_slice(&[mask, then, else_, dst]),
	}
}

/// The register `op` writes.
fn destination(op: KernelOp) -> KReg {
	match op {
		KernelOp::Input {
			dst,
			..
		}
		| KernelOp::Cmp {
			dst,
			..
		}
		| KernelOp::Arith {
			dst,
			..
		}
		| KernelOp::Neg {
			dst,
			..
		}
		| KernelOp::Not {
			dst,
			..
		}
		| KernelOp::Truthy {
			dst,
			..
		}
		| KernelOp::MaskAnd {
			dst,
			..
		}
		| KernelOp::MaskAndNot {
			dst,
			..
		}
		| KernelOp::Blend {
			dst,
			..
		} => dst,
	}
}

/// The representation `reg` holds, `Boxed` for a register no operation has written.
fn held(reprs: &[Option<KRegRepr>], reg: KReg) -> KRegRepr {
	reprs.get(reg.0 as usize).copied().flatten().unwrap_or(KRegRepr::Boxed)
}

/// The representation of the result of `op` over operands of representations `a` and `b`.
///
/// An integer pair keeps the column for the operators that answer an integer or raise, and `/`
/// answers a float for a zero divisor, so it is `Boxed`. A float pair keeps the column. Every
/// other pair, and every operator with no column form, is `Boxed`.
fn arith_repr(op: ArithOp, a: KRegRepr, b: KRegRepr) -> KRegRepr {
	match (a, b) {
		(KRegRepr::Col(ColKind::Int), KRegRepr::Col(ColKind::Int)) => match op {
			ArithOp::Add | ArithOp::Subtract | ArithOp::Multiply | ArithOp::Remainder => {
				KRegRepr::Col(ColKind::Int)
			}
			ArithOp::Divide
			| ArithOp::Power
			| ArithOp::Range {
				..
			} => KRegRepr::Boxed,
		},
		(KRegRepr::Col(ColKind::Float), KRegRepr::Col(ColKind::Float)) => match op {
			ArithOp::Add
			| ArithOp::Subtract
			| ArithOp::Multiply
			| ArithOp::Divide
			| ArithOp::Remainder => KRegRepr::Col(ColKind::Float),
			ArithOp::Power
			| ArithOp::Range {
				..
			} => KRegRepr::Boxed,
		},
		_ => KRegRepr::Boxed,
	}
}

/// The representation of every kernel register of a typed kernel, by register number.
///
/// The table, row by row:
/// - an `Input` of the element or of one of its fields is a column of `col`, and an `Invariant`
///   input is `Boxed`;
/// - a comparison and `Not` are a boolean column;
/// - integer `+ - * %` is an integer column and float arithmetic a float column, while integer `/`
///   and every other operand pair are `Boxed`;
/// - `Neg` of a column is that column, and of anything else `Boxed`;
/// - `Truthy`, `MaskAnd` and `MaskAndNot` are masks;
/// - `Blend` is a column only when both arms are the same column, and `Boxed` otherwise;
/// - any other operation with a `Boxed` operand is `Boxed`.
///
/// A register no operation writes is a mask where an operation reads it as one (the live mask a
/// body starts under) and `Boxed` otherwise, so the answer is always a representation the kernel
/// can hold.
#[allow(dead_code)] // The producer is request L10-5; until it lands only tests call it.
pub(super) fn regs_repr(
	ops: &[KernelOp],
	inputs: &[KernelInput],
	col: ColKind,
) -> SmallVec<[KRegRepr; 16]> {
	let count = ops.iter().copied().flat_map(registers).map(|reg| reg.0 as usize + 1).max();
	let count = count.unwrap_or(0);
	let mut reprs: Vec<Option<KRegRepr>> = vec![None; count];
	let mut masks = vec![false; count];
	for op in ops {
		let repr = match *op {
			KernelOp::Input {
				input,
				..
			} => match inputs.get(input as usize) {
				Some(KernelInput::Elem | KernelInput::ElemField(_)) => KRegRepr::Col(col),
				Some(KernelInput::Invariant(_)) | None => KRegRepr::Boxed,
			},
			KernelOp::Cmp {
				a,
				b,
				under,
				..
			} => {
				masks[under.0 as usize] = true;
				if held(&reprs, a) == KRegRepr::Boxed || held(&reprs, b) == KRegRepr::Boxed {
					KRegRepr::Boxed
				} else {
					KRegRepr::Col(ColKind::Bool)
				}
			}
			KernelOp::Arith {
				op,
				a,
				b,
				under,
				..
			} => {
				masks[under.0 as usize] = true;
				arith_repr(op, held(&reprs, a), held(&reprs, b))
			}
			KernelOp::Neg {
				a,
				under,
				..
			} => {
				masks[under.0 as usize] = true;
				match held(&reprs, a) {
					column @ KRegRepr::Col(ColKind::Int | ColKind::Float) => column,
					KRegRepr::Col(ColKind::Bool | ColKind::Str)
					| KRegRepr::Mask
					| KRegRepr::Boxed => KRegRepr::Boxed,
				}
			}
			KernelOp::Not {
				a,
				under,
				..
			} => {
				masks[under.0 as usize] = true;
				if held(&reprs, a) == KRegRepr::Boxed {
					KRegRepr::Boxed
				} else {
					KRegRepr::Col(ColKind::Bool)
				}
			}
			KernelOp::Truthy {
				under,
				..
			} => {
				masks[under.0 as usize] = true;
				KRegRepr::Mask
			}
			KernelOp::MaskAnd {
				a,
				b,
				..
			}
			| KernelOp::MaskAndNot {
				a,
				b,
				..
			} => {
				masks[a.0 as usize] = true;
				masks[b.0 as usize] = true;
				KRegRepr::Mask
			}
			KernelOp::Blend {
				mask,
				then,
				else_,
				..
			} => {
				masks[mask.0 as usize] = true;
				match (held(&reprs, then), held(&reprs, else_)) {
					(KRegRepr::Col(a), KRegRepr::Col(b)) if a == b => KRegRepr::Col(a),
					_ => KRegRepr::Boxed,
				}
			}
		};
		reprs[destination(*op).0 as usize] = Some(repr);
	}
	reprs
		.into_iter()
		.zip(masks)
		.map(|(repr, mask)| match (repr, mask) {
			(Some(repr), _) => repr,
			(None, true) => KRegRepr::Mask,
			(None, false) => KRegRepr::Boxed,
		})
		.collect()
}

/// The register holding the value the sink consumes, after the shape of `site` is checked.
///
/// A site beyond the kernel bounds, an empty body and an `Input` naming an input the site does not
/// hold answer [`IrError::Invariant`]: the physical plan's eligibility check admits none of them.
fn checked_out(site: &KernelSite<'_>) -> Result<KReg, IrError> {
	if site.inputs.len() > KERNEL_MAX_INPUTS || site.body.len() > KERNEL_MAX_OPS {
		return Err(IrError::Invariant {
			what: "a kernel site beyond the kernel bounds",
		});
	}
	let Some(last) = site.body.last() else {
		return Err(IrError::Invariant {
			what: "a kernel site with an empty body",
		});
	};
	for op in site.body {
		if let KernelOp::Input {
			input,
			..
		} = *op && usize::from(input) >= site.inputs.len()
		{
			return Err(IrError::Invariant {
				what: "a kernel body reads an input its site does not hold",
			});
		}
	}
	Ok(destination(*last))
}

/// The labels of both arms of a site, for `EXPLAIN`.
fn explain(site: &KernelSite<'_>) -> KernelExplain {
	let sink = match site.sink {
		KernelSink::Map => "map",
		KernelSink::FilterElem {
			..
		} => "filter",
	};
	let col = match site.col {
		ColKind::Int => "int",
		ColKind::Float => "float",
		ColKind::Bool => "bool",
		ColKind::Str => "str",
	};
	KernelExplain {
		typed: format!("typed kernel over {col} columns ({sink})").into_boxed_str(),
		generic: format!("generic per-element loop ({sink})").into_boxed_str(),
	}
}

/// The specification of the kernel at `site`, over the array in `items` and the collection in
/// `dst`.
///
/// Everything the physical plan fixed is copied; the register representations come from
/// [`regs_repr`] and the output register is the destination of the body's last operation.
///
/// A site beyond [`KERNEL_MAX_INPUTS`] inputs or [`KERNEL_MAX_OPS`] operations answers
/// [`IrError::Invariant`]: the physical plan's eligibility bound is the first guard.
#[allow(dead_code)] // The producer is request L10-5; until it lands only tests call it.
pub(super) fn kernel_spec(
	site: &KernelSite<'_>,
	items: Reg,
	dst: Reg,
) -> Result<KernelSpec, IrError> {
	let out = checked_out(site)?;
	Ok(KernelSpec {
		items,
		dst,
		sink: site.sink,
		inputs: site.inputs.iter().cloned().collect(),
		ops: site.body.iter().copied().collect(),
		out,
		regs: regs_repr(site.body, site.inputs, site.col),
		col: site.col,
		batch: site.batch,
		explain: explain(site),
	})
}

/// The loop head emitted after a collection is initialised: the result array, the position
/// at zero and the first element, then the charge for the re-entry the invocations take.
fn loop_head(em: &mut Emitter, items: Reg, dst: Reg, end: Label) -> Result<(), IrError> {
	em.emit(Instr::NewArray {
		dst,
	});
	let pos = em.reg()?;
	let zero = em.constant(Value::from(0i64))?;
	em.emit(Instr::Load {
		dst: pos,
		src: Operand::Const(zero),
	});
	let elem = em.reg()?;
	let index = em.reg()?;
	em.emit(Instr::ElemNext {
		array: items,
		pos,
		elem,
		index,
		done: end,
	});
	em.emit(Instr::Dive {
		frame: None,
	});
	Ok(())
}

/// Emits the head of the kernel at `site`, then the `Kernel` instruction, then binds the label
/// the loop's exits continue at. Answers the register that holds the site's result.
///
/// `src` is the already evaluated operand the head reads: the array of a function form, the
/// receiver of a method form and the filtered value of an element filter. The sequences are,
/// in order:
/// - function form: `CallAllowed`, `ArrayArg`, `NewArray`, a load of position zero, `ElemNext` and
///   `Dive`;
/// - method form: a load of the receiver, `MapBegin`, `CallAllowed`, `MethodRegion`, then the
///   function form's loop head. The caller emits what follows the bound label: `TryEnd`, the jump
///   past the other path, the ordinary method call at `other` and the label that jump targets;
/// - element filter: `FilterBegin` alone.
///
/// A site that [`kernel_spec`] refuses emits nothing.
#[allow(dead_code)] // The producer is request L10-5; until it lands only tests call it.
pub(super) fn emit_head(
	em: &mut Emitter,
	site: &KernelSite<'_>,
	src: Operand,
) -> Result<Reg, IrError> {
	checked_out(site)?;
	let items = em.reg()?;
	let dst = em.reg()?;
	let spec = kernel_spec(site, items, dst)?;
	let end = em.label()?;
	match site.head {
		KernelHead::FunctionForm {
			call,
		} => {
			em.emit(Instr::CallAllowed {
				call,
				frame: None,
			});
			em.emit(Instr::ArrayArg {
				src,
				dst: items,
				call,
				position: 1,
			});
			loop_head(em, items, dst, end)?;
		}
		KernelHead::MethodForm {
			method,
			call,
			other,
		} => {
			let held = em.reg()?;
			let scratch = em.reg()?;
			em.emit(Instr::Load {
				dst: held,
				src,
			});
			em.emit(Instr::MapBegin {
				src: Operand::Reg(held),
				items,
				dst: scratch,
				other,
				sets: false,
			});
			em.emit(Instr::CallAllowed {
				call,
				frame: None,
			});
			em.emit(Instr::MethodRegion {
				method,
			});
			loop_head(em, items, dst, end)?;
		}
		KernelHead::ElementFilter => {
			em.emit(Instr::FilterBegin {
				src,
				items,
				dst,
				none: end,
			});
		}
	}
	em.emit(Instr::Ir(IrInstr::kernel(spec)));
	em.bind(end)?;
	Ok(dst)
}

#[cfg(test)]
mod tests {
	use super::super::super::ir_instr::IrForm;
	use super::super::super::program::{CompareOp, Source};
	use super::*;

	const BATCH: Batch = Batch {
		max_iterations: 1024,
		max_bytes: 65536,
	};

	fn k(n: u8) -> KReg {
		KReg(n)
	}

	fn input(input: u8, dst: u8) -> KernelOp {
		KernelOp::Input {
			input,
			dst: k(dst),
		}
	}

	fn arith(op: ArithOp, a: u8, b: u8, dst: u8) -> KernelOp {
		KernelOp::Arith {
			op,
			a: k(a),
			b: k(b),
			dst: k(dst),
			under: k(0),
		}
	}

	/// The representations of `ops` over an element input at slot 0 and an invariant at slot 1.
	fn repr_of(ops: &[KernelOp], col: ColKind) -> SmallVec<[KRegRepr; 16]> {
		let inputs = [KernelInput::Elem, KernelInput::Invariant(Operand::Const(0))];
		regs_repr(ops, &inputs, col)
	}

	fn site<'a>(
		head: KernelHead,
		body: &'a [KernelBodyOp],
		inputs: &'a [KernelInput],
	) -> KernelSite<'a> {
		KernelSite {
			head,
			sink: KernelSink::Map,
			col: ColKind::Int,
			batch: BATCH,
			body,
			inputs,
		}
	}

	/// `$v + 1`-shaped body over one element input: live mask in register 0.
	fn small_body() -> Vec<KernelOp> {
		vec![input(0, 1), input(1, 2), arith(ArithOp::Add, 1, 2, 3)]
	}

	fn inputs() -> Vec<KernelInput> {
		vec![KernelInput::Elem, KernelInput::Invariant(Operand::Const(0))]
	}

	#[test]
	fn kernel_regs_repr() {
		use KRegRepr::{Boxed, Col, Mask};
		// Elem inputs are columns of the shared kind; an invariant is boxed.
		let ops = [input(0, 1), input(1, 2)];
		assert_eq!(&repr_of(&ops, ColKind::Int)[1..], &[Col(ColKind::Int), Boxed]);
		// Integer + - * % keep the column; `/` is boxed.
		for op in [ArithOp::Add, ArithOp::Subtract, ArithOp::Multiply, ArithOp::Remainder] {
			let ops = [input(0, 1), input(0, 2), arith(op, 1, 2, 3)];
			assert_eq!(repr_of(&ops, ColKind::Int)[3], Col(ColKind::Int));
		}
		let ops = [input(0, 1), input(0, 2), arith(ArithOp::Divide, 1, 2, 3)];
		assert_eq!(repr_of(&ops, ColKind::Int)[3], Boxed);
		// Float arithmetic keeps the column, a mixed pair does not.
		let ops = [input(0, 1), input(0, 2), arith(ArithOp::Divide, 1, 2, 3)];
		assert_eq!(repr_of(&ops, ColKind::Float)[3], Col(ColKind::Float));
		// An invariant operand makes the result boxed.
		let ops = [input(0, 1), input(1, 2), arith(ArithOp::Add, 1, 2, 3)];
		assert_eq!(repr_of(&ops, ColKind::Int)[3], Boxed);
		// A comparison is a boolean column, and boxed over a boxed operand.
		let cmp = |b| KernelOp::Cmp {
			op: CompareOp::LessThan,
			a: k(1),
			b: k(b),
			dst: k(3),
			under: k(0),
		};
		let ops = [input(0, 1), input(0, 2), cmp(2)];
		assert_eq!(repr_of(&ops, ColKind::Str)[3], Col(ColKind::Bool));
		let ops = [input(0, 1), input(1, 2), cmp(2)];
		assert_eq!(repr_of(&ops, ColKind::Str)[3], Boxed);
		// Truthy and the mask operations are masks.
		let ops = [
			input(0, 1),
			KernelOp::Truthy {
				a: k(1),
				dst: k(2),
				under: k(0),
			},
			KernelOp::MaskAnd {
				a: k(2),
				b: k(0),
				dst: k(3),
			},
			KernelOp::MaskAndNot {
				a: k(3),
				b: k(2),
				dst: k(4),
			},
		];
		let reprs = repr_of(&ops, ColKind::Int);
		assert_eq!(&reprs[2..], &[Mask, Mask, Mask]);
		// Neg keeps an int or float column.
		let neg = KernelOp::Neg {
			a: k(1),
			dst: k(2),
			under: k(0),
		};
		assert_eq!(repr_of(&[input(0, 1), neg], ColKind::Int)[2], Col(ColKind::Int));
		assert_eq!(repr_of(&[input(0, 1), neg], ColKind::Str)[2], Boxed);
		// Not is a boolean column.
		let not = KernelOp::Not {
			a: k(1),
			dst: k(2),
			under: k(0),
		};
		assert_eq!(repr_of(&[input(0, 1), not], ColKind::Bool)[2], Col(ColKind::Bool));
		// Blend is a column only when both arms are the same column.
		let blend = |then, else_| KernelOp::Blend {
			mask: k(0),
			then: k(then),
			else_: k(else_),
			dst: k(5),
		};
		let same = [input(0, 1), input(0, 2), blend(1, 2)];
		assert_eq!(repr_of(&same, ColKind::Int)[5], Col(ColKind::Int));
		let cmp_arm = KernelOp::Cmp {
			op: CompareOp::MoreThan,
			a: k(1),
			b: k(2),
			dst: k(3),
			under: k(0),
		};
		let mixed = [input(0, 1), input(0, 2), cmp_arm, blend(1, 3)];
		assert_eq!(repr_of(&mixed, ColKind::Int)[5], Boxed);
		// Anything with a boxed operand is boxed.
		let boxed = [input(1, 1), input(0, 2), arith(ArithOp::Add, 2, 1, 3)];
		assert_eq!(repr_of(&boxed, ColKind::Int)[3], Boxed);
	}

	#[test]
	fn kernel_lowering_head() {
		let body = small_body();
		let inputs = inputs();
		let mut em = Emitter::new();
		let src = Operand::Reg(em.reg().unwrap());
		let call = em.call(call_spec("array::map")).unwrap();
		let head = site(
			KernelHead::FunctionForm {
				call,
			},
			&body,
			&inputs,
		);
		let dst = emit_head(&mut em, &head, src).unwrap();
		em.emit(Instr::EndRow);
		let code = em.finish_pipeline(Source::Once).unwrap().code;
		assert!(matches!(
			code[..],
			[
				Instr::CallAllowed {
					frame: None,
					..
				},
				Instr::ArrayArg {
					position: 1,
					..
				},
				Instr::NewArray { .. },
				Instr::Load { .. },
				Instr::ElemNext { .. },
				Instr::Dive {
					frame: None
				},
				Instr::Ir(_),
				Instr::EndRow,
			]
		));
		// The loop's exit is the instruction after the kernel.
		let Instr::ElemNext {
			done,
			..
		} = code[4]
		else {
			unreachable!()
		};
		assert_eq!(done as usize, 7);
		assert!(matches!(code[2], Instr::NewArray { dst: d } if d == dst));
		let Instr::Ir(kernel) = &code[6] else {
			unreachable!()
		};
		let IrForm::Kernel(spec) = kernel.form() else {
			panic!("the head ends in a kernel")
		};
		assert_eq!(spec.dst, dst);
		assert_eq!(spec.out, k(3));

		// The method form.
		let mut em = Emitter::new();
		let recv = Operand::Reg(em.reg().unwrap());
		let other = em.label().unwrap();
		let call = em.call(call_spec("array::map")).unwrap();
		let head = site(
			KernelHead::MethodForm {
				method: 0,
				call,
				other,
			},
			&body,
			&inputs,
		);
		emit_head(&mut em, &head, recv).unwrap();
		em.bind(other).unwrap();
		em.emit(Instr::EndRow);
		let code = em.finish_pipeline(Source::Once).unwrap().code;
		assert!(matches!(
			code[..],
			[
				Instr::Load { .. },
				Instr::MapBegin { .. },
				Instr::CallAllowed {
					frame: None,
					..
				},
				Instr::MethodRegion {
					method: 0
				},
				Instr::NewArray { .. },
				Instr::Load { .. },
				Instr::ElemNext { .. },
				Instr::Dive {
					frame: None
				},
				Instr::Ir(_),
				Instr::EndRow,
			]
		));
		let Instr::MapBegin {
			other: target,
			..
		} = code[1]
		else {
			unreachable!()
		};
		assert_eq!(target as usize, 9);

		// The element filter emits its begin and nothing else before the kernel.
		let mut em = Emitter::new();
		let src = Operand::Reg(em.reg().unwrap());
		let mut head = site(KernelHead::ElementFilter, &body, &inputs);
		head.sink = KernelSink::FilterElem {
			collect: true,
		};
		emit_head(&mut em, &head, src).unwrap();
		em.emit(Instr::EndRow);
		let code = em.finish_pipeline(Source::Once).unwrap().code;
		assert!(matches!(code[..], [Instr::FilterBegin { .. }, Instr::Ir(_), Instr::EndRow]));
		let Instr::FilterBegin {
			none,
			..
		} = code[0]
		else {
			unreachable!()
		};
		assert_eq!(none as usize, 2);
	}

	#[test]
	fn kernel_spec_bounds() {
		let body = small_body();
		let inputs = inputs();
		let refused = |result: Result<KernelSpec, IrError>| {
			matches!(
				result,
				Err(IrError::Invariant {
					what: "a kernel site beyond the kernel bounds"
				})
			)
		};
		// The bounds themselves are accepted.
		let exact_ops: Vec<KernelOp> = std::iter::once(input(0, 1))
			.chain((0..31).map(|_| arith(ArithOp::Add, 1, 1, 2)))
			.collect();
		assert_eq!(exact_ops.len(), KERNEL_MAX_OPS);
		let exact_inputs = vec![KernelInput::Elem; KERNEL_MAX_INPUTS];
		assert!(
			kernel_spec(&site(KernelHead::ElementFilter, &exact_ops, &exact_inputs), 1, 2).is_ok()
		);
		// Five inputs.
		let many_inputs = vec![KernelInput::Elem; KERNEL_MAX_INPUTS + 1];
		assert!(refused(kernel_spec(&site(KernelHead::ElementFilter, &body, &many_inputs), 1, 2)));
		// Thirty-three operations.
		let many_ops: Vec<KernelOp> = std::iter::once(input(0, 1))
			.chain((0..KERNEL_MAX_OPS).map(|_| arith(ArithOp::Add, 1, 1, 2)))
			.collect();
		assert_eq!(many_ops.len(), KERNEL_MAX_OPS + 1);
		assert!(refused(kernel_spec(&site(KernelHead::ElementFilter, &many_ops, &inputs), 1, 2)));
		// A refused site emits nothing.
		let mut em = Emitter::new();
		let src = Operand::Reg(em.reg().unwrap());
		assert!(
			emit_head(&mut em, &site(KernelHead::ElementFilter, &many_ops, &inputs), src).is_err()
		);
		assert!(em.code().is_empty());
	}

	#[test]
	fn a_kernel_site_the_eligibility_check_refuses_is_an_invariant() {
		let inputs = inputs();
		// An empty body.
		assert!(matches!(
			kernel_spec(&site(KernelHead::ElementFilter, &[], &inputs), 1, 2),
			Err(IrError::Invariant {
				what: "a kernel site with an empty body"
			})
		));
		// An input the site does not hold.
		let body = vec![input(5, 1)];
		assert!(matches!(
			kernel_spec(&site(KernelHead::ElementFilter, &body, &inputs), 1, 2),
			Err(IrError::Invariant {
				what: "a kernel body reads an input its site does not hold"
			})
		));
	}

	#[test]
	fn kernel_explain_lists_both_arms() {
		let body = small_body();
		let inputs = inputs();
		let spec = kernel_spec(&site(KernelHead::ElementFilter, &body, &inputs), 1, 2).unwrap();
		assert!(spec.explain.typed.contains("typed kernel"));
		assert!(spec.explain.typed.contains("int"));
		assert!(spec.explain.generic.contains("generic"));
		assert_ne!(spec.explain.typed, spec.explain.generic);
		// The batch and the representations are copied with the labels.
		assert_eq!(spec.batch, BATCH);
		assert_eq!(spec.regs.len(), 4);
	}

	fn call_spec(name: &str) -> super::super::super::program::CallSpec {
		super::super::super::program::CallSpec {
			name: name.to_owned(),
			plan_depth: 0,
			index: None,
			closure_args: Vec::new(),
		}
	}
}
