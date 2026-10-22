//! Memo cells: the per-execution cache of a hoisted, row-independent value.
//!
//! A cell is numbered per program in the order its site first appears, and a site that appears
//! again keeps its cell. A site is lowered to a read of its cell that jumps past the computation
//! on a hit, the computation, and a write of its cell.
//!
//! # Shapes
//!
//! - A value site: `MemoGet { cell, dst, hit }`, the code that computes the value into `dst`,
//!   `MemoPut { cell, src }`, and `hit` bound. The value is in `dst` on either path.
//! - A truthy-only site, consumed only through its truthiness: `MemoTruthy { cell, then, otherwise
//!   }` tests the kept value in place and copies nothing. When the cell is empty it falls through
//!   into the computation, then `MemoPut`, then a re-test of the value that jumps to `then` or to
//!   `otherwise`. The consumer binds `then` and `otherwise` at its own two continuations.
//!
//! # Scopes
//!
//! A [`MemoScope::For`] cell is emptied by one `MemoReset` that the caller places at the loop's
//! entry, before its first iteration, over the cells numbered since the loop was entered. A
//! [`MemoScope::SelectGroup`] cell emits no reset: its owning group is recorded in
//! [`Cells::owner`], and the host empties it when the group starts.

use std::collections::HashMap;

use super::emit::Emitter;
use crate::exec::ir::error::IrError;
use crate::exec::vm::ir_instr::{IrInstr, MemoGet, MemoPut, MemoReset, MemoTruthy};
use crate::exec::vm::program::{Instr, Label, Operand, Reg};

/// Where a memo cell's lifetime ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MemoScope {
	/// The cell is cleared when a `FOR` loop is entered.
	For,
	/// The cell is cleared by the host when the numbered group starts.
	SelectGroup(u16),
}

impl MemoScope {
	/// The group that owns a cell of this scope, `None` for a cell no group owns.
	fn owner(self) -> Option<u16> {
		match self {
			Self::For => None,
			Self::SelectGroup(group) => Some(group),
		}
	}
}

/// One memo site, as the logical plan fixed it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct MemoSite {
	/// The site's identity within the program. Two openings of one id share a cell.
	pub(super) id: u32,
	pub(super) scope: MemoScope,
	/// Whether the value is consumed only through its truthiness.
	pub(super) truthy_only: bool,
}

/// The cell numbering of one program: the next cell number, and the owner group of each cell.
#[derive(Debug, Default)]
pub(super) struct Cells {
	next: u16,
	/// The cell of each site id seen so far.
	ids: HashMap<u32, u16>,
	/// The owning group of each cell, indexed by cell number.
	owner: Vec<Option<u16>>,
}

#[allow(dead_code)] // Called by the lowering of a memo site, which no producer builds yet.
impl Cells {
	/// A numbering that has handed out no cell.
	pub(super) fn new() -> Self {
		Self::default()
	}

	/// How many cells have been numbered. It is also the number the next new site takes.
	pub(super) fn count(&self) -> u16 {
		self.next
	}

	/// The owning group of each cell, indexed by cell number: `Some` for a cell of a select
	/// group's scope and `None` for any other.
	pub(super) fn owner(&self) -> &[Option<u16>] {
		&self.owner
	}

	/// The cell of `site`, numbered on the site's first appearance.
	///
	/// A site that appears again with another owning group than its first appearance gave it
	/// answers [`IrError::Stale`]. A numbering with no cell number left answers
	/// [`IrError::Capacity`] and is left as it was.
	fn cell_of(&mut self, site: &MemoSite) -> Result<u16, IrError> {
		let owner = site.scope.owner();
		if let Some(&cell) = self.ids.get(&site.id) {
			return if self.owner[cell as usize] == owner {
				Ok(cell)
			} else {
				Err(IrError::Stale("memo::open: a site changed its owning group"))
			};
		}
		let cell = self.next;
		self.next = cell.checked_add(1).ok_or(IrError::Capacity("memo cells"))?;
		self.ids.insert(site.id, cell);
		self.owner.push(owner);
		Ok(cell)
	}
}

/// The open half of a memo site: the cell, the register a hit leaves the value in, and the labels
/// a hit jumps to.
#[derive(Debug)]
pub(super) struct MemoOpen {
	cell: u16,
	/// The register `MemoGet` copies a hit into. A truthy-only site copies nothing and has none.
	dst: Option<Reg>,
	/// Where a hit continues: past the computation for a value site, the consumer's truthy
	/// continuation for a truthy-only site.
	hit: Label,
	/// The consumer's falsy continuation, set for a truthy-only site only.
	otherwise: Option<Label>,
}

#[allow(dead_code)] // Called by the lowering of a memo site, which no producer builds yet.
impl MemoOpen {
	/// The memo cell of the site.
	pub(super) fn cell(&self) -> u16 {
		self.cell
	}

	/// The register that holds the value after a value site's `close`, on a hit and on a miss
	/// alike: `close` copies the computed value into it. `None` for a truthy-only site.
	pub(super) fn dst(&self) -> Option<Reg> {
		self.dst
	}

	/// The label a hit continues at. For a truthy-only site the caller binds it at the consumer's
	/// truthy continuation; for a value site [`close`] binds it.
	pub(super) fn hit(&self) -> Label {
		self.hit
	}

	/// The consumer's falsy continuation of a truthy-only site, which the caller binds.
	pub(super) fn otherwise(&self) -> Option<Label> {
		self.otherwise
	}
}

/// Opens `site`, emitting its cell read (or its truthiness test when the site is truthy-only).
///
/// The code that computes the site's value follows, and [`close`] ends the site. A value site's
/// computation may leave its value in any register: `close` copies it into [`MemoOpen::dst`] so
/// that both paths agree on the register.
pub(super) fn open(
	cells: &mut Cells,
	em: &mut Emitter,
	site: &MemoSite,
) -> Result<MemoOpen, IrError> {
	let cell = cells.cell_of(site)?;
	let hit = em.label()?;
	if site.truthy_only {
		let otherwise = em.label()?;
		em.emit(Instr::Ir(IrInstr::memo_truthy(MemoTruthy {
			cell,
			then: hit,
			otherwise,
		})));
		return Ok(MemoOpen {
			cell,
			dst: None,
			hit,
			otherwise: Some(otherwise),
		});
	}
	let dst = em.reg()?;
	em.emit(Instr::Ir(IrInstr::memo_get(MemoGet {
		cell,
		dst,
		hit,
	})));
	Ok(MemoOpen {
		cell,
		dst: Some(dst),
		hit,
		otherwise: None,
	})
}

/// Closes the site `open` opened, after the code that computed `value`.
///
/// Offers `value` to the cell. A value site first copies `value` into [`MemoOpen::dst`] when it
/// is another register, so a hit and a miss both leave the value there, and then binds the hit
/// label, so a hit skips the computation, the copy and the offer. A truthy-only site re-tests
/// `value` and jumps to the consumer's continuation, which the caller binds: to [`MemoOpen::hit`]
/// when `value` is truthy and to [`MemoOpen::otherwise`] when it is not.
pub(super) fn close(em: &mut Emitter, open: &MemoOpen, value: Reg) -> Result<(), IrError> {
	if let Some(dst) = open.dst
		&& dst != value
	{
		em.emit(Instr::Load {
			dst,
			src: Operand::Reg(value),
		});
	}
	em.emit(Instr::Ir(IrInstr::memo_put(MemoPut {
		cell: open.cell,
		src: value,
	})));
	let Some(otherwise) = open.otherwise else {
		return em.bind(open.hit);
	};
	em.emit(Instr::JumpIf {
		cond: Operand::Reg(value),
		truthy: true,
		target: open.hit,
	});
	em.emit(Instr::Jump {
		target: otherwise,
	});
	Ok(())
}

/// Emits the `MemoReset` of a `FOR` loop's entry over the `count` cells from `first`.
///
/// No instruction is emitted when `count` is zero.
pub(super) fn reset_for_scope(em: &mut Emitter, first: u16, count: u16) {
	if count == 0 {
		return;
	}
	em.emit(Instr::Ir(IrInstr::memo_reset(MemoReset {
		first,
		count,
	})));
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::vm::ir_instr::IrForm;

	fn site(id: u32, scope: MemoScope, truthy_only: bool) -> MemoSite {
		MemoSite {
			id,
			scope,
			truthy_only,
		}
	}

	/// The forms among the code emitted so far, with other instructions as `None`.
	fn forms(em: &Emitter) -> Vec<Option<&IrForm>> {
		em.code()
			.iter()
			.map(|instr| {
				if let Instr::Ir(ir) = instr {
					Some(ir.form())
				} else {
					None
				}
			})
			.collect()
	}

	#[test]
	fn memo_lowering_shape() {
		let mut em = Emitter::new();
		let mut cells = Cells::new();

		// A value site: get, region, put, with the hit bound after the put.
		let open_value = open(&mut cells, &mut em, &site(7, MemoScope::For, false)).unwrap();
		let dst = open_value.dst().expect("a value site has a register");
		let hit = open_value.hit();
		em.emit(Instr::Load {
			dst,
			src: Operand::Const(0),
		});
		close(&mut em, &open_value, dst).unwrap();
		let after = em.here();
		let emitted = forms(&em);
		assert_eq!(emitted.len(), 3);
		let Some(IrForm::MemoGet(get)) = emitted[0] else {
			panic!("expected a MemoGet first, got {emitted:?}");
		};
		assert!(emitted[1].is_none());
		let Some(IrForm::MemoPut(put)) = emitted[2] else {
			panic!("expected a MemoPut last, got {emitted:?}");
		};
		assert_eq!((get.cell, get.dst, get.hit), (0, dst, hit));
		assert_eq!((put.cell, put.src), (0, dst));
		// The hit label binds to the instruction after the put.
		let pipeline = em.finish_pipeline(crate::exec::vm::program::Source::Once).unwrap();
		let Instr::Ir(first) = &pipeline.code[0] else {
			panic!("the first instruction is the get");
		};
		let IrForm::MemoGet(get) = first.form() else {
			panic!("the first instruction is the get");
		};
		assert_eq!(get.hit, after);

		// A truthy-only site: test, region, put, re-test, with both labels left to the consumer.
		let mut em = Emitter::new();
		let open_truthy = open(&mut cells, &mut em, &site(8, MemoScope::For, true)).unwrap();
		assert_eq!(open_truthy.dst(), None);
		let (then, otherwise) = (open_truthy.hit(), open_truthy.otherwise().unwrap());
		let value = em.reg().unwrap();
		em.emit(Instr::Load {
			dst: value,
			src: Operand::Const(0),
		});
		close(&mut em, &open_truthy, value).unwrap();
		let emitted = forms(&em);
		let Some(IrForm::MemoTruthy(truthy)) = emitted[0] else {
			panic!("expected a MemoTruthy first, got {emitted:?}");
		};
		assert_eq!((truthy.cell, truthy.then, truthy.otherwise), (1, then, otherwise));
		assert!(matches!(emitted[2], Some(IrForm::MemoPut(_))));
		assert!(matches!(
			em.code()[3],
			Instr::JumpIf {
				truthy: true,
				target,
				..
			} if target == then
		));
		assert!(matches!(
			em.code()[4],
			Instr::Jump {
				target
			} if target == otherwise
		));

		// A FOR scope emits one reset over its cells; a select group's cells emit none.
		let mut em = Emitter::new();
		reset_for_scope(&mut em, 0, 2);
		assert_eq!(forms(&em).len(), 1);
		let Some(IrForm::MemoReset(reset)) = forms(&em)[0] else {
			panic!("expected a MemoReset");
		};
		assert_eq!((reset.first, reset.count), (0, 2));
		let mut em = Emitter::new();
		reset_for_scope(&mut em, 2, 0);
		assert!(em.code().is_empty());

		let mut em = Emitter::new();
		let mut cells = Cells::new();
		let opened = open(&mut cells, &mut em, &site(1, MemoScope::SelectGroup(3), false)).unwrap();
		close(&mut em, &opened, 1).unwrap();
		assert_eq!(cells.owner(), &[Some(3)]);
		assert!(!forms(&em).iter().any(|form| matches!(form, Some(IrForm::MemoReset(_)))));
	}

	#[test]
	fn a_value_in_another_register_is_copied_into_dst_before_the_put() {
		let mut em = Emitter::new();
		let mut cells = Cells::new();
		let opened = open(&mut cells, &mut em, &site(1, MemoScope::For, false)).unwrap();
		let dst = opened.dst().expect("a value site has a register");
		let value = em.reg().unwrap();
		assert_ne!(value, dst);
		em.emit(Instr::Load {
			dst: value,
			src: Operand::Const(0),
		});
		close(&mut em, &opened, value).unwrap();
		let emitted = forms(&em);
		assert_eq!(emitted.len(), 4);
		assert!(matches!(emitted[0], Some(IrForm::MemoGet(_))));
		assert!(matches!(
			em.code()[2],
			Instr::Load {
				dst: copied,
				src: Operand::Reg(from),
			} if copied == dst && from == value
		));
		let Some(IrForm::MemoPut(put)) = emitted[3] else {
			panic!("expected a MemoPut last, got {emitted:?}");
		};
		assert_eq!(put.src, value);
	}

	#[test]
	fn memo_cells_numbered_per_program() {
		let mut em = Emitter::new();
		let mut cells = Cells::new();
		let mut cell_of = |id: u32, scope: MemoScope| {
			open(&mut cells, &mut em, &site(id, scope, false)).unwrap().cell()
		};
		// First appearance numbers densely, whatever the ids are.
		assert_eq!(cell_of(90, MemoScope::For), 0);
		assert_eq!(cell_of(4, MemoScope::SelectGroup(2)), 1);
		assert_eq!(cell_of(55, MemoScope::For), 2);
		// A site that appears again keeps its cell and takes no number.
		assert_eq!(cell_of(4, MemoScope::SelectGroup(2)), 1);
		assert_eq!(cell_of(90, MemoScope::For), 0);
		assert_eq!(cell_of(6, MemoScope::For), 3);
		assert_eq!(cells.count(), 4);
		assert_eq!(cells.owner(), &[None, Some(2), None, None]);

		// A second program numbers from zero again.
		let mut other = Cells::new();
		let mut em = Emitter::new();
		assert_eq!(open(&mut other, &mut em, &site(4, MemoScope::For, false)).unwrap().cell(), 0);
	}

	#[test]
	fn a_site_cannot_change_its_owning_group() {
		let mut em = Emitter::new();
		let mut cells = Cells::new();
		open(&mut cells, &mut em, &site(1, MemoScope::SelectGroup(0), false)).unwrap();
		let err = open(&mut cells, &mut em, &site(1, MemoScope::SelectGroup(1), false));
		assert!(matches!(err, Err(IrError::Stale(_))));
		assert_eq!(cells.count(), 1);
	}

	#[test]
	fn cell_numbers_run_out_as_capacity() {
		let mut em = Emitter::new();
		let mut cells = Cells::new();
		cells.next = u16::MAX;
		let err = open(&mut cells, &mut em, &site(1, MemoScope::For, false));
		assert!(matches!(err, Err(IrError::Capacity("memo cells"))));
		assert_eq!(cells.count(), u16::MAX);
		assert!(cells.owner().is_empty());
	}
}
