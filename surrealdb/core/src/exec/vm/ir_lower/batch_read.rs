//! The fused batched read of a loop's `phys.prefetch`.
//!
//! One `BatchRead` serves a loop's point keys together, so the loop reads its records in one
//! storage call per batch instead of one per iteration. Which keys are batched changes only the
//! number of storage calls, never an answer.
//!
//! # Which keys are batched
//!
//! The prefetch names its keys as operand indices of its own op. A point whose operand is the
//! loop's element becomes [`SiteKey::Elem`], and the points whose operands are literal record ids
//! that are not ranges become one [`SiteKey::Literal`]. Every other point and every range is left
//! to the loop's own reads, which read the same records one at a time.

use smallvec::SmallVec;

use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::ids::{OpId, ValueId};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::{Batch, PhysOp};
use crate::exec::vm::ir_instr::{BatchRead, BufferScope, IrInstr, LiteralKey, LookAhead, SiteKey};
use crate::exec::vm::plain::ValueData;
use crate::exec::vm::program::Reg;
use crate::val::{RecordIdKey, Value};

/// Numbers the buffer sites of one program. A site number is unique within the program.
pub(super) struct Sites(u32);

#[allow(dead_code)] // Called by the phys.loop arm of the direct entry.
impl Sites {
	/// A counter that has handed out no site.
	pub(super) fn new() -> Self {
		Self(0)
	}

	/// The next site number, or [`IrError::Capacity`] when the numbers are exhausted.
	fn next(&mut self) -> Result<u32, IrError> {
		let site = self.0;
		self.0 = site.checked_add(1).ok_or(IrError::Capacity("buffer sites"))?;
		Ok(site)
	}
}

/// Lowers the loop's `phys.prefetch` to the pair of instructions that bracket it.
///
/// `iter_slot` and `elem` are the loop's iterator slot and element register, and `elem_value` is
/// the loop body's element block argument. The first instruction of the pair is the reset the
/// caller places immediately before the iterator starts, and the second is the read it places as
/// the first instruction after the iterator advances. Answers `None` when no key of the prefetch
/// can be batched, and when `placed_morsels` is set: a loop inside a morsel or region body is
/// never batched. A site number is taken only when the pair is answered.
///
/// The read keeps the order of the prefetch's points. It has one entry for the element, however
/// many points name it, and one entry for all the literal record ids, placed at the first of their
/// points. It looks ahead through the iterator when the element is a key, and otherwise looks
/// ahead nowhere. `batch` is copied into the read's bounds.
///
/// An op `prefetch` that is not a live `phys.prefetch`, or whose points name an operand it does
/// not have, answers [`IrError::Stale`].
#[allow(clippy::too_many_arguments)]
#[allow(dead_code)] // Called by the phys.loop arm of the direct entry.
pub(super) fn lower_loop_prefetch(
	sites: &mut Sites,
	ir: &Ir,
	prefetch: OpId,
	elem_value: ValueId,
	iter_slot: u16,
	elem: Reg,
	batch: Batch,
	placed_morsels: bool,
) -> Result<Option<(IrInstr, IrInstr)>, IrError> {
	if placed_morsels {
		return Ok(None);
	}
	let Some(data) = ir.op(prefetch) else {
		return Err(IrError::Stale("batch_read::lower_loop_prefetch: the prefetch"));
	};
	let OpKind::Phys(PhysOp::Prefetch(keys)) = &data.kind else {
		return Err(IrError::Stale(
			"batch_read::lower_loop_prefetch: an op that is not phys.prefetch",
		));
	};
	let mut elem_key = false;
	let mut literals: Vec<LiteralKey> = Vec::new();
	// Index in `found` of the entry that holds the literal keys. Set when the first literal key is
	// seen.
	let mut literal_at = None;
	let mut found: Vec<Option<SiteKey>> = Vec::new();
	for point in &keys.points {
		let Some(operand) = data.operands.get(usize::from(*point)).copied() else {
			return Err(IrError::Stale("batch_read::lower_loop_prefetch: a point's operand"));
		};
		if operand == elem_value {
			if !elem_key {
				elem_key = true;
				found.push(Some(SiteKey::Elem));
			}
		} else if let Some(key) = literal_key(ir, operand) {
			literals.push(key);
			if literal_at.is_none() {
				literal_at = Some(found.len());
				found.push(None);
			}
		}
	}
	if let Some(at) = literal_at {
		found[at] = Some(SiteKey::Literal(std::mem::take(&mut literals).into_boxed_slice()));
	}
	let keys: SmallVec<[SiteKey; 4]> = found.into_iter().flatten().collect();
	if keys.is_empty() {
		return Ok(None);
	}
	let site = sites.next()?;
	let ahead = match elem_key {
		true => LookAhead::Iter {
			slot: iter_slot,
			elem,
		},
		false => LookAhead::None,
	};
	let read = BatchRead {
		scope: BufferScope::Loop {
			site,
		},
		ahead,
		keys: keys.into_vec().into_boxed_slice(),
		max_positions: batch.max_iterations,
		max_bytes: batch.max_bytes,
	};
	Ok(Some((IrInstr::batch_reset(site), IrInstr::batch_read(read))))
}

/// The literal record id `value` is, when it is the result of an `expr.literal` holding a record
/// id whose key is not a range.
fn literal_key(ir: &Ir, value: ValueId) -> Option<LiteralKey> {
	let ValueDef::OpResult {
		op,
		..
	} = ir.value(value)?.def
	else {
		return None;
	};
	let OpKind::Expr(ExprOp::Literal(literal)) = &ir.op(op)?.kind else {
		return None;
	};
	let Value::RecordId(id) = &**literal else {
		return None;
	};
	if matches!(id.key, RecordIdKey::Range(_)) {
		return None;
	}
	let key = ValueData::new(id.key.clone(), "a literal record id key").ok()?;
	Some(LiteralKey {
		table: id.table.clone(),
		key,
	})
}

#[cfg(test)]
mod tests {
	use std::ops::Bound;

	use surrealdb_strand::Strand;

	use super::*;
	use crate::exec::ir::phys::ops::PrefetchKeys;
	use crate::exec::vm::ir_instr::IrForm;
	use crate::exec::vm::ir_lower::tests::Builder;
	use crate::val::{RecordId, RecordIdKeyRange, TableName};

	const BATCH: Batch = Batch {
		max_iterations: 24,
		max_bytes: 4096,
	};

	/// A record id literal of table `table` and key `key`.
	fn record(b: &mut Builder, table: &str, key: i64) -> ValueId {
		b.literal(Value::RecordId(RecordId::new(TableName::from(table), key)))
	}

	/// A prefetch op over `operands`, whose points and ranges name operands by index.
	fn prefetch(
		b: &mut Builder,
		operands: &[ValueId],
		points: &[u16],
		ranges: &[(u16, u16)],
	) -> OpId {
		b.add_phys(
			PhysOp::Prefetch(Box::new(PrefetchKeys {
				points: points.iter().copied().collect(),
				ranges: ranges.iter().copied().collect(),
			})),
			operands,
		)
	}

	/// A stand-in for the loop's element block argument: a value id with no record id behind it.
	fn element(b: &mut Builder) -> ValueId {
		b.literal(Value::from(0))
	}

	fn lower(
		b: &Builder,
		sites: &mut Sites,
		op: OpId,
		elem_value: ValueId,
		placed_morsels: bool,
	) -> Option<(IrInstr, IrInstr)> {
		lower_loop_prefetch(sites, &b.ir, op, elem_value, 3, 7, BATCH, placed_morsels)
			.expect("the prefetch lowers")
	}

	fn read_of(instr: &IrInstr) -> &BatchRead {
		let IrForm::BatchRead(read) = instr.form() else {
			panic!("expected a BatchRead, got {:?}", instr.form());
		};
		read
	}

	#[test]
	fn batch_read_placed_after_iter_next() {
		let mut b = Builder::new();
		let elem = element(&mut b);
		let op = prefetch(&mut b, &[elem], &[0], &[]);
		let (_, read) = lower(&b, &mut Sites::new(), op, elem, false).expect("a key is batched");
		let read = read_of(&read);
		// The read follows IterNext, so the element is in the register IterNext wrote and the
		// elements after it are the rest of the iterator the loop started.
		assert_eq!(
			read.ahead,
			LookAhead::Iter {
				slot: 3,
				elem: 7
			}
		);
		assert_eq!(read.keys.as_ref(), &[SiteKey::Elem]);
	}

	#[test]
	fn batch_reset_before_iter_start() {
		let mut b = Builder::new();
		let elem = element(&mut b);
		let op = prefetch(&mut b, &[elem], &[0], &[]);
		let (reset, read) =
			lower(&b, &mut Sites::new(), op, elem, false).expect("a key is batched");
		// The reset names the site the read fills, so every entry to the loop starts with an empty
		// buffer.
		let IrForm::BatchReset {
			site,
		} = *reset.form()
		else {
			panic!("expected a BatchReset");
		};
		assert_eq!(
			read_of(&read).scope,
			BufferScope::Loop {
				site
			}
		);
	}

	#[test]
	fn batch_read_sites_unique() {
		let mut b = Builder::new();
		let elem = element(&mut b);
		let first = prefetch(&mut b, &[elem], &[0], &[]);
		let second = prefetch(&mut b, &[elem], &[0], &[]);
		let mut sites = Sites::new();
		let (a, _) = lower(&b, &mut sites, first, elem, false).expect("a key is batched");
		let (c, _) = lower(&b, &mut sites, second, elem, false).expect("a key is batched");
		assert_ne!(a, c);
	}

	#[test]
	fn batch_read_skips_unexpressible_keys() {
		let mut b = Builder::new();
		let elem = element(&mut b);
		let field = b.add(
			ExprOp::Field {
				name: Strand::new("friend"),
			},
			&[elem],
		);
		let begin = record(&mut b, "t", 1);
		let end = record(&mut b, "t", 9);
		let span = b.literal(Value::RecordId(RecordId::new(
			TableName::from("t"),
			Box::new(RecordIdKeyRange {
				start: Bound::Unbounded,
				end: Bound::Unbounded,
			}),
		)));
		let one = record(&mut b, "t", 2);
		// A field of the element and a record id range are skipped, a range of two points is
		// skipped, and the element and the plain literal remain, in the prefetch's order.
		let op = prefetch(&mut b, &[field, span, begin, end, elem, one], &[0, 1, 4, 5], &[(2, 3)]);
		let mut sites = Sites::new();
		let (_, read) = lower(&b, &mut sites, op, elem, false).expect("keys are batched");
		let keys = &read_of(&read).keys;
		assert_eq!(keys.len(), 2);
		assert_eq!(keys[0], SiteKey::Elem);
		let SiteKey::Literal(literals) = &keys[1] else {
			panic!("expected the literal key");
		};
		assert_eq!(literals.len(), 1);
		assert_eq!(literals[0].table, TableName::from("t"));
		assert_eq!(*literals[0].key, RecordIdKey::Number(2));

		// Nothing is left to batch: no instruction, and no site is taken.
		let only = prefetch(&mut b, &[field, begin, end], &[0], &[(1, 2)]);
		assert!(lower(&b, &mut sites, only, elem, false).is_none());
		let none = prefetch(&mut b, &[], &[], &[]);
		assert!(lower(&b, &mut sites, none, elem, false).is_none());
		let next = prefetch(&mut b, &[elem], &[0], &[]);
		let (reset, _) = lower(&b, &mut sites, next, elem, false).expect("a key is batched");
		assert_eq!(
			*reset.form(),
			IrForm::BatchReset {
				site: 1
			}
		);
	}

	#[test]
	fn batch_read_literals_only_look_nowhere_ahead() {
		let mut b = Builder::new();
		let elem = element(&mut b);
		let first = record(&mut b, "t", 1);
		let second = record(&mut b, "u", 2);
		let op = prefetch(&mut b, &[first, second], &[0, 1], &[]);
		let (_, read) = lower(&b, &mut Sites::new(), op, elem, false).expect("keys are batched");
		let read = read_of(&read);
		assert_eq!(read.ahead, LookAhead::None);
		assert_eq!(read.keys.len(), 1);
		let SiteKey::Literal(literals) = &read.keys[0] else {
			panic!("expected the literal key");
		};
		assert_eq!(literals.len(), 2);
	}

	#[test]
	fn batch_read_never_under_morsels() {
		let mut b = Builder::new();
		let elem = element(&mut b);
		let op = prefetch(&mut b, &[elem], &[0], &[]);
		let mut sites = Sites::new();
		assert!(lower(&b, &mut sites, op, elem, true).is_none());
		// No site was taken by the refusal.
		let (reset, _) = lower(&b, &mut sites, op, elem, false).expect("a key is batched");
		assert_eq!(
			*reset.form(),
			IrForm::BatchReset {
				site: 0
			}
		);
	}

	#[test]
	fn batch_read_copies_batch() {
		let mut b = Builder::new();
		let elem = element(&mut b);
		let op = prefetch(&mut b, &[elem], &[0], &[]);
		let (_, read) = lower(&b, &mut Sites::new(), op, elem, false).expect("a key is batched");
		let read = read_of(&read);
		assert_eq!(read.max_positions, BATCH.max_iterations);
		assert_eq!(read.max_bytes, BATCH.max_bytes);
	}

	#[test]
	fn batch_read_refuses_a_point_without_an_operand() {
		let mut b = Builder::new();
		let elem = element(&mut b);
		let op = prefetch(&mut b, &[elem], &[1], &[]);
		let refused = lower_loop_prefetch(&mut Sites::new(), &b.ir, op, elem, 3, 7, BATCH, false);
		assert!(matches!(refused, Err(IrError::Stale(_))));
	}
}
