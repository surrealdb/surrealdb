//! The structural digest of a physical op's identity-bearing attributes.
//!
//! The digest covers the attributes that decide how the op computes its value (the access path,
//! the method, the guard) and skips labels, locations, provenance and the placement decision: two
//! pipelines that differ only in their decision have equal digests. A pipeline's id, edges and
//! logical origin are scoped to its container and are not identity, so a pipeline digests as its
//! mnemonic alone.
//!
//! The digest is a 128-bit FNV-1a over a byte stream that begins with the op's mnemonic and then
//! holds each attribute in declaration order. The stream does not depend on the platform or the
//! build: integers are fed little-endian at a fixed width, a string or sequence is
//! length-prefixed, an enum is fed as an explicit discriminant byte followed by its payload, and a
//! kind is fed as its canonical text. No `Debug` text and no `std::hash::Hash` implementation is
//! fed.
//!
//! An op whose attributes are not digested answers [`AttrDigest::OPAQUE`], which equals no digest
//! this function computes and which a consumer reads as "never the same value twice". Four reasons
//! put a variant there:
//!
//! - **No stable encoding.** The attribute embeds a type of the planner or the language that has no
//!   canonical byte form: a stored value or an access built from values (`phys.index_scan`,
//!   `phys.index_union`, `phys.bitmap`, `phys.fulltext_scan`, `phys.key_range_scan`,
//!   `phys.knn_scan`, `phys.knn_topk`, `phys.count`), a field path or output path (`phys.layer`,
//!   `phys.sort`, `phys.aggregate`, `phys.project`), a stored definition or statement-family
//!   attribute (`phys.define`, `phys.remove`, `phys.alter`, `phys.rebuild`, `phys.access`,
//!   `phys.live`, `phys.show`, `phys.info`, `phys.match`, `phys.match_step`, `phys.let`,
//!   `phys.ddl_guard`, `phys.write`, `phys.write_stage`, `phys.assign`, `phys.unset`,
//!   `phys.content`), and, inside an otherwise encodable op, a pre-decoded sort key or predicate
//!   (`phys.table_scan` with a top-k key, a prefilter or record-id bounds) or a computed-field read
//!   (`phys.permission_filter` with computed reads).
//! - **Meaning in regions.** The value of the op is decided by the regions it owns (a predicate, a
//!   key, a body or an arm), which the digest does not cover, so equal attributes do not make equal
//!   values (`phys.filter`, `phys.fetch`, `phys.split`, `phys.subquery`, `phys.eval`, `phys.block`,
//!   `phys.if`, `phys.sleep`, `phys.union`, `phys.join`, `phys.permission_filter`, `phys.choose`,
//!   `phys.timeout`). `phys.pipeline` is the exception: it digests as its mnemonic alone, so two
//!   pipelines that differ only in placement are one value.
//! - **Not a value.** The op is a statement, a loop, a unit load or a terminator, whose effect and
//!   position, not its attributes, make it what it is (`phys.request`, `phys.txn`, `phys.stmt`,
//!   `phys.statement`, `phys.kill`, `phys.loop`, `phys.load_unit`, `phys.yield`, `phys.return`,
//!   `phys.break`, `phys.next`).
//! - **Wraps a statement's rows.** The op changes the snapshot its input is read at, collapses the
//!   input to one value or replaces it by a rendering of its plan, so its value is its input's and
//!   no attribute of its own (`phys.version_scope`, `phys.only`, `phys.explain`).

#![deny(clippy::wildcard_enum_match_arm)]

use super::ops::{LookupKeyShape, MaterializeImage, PhysOp};
use crate::exec::ir::iface::choose::IndexRef;
use crate::exec::ir::iface::identity::AttrDigest;
use crate::exec::ir::logical::ops::UseTarget;
use crate::exec::vm::payload::{Spill, ValuesRead};
use crate::kvs::Direction;

const FNV_OFFSET: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
const FNV_PRIME: u128 = 0x0000_0000_0100_0000_0000_0000_0000_013b;

/// The byte stream of one digest.
struct Encoder {
	hash: u128,
	/// The whole stream, kept for [`PhysOp::attrs_eq`]; `None` when only the digest is wanted.
	kept: Option<Vec<u8>>,
}

impl Encoder {
	/// A stream that starts with the mnemonic of `op`, which separates the ops of one attribute
	/// shape. With `keep`, the stream is also kept whole.
	fn new(op: &PhysOp, keep: bool) -> Self {
		let mut encoder = Self {
			hash: FNV_OFFSET,
			kept: keep.then(Vec::new),
		};
		encoder.str(op.tag().mnemonic());
		encoder
	}

	fn bytes(&mut self, bytes: &[u8]) {
		if let Some(kept) = &mut self.kept {
			kept.extend_from_slice(bytes);
		}
		for byte in bytes {
			self.hash ^= u128::from(*byte);
			self.hash = self.hash.wrapping_mul(FNV_PRIME);
		}
	}

	fn u8(&mut self, value: u8) {
		self.bytes(&[value]);
	}

	fn u16(&mut self, value: u16) {
		self.bytes(&value.to_le_bytes());
	}

	fn len(&mut self, len: usize) {
		self.bytes(&(len as u64).to_le_bytes());
	}

	fn bool(&mut self, value: bool) {
		self.u8(u8::from(value));
	}

	fn str(&mut self, value: &str) {
		self.len(value.len());
		self.bytes(value.as_bytes());
	}

	fn option_u16(&mut self, value: Option<u16>) {
		match value {
			None => self.u8(0),
			Some(value) => {
				self.u8(1);
				self.u16(value);
			}
		}
	}

	fn index(&mut self, index: &IndexRef) {
		self.str(index.table.as_str());
		self.str(index.index.as_str());
	}

	fn spill(&mut self, spill: Spill) {
		self.u8(match spill {
			Spill::Bounded => 0,
			Spill::Spillable => 1,
		});
	}

	/// The digest, never equal to [`AttrDigest::OPAQUE`].
	fn finish(self) -> AttrDigest {
		let mut bytes = self.hash.to_be_bytes();
		if bytes == AttrDigest::OPAQUE.0 {
			bytes[15] = 0;
		}
		AttrDigest(bytes)
	}
}

impl PhysOp {
	/// The digest of the op's identity-bearing attributes, or [`AttrDigest::OPAQUE`] when the op's
	/// attributes are not digested, which equals no digest this function computes.
	pub(crate) fn attr_digest(&self) -> AttrDigest {
		let mut encoder = Encoder::new(self, false);
		if !self.encode(&mut encoder) {
			return AttrDigest::OPAQUE;
		}
		encoder.finish()
	}

	/// Whether `self` and `other` have equal identity-bearing attributes: their byte streams are
	/// equal, byte for byte. A digest hit is a match only when this holds, so two ops whose
	/// digests collide are never one value. An op whose attributes are not digested equals no op,
	/// itself included.
	pub(crate) fn attrs_eq(&self, other: &PhysOp) -> bool {
		let mut left = Encoder::new(self, true);
		let mut right = Encoder::new(other, true);
		self.encode(&mut left) && other.encode(&mut right) && left.kept == right.kept
	}

	/// Writes the op's attributes to `encoder`, or answers `false` for an op whose attributes are
	/// not digested.
	fn encode(&self, encoder: &mut Encoder) -> bool {
		match self {
			PhysOp::TableScan(attrs) => {
				if attrs.topk.is_some() || attrs.prefilter.is_some() || attrs.bounds.is_some() {
					return false;
				}
				encoder.str(attrs.table.as_str());
				encoder.u8(match attrs.direction {
					Direction::Forward => 0,
					Direction::Backward => 1,
				});
				encoder.bool(attrs.lightweight);
				encoder.bool(attrs.id_order);
			}
			PhysOp::IndexLookup(attrs) => {
				encoder.str(attrs.table.as_str());
				encoder.index(&attrs.index);
				encoder.str(attrs.field.as_str());
				encoder.u8(match attrs.key {
					LookupKeyShape::Parent => 0,
					LookupKeyShape::Probe => 1,
				});
			}
			PhysOp::RecordScan(attrs) => {
				encoder.str(attrs.table.as_str());
				encoder.bool(attrs.for_update);
				encoder.bool(attrs.lightweight);
			}
			PhysOp::Vacant {
				table,
			}
			| PhysOp::Enforced {
				table,
			} => encoder.str(table.as_str()),
			PhysOp::Values {
				read,
			} => match read {
				ValuesRead::Expression {
					written,
				} => {
					encoder.u8(0);
					encoder.bool(*written);
				}
				ValuesRead::Dynamic => encoder.u8(1),
				ValuesRead::Locked => encoder.u8(2),
			},
			PhysOp::Probe {
				field,
			} => encoder.str(field.as_str()),
			PhysOp::Distinct {
				spill,
			} => encoder.spill(*spill),
			PhysOp::Limit {
				limit,
				start,
			} => {
				encoder.option_u16(*limit);
				encoder.option_u16(*start);
			}
			PhysOp::Use {
				target,
			} => encoder.u8(match target {
				UseTarget::Ns => 0,
				UseTarget::Db => 1,
				UseTarget::NsDb => 2,
				UseTarget::Default => 3,
			}),
			PhysOp::Option {
				name,
				value,
			} => {
				encoder.str(name.as_str());
				encoder.bool(*value);
			}
			PhysOp::Prefetch(keys) => {
				encoder.len(keys.points.len());
				for point in &keys.points {
					encoder.u16(*point);
				}
				encoder.len(keys.ranges.len());
				for (begin, end) in &keys.ranges {
					encoder.u16(*begin);
					encoder.u16(*end);
				}
			}
			PhysOp::Materialize {
				image,
			} => encoder.u8(match image {
				MaterializeImage::KeyAndValue => 0,
			}),
			// The id, edges and logical origin of a pipeline are scoped to its container.
			PhysOp::Pipeline(..) => {}
			PhysOp::Join(..)
			| PhysOp::PermissionFilter(..)
			| PhysOp::Choose(..)
			| PhysOp::Subquery
			| PhysOp::Union
			| PhysOp::Filter
			| PhysOp::Fetch
			| PhysOp::Split {
				..
			}
			| PhysOp::Eval
			| PhysOp::Block
			| PhysOp::Sleep
			| PhysOp::If {
				..
			}
			| PhysOp::IndexScan(..)
			| PhysOp::IndexUnion(..)
			| PhysOp::Bitmap(..)
			| PhysOp::FullTextScan(..)
			| PhysOp::KeyRangeScan(..)
			| PhysOp::KnnScan(..)
			| PhysOp::KnnTopK(..)
			| PhysOp::Count(..)
			| PhysOp::Layer(..)
			| PhysOp::Sort(..)
			| PhysOp::Window(..)
			| PhysOp::Aggregate(..)
			| PhysOp::Project(..)
			| PhysOp::Request {
				..
			}
			| PhysOp::Txn {
				..
			}
			| PhysOp::Stmt {
				..
			}
			| PhysOp::Statement {
				..
			}
			| PhysOp::Match {
				..
			}
			| PhysOp::MatchStep(..)
			| PhysOp::DdlGuard {
				..
			}
			| PhysOp::Define(..)
			| PhysOp::Remove(..)
			| PhysOp::Alter(..)
			| PhysOp::Rebuild(..)
			| PhysOp::Info {
				..
			}
			| PhysOp::Access(..)
			| PhysOp::Kill
			| PhysOp::Live(..)
			| PhysOp::Subscriber(..)
			| PhysOp::Show(..)
			| PhysOp::Write(..)
			| PhysOp::WriteStage {
				..
			}
			| PhysOp::Events(..)
			| PhysOp::Timeout
			| PhysOp::VersionScope
			| PhysOp::Only
			| PhysOp::Explain {
				..
			}
			| PhysOp::Assign(..)
			| PhysOp::Unset {
				..
			}
			| PhysOp::Content {
				..
			}
			| PhysOp::Let(..)
			| PhysOp::Loop {
				..
			}
			| PhysOp::LoadUnit(..)
			| PhysOp::Yield
			| PhysOp::Return
			| PhysOp::Break
			| PhysOp::Next => return false,
		}
		true
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::{Strand, TableName};

	use super::*;
	use crate::exec::ir::phys::ops::{
		IndexLookupAttrs, PipelineAttrs, PipelineEdge, PipelineId, ReadClauses, TableScanAttrs,
	};

	fn table(text: &str) -> TableName {
		TableName::new(Strand::new(text))
	}

	fn scan(text: &str, direction: Direction) -> PhysOp {
		PhysOp::TableScan(Box::new(TableScanAttrs {
			probes: None,
			bounds: None,
			prefilter: None,
			table: table(text),
			direction,
			topk: None,
			lightweight: false,
			id_order: true,
			read: ReadClauses::default(),
		}))
	}

	fn lookup(index: &str) -> PhysOp {
		PhysOp::IndexLookup(Box::new(IndexLookupAttrs {
			table: table("t"),
			index: IndexRef {
				table: table("t"),
				index: Strand::new(index),
				position: 0,
				scalar: Box::default(),
			},
			field: Strand::new("a"),
			key: LookupKeyShape::Parent,
			read: ReadClauses::default(),
		}))
	}

	fn pipeline(id: u32, edges: u32) -> PhysOp {
		PhysOp::Pipeline(Box::new(PipelineAttrs {
			id: PipelineId(id),
			edges: (0..edges).map(|at| (PipelineEdge::Feeds, PipelineId(at))).collect(),
			logical: None,
		}))
	}

	#[test]
	fn equal_attributes_equal_digests() {
		let first = scan("t", Direction::Forward).attr_digest();
		assert_eq!(first, scan("t", Direction::Forward).attr_digest());
		assert_ne!(first, AttrDigest::OPAQUE);
		assert_eq!(lookup("ix").attr_digest(), lookup("ix").attr_digest());
	}

	#[test]
	fn a_different_access_path_differs() {
		let forward = scan("t", Direction::Forward).attr_digest();
		assert_ne!(forward, scan("u", Direction::Forward).attr_digest());
		assert_ne!(forward, scan("t", Direction::Backward).attr_digest());
		assert_ne!(forward, lookup("ix").attr_digest());
		assert_ne!(lookup("ix").attr_digest(), lookup("iy").attr_digest());
	}

	/// Attributes are equal exactly when every digested attribute is, and an op whose attributes
	/// are not digested equals no op.
	#[test]
	fn attrs_eq_compares_every_attribute() {
		assert!(scan("t", Direction::Forward).attrs_eq(&scan("t", Direction::Forward)));
		assert!(!scan("t", Direction::Forward).attrs_eq(&scan("u", Direction::Forward)));
		assert!(!scan("t", Direction::Forward).attrs_eq(&scan("t", Direction::Backward)));
		assert!(!lookup("ix").attrs_eq(&lookup("iy")));
		assert!(pipeline(0, 0).attrs_eq(&pipeline(7, 2)));
		assert!(!PhysOp::Filter.attrs_eq(&PhysOp::Filter));
	}

	#[test]
	fn pipeline_ids_are_not_identity() {
		assert_eq!(pipeline(0, 0).attr_digest(), pipeline(7, 2).attr_digest());
		assert_ne!(pipeline(0, 0).attr_digest(), AttrDigest::OPAQUE);
	}

	#[test]
	fn a_region_owning_op_is_opaque() {
		for op in [
			PhysOp::Filter,
			PhysOp::Fetch,
			PhysOp::Split {
				targets: Box::new([]),
			},
			PhysOp::Subquery,
			PhysOp::Eval,
			PhysOp::Block,
			PhysOp::If {
				value: false,
			},
			PhysOp::Sleep,
			PhysOp::Union,
		] {
			assert_eq!(op.attr_digest(), AttrDigest::OPAQUE, "{}", op.tag().mnemonic());
		}
	}

	#[test]
	fn an_undigested_op_is_opaque() {
		assert_eq!(PhysOp::Yield.attr_digest(), AttrDigest::OPAQUE);
		assert_eq!(
			PhysOp::Loop {
				kind: crate::exec::ir::logical::ops::LoopKind::For,
				first: false,
				batch: crate::exec::ir::phys::ops::Batch {
					max_iterations: 1,
					max_bytes: 1,
				},
				unroll: 1,
				value: false,
				step: None,
			}
			.attr_digest(),
			AttrDigest::OPAQUE
		);
	}

	#[test]
	fn a_scan_with_a_prefilter_is_never_the_same_value_as_another() {
		use crate::exec::pre_decode_filter::PreDecodeShape;
		let PhysOp::TableScan(mut attrs) = scan("t", Direction::Forward) else {
			panic!("a table scan");
		};
		assert_ne!(PhysOp::TableScan(attrs.clone()).attr_digest(), AttrDigest::OPAQUE);
		attrs.prefilter = Some(PreDecodeShape::And(Vec::new()));
		assert_eq!(PhysOp::TableScan(attrs).attr_digest(), AttrDigest::OPAQUE);
	}

	#[test]
	fn a_scan_with_id_bounds_is_never_the_same_value_as_another() {
		use crate::exec::ir::phys::ops::{IdBoundsShape, KeyBoundShape, KeyRangeBoundShape};
		let PhysOp::TableScan(mut attrs) = scan("t", Direction::Forward) else {
			panic!("a table scan");
		};
		attrs.bounds = Some(IdBoundsShape {
			low: Some(KeyRangeBoundShape {
				bound: KeyBoundShape::Operand(0),
				inclusive: false,
			}),
			high: None,
		});
		assert_eq!(PhysOp::TableScan(attrs).attr_digest(), AttrDigest::OPAQUE);
	}
}
