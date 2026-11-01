//! The structural digest of a logical op's identity-bearing attributes.
//!
//! The digest covers the attributes that decide what the op means and skips labels, locations,
//! provenance and placement. Statement, write stage, loop, terminator and transaction region ops
//! are opaque leaves. Their digest is `OPAQUE`, so two of them are never merged as one value.
//!
//! The digest is a 128-bit FNV-1a over a byte stream that begins with the op's mnemonic and then
//! holds each attribute in declaration order. The stream does not depend on the platform: integers
//! are fed little-endian and `usize` is widened to 64 bits, a string or sequence is
//! length-prefixed, an enum is fed as its variant index followed by its payload, and a kind is fed
//! as its canonical text.

#![deny(clippy::wildcard_enum_match_arm)]

use std::hash::{Hash, Hasher};

use super::ops::{FieldShape, FieldsShape, LogOp};
use crate::exec::ir::iface::identity::AttrDigest;
use crate::exec::ir::iface::statements::DdlGuard;
use crate::iam::Action;

const FNV_OFFSET: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
const FNV_PRIME: u128 = 0x0000_0000_0100_0000_0000_0000_0000_013b;

/// A hasher whose result depends only on the bytes it is fed, in the order it is fed them.
struct StableHasher {
	hash: u128,
	/// The whole stream, kept for [`LogOp::attrs_eq`]; `None` when only the digest is wanted.
	kept: Option<Vec<u8>>,
}

macro_rules! write_le {
	($($method:ident($ty:ty)),+ $(,)?) => {
		$(
			fn $method(&mut self, value: $ty) {
				self.write(&value.to_le_bytes());
			}
		)+
	};
}

impl Hasher for StableHasher {
	fn write(&mut self, bytes: &[u8]) {
		if let Some(kept) = &mut self.kept {
			kept.extend_from_slice(bytes);
		}
		for byte in bytes {
			self.hash ^= u128::from(*byte);
			self.hash = self.hash.wrapping_mul(FNV_PRIME);
		}
	}

	write_le!(
		write_u16(u16),
		write_u32(u32),
		write_u64(u64),
		write_u128(u128),
		write_i16(i16),
		write_i32(i32),
		write_i64(i64),
		write_i128(i128),
	);

	fn write_usize(&mut self, value: usize) {
		self.write_u64(value as u64);
	}

	fn write_isize(&mut self, value: isize) {
		self.write_i64(value as i64);
	}

	fn finish(&self) -> u64 {
		(self.hash ^ (self.hash >> 64)) as u64
	}
}

impl StableHasher {
	/// Starts the stream with the op's mnemonic, so ops with the same attribute layout still digest
	/// differently. With `keep`, the stream is also kept whole.
	fn new(op: &LogOp, keep: bool) -> Self {
		let mut hasher = Self {
			hash: FNV_OFFSET,
			kept: keep.then(Vec::new),
		};
		op.tag().mnemonic().hash(&mut hasher);
		hasher
	}

	/// The digest of the bytes fed so far. If it would equal [`AttrDigest::OPAQUE`], the lowest
	/// byte is cleared so a computed digest never equals it.
	fn digest(&self) -> AttrDigest {
		let mut bytes = self.hash.to_be_bytes();
		if bytes == AttrDigest::OPAQUE.0 {
			bytes[15] = 0;
		}
		AttrDigest(bytes)
	}
}

/// Feeds a projection list: the alias of each field and the text of the kind it is coerced to.
fn hash_fields(fields: &FieldsShape, hasher: &mut StableHasher) {
	match fields {
		FieldsShape::Value {
			alias,
		} => {
			0u8.hash(hasher);
			alias.hash(hasher);
		}
		FieldsShape::Select(list) => {
			1u8.hash(hasher);
			list.len().hash(hasher);
			for field in list.iter() {
				match field {
					FieldShape::All => 0u8.hash(hasher),
					FieldShape::Single {
						alias,
						kind,
					} => {
						1u8.hash(hasher);
						alias.hash(hasher);
						kind.as_ref().map(ToString::to_string).hash(hasher);
					}
				}
			}
		}
	}
}

/// Feeds an authority check. `Action` has no `Hash`, so its variant is fed by index.
fn hash_guard(guard: &DdlGuard, hasher: &mut StableHasher) {
	match guard {
		DdlGuard::RefuseNoWrite => 0u8.hash(hasher),
		DdlGuard::Allow {
			action,
			resource,
			base,
		} => {
			1u8.hash(hasher);
			match action {
				Action::View => 0u8.hash(hasher),
				Action::Edit => 1u8.hash(hasher),
			}
			resource.hash(hasher);
			base.hash(hasher);
		}
	}
}

impl LogOp {
	/// The digest of the op's identity-bearing attributes, or [`AttrDigest::OPAQUE`] for an op
	/// whose attributes are not digested, which equals no digest this function computes.
	pub(crate) fn attr_digest(&self) -> AttrDigest {
		let mut hasher = StableHasher::new(self, false);
		if !self.feed(&mut hasher) {
			return AttrDigest::OPAQUE;
		}
		hasher.digest()
	}

	/// Whether `self` and `other` have equal identity-bearing attributes: their byte streams are
	/// equal, byte for byte. A digest hit is a match only when this holds, so two ops whose
	/// digests collide are never one value. An op whose attributes are not digested equals no op,
	/// itself included.
	///
	/// The stream is fed through each attribute's `Hash`. The language's literals and literal kinds
	/// hash their variant first, so `1`, `1f` and `1dec` feed different streams.
	pub(crate) fn attrs_eq(&self, other: &LogOp) -> bool {
		let mut left = StableHasher::new(self, true);
		let mut right = StableHasher::new(other, true);
		self.feed(&mut left) && other.feed(&mut right) && left.kept == right.kept
	}

	/// Feeds the op's attributes to `hasher`, or answers `false` for an op whose attributes are
	/// not digested.
	fn feed(&self, hasher: &mut StableHasher) -> bool {
		match self {
			LogOp::Request {
				..
			}
			| LogOp::Txn {
				..
			}
			| LogOp::Stmt {
				..
			}
			| LogOp::Statement {
				..
			}
			| LogOp::Write(..)
			| LogOp::WriteStage {
				..
			}
			| LogOp::Events(..)
			| LogOp::Define(..)
			| LogOp::Remove(..)
			| LogOp::Alter(..)
			| LogOp::Rebuild(..)
			| LogOp::Live(..)
			| LogOp::Access(..)
			| LogOp::Kill
			| LogOp::Return
			| LogOp::Break
			| LogOp::Next
			| LogOp::Yield
			| LogOp::Choose(..)
			| LogOp::Loop {
				..
			} => return false,
			LogOp::Source(attrs) => attrs.hash(hasher),
			LogOp::Values {
				for_update,
				written,
			} => {
				for_update.hash(hasher);
				written.hash(hasher);
			}
			LogOp::Match {
				bindings,
			} => bindings.hash(hasher),
			LogOp::MatchRead {
				optional_group,
				patterns,
			} => {
				optional_group.hash(hasher);
				patterns.hash(hasher);
			}
			LogOp::MatchUpdate(attrs) => attrs.hash(hasher),
			LogOp::MatchDelete {
				target,
				detach,
			} => {
				target.hash(hasher);
				detach.hash(hasher);
			}
			LogOp::MatchInsert(attrs) => attrs.hash(hasher),
			LogOp::MatchOutput(shape) => shape.hash(hasher),
			LogOp::BindStep(shape) => shape.hash(hasher),
			LogOp::Assign(attrs) => attrs.hash(hasher),
			LogOp::Unset {
				place,
			} => place.hash(hasher),
			LogOp::Content {
				mode,
			} => mode.hash(hasher),
			LogOp::DdlGuard {
				guard,
			} => hash_guard(guard, hasher),
			LogOp::Join {
				kind,
				alias,
			} => {
				kind.hash(hasher);
				alias.hash(hasher);
			}
			LogOp::PermissionFilter {
				guard,
				computed,
			} => {
				guard.hash(hasher);
				computed.hash(hasher);
			}
			LogOp::Aggregate(shape) => {
				shape.groups.hash(hasher);
				hash_fields(&shape.fields, hasher);
				shape.names.hash(hasher);
				shape.first_value.hash(hasher);
			}
			LogOp::Sort(attrs) => attrs.hash(hasher),
			LogOp::Window(attrs) => attrs.hash(hasher),
			LogOp::Limit {
				limit,
				start,
			} => {
				limit.hash(hasher);
				start.hash(hasher);
			}
			LogOp::Project(attrs) => {
				hash_fields(&attrs.fields, hasher);
				attrs.layer.hash(hasher);
				attrs.omit.hash(hasher);
				attrs.computed.hash(hasher);
				attrs.judged.hash(hasher);
			}
			LogOp::Explain {
				analyze,
				format,
			} => {
				analyze.hash(hasher);
				format.hash(hasher);
			}
			LogOp::Let(attrs) => {
				attrs.name.hash(hasher);
				attrs.binding.hash(hasher);
				attrs.ty.as_ref().map(ToString::to_string).hash(hasher);
			}
			LogOp::Info {
				subject,
				structure,
			} => {
				subject.hash(hasher);
				structure.hash(hasher);
			}
			LogOp::Use {
				target,
			} => target.hash(hasher),
			LogOp::Option {
				name,
				value,
			} => {
				name.hash(hasher);
				value.hash(hasher);
			}
			LogOp::Show(attrs) => attrs.hash(hasher),
			LogOp::Subscriber(attrs) => attrs.hash(hasher),
			LogOp::Split {
				targets,
			} => targets.hash(hasher),
			LogOp::Subquery
			| LogOp::UnionAll
			| LogOp::Filter
			| LogOp::Distinct
			| LogOp::Fetch
			| LogOp::Timeout
			| LogOp::VersionScope
			| LogOp::Only
			| LogOp::Eval
			| LogOp::Block
			| LogOp::If
			| LogOp::Sleep => {}
		}
		true
	}
}
