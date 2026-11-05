//! The structural digest of an expression op's identity-bearing attributes.
//!
//! The digest covers the attributes that decide which value the op computes and skips labels,
//! locations, provenance, placement, operands, regions and frames. Two ops whose attributes are
//! equal ([`ExprOp::attrs_eq`]) and whose operands, frame and snapshot are equal compute one value,
//! subject to the rules of the analysis that asks and to the region rule on
//! [`ExprOp::attr_digest`]. Equal digests are a candidate for that, never a proof of it.
//!
//! The digest is the first 16 bytes of SHA-256 over a canonical byte string: a domain tag, the op's
//! mnemonic, then each payload field in declaration order. Every enum writes a discriminant byte
//! and every variable-length field a `u32` little-endian length prefix, so no two payloads share a
//! byte string by shifting a boundary. A [`Value`] is written through its `revision` encoding,
//! never through `Hash`, `PartialEq` or text: `Number`'s `Hash` and `PartialEq` equate `1`, `1f`
//! and `1dec`, and `0f` with `-0f`. A payload type of the language that has no structure of its own
//! here is written as its `Debug` text, which keeps variant names and so tells apart what equality
//! may normalise. The digest names an identity within one process; nothing persists it, so it is
//! not stable across builds.

#![deny(clippy::wildcard_enum_match_arm)]

use std::fmt::Debug;
use std::ops::Bound;

use sha2::{Digest, Sha256};

use super::ops::{
	ConvertMode, DestructureEntry, DispatchTarget, ExprOp, KeyBound, KeyShape, NestedStrategy,
	NowWhich, OnFail, RaiseKind, ShortCircuitOp,
};
use crate::exec::ir::iface::identity::AttrDigest;
use crate::val::Value;

/// The first bytes of every canonical byte string.
const DOMAIN: &[u8] = b"expr\0";

impl ExprOp {
	/// The digest of the op's identity-bearing attributes, or [`AttrDigest::OPAQUE`] for
	/// `closure_create`, the only op whose attributes are not digested: its `body_sql` is not the
	/// identity of its body, because two bodies can print alike.
	///
	/// No other op answers `OPAQUE`: a digest whose bytes equal `OPAQUE`'s has its last byte
	/// XORed with `0x01`, so a computed digest never equals it.
	///
	/// The digest covers attributes only, and an op that owns regions has no region component in
	/// it. Equal digests and operands do not make two such ops one value: `a AND b` and `a AND c`
	/// share tag, digest and operand `a`. A key built from this digest compares the op's regions as
	/// well, or is opaque.
	pub(crate) fn attr_digest(&self) -> AttrDigest {
		let mut c = Canon::new(self, Sha256::new());
		if !self.canon(&mut c) {
			return AttrDigest::OPAQUE;
		}
		c.finish()
	}

	/// Whether `self` and `other` have equal identity-bearing attributes: their canonical byte
	/// strings are equal, byte for byte. A digest hit is a match only when this holds, so two ops
	/// whose digests collide are never one value. `closure_create` equals no op, itself included.
	pub(crate) fn attrs_eq(&self, other: &ExprOp) -> bool {
		let mut left = Canon::new(self, Vec::new());
		let mut right = Canon::new(other, Vec::new());
		self.canon(&mut left) && other.canon(&mut right) && left.0 == right.0
	}

	/// Writes the op's attributes to `c`. Answers `false`, with nothing written past the mnemonic,
	/// for `closure_create`, whose attributes are not digested.
	fn canon<S: Sink>(&self, c: &mut Canon<S>) -> bool {
		match self {
			ExprOp::ClosureCreate(_) => return false,
			ExprOp::Literal(v) => c.value(v),
			ExprOp::Array
			| ExprOp::Set
			| ExprOp::Doc
			| ExprOp::Index
			| ExprOp::First
			| ExprOp::Last
			| ExprOp::Each
			| ExprOp::Optional
			| ExprOp::RepeatRecurse
			| ExprOp::Invoke
			| ExprOp::If
			| ExprOp::Throw
			| ExprOp::Yield
			| ExprOp::Truthy => {}
			ExprOp::Subquery(strategy) => c.strategy(strategy),
			ExprOp::Object {
				keys,
			} => c.names(keys),
			ExprOp::RecordId(attrs) => {
				c.text(attrs.table.as_str());
				c.key_shape(&attrs.key_shape);
			}
			ExprOp::FreeParam {
				name,
			}
			| ExprOp::SessionParam {
				name,
			}
			| ExprOp::CatalogParam {
				name,
			}
			| ExprOp::Field {
				name,
			}
			| ExprOp::Method {
				name,
			} => c.text(name.as_str()),
			ExprOp::Access {
				field,
			}
			| ExprOp::SetField {
				field,
			} => c.text(field.as_str()),
			// Every part of every place is digested, with the slots and the text of an unaddressed
			// part, so two writes that differ only in their target never share a key.
			ExprOp::TupleRow {
				places,
			} => c.debug(places),
			ExprOp::Table {
				name,
			} => c.text(name.as_str()),
			ExprOp::Mock(mock) => c.debug(mock),
			ExprOp::Constant {
				c: constant,
			} => c.debug(constant),
			ExprOp::ComputedRead(attrs) => {
				c.text(attrs.field.as_str());
				c.debug(&attrs.def.0);
				c.debug(&attrs.def.1);
			}
			ExprOp::Deref {
				table_hint,
			} => match table_hint {
				None => c.byte(0),
				Some(table) => {
					c.byte(1);
					c.text(table.as_str());
				}
			},
			ExprOp::Flatten {
				explicit,
			} => c.byte(u8::from(*explicit)),
			ExprOp::Dispatch(target) => match &**target {
				DispatchTarget::Method(name) => {
					c.byte(0);
					c.text(name.as_str());
				}
				DispatchTarget::Invoke => c.byte(1),
			},
			ExprOp::Destructure {
				shape,
			} => c.entries(shape),
			ExprOp::Prefix(op) => c.debug(op),
			ExprOp::Convert(attrs) => {
				c.byte(match attrs.mode {
					ConvertMode::Cast => 0,
					ConvertMode::Coerce => 1,
				});
				c.debug(&attrs.kind);
				c.u32(attrs.site.0);
				c.byte(match attrs.on_fail {
					OnFail::Error => 0,
					OnFail::None => 1,
				});
			}
			ExprOp::Postfix {
				op,
			} => c.debug(op),
			ExprOp::Binary {
				op,
			} => c.debug(op),
			ExprOp::ShortCircuit {
				op,
			} => c.byte(match op {
				ShortCircuitOp::And => 0,
				ShortCircuitOp::Or => 1,
				ShortCircuitOp::NullCoalescing => 2,
				ShortCircuitOp::TernaryCondition => 3,
			}),
			ExprOp::Call(function) => c.debug(function),
			ExprOp::Agg {
				kind,
				combine,
			} => {
				c.text(kind);
				c.debug(combine);
			}
			ExprOp::Now {
				which,
			} => c.byte(match which {
				NowWhich::Datetime => 0,
				NowWhich::UnixSeconds => 1,
				NowWhich::UnixMillis => 2,
				NowWhich::UnixMicros => 3,
				NowWhich::UnixNanos => 4,
			}),
			ExprOp::Raise(kind) => match &**kind {
				RaiseKind::Folded(error) => {
					c.byte(0);
					c.text(&error.text);
				}
				RaiseKind::InvalidControlFlow => c.byte(1),
				RaiseKind::Misplaced(control) => {
					c.byte(2);
					c.debug(control);
				}
				RaiseKind::Refused(refusal) => {
					c.byte(3);
					c.debug(refusal);
				}
			},
			ExprOp::DepthCharge {
				levels,
			} => c.u32(*levels),
		}
		true
	}
}

/// Where a canonical byte string is written: hashed for the digest, or kept whole for
/// [`ExprOp::attrs_eq`].
trait Sink {
	fn put(&mut self, bytes: &[u8]);
}

impl Sink for Sha256 {
	fn put(&mut self, bytes: &[u8]) {
		self.update(bytes);
	}
}

impl Sink for Vec<u8> {
	fn put(&mut self, bytes: &[u8]) {
		self.extend_from_slice(bytes);
	}
}

/// The canonical byte string of one op, written to its sink as it is built.
struct Canon<S>(S);

impl<S: Sink> Canon<S> {
	/// Starts the byte string with the domain tag and the op's mnemonic.
	fn new(op: &ExprOp, mut sink: S) -> Self {
		sink.put(DOMAIN);
		let mut c = Self(sink);
		c.text(op.tag().mnemonic());
		c
	}

	fn byte(&mut self, b: u8) {
		self.0.put(&[b]);
	}

	fn u32(&mut self, v: u32) {
		self.0.put(&v.to_le_bytes());
	}

	fn i64(&mut self, v: i64) {
		self.0.put(&v.to_le_bytes());
	}

	/// A variable-length field: its length as `u32` little-endian, then its bytes. A length past
	/// `u32::MAX` saturates; the bytes that follow are still hashed in full.
	fn bytes(&mut self, b: &[u8]) {
		self.u32(u32::try_from(b.len()).unwrap_or(u32::MAX));
		self.0.put(b);
	}

	fn text(&mut self, s: &str) {
		self.bytes(s.as_bytes());
	}

	/// A list of names: the count, then each name.
	fn names(&mut self, names: &[String]) {
		self.u32(u32::try_from(names.len()).unwrap_or(u32::MAX));
		for name in names {
			self.text(name);
		}
	}

	/// A payload type of the language that has no structure here: its `Debug` text.
	fn debug(&mut self, v: &impl Debug) {
		self.text(&format!("{v:?}"));
	}

	/// A value through its `revision` encoding. A value that encoding refuses, a closure, is
	/// written as its `Debug` text under its own discriminant, so the digest stays total.
	fn value(&mut self, v: &Value) {
		match revision::to_vec(v) {
			Ok(encoded) => {
				self.byte(0);
				self.bytes(&encoded);
			}
			Err(_) => {
				self.byte(1);
				self.debug(v);
			}
		}
	}

	fn strategy(&mut self, strategy: &NestedStrategy) {
		match strategy {
			NestedStrategy::Apply => self.byte(0),
			NestedStrategy::Once => self.byte(1),
			NestedStrategy::Hash {
				field,
			} => {
				self.byte(2);
				self.text(field.as_str());
			}
			NestedStrategy::Lookup {
				index,
				hashed,
			} => {
				self.byte(3);
				self.text(index.table.as_str());
				self.text(index.index.as_str());
				self.u32(index.position);
				self.u32(u32::try_from(index.scalar.len()).unwrap_or(u32::MAX));
				for scalar in &index.scalar {
					self.byte(u8::from(*scalar));
				}
				self.byte(u8::from(*hashed));
			}
		}
	}

	fn key_shape(&mut self, shape: &KeyShape) {
		match shape {
			KeyShape::Number(n) => {
				self.byte(0);
				self.i64(*n);
			}
			KeyShape::String(s) => {
				self.byte(1);
				self.text(s.as_str());
			}
			KeyShape::Uuid(u) => {
				self.byte(2);
				self.debug(u);
			}
			KeyShape::Generate(g) => {
				self.byte(3);
				self.debug(g);
			}
			KeyShape::Array {
				len,
			} => {
				self.byte(4);
				self.u32(*len);
			}
			KeyShape::Object {
				keys,
			} => {
				self.byte(5);
				self.names(keys);
			}
			KeyShape::Range(range) => {
				self.byte(6);
				self.bound(&range.start);
				self.bound(&range.end);
			}
		}
	}

	fn bound(&mut self, bound: &Bound<KeyBound>) {
		match bound {
			Bound::Included(key) => {
				self.byte(0);
				self.key_bound(key);
			}
			Bound::Excluded(key) => {
				self.byte(1);
				self.key_bound(key);
			}
			Bound::Unbounded => self.byte(2),
		}
	}

	fn key_bound(&mut self, key: &KeyBound) {
		match key {
			KeyBound::Number(n) => {
				self.byte(0);
				self.i64(*n);
			}
			KeyBound::String(s) => {
				self.byte(1);
				self.text(s.as_str());
			}
			KeyBound::Uuid(u) => {
				self.byte(2);
				self.debug(u);
			}
			KeyBound::Generate(g) => {
				self.byte(3);
				self.debug(g);
			}
			KeyBound::Array {
				len,
			} => {
				self.byte(4);
				self.u32(*len);
			}
			KeyBound::Object {
				keys,
			} => {
				self.byte(5);
				self.names(keys);
			}
		}
	}

	/// The entries of a destructure pattern: the count, then each entry.
	fn entries(&mut self, entries: &[DestructureEntry]) {
		self.u32(u32::try_from(entries.len()).unwrap_or(u32::MAX));
		for entry in entries {
			match entry {
				DestructureEntry::All(f) => {
					self.byte(0);
					self.text(f.as_str());
				}
				DestructureEntry::Field(f) => {
					self.byte(1);
					self.text(f.as_str());
				}
				DestructureEntry::Aliased(f) => {
					self.byte(2);
					self.text(f.as_str());
				}
				DestructureEntry::Destructure(f, nested) => {
					self.byte(3);
					self.text(f.as_str());
					self.entries(nested);
				}
				DestructureEntry::Rest => self.byte(4),
			}
		}
	}
}

impl Canon<Sha256> {
	fn finish(self) -> AttrDigest {
		let full = self.0.finalize();
		let mut head = [0u8; 16];
		head.copy_from_slice(&full[..16]);
		displaced(head)
	}
}

/// `head` as a digest that is never [`AttrDigest::OPAQUE`]: bytes equal to `OPAQUE`'s have their
/// last byte XORed with `0x01`.
fn displaced(mut head: [u8; 16]) -> AttrDigest {
	if AttrDigest(head) == AttrDigest::OPAQUE {
		head[15] ^= 0x01;
	}
	AttrDigest(head)
}

#[cfg(test)]
mod tests {
	use rust_decimal::Decimal;
	use surrealdb_strand::{Strand, TableName};

	use super::*;
	use crate::exec::ir::expr::ops::{
		ClosureAttrs, ComputedReadAttrs, ConvertAttrs, ConvertSite, ExprTag, FoldedError, KeyRange,
		NestedStrategy, RecordIdAttrs,
	};
	use crate::exec::ir::iface::statements::{Place, PlacePart, UnaddressedPart};
	use crate::exec::ir::iface::{CatalogObjectKey, IndexRef};
	use crate::exec::logical::footprint::CatalogKind;
	use crate::exec::logical::pipelines::AggregateCombine;
	use crate::exec::vm::compile_cache::ObjectVersion;
	use crate::expr::logical::LogicalPostfixOperator;
	use crate::expr::{BinaryOperator, Constant, Function, Kind, Mock, PrefixOperator};
	use crate::val::Number;

	fn strand(text: &str) -> Strand {
		Strand::new(text)
	}

	fn table(text: &str) -> TableName {
		TableName::new(strand(text))
	}

	fn tuple_row(places: Vec<Vec<PlacePart>>) -> ExprOp {
		ExprOp::TupleRow {
			places: places
				.into_iter()
				.map(|parts| Place {
					parts: parts.into_boxed_slice(),
				})
				.collect(),
		}
	}

	fn literal(value: Value) -> ExprOp {
		ExprOp::Literal(Box::new(value))
	}

	fn convert(site: u32) -> ExprOp {
		ExprOp::Convert(Box::new(ConvertAttrs {
			mode: ConvertMode::Cast,
			kind: Kind::Int,
			site: ConvertSite(site),
			on_fail: OnFail::Error,
		}))
	}

	fn raise(kind: RaiseKind) -> ExprOp {
		ExprOp::Raise(Box::new(kind))
	}

	fn folded(text: &str) -> ExprOp {
		raise(RaiseKind::Folded(FoldedError {
			text: text.to_owned(),
		}))
	}

	/// The simplest op of each tag.
	fn fixture(tag: ExprTag) -> ExprOp {
		match tag {
			ExprTag::Literal => literal(Value::from(1i64)),
			ExprTag::Array => ExprOp::Array,
			ExprTag::Set => ExprOp::Set,
			ExprTag::Object => ExprOp::Object {
				keys: Box::new(["a".to_owned()]),
			},
			ExprTag::RecordId => ExprOp::RecordId(Box::new(RecordIdAttrs {
				table: table("t"),
				key_shape: KeyShape::Range(Box::new(KeyRange {
					start: Bound::Included(KeyBound::Number(1)),
					end: Bound::Unbounded,
				})),
			})),
			ExprTag::FreeParam => ExprOp::FreeParam {
				name: strand("q"),
			},
			ExprTag::SessionParam => ExprOp::SessionParam {
				name: strand("auth"),
			},
			ExprTag::CatalogParam => ExprOp::CatalogParam {
				name: strand("p"),
			},
			ExprTag::Table => ExprOp::Table {
				name: table("t"),
			},
			ExprTag::Mock => ExprOp::Mock(Box::new(Mock::Count(table("t"), 1))),
			ExprTag::Constant => ExprOp::Constant {
				c: Constant::MathE,
			},
			ExprTag::Field => ExprOp::Field {
				name: strand("a"),
			},
			ExprTag::ComputedRead => ExprOp::ComputedRead(Box::new(ComputedReadAttrs {
				field: strand("a"),
				def: (
					CatalogObjectKey {
						kind: CatalogKind::Field,
						ns: None,
						db: None,
						table: Some(table("t")),
						name: Some(strand("a")),
					},
					ObjectVersion::Inline,
				),
			})),
			ExprTag::Doc => ExprOp::Doc,
			ExprTag::Access => ExprOp::Access {
				field: strand("a"),
			},
			ExprTag::Deref => ExprOp::Deref {
				table_hint: Some(Box::new(table("t"))),
			},
			ExprTag::Index => ExprOp::Index,
			ExprTag::First => ExprOp::First,
			ExprTag::Last => ExprOp::Last,
			ExprTag::Each => ExprOp::Each,
			ExprTag::Flatten => ExprOp::Flatten {
				explicit: true,
			},
			ExprTag::Dispatch => ExprOp::Dispatch(Box::new(DispatchTarget::Method(strand("len")))),
			ExprTag::Method => ExprOp::Method {
				name: strand("len"),
			},
			ExprTag::Destructure => ExprOp::Destructure {
				shape: Box::new([
					DestructureEntry::Field(strand("a")),
					DestructureEntry::Destructure(
						strand("b"),
						Box::new([DestructureEntry::Aliased(strand("c")), DestructureEntry::Rest]),
					),
				]),
			},
			ExprTag::Optional => ExprOp::Optional,
			ExprTag::RepeatRecurse => ExprOp::RepeatRecurse,
			ExprTag::SetField => ExprOp::SetField {
				field: strand("a"),
			},
			ExprTag::TupleRow => tuple_row(vec![vec![PlacePart::Field(strand("a"))]]),
			ExprTag::Prefix => ExprOp::Prefix(Box::new(PrefixOperator::Not)),
			ExprTag::Convert => convert(0),
			ExprTag::Postfix => ExprOp::Postfix {
				op: LogicalPostfixOperator::Range,
			},
			ExprTag::Invoke => ExprOp::Invoke,
			ExprTag::Binary => ExprOp::Binary {
				op: BinaryOperator::Add,
			},
			ExprTag::ShortCircuit => ExprOp::ShortCircuit {
				op: ShortCircuitOp::And,
			},
			ExprTag::Call => ExprOp::call(Function::Normal("math::abs".to_owned())),
			ExprTag::ClosureCreate => ExprOp::ClosureCreate(Box::new(ClosureAttrs {
				params: Box::new([]),
				returns: None,
				body_sql: "1".into(),
				captures: Box::new([]),
				reads_row: false,
				reads_root: false,
				literal: Default::default(),
			})),
			ExprTag::If => ExprOp::If,
			ExprTag::Throw => ExprOp::Throw,
			ExprTag::Subquery => ExprOp::Subquery(NestedStrategy::Apply),
			ExprTag::Yield => ExprOp::Yield,
			ExprTag::Truthy => ExprOp::Truthy,
			ExprTag::Agg => ExprOp::Agg {
				kind: "count",
				combine: AggregateCombine::Exact,
			},
			ExprTag::Now => ExprOp::Now {
				which: NowWhich::Datetime,
			},
			ExprTag::Raise => raise(RaiseKind::InvalidControlFlow),
			ExprTag::DepthCharge => ExprOp::DepthCharge {
				levels: 1,
			},
		}
	}

	/// Asserts that no two ops of `ops` share a digest or have equal attributes, and that each
	/// has attributes equal to its own.
	fn assert_all_distinct(ops: &[ExprOp]) {
		for (i, a) in ops.iter().enumerate() {
			assert!(a.attrs_eq(a), "{a:?} differs from itself");
			for b in &ops[i + 1..] {
				assert_ne!(a.attr_digest(), b.attr_digest(), "{a:?} collides with {b:?}");
				assert!(!a.attrs_eq(b), "{a:?} has the attributes of {b:?}");
			}
		}
	}

	/// `1`, `1f` and `1dec` compare equal as values, and are three sets of attributes.
	#[test]
	fn equal_numbers_of_other_kinds_are_other_attributes() {
		let int = literal(Value::from(1i64));
		let float = literal(Value::Number(Number::Float(1.0)));
		let decimal = literal(Value::Number(Number::Decimal(Decimal::ONE)));
		assert_eq!(Value::from(1i64), Value::Number(Number::Float(1.0)));
		assert!(int.attrs_eq(&literal(Value::from(1i64))));
		assert!(!int.attrs_eq(&float));
		assert!(!int.attrs_eq(&decimal));
		assert!(!float.attrs_eq(&decimal));
		assert!(
			!literal(Value::Number(Number::Float(0.0)))
				.attrs_eq(&literal(Value::Number(Number::Float(-0.0))))
		);
	}

	#[test]
	fn literal_kinds_never_collide() {
		assert_all_distinct(&[
			literal(Value::from(1i64)),
			literal(Value::Number(Number::Float(1.0))),
			literal(Value::Number(Number::Decimal(Decimal::ONE))),
			literal(Value::Number(Number::Float(0.0))),
			literal(Value::Number(Number::Float(-0.0))),
			literal(Value::None),
			literal(Value::Null),
			literal(Value::from("1")),
		]);
	}

	#[test]
	fn attributes_decide_the_digest() {
		let field = |name: &str| ExprOp::Field {
			name: strand(name),
		};
		let names = |keys: [&str; 2]| ExprOp::Object {
			keys: keys.map(str::to_owned).into(),
		};
		let binary = |op: BinaryOperator| ExprOp::Binary {
			op,
		};
		assert_all_distinct(&[
			field("a"),
			field("b"),
			ExprOp::Access {
				field: strand("a"),
			},
			convert(0),
			convert(1),
			binary(BinaryOperator::Add),
			binary(BinaryOperator::Subtract),
			folded("x"),
			folded("y"),
			raise(RaiseKind::InvalidControlFlow),
			names(["ab", "c"]),
			names(["a", "bc"]),
			tuple_row(vec![vec![PlacePart::Field(strand("a"))]]),
			tuple_row(vec![vec![PlacePart::Field(strand("b"))]]),
			tuple_row(vec![vec![PlacePart::Field(strand("a")), PlacePart::First]]),
			tuple_row(vec![vec![PlacePart::Field(strand("a")), PlacePart::Last]]),
			tuple_row(vec![vec![PlacePart::Field(strand("a")), PlacePart::Dynamic(1)]]),
			tuple_row(vec![vec![PlacePart::Field(strand("a")), PlacePart::Index(1)]]),
			tuple_row(vec![vec![PlacePart::Field(strand("a")), PlacePart::Filter(0)]]),
			tuple_row(vec![
				vec![PlacePart::Field(strand("a"))],
				vec![PlacePart::Field(strand("b"))],
			]),
			tuple_row(vec![
				vec![PlacePart::Field(strand("b"))],
				vec![PlacePart::Field(strand("a"))],
			]),
			tuple_row(vec![vec![PlacePart::Unaddressed(UnaddressedPart::Method, "x()".into())]]),
			tuple_row(vec![vec![PlacePart::Unaddressed(UnaddressedPart::Method, "y()".into())]]),
		]);
	}

	#[test]
	fn the_strategy_of_a_subquery_decides_the_digest() {
		let index = |position: u32, scalar: &[bool]| IndexRef {
			table: table("t"),
			index: strand("ix"),
			position,
			scalar: scalar.into(),
		};
		let lookup = |index: IndexRef, hashed: bool| {
			ExprOp::Subquery(NestedStrategy::Lookup {
				index: Box::new(index),
				hashed,
			})
		};
		let hash = |field: &str| {
			ExprOp::Subquery(NestedStrategy::Hash {
				field: strand(field),
			})
		};
		assert_all_distinct(&[
			ExprOp::Subquery(NestedStrategy::Apply),
			ExprOp::Subquery(NestedStrategy::Once),
			hash("a"),
			hash("b"),
			lookup(index(0, &[]), false),
			lookup(index(0, &[]), true),
			lookup(index(1, &[]), false),
			lookup(index(0, &[true]), false),
			lookup(index(0, &[false]), false),
		]);
	}

	#[test]
	fn equal_attributes_equal_digests() {
		for tag in ExprTag::ALL {
			assert_eq!(
				fixture(*tag).attr_digest(),
				fixture(*tag).attr_digest(),
				"{} digests differently on two builds",
				tag.mnemonic()
			);
		}
		assert_eq!(convert(7).attr_digest(), convert(7).attr_digest());
	}

	#[test]
	fn only_closure_create_is_opaque() {
		for tag in ExprTag::ALL {
			let opaque = fixture(*tag).attr_digest() == AttrDigest::OPAQUE;
			assert_eq!(opaque, *tag == ExprTag::ClosureCreate, "{}", tag.mnemonic());
		}
	}

	#[test]
	fn a_real_digest_that_equals_opaque_is_displaced() {
		let moved = displaced([0xFF; 16]);
		assert_ne!(moved, AttrDigest::OPAQUE);
		assert_eq!(moved.0[15], 0xFE);
		assert_eq!(moved.0[..15], [0xFF; 15]);
		let other = [7u8; 16];
		assert_eq!(displaced(other), AttrDigest(other));
	}
}
