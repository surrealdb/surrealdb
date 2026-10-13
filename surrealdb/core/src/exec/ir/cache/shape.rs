//! The structural digest of a statement, which keys a unit compiled for that shape.
//!
//! The digest is process-local: it feeds the AST's derived [`Hash`] into SHA-256, and `Hash` is not
//! a stable encoding across builds, platforms or releases. It keys nothing persisted.

use std::hash::{Hash, Hasher};

use sha2::{Digest, Sha256};
use surrealdb_expr::parser::TopLevelExpr;

use crate::exec::ir::iface::premise::ShapeDigest;

/// The domain tag every digest starts with.
const DOMAIN: &[u8] = b"surrealdb.ir.shape\0";

/// A [`Hasher`] that streams every written byte into a SHA-256 state.
///
/// Only the bytes matter: [`Hasher::finish`] has no 64-bit result to give and is never used.
struct Sha256Hasher(Sha256);

impl Hasher for Sha256Hasher {
	fn write(&mut self, bytes: &[u8]) {
		self.0.update(bytes);
	}

	fn finish(&self) -> u64 {
		0
	}
}

/// The process-local structural digest of `stmt`: SHA-256 over a domain tag, then the AST's derived
/// `Hash` fed through a hashing adapter. Equal ASTs give equal digests within one build and
/// process. It is not a stable encoding and keys nothing persisted.
pub(crate) fn digest(stmt: &TopLevelExpr) -> ShapeDigest {
	let mut hasher = Sha256Hasher(Sha256::new_with_prefix(DOMAIN));
	stmt.hash(&mut hasher);
	ShapeDigest(hasher.0.finalize().into())
}

/// The 64-bit unit key of a digest: its first 8 bytes, little-endian. Keys
/// `UnitObject::DynamicTargetArm`.
pub(crate) fn arm_key(digest: &ShapeDigest) -> u64 {
	let mut head = [0u8; 8];
	head.copy_from_slice(&digest.0[..8]);
	u64::from_le_bytes(head)
}
