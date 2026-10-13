//! Size and effort bounds the IR guarantees to its consumers.
//!
//! Each constant is a contract: a producer stays within it, and a consumer may rely on it.

/// The most bytes one op's inline data occupies on a 64-bit target: the op's kind (a dialect op
/// of at most 32 bytes under a tag), its operand, result and region lists, its location and its
/// provenance. Payloads larger than 24 bytes are boxed in their variant, so a container's op
/// storage stays dense. The bound is the measured layout: growing `OpData` past it fails the
/// build.
pub(crate) const OP_DATA_MAX_BYTES: usize = 208;

/// The most bytes of encoded result a plan-time fold produces. A larger result is not folded.
pub(crate) const FOLD_MAX_BYTES: usize = 1 << 20;

/// The instruction budget of the restricted host a plan-time fold runs in. A fold that exhausts
/// it is not folded.
pub(crate) const FOLD_FUEL: u64 = 100_000;

/// The most iterations a loop may have and still be unrolled. Past it the loop stays a loop.
pub(crate) const UNROLL_MAX_ITERATIONS: u32 = 64;

/// The most ops unrolling one loop may create. Past it the loop stays a loop.
pub(crate) const UNROLL_MAX_OPS: u32 = 4_096;
