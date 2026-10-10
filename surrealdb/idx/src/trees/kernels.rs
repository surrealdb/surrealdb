//! Distance kernels and a memory hint for the vector indexes.
//!
//! On 64-bit non-WASM targets (the `diskann` cfg) the `f32` kernels are
//! `diskann-vector`'s: they select the widest instruction set the CPU offers —
//! AVX-512, AVX2, NEON — when they first run, whatever the build targets.
//! Elsewhere they are portable Rust. Both accumulate in `f32` and sum in
//! different orders, so the same pair of vectors can differ in the low digits
//! across targets; within one process a pair always scores the same.

#[cfg(diskann)]
use diskann_vector::distance::{InnerProduct, SquaredL2};
#[cfg(diskann)]
use diskann_vector::{MathematicalValue, PureDistanceFunction};

/// The dot product of two `f32` vectors of equal length.
#[cfg(diskann)]
#[inline]
pub(crate) fn dot_f32(a: &[f32], b: &[f32]) -> f32 {
	let value: MathematicalValue<f32> =
		<InnerProduct as PureDistanceFunction<&[f32], &[f32], MathematicalValue<f32>>>::evaluate(
			a, b,
		);
	value.into_inner()
}

/// The dot product of two `f32` vectors of equal length.
#[cfg(not(diskann))]
#[inline]
pub(crate) fn dot_f32(a: &[f32], b: &[f32]) -> f32 {
	ndarray::ArrayView1::from(a).dot(&ndarray::ArrayView1::from(b))
}

/// The squared Euclidean distance between two `f32` vectors of equal length.
#[cfg(diskann)]
#[inline]
pub(crate) fn squared_l2_f32(a: &[f32], b: &[f32]) -> f32 {
	let value: MathematicalValue<f32> =
		<SquaredL2 as PureDistanceFunction<&[f32], &[f32], MathematicalValue<f32>>>::evaluate(a, b);
	value.into_inner()
}

/// The squared Euclidean distance between two `f32` vectors of equal length.
#[cfg(not(diskann))]
#[inline]
pub(crate) fn squared_l2_f32(a: &[f32], b: &[f32]) -> f32 {
	a.iter()
		.zip(b)
		.map(|(x, y)| {
			let d = x - y;
			d * d
		})
		.sum()
}

/// The cache line size the prefetch steps by.
#[cfg(any(target_arch = "x86_64", test))]
const CACHE_LINE: usize = 64;

/// Asks the CPU to start loading `values` into its caches, so that a
/// computation reading them shortly after does not stall on memory.
///
/// A hint only: it changes no value, reads nothing into the program and cannot
/// fault. On targets other than `x86_64` it does nothing.
#[inline]
pub(crate) fn prefetch<T>(values: &[T]) {
	#[cfg(target_arch = "x86_64")]
	{
		use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};

		let start = values.as_ptr().cast::<i8>();
		for offset in line_offsets(start.addr(), std::mem::size_of_val(values)) {
			// SAFETY: `line_offsets` yields offsets below the slice's length in
			// bytes, so `start.add(offset)` stays within the allocation
			// `values` borrows and the pointer arithmetic is in bounds.
			// `_mm_prefetch` needs SSE, which every `x86_64` target has; the
			// instruction only hints the cache hierarchy, so the address is
			// never dereferenced by the program and a prefetch cannot fault.
			unsafe { _mm_prefetch::<_MM_HINT_T0>(start.add(offset)) };
		}
	}
	#[cfg(not(target_arch = "x86_64"))]
	let _ = values;
}

/// The offsets into a slice of `bytes` bytes at address `addr` at which one
/// prefetch per cache line reaches every line the slice spans: its first
/// byte, then the first byte of each later line. A slice rarely starts on a
/// line boundary, so it can span one line more than its length alone
/// suggests. Every offset is below `bytes`, and none is yielded for an empty
/// slice.
#[cfg(any(target_arch = "x86_64", test))]
#[inline]
fn line_offsets(addr: usize, bytes: usize) -> impl Iterator<Item = usize> {
	let next_line = CACHE_LINE - addr % CACHE_LINE;
	(bytes > 0).then_some(0).into_iter().chain((next_line..bytes).step_by(CACHE_LINE))
}

#[cfg(test)]
mod tests {
	use super::*;

	fn vectors(len: usize) -> (Vec<f32>, Vec<f32>) {
		let a = (0..len).map(|i| ((i * 37 % 101) as f32 - 50.0) / 7.0).collect();
		let b = (0..len).map(|i| ((i * 53 % 97) as f32 - 48.0) / 5.0).collect();
		(a, b)
	}

	/// The kernels agree with an `f64` reference to `f32` precision, over
	/// lengths that exercise every tail a SIMD kernel can leave, including
	/// empty vectors.
	#[test]
	fn kernels_match_an_f64_reference() {
		for len in [0, 1, 3, 7, 8, 15, 16, 17, 31, 64, 100, 768, 1023] {
			let (a, b) = vectors(len);
			let dot: f64 = a.iter().zip(&b).map(|(x, y)| *x as f64 * *y as f64).sum();
			let l2: f64 = a.iter().zip(&b).map(|(x, y)| (*x as f64 - *y as f64).powi(2)).sum();
			let scale = a.iter().zip(&b).map(|(x, y)| (*x as f64 * *y as f64).abs()).sum::<f64>();
			assert!(
				(dot_f32(&a, &b) as f64 - dot).abs() <= 1e-5 * scale.max(1.0),
				"dot, len {len}"
			);
			assert!(
				(squared_l2_f32(&a, &b) as f64 - l2).abs() <= 1e-5 * l2.max(1.0),
				"squared L2, len {len}"
			);
		}
	}

	/// One offset per cache line a slice spans, at every alignment and length:
	/// each in bounds, on a distinct line, and together covering the line of
	/// every byte.
	#[test]
	fn line_offsets_reach_every_line_once() {
		for misalign in 0..CACHE_LINE {
			let addr = 4096 + misalign;
			for bytes in [0, 1, 4, 31, 32, 33, 63, 64, 65, 96, 127, 128, 129, 3072, 3073] {
				let offsets: Vec<usize> = line_offsets(addr, bytes).collect();
				assert!(offsets.iter().all(|&o| o < bytes), "in bounds: {misalign} {bytes}");
				let lines: Vec<usize> = offsets.iter().map(|o| (addr + o) / CACHE_LINE).collect();
				let spanned: Vec<usize> = if bytes == 0 {
					Vec::new()
				} else {
					(addr / CACHE_LINE..=(addr + bytes - 1) / CACHE_LINE).collect()
				};
				assert_eq!(lines, spanned, "lines: misalign {misalign}, bytes {bytes}");
			}
		}
	}

	/// A prefetch changes nothing, for every length and alignment of slice,
	/// empty and one-element slices and slices ending mid cache line included.
	#[test]
	fn prefetch_changes_nothing() {
		let values: Vec<f32> = (0..1000).map(|i| i as f32).collect();
		let before = values.clone();
		for start in 0..17 {
			for end in [start, start + 1, start + 15, start + 16, start + 17, 1000] {
				prefetch(&values[start..end]);
			}
		}
		prefetch::<u8>(&[]);
		prefetch(&[0u8; 3]);
		assert_eq!(values, before);
	}
}
