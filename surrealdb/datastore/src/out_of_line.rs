//! A future polled through a function pointer taken where it was built.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A future polled through a function pointer taken where it was built.
///
/// An `async fn` body is generated in every crate that polls it. Wrapping the
/// body in `OutOfLine` from a non-generic `#[inline(never)]` constructor takes the
/// poll function's address in the defining crate, so the body is generated once
/// there however many crates await the constructor's result.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct OutOfLine<F: Future> {
	fut: F,
	poll: fn(Pin<&mut F>, &mut Context<'_>) -> Poll<F::Output>,
}

impl<F: Future> OutOfLine<F> {
	/// Wraps `fut`, taking the address of its poll function in the calling crate.
	#[inline(always)]
	pub fn new(fut: F) -> Self {
		Self {
			fut,
			poll: <F as Future>::poll,
		}
	}
}

impl<F: Future> Future for OutOfLine<F> {
	type Output = F::Output;

	#[inline]
	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
		// SAFETY: `fut` is pinned whenever `self` is: it is never moved out of
		// `self`, and `OutOfLine` implements neither `Unpin` by hand nor `Drop`.
		let this = unsafe { self.get_unchecked_mut() };
		(this.poll)(unsafe { Pin::new_unchecked(&mut this.fut) }, cx)
	}
}
