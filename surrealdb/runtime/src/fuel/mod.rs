//! The fuel a running program spends, and how a function walking a
//! collection spends it.
//!
//! Every unit of work a running program does draws on one meter, the
//! thread's: a row a pipeline takes, an iteration of a loop, the elements a
//! host service or a function walks. The executor refills the meter with its
//! quantum at its yield point; between two yield points a thread does at most
//! a quantum of units. The executor's side — when it yields, what it checks
//! there, and through which scheduler — lives in the executor; this module
//! holds the meter itself, so that the functions in this crate draw on the
//! same one.
//!
//! # Walks
//!
//! A function whose work grows with its input is written as an `async fn`
//! that walks its input a stretch at a time ([`walk`], [`map`], [`fold`],
//! [`sort_by`]) and charges each stretch with [`tick`]. When a charge spends
//! the meter, the walk asks for a yield point and is pending once. What
//! answers depends on who drives it:
//!
//! - the executor drives it through its own wrapper, which reaches its yield point (the deadline
//!   and cancellation checked, the meter refilled, the thread given up) and polls the walk again;
//! - a synchronous caller drives it with [`now`], which polls it again at once: the charges stand,
//!   so the executor yields at its next yield point.
//!
//! A walk polled by a driver that knows neither is woken as it pends, so it
//! behaves as a plain yield: no driver is left waiting on a walk.

mod sort;

use std::cell::Cell;
use std::future::Future;
use std::ops::ControlFlow;
use std::pin::{Pin, pin};
use std::task::{Context, Poll, Waker};

pub use sort::{LoserTree, sort_by};

/// The units a thread holds before any program on it has yielded.
const INITIAL: u32 = 4096;

/// The most elements a walk takes between two charges.
const STRETCH: u32 = 256;

/// The bytes a walk over a string or a byte buffer takes for a unit.
pub const BYTES_PER_UNIT: usize = 64;

thread_local! {
	/// The units this thread's polls may still spend before they yield.
	static LEFT: Cell<u32> = const { Cell::new(INITIAL) };
	/// The quantum the thread's meter was last refilled or capped with.
	static QUANTUM: Cell<u32> = const { Cell::new(INITIAL) };
	/// Whether a walk has spent the meter and is pending until its driver
	/// reaches a yield point.
	static ASKED: Cell<bool> = const { Cell::new(false) };
}

/// Spend `units` of the thread's fuel, answering whether the meter is spent.
#[inline]
pub fn charge(units: u32) -> bool {
	LEFT.with(|left| {
		let rest = left.get().saturating_sub(units);
		left.set(rest);
		rest == 0
	})
}

/// Whether the thread's fuel is spent.
#[inline]
pub fn spent() -> bool {
	LEFT.with(|left| left.get() == 0)
}

/// The units the thread may still spend.
#[inline]
pub fn left() -> u32 {
	LEFT.with(Cell::get)
}

/// The quantum the thread's meter was last refilled or capped with: the
/// most elements a walk takes in one go, such as one sorted run.
#[inline]
pub fn quantum() -> u32 {
	QUANTUM.with(Cell::get)
}

/// Refill the thread's fuel with `quantum` units.
#[inline]
pub fn refill(quantum: u32) {
	let quantum = quantum.max(1);
	LEFT.with(|left| left.set(quantum));
	QUANTUM.with(|held| held.set(quantum));
}

/// Hold the thread's fuel to at most `quantum` units, and take `quantum` as
/// the thread's quantum: a statement starting under a smaller quantum than
/// the thread's last refill spends no more than its own before its first
/// yield.
#[inline]
pub fn cap(quantum: u32) {
	let quantum = quantum.max(1);
	LEFT.with(|left| left.set(left.get().min(quantum)));
	QUANTUM.with(|held| held.set(quantum));
}

/// The units a walk of `len` elements costs, saturated at the meter's width.
#[inline]
pub fn units(len: usize) -> u32 {
	u32::try_from(len).unwrap_or(u32::MAX)
}

/// The units a walk over `len` bytes costs, at least one.
#[inline]
pub fn byte_units(len: usize) -> u32 {
	units(len / BYTES_PER_UNIT + 1)
}

/// The elements a walk takes before its next charge: no more than the fuel
/// left, so a stretch ends where the meter is spent. Under a driver that
/// answers a request for a yield point at once ([`now`]) the meter stays
/// spent, and a stretch is a full one.
#[inline]
pub fn stretch() -> usize {
	match left() {
		0 => STRETCH as usize,
		left => left.min(STRETCH) as usize,
	}
}

/// The elements a walk takes next: a yield point first when the meter is
/// already spent — by a charge that came before the walk — so that no
/// stretch starts on fuel that is gone.
#[inline]
pub async fn next_stretch() -> usize {
	if spent() {
		Asked(false).await
	}
	stretch()
}

/// Spend `units` of the thread's fuel; once that spends the meter, ask the
/// walk's driver for a yield point and return once it has reached one.
#[inline]
pub async fn tick(units: u32) {
	if charge(units) {
		Asked(false).await
	}
}

/// Whether a walk has asked for a yield point since the last call, clearing
/// the request: a driver calls it before polling a walk, to drop a stale
/// request, and after the walk is pending, to tell a request from a wait.
#[inline]
pub fn take_asked() -> bool {
	ASKED.with(|asked| asked.replace(false))
}

/// A walk's request for a yield point: pending once, with the request
/// raised and its task woken, then ready.
struct Asked(bool);

impl Future for Asked {
	type Output = ();

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
		if self.0 {
			return Poll::Ready(());
		}
		self.0 = true;
		ASKED.with(|asked| asked.set(true));
		cx.waker().wake_by_ref();
		Poll::Pending
	}
}

/// Drive `walk` to its end on this thread without yielding: each request for
/// a yield point is answered at once, and the units it charged stand.
///
/// # Panics
///
/// When `walk` waits on anything but its own yield points: a walk is CPU
/// work, and no one would wake it.
pub fn now<F: Future>(walk: F) -> F::Output {
	let mut walk = pin!(walk);
	let mut cx = Context::from_waker(Waker::noop());
	loop {
		take_asked();
		if let Poll::Ready(out) = walk.as_mut().poll(&mut cx) {
			return out;
		}
		assert!(
			take_asked(),
			"a walk driven synchronously waited on something but its yield point"
		);
	}
}

/// Walk `items`, calling `f` on each until it breaks, a stretch at a time,
/// a unit an element. Answers whether the walk reached the end.
pub async fn walk<I: IntoIterator>(
	items: I,
	mut f: impl FnMut(I::Item) -> ControlFlow<()>,
) -> bool {
	let mut items = items.into_iter();
	loop {
		let stretch = next_stretch().await;
		let mut taken = 0;
		while taken < stretch {
			let Some(item) = items.next() else {
				tick(units(taken)).await;
				return true;
			};
			taken += 1;
			if f(item).is_break() {
				tick(units(taken)).await;
				return false;
			}
		}
		tick(units(taken)).await;
	}
}

/// `out` extended by `items`, a stretch at a time, a unit an element.
pub async fn extend<T>(out: &mut Vec<T>, items: impl IntoIterator<Item = T>) {
	let mut items = items.into_iter();
	loop {
		let stretch = next_stretch().await;
		let before = out.len();
		out.extend(items.by_ref().take(stretch));
		let taken = out.len() - before;
		tick(units(taken)).await;
		if taken < stretch {
			return;
		}
	}
}

/// `items` mapped by `f`, a stretch at a time, a unit an element.
pub async fn map<I: IntoIterator, U>(items: I, mut f: impl FnMut(I::Item) -> U) -> Vec<U> {
	let items = items.into_iter();
	let mut out = Vec::with_capacity(items.size_hint().0);
	walk(items, |item| {
		out.push(f(item));
		ControlFlow::Continue(())
	})
	.await;
	out
}

/// `items` mapped by `f` until it fails, a stretch at a time, a unit an
/// element.
pub async fn try_map<I: IntoIterator, U, E>(
	items: I,
	mut f: impl FnMut(I::Item) -> Result<U, E>,
) -> Result<Vec<U>, E> {
	let items = items.into_iter();
	let mut out = Vec::with_capacity(items.size_hint().0);
	let mut failed = None;
	walk(items, |item| match f(item) {
		Ok(value) => {
			out.push(value);
			ControlFlow::Continue(())
		}
		Err(e) => {
			failed = Some(e);
			ControlFlow::Break(())
		}
	})
	.await;
	match failed {
		Some(e) => Err(e),
		None => Ok(out),
	}
}

/// `items` folded into `init` by `f`, a stretch at a time, a unit an
/// element.
pub async fn fold<I: IntoIterator, A>(items: I, init: A, mut f: impl FnMut(A, I::Item) -> A) -> A {
	let mut acc = Some(init);
	walk(items, |item| {
		acc = acc.take().map(|acc| f(acc, item));
		ControlFlow::Continue(())
	})
	.await;
	acc.expect("a fold holds its accumulator between elements")
}

/// `items` folded into `init` by `f` until it fails, a stretch at a time, a
/// unit an element.
pub async fn try_fold<I: IntoIterator, A, E>(
	items: I,
	init: A,
	mut f: impl FnMut(A, I::Item) -> Result<A, E>,
) -> Result<A, E> {
	let mut acc = Some(init);
	let mut failed = None;
	walk(items, |item| {
		let held = acc.take().expect("a fold holds its accumulator between elements");
		match f(held, item) {
			Ok(next) => {
				acc = Some(next);
				ControlFlow::Continue(())
			}
			Err(e) => {
				failed = Some(e);
				ControlFlow::Break(())
			}
		}
	})
	.await;
	match failed {
		Some(e) => Err(e),
		None => Ok(acc.expect("a fold holds its accumulator between elements")),
	}
}

/// Whether `f` holds for any of `items`, a stretch at a time, a unit an
/// element, stopping at the first that does.
pub async fn any<I: IntoIterator>(items: I, mut f: impl FnMut(I::Item) -> bool) -> bool {
	!walk(items, |item| {
		if f(item) {
			ControlFlow::Break(())
		} else {
			ControlFlow::Continue(())
		}
	})
	.await
}

/// The first of `items` `f` holds for, a stretch at a time, a unit an
/// element.
pub async fn find<I: IntoIterator>(
	items: I,
	mut f: impl FnMut(&I::Item) -> bool,
) -> Option<I::Item> {
	let mut found = None;
	walk(items, |item| {
		if f(&item) {
			found = Some(item);
			ControlFlow::Break(())
		} else {
			ControlFlow::Continue(())
		}
	})
	.await;
	found
}

#[cfg(test)]
mod tests {
	use super::*;

	/// A walk driven synchronously charges its elements and reaches its end,
	/// however often it spends the meter.
	#[test]
	fn a_walk_driven_now_charges_and_finishes() {
		refill(10);
		let out = now(map(0..1000, |i| i * 2));
		assert_eq!(out.len(), 1000);
		assert_eq!(out[999], 1998);
		assert!(spent());
		refill(INITIAL);
	}

	/// A walk asks for a yield point once per quantum of elements.
	#[test]
	fn a_walk_asks_once_per_quantum() {
		refill(10);
		let mut walk = pin!(fold(0..100u32, 0u32, |a, i| a + i));
		let mut cx = Context::from_waker(Waker::noop());
		let mut asked = 0;
		let total = loop {
			match walk.as_mut().poll(&mut cx) {
				Poll::Ready(total) => break total,
				Poll::Pending => {
					assert!(take_asked());
					asked += 1;
					refill(10);
				}
			}
		};
		assert_eq!(total, 4950);
		assert_eq!(asked, 10);
		refill(INITIAL);
	}
}
