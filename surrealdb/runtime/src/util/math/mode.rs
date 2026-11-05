use std::collections::BTreeMap;

use surrealdb_expr::val::Number;

use crate::fuel;

/// The most frequent number in `array`, the greatest of those equally
/// frequent, or NaN for an empty one: a walk of the numbers counting each,
/// then of the distinct numbers.
pub async fn mode(array: Vec<Number>) -> Number {
	let frequencies = fuel::fold(array, BTreeMap::new(), |mut freqs, value| {
		*freqs.entry(value).or_insert(0u32) += 1;
		freqs
	})
	.await;
	let mut best: Option<(Number, u32)> = None;
	fuel::walk(frequencies, |(value, n)| {
		if best.clone().is_none_or(|(_, most)| n >= most) {
			best = Some((value, n));
		}
		std::ops::ControlFlow::Continue(())
	})
	.await;
	best.map(|(value, _)| value).unwrap_or(Number::NAN)
}
