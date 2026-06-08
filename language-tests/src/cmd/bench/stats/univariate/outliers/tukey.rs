//! Tukey's method
//!
//! The original method uses two "fences" to classify the data. All the observations "inside" the
//! fences are considered "normal", and the rest are considered outliers.
//!
//! The fences are computed from the quartiles of the sample, according to the following formula:
//!
//! ``` ignore
//! // q1, q3 are the first and third quartiles
//! let iqr = q3 - q1;  // The interquartile range
//! let (f1, f2) = (q1 - 1.5 * iqr, q3 + 1.5 * iqr);  // the "fences"
//!
//! let is_outlier = |x| if x > f1 && x < f2 { true } else { false };
//! ```
//!
//! The classifier provided here adds two extra outer fences:
//!
//! ``` ignore
//! let (f3, f4) = (q1 - 3 * iqr, q3 + 3 * iqr);  // the outer "fences"
//! ```
//!
//! The extra fences add a sense of "severity" to the classification. Data points outside of the
//! outer fences are considered "severe" outliers, whereas points outside the inner fences are just
//! "mild" outliers, and, as the original method, everything inside the inner fences is considered
//! "normal" data.
//!
//! Some ASCII art for the visually oriented people:
//!
//! ``` ignore
//!          LOW-ish                NORMAL-ish                 HIGH-ish
//!         x   |       +    |  o o  o    o   o o  o  |        +   |   x
//!             f3           f1                       f2           f4
//!
//! Legend:
//! o: "normal" data (not an outlier)
//! +: "mild" outlier
//! x: "severe" outlier
//! ```

use std::ops::{Deref, Index};
use std::slice;

use crate::cmd::bench::stats::univariate::Sample;

use surrealdb_types::SurrealValue;

use self::Label::*;

/// A classified/labeled sample.
///
/// The labeled data can be accessed using the indexing operator. The order of the data points is
/// retained.
///
/// NOTE: Due to limitations in the indexing traits, only the label is returned. Once the
/// `IndexGet` trait lands in stdlib, the indexing operation will return a `(data_point, label)`
/// pair.
#[derive(Clone, Copy)]
pub struct LabeledSample<'a> {
	fences: (f64, f64, f64, f64),
	sample: &'a Sample,
}

impl<'a> LabeledSample<'a> {
	/// Returns the number of data points per label
	///
	/// - Time: `O(length)`
	#[allow(clippy::similar_names)]
	pub fn count(&self) -> (usize, usize, usize, usize, usize) {
		let (mut los, mut lom, mut noa, mut him, mut his) = (0, 0, 0, 0, 0);

		for (_, label) in self {
			match label {
				LowSevere => {
					los += 1;
				}
				LowMild => {
					lom += 1;
				}
				NotAnOutlier => {
					noa += 1;
				}
				HighMild => {
					him += 1;
				}
				HighSevere => {
					his += 1;
				}
			}
		}

		(los, lom, noa, him, his)
	}

	/// Returns the fences used to classify the outliers
	pub fn fences(&self) -> (f64, f64, f64, f64) {
		self.fences
	}

	/// Returns an iterator over the labeled data
	pub fn iter(&self) -> Iter<'a> {
		Iter {
			fences: self.fences,
			iter: self.sample.iter(),
		}
	}
}

impl<'a> Deref for LabeledSample<'a> {
	type Target = Sample;

	fn deref(&self) -> &Sample {
		self.sample
	}
}

// FIXME Use the `IndexGet` trait
impl<'a> Index<usize> for LabeledSample<'a> {
	type Output = Label;

	#[allow(clippy::similar_names)]
	fn index(&self, i: usize) -> &Label {
		static LOW_SEVERE: Label = LowSevere;
		static LOW_MILD: Label = LowMild;
		static HIGH_MILD: Label = HighMild;
		static HIGH_SEVERE: Label = HighSevere;
		static NOT_AN_OUTLIER: Label = NotAnOutlier;

		let x = self.sample[i];
		let (lost, lomt, himt, hist) = self.fences;

		if x < lost {
			&LOW_SEVERE
		} else if x > hist {
			&HIGH_SEVERE
		} else if x < lomt {
			&LOW_MILD
		} else if x > himt {
			&HIGH_MILD
		} else {
			&NOT_AN_OUTLIER
		}
	}
}

impl<'a> IntoIterator for &LabeledSample<'a> {
	type Item = (f64, Label);
	type IntoIter = Iter<'a>;

	fn into_iter(self) -> Iter<'a> {
		self.iter()
	}
}

/// Iterator over the labeled data
pub struct Iter<'a> {
	fences: (f64, f64, f64, f64),
	iter: slice::Iter<'a, f64>,
}

impl<'a> Iterator for Iter<'a> {
	type Item = (f64, Label);

	#[allow(clippy::similar_names)]
	fn next(&mut self) -> Option<(f64, Label)> {
		self.iter.next().map(|&x| {
			let (lost, lomt, himt, hist) = self.fences;

			let label = if x < lost {
				LowSevere
			} else if x > hist {
				HighSevere
			} else if x < lomt {
				LowMild
			} else if x > himt {
				HighMild
			} else {
				NotAnOutlier
			};

			(x, label)
		})
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.iter.size_hint()
	}
}

/// Labels used to classify outliers
pub enum Label {
	/// A "mild" outlier in the "high" spectrum
	HighMild,
	/// A "severe" outlier in the "high" spectrum
	HighSevere,
	/// A "mild" outlier in the "low" spectrum
	LowMild,
	/// A "severe" outlier in the "low" spectrum
	LowSevere,
	/// A normal data point
	NotAnOutlier,
}

impl SurrealValue for Label {
	fn kind_of() -> surrealdb_types::Kind {
		surrealdb_types::Kind::String
	}

	fn into_value(self) -> surrealdb_types::Value {
		match self {
			Label::HighMild => "HighMild".into_value(),
			Label::LowMild => "LowMild".into_value(),
			Label::HighSevere => "HighSevere".into_value(),
			Label::LowSevere => "LowSevere".into_value(),
			Label::NotAnOutlier => "NotAnOutlier".into_value(),
		}
	}

	fn from_value(value: surrealdb_types::Value) -> Result<Self, surrealdb_types::Error>
	where
		Self: Sized,
	{
		let str = String::from_value(value)?;
		match str.as_str() {
			"HighMild" => Ok(Self::HighMild),
			"LowMild" => Ok(Self::LowMild),
			"HighSevere" => Ok(Self::HighSevere),
			"LowSevere" => Ok(Self::LowSevere),
			"NotAnOutlier" => Ok(Self::NotAnOutlier),
			_ => Err(surrealdb_types::Error::serialization(
				format!("Invalid measurement label type `{str}`"),
				None,
			)),
		}
	}
}

impl Label {
	/// Checks if the data point has an "unusually" high value
	pub fn is_high(&self) -> bool {
		matches!(*self, HighMild | HighSevere)
	}

	/// Checks if the data point is labeled as a "mild" outlier
	pub fn is_mild(&self) -> bool {
		matches!(*self, HighMild | LowMild)
	}

	/// Checks if the data point has an "unusually" low value
	pub fn is_low(&self) -> bool {
		matches!(*self, LowMild | LowSevere)
	}

	/// Checks if the data point is labeled as an outlier
	pub fn is_outlier(&self) -> bool {
		!matches!(*self, NotAnOutlier)
	}

	/// Checks if the data point is labeled as a "severe" outlier
	pub fn is_severe(&self) -> bool {
		matches!(*self, HighSevere | LowSevere)
	}
}

/// Classifies the sample, and returns a labeled sample.
///
/// - Time: `O(N log N) where N = length`
pub fn classify(sample: &Sample) -> LabeledSample<'_> {
	let (q1, _, q3) = sample.percentiles().quartiles();
	let iqr = q3 - q1;

	// Mild
	let k_m = 1.5;
	// Severe
	let k_s = 3.0;

	LabeledSample {
		fences: (q1 - k_s * iqr, q1 - k_m * iqr, q3 + k_m * iqr, q3 + k_s * iqr),
		sample,
	}
}
