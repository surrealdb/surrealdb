//! Helper traits for tupling/untupling

use super::Distribution;

/// Any tuple: `(A, B, ..)`
pub trait Tuple: Sized {
	/// A tuple of distributions associated with this tuple
	type Distributions: TupledDistributions<Item = Self>;

	/// A tuple of vectors associated with this tuple
	type Builder: TupledDistributionsBuilder<Item = Self>;
}

/// A tuple of distributions: `(Distribution, Distribution, ..)`
pub trait TupledDistributions: Sized {
	/// A tuple that can be pushed/inserted into the tupled distributions
	type Item: Tuple<Distributions = Self>;
}

/// A tuple of vecs used to build distributions.
pub trait TupledDistributionsBuilder: Sized {
	/// A tuple that can be pushed/inserted into the tupled distributions
	type Item: Tuple<Builder = Self>;

	/// Creates a new tuple of vecs
	fn new(size: usize) -> Self;

	/// Push one element into each of the vecs
	fn push(&mut self, tuple: Self::Item);

	/// Append one tuple of vecs to this one, leaving the vecs in the other tuple empty
	fn extend(&mut self, other: &mut Self);

	/// Convert the tuple of vectors into a tuple of distributions
	fn complete(self) -> <Self::Item as Tuple>::Distributions;
}

impl Tuple for (f64,) {
	type Distributions = (Distribution,);
	type Builder = (Vec<f64>,);
}

impl TupledDistributions for (Distribution,) {
	type Item = (f64,);
}

impl TupledDistributionsBuilder for (Vec<f64>,) {
	type Item = (f64,);

	fn new(size: usize) -> (Vec<f64>,) {
		(Vec::with_capacity(size),)
	}

	fn push(&mut self, tuple: (f64,)) {
		(self.0).push(tuple.0);
	}

	fn extend(&mut self, other: &mut (Vec<f64>,)) {
		(self.0).append(&mut other.0);
	}

	fn complete(self) -> (Distribution,) {
		(Distribution(self.0.into_boxed_slice()),)
	}
}

impl Tuple for (f64, f64) {
	type Distributions = (Distribution, Distribution);
	type Builder = (Vec<f64>, Vec<f64>);
}

impl TupledDistributions for (Distribution, Distribution) {
	type Item = (f64, f64);
}

impl TupledDistributionsBuilder for (Vec<f64>, Vec<f64>) {
	type Item = (f64, f64);

	fn new(size: usize) -> (Vec<f64>, Vec<f64>) {
		(Vec::with_capacity(size), Vec::with_capacity(size))
	}

	fn push(&mut self, tuple: (f64, f64)) {
		(self.0).push(tuple.0);
		(self.1).push(tuple.1);
	}

	fn extend(&mut self, other: &mut (Vec<f64>, Vec<f64>)) {
		(self.0).append(&mut other.0);
		(self.1).append(&mut other.1);
	}

	fn complete(self) -> (Distribution, Distribution) {
		(Distribution(self.0.into_boxed_slice()), Distribution(self.1.into_boxed_slice()))
	}
}

impl Tuple for (f64, f64, f64) {
	type Distributions = (Distribution, Distribution, Distribution);
	type Builder = (Vec<f64>, Vec<f64>, Vec<f64>);
}

impl TupledDistributions for (Distribution, Distribution, Distribution) {
	type Item = (f64, f64, f64);
}

impl TupledDistributionsBuilder for (Vec<f64>, Vec<f64>, Vec<f64>) {
	type Item = (f64, f64, f64);

	fn new(size: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
		(Vec::with_capacity(size), Vec::with_capacity(size), Vec::with_capacity(size))
	}

	fn push(&mut self, tuple: (f64, f64, f64)) {
		(self.0).push(tuple.0);
		(self.1).push(tuple.1);
		(self.2).push(tuple.2);
	}

	fn extend(&mut self, other: &mut (Vec<f64>, Vec<f64>, Vec<f64>)) {
		(self.0).append(&mut other.0);
		(self.1).append(&mut other.1);
		(self.2).append(&mut other.2);
	}

	fn complete(self) -> (Distribution, Distribution, Distribution) {
		(
			Distribution(self.0.into_boxed_slice()),
			Distribution(self.1.into_boxed_slice()),
			Distribution(self.2.into_boxed_slice()),
		)
	}
}

impl Tuple for (f64, f64, f64, f64) {
	type Distributions = (Distribution, Distribution, Distribution, Distribution);
	type Builder = (Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>);
}

impl TupledDistributions for (Distribution, Distribution, Distribution, Distribution) {
	type Item = (f64, f64, f64, f64);
}

impl TupledDistributionsBuilder for (Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>) {
	type Item = (f64, f64, f64, f64);

	fn new(size: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>) {
		(
			Vec::with_capacity(size),
			Vec::with_capacity(size),
			Vec::with_capacity(size),
			Vec::with_capacity(size),
		)
	}

	fn push(&mut self, tuple: (f64, f64, f64, f64)) {
		(self.0).push(tuple.0);
		(self.1).push(tuple.1);
		(self.2).push(tuple.2);
		(self.3).push(tuple.3);
	}

	fn extend(&mut self, other: &mut (Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>)) {
		(self.0).append(&mut other.0);
		(self.1).append(&mut other.1);
		(self.2).append(&mut other.2);
		(self.3).append(&mut other.3);
	}

	fn complete(self) -> (Distribution, Distribution, Distribution, Distribution) {
		(
			Distribution(self.0.into_boxed_slice()),
			Distribution(self.1.into_boxed_slice()),
			Distribution(self.2.into_boxed_slice()),
			Distribution(self.3.into_boxed_slice()),
		)
	}
}
