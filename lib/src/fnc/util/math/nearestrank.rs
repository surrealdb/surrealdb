use crate::sql::number::{Number,Sorted};
use super::percentile::Percentile;

pub trait Nearestrank {
	/// (Assuming this is an alias for Percentile or is this a numeric rank)
	fn nearestrank(self, rank: Number) -> Number;
}

impl Nearestrank for Sorted<&Vec<Number>> {
	fn nearestrank(self, rank: Number) -> Number {
		self.percentile(rank)
		//   //  if this is numeric rank, get the lowest of the top `rank` items:
		// super::top::Top::top(self, rank).iter().min().unwrap_or(&Number::Float(f64::NAN)).clone()
	}
}
