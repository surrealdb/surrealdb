use surrealdb_expr::val::Number;
use surrealdb_expr::val::number::Sorted;

use super::percentile::Percentile;

pub trait Interquartile {
	/// Interquartile Range - the difference between the upper and lower
	/// quartiles Q_3 - Q_1 [ or P_75 - P-25 ]
	fn interquartile(self) -> f64;
}

impl Interquartile for Sorted<&Vec<Number>> {
	fn interquartile(self) -> f64 {
		self.percentile(Number::from(75)) - self.percentile(Number::from(25))
	}
}
