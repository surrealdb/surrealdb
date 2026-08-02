use surrealdb_expr::val::Number;
use surrealdb_expr::val::number::Sorted;

use super::percentile::Percentile;

pub trait Midhinge {
	/// Tukey Midhinge - the average of the 1st and 3rd Quartiles
	fn midhinge(&self) -> f64;
}

impl Midhinge for Sorted<&Vec<Number>> {
	fn midhinge(&self) -> f64 {
		(self.percentile(Number::from(75)) + self.percentile(Number::from(25))) / 2.0
	}
}
