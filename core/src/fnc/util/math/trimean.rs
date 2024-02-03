use super::{median::Median, midhinge::Midhinge};
use crate::sql::number::{Number, Sorted};

pub trait Trimean {
	/// Bowley's Trimean - the Average of the median and the MidHinge
	/// ( 2 * Q_2 + Q_1 + Q_3 ) / 4 == ( Q_2 + ( Q_1 + Q_3 ) ) / 2
	fn trimean(self) -> f64;
}

impl Trimean for Sorted<&Vec<Number>> {
	fn trimean(self) -> f64 {
		(self.midhinge() + self.median()) * 0.5
	}
}
