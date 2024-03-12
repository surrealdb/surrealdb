// TODO
// https://docs.rs/statistical/1.0.0/src/statistical/stats_.rs.html
// https://rust-lang-nursery.github.io/rust-cookbook/science/mathematics/statistics.html

pub mod bottom;
pub mod deviation;
pub mod interquartile;
pub mod mean;
pub mod median;
pub mod midhinge;
pub mod mode;
pub mod nearestrank;
pub mod percentile;
pub mod spread;
pub mod top;
pub mod trimean;
pub mod variance;
pub mod vector;

pub(crate) trait ToFloat {
	fn to_float(&self) -> f64;
}

impl ToFloat for f64 {
	fn to_float(&self) -> f64 {
		*self
	}
}

impl ToFloat for f32 {
	fn to_float(&self) -> f64 {
		*self as f64
	}
}

impl ToFloat for i64 {
	fn to_float(&self) -> f64 {
		*self as f64
	}
}

impl ToFloat for i32 {
	fn to_float(&self) -> f64 {
		*self as f64
	}
}

impl ToFloat for i16 {
	fn to_float(&self) -> f64 {
		*self as f64
	}
}
