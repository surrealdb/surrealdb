use anyhow::Result;
use surrealdb_expr::val::{Number, Value};

use crate::util::math::vector::{
	Add, Angle, CrossProduct, Divide, DotProduct, Magnitude, Multiply, Normalize, Project, Scale,
	Subtract,
};

pub fn add((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value> {
	Ok(a.add(&b)?.into_iter().map(Value::from).collect::<Vec<_>>().into())
}

pub fn angle((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value> {
	Ok(a.angle(&b)?.into())
}

pub fn divide((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value> {
	Ok(a.divide(&b)?.into_iter().map(Value::from).collect::<Vec<_>>().into())
}

pub fn cross((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value> {
	Ok(a.cross(&b)?.into_iter().map(Value::from).collect::<Vec<_>>().into())
}

pub fn dot((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value> {
	Ok(a.dot(&b)?.into())
}

pub fn magnitude((a,): (Vec<Number>,)) -> Result<Value> {
	Ok(a.magnitude().into())
}

pub fn multiply((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value> {
	Ok(a.multiply(&b)?.into_iter().map(Value::from).collect::<Vec<_>>().into())
}

pub fn normalize((a,): (Vec<Number>,)) -> Result<Value> {
	Ok(a.normalize()?.into_iter().map(Value::from).collect::<Vec<_>>().into())
}

pub fn project((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value> {
	Ok(a.project(&b)?.into_iter().map(Value::from).collect::<Vec<_>>().into())
}

pub fn subtract((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value> {
	Ok(a.subtract(&b)?.into_iter().map(Value::from).collect::<Vec<_>>().into())
}

pub fn scale((a, b): (Vec<Number>, Number)) -> Result<Value> {
	Ok(a.scale(&b)?.into_iter().map(Value::from).collect::<Vec<_>>().into())
}

pub mod distance {
	use anyhow::{Result, ensure};
	use surrealdb_expr::expr::Error;
	use surrealdb_expr::val::{Number, Value};

	use crate::util::math::vector::{
		ChebyshevDistance, EuclideanDistance, HammingDistance, ManhattanDistance,
		MinkowskiDistance, check_same_dimension,
	};

	pub fn chebyshev((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value> {
		Ok(a.chebyshev_distance(&b)?.into())
	}

	pub fn euclidean((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value> {
		Ok(a.euclidean_distance(&b)?.into())
	}

	pub fn hamming((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value> {
		Ok(a.hamming_distance(&b)?.into())
	}

	pub fn mahalanobis((a, b, c): (Vec<Number>, Vec<Number>, Vec<Vec<Number>>)) -> Result<Value> {
		check_same_dimension("vector::distance::mahalanobis", &a, &b)?;
		ensure!(
			!a.is_empty(),
			Error::InvalidFunctionArguments {
				name: String::from("vector::distance::mahalanobis"),
				message: String::from("The two vectors must not be empty."),
			}
		);
		let n = a.len();
		ensure!(
			c.len() == n && c.iter().all(|row| row.len() == n),
			Error::InvalidFunctionArguments {
				name: String::from("vector::distance::mahalanobis"),
				message: String::from(
					"The covariance matrix must be a square matrix of the same dimension as the vectors."
				),
			}
		);
		let cov: Vec<Vec<f64>> =
			c.iter().map(|row| row.iter().map(|v| v.to_float()).collect()).collect();
		for (i, row) in cov.iter().enumerate() {
			for (j, v) in row.iter().enumerate().take(i) {
				ensure!(
					*v == cov[j][i],
					Error::InvalidFunctionArguments {
						name: String::from("vector::distance::mahalanobis"),
						message: String::from(
							"The covariance matrix must be symmetric positive-definite."
						),
					}
				);
			}
		}
		let mut l = vec![vec![0.0; n]; n];
		for i in 0..n {
			for j in 0..=i {
				let s = (0..j).map(|k| l[i][k] * l[j][k]).sum::<f64>();
				if i == j {
					let d = cov[i][i] - s;
					ensure!(
						d > 0.0,
						Error::InvalidFunctionArguments {
							name: String::from("vector::distance::mahalanobis"),
							message: String::from(
								"The covariance matrix must be symmetric positive-definite."
							),
						}
					);
					l[i][j] = d.sqrt();
				} else {
					l[i][j] = (cov[i][j] - s) / l[j][j];
				}
			}
		}
		let mut y = vec![0.0; n];
		for i in 0..n {
			let s = (0..i).map(|k| l[i][k] * y[k]).sum::<f64>();
			y[i] = (a[i].to_float() - b[i].to_float() - s) / l[i][i];
		}
		Ok(y.iter().map(|v| v * v).sum::<f64>().sqrt().into())
	}

	pub fn manhattan((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value> {
		Ok(a.manhattan_distance(&b)?.into())
	}

	pub fn minkowski((a, b, o): (Vec<Number>, Vec<Number>, Number)) -> Result<Value> {
		Ok(a.minkowski_distance(&b, &o)?.into())
	}
}

pub mod similarity {

	use std::collections::HashSet;

	use anyhow::{Result, ensure};
	use surrealdb_expr::expr::Error;
	use surrealdb_expr::val::{Number, Value};

	use crate::util::math::vector::{CosineSimilarity, check_same_dimension, deviation};

	pub fn cosine((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value> {
		Ok(a.cosine_similarity(&b)?.into())
	}

	pub fn jaccard((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value> {
		let a: HashSet<&Number> = HashSet::from_iter(a.iter());
		let b: HashSet<&Number> = HashSet::from_iter(b.iter());
		let union = a.union(&b).count();
		if union == 0 {
			return Ok(1.0.into());
		}
		let intersection = a.intersection(&b).count();
		Ok((intersection as f64 / union as f64).into())
	}

	pub fn pearson((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value> {
		check_same_dimension("vector::similarity::pearson", &a, &b)?;
		let a: Vec<f64> = a.iter().map(|n| n.to_float()).collect();
		let b: Vec<f64> = b.iter().map(|n| n.to_float()).collect();
		Ok(correlate("vector::similarity::pearson", &a, &b)?.into())
	}

	pub fn spearman((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value> {
		check_same_dimension("vector::similarity::spearman", &a, &b)?;
		let a = rank(&a);
		let b = rank(&b);
		Ok(correlate("vector::similarity::spearman", &a, &b)?.into())
	}

	fn correlate(fnc: &str, a: &[f64], b: &[f64]) -> Result<f64> {
		ensure!(
			a.len() >= 2,
			Error::InvalidFunctionArguments {
				name: String::from(fnc),
				message: String::from("The two vectors must have a dimension of at least 2."),
			}
		);
		let m1 = a.iter().sum::<f64>() / a.len() as f64;
		let m2 = b.iter().sum::<f64>() / b.len() as f64;
		let std_dev1 = deviation(a, m1, false);
		let std_dev2 = deviation(b, m2, false);
		ensure!(
			std_dev1 != 0.0 && std_dev2 != 0.0,
			Error::InvalidFunctionArguments {
				name: String::from(fnc),
				message: String::from("The two vectors must not have a variance of zero."),
			}
		);
		let covar: f64 = a.iter().zip(b.iter()).map(|(x, y)| (x - m1) * (y - m2)).sum();
		let covar = covar / a.len() as f64;
		Ok(covar / (std_dev1 * std_dev2))
	}

	fn rank(v: &[Number]) -> Vec<f64> {
		let mut idx: Vec<usize> = (0..v.len()).collect();
		idx.sort_by(|&a, &b| v[a].cmp(&v[b]));
		let mut ranks = vec![0.0; v.len()];
		let mut i = 0;
		while i < idx.len() {
			let mut j = i;
			while j + 1 < idx.len() && v[idx[j + 1]] == v[idx[i]] {
				j += 1;
			}
			let rank = (i + j) as f64 / 2.0 + 1.0;
			for &k in &idx[i..=j] {
				ranks[k] = rank;
			}
			i = j + 1;
		}
		ranks
	}
}

#[cfg(test)]
mod tests {
	use rust_decimal::Decimal;
	use surrealdb_expr::val::Number;

	use super::*;

	#[test]
	fn vector_scale_int() {
		let input_vector: Vec<Number> = vec![1, 2, 3, 4].into_iter().map(Number::Int).collect();
		let scalar_int = Number::Int(2);

		let result: Result<Value> = scale((input_vector, scalar_int));

		let expected_output: Vec<_> =
			vec![2, 4, 6, 8].into_iter().map(Number::Int).map(Value::from).collect();

		assert_eq!(result.unwrap(), Value::from(expected_output));
	}

	#[test]
	fn vector_scale_float() {
		let input_vector: Vec<Number> = vec![1, 2, 3, 4].into_iter().map(Number::Int).collect();
		let scalar_float = Number::Float(1.51);

		let result: Result<Value> = scale((input_vector, scalar_float));
		let expected_output = vec![1.51, 3.02, 4.53, 6.04]
			.into_iter()
			.map(Number::Float)
			.map(Value::from)
			.collect::<Value>();
		assert_eq!(result.unwrap(), expected_output);
	}

	#[test]
	fn vector_scale_decimal() {
		let input_vector: Vec<Number> = vec![1, 2, 3, 4].into_iter().map(Number::Int).collect();
		let scalar_decimal = Number::Decimal(Decimal::new(3141, 3));

		let result: Result<Value> = scale((input_vector, scalar_decimal));
		let expected_output: Vec<_> = vec![
			Value::Number(Number::Decimal(Decimal::new(3141, 3))), // 3.141 * 1
			Value::Number(Number::Decimal(Decimal::new(6282, 3))), // 3.141 * 2
			Value::Number(Number::Decimal(Decimal::new(9423, 3))), // 3.141 * 3
			Value::Number(Number::Decimal(Decimal::new(12564, 3))), // 3.141 * 4
		];
		assert_eq!(result.unwrap(), Value::from(expected_output));
	}
}
