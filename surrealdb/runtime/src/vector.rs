use anyhow::Result;
use surrealdb_expr::expr::Error;
use surrealdb_expr::val::{Number, Value};

use crate::args::{Args, Arity, FromArg};
use crate::util::math::vector::{
	Add, Angle, CrossProduct, Divide, DotProduct, Magnitude, Multiply, Normalize, Project, Scale,
	Subtract, add_assign,
};

/// A vector argument read in place: an array whose every element is a number.
///
/// The measures read its elements as the values they are, so an argument is
/// taken without copying its numbers out. A value that is not such an array
/// fails as an argument of type `array<number>` fails, with the same error.
pub struct NumberArray(Vec<Value>);

impl std::ops::Deref for NumberArray {
	type Target = [Value];

	fn deref(&self) -> &[Value] {
		&self.0
	}
}

impl FromArg for NumberArray {
	fn arity() -> Arity {
		Arity {
			lower: 1,
			upper: Some(1),
		}
	}

	fn from_arg(name: &str, args: &mut Args) -> Result<Self> {
		let (idx, value) = args.next().ok_or_else(|| Error::InvalidFunctionArguments {
			name: name.to_owned(),
			message: "Missing an argument".to_string(),
		})?;
		if let Value::Array(ref array) = value
			&& array.iter().all(Value::is_number)
		{
			let Value::Array(array) = value else {
				unreachable!()
			};
			return Ok(Self(array.0));
		}
		match value.coerce_to::<Vec<Number>>() {
			Ok(numbers) => Ok(Self(numbers.into_iter().map(Value::Number).collect())),
			Err(e) => Err(Error::InvalidFunctionArguments {
				name: name.to_owned(),
				message: format!("Argument {idx} was the wrong type. {e}"),
			}
			.into()),
		}
	}
}

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

pub fn dot((a, b): (NumberArray, NumberArray)) -> Result<Value> {
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

/// Sum a collection of equal-dimension vectors elementwise.
///
/// Every vector must share the same dimension; a mismatch is an error rather
/// than a truncated or zero-padded result. An empty collection has no dimension
/// to return a zero vector for, so it yields `NONE`.
pub fn sum((vectors,): (Vec<Vec<Number>>,)) -> Result<Value> {
	let mut vectors = vectors.into_iter();
	let Some(mut total) = vectors.next() else {
		return Ok(Value::None);
	};
	for vector in vectors {
		add_assign("vector::sum", &mut total, &vector)?;
	}
	Ok(total.into_iter().map(Value::from).collect::<Vec<_>>().into())
}

pub fn scale((a, b): (Vec<Number>, Number)) -> Result<Value> {
	Ok(a.scale(&b)?.into_iter().map(Value::from).collect::<Vec<_>>().into())
}

pub mod distance {
	use anyhow::{Result, ensure};
	use surrealdb_expr::expr::Error;
	use surrealdb_expr::val::{Number, Value};

	use super::NumberArray;
	use crate::util::math::vector::{
		ChebyshevDistance, CosineDistance, EuclideanDistance, HammingDistance, ManhattanDistance,
		MinkowskiDistance, check_same_dimension,
	};

	pub fn chebyshev((a, b): (NumberArray, NumberArray)) -> Result<Value> {
		Ok(a.chebyshev_distance(&b)?.into())
	}

	pub fn cosine((a, b): (NumberArray, NumberArray)) -> Result<Value> {
		Ok(a.cosine_distance(&b)?.into())
	}

	pub fn euclidean((a, b): (NumberArray, NumberArray)) -> Result<Value> {
		Ok(a.euclidean_distance(&b)?.into())
	}

	pub fn hamming((a, b): (NumberArray, NumberArray)) -> Result<Value> {
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

	pub fn manhattan((a, b): (NumberArray, NumberArray)) -> Result<Value> {
		Ok(a.manhattan_distance(&b)?.into())
	}

	pub fn minkowski((a, b, o): (NumberArray, NumberArray, Number)) -> Result<Value> {
		Ok(a.minkowski_distance(&b, &o)?.into())
	}
}

pub mod similarity {

	use anyhow::{Result, ensure};
	use surrealdb_expr::expr::Error;
	use surrealdb_expr::val::{Number, Value};

	use super::NumberArray;
	use crate::util::math::vector::{
		CosineSimilarity, JaccardSimilarity, check_same_dimension, deviation,
	};

	pub fn cosine((a, b): (NumberArray, NumberArray)) -> Result<Value> {
		Ok(a.cosine_similarity(&b)?.into())
	}

	pub fn jaccard((a, b): (NumberArray, NumberArray)) -> Result<Value> {
		Ok(a.jaccard_similarity(&b)?.into())
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

	fn values(numbers: impl IntoIterator<Item = Number>) -> Value {
		Value::from(numbers.into_iter().map(Value::from).collect::<Vec<_>>())
	}

	fn floats(len: usize, seed: f64) -> Vec<Number> {
		(0..len).map(|i| Number::Float(((i as f64 + seed) * 0.37).sin() * 3.0)).collect()
	}

	fn args<T: crate::args::FromArgs>(name: &str, a: Value, b: Value) -> Result<T> {
		T::from_args(name, vec![a, b])
	}

	fn number(v: Value) -> Number {
		v.into_number().unwrap_or_else(|| panic!("not a number"))
	}

	/// A vector argument that is not an array of numbers fails with the error
	/// an `array<number>` argument fails with.
	#[test]
	fn a_vector_argument_fails_as_an_array_of_numbers_fails() {
		let name = "vector::distance::euclidean";
		for bad in [
			Value::from("text"),
			Value::None,
			Value::from(vec![Value::from(1), Value::from("x")]),
			Value::from(vec![Value::from(1), Value::from(vec![Value::from(2)])]),
		] {
			let ok = values([Number::Int(1), Number::Int(2)]);
			let got = args::<(NumberArray, NumberArray)>(name, ok.clone(), bad.clone())
				.err()
				.map(|e| e.to_string());
			let want =
				args::<(Vec<Number>, Vec<Number>)>(name, ok, bad).err().map(|e| e.to_string());
			assert!(want.is_some());
			assert_eq!(got, want);
		}
	}

	/// The measures keep the number kinds they answer: integer and decimal
	/// vectors answer in exact arithmetic, an empty vector answers the empty
	/// integer sum, and a vector of floats answers a float.
	#[test]
	fn measures_keep_their_number_kinds() {
		use crate::util::math::vector::{DotProduct, ManhattanDistance};
		let ints = |v: &[i64]| v.iter().map(|i| Number::Int(*i)).collect::<Vec<_>>();
		let dec =
			|v: &[i64]| v.iter().map(|i| Number::Decimal(Decimal::new(*i, 1))).collect::<Vec<_>>();
		let call =
			|f: fn((NumberArray, NumberArray)) -> Result<Value>, a: &[Number], b: &[Number]| {
				f(args("f", values(a.to_vec()), values(b.to_vec())).unwrap()).unwrap()
			};
		let big = ints(&[i64::MAX, 3, 4, 5, 6, 7, 8, 9, 10]);
		let zero = ints(&[0, 0, 0, 0, 0, 0, 0, 0, 0]);
		assert_eq!(call(dot, &ints(&[1, 2, 3]), &ints(&[4, 5, 6])), Value::from(Number::Int(32)));
		assert_eq!(call(distance::manhattan, &big[1..], &zero[1..]), Value::from(Number::Int(52)));
		assert!(matches!(
			call(distance::manhattan, &dec(&[15, 20]), &dec(&[5, 5])),
			Value::Number(Number::Decimal(_))
		));
		assert_eq!(call(dot, &[], &[]), Value::from(Number::Int(0)));
		assert_eq!(call(distance::manhattan, &[], &[]), Value::from(Number::Int(0)));
		assert_eq!(call(distance::hamming, &ints(&[1, 2, 3]), &ints(&[1, 0, 3])), Value::from(1));
		assert!(matches!(
			call(dot, &floats(20, 0.0), &floats(20, 1.0)),
			Value::Number(Number::Float(_))
		));
		// Integers whose difference an `i64` or an `f64` cannot hold exactly.
		assert_eq!(
			call(distance::euclidean, &ints(&[i64::MAX]), &ints(&[i64::MIN])),
			Value::from(Number::Float(u64::MAX as f64))
		);
		assert_eq!(
			call(distance::chebyshev, &ints(&[i64::MAX, 0]), &ints(&[i64::MIN, 0])),
			Value::from(Number::Float(u64::MAX as f64))
		);
		// The trait methods over numbers answer as the functions do.
		assert_eq!(ints(&[1, 2, 3]).dot(&ints(&[4, 5, 6])).unwrap(), Number::Int(32));
		assert_eq!(ints(&[1, 2]).manhattan_distance(&ints(&[4, 6])).unwrap(), Number::Int(7));
	}

	/// Over vectors of floats long enough to fill the lanes, each measure
	/// agrees with a plain sequential sum to within the rounding error of a sum
	/// taken in another order: of the order of `n · ε · Σ|termᵢ|`, which is
	/// measured against the magnitudes of the terms, not against the sum.
	#[test]
	fn float_measures_agree_with_a_sequential_sum() {
		let f = |n: &Number| n.to_float();
		for len in [1, 3, 7, 8, 9, 16, 31, 768] {
			let (a, b) = (floats(len, 0.5), floats(len, 1.5));
			let bound = 4.0 * len as f64 * f64::EPSILON;
			let close = |x: Number, want: f64, scale: f64| {
				let x = x.to_float();
				assert!((x - want).abs() <= bound * scale, "{len}: {x} vs {want}");
			};
			let pairs = || a.iter().zip(&b).map(|(x, y)| (f(x), f(y)));
			let dot_ref: f64 = pairs().map(|(x, y)| x * y).sum();
			let dot_scale: f64 = pairs().map(|(x, y)| (x * y).abs()).sum();
			let na: f64 = a.iter().map(|x| f(x) * f(x)).sum();
			let nb: f64 = b.iter().map(|x| f(x) * f(x)).sum();
			let l2: f64 = pairs().map(|(x, y)| (x - y) * (x - y)).sum::<f64>().sqrt();
			let l1: f64 = pairs().map(|(x, y)| (x - y).abs()).sum();
			let p3: f64 =
				pairs().map(|(x, y)| (x - y).abs().powf(3.0)).sum::<f64>().powf(1.0 / 3.0);
			let pair = || {
				args::<(NumberArray, NumberArray)>("m", values(a.clone()), values(b.clone()))
					.unwrap()
			};
			let run =
				|m: fn((NumberArray, NumberArray)) -> Result<Value>| number(m(pair()).unwrap());
			let norms = na.sqrt() * nb.sqrt();
			close(run(dot), dot_ref, dot_scale);
			close(run(distance::euclidean), l2, l2);
			close(run(distance::manhattan), l1, l1);
			close(run(similarity::cosine), dot_ref / norms, dot_scale / norms);
			let (x, y) = pair();
			close(number(distance::minkowski((x, y, Number::Int(3))).unwrap()), p3, p3);
		}
	}

	/// Finite terms that cancel across lanes carry each lane past the largest
	/// float, while the sum in order stays finite: every measure answers the sum
	/// in order.
	#[test]
	fn a_lane_past_the_largest_float_sums_in_order() {
		use crate::util::math::vector::lane_sum;
		let pattern = [1e308, -1e308, 1e308, -1e308, -1e308, 1e308, -1e308, 1e308];
		let a: Vec<f64> = pattern.iter().chain(&pattern).copied().collect();
		let b = [1.0; 16];
		assert_eq!(lane_sum(&a, &b, |x, y| x * y), 0.0);
		let floats = |v: &[f64]| values(v.iter().map(|&f| Number::Float(f)));
		let pair = || args::<(NumberArray, NumberArray)>("m", floats(&a), floats(&b)).unwrap();
		assert_eq!(number(dot(pair()).unwrap()), Number::Float(0.0));
		assert_eq!(number(similarity::cosine(pair()).unwrap()), Number::Float(0.0));
	}

	/// A sum of terms that cancel answers the order of the lanes, which can
	/// differ from the sequential sum by the whole of the sum; one whose
	/// sequential sum passes the largest float partway, where no lane does, is
	/// finite.
	#[test]
	fn a_cancelling_sum_answers_the_order_of_the_lanes() {
		let floats = |v: &[f64]| values(v.iter().map(|&f| Number::Float(f)));
		let ones = [1.0; 8];
		let dot_of = |a: &[f64]| {
			number(
				dot(args::<(NumberArray, NumberArray)>("m", floats(a), floats(&ones)).unwrap())
					.unwrap(),
			)
		};
		let cancelling = [1e16, -1.0, 0.0, 0.0, -1e16, 0.0, 0.0, 0.0];
		assert_eq!(cancelling.iter().sum::<f64>(), 0.0);
		assert_eq!(dot_of(&cancelling), Number::Float(-1.0));
		let huge = [1e308, 1e308, 0.0, 0.0, -1e308, -1e308, 0.0, 0.0];
		assert_eq!(huge.iter().sum::<f64>(), f64::INFINITY);
		assert_eq!(dot_of(&huge), Number::Float(0.0));
	}

	/// A vector shorter than the lanes sums its terms in order from `-0.0`, as
	/// a sequential sum of `f64`s does, so its answer is the sequential sum's,
	/// to the last bit: an empty vector's and one of `-0.0` terms' included.
	#[test]
	fn a_short_vector_sums_in_order() {
		use crate::util::math::vector::lane_sum;
		for (a, b) in [
			(vec![0.1, 0.2, 0.3, 1e16, -1e16, 0.7, 0.9], vec![1.0; 7]),
			(vec![0.0; 7], vec![-1.0; 7]),
			(Vec::new(), Vec::new()),
		] {
			let seq: f64 = a.iter().zip(&b).map(|(x, y)| x * y).sum();
			assert_eq!(lane_sum(&a, &b, |x, y| x * y).to_bits(), seq.to_bits(), "{a:?}");
		}
	}

	/// A measure whose sum is exactly zero answers the zero its definition
	/// sums to: a sum of `f64`s starts from `-0.0`, and a dot product in
	/// [`Number`] arithmetic from the integer `0`, whatever the length.
	#[test]
	fn a_zero_sum_keeps_the_sign_its_definition_gives_it() {
		let floats = |v: &[f64]| values(v.iter().map(|&f| Number::Float(f)));
		let pair = |a: &[f64], b: &[f64]| {
			args::<(NumberArray, NumberArray)>("m", floats(a), floats(b)).unwrap()
		};
		let bits = |n: Number| n.to_float().to_bits();
		let empty = number(distance::euclidean(pair(&[], &[])).unwrap());
		assert_eq!(bits(empty), (-0.0f64).to_bits());
		let (x, y) = pair(&[], &[]);
		let order = Number::Int(-1);
		assert_eq!(
			bits(number(distance::minkowski((x, y, order)).unwrap())),
			f64::NEG_INFINITY.to_bits()
		);
		for len in [3, 8, 16] {
			let (a, b) = (vec![0.0; len], vec![-1.0; len]);
			let want: Number =
				a.iter().zip(&b).map(|(x, y)| Number::Float(*x) * Number::Float(*y)).sum();
			assert_eq!(bits(number(dot(pair(&a, &b)).unwrap())), bits(want), "dot {len}");
			let cosine = number(similarity::cosine(pair(&a, &b)).unwrap());
			assert!(cosine.to_float().is_nan(), "cosine {len}");
		}
	}
}
