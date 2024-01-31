use crate::err::Error;
use crate::fnc::util::math::deviation::deviation;
use crate::fnc::util::math::mean::Mean;
use crate::fnc::util::math::ToFloat;
use crate::sql::index::{Distance, VectorType};
use crate::sql::{Array, Number, Value};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::ops::{Mul, Sub};
use std::sync::Arc;

/// In the context of a Symmetric MTree index, the term object refers to a vector, representing the indexed item.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[revisioned(revision = 1)]
pub enum TreeVector {
	F64(Vec<f64>, Option<u64>),
	F32(Vec<f32>, Option<u64>),
	I64(Vec<i64>, Option<u64>),
	I32(Vec<i32>, Option<u64>),
	I16(Vec<i16>, Option<u64>),
}

/// For vectors, as we want to support very large vectors, we want to avoid copy or clone.
/// So the requirement is multiple ownership but not thread safety.
/// However, because we are running in an async context, and because we are using cache structures that use the Arc as a key,
/// the cached objects has to be Sent, which then requires the use of Arc (rather than just Rc).
pub type SharedVector = Arc<TreeVector>;

impl Hash for TreeVector {
	fn hash<H: Hasher>(&self, state: &mut H) {
		state.write_u64(
			match self {
				Self::F64(_, h) => h,
				Self::F32(_, h) => h,
				Self::I64(_, h) => h,
				Self::I32(_, h) => h,
				Self::I16(_, h) => h,
			}
			.expect("TreeVector without hash"),
		)
	}
}

impl PartialEq for TreeVector {
	fn eq(&self, other: &Self) -> bool {
		match (self, other) {
			(Self::F64(v, _), Self::F64(v_o, _)) => v == v_o,
			(Self::F32(v, _), Self::F32(v_o, _)) => v == v_o,
			(Self::I64(v, _), Self::I64(v_o, _)) => v == v_o,
			(Self::I32(v, _), Self::I32(v_o, _)) => v == v_o,
			(Self::I16(v, _), Self::I16(v_o, _)) => v == v_o,
			_ => false,
		}
	}
}

impl Eq for TreeVector {}

impl PartialOrd for TreeVector {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for TreeVector {
	fn cmp(&self, other: &Self) -> Ordering {
		use TreeVector::*;
		match (self, other) {
			(F64(v, _), F64(v_o, _)) => v.partial_cmp(v_o).unwrap_or(Ordering::Equal),
			(F32(v, _), F32(v_o, _)) => v.partial_cmp(v_o).unwrap_or(Ordering::Equal),
			(I64(v, _), I64(v_o, _)) => v.cmp(v_o),
			(I32(v, _), I32(v_o, _)) => v.cmp(v_o),
			(I16(v, _), I16(v_o, _)) => v.cmp(v_o),
			(F64(_, _), _) => Ordering::Less,
			(_, F64(_, _)) => Ordering::Greater,
			(F32(_, _), _) => Ordering::Less,
			(_, F32(_, _)) => Ordering::Greater,
			(I64(_, _), _) => Ordering::Less,
			(_, I64(_, _)) => Ordering::Greater,
			(I32(_, _), _) => Ordering::Less,
			(_, I32(_, _)) => Ordering::Greater,
		}
	}
}

impl TreeVector {
	pub(super) fn new(t: VectorType, d: usize) -> Self {
		match t {
			VectorType::F64 => Self::F64(Vec::with_capacity(d), None),
			VectorType::F32 => Self::F32(Vec::with_capacity(d), None),
			VectorType::I64 => Self::I64(Vec::with_capacity(d), None),
			VectorType::I32 => Self::I32(Vec::with_capacity(d), None),
			VectorType::I16 => Self::I16(Vec::with_capacity(d), None),
		}
	}

	pub(super) fn compute_hash(&mut self) {
		let mut hasher = DefaultHasher::new();
		match self {
			TreeVector::F64(v, h) => {
				Self::compute_hash_f64(v, &mut hasher);
				h.replace(hasher.finish());
			}
			TreeVector::F32(v, h) => {
				Self::compute_hash_f32(v, &mut hasher);
				h.replace(hasher.finish());
			}
			TreeVector::I64(v, h) => {
				Self::compute_hash_i64(v, &mut hasher);
				h.replace(hasher.finish());
			}
			TreeVector::I32(v, h) => {
				Self::compute_hash_i32(v, &mut hasher);
				h.replace(hasher.finish());
			}
			TreeVector::I16(v, h) => {
				Self::compute_hash_i16(v, &mut hasher);
				h.replace(hasher.finish());
			}
		}
	}

	fn compute_hash_f64<H: Hasher>(v: &[f64], state: &mut H) {
		for item in v {
			state.write_u64(item.to_bits());
		}
	}

	fn compute_hash_f32<H: Hasher>(v: &[f32], state: &mut H) {
		for item in v {
			state.write_u32(item.to_bits());
		}
	}

	fn compute_hash_i64<H: Hasher>(v: &[i64], state: &mut H) {
		for item in v {
			state.write_i64(*item);
		}
	}

	fn compute_hash_i32<H: Hasher>(v: &[i32], state: &mut H) {
		for item in v {
			state.write_i32(*item);
		}
	}

	fn compute_hash_i16<H: Hasher>(v: &[i16], state: &mut H) {
		for item in v {
			state.write_i16(*item);
		}
	}

	pub(super) fn try_from_value(t: VectorType, d: usize, v: Value) -> Result<Self, Error> {
		let mut vec = TreeVector::new(t, d);
		vec.check_vector_value(v)?;
		vec.compute_hash();
		Ok(vec)
	}

	fn check_vector_value(&mut self, value: Value) -> Result<(), Error> {
		match value {
			Value::Array(a) => {
				for v in a {
					self.check_vector_value(v)?;
				}
				Ok(())
			}
			Value::Number(n) => {
				self.add(n);
				Ok(())
			}
			_ => Err(Error::InvalidVectorValue(value.clone().to_raw_string())),
		}
	}

	pub(super) fn try_from_array(t: VectorType, a: Array) -> Result<Self, Error> {
		let mut vec = TreeVector::new(t, a.len());
		for v in a.0 {
			if let Value::Number(n) = v {
				vec.add(n);
			} else {
				return Err(Error::InvalidVectorType {
					current: v.clone().to_string(),
					expected: "Number",
				});
			}
		}
		vec.compute_hash();
		Ok(vec)
	}

	pub(super) fn add(&mut self, n: Number) {
		match self {
			TreeVector::F64(v, h) => {
				v.push(n.to_float());
				*h = None;
			}
			TreeVector::F32(v, h) => {
				v.push(n.to_float() as f32);
				*h = None;
			}
			TreeVector::I64(v, h) => {
				v.push(n.to_int());
				*h = None;
			}
			TreeVector::I32(v, h) => {
				v.push(n.to_int() as i32);
				*h = None;
			}
			TreeVector::I16(v, h) => {
				v.push(n.to_int() as i16);
				*h = None;
			}
		};
	}

	pub(super) fn len(&self) -> usize {
		match self {
			TreeVector::F64(v, _) => v.len(),
			TreeVector::F32(v, _) => v.len(),
			TreeVector::I64(v, _) => v.len(),
			TreeVector::I32(v, _) => v.len(),
			TreeVector::I16(v, _) => v.len(),
		}
	}

	pub(super) fn check_expected_dimension(current: usize, expected: usize) -> Result<(), Error> {
		if current != expected {
			Err(Error::InvalidVectorDimension {
				current,
				expected,
			})
		} else {
			Ok(())
		}
	}

	pub(super) fn check_dimension(&self, expected_dim: usize) -> Result<(), Error> {
		Self::check_expected_dimension(self.len(), expected_dim)
	}

	fn chebyshev<T>(a: &[T], b: &[T]) -> f64
	where
		T: ToFloat,
	{
		a.iter()
			.zip(b.iter())
			.map(|(a, b)| (a.to_float() - b.to_float()).abs())
			.fold(f64::MIN, f64::max)
	}

	pub(crate) fn chebyshev_distance(&self, other: &Self) -> f64 {
		match (self, other) {
			(TreeVector::F64(a, _), TreeVector::F64(b, _)) => Self::chebyshev(a, b),
			(TreeVector::F32(a, _), TreeVector::F32(b, _)) => Self::chebyshev(a, b),
			(TreeVector::I64(a, _), TreeVector::I64(b, _)) => Self::chebyshev(a, b),
			(TreeVector::I32(a, _), TreeVector::I32(b, _)) => Self::chebyshev(a, b),
			(TreeVector::I16(a, _), TreeVector::I16(b, _)) => Self::chebyshev(a, b),
			_ => f64::NAN,
		}
	}

	fn dot<T>(a: &[T], b: &[T]) -> f64
	where
		T: Mul<Output = T> + Copy + ToFloat,
	{
		a.iter().zip(b.iter()).map(|(&x, &y)| x.to_float() * y.to_float()).sum::<f64>()
	}

	fn magnitude<T>(v: &[T]) -> f64
	where
		T: ToFloat + Copy,
	{
		v.iter()
			.map(|&x| {
				let x = x.to_float();
				x * x
			})
			.sum::<f64>()
			.sqrt()
	}

	fn normalize<T>(v: &[T]) -> Vec<f64>
	where
		T: ToFloat + Copy,
	{
		let mag = Self::magnitude(v);
		if mag == 0.0 {
			vec![0.0; v.len()] // Return a zero vector if magnitude is zero
		} else {
			v.iter().map(|&x| x.to_float() / mag).collect()
		}
	}

	fn cosine<T>(a: &[T], b: &[T]) -> f64
	where
		T: ToFloat + Mul<Output = T> + Copy,
	{
		let norm_a = Self::normalize(a);
		let norm_b = Self::normalize(b);
		let mut s = Self::dot(&norm_a, &norm_b);
		if s < -1.0 {
			s = -1.0;
		}
		if s > 1.0 {
			s = 1.0;
		}
		1.0 - s
	}

	pub(crate) fn cosine_distance(&self, other: &Self) -> f64 {
		match (self, other) {
			(TreeVector::F64(a, _), TreeVector::F64(b, _)) => Self::cosine(a, b),
			(TreeVector::F32(a, _), TreeVector::F32(b, _)) => Self::cosine(a, b),
			(TreeVector::I64(a, _), TreeVector::I64(b, _)) => Self::cosine(a, b),
			(TreeVector::I32(a, _), TreeVector::I32(b, _)) => Self::cosine(a, b),
			(TreeVector::I16(a, _), TreeVector::I16(b, _)) => Self::cosine(a, b),
			_ => f64::NAN,
		}
	}

	fn euclidean<T>(a: &[T], b: &[T]) -> f64
	where
		T: ToFloat,
	{
		a.iter()
			.zip(b.iter())
			.map(|(a, b)| (a.to_float() - b.to_float()).powi(2))
			.sum::<f64>()
			.sqrt()
	}

	pub(crate) fn euclidean_distance(&self, other: &Self) -> f64 {
		match (self, other) {
			(TreeVector::F64(a, _), TreeVector::F64(b, _)) => Self::euclidean(a, b),
			(TreeVector::F32(a, _), TreeVector::F32(b, _)) => Self::euclidean(a, b),
			(TreeVector::I64(a, _), TreeVector::I64(b, _)) => Self::euclidean(a, b),
			(TreeVector::I32(a, _), TreeVector::I32(b, _)) => Self::euclidean(a, b),
			(TreeVector::I16(a, _), TreeVector::I16(b, _)) => Self::euclidean(a, b),
			_ => f64::INFINITY,
		}
	}
	fn hamming<T>(a: &[T], b: &[T]) -> f64
	where
		T: PartialEq,
	{
		a.iter().zip(b.iter()).filter(|&(a, b)| a != b).count() as f64
	}

	pub(crate) fn hamming_distance(&self, other: &Self) -> f64 {
		match (self, other) {
			(TreeVector::F64(a, _), TreeVector::F64(b, _)) => Self::hamming(a, b),
			(TreeVector::F32(a, _), TreeVector::F32(b, _)) => Self::hamming(a, b),
			(TreeVector::I64(a, _), TreeVector::I64(b, _)) => Self::hamming(a, b),
			(TreeVector::I32(a, _), TreeVector::I32(b, _)) => Self::hamming(a, b),
			(TreeVector::I16(a, _), TreeVector::I16(b, _)) => Self::hamming(a, b),
			_ => f64::NAN,
		}
	}

	fn jaccard_f64(a: &[f64], b: &[f64]) -> f64 {
		let set_a: HashSet<u64> = HashSet::from_iter(a.iter().map(|f| f.to_bits()));
		let set_b: HashSet<u64> = HashSet::from_iter(b.iter().map(|f| f.to_bits()));
		let intersection_size = set_a.intersection(&set_b).count() as f64;
		let union_size = set_a.union(&set_b).count() as f64;
		intersection_size / union_size
	}

	fn jaccard_f32(a: &[f32], b: &[f32]) -> f64 {
		let set_a: HashSet<u32> = HashSet::from_iter(a.iter().map(|f| f.to_bits()));
		let set_b: HashSet<u32> = HashSet::from_iter(b.iter().map(|f| f.to_bits()));
		let intersection_size = set_a.intersection(&set_b).count() as f64;
		let union_size = set_a.union(&set_b).count() as f64;
		intersection_size / union_size
	}

	fn jaccard_integers<T>(a: &[T], b: &[T]) -> f64
	where
		T: Eq + Hash,
	{
		let set_a: HashSet<&T> = HashSet::from_iter(a.iter());
		let set_b: HashSet<&T> = HashSet::from_iter(b.iter());
		let intersection_size = set_a.intersection(&set_b).count() as f64;
		let union_size = set_a.union(&set_b).count() as f64;
		intersection_size / union_size
	}

	pub(crate) fn jaccard_similarity(&self, other: &Self) -> f64 {
		match (self, other) {
			(TreeVector::F64(a, _), TreeVector::F64(b, _)) => Self::jaccard_f64(a, b),
			(TreeVector::F32(a, _), TreeVector::F32(b, _)) => Self::jaccard_f32(a, b),
			(TreeVector::I64(a, _), TreeVector::I64(b, _)) => Self::jaccard_integers(a, b),
			(TreeVector::I32(a, _), TreeVector::I32(b, _)) => Self::jaccard_integers(a, b),
			(TreeVector::I16(a, _), TreeVector::I16(b, _)) => Self::jaccard_integers(a, b),
			_ => f64::NAN,
		}
	}

	fn manhattan<T>(a: &[T], b: &[T]) -> f64
	where
		T: Sub<Output = T> + ToFloat + Copy,
	{
		a.iter().zip(b.iter()).map(|(&a, &b)| ((a - b).to_float()).abs()).sum()
	}

	pub(crate) fn manhattan_distance(&self, other: &Self) -> f64 {
		match (self, other) {
			(TreeVector::F64(a, _), TreeVector::F64(b, _)) => Self::manhattan(a, b),
			(TreeVector::F32(a, _), TreeVector::F32(b, _)) => Self::manhattan(a, b),
			(TreeVector::I64(a, _), TreeVector::I64(b, _)) => Self::manhattan(a, b),
			(TreeVector::I32(a, _), TreeVector::I32(b, _)) => Self::manhattan(a, b),
			(TreeVector::I16(a, _), TreeVector::I16(b, _)) => Self::manhattan(a, b),
			_ => f64::NAN,
		}
	}

	fn minkowski<T>(a: &[T], b: &[T], order: f64) -> f64
	where
		T: ToFloat,
	{
		let dist: f64 = a
			.iter()
			.zip(b.iter())
			.map(|(a, b)| (a.to_float() - b.to_float()).abs().powf(order))
			.sum();
		dist.powf(1.0 / order)
	}

	pub(crate) fn minkowski_distance(&self, other: &Self, order: f64) -> f64 {
		match (self, other) {
			(TreeVector::F64(a, _), TreeVector::F64(b, _)) => Self::minkowski(a, b, order),
			(TreeVector::F32(a, _), TreeVector::F32(b, _)) => Self::minkowski(a, b, order),
			(TreeVector::I64(a, _), TreeVector::I64(b, _)) => Self::minkowski(a, b, order),
			(TreeVector::I32(a, _), TreeVector::I32(b, _)) => Self::minkowski(a, b, order),
			(TreeVector::I16(a, _), TreeVector::I16(b, _)) => Self::minkowski(a, b, order),
			_ => f64::NAN,
		}
	}

	fn pearson<T>(a: &[T], b: &[T]) -> f64
	where
		T: ToFloat,
	{
		let m1 = a.mean();
		let m2 = b.mean();
		let covar: f64 =
			a.iter().zip(b.iter()).map(|(x, y)| (x.to_float() - m1) * (y.to_float() - m2)).sum();
		let covar = covar / a.len() as f64;
		let std_dev1 = deviation(a, m1, false);
		let std_dev2 = deviation(b, m2, false);
		covar / (std_dev1 * std_dev2)
	}

	fn pearson_similarity(&self, other: &Self) -> f64 {
		match (self, other) {
			(TreeVector::F64(a, _), TreeVector::F64(b, _)) => Self::pearson(a, b),
			(TreeVector::F32(a, _), TreeVector::F32(b, _)) => Self::pearson(a, b),
			(TreeVector::I64(a, _), TreeVector::I64(b, _)) => Self::pearson(a, b),
			(TreeVector::I32(a, _), TreeVector::I32(b, _)) => Self::pearson(a, b),
			(TreeVector::I16(a, _), TreeVector::I16(b, _)) => Self::pearson(a, b),
			_ => f64::NAN,
		}
	}
}
impl Distance {
	pub(super) fn dist(&self, a: &TreeVector, b: &TreeVector) -> f64 {
		match self {
			Distance::Chebyshev => a.chebyshev_distance(b),
			Distance::Cosine => a.cosine_distance(b),
			Distance::Euclidean => a.euclidean_distance(b),
			Distance::Hamming => a.hamming_distance(b),
			Distance::Jaccard => a.jaccard_similarity(b),
			Distance::Manhattan => a.manhattan_distance(b),
			Distance::Minkowski(order) => a.minkowski_distance(b, order.to_float()),
			Distance::Pearson => a.pearson_similarity(b),
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::trees::knn::tests::{get_seed_rnd, new_random_vec};
	use crate::idx::trees::vector::TreeVector;
	use crate::sql::index::{Distance, VectorType};
	use std::collections::HashSet;
	use std::hash::{Hash, Hasher};
	use std::sync::Arc;
	use std::time::SystemTime;

	fn test_distance(dist: Distance, size: usize, dim: usize) {
		let mut rng = get_seed_rnd();
		let mut coll = Vec::with_capacity(size);
		for vt in
			[VectorType::F64, VectorType::F32, VectorType::I64, VectorType::I32, VectorType::I16]
		{
			let integer = dist == Distance::Jaccard;
			for _ in 0..size {
				let v1 = new_random_vec(&mut rng, vt, dim, integer);
				let v2 = new_random_vec(&mut rng, vt, dim, integer);
				coll.push((v1, v2));
			}
			let mut num_zero = 0;
			for (i, (v1, v2)) in coll.iter().enumerate() {
				let d = dist.dist(v1, v2);
				assert!(
					d.is_finite() && !d.is_nan(),
					"i: {i} - vt: {vt} - v1: {v1:?} - v2: {v2:?}"
				);
				assert_ne!(d, f64::NAN, "i: {i} - vt: {vt} - v1: {v1:?} - v2: {v2:?}");
				assert_ne!(d, f64::INFINITY, "i: {i} - vt: {vt} - v1: {v1:?} - v2: {v2:?}");
				if d == 0.0 {
					num_zero += 1;
				}
			}
			let zero_rate = num_zero as f64 / size as f64;
			assert!(zero_rate < 0.1, "vt: {vt} - zero_rate: {zero_rate}");
		}
	}

	#[test]
	fn test_distance_chebyshev() {
		test_distance(Distance::Chebyshev, 2000, 1536);
	}

	#[test]
	fn test_distance_cosine() {
		test_distance(Distance::Cosine, 2000, 1536);
	}

	#[test]
	fn test_distance_euclidean() {
		test_distance(Distance::Euclidean, 2000, 1536);
	}

	#[test]
	fn test_distance_hamming() {
		test_distance(Distance::Hamming, 2000, 1536);
	}

	#[test]
	fn test_distance_jaccard() {
		test_distance(Distance::Jaccard, 1000, 1536);
	}
	#[test]
	fn test_distance_manhattan() {
		test_distance(Distance::Manhattan, 2000, 1536);
	}
	#[test]
	fn test_distance_minkowski() {
		test_distance(Distance::Minkowski(2.into()), 2000, 1536);
	}

	#[test]
	fn test_distance_pearson() {
		test_distance(Distance::Pearson, 2000, 1536);
	}

	#[test]
	fn test_vector_hash() {
		let capacity = 10000;
		let mut rng = get_seed_rnd();
		let mut collection_vec = Vec::with_capacity(capacity);
		let mut collection_hash_vec: Vec<Arc<VectorComputedHash>> = Vec::with_capacity(capacity);

		// Build collections
		for _ in 0..capacity {
			let vec = new_random_vec(&mut rng, VectorType::F64, 1000, false);
			collection_hash_vec.push(Arc::new(vec.as_ref().into()));
			collection_vec.push(vec);
		}

		// Construct a set with computed hashes
		let computed_hash = {
			let mut h = HashSet::with_capacity(capacity);
			let time = SystemTime::now();
			for v in &collection_hash_vec {
				h.insert(v.clone());
			}
			time.elapsed().unwrap().as_micros()
		};

		// Construct a set with stored hashes
		let stored_hash = {
			let mut h = HashSet::with_capacity(capacity);
			let time = SystemTime::now();
			for v in &collection_vec {
				h.insert(v.clone());
			}
			time.elapsed().unwrap().as_micros()
		};

		// Check that store hash insertion are more than 50 time faster than computed hash.
		let factor = computed_hash as f64 / stored_hash as f64;
		assert!(
			factor > 50.0,
			"Factor: {factor} - Compute hash: {computed_hash} - Stored hash: {stored_hash}"
		);
	}

	struct VectorComputedHash(Vec<f64>);

	impl Hash for VectorComputedHash {
		fn hash<H: Hasher>(&self, state: &mut H) {
			TreeVector::compute_hash_f64(&self.0, state);
		}
	}

	impl PartialEq<Self> for VectorComputedHash {
		fn eq(&self, other: &Self) -> bool {
			self.0 == other.0
		}
	}

	impl Eq for VectorComputedHash {}

	impl From<&TreeVector> for VectorComputedHash {
		fn from(v: &TreeVector) -> Self {
			match v {
				TreeVector::F64(v, _) => Self(v.clone()),
				_ => panic!("Not supported"),
			}
		}
	}
}
