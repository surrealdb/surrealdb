//! The vector measures a program answers where the row lies: a call of a
//! two-argument `vector::*` builtin whose one argument is a field of the row and
//! whose other is a vector in hand before the row ([`Instr::Measure`]).
//!
//! A measure answers what the call answers, value and error alike, and at the
//! time the call would. The call's capability check and its query argument are
//! settled once as the run of their group starts ([`MeasureSettled`]). Where both
//! succeed and the query does not depend on the row, the interpreter answers each
//! row: a row whose field holds an array of numbers of the query's dimension is
//! measured from the numbers read where the field lies, through the same measure
//! the builtin computes, and any other row is answered by the registry's builtin
//! itself, over the field decoded, so its error is the builtin's. Otherwise the
//! host answers each row as the call's own instructions do, raising a refusal or a
//! failure with the error the call raises, and never sooner than the first row: a
//! table with no row raises nothing.
//!
//! [`Instr::Measure`]: super::program::Instr::Measure

use surrealdb_runtime::util::math::vector::{
	ChebyshevDistance, CosineDistance, CosineSimilarity, DotProduct, EuclideanDistance,
	HammingDistance, JaccardSimilarity, ManhattanDistance,
};

use crate::exec::function::ScalarFunction;
use crate::val::{Number, Value};

/// A two-argument `vector::*` or `geo::distance` builtin a measure answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MeasureKind {
	Euclidean,
	Manhattan,
	Chebyshev,
	Hamming,
	CosineDistance,
	Cosine,
	Jaccard,
	Dot,
	GeoDistance,
}

impl MeasureKind {
	/// The measure of builtin `name`, where a measure answers it.
	pub(super) fn of(name: &str) -> Option<Self> {
		Some(match name {
			"vector::distance::euclidean" => Self::Euclidean,
			"vector::distance::manhattan" => Self::Manhattan,
			"vector::distance::chebyshev" => Self::Chebyshev,
			"vector::distance::hamming" => Self::Hamming,
			"vector::distance::cosine" => Self::CosineDistance,
			"vector::similarity::cosine" => Self::Cosine,
			"vector::similarity::jaccard" => Self::Jaccard,
			"vector::dot" => Self::Dot,
			"geo::distance" => Self::GeoDistance,
			_ => return None,
		})
	}

	/// The measure of `a` and `b`, in the builtin's argument order, as the
	/// builtin computes it over two arrays of these numbers.
	pub(super) fn compute(self, a: &[Number], b: &[Number]) -> anyhow::Result<Number> {
		match self {
			Self::Euclidean => a.euclidean_distance(b),
			Self::Manhattan => a.manhattan_distance(b),
			Self::Chebyshev => a.chebyshev_distance(b),
			Self::Hamming => a.hamming_distance(b),
			Self::CosineDistance => a.cosine_distance(b),
			Self::Cosine => a.cosine_similarity(b),
			Self::Jaccard => a.jaccard_similarity(b),
			Self::Dot => a.dot(b),
			Self::GeoDistance => anyhow::bail!("geo::distance does not operate on number vectors"),
		}
	}
}

/// A measure's call: the builtin, the call site whose name the capability check
/// reads and the run's cell is held under, and the query argument.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct MeasureSpec {
	pub(super) kind: MeasureKind,
	/// The call site in [`Program::calls`](super::program::Program): the builtin
	/// the capability check names, and the index of the run's
	/// [`MeasureSettled`].
	pub(super) call: u32,
	/// The query argument, read with no row as the run starts where its read cannot depend on
	/// the row, and by the host for each row otherwise.
	pub(super) query: MeasureQuery,
	/// Whether the query is the builtin's first argument and the row's field its
	/// second.
	pub(super) query_first: bool,
}

impl MeasureSpec {
	/// The arguments of the call, in the builtin's order.
	pub(super) fn args(&self, field: Value, query: Value) -> Vec<Value> {
		if self.query_first {
			vec![query, field]
		} else {
			vec![field, query]
		}
	}

	/// The measure of the row's numbers `field` and the query's `query`, in the
	/// builtin's order.
	pub(super) fn compute(&self, field: &[Number], query: &[Number]) -> anyhow::Result<Number> {
		if self.query_first {
			self.kind.compute(query, field)
		} else {
			self.kind.compute(field, query)
		}
	}

	/// The spatial distance between the row's point `field` and the query's `query`, in the
	/// builtin's order.
	pub(super) fn compute_point(
		&self,
		field: (f64, f64),
		query: (f64, f64),
	) -> anyhow::Result<Value> {
		let (a, b) = if self.query_first {
			(query, field)
		} else {
			(field, query)
		};
		crate::fnc::geo::distance((
			crate::val::Geometry::Point(a.into()),
			crate::val::Geometry::Point(b.into()),
		))
	}
}

/// Where a measure's query argument is read from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MeasureQuery {
	/// A literal, [`Program::consts`](super::program::Program)`[index]`.
	Const(u32),
	/// A parameter of the request, [`Program::params`](super::program::Program)`[index]`, which
	/// may resolve to one the catalog defines.
	Param(u32),
}

/// What a measure's call settles as the run of its group starts.
#[derive(Debug)]
pub(super) enum MeasureSettled {
	/// The host answers the call for each row, as the call's own instructions answer it: its
	/// capability check, its query read with the row in scope, then the registry's builtin. A
	/// run settles a measure so where the check refuses the call, where the query's read fails,
	/// where the query is a parameter whose read judges a `PERMISSIONS` predicate against each
	/// row, and where the builtin cannot be called without the host. A refusal or a failure is
	/// raised by the host as the call raises it, by the first row that reaches the call. A run
	/// holding such a measure reads that pipeline's rows decoded.
	Host,
	/// The call is allowed, its query read, and the interpreter answers each row.
	Allowed {
		/// The registry's builtin of the call's name, which answers a row the measure does not
		/// compute itself. It is pure and called synchronously.
		function: &'static dyn ScalarFunction,
		/// The query argument.
		query: Value,
		/// The numbers of the query, where it is an array of numbers only.
		numbers: Option<Box<[Number]>>,
		/// The `(lon, lat)` coordinates of the query, where it is a geometry point.
		point: Option<(f64, f64)>,
	},
}

impl MeasureSettled {
	/// The outcome of a call `function` answers that the capability check allowed, whose query
	/// argument is `query`.
	pub(super) fn allowed(function: &'static dyn ScalarFunction, query: Value) -> Self {
		let numbers = if let Value::Array(array) = &query {
			array
				.iter()
				.map(|value| {
					if let Value::Number(number) = value {
						Some(number.clone())
					} else {
						None
					}
				})
				.collect::<Option<Box<[Number]>>>()
		} else {
			None
		};
		let point = if let Value::Geometry(crate::val::Geometry::Point(pt)) = &query {
			Some((pt.x(), pt.y()))
		} else {
			None
		};
		Self::Allowed {
			function,
			query,
			numbers,
			point,
		}
	}
}
