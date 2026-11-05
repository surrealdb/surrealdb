//! The pgvector surface of the PostgreSQL front end: the `vector(n)` type, its text form, the
//! distance operators and functions, and the operator classes of an `hnsw` index.
//!
//! The operators `<->`, `<=>`, `<#>` and `<+>` are claimed by the PostgreSQL binder only. Each is
//! bound to the distance function of the same meaning, so an operator and its function are one
//! node of the bound IR.

use std::str::FromStr;
use std::sync::LazyLock;

use surrealdb_expr::expr::index::Distance;
use surrealdb_expr::val::Regex;

use crate::error::PlanError;

/// The text form of a vector, which text is checked against when it is read where the statement
/// runs: a bracketed, comma-separated list of numbers.
pub fn text_form() -> Result<Regex, PlanError> {
	static FORM: LazyLock<Option<Regex>> = LazyLock::new(|| {
		let number = r"[-+]?(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[eE][-+]?[0-9]+)?";
		Regex::from_str(&format!(
			r"^[ \t\r\n]*\[[ \t\r\n]*{number}(?:[ \t\r\n]*,[ \t\r\n]*{number})*[ \t\r\n]*\][ \t\r\n]*$"
		))
		.ok()
	});
	FORM.clone().ok_or_else(|| PlanError::Custom("the text form of a vector".to_string()))
}

/// The error a text that is not the text form of a vector raises where it is read.
pub const MALFORMED: &str = "invalid input syntax for type vector";

/// The most dimensions a `vector` has.
pub const MAX_DIMENSIONS: usize = 16_000;

/// The most dimensions of a column an `hnsw` index covers.
pub const MAX_INDEXED_DIMENSIONS: usize = 2_000;

/// A distance the `vector` type is compared by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorMetric {
	/// Euclidean distance (`<->`, `l2_distance`).
	L2,
	/// One minus the cosine similarity (`<=>`, `cosine_distance`).
	Cosine,
	/// The negated inner product (`<#>`, `vector_negative_inner_product`).
	NegativeInnerProduct,
	/// Manhattan distance (`<+>`, `l1_distance`).
	L1,
}

impl VectorMetric {
	/// The metric of the distance operator `op`, if it is one.
	pub fn from_operator(op: &str) -> Option<Self> {
		match op {
			"<->" => Some(Self::L2),
			"<=>" => Some(Self::Cosine),
			"<#>" => Some(Self::NegativeInnerProduct),
			"<+>" => Some(Self::L1),
			_ => None,
		}
	}

	/// The metric of the function `name`, which is a distance function the front end binds and
	/// lowers as an operator.
	pub fn from_function(name: &str) -> Option<Self> {
		match name {
			"l2_distance" => Some(Self::L2),
			"cosine_distance" => Some(Self::Cosine),
			"vector_negative_inner_product" => Some(Self::NegativeInnerProduct),
			"l1_distance" => Some(Self::L1),
			_ => None,
		}
	}

	/// The name of the function that computes this metric.
	pub fn function(self) -> &'static str {
		match self {
			Self::L2 => "l2_distance",
			Self::Cosine => "cosine_distance",
			Self::NegativeInnerProduct => "vector_negative_inner_product",
			Self::L1 => "l1_distance",
		}
	}

	/// The distance an index with this metric ranks by, which a nearest-neighbour search of the
	/// same metric is answered from.
	pub fn index_distance(self) -> Distance {
		match self {
			Self::L2 => Distance::Euclidean,
			Self::Cosine => Distance::Cosine,
			Self::NegativeInnerProduct => Distance::InnerProduct,
			Self::L1 => Distance::Manhattan,
		}
	}

	/// The metric of the `hnsw` operator class `name`.
	pub fn from_hnsw_opclass(name: &str) -> Option<Self> {
		match name {
			"vector_l2_ops" => Some(Self::L2),
			"vector_cosine_ops" => Some(Self::Cosine),
			"vector_ip_ops" => Some(Self::NegativeInnerProduct),
			"vector_l1_ops" => Some(Self::L1),
			_ => None,
		}
	}
}

/// Whether `name` is a function the front end binds as a vector distance.
pub fn is_distance_function(name: &str) -> bool {
	VectorMetric::from_function(name).is_some()
}

/// The dimensions a `vector` type name asks for: `None` for a bare `vector`, and an error for a
/// count a `vector` cannot have.
pub fn check_dimensions(count: i64) -> Result<usize, PlanError> {
	if count < 1 {
		return Err(PlanError::InvalidParameterValue(
			"dimensions for type vector must be at least 1".into(),
		));
	}
	match usize::try_from(count) {
		Ok(count) if count <= MAX_DIMENSIONS => Ok(count),
		Ok(_) | Err(_) => Err(PlanError::InvalidParameterValue(format!(
			"dimensions for type vector cannot exceed {MAX_DIMENSIONS}"
		))),
	}
}

/// The elements of the text form of a vector, `[1,2,3]`.
///
/// Spaces around the brackets and the elements are ignored, as PostgreSQL ignores them. The
/// elements are finite numbers: `NaN` and the infinities are refused. An empty vector is refused
/// too, a `vector` having at least one dimension.
pub fn parse_text(text: &str) -> Result<Vec<f64>, PlanError> {
	let malformed = || PlanError::InvalidTextRepresentation(format!("vector: \"{text}\""));
	let inner = text
		.trim()
		.strip_prefix('[')
		.and_then(|rest| rest.strip_suffix(']'))
		.ok_or_else(malformed)?;
	let mut elements = Vec::new();
	for item in inner.split(',') {
		let item = item.trim();
		let value: f64 = item.parse().map_err(|_| malformed())?;
		if item.is_empty() || !value.is_finite() {
			return Err(malformed());
		}
		elements.push(value);
		if elements.len() > MAX_DIMENSIONS {
			return Err(PlanError::InvalidParameterValue(format!(
				"vector cannot have more than {MAX_DIMENSIONS} dimensions"
			)));
		}
	}
	Ok(elements)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn text_form_is_a_bracketed_list_of_finite_numbers() {
		assert_eq!(parse_text("[1,2,3]").unwrap(), [1.0, 2.0, 3.0]);
		assert_eq!(parse_text(" [ 1.5 , -2e1 ] ").unwrap(), [1.5, -20.0]);
	}

	#[test]
	fn text_form_refuses_what_is_not_a_vector() {
		for text in ["", "[]", "[1,]", "[,1]", "1,2", "[1,2", "[a]", "[NaN]", "[inf]", "[1 2]"] {
			assert!(
				matches!(parse_text(text), Err(PlanError::InvalidTextRepresentation(_))),
				"{text}"
			);
		}
	}

	#[test]
	fn text_form_has_at_most_the_most_dimensions() {
		let at_limit = format!("[{}]", vec!["1"; MAX_DIMENSIONS].join(","));
		assert_eq!(parse_text(&at_limit).unwrap().len(), MAX_DIMENSIONS);
		let over = format!("[{}]", vec!["1"; MAX_DIMENSIONS + 1].join(","));
		assert!(matches!(parse_text(&over), Err(PlanError::InvalidParameterValue(_))));
	}

	#[test]
	fn dimensions_are_between_one_and_the_limit() {
		assert_eq!(check_dimensions(1).unwrap(), 1);
		assert_eq!(check_dimensions(16_000).unwrap(), 16_000);
		for count in [0, -1, 16_001, i64::MAX] {
			assert!(matches!(check_dimensions(count), Err(PlanError::InvalidParameterValue(_))));
		}
	}

	#[test]
	fn an_operator_and_its_function_share_a_metric() {
		for op in ["<->", "<=>", "<#>", "<+>"] {
			let metric = VectorMetric::from_operator(op).unwrap();
			assert_eq!(VectorMetric::from_function(metric.function()), Some(metric));
		}
		assert_eq!(VectorMetric::from_operator("<@"), None);
	}
}

#[cfg(test)]
mod form_tests {
	use super::*;

	#[test]
	fn the_text_form_regex_compiles() {
		assert!(text_form().is_ok());
	}
}
