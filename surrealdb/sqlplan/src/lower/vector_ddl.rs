//! DDL lowering of the pgvector surface: the `vector(n)` column type, `CREATE EXTENSION vector`,
//! and `CREATE INDEX ... USING hnsw`.

use surrealdb_expr::expr::index::{HnswParams, VectorType};
use surrealdb_expr::expr::index_kind::Index;
use surrealdb_expr::expr::{Expr, Kind};
use surrealdb_pgsql::ast::expr::{Expr as PgExpr, TypeName};
use surrealdb_pgsql::ast::literal::Literal as PgLiteral;
use surrealdb_pgsql::ast::stmt::{CreateExtensionStmt, CreateIndexStmt};

use crate::error::PlanError;
use crate::vector::{MAX_INDEXED_DIMENSIONS, VectorMetric, check_dimensions};

/// The default of the `m` storage parameter of an `hnsw` index.
const DEFAULT_M: i64 = 16;
/// The bounds of the `m` storage parameter.
const M_BOUNDS: (i64, i64) = (2, 100);
/// The default of the `ef_construction` storage parameter of an `hnsw` index.
const DEFAULT_EF_CONSTRUCTION: i64 = 64;
/// The bounds of the `ef_construction` storage parameter.
const EF_CONSTRUCTION_BOUNDS: (i64, i64) = (4, 1000);

/// Whether `type_name` names the `vector` type, whatever its modifiers and array bounds.
pub(crate) fn is_vector(type_name: &TypeName) -> bool {
	type_name.name.parts.last().is_some_and(|name| name.eq_ignore_ascii_case("vector"))
}

/// The dimensions a `vector` type name asks for: `None` when it is not a `vector` and `Some(None)`
/// when it names no dimensions.
pub(crate) fn dimensions(type_name: &TypeName) -> Result<Option<Option<usize>>, PlanError> {
	if !is_vector(type_name) {
		return Ok(None);
	}
	let Some(modifier) = type_name.modifiers.first() else {
		return Ok(Some(None));
	};
	if type_name.modifiers.len() > 1 {
		return Err(PlanError::InvalidParameterValue(
			"type modifier is not allowed for type \"vector\"".into(),
		));
	}
	let count = modifier.parse::<i64>().map_err(|_| {
		PlanError::InvalidParameterValue(format!(
			"invalid type modifier for type vector: {modifier}"
		))
	})?;
	Ok(Some(Some(check_dimensions(count)?)))
}

/// Whether `type_name` is a cast target that is a vector, which is read as the text form of one.
///
/// `Some` holds the dimensions it asks for. A cast to a vector of invalid dimensions is refused.
pub(crate) fn cast_target(type_name: &TypeName) -> Result<Option<Option<usize>>, PlanError> {
	dimensions(type_name)
}

/// The kind of a `vector` type name: an array of floats of the dimensions it names. A type name
/// that is not valid is refused where a statement is lowered ([`check_column`]), so a kind is
/// still produced for it.
pub(crate) fn kind(type_name: &TypeName) -> Kind {
	let count = match dimensions(type_name) {
		Ok(Some(Some(count))) => u64::try_from(count).ok(),
		Ok(_) | Err(_) => None,
	};
	let vector = Kind::Array(Box::new(Kind::Float), count);
	if type_name.array_bounds.is_empty() {
		vector
	} else {
		Kind::Array(Box::new(vector), None)
	}
}

/// The kind of a MySQL `VECTOR(n)` column type, which is the kind of the PostgreSQL `vector(n)`
/// with the same dimensions. `type_str` is the lower-case type text, such as `vector(768)`. A count
/// that is not valid gives the kind with no dimensions, as [`kind`] does.
pub(crate) fn mysql_kind(type_str: &str) -> Kind {
	let count = type_str
		.split_once('(')
		.and_then(|(_, args)| args.split(')').next())
		.and_then(|count| count.trim().parse::<i64>().ok())
		.and_then(|count| check_dimensions(count).ok())
		.and_then(|count| u64::try_from(count).ok());
	Kind::Array(Box::new(Kind::Float), count)
}

/// Refuse a column type that is a `vector` of no or of invalid dimensions.
///
/// A column that holds a vector has its dimensions, which is what tells it from an array of
/// floats; a `vector` cast has none.
pub(crate) fn check_column(type_name: &TypeName) -> Result<(), PlanError> {
	match dimensions(type_name)? {
		Some(None) => Err(PlanError::UnsupportedStatement(
			"a vector column without dimensions; declare it as vector(n)".into(),
		)),
		Some(Some(_)) if !type_name.array_bounds.is_empty() => {
			Err(PlanError::UnsupportedStatement("an array of vector columns".into()))
		}
		Some(Some(_)) | None => Ok(()),
	}
}

/// Lower `CREATE EXTENSION`: the `vector` extension is built in, so naming it defines nothing, and
/// any other extension does not exist.
pub(crate) fn lower_create_extension(stmt: &CreateExtensionStmt) -> Result<Vec<Expr>, PlanError> {
	if !stmt.name.eq_ignore_ascii_case("vector") {
		return Err(PlanError::ExtensionNotAvailable(stmt.name.clone()));
	}
	Ok(Vec::new())
}

/// The value of the integer storage parameter `name`, within `bounds`.
fn integer_option(name: &str, value: &PgExpr, bounds: (i64, i64)) -> Result<i64, PlanError> {
	let parsed = if let PgExpr::Literal {
		lit,
		..
	} = value
	{
		match lit {
			PgLiteral::Integer(value) => Some(*value),
			PgLiteral::String(text) => text.trim().parse::<i64>().ok(),
			PgLiteral::Float(_)
			| PgLiteral::Numeric(_)
			| PgLiteral::Boolean(_)
			| PgLiteral::Null
			| PgLiteral::BitString {
				..
			} => None,
		}
	} else {
		None
	};
	let Some(value) = parsed else {
		return Err(PlanError::InvalidParameterValue(format!(
			"invalid value for integer option \"{name}\""
		)));
	};
	if value < bounds.0 || value > bounds.1 {
		return Err(PlanError::InvalidParameterValue(format!(
			"value {value} out of bounds for option \"{name}\"; valid values are between \"{}\" and \"{}\"",
			bounds.0, bounds.1
		)));
	}
	Ok(value)
}

/// The index an `hnsw` access method defines for `idx`, over a column of `dimensions` dimensions.
///
/// The operator class names the distance, and the storage parameters `m` and `ef_construction`
/// are read as pgvector reads them, with its defaults. Whatever the engine's index cannot do is
/// refused: unique, multicolumn, partial and ordered indexes, and an index over an expression.
pub(crate) fn hnsw_index(
	idx: &CreateIndexStmt,
	dimensions: Option<usize>,
) -> Result<Index, PlanError> {
	const METHOD: &str = "hnsw";
	let refuse = |what: &str| {
		Err(PlanError::UnsupportedStatement(format!(
			"access method \"{METHOD}\" does not support {what}"
		)))
	};
	if idx.unique {
		return refuse("unique indexes");
	}
	let [column] = idx.columns.as_slice() else {
		return refuse("multicolumn indexes");
	};
	if idx.predicate.is_some() {
		return refuse("partial indexes");
	}
	if column.direction.is_some() {
		return refuse("ASC/DESC options");
	}
	if column.nulls.is_some() {
		return refuse("NULLS FIRST/LAST options");
	}
	if !matches!(&column.expr, PgExpr::ColumnRef(_)) {
		return refuse("expression indexes");
	}

	let Some(opclass) = &column.opclass else {
		return Err(PlanError::NoDefaultOperatorClass {
			type_name: "vector".into(),
			method: METHOD.into(),
		});
	};
	let opclass_name =
		opclass.parts.last().map(|name| name.to_ascii_lowercase()).unwrap_or_default();
	let Some(metric) = VectorMetric::from_hnsw_opclass(&opclass_name) else {
		if ["halfvec_", "sparsevec_", "bit_"].iter().any(|prefix| opclass_name.starts_with(prefix))
		{
			return Err(PlanError::UnsupportedStatement(format!(
				"operator class \"{opclass_name}\": only the vector type is supported"
			)));
		}
		return Err(PlanError::UndefinedOperatorClass {
			name: opclass_name,
			method: METHOD.into(),
		});
	};

	let mut m = DEFAULT_M;
	let mut ef_construction = DEFAULT_EF_CONSTRUCTION;
	for option in &idx.options {
		let Some(value) = &option.value else {
			return Err(PlanError::InvalidParameterValue(format!(
				"parameter \"{}\" requires a value",
				option.name
			)));
		};
		match option.name.as_str() {
			"m" => m = integer_option("m", value, M_BOUNDS)?,
			"ef_construction" => {
				ef_construction = integer_option("ef_construction", value, EF_CONSTRUCTION_BOUNDS)?
			}
			other => {
				return Err(PlanError::InvalidParameterValue(format!(
					"unrecognized parameter \"{other}\""
				)));
			}
		}
	}
	if ef_construction < 2 * m {
		return Err(PlanError::InvalidParameterValue(
			"ef_construction must be greater than or equal to 2 * m".into(),
		));
	}

	let Some(dimensions) = dimensions else {
		return Err(PlanError::InvalidParameterValue("column does not have dimensions".into()));
	};
	if dimensions > MAX_INDEXED_DIMENSIONS {
		return Err(PlanError::ProgramLimitExceeded(format!(
			"column cannot have more than {MAX_INDEXED_DIMENSIONS} dimensions for {METHOD} index"
		)));
	}
	let (Ok(dimension), Ok(m), Ok(ef_construction)) =
		(u16::try_from(dimensions), u8::try_from(m), u16::try_from(ef_construction))
	else {
		return Err(PlanError::InvalidParameterValue("hnsw parameter out of range".into()));
	};
	Ok(Index::Hnsw(HnswParams {
		dimension,
		distance: metric.index_distance(),
		vector_type: VectorType::F32,
		m,
		m0: m.saturating_mul(2),
		ml: (1.0 / f64::from(m).ln()).into(),
		ef_construction,
		extend_candidates: false,
		keep_pruned_connections: false,
		use_hashed_vector: false,
	}))
}

/// Refuse what only an `hnsw` index reads, on an index of another access method: an operator class
/// or a storage parameter would otherwise be dropped without a word.
pub(crate) fn refuse_vector_options(idx: &CreateIndexStmt) -> Result<(), PlanError> {
	if idx.columns.iter().any(|column| column.opclass.is_some()) {
		return Err(PlanError::UnsupportedStatement(
			"an operator class on an index that is not an hnsw index".into(),
		));
	}
	if !idx.options.is_empty() {
		return Err(PlanError::UnsupportedStatement(
			"storage parameters on an index that is not an hnsw index".into(),
		));
	}
	Ok(())
}
