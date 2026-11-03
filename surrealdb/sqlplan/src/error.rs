//! Error types for SQL binding and planning.

use thiserror::Error;

/// Error returned during binding or planning of an SQL query.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PlanError {
	#[error("schema or table \"{0}\" does not exist")]
	UndefinedTable(String),

	#[error("column \"{0}\" does not exist")]
	UndefinedColumn(String),

	#[error("column reference \"{0}\" is ambiguous")]
	AmbiguousColumn(String),

	#[error("missing FROM-clause entry for table \"{0}\"")]
	MissingFromClauseEntry(String),

	#[error("GROUP BY position {0} is not in select list")]
	InvalidGroupByPosition(usize),

	#[error("ORDER BY position {0} is not in select list")]
	InvalidOrderByPosition(usize),

	#[error("for SELECT DISTINCT, ORDER BY expressions must appear in select list")]
	DistinctOrderByNotSelected,

	#[error(
		"column \"{0}\" must appear in the GROUP BY clause or be used in an aggregate function"
	)]
	ColumnNotInGroupBy(String),

	#[error("aggregate functions are not allowed in {0}")]
	AggregateNotAllowed(String),

	#[error("window functions are not allowed in {0}")]
	WindowNotAllowed(String),

	#[error("cannot override PARTITION BY clause of window \"{0}\"")]
	WindowPartitionOverride(String),

	#[error("cannot override ORDER BY clause of window \"{0}\"")]
	WindowOrderOverride(String),

	#[error("cannot copy window \"{0}\" because it has a frame clause")]
	WindowFrameCopy(String),

	#[error("function \"{0}\" does not exist")]
	UndefinedFunction(String),

	#[error("operator does not exist: {0}")]
	UndefinedOperator(String),

	#[error("function \"{name}\" requires {expected} arguments, but {got} were provided")]
	FunctionArgumentCountMismatch {
		name: String,
		expected: String,
		got: usize,
	},

	#[error("function \"{0}\" must be called with an OVER clause")]
	WindowFunctionWithoutOver(String),

	#[error("subquery in FROM must have an alias")]
	SubqueryMustHaveAlias,

	#[error("parameter ${0} out of range")]
	ParameterOutOfRange(usize),

	#[error("division by zero")]
	DivisionByZero,

	#[error("bigint out of range")]
	NumericValueOutOfRange,

	/// A constant beyond the range of the type it is assigned to, with PostgreSQL's message for
	/// that type: `smallint out of range`, `numeric field overflow`, `value out of range:
	/// overflow`.
	#[error("{0}")]
	ValueOutOfRange(String),

	#[error("invalid input syntax for type: {0}")]
	InvalidTextRepresentation(String),

	#[error("unsupported statement: {0}")]
	UnsupportedStatement(String),

	#[error("unsupported expression: {0}")]
	UnsupportedExpression(String),

	#[error("there is no transaction in progress")]
	NoActiveTransaction,

	#[error("current transaction is aborted, commands ignored until end of transaction block")]
	InFailedTransaction,

	#[error("savepoint \"{0}\" does not exist")]
	SavepointDoesNotExist(String),

	#[error("there is already a transaction in progress")]
	ActiveTransactionAlreadyInProgress,

	#[error("unrecognized configuration parameter \"{0}\"")]
	UndefinedObject(String),

	#[error("constraint \"{constraint}\" of relation \"{table}\" does not exist")]
	UndefinedConstraint {
		constraint: String,
		table: String,
	},

	#[error("type \"{0}\" does not exist")]
	UndefinedType(String),

	#[error("constraint \"{constraint}\" of domain \"{domain}\" does not exist")]
	UndefinedDomainConstraint {
		constraint: String,
		domain: String,
	},

	#[error("constraint \"{constraint}\" for domain \"{domain}\" already exists")]
	DuplicateDomainConstraint {
		constraint: String,
		domain: String,
	},

	#[error("enum label \"{0}\" used more than once")]
	DuplicateEnumLabel(String),

	#[error("column \"{0}\" specified more than once")]
	DuplicateColumn(String),

	#[error("invalid parameter value: {0}")]
	InvalidParameterValue(String),

	#[error("{0}")]
	DatatypeMismatch(String),

	#[error("data exception: {0}")]
	DataException(String),

	#[error("program limit exceeded: {0}")]
	ProgramLimitExceeded(String),

	#[error("operator class \"{name}\" does not exist for access method \"{method}\"")]
	UndefinedOperatorClass {
		name: String,
		method: String,
	},

	#[error("data type {type_name} has no default operator class for access method \"{method}\"")]
	NoDefaultOperatorClass {
		type_name: String,
		method: String,
	},

	#[error("extension \"{0}\" is not available")]
	ExtensionNotAvailable(String),

	#[error("planning error: {0}")]
	Custom(String),
}

impl PlanError {
	/// Convenience constructor for unsupported feature errors.
	pub fn unsupported(msg: impl Into<String>) -> Self {
		Self::UnsupportedExpression(msg.into())
	}

	/// Return standard SQLSTATE error code for PostgreSQL.
	pub fn pg_sqlstate(&self) -> &'static str {
		match self {
			Self::UndefinedTable(_) | Self::MissingFromClauseEntry(_) => "42P01", /* undefined_table */
			Self::UndefinedColumn(_) => "42703",                                  /* undefined_column */
			Self::AmbiguousColumn(_) => "42702",                                  /* ambiguous_column */
			Self::InvalidGroupByPosition(_)
			| Self::InvalidOrderByPosition(_)
			| Self::DistinctOrderByNotSelected => "42P10", /* invalid_column_reference */
			Self::ColumnNotInGroupBy(_) => "42803",                               // grouping_error
			Self::AggregateNotAllowed(_) => "42803",                              // grouping_error
			Self::WindowNotAllowed(_)
			| Self::WindowPartitionOverride(_)
			| Self::WindowOrderOverride(_)
			| Self::WindowFrameCopy(_) => "42P20", /* windowing_error */
			Self::UndefinedFunction(_)
			| Self::UndefinedOperator(_)
			| Self::FunctionArgumentCountMismatch {
				..
			} => "42883", /* undefined_function */
			Self::WindowFunctionWithoutOver(_) => "42809",                        /* wrong_object_type */
			Self::SubqueryMustHaveAlias => "42601",                               // syntax_error
			Self::ParameterOutOfRange(_) => "22023", // invalid_parameter_value
			Self::DivisionByZero => "22012",         // division_by_zero
			Self::NumericValueOutOfRange | Self::ValueOutOfRange(_) => "22003", /* numeric_value_out_of_range */
			Self::InvalidTextRepresentation(_) => "22P02", // invalid_text_representation
			Self::UnsupportedStatement(_) | Self::UnsupportedExpression(_) => "0A000", /* feature_not_supported */
			Self::NoActiveTransaction => "25P01", // no_active_sql_transaction
			Self::InFailedTransaction => "25P02", // in_failed_sql_transaction
			Self::SavepointDoesNotExist(_) => "3B001", // invalid_savepoint_specification
			Self::ActiveTransactionAlreadyInProgress => "25001", // active_sql_transaction
			Self::UndefinedObject(_)
			| Self::UndefinedType(_)
			| Self::UndefinedDomainConstraint {
				..
			}
			| Self::UndefinedConstraint {
				..
			} => "42704", // undefined_object
			Self::DuplicateDomainConstraint {
				..
			}
			| Self::DuplicateEnumLabel(_) => "42710", // duplicate_object
			Self::DuplicateColumn(_) => "42701",  // duplicate_column
			Self::InvalidParameterValue(_) => "22023", // invalid_parameter_value
			Self::DatatypeMismatch(_) => "42804", // datatype_mismatch
			Self::DataException(_) => "22000",    // data_exception
			Self::ProgramLimitExceeded(_) => "54000", // program_limit_exceeded
			Self::UndefinedOperatorClass {
				..
			}
			| Self::NoDefaultOperatorClass {
				..
			} => "42704", /* undefined_object */
			Self::ExtensionNotAvailable(_) => "58P01", // undefined_file
			Self::Custom(_) => "42000",
		}
	}

	/// Return standard MySQL error code.
	pub fn mysql_error_code(&self) -> u16 {
		match self {
			Self::UndefinedTable(_) => 1146,         // ER_NO_SUCH_TABLE
			Self::UndefinedColumn(_) => 1054,        // ER_BAD_FIELD_ERROR
			Self::AmbiguousColumn(_) => 1052,        // ER_NON_UNIQ_ERROR
			Self::MissingFromClauseEntry(_) => 1054, // ER_BAD_FIELD_ERROR
			Self::InvalidGroupByPosition(_) | Self::InvalidOrderByPosition(_) => 1054,
			Self::DistinctOrderByNotSelected => 3065, // ER_FIELD_IN_ORDER_NOT_SELECT
			Self::ColumnNotInGroupBy(_) => 1055,      // ER_WRONG_FIELD_WITH_GROUP
			Self::AggregateNotAllowed(_) | Self::WindowNotAllowed(_) => 1111, /* ER_INVALID_GROUP_FUNC_USE */
			Self::WindowPartitionOverride(_) => 3581, // ER_WINDOW_NO_CHILD_PARTITIONING
			Self::WindowFrameCopy(_) => 3582,         // ER_WINDOW_NO_INHERIT_FRAME
			Self::WindowOrderOverride(_) => 3583,     // ER_WINDOW_NO_REDEFINE_ORDER_BY
			Self::UndefinedFunction(_) => 1305,       // ER_SP_DOES_NOT_EXIST
			Self::FunctionArgumentCountMismatch {
				..
			} => 1582, /* ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT */
			Self::WindowFunctionWithoutOver(_) => 3593, // ER_WINDOW_FUNCTION_WITHOUT_OVER
			Self::SubqueryMustHaveAlias => 1248,      // ER_DERIVED_MUST_HAVE_ALIAS
			Self::ParameterOutOfRange(_) => 1210,     // ER_WRONG_ARGUMENTS
			Self::DivisionByZero => 1365,             // ER_DIVISION_BY_ZERO
			Self::NumericValueOutOfRange | Self::ValueOutOfRange(_) => 1690, // ER_DATA_OUT_OF_RANGE
			Self::InvalidTextRepresentation(_) => 1264, // ER_DATA_OUT_OF_RANGE
			Self::UnsupportedStatement(_) | Self::UnsupportedExpression(_) => 1235, /* ER_NOT_SUPPORTED_YET */
			Self::NoActiveTransaction => 1105,                                      /* ER_UNKNOWN_ERROR */
			Self::InFailedTransaction => 1105,                                      /* ER_UNKNOWN_ERROR */
			Self::SavepointDoesNotExist(_) => 1305, // ER_SP_DOES_NOT_EXIST
			Self::ActiveTransactionAlreadyInProgress => 1105, // ER_UNKNOWN_ERROR
			Self::UndefinedObject(_) => 1193,       // ER_UNKNOWN_SYSTEM_VARIABLE
			Self::UndefinedConstraint {
				..
			} => 1091, // ER_CANT_DROP_FIELD_OR_KEY
			Self::InvalidParameterValue(_) => 1210, // ER_WRONG_ARGUMENTS
			Self::DatatypeMismatch(_) => 1366,      // ER_TRUNCATED_WRONG_VALUE_FOR_FIELD
			Self::DuplicateColumn(_) => 1060,       // ER_DUP_FIELDNAME
			Self::UndefinedType(_)
			| Self::UndefinedDomainConstraint {
				..
			}
			| Self::DuplicateDomainConstraint {
				..
			}
			| Self::DuplicateEnumLabel(_)
			| Self::DataException(_)
			| Self::ProgramLimitExceeded(_)
			| Self::UndefinedOperatorClass {
				..
			}
			| Self::NoDefaultOperatorClass {
				..
			}
			| Self::ExtensionNotAvailable(_) => 1105, // ER_UNKNOWN_ERROR
			Self::UndefinedOperator(_) | Self::Custom(_) => 1064,
		}
	}
}
