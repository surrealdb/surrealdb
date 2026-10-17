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

	#[error(
		"column \"{0}\" must appear in the GROUP BY clause or be used in an aggregate function"
	)]
	ColumnNotInGroupBy(String),

	#[error("aggregate functions are not allowed in {0}")]
	AggregateNotAllowed(String),

	#[error("window functions are not allowed in {0}")]
	WindowNotAllowed(String),

	#[error("function \"{0}\" does not exist")]
	UndefinedFunction(String),

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
			Self::InvalidGroupByPosition(_) | Self::InvalidOrderByPosition(_) => "42P10", /* invalid_column_reference */
			Self::ColumnNotInGroupBy(_) => "42803", // grouping_error
			Self::AggregateNotAllowed(_) | Self::WindowNotAllowed(_) => "42803", // grouping_error
			Self::UndefinedFunction(_)
			| Self::FunctionArgumentCountMismatch {
				..
			} => "42883", /* undefined_function */
			Self::WindowFunctionWithoutOver(_) => "42809", // wrong_object_type
			Self::SubqueryMustHaveAlias => "42601", // syntax_error
			Self::ParameterOutOfRange(_) => "22023", // invalid_parameter_value
			Self::DivisionByZero => "22012",        // division_by_zero
			Self::InvalidTextRepresentation(_) => "22P02", // invalid_text_representation
			Self::UnsupportedStatement(_) | Self::UnsupportedExpression(_) => "0A000", /* feature_not_supported */
			Self::NoActiveTransaction => "25P01", // no_active_sql_transaction
			Self::InFailedTransaction => "25P02", // in_failed_sql_transaction
			Self::SavepointDoesNotExist(_) => "3B001", // invalid_savepoint_specification
			Self::ActiveTransactionAlreadyInProgress => "25001", // active_sql_transaction
			Self::UndefinedObject(_) => "42704",  // undefined_object
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
			Self::ColumnNotInGroupBy(_) => 1055, // ER_WRONG_FIELD_WITH_GROUP
			Self::AggregateNotAllowed(_) | Self::WindowNotAllowed(_) => 1111, /* ER_INVALID_GROUP_FUNC_USE */
			Self::UndefinedFunction(_) => 1305,  // ER_SP_DOES_NOT_EXIST
			Self::FunctionArgumentCountMismatch {
				..
			} => 1582, /* ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT */
			Self::WindowFunctionWithoutOver(_) => 3593, // ER_WINDOW_FUNCTION_WITHOUT_OVER
			Self::SubqueryMustHaveAlias => 1248, // ER_DERIVED_MUST_HAVE_ALIAS
			Self::ParameterOutOfRange(_) => 1210, // ER_WRONG_ARGUMENTS
			Self::DivisionByZero => 1365,        // ER_DIVISION_BY_ZERO
			Self::InvalidTextRepresentation(_) => 1264, // ER_DATA_OUT_OF_RANGE
			Self::UnsupportedStatement(_) | Self::UnsupportedExpression(_) => 1235, /* ER_NOT_SUPPORTED_YET */
			Self::NoActiveTransaction => 1105,                                      /* ER_UNKNOWN_ERROR */
			Self::InFailedTransaction => 1105,                                      /* ER_UNKNOWN_ERROR */
			Self::SavepointDoesNotExist(_) => 1305, // ER_SP_DOES_NOT_EXIST
			Self::ActiveTransactionAlreadyInProgress => 1105, // ER_UNKNOWN_ERROR
			Self::UndefinedObject(_) => 1193,       // ER_UNKNOWN_SYSTEM_VARIABLE
			Self::Custom(_) => 1064,
		}
	}
}
