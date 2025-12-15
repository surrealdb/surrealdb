use surrealdb_types::{ToSql, Value};

/// Builder for constructing WHERE clause conditions
#[derive(Clone, Debug)]
pub struct ConditionBuilder {
	conditions: Vec<Condition>,
}

/// Represents a single condition in the WHERE clause
#[derive(Clone, Debug)]
enum Condition {
	Comparison {
		field: String,
		op: ComparisonOp,
		value: ConditionValue,
	},
	Logical {
		op: LogicalOp,
	},
}

/// Comparison operators
#[derive(Clone, Debug)]
enum ComparisonOp {
	Equal,
	NotEqual,
	GreaterThan,
	GreaterThanEqual,
	LessThan,
	LessThanEqual,
	Contains,
	NotContains,
	Inside,
	NotInside,
}

/// Logical operators
#[derive(Clone, Debug)]
enum LogicalOp {
	And,
	Or,
}

/// Value types for conditions
#[derive(Clone, Debug)]
pub(crate) enum ConditionValue {
	Literal(Value),
	Parameter(String),
}

impl ConditionBuilder {
	/// Create a new condition builder
	pub fn new() -> Self {
		Self {
			conditions: Vec::new(),
		}
	}

	/// Start a field comparison
	pub fn field(self, field: impl Into<String>) -> FieldBuilder {
		FieldBuilder {
			builder: self,
			field: field.into(),
		}
	}

	/// Add an AND logical operator
	pub fn and(mut self) -> Self {
		self.conditions.push(Condition::Logical {
			op: LogicalOp::And,
		});
		self
	}

	/// Add an OR logical operator
	pub fn or(mut self) -> Self {
		self.conditions.push(Condition::Logical {
			op: LogicalOp::Or,
		});
		self
	}

	/// Add a comparison condition
	fn add_comparison(
		mut self,
		field: String,
		op: ComparisonOp,
		value: ConditionValue,
	) -> Self {
		self.conditions.push(Condition::Comparison { field, op, value });
		self
	}

	/// Convert the condition builder to a SQL WHERE clause string
	pub fn to_sql(&self) -> String {
		if self.conditions.is_empty() {
			return String::new();
		}

		let mut sql = String::new();
		let mut iter = self.conditions.iter().peekable();

		while let Some(condition) = iter.next() {
			match condition {
				Condition::Comparison { field, op, value } => {
					sql.push_str(field);
					sql.push(' ');
					sql.push_str(&op.to_sql());
					sql.push(' ');
					sql.push_str(&value.to_sql());
				}
				Condition::Logical { op } => {
					sql.push(' ');
					sql.push_str(&op.to_sql());
					sql.push(' ');
				}
			}
		}

		sql.trim().to_string()
	}
}

impl Default for ConditionBuilder {
	fn default() -> Self {
		Self::new()
	}
}

/// Builder for field comparisons
#[derive(Clone, Debug)]
pub struct FieldBuilder {
	builder: ConditionBuilder,
	field: String,
}

impl FieldBuilder {
	/// Equal comparison
	pub fn eq<V: Into<ConditionValue>>(self, value: V) -> ConditionBuilder {
		self.builder
			.add_comparison(self.field, ComparisonOp::Equal, value.into())
	}

	/// Not equal comparison
	pub fn ne<V: Into<ConditionValue>>(self, value: V) -> ConditionBuilder {
		self.builder
			.add_comparison(self.field, ComparisonOp::NotEqual, value.into())
	}

	/// Greater than comparison
	pub fn gt<V: Into<ConditionValue>>(self, value: V) -> ConditionBuilder {
		self.builder
			.add_comparison(self.field, ComparisonOp::GreaterThan, value.into())
	}

	/// Greater than or equal comparison
	pub fn gte<V: Into<ConditionValue>>(self, value: V) -> ConditionBuilder {
		self.builder
			.add_comparison(self.field, ComparisonOp::GreaterThanEqual, value.into())
	}

	/// Less than comparison
	pub fn lt<V: Into<ConditionValue>>(self, value: V) -> ConditionBuilder {
		self.builder
			.add_comparison(self.field, ComparisonOp::LessThan, value.into())
	}

	/// Less than or equal comparison
	pub fn lte<V: Into<ConditionValue>>(self, value: V) -> ConditionBuilder {
		self.builder
			.add_comparison(self.field, ComparisonOp::LessThanEqual, value.into())
	}

	/// Contains comparison
	pub fn contains<V: Into<ConditionValue>>(self, value: V) -> ConditionBuilder {
		self.builder
			.add_comparison(self.field, ComparisonOp::Contains, value.into())
	}

	/// Not contains comparison
	pub fn not_contains<V: Into<ConditionValue>>(self, value: V) -> ConditionBuilder {
		self.builder
			.add_comparison(self.field, ComparisonOp::NotContains, value.into())
	}

	/// Inside comparison
	pub fn inside<V: Into<ConditionValue>>(self, value: V) -> ConditionBuilder {
		self.builder
			.add_comparison(self.field, ComparisonOp::Inside, value.into())
	}

	/// Not inside comparison
	pub fn not_inside<V: Into<ConditionValue>>(self, value: V) -> ConditionBuilder {
		self.builder
			.add_comparison(self.field, ComparisonOp::NotInside, value.into())
	}
}

impl ComparisonOp {
	fn to_sql(&self) -> &'static str {
		match self {
			ComparisonOp::Equal => "=",
			ComparisonOp::NotEqual => "!=",
			ComparisonOp::GreaterThan => ">",
			ComparisonOp::GreaterThanEqual => ">=",
			ComparisonOp::LessThan => "<",
			ComparisonOp::LessThanEqual => "<=",
			ComparisonOp::Contains => "CONTAINS",
			ComparisonOp::NotContains => "CONTAINSNOT",
			ComparisonOp::Inside => "INSIDE",
			ComparisonOp::NotInside => "NOTINSIDE",
		}
	}
}

impl LogicalOp {
	fn to_sql(&self) -> &'static str {
		match self {
			LogicalOp::And => "AND",
			LogicalOp::Or => "OR",
		}
	}
}

impl ConditionValue {
	fn to_sql(&self) -> String {
		match self {
			ConditionValue::Literal(v) => {
				// Use ToSql trait to format the value
				v.to_sql()
			}
			ConditionValue::Parameter(p) => {
				// Parameter placeholder (already has $ prefix)
				if p.starts_with('$') {
					p.clone()
				} else {
					format!("${}", p)
				}
			}
		}
	}
}

// Implement Into<ConditionValue> for common types
impl From<Value> for ConditionValue {
	fn from(value: Value) -> Self {
		ConditionValue::Literal(value)
	}
}

impl From<&str> for ConditionValue {
	fn from(value: &str) -> Self {
		if value.starts_with('$') {
			ConditionValue::Parameter(value.to_string())
		} else {
			ConditionValue::Literal(Value::String(value.to_string()))
		}
	}
}

impl From<String> for ConditionValue {
	fn from(value: String) -> Self {
		if value.starts_with('$') {
			ConditionValue::Parameter(value)
		} else {
			ConditionValue::Literal(Value::String(value))
		}
	}
}

impl From<i64> for ConditionValue {
	fn from(value: i64) -> Self {
		ConditionValue::Literal(Value::Number(value.into()))
	}
}

impl From<u64> for ConditionValue {
	fn from(value: u64) -> Self {
		ConditionValue::Literal(Value::Number((value as i64).into()))
	}
}

impl From<i32> for ConditionValue {
	fn from(value: i32) -> Self {
		ConditionValue::Literal(Value::Number(value.into()))
	}
}

impl From<u32> for ConditionValue {
	fn from(value: u32) -> Self {
		ConditionValue::Literal(Value::Number((value as i64).into()))
	}
}

impl From<f64> for ConditionValue {
	fn from(value: f64) -> Self {
		ConditionValue::Literal(Value::Number(value.into()))
	}
}

impl From<bool> for ConditionValue {
	fn from(value: bool) -> Self {
		ConditionValue::Literal(Value::Bool(value))
	}
}

