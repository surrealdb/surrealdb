use surrealdb_types::{SurrealValue, Value};

use crate::sql::{BuildSql, BuildSqlContext};

/// Type alias for condition builder closures to help with type inference
pub type ConditionClosure = dyn FnOnce(ConditionBuilder) -> ConditionBuilder;

#[derive(Clone, Debug, Default)]
pub struct Condition(pub Vec<ConditionBuilder>);

impl Condition {
	pub fn is_empty(&self) -> bool {
		self.0.iter().all(|cond| cond.is_empty())
	}
}

impl BuildSql for Condition {
	fn build(self, ctx: &mut BuildSqlContext) {
		let mut iter = self.0.into_iter().filter(|cond| !cond.is_empty()).enumerate();

		while let Some((index, condition)) = iter.next() {
			if index > 0 {
				ctx.push(" AND ");
			}

			ctx.push("(");
			ctx.push(condition);
			ctx.push(")");
		}
	}
}

pub trait IntoCondition {
	fn build(self, cond: &mut Condition);
}

impl<F> IntoCondition for F
where
	F: FnOnce(ConditionBuilder) -> ConditionBuilder,
{
	fn build(self, cond: &mut Condition) {
		let builder = ConditionBuilder::new();
		let builder = self(builder);
		cond.0.push(builder);
	}
}

impl IntoCondition for String {
	fn build(self, cond: &mut Condition) {
		cond.0.push(ConditionBuilder::new().raw(self));
	}
}

impl IntoCondition for &str {
	fn build(self, cond: &mut Condition) {
		cond.0.push(ConditionBuilder::new().raw(self));
	}
}

impl IntoCondition for &String {
	fn build(self, cond: &mut Condition) {
		cond.0.push(ConditionBuilder::new().raw(self.clone()));
	}
}

/// Builder for constructing WHERE clause conditions
#[derive(Clone, Debug)]
pub struct ConditionBuilder {
	conditions: Vec<ConditionInner>,
}

/// Represents a single condition in the WHERE clause
#[derive(Clone, Debug)]
enum ConditionInner {
	Comparison {
		field: String,
		op: ComparisonOp,
		value: Value,
	},
	Logical {
		op: LogicalOp,
	},
	Raw {
		sql: String,
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
		self.conditions.push(ConditionInner::Logical {
			op: LogicalOp::And,
		});
		self
	}

	/// Add an OR logical operator
	pub fn or(mut self) -> Self {
		self.conditions.push(ConditionInner::Logical {
			op: LogicalOp::Or,
		});
		self
	}

	/// Add a raw SQL condition
	///
	/// # Example
	/// ```ignore
	/// db.select("user")
	///     .where(|w| {
	///         w.field("age").gt(18)
	///          .and()
	///          .raw("name != 'admin'")
	///     })
	///     .collect().await?;
	/// ```
	pub fn raw(mut self, sql: impl Into<String>) -> Self {
		self.conditions.push(ConditionInner::Raw {
			sql: sql.into(),
		});
		self
	}

	/// Add a comparison condition
	fn add_comparison(
		mut self,
		field: String,
		op: ComparisonOp,
		value: Value,
	) -> Self {
		self.conditions.push(ConditionInner::Comparison { field, op, value });
		self
	}

	pub fn is_empty(&self) -> bool {
		self.conditions.is_empty()
	}
}

impl BuildSql for ConditionBuilder {
	fn build(self, ctx: &mut BuildSqlContext) {
		let mut iter = self.conditions.into_iter().peekable();

		while let Some(condition) = iter.next() {
			match condition {
				ConditionInner::Comparison { field, op, value } => {
					ctx.push(field);
					ctx.push(op);
					let var = ctx.var(value);
					ctx.push(var);
				}
				ConditionInner::Logical { op } => {
					ctx.push(op);
				}
				ConditionInner::Raw { sql: raw_sql } => {
					ctx.push(raw_sql);
				}
			}
		}
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
	pub fn eq<V: SurrealValue>(self, value: V) -> ConditionBuilder {
		self.builder
			.add_comparison(self.field, ComparisonOp::Equal, value.into_value())
	}

	/// Not equal comparison
	pub fn ne<V: SurrealValue>(self, value: V) -> ConditionBuilder {
		self.builder
			.add_comparison(self.field, ComparisonOp::NotEqual, value.into_value())
	}

	/// Greater than comparison
	pub fn gt<V: SurrealValue>(self, value: V) -> ConditionBuilder {
		self.builder
			.add_comparison(self.field, ComparisonOp::GreaterThan, value.into_value())
	}

	/// Greater than or equal comparison
	pub fn gte<V: SurrealValue>(self, value: V) -> ConditionBuilder {
		self.builder
			.add_comparison(self.field, ComparisonOp::GreaterThanEqual, value.into_value())
	}

	/// Less than comparison
	pub fn lt<V: SurrealValue>(self, value: V) -> ConditionBuilder {
		self.builder
			.add_comparison(self.field, ComparisonOp::LessThan, value.into_value())
	}

	/// Less than or equal comparison
	pub fn lte<V: SurrealValue>(self, value: V) -> ConditionBuilder {
		self.builder
			.add_comparison(self.field, ComparisonOp::LessThanEqual, value.into_value())
	}

	/// Contains comparison
	pub fn contains<V: SurrealValue>(self, value: V) -> ConditionBuilder {
		self.builder
			.add_comparison(self.field, ComparisonOp::Contains, value.into_value())
	}

	/// Not contains comparison
	pub fn not_contains<V: SurrealValue>(self, value: V) -> ConditionBuilder {
		self.builder
			.add_comparison(self.field, ComparisonOp::NotContains, value.into_value())
	}

	/// Inside comparison
	pub fn inside<V: SurrealValue>(self, value: V) -> ConditionBuilder {
		self.builder
			.add_comparison(self.field, ComparisonOp::Inside, value.into_value())
	}

	/// Not inside comparison
	pub fn not_inside<V: SurrealValue>(self, value: V) -> ConditionBuilder {
		self.builder
			.add_comparison(self.field, ComparisonOp::NotInside, value.into_value())
	}
}

impl BuildSql for ComparisonOp {
	fn build(self, ctx: &mut BuildSqlContext) {
		let op = match self {
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
		};
		ctx.push(format!(" {op} "));
	}
}

impl BuildSql for LogicalOp {
	fn build(self, ctx: &mut BuildSqlContext) {
		let op = match self {
			LogicalOp::And => "AND",
			LogicalOp::Or => "OR",
		};

		ctx.push(format!(" {op} "));
	}
}