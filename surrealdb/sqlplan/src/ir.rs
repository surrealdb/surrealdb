//! Bound Intermediate Representation (Bound IR) with explicit SQL semantics.

/// A fully bound SQL `SELECT` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundSelect {
	/// Common Table Expressions (WITH clause).
	pub with: Option<BoundWithClause>,
	/// Relations in FROM clause.
	pub from: Option<BoundTableSource>,
	/// Bound WHERE predicate.
	pub where_clause: Option<BoundExpr>,
	/// Target projection list.
	pub targets: Vec<BoundTarget>,
	/// Expressions in GROUP BY.
	pub group_by: Vec<BoundExpr>,
	/// Bound HAVING condition.
	pub having: Option<BoundExpr>,
	/// Bound named WINDOW definitions.
	pub windows: Vec<BoundWindowDef>,
	/// Ordering expressions in ORDER BY.
	pub order_by: Vec<BoundOrder>,
	/// LIMIT count expression.
	pub limit: Option<BoundExpr>,
	/// OFFSET count expression.
	pub offset: Option<BoundExpr>,
	/// Optional DISTINCT modifier.
	pub distinct: Option<BoundDistinct>,
	/// Optional set operation (UNION, INTERSECT, EXCEPT).
	pub set_op: Option<BoundSetOperation>,
	/// Whether row locking (`FOR UPDATE` / `FOR SHARE`) was requested.
	pub for_update: bool,
	/// User-defined tables discovered from catalog (for system catalog introspection).
	pub user_tables: Vec<crate::catalog::TableSchema>,
}

/// A bound WITH clause containing Common Table Expressions.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundWithClause {
	pub recursive: bool,
	pub ctes: Vec<BoundCte>,
}

/// A bound Common Table Expression.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundCte {
	pub name: String,
	pub columns: Vec<String>,
	pub materialized: Option<bool>,
	pub query: Box<BoundSelect>,
}

/// A set operation kind in Bound IR (e.g. `UNION`, `INTERSECT`, `EXCEPT`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundSetOpKind {
	Union,
	Intersect,
	Except,
}

/// A set operation chaining two queries in Bound IR.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundSetOperation {
	pub op: BoundSetOpKind,
	pub all: bool,
	pub right: Box<BoundSelect>,
}

/// A target item in the SELECT projection list.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundTarget {
	/// Evaluated expression for the column.
	pub expr: BoundExpr,
	/// Explicit alias (`AS name`) or synthesized column name.
	pub alias: Option<String>,
	/// Whether this target originated from an unadorned `*`.
	pub is_wildcard: bool,
	/// Whether this target originated from a qualified wildcard `tbl.*`.
	pub table_wildcard: Option<String>,
}

/// A table reference or join tree in a FROM clause.
#[derive(Debug, Clone, PartialEq)]
pub enum BoundTableSource {
	/// Direct table source.
	Table {
		/// Schema namespace (e.g. `public` or schema name).
		schema: Option<String>,
		/// Table name.
		name: String,
		/// Assigned relation index in plan (0 for primary, 1..N for joins).
		relation_index: usize,
		/// Resolved table alias or table name.
		alias: String,
	},
	/// Subquery in FROM.
	Subquery {
		query: Box<BoundSelect>,
		relation_index: usize,
		alias: String,
	},
	/// Joined relations.
	Join {
		left: Box<BoundTableSource>,
		join_type: BoundJoinType,
		right: Box<BoundTableSource>,
		condition: Option<BoundJoinCondition>,
	},
	/// Query without a FROM clause (e.g. `SELECT 1 + 1`).
	Values,
}

/// Join types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundJoinType {
	Inner,
	Left,
	Right,
	Full,
	Cross,
}

/// Join condition.
#[derive(Debug, Clone, PartialEq)]
pub enum BoundJoinCondition {
	On(BoundExpr),
	Using(Vec<String>),
}

/// A bound column reference, resolved to a specific relation index and column name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BoundColumnRef {
	/// 0-based relation index in the join tree (0 = primary/leftmost table, 1..N = joined tables).
	pub relation_index: usize,
	/// Name of relation or alias that exposed this column.
	pub relation_name: String,
	/// Column name within the relation.
	pub column: String,
	/// Correlation depth: 0 for local relation, 1+ for outer scopes.
	pub correlation_depth: usize,
}

/// Bound literal value.
#[derive(Debug, Clone, PartialEq)]
pub enum BoundLiteral {
	Integer(i64),
	Float(f64),
	String(String),
	Boolean(bool),
	Null,
}

/// Binary operator in Bound IR.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundBinaryOp {
	Add,
	Subtract,
	Multiply,
	Divide,
	Remainder,
	Equal,
	NotEqual,
	LessThan,
	LessThanEqual,
	GreaterThan,
	GreaterThanEqual,
	And,
	Or,
	Concat,
	/// Null-safe equal (`<=>` in MySQL, `IS NOT DISTINCT FROM` in PostgreSQL).
	NullSafeEqual,
	/// Null-safe not equal (`IS DISTINCT FROM` in PostgreSQL).
	DistinctFrom,
	/// SQL pattern matching (`LIKE`).
	Like,
	/// Case-insensitive SQL pattern matching (`ILIKE`).
	ILike,
	/// Regular expression match (`REGEXP` / `~`).
	Regexp,
	/// Integer division (`DIV` in MySQL).
	IntDiv,
	/// JSON object field or array element extraction (`->`).
	JsonExtract,
	/// JSON object field or array element extraction as text (`->>`).
	JsonExtractText,
	/// JSON path extraction as text (`#>>`).
	JsonPathExtractText,
	/// JSON containment (`@>`).
	JsonContains,
}

/// Unary operator in Bound IR.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundUnaryOp {
	Not,
	Positive,
	Negate,
}

/// A bound scalar expression.
#[derive(Debug, Clone, PartialEq)]
pub enum BoundExpr {
	/// Literal value.
	Literal(BoundLiteral),

	/// Resolved column reference with known relation index.
	Column(BoundColumnRef),

	/// Dynamic field access on record or nested JSON object (e.g. `doc.field`).
	Access {
		base: Box<BoundExpr>,
		field: String,
	},

	/// Positional parameter (e.g. `$1`, `$2` in PG or `?` in MySQL).
	Param {
		index: usize,
		name: String,
	},

	/// Binary operation.
	Binary {
		left: Box<BoundExpr>,
		op: BoundBinaryOp,
		right: Box<BoundExpr>,
	},

	/// Unary operation.
	Unary {
		op: BoundUnaryOp,
		expr: Box<BoundExpr>,
	},

	/// Function call (aggregate or scalar).
	FunctionCall {
		name: String,
		args: Vec<BoundExpr>,
		distinct: bool,
	},

	/// CASE expression.
	Case {
		operand: Option<Box<BoundExpr>>,
		branches: Vec<(BoundExpr, BoundExpr)>,
		else_expr: Option<Box<BoundExpr>>,
	},

	/// BETWEEN expression.
	Between {
		expr: Box<BoundExpr>,
		low: Box<BoundExpr>,
		high: Box<BoundExpr>,
		negated: bool,
	},

	/// IN (list) expression.
	InList {
		expr: Box<BoundExpr>,
		list: Vec<BoundExpr>,
		negated: bool,
	},

	/// IS NULL / IS NOT NULL test.
	IsNull {
		expr: Box<BoundExpr>,
		negated: bool,
	},

	/// Type cast.
	Cast {
		expr: Box<BoundExpr>,
		target_type: String,
	},

	/// Scalar subquery `(SELECT ...)`.
	ScalarSubquery(Box<BoundSelect>),

	/// `EXISTS (SELECT ...)` predicate.
	Exists(Box<BoundSelect>),

	/// `expr [NOT] IN (SELECT ...)` membership test.
	InSubquery {
		expr: Box<BoundExpr>,
		subquery: Box<BoundSelect>,
		negated: bool,
	},

	/// `ARRAY(SELECT ...)` array subquery.
	ArraySubquery(Box<BoundSelect>),

	/// Window function evaluation: `func(args) OVER (window_spec)`.
	WindowFunction {
		name: String,
		args: Vec<BoundExpr>,
		distinct: bool,
		spec: BoundWindowSpec,
	},

	/// User-defined variable (e.g. `@var`).
	UserVar(String),

	/// System variable (e.g. `@@var` or `@@global.var`).
	SystemVar {
		name: String,
		is_global: bool,
	},
}

/// Units for a window frame specification (`ROWS`, `RANGE`, `GROUPS`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundWindowFrameUnits {
	Rows,
	Range,
	Groups,
}

/// A bound in a window frame specification.
#[derive(Debug, Clone, PartialEq)]
pub enum BoundWindowFrameBound {
	CurrentRow,
	UnboundedPreceding,
	UnboundedFollowing,
	Preceding(Box<BoundExpr>),
	Following(Box<BoundExpr>),
}

/// Exclusion clause in a window frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundWindowFrameExclusion {
	CurrentRow,
	Group,
	Ties,
	NoOthers,
}

/// Specification of a window frame (`ROWS BETWEEN ... AND ...`).
#[derive(Debug, Clone, PartialEq)]
pub struct BoundWindowFrame {
	pub units: BoundWindowFrameUnits,
	pub start: BoundWindowFrameBound,
	pub end: Option<BoundWindowFrameBound>,
	pub exclusion: Option<BoundWindowFrameExclusion>,
}

/// Bound specification of a window in an `OVER` clause or `WINDOW` definition.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundWindowSpec {
	pub window_name: Option<String>,
	pub partition_by: Vec<BoundExpr>,
	pub order_by: Vec<BoundOrder>,
	pub frame: Option<BoundWindowFrame>,
}

/// Named window definition in a `SELECT` statement (`WINDOW name AS (spec)`).
#[derive(Debug, Clone, PartialEq)]
pub struct BoundWindowDef {
	pub name: String,
	pub spec: BoundWindowSpec,
}

/// Direction in ORDER BY.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundOrderDirection {
	Asc,
	Desc,
}

/// An ORDER BY item with explicit direction and NULL ordering.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundOrder {
	pub expr: BoundExpr,
	pub direction: BoundOrderDirection,
	pub nulls_first: bool,
}

/// DISTINCT modifier.
#[derive(Debug, Clone, PartialEq)]
pub enum BoundDistinct {
	All,
	On(Vec<BoundExpr>),
}

impl BoundExpr {
	/// Check if this expression contains any parameter placeholders.
	pub fn has_params(&self) -> bool {
		match self {
			BoundExpr::Param {
				..
			} => true,
			BoundExpr::Binary {
				left,
				right,
				..
			} => left.has_params() || right.has_params(),
			BoundExpr::Unary {
				expr,
				..
			} => expr.has_params(),
			BoundExpr::Access {
				base,
				..
			} => base.has_params(),
			BoundExpr::FunctionCall {
				args,
				..
			} => args.iter().any(|a| a.has_params()),
			BoundExpr::Case {
				operand,
				branches,
				else_expr,
			} => {
				operand.as_ref().is_some_and(|o| o.has_params())
					|| branches.iter().any(|(w, t)| w.has_params() || t.has_params())
					|| else_expr.as_ref().is_some_and(|e| e.has_params())
			}
			BoundExpr::Between {
				expr,
				low,
				high,
				..
			} => expr.has_params() || low.has_params() || high.has_params(),
			BoundExpr::InList {
				expr,
				list,
				..
			} => expr.has_params() || list.iter().any(|e| e.has_params()),
			BoundExpr::IsNull {
				expr,
				..
			} => expr.has_params(),
			BoundExpr::Cast {
				expr,
				..
			} => expr.has_params(),
			BoundExpr::WindowFunction {
				args,
				spec,
				..
			} => {
				args.iter().any(|a| a.has_params())
					|| spec.partition_by.iter().any(|p| p.has_params())
					|| spec.order_by.iter().any(|o| o.expr.has_params())
					|| spec.frame.as_ref().is_some_and(|f| {
						let start_has = match &f.start {
							BoundWindowFrameBound::Preceding(e)
							| BoundWindowFrameBound::Following(e) => e.has_params(),
							_ => false,
						};
						let end_has = match &f.end {
							Some(
								BoundWindowFrameBound::Preceding(e)
								| BoundWindowFrameBound::Following(e),
							) => e.has_params(),
							_ => false,
						};
						start_has || end_has
					})
			}
			_ => false,
		}
	}

	/// Recursively check if this expression contains a window function.
	pub fn contains_window(&self) -> bool {
		match self {
			BoundExpr::WindowFunction {
				..
			} => true,
			BoundExpr::Binary {
				left,
				right,
				..
			} => left.contains_window() || right.contains_window(),
			BoundExpr::Unary {
				expr,
				..
			} => expr.contains_window(),
			BoundExpr::Access {
				base,
				..
			} => base.contains_window(),
			BoundExpr::FunctionCall {
				args,
				..
			} => args.iter().any(|a| a.contains_window()),
			BoundExpr::Between {
				expr,
				low,
				high,
				..
			} => expr.contains_window() || low.contains_window() || high.contains_window(),
			BoundExpr::InList {
				expr,
				list,
				..
			} => expr.contains_window() || list.iter().any(|e| e.contains_window()),
			BoundExpr::IsNull {
				expr,
				..
			} => expr.contains_window(),
			BoundExpr::Cast {
				expr,
				..
			} => expr.contains_window(),
			BoundExpr::Case {
				operand,
				branches,
				else_expr,
				..
			} => {
				operand.as_ref().is_some_and(|op| op.contains_window())
					|| branches.iter().any(|(w, t)| w.contains_window() || t.contains_window())
					|| else_expr.as_ref().is_some_and(|el| el.contains_window())
			}
			_ => false,
		}
	}
}

impl BoundSelect {
	/// Check if this SELECT statement contains any parameter placeholders.
	pub fn has_params(&self) -> bool {
		self.targets.iter().any(|t| t.expr.has_params())
			|| self.where_clause.as_ref().is_some_and(|w| w.has_params())
			|| self.having.as_ref().is_some_and(|h| h.has_params())
			|| self.windows.iter().any(|w| {
				w.spec.partition_by.iter().any(|p| p.has_params())
					|| w.spec.order_by.iter().any(|o| o.expr.has_params())
			}) || self.set_op.as_ref().is_some_and(|s| s.right.has_params())
	}
}
