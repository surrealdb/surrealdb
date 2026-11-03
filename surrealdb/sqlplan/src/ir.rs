//! Bound Intermediate Representation (Bound IR) with explicit SQL semantics.

/// A fully bound SQL `SELECT` statement.
#[derive(Debug, Clone, Default, PartialEq)]
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
	/// User-defined types discovered from the catalog, for `pg_type`, `pg_enum` and the other
	/// catalog views that list them.
	pub user_types: crate::catalog::UserTypes,
	/// The database the statement runs in, which the catalog views name as their catalog.
	pub database: Option<String>,
	/// The HNSW indexes over the table the select reads, when it reads one table.
	pub hnsw_indexes: Vec<BoundHnswIndex>,
	/// The columns of the query directly above this one that the queries nested inside this one
	/// read ([`BoundColumnRef::carried_as`]); those queries stand two or more levels below the
	/// query that holds the columns. The select evaluates each once, when it is entered, and
	/// binds the value to the parameter that names it.
	pub carried: Vec<CarriedColumn>,
}

/// A column of a query that the queries nested two or more levels below it read, bound to a
/// parameter by the query one level below it.
#[derive(Debug, Clone, PartialEq)]
pub struct CarriedColumn {
	/// The name of the parameter.
	pub name: String,
	/// The column as the query that binds the parameter reads it: one level up
	/// (`correlation_depth` 1), as the row of the query above is that query's `$parent`.
	pub column: BoundColumnRef,
}

/// An HNSW index over a column of a table, and the distance it ranks by.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundHnswIndex {
	/// The indexed column.
	pub column: String,
	/// The metric the index ranks by.
	pub metric: crate::vector::VectorMetric,
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

impl BoundTarget {
	/// Whether the target is a `*` or a `t.*` over a relation whose columns the binder could not
	/// list, so that the columns it stands for are known only from the rows that come back. A
	/// column that is merely named `*` (written quoted) is an ordinary column.
	pub fn is_unlisted_wildcard(&self) -> bool {
		(self.is_wildcard || self.table_wildcard.is_some())
			&& matches!(&self.expr, BoundExpr::Column(col) if col.column == "*")
	}
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
	/// A `LATERAL` subquery of expressions: one row of the relation for each combination of
	/// the set-returning calls in `targets`, evaluated over each row of the relations to its left.
	/// Each target is a column of the relation, named by its `alias`.
	Lateral {
		targets: Vec<BoundTarget>,
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

/// How text compared with a record key is read.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum KeyRead {
	/// As it is.
	#[default]
	None,
	/// As the integer it spells.
	Int,
	/// As a uuid.
	Uuid,
}

impl KeyRead {
	/// How text is read as a key of type `ty`.
	pub fn of(ty: crate::SqlType) -> Self {
		if matches!(ty, crate::SqlType::Int2 | crate::SqlType::Int4 | crate::SqlType::Int8) {
			Self::Int
		} else if ty == crate::SqlType::Uuid {
			Self::Uuid
		} else {
			Self::None
		}
	}
}

/// A bound column reference, resolved to a specific relation index and column name.
///
/// Equality is that of every field: two references are equal when they were bound to the same
/// relation and describe the same column. A reference whose relation the schema left open
/// ([`Self::also_in`]) is therefore not equal to the reference to any relation it may hold.
/// [`Self::is_compatible_with`] asks the looser question, whether two references may read the same
/// column.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BoundColumnRef {
	/// 0-based relation index in the join tree (0 = primary/leftmost table, 1..N = joined tables).
	pub relation_index: usize,
	/// Name of relation or alias that exposed this column.
	pub relation_name: String,
	/// Column name within the relation.
	pub column: String,
	/// Declared SQL type, retained through direct projections, aliases and outer scopes.
	pub declared_type: crate::SqlType,
	/// Correlation depth: 0 for local relation, 1+ for outer scopes.
	pub correlation_depth: usize,
	/// The base table whose record key this reference reads.
	///
	/// Set only for the `id` column of a base-table relation, where the stored `id` field is the
	/// engine's full record id and the SQL-visible value is its key. It is `None` for every other
	/// column, and for the `id` of a derived table, subquery alias, CTE or `VALUES` list, whose
	/// `id` is already a plain value.
	pub id_table: Option<String>,
	/// How text compared with this column's key is read: the type of the `id` of the table this
	/// column is the `id` of, or links to, when the catalog says text is read as that type
	/// ([`crate::catalog::BinderCatalog::write_type`]). [`KeyRead::None`] for every other column.
	pub key_read: KeyRead,
	/// The table whose record links this column holds.
	///
	/// Set for a base-table field declared as a link to one table (`record<t>`) and retained
	/// through direct projections and unambiguous wildcards of derived relations and CTEs. The
	/// stored value is the link; comparisons with a key name the record of this table. Base-table
	/// `id`, system catalog columns and computed expressions do not carry this provenance.
	pub link_table: Option<String>,
	/// The enum whose labels this column holds.
	///
	/// Set for a base-table field declared as an enum, or a set or an array of one, and retained
	/// through direct projections and unambiguous wildcards of derived relations and CTEs as
	/// `link_table` is. A comparison, an ordering or a `MIN` and `MAX` over a column that holds
	/// one label follows the enum's declaration order rather than the labels' spelling, and the
	/// result descriptor names the enum for the wire.
	pub enum_column: Option<Box<crate::catalog::EnumColumn>>,
	/// The user-defined composite type the column is declared with, by name, when it is one. The
	/// result descriptor names the type for the wire, which types the column by its fields.
	pub composite: Option<String>,
	/// The SQL type a base-table column is declared with (`SQL TYPE`), which the result
	/// descriptor reports for the wire in place of the type `declared_type` names.
	pub declared_sql: Option<Box<crate::declared_type::DeclaredSqlType>>,
	/// Whether the reference reads an output column of an aggregation, from a clause that is
	/// evaluated over the rows the aggregation produced (`HAVING`, an `ORDER BY` expression).
	///
	/// An `id` column read this way holds the stored record id of whichever relation was
	/// grouped, which is absent for the group of the null-extended side of an outer join, so the
	/// key of its record id is read through a guard.
	pub aggregate_output: bool,
	/// Further relations of the same FROM clause that may hold this column, when the schema does
	/// not say which relation does.
	///
	/// Only an unqualified column of a join has any: the relations that do not list their columns
	/// (schemaless tables) and are not ruled out by the schema of another. The column is then the
	/// value of the first of `relation_index` and these that holds one, and `declared_type` and
	/// `link_table` are those of `relation_index`. A qualified column, the column of a single
	/// relation and the `id` column have none, and a column of an outer query
	/// (`correlation_depth` above 0) is read from the outer row by name, whatever this holds.
	pub also_in: Vec<usize>,
	/// The parameter that holds the value of this column, when it belongs to a query two or more
	/// levels above the reference (`correlation_depth` above 1).
	///
	/// The engine reaches the row of the query directly above a subquery (`$parent`) and no row
	/// further up. The query one level below the column's query reads the value instead
	/// ([`BoundSelect::carried`]) and binds it to this parameter, which every query nested in it
	/// reads by name. `None` for a column of the query itself or of the query directly above it.
	pub carried_as: Option<String>,
}

impl BoundColumnRef {
	/// The relations that may hold this column, the one it is bound to first.
	fn relations(&self) -> impl Iterator<Item = usize> + '_ {
		std::iter::once(self.relation_index).chain(self.also_in.iter().copied())
	}

	/// Whether the two references may read the same column: they describe the same column, and
	/// either are bound to the same relation or have a relation in common. A reference whose
	/// relation the schema left open reads the same column as one of any relation it may hold,
	/// so the `v` of a statement is its `t.v` when `t` may hold `v`.
	///
	/// The relation is reflexive and symmetric but not transitive: `t.v` and `u.v` are both
	/// compatible with `v` and not with each other. It answers a question about a pair, so a
	/// caller takes the first reference that matches and never groups references by it; equality
	/// is [`PartialEq`].
	pub fn is_compatible_with(&self, other: &Self) -> bool {
		let same_relation = if self.also_in.is_empty() && other.also_in.is_empty() {
			self.relation_index == other.relation_index && self.relation_name == other.relation_name
		} else {
			self.relations().any(|relation| other.relations().any(|held| held == relation))
		};
		same_relation
			&& self.column == other.column
			&& self.declared_type == other.declared_type
			&& self.correlation_depth == other.correlation_depth
			&& self.carried_as == other.carried_as
			&& self.id_table == other.id_table
			&& self.key_read == other.key_read
			&& self.link_table == other.link_table
			&& self.enum_column == other.enum_column
			&& self.composite == other.composite
			&& self.aggregate_output == other.aggregate_output
	}
}

impl BoundExpr {
	/// Whether the two expressions may read the same value: they are the same expression but for
	/// their columns, which are compared with [`BoundColumnRef::is_compatible_with`]. A subquery
	/// and the window specification of a window function are compared as bound, since their
	/// columns are resolved against their own scope.
	///
	/// Like the relation between columns it is no equivalence, so it is applied to a pair and
	/// never used to group expressions.
	pub fn is_compatible_with(&self, other: &Self) -> bool {
		fn all(left: &[BoundExpr], right: &[BoundExpr]) -> bool {
			left.len() == right.len()
				&& left.iter().zip(right).all(|(left, right)| left.is_compatible_with(right))
		}
		fn optional(left: Option<&BoundExpr>, right: Option<&BoundExpr>) -> bool {
			match (left, right) {
				(Some(left), Some(right)) => left.is_compatible_with(right),
				(None, None) => true,
				(Some(_), None) | (None, Some(_)) => false,
			}
		}
		match self {
			BoundExpr::Column(col) => {
				matches!(other, BoundExpr::Column(o) if col.is_compatible_with(o))
			}
			BoundExpr::Access {
				base,
				field,
			} => matches!(other, BoundExpr::Access { base: b, field: f }
				if field == f && base.is_compatible_with(b)),
			BoundExpr::Binary {
				left,
				op,
				right,
			} => matches!(other, BoundExpr::Binary { left: l, op: o, right: r }
				if op == o && left.is_compatible_with(l) && right.is_compatible_with(r)),
			BoundExpr::Unary {
				op,
				expr,
			} => matches!(other, BoundExpr::Unary { op: o, expr: e }
				if op == o && expr.is_compatible_with(e)),
			BoundExpr::FunctionCall {
				name,
				args,
				distinct,
			} => matches!(other, BoundExpr::FunctionCall { name: n, args: a, distinct: d }
				if name == n && distinct == d && all(args, a)),
			BoundExpr::Case {
				operand,
				branches,
				else_expr,
			} => matches!(other, BoundExpr::Case { operand: o, branches: b, else_expr: e }
				if optional(operand.as_deref(), o.as_deref())
					&& branches.len() == b.len()
					&& branches.iter().zip(b).all(|(l, r)| {
						l.0.is_compatible_with(&r.0) && l.1.is_compatible_with(&r.1)
					})
					&& optional(else_expr.as_deref(), e.as_deref())),
			BoundExpr::Between {
				expr,
				low,
				high,
				negated,
			} => matches!(other, BoundExpr::Between { expr: e, low: l, high: h, negated: n }
				if negated == n
					&& expr.is_compatible_with(e)
					&& low.is_compatible_with(l)
					&& high.is_compatible_with(h)),
			BoundExpr::InList {
				expr,
				list,
				negated,
			} => matches!(other, BoundExpr::InList { expr: e, list: l, negated: n }
				if negated == n && expr.is_compatible_with(e) && all(list, l)),
			BoundExpr::IsNull {
				expr,
				negated,
			} => matches!(other, BoundExpr::IsNull { expr: e, negated: n }
				if negated == n && expr.is_compatible_with(e)),
			BoundExpr::Cast {
				expr,
				target_type,
			} => matches!(other, BoundExpr::Cast { expr: e, target_type: t }
				if target_type == t && expr.is_compatible_with(e)),
			BoundExpr::InSubquery {
				expr,
				subquery,
				negated,
			} => matches!(other, BoundExpr::InSubquery { expr: e, subquery: s, negated: n }
				if negated == n && subquery == s && expr.is_compatible_with(e)),
			BoundExpr::WindowFunction {
				name,
				args,
				distinct,
				spec,
			} => {
				matches!(other, BoundExpr::WindowFunction { name: n, args: a, distinct: d, spec: s }
				if name == n && distinct == d && spec == s && all(args, a))
			}
			BoundExpr::Literal(_)
			| BoundExpr::Param {
				..
			}
			| BoundExpr::ScalarSubquery(_)
			| BoundExpr::Exists(_)
			| BoundExpr::ArraySubquery(_)
			| BoundExpr::UserVar(_)
			| BoundExpr::SystemVar {
				..
			} => self == other,
		}
	}
}

/// Bound literal value.
#[derive(Debug, Clone, PartialEq)]
pub enum BoundLiteral {
	Integer(i64),
	/// An integer above `i64::MAX` that fits 64 unsigned bits.
	Unsigned(u64),
	Float(f64),
	/// An exact number as written in the source, without digit separators: PostgreSQL's numeric
	/// constant (`1.5`, `1e3`) and MySQL's exact-value literal (`1.5`). It is read as `numeric`,
	/// held as a decimal when it fits one and as a bigdecimal otherwise.
	Numeric(String),
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
	/// Regular expression match (`~`).
	Regexp,
	/// Regular expression match that ignores case (`REGEXP` in MySQL, `~*` in PostgreSQL).
	RegexpCi,
	/// Exponentiation (`^` in PostgreSQL).
	Power,
	/// Integer division (`DIV` in MySQL).
	IntDiv,
	/// JSON object field or array element extraction (`->`).
	JsonExtract,
	/// JSON object field or array element extraction as text (`->>`).
	JsonExtractText,
	/// JSON path extraction (`#>`).
	JsonPathExtract,
	/// JSON path extraction as text (`#>>`).
	JsonPathExtractText,
	/// JSON containment (`@>`).
	JsonContains,
	/// A distance between vectors (`<->`, `<=>`, `<#>`, `<+>` in PostgreSQL).
	VectorDistance(crate::vector::VectorMetric),
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
							BoundWindowFrameBound::CurrentRow
							| BoundWindowFrameBound::UnboundedPreceding
							| BoundWindowFrameBound::UnboundedFollowing => false,
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
			BoundExpr::Literal(_)
			| BoundExpr::Column(_)
			| BoundExpr::ScalarSubquery(_)
			| BoundExpr::Exists(_)
			| BoundExpr::InSubquery {
				..
			}
			| BoundExpr::ArraySubquery(_)
			| BoundExpr::UserVar(_)
			| BoundExpr::SystemVar {
				..
			} => false,
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
			BoundExpr::Literal(_)
			| BoundExpr::Column(_)
			| BoundExpr::Param {
				..
			}
			| BoundExpr::ScalarSubquery(_)
			| BoundExpr::Exists(_)
			| BoundExpr::InSubquery {
				..
			}
			| BoundExpr::ArraySubquery(_)
			| BoundExpr::UserVar(_)
			| BoundExpr::SystemVar {
				..
			} => false,
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
