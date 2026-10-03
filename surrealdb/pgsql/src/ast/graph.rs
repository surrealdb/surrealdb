//! SQL:2023 Part 16 (SQL/PGQ) Property Graph Query AST nodes.

use crate::ast::expr::{Expr, QualifiedName};
use crate::span::Span;

/// `CREATE PROPERTY GRAPH` statement (SQL:2023 Part 16).
#[derive(Debug, Clone, PartialEq)]
pub struct CreatePropertyGraphStmt {
	/// True if `IF NOT EXISTS` is specified.
	pub if_not_exists: bool,
	/// Property graph name.
	pub name: QualifiedName,
	/// Vertex table definitions.
	pub vertex_tables: Vec<VertexTableDef>,
	/// Edge table definitions.
	pub edge_tables: Vec<EdgeTableDef>,
	/// Source span.
	pub span: Span,
}

impl CreatePropertyGraphStmt {
	/// Recursively set all spans to zero.
	pub fn normalize_spans(&mut self) {
		let zero = Span::empty(0);
		self.span = zero;
		self.name.span = zero;
		for vt in &mut self.vertex_tables {
			vt.normalize_spans();
		}
		for et in &mut self.edge_tables {
			et.normalize_spans();
		}
	}
}

/// Vertex table specification inside `CREATE PROPERTY GRAPH`.
#[derive(Debug, Clone, PartialEq)]
pub struct VertexTableDef {
	/// Base relational table name.
	pub table_name: QualifiedName,
	/// Optional key columns (`KEY (col, ...)`).
	pub key_columns: Vec<String>,
	/// Optional label (`LABEL label_name`).
	pub label: Option<String>,
	/// Properties exposed (`PROPERTIES (col, ...)`, or empty for default all).
	pub properties: Vec<String>,
	/// Source span.
	pub span: Span,
}

impl VertexTableDef {
	/// Recursively set all spans to zero.
	pub fn normalize_spans(&mut self) {
		let zero = Span::empty(0);
		self.span = zero;
		self.table_name.span = zero;
	}
}

/// Edge table specification inside `CREATE PROPERTY GRAPH`.
#[derive(Debug, Clone, PartialEq)]
pub struct EdgeTableDef {
	/// Base relational edge table name.
	pub table_name: QualifiedName,
	/// Optional key columns (`KEY (col, ...)`).
	pub key_columns: Vec<String>,
	/// Source vertex table reference: `SOURCE KEY (...) REFERENCES vertex_table (...)`.
	pub source_table: QualifiedName,
	/// Source key columns in this edge table.
	pub source_key: Vec<String>,
	/// Referenced key columns in the source vertex table.
	pub source_references: Vec<String>,
	/// Target vertex table reference: `TARGET KEY (...) REFERENCES vertex_table (...)`.
	pub target_table: QualifiedName,
	/// Target key columns in this edge table.
	pub target_key: Vec<String>,
	/// Referenced key columns in the target vertex table.
	pub target_references: Vec<String>,
	/// Optional label (`LABEL label_name`).
	pub label: Option<String>,
	/// Properties exposed (`PROPERTIES (col, ...)`).
	pub properties: Vec<String>,
	/// Source span.
	pub span: Span,
}

impl EdgeTableDef {
	/// Recursively set all spans to zero.
	pub fn normalize_spans(&mut self) {
		let zero = Span::empty(0);
		self.span = zero;
		self.table_name.span = zero;
		self.source_table.span = zero;
		self.target_table.span = zero;
	}
}

/// `DROP PROPERTY GRAPH` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DropPropertyGraphStmt {
	/// True if `IF EXISTS` was specified.
	pub if_exists: bool,
	/// Graph name.
	pub name: QualifiedName,
	/// True if `CASCADE` was specified, false for `RESTRICT` (default).
	pub cascade: bool,
	/// Source span.
	pub span: Span,
}

impl DropPropertyGraphStmt {
	/// Recursively set all spans to zero.
	pub fn normalize_spans(&mut self) {
		let zero = Span::empty(0);
		self.span = zero;
		self.name.span = zero;
	}
}

/// A graph pattern in a `MATCH` clause: `MATCH path_pattern, ... [WHERE expr]`.
#[derive(Debug, Clone, PartialEq)]
pub struct GraphPattern {
	/// Path patterns in the match clause.
	pub paths: Vec<GraphPathPattern>,
	/// Optional graph pattern filter `WHERE`.
	pub where_clause: Option<Expr>,
	/// Source span.
	pub span: Span,
}

impl GraphPattern {
	/// Recursively set all spans to zero.
	pub fn normalize_spans(&mut self) {
		let zero = Span::empty(0);
		self.span = zero;
		for p in &mut self.paths {
			p.normalize_spans();
		}
		if let Some(w) = &mut self.where_clause {
			w.normalize_spans();
		}
	}
}

/// A path pattern consisting of alternating vertices and edges.
#[derive(Debug, Clone, PartialEq)]
pub struct GraphPathPattern {
	/// Path variable name (e.g. `p = (a)-[e]->(b)`).
	pub path_variable: Option<String>,
	/// Alternating vertex and edge elements.
	pub elements: Vec<GraphElementPattern>,
	/// Source span.
	pub span: Span,
}

impl GraphPathPattern {
	/// Recursively set all spans to zero.
	pub fn normalize_spans(&mut self) {
		let zero = Span::empty(0);
		self.span = zero;
		for elem in &mut self.elements {
			elem.normalize_spans();
		}
	}
}

/// Element in a graph path pattern.
#[derive(Debug, Clone, PartialEq)]
pub enum GraphElementPattern {
	/// Vertex node pattern `(v:Label)`.
	Vertex(GraphVertexPattern),
	/// Edge pattern `-[e:Label]->`.
	Edge(GraphEdgePattern),
}

impl GraphElementPattern {
	/// Recursively set all spans to zero.
	pub fn normalize_spans(&mut self) {
		match self {
			Self::Vertex(v) => v.normalize_spans(),
			Self::Edge(e) => e.normalize_spans(),
		}
	}
}

/// Vertex pattern node: `( [variable] [:label] [WHERE filter] )`.
#[derive(Debug, Clone, PartialEq)]
pub struct GraphVertexPattern {
	/// Optional variable name.
	pub variable: Option<String>,
	/// Labels or label expression terms.
	pub labels: Vec<String>,
	/// Optional in-node WHERE filter.
	pub where_clause: Option<Expr>,
	/// Source span.
	pub span: Span,
}

impl GraphVertexPattern {
	/// Recursively set all spans to zero.
	pub fn normalize_spans(&mut self) {
		let zero = Span::empty(0);
		self.span = zero;
		if let Some(w) = &mut self.where_clause {
			w.normalize_spans();
		}
	}
}

/// Edge pattern direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphEdgeDirection {
	/// Outgoing edge (`-[...]->`).
	Right,
	/// Incoming edge (`<-[...]--`).
	Left,
	/// Undirected / any direction (`-[...]-`).
	Any,
	/// Bidirectional edge (`<-[...]-->` or `<->`).
	Bidirectional,
}

/// Edge pattern node: `-[ [variable] [:label] [quantifier] [WHERE filter] ]->`.
#[derive(Debug, Clone, PartialEq)]
pub struct GraphEdgePattern {
	/// Direction of edge.
	pub direction: GraphEdgeDirection,
	/// Optional variable name.
	pub variable: Option<String>,
	/// Labels or label expression terms.
	pub labels: Vec<String>,
	/// Optional quantifier (`*`, `+`, `{1, 5}`).
	pub quantifier: Option<GraphQuantifier>,
	/// Optional in-edge WHERE filter.
	pub where_clause: Option<Expr>,
	/// Source span.
	pub span: Span,
}

impl GraphEdgePattern {
	/// Recursively set all spans to zero.
	pub fn normalize_spans(&mut self) {
		let zero = Span::empty(0);
		self.span = zero;
		if let Some(w) = &mut self.where_clause {
			w.normalize_spans();
		}
	}
}

/// Quantifier on graph path or edge pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphQuantifier {
	/// Zero or more occurrences (`*` or `{0,}`).
	ZeroOrMore,
	/// One or more occurrences (`+` or `{1,}`).
	OneOrMore,
	/// Zero or one occurrence (`?` or `{0, 1}`).
	ZeroOrOne,
	/// Exact count (`{n}`).
	Exact(u32),
	/// Bounded range (`{min, max}` or `{min,}`).
	Range(u32, Option<u32>),
}

/// Column projection in `GRAPH_TABLE (...) COLUMNS (expr [AS alias], ...)`.
#[derive(Debug, Clone, PartialEq)]
pub struct GraphColumnDef {
	/// Expression computed from pattern variables.
	pub expr: Expr,
	/// Optional alias name for the output column.
	pub alias: Option<String>,
	/// Source span.
	pub span: Span,
}

impl GraphColumnDef {
	/// Recursively set all spans to zero.
	pub fn normalize_spans(&mut self) {
		let zero = Span::empty(0);
		self.span = zero;
		self.expr.normalize_spans();
	}
}
