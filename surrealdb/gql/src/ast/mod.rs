//! The GQL abstract syntax tree.
//!
//! The AST represents the full parsed surface of the supported grammar subset
//! (see `doc/gql/REFERENCE.md`), deliberately wider than what the lowering
//! executes: constructs the lowering does not yet support — label expressions
//! (`!`/`&`/`|`/`%`), undirected and multi-directional edges, `GROUP BY`,
//! aggregates, and the like — are represented in full so the lowering can
//! reject them with precise spans rather than the parser producing generic
//! syntax errors. (The v2 lowering does execute the constructs an earlier
//! draft parsed-then-rejected: `OPTIONAL MATCH`, comma-separated and
//! multi-hop patterns, path variables, repeated node variables and the full
//! quantifier set — see `doc/gql/V2_DESIGN.md` and the v1→v2 table in
//! `doc/gql/REFERENCE.md`.)
//!
//! Every node carries (or can produce) a [`Span`] into the source text.

use crate::syn::token::Span;

/// A complete GQL query: a linear program.
#[derive(Clone, Debug, PartialEq)]
pub struct GqlQuery {
	pub program: LinearQuery,
}

/// A linear query: a sequence of data-accessing steps — `MATCH`/`OPTIONAL`
/// reads and `INSERT`/`SET`/`REMOVE`/`DELETE` mutations, in any textual order —
/// and an optional trailing `RETURN`. The binding table threads through the
/// steps in order (`ambientLinearDataModifyingStatementBody`, GQL.g4:394).
///
/// A `MATCH` that follows a mutation re-reads the live (post-mutation) state in
/// the same write transaction, so a later read observes an earlier write within
/// the query.
#[derive(Clone, Debug, PartialEq)]
pub struct LinearQuery {
	/// The steps in textual order. Empty only for a bare `RETURN` (rejected in
	/// lowering: a query needs a `MATCH` or a mutation).
	pub steps: Vec<GqlStep>,
	/// The trailing `RETURN`, or `None` for a mutation-only query (ISO allows a
	/// linear data-modifying statement to end without a result statement).
	pub ret: Option<ReturnClause>,
	pub span: Span,
}

/// One step of a [`LinearQuery`]: a read clause or a data-modifying statement.
#[derive(Clone, Debug, PartialEq)]
pub enum GqlStep {
	/// A `MATCH` clause or `OPTIONAL` operand (a `simpleQueryStatement`).
	Read(MatchItem),
	/// An `INSERT`/`SET`/`REMOVE`/`DELETE` (a `simpleDataModifyingStatement`).
	Mutate(MutationStatement),
}

/// A GQL data-modifying statement (`primitiveDataModifyingStatement`,
/// GQL.g4:412).
#[derive(Clone, Debug, PartialEq)]
pub enum MutationStatement {
	/// `INSERT <insertGraphPattern>`
	Insert(InsertStatement),
	/// `SET setItem (, setItem)*`
	Set(SetStatement),
	/// `REMOVE removeItem (, removeItem)*`
	Remove(RemoveStatement),
	/// `(DETACH | NODETACH)? DELETE deleteItem (, deleteItem)*`
	Delete(DeleteStatement),
}

/// A `SET` statement (`setStatement`, GQL.g4:427).
#[derive(Clone, Debug, PartialEq)]
pub struct SetStatement {
	pub items: Vec<SetItem>,
	pub span: Span,
}

/// A single `SET` item (`setItem`, GQL.g4:435).
#[derive(Clone, Debug, PartialEq)]
pub enum SetItem {
	/// `a.p = v` (`setPropertyItem`).
	Property {
		var: Ident,
		prop: Ident,
		value: GqlExpr,
		span: Span,
	},
	/// `a = { k: v, … }` (`setAllPropertiesItem`).
	AllProperties {
		var: Ident,
		props: Vec<(Ident, GqlExpr)>,
		span: Span,
	},
	/// `a:Label` / `a IS Label` (`setLabelItem`). Parsed in full, rejected in
	/// lowering (a SurrealDB record belongs to exactly one table).
	Label {
		var: Ident,
		label: Ident,
		span: Span,
	},
}

/// A `REMOVE` statement (`removeStatement`, GQL.g4:455).
#[derive(Clone, Debug, PartialEq)]
pub struct RemoveStatement {
	pub items: Vec<RemoveItem>,
	pub span: Span,
}

/// A single `REMOVE` item (`removeItem`, GQL.g4:463).
#[derive(Clone, Debug, PartialEq)]
pub enum RemoveItem {
	/// `a.p` (`removePropertyItem`).
	Property {
		var: Ident,
		prop: Ident,
		span: Span,
	},
	/// `a:Label` / `a IS Label` (`removeLabelItem`). Parsed, rejected in
	/// lowering.
	Label {
		var: Ident,
		label: Ident,
		span: Span,
	},
}

/// A `DELETE` statement (`deleteStatement`, GQL.g4:478).
#[derive(Clone, Debug, PartialEq)]
pub struct DeleteStatement {
	pub detach: DetachMode,
	/// `deleteItem = valueExpression`; each must lower to a bound variable.
	pub items: Vec<GqlExpr>,
	pub span: Span,
}

/// The detach mode of a `DELETE` (`DETACH`/`NODETACH`, GQL.g4:479). `NoDetach`
/// is the ISO default: deleting a node that still has connected edges is an
/// error. `Detach` cascades the connected edges.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub enum DetachMode {
	Detach,
	NoDetach,
}

/// An `INSERT` statement (`insertStatement`, GQL.g4:421): one or more insert
/// path patterns describing the nodes and edges to create.
#[derive(Clone, Debug, PartialEq)]
pub struct InsertStatement {
	pub paths: Vec<InsertPath>,
	pub span: Span,
}

/// A single insert path pattern (`insertPathPattern`, GQL.g4:860): a start node
/// followed by zero or more edge-node steps.
#[derive(Clone, Debug, PartialEq)]
pub struct InsertPath {
	pub start: InsertNode,
	pub steps: Vec<(InsertEdge, InsertNode)>,
	pub span: Span,
}

/// An insert node pattern (`insertNodePattern`, GQL.g4:864): `(a:Label {k: v})`.
/// A node with a fresh variable and a label is created; a node that reuses a
/// `MATCH`-bound variable (no label, no props) references an existing record.
#[derive(Clone, Debug, PartialEq)]
pub struct InsertNode {
	pub var: Option<Ident>,
	pub label: Option<Ident>,
	pub props: Vec<(Ident, GqlExpr)>,
	pub span: Span,
}

/// An insert edge pattern (`insertEdgePattern`, GQL.g4:868). Undirected edges
/// are rejected in the parser.
#[derive(Clone, Debug, PartialEq)]
pub struct InsertEdge {
	pub var: Option<Ident>,
	pub label: Option<Ident>,
	pub direction: InsertEdgeDir,
	pub props: Vec<(Ident, GqlExpr)>,
	pub span: Span,
}

/// The direction of an insert edge (the two directed forms of
/// `insertEdgePattern`).
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub enum InsertEdgeDir {
	/// `<-[…]-`
	Left,
	/// `-[…]->`
	Right,
}

/// A single item in the leading `matchStatement+` of a query
/// (`matchStatement`, GQL.g4:578): either a plain `MATCH` clause or an
/// `OPTIONAL` operand.
///
/// The `OPTIONAL` operand is its own variant — rather than a `bool` on
/// [`MatchClause`] — because a brace/paren block (`OPTIONAL { MATCH …; MATCH …
/// }`) groups *several* inner MATCH statements into one left-outer, all-or-
/// nothing unit (V2_DESIGN R3); a flat per-clause flag cannot distinguish that
/// from two independent `OPTIONAL MATCH` clauses.
#[derive(Clone, Debug, PartialEq)]
pub enum MatchItem {
	/// A plain `MATCH <pattern…>` clause.
	Match(MatchClause),
	/// An `OPTIONAL` operand: `OPTIONAL MATCH …` (the plain form is an
	/// [`OptionalBlock`] of a single item), `OPTIONAL { … }` or `OPTIONAL ( …
	/// )` (`optionalMatchStatement`, GQL.g4:587).
	Optional(OptionalBlock),
}

/// The operand of an `OPTIONAL`: one or more inner [`MatchItem`]s forming a
/// single left-outer, all-or-nothing unit (`matchStatementBlock`,
/// GQL.g4:597). The plain `OPTIONAL MATCH …` form is a block of exactly one
/// item; the brace/paren forms hold the block's `matchStatement+`, which may
/// themselves nest further `OPTIONAL`s.
#[derive(Clone, Debug, PartialEq)]
pub struct OptionalBlock {
	pub items: Vec<MatchItem>,
	/// The span from the `OPTIONAL` keyword to the end of the operand.
	pub span: Span,
}

/// A single `MATCH` clause (`simpleMatchStatement`, GQL.g4:583).
///
/// The `WHERE` clause belongs to the graph pattern of the MATCH clause
/// (`graphPatternWhereClause`, GQL.g4:847), not to the enclosing statement.
#[derive(Clone, Debug, PartialEq)]
pub struct MatchClause {
	pub patterns: Vec<PathPattern>,
	pub where_clause: Option<GqlExpr>,
	pub span: Span,
}

/// A single path pattern: a start node followed by zero or more edge-node
/// steps.
#[derive(Clone, Debug, PartialEq)]
pub struct PathPattern {
	/// The declared path variable: `p = (a)-[k]->(b)`.
	pub path_var: Option<Ident>,
	/// The path-search / path-mode prefix (`pathPatternPrefix`, GQL.g4:896-962),
	/// or `None` when the pattern carries no prefix (the default: every path,
	/// `WALK` mode).
	pub prefix: Option<PathPatternPrefix>,
	pub start: NodePattern,
	pub steps: Vec<PathStep>,
}

/// A parsed path-pattern prefix (`pathPatternPrefix`, GQL.g4:896-962): a
/// path-search selector together with an optional path mode. The full ISO
/// surface is represented so the lowering can reject the unsupported
/// combinations with precise spans.
#[derive(Clone, Debug, PartialEq)]
pub struct PathPatternPrefix {
	pub kind: PathSearchKind,
	/// The explicit path mode (`WALK`/`TRAIL`/`SIMPLE`/`ACYCLIC`), or `None` when
	/// omitted (the ISO default is `WALK`).
	pub mode: Option<PathMode>,
	pub span: Span,
}

/// The path-search selector of a [`PathPatternPrefix`]. A bare path-mode prefix
/// (`WALK`/`TRAIL`/… with no search word) is represented as
/// [`PathSearchKind::All`] (every path), carrying only its mode.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub enum PathSearchKind {
	/// `ALL` (or a bare path-mode prefix): every path.
	All,
	/// `ANY [k]`: any `k` paths (`None` ⇒ 1).
	Any {
		count: Option<u32>,
	},
	/// `ALL SHORTEST`: every minimum-length path.
	AllShortest,
	/// `ANY SHORTEST`: one minimum-length path.
	AnyShortest,
	/// `SHORTEST k`: the `k` shortest paths.
	ShortestCounted {
		count: u32,
	},
	/// `SHORTEST [k] GROUP(S)`: every path in the `k` smallest length groups
	/// (`None` ⇒ 1).
	ShortestGroups {
		count: Option<u32>,
	},
}

/// An ISO path mode (`pathMode`, GQL.g4:907-912): the node/edge repetition
/// discipline of a matched path.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub enum PathMode {
	/// `WALK`: nodes and edges may repeat (the ISO default).
	Walk,
	/// `TRAIL`: no repeated edge.
	Trail,
	/// `SIMPLE`: no repeated node, except the path may close on its start.
	Simple,
	/// `ACYCLIC`: no repeated node.
	Acyclic,
}

/// A single hop in a path pattern: an edge pattern and the node it leads to.
#[derive(Clone, Debug, PartialEq)]
pub struct PathStep {
	pub edge: EdgePattern,
	pub node: NodePattern,
}

/// A node pattern: `(var:Label WHERE expr)` or `(var:Label {k: v})`.
#[derive(Clone, Debug, PartialEq)]
pub struct NodePattern {
	pub var: Option<Ident>,
	pub label: Option<LabelExpr>,
	pub predicate: Option<ElementPredicate>,
	pub span: Span,
}

/// The predicate inside a node or edge pattern filler. A filler has either an
/// inline `WHERE` or a property map, never both (`elementPatternPredicate`,
/// GQL.g4:1009).
#[derive(Clone, Debug, PartialEq)]
pub enum ElementPredicate {
	Where(GqlExpr),
	Props(Vec<(Ident, GqlExpr)>),
}

/// An edge pattern: `-[var:Label WHERE expr]->` and its abbreviated forms.
#[derive(Clone, Debug, PartialEq)]
pub struct EdgePattern {
	pub var: Option<Ident>,
	pub label: Option<LabelExpr>,
	pub direction: EdgeDirection,
	pub predicate: Option<ElementPredicate>,
	pub quantifier: Option<Quantifier>,
	pub span: Span,
}

/// The direction of an edge pattern. All seven grammar kinds
/// (GQL.g4:1035-1086), in grammar order.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub enum EdgeDirection {
	/// `<-[…]-` or `<-`
	Left,
	/// `~[…]~` or `~`
	Undirected,
	/// `-[…]->` or `->`
	Right,
	/// `<~[…]~` or `<~`
	LeftOrUndirected,
	/// `~[…]~>` or `~>`
	UndirectedOrRight,
	/// `<-[…]->` or `<->`
	LeftOrRight,
	/// `-[…]-` or `-`
	Any,
}

/// A graph pattern quantifier, postfix on an edge pattern
/// (GQL.g4:1125-1146).
#[derive(Clone, Debug, PartialEq)]
pub struct Quantifier {
	pub kind: QuantifierKind,
	pub span: Span,
}

/// The kind of a graph pattern quantifier.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub enum QuantifierKind {
	/// `*`, equivalent to `{0,}`.
	Star,
	/// `+`, equivalent to `{1,}`.
	Plus,
	/// `?`, an optional path factor.
	Question,
	/// `{n}`
	Fixed(u32),
	/// `{n,m}`, `{n,}`, `{,m}` or `{,}`.
	Range(Option<u32>, Option<u32>),
}

/// A label expression (GQL.g4:1102-1109). Precedence: `!` > `&` > `|`.
#[derive(Clone, Debug, PartialEq)]
pub enum LabelExpr {
	/// `Label`
	Name(Ident),
	/// `%`
	Wildcard(Span),
	/// `!expr`, with the span covering the `!` and its operand.
	Negation(Box<LabelExpr>, Span),
	/// `expr & expr`, with the span covering both operands.
	Conjunction(Box<LabelExpr>, Box<LabelExpr>, Span),
	/// `expr | expr`, with the span covering both operands.
	Disjunction(Box<LabelExpr>, Box<LabelExpr>, Span),
}

impl LabelExpr {
	/// Returns the span of the source text this label expression covers.
	///
	/// Constant time: operator chains can be arbitrarily deep, so no variant
	/// derives its span recursively.
	pub fn span(&self) -> Span {
		match self {
			LabelExpr::Name(ident) => ident.span,
			LabelExpr::Wildcard(span) => *span,
			LabelExpr::Negation(_, span)
			| LabelExpr::Conjunction(_, _, span)
			| LabelExpr::Disjunction(_, _, span) => *span,
		}
	}

	/// Returns whether this node holds child label expressions which the
	/// manual [`Drop`] must release iteratively.
	fn has_children(&self) -> bool {
		matches!(
			self,
			LabelExpr::Negation(..) | LabelExpr::Conjunction(..) | LabelExpr::Disjunction(..)
		)
	}

	/// Moves the children of this node onto `stack`, leaving leaves in their
	/// place, so that [`Drop`] can release arbitrarily deep trees without
	/// recursing on the machine stack.
	fn take_children(&mut self, stack: &mut Vec<LabelExpr>) {
		fn take(child: &mut Box<LabelExpr>, stack: &mut Vec<LabelExpr>) {
			let child = std::mem::replace(&mut **child, LabelExpr::Wildcard(Span::empty()));
			if child.has_children() {
				stack.push(child);
			}
		}
		match self {
			LabelExpr::Name(_) | LabelExpr::Wildcard(_) => {}
			LabelExpr::Negation(child, _) => take(child, stack),
			LabelExpr::Conjunction(left, right, _) | LabelExpr::Disjunction(left, right, _) => {
				take(left, stack);
				take(right, stack);
			}
		}
	}
}

/// The derived recursive drop would overflow the machine stack on the deep
/// linear `!`/`&`/`|` chains the parser builds without consuming nesting
/// budget, so deep trees are torn down iteratively via a worklist.
impl Drop for LabelExpr {
	fn drop(&mut self) {
		if !self.has_children() {
			return;
		}
		let mut stack = Vec::new();
		self.take_children(&mut stack);
		while let Some(mut expr) = stack.pop() {
			expr.take_children(&mut stack);
		}
	}
}

/// The `RETURN` clause, including the trailing `GROUP BY` and
/// `ORDER BY`/`OFFSET`/`LIMIT` page statement.
#[derive(Clone, Debug, PartialEq)]
pub struct ReturnClause {
	pub quantifier: Option<SetQuantifier>,
	pub items: ReturnItems,
	/// The `GROUP BY` grouping elements (`groupByClause`, GQL.g4:1313), empty
	/// when absent. They sit between the return items and `ORDER BY`.
	pub group_by: Vec<GqlGroupItem>,
	pub order_by: Vec<OrderItem>,
	/// The `OFFSET`/`SKIP` count: an unsigned integer or a parameter.
	pub skip: Option<GqlExpr>,
	/// The `LIMIT` count: an unsigned integer or a parameter.
	pub limit: Option<GqlExpr>,
	pub span: Span,
}

/// A single `GROUP BY` grouping element. The GQL grammar restricts these to
/// binding variable references; we parse the same value-expression grammar as
/// the return items and `ORDER BY` keys (so `a.name` is accepted) and let the
/// lowering enforce the shape.
#[derive(Clone, Debug, PartialEq)]
pub struct GqlGroupItem {
	pub expr: GqlExpr,
	pub span: Span,
}

/// The set quantifier of a `RETURN` clause (GQL.g4:2405).
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub enum SetQuantifier {
	Distinct,
	All,
}

/// The projection list of a `RETURN` clause.
#[derive(Clone, Debug, PartialEq)]
pub enum ReturnItems {
	/// `RETURN *`
	Star,
	/// `RETURN expr [AS alias], …`
	Items(Vec<ReturnItem>),
}

/// A single `RETURN` item.
#[derive(Clone, Debug, PartialEq)]
pub struct ReturnItem {
	pub expr: GqlExpr,
	pub alias: Option<Ident>,
	/// The verbatim source slice of the expression, used as the default
	/// column name when no alias is given.
	pub text: String,
}

/// A single `ORDER BY` sort specification.
#[derive(Clone, Debug, PartialEq)]
pub struct OrderItem {
	pub expr: GqlExpr,
	/// `ASC`/`ASCENDING` is `Some(true)`, `DESC`/`DESCENDING` is
	/// `Some(false)`, `None` when unspecified.
	pub ascending: Option<bool>,
	/// `NULLS FIRST` is `Some(true)`, `NULLS LAST` is `Some(false)`, `None`
	/// when unspecified.
	pub nulls_first: Option<bool>,
	pub span: Span,
}

/// An identifier, with the span of its source text.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Ident {
	pub name: String,
	pub span: Span,
}

/// A GQL value expression.
#[derive(Clone, Debug, PartialEq)]
pub enum GqlExpr {
	Literal(GqlLiteral, Span),
	/// `$name`. Substituted parameter references (`$$name`) are rejected at
	/// parse time.
	Param {
		name: String,
		span: Span,
	},
	/// A binding variable reference.
	Variable(Ident),
	/// A property reference: `expr.name`. The span covers the base
	/// expression and the property name; it is stored rather than derived so
	/// that [`GqlExpr::span`] does not recurse through deep chains.
	Property(Box<GqlExpr>, Ident, Span),
	Unary {
		op: UnaryOp,
		expr: Box<GqlExpr>,
		span: Span,
	},
	Binary {
		left: Box<GqlExpr>,
		op: BinaryOp,
		right: Box<GqlExpr>,
		span: Span,
	},
	/// A boolean test: `expr IS [NOT] TRUE|FALSE|UNKNOWN`.
	IsBool {
		expr: Box<GqlExpr>,
		value: TruthValue,
		negated: bool,
		span: Span,
	},
	/// A null test: `expr IS [NOT] NULL`.
	IsNull {
		expr: Box<GqlExpr>,
		negated: bool,
		span: Span,
	},
	FunctionCall {
		name: Ident,
		/// The leading set quantifier of an aggregate call:
		/// `count(DISTINCT a)` (`generalSetFunction`, GQL.g4:2387).
		quantifier: Option<SetQuantifier>,
		/// The span of the `*` in a `count(*)` aggregate (GQL.g4:2381).
		/// When set, `args` is empty.
		star: Option<Span>,
		args: Vec<GqlExpr>,
		span: Span,
	},
	/// A list literal: `[a, b, c]`.
	List(Vec<GqlExpr>, Span),
	/// A record literal: `{k: v, …}`.
	Map(Vec<(Ident, GqlExpr)>, Span),
}

impl GqlExpr {
	/// Returns the span of the source text this expression covers.
	///
	/// Constant time: linear chains can be arbitrarily deep, so no variant
	/// derives its span recursively.
	pub fn span(&self) -> Span {
		match self {
			GqlExpr::Literal(_, span) => *span,
			GqlExpr::Param {
				span,
				..
			} => *span,
			GqlExpr::Variable(ident) => ident.span,
			GqlExpr::Property(_, _, span) => *span,
			GqlExpr::Unary {
				span,
				..
			} => *span,
			GqlExpr::Binary {
				span,
				..
			} => *span,
			GqlExpr::IsBool {
				span,
				..
			} => *span,
			GqlExpr::IsNull {
				span,
				..
			} => *span,
			GqlExpr::FunctionCall {
				span,
				..
			} => *span,
			GqlExpr::List(_, span) => *span,
			GqlExpr::Map(_, span) => *span,
		}
	}

	/// Returns whether this node holds child expressions which the manual
	/// [`Drop`] must release iteratively.
	fn has_children(&self) -> bool {
		match self {
			GqlExpr::Literal(..)
			| GqlExpr::Param {
				..
			}
			| GqlExpr::Variable(_) => false,
			GqlExpr::Property(..)
			| GqlExpr::Unary {
				..
			}
			| GqlExpr::Binary {
				..
			}
			| GqlExpr::IsBool {
				..
			}
			| GqlExpr::IsNull {
				..
			} => true,
			GqlExpr::FunctionCall {
				args,
				..
			} => !args.is_empty(),
			GqlExpr::List(items, _) => !items.is_empty(),
			GqlExpr::Map(fields, _) => !fields.is_empty(),
		}
	}

	/// Moves the children of this node onto `stack`, leaving leaves in their
	/// place, so that [`Drop`] can release arbitrarily deep trees without
	/// recursing on the machine stack.
	fn take_children(&mut self, stack: &mut Vec<GqlExpr>) {
		fn take(child: &mut Box<GqlExpr>, stack: &mut Vec<GqlExpr>) {
			let child =
				std::mem::replace(&mut **child, GqlExpr::Literal(GqlLiteral::Null, Span::empty()));
			if child.has_children() {
				stack.push(child);
			}
		}
		match self {
			GqlExpr::Literal(..)
			| GqlExpr::Param {
				..
			}
			| GqlExpr::Variable(_) => {}
			GqlExpr::Property(base, _, _) => take(base, stack),
			GqlExpr::Unary {
				expr,
				..
			}
			| GqlExpr::IsBool {
				expr,
				..
			}
			| GqlExpr::IsNull {
				expr,
				..
			} => take(expr, stack),
			GqlExpr::Binary {
				left,
				right,
				..
			} => {
				take(left, stack);
				take(right, stack);
			}
			GqlExpr::FunctionCall {
				args,
				..
			} => stack.extend(args.drain(..).filter(GqlExpr::has_children)),
			GqlExpr::List(items, _) => stack.extend(items.drain(..).filter(GqlExpr::has_children)),
			GqlExpr::Map(fields, _) => {
				stack.extend(fields.drain(..).map(|(_, value)| value).filter(GqlExpr::has_children))
			}
		}
	}
}

/// The derived recursive drop would overflow the machine stack on the deep
/// linear chains the parser deliberately builds without consuming nesting
/// budget (property postfixes, binary operator spines, `NOT`/sign prefixes),
/// so deep trees are torn down iteratively via a worklist.
impl Drop for GqlExpr {
	fn drop(&mut self) {
		if !self.has_children() {
			return;
		}
		let mut stack = Vec::new();
		self.take_children(&mut stack);
		while let Some(mut expr) = stack.pop() {
			expr.take_children(&mut stack);
		}
	}
}

/// A unary (prefix) operator.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub enum UnaryOp {
	/// `NOT`
	Not,
	/// `-`
	Neg,
	/// `+`
	Plus,
}

/// A binary operator, in precedence order from loosest to tightest binding
/// (see `doc/gql/REFERENCE.md` section (e)).
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub enum BinaryOp {
	/// `OR`
	Or,
	/// `XOR`
	Xor,
	/// `AND`
	And,
	/// `=`
	Eq,
	/// `<>` — GQL has no `!=`.
	Neq,
	/// `<`
	Lt,
	/// `<=`
	Lte,
	/// `>`
	Gt,
	/// `>=`
	Gte,
	/// `||`
	Concat,
	/// `+`
	Add,
	/// `-`
	Sub,
	/// `*`
	Mul,
	/// `/`
	Div,
}

/// A truth value in a boolean test: `IS [NOT] TRUE|FALSE|UNKNOWN`.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub enum TruthValue {
	True,
	False,
	Unknown,
}

/// A GQL literal value.
#[derive(Clone, Debug, PartialEq)]
pub enum GqlLiteral {
	Null,
	Bool(bool),
	Integer(i64),
	Float(f64),
	String(String),
}
