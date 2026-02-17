use common::ids::IdSet;
use common::span::Span;
use rust_decimal::Decimal;

mod mac;
mod types;
pub mod vis;
mod visit;

pub use types::{Node, NodeId, NodeList, NodeListId, Spanned, UniqueNode};

use crate::mac::ast_type;
use crate::types::AstSpan;

type NodeSet<T> = IdSet<u32, T>;

library! {
	/// Type containing all the possible node types.
	#[derive(Debug)]
	Library{
		query: Vec<Query>,

		top_level_expr: Vec<TopLevelExpr>,
		top_level_exprs: Vec<NodeList<TopLevelExpr>>,

		transactions: Vec<Transaction>,

		exprs: Vec<Expr>,

		builtin: Vec<Builtin>,
		floats: Vec<Spanned<f64>>,
		integer: Vec<Integer>,
		decimal: Vec<Spanned<Decimal>>,

		point: Vec<Point>,

		binary: Vec<BinaryExpr>,
		postfix: Vec<PostfixExpr>,
		prefix: Vec<PrefixExpr>,
		idiom: Vec<IdiomExpr>,

		destructure: Vec<Destructure>,
		destructures: Vec<NodeList<Destructure>>,

		path: Vec<Path>,
		path_segment: Vec<PathSegment>,
		path_segments: Vec<NodeList<PathSegment>>,

		params: Vec<Ident>,
		ident: Vec<Ident>,
		idents: Vec<NodeListId<Ident>>,
		#[set]
		strings: NodeSet<String>,
	}
}
pub type Ast = types::Ast<Library>;

impl UniqueNode for String {}
impl Node for f64 {}
impl Node for Decimal {}

ast_type! {
	pub struct Query {
		pub exprs: Option<NodeListId<TopLevelExpr>>,
	}
}

ast_type! {
	pub struct Ident {
		pub text: NodeId<String>,
	}
}

ast_type! {
	pub struct Param {
		pub text: NodeId<String>,
	}
}

#[derive(Debug)]
pub enum Distance {
	Chebyshev,
	Cosine,
	Euclidean,
	Hamming,
	Jaccard,
	Manhattan,
	Minkowski(f64),
	Pearson,
}

#[derive(Debug)]
pub enum MatchesOperator {
	And,
	Or,
}

#[derive(Debug)]
pub enum BinaryOperator {
	/// `-`
	Subtract,
	/// `+`
	Add,
	/// `*`, `×`
	Multiply,
	/// `/`
	Divide,
	/// `%`
	Remainder,
	/// `**`
	Power,
	/// `=`
	Equal,
	/// `==`
	ExactEqual,
	/// `!=`
	NotEqual,
	/// `*=`
	AllEqual,
	/// `?=`
	AnyEqual,

	/// `||`, `OR`
	Or,
	/// `&&`, `AND`
	And,
	/// `??`
	NullCoalescing,
	/// `?:`
	TenaryCondition,

	/// `<`
	LessThan,
	/// `<=`
	LessThanEqual,
	/// `>`
	GreaterThan,
	/// `>=`
	GreaterThanEqual,

	/// `∋`
	Contain,
	/// `∌`
	NotContain,
	/// `⊇`
	ContainAll,
	/// `⊃`
	ContainAny,
	/// `⊅`
	ContainNone,
	/// `∈`
	Inside,
	/// `∉`
	NotInside,
	/// `⊆`
	AllInside,
	/// `⊂`
	AnyInside,
	/// `⊄`
	NoneInside,

	/// `OUTSIDE`
	Outside,
	/// `INTERSECTS`
	Intersects,

	/// `..`
	Range,
	/// `..=`
	RangeInclusive,
	/// `>..`
	RangeSkip,
	/// `>..=`
	RangeSkipInclusive,

	// `@@`
	Matches {
		reference: u8,
		operator: MatchesOperator,
	},
	KNearestNeighbour {
		k: u32,
		distance: Distance,
	},
	KTree {
		k: u32,
	},
	KApproximate {
		k: u32,
		ef: u32,
	},
}

#[derive(Debug)]
pub enum DestructureOperator {
	/// { field.* }
	All,
	/// { field : EXPR }
	Expr(NodeId<Expr>),
	/// { field.{ .. } }
	Destructure(Option<NodeListId<Destructure>>),
}

ast_type! {
	pub struct Destructure {
		pub field: NodeId<Ident>,
		pub op: Option<Spanned<DestructureOperator>>,
	}
}

#[derive(Debug)]
pub enum IdiomOperator {
	/// [*] | .*
	All,
	/// [$]
	Last,
	/// .field
	Field(NodeId<String>),
	/// \[EXPR\]
	Index(NodeId<Expr>),
	/// \[? EXPR\] | \[WHERE EXPR\]
	Where(NodeId<Expr>),
	/// .?
	Option,
	/// .@
	Repeat,
	/// .{ .. }
	Destructure(Option<NodeListId<Destructure>>),
}

#[derive(Debug)]
pub enum PostfixOperator {
	Range,
	RangeSkip,

	/// .field(EXPR*)
	MethodCall(NodeId<String>, Option<NodeListId<Expr>>),
	/// (EXPR*)
	Call(Option<NodeListId<Expr>>),
}

ast_type! {
	pub enum PrefixOperator {
		Not(Span),
		Negate(Span),
		Positive(Span),
		Range(Span),
		RangeInclusive(Span),
		Cast(NodeId<Kind>),
	}
}

ast_type! {
	pub enum Kind {
		None(Span),
	}
}

ast_type! {
	pub enum Builtin {
		True(Span),
		False(Span),
		None(Span),
		Null(Span),
	}
}

#[derive(Debug)]
pub enum Sign {
	Plus,
	Minus,
}

#[derive(Debug)]
pub struct Integer {
	pub value: u64,
	pub sign: Sign,
	pub span: Span,
}
impl Node for Integer {}
impl AstSpan for Integer {
	fn ast_span<L: types::NodeLibrary>(&self, _: &types::Ast<L>) -> Span {
		self.span
	}
}

ast_type! {
	pub struct BinaryExpr {
		pub left: NodeId<Expr>,
		pub op: Spanned<BinaryOperator>,
		pub right: NodeId<Expr>,
	}
}

ast_type! {
	pub struct PostfixExpr {
		pub left: NodeId<Expr>,
		pub op: Spanned<PostfixOperator>,
	}
}

ast_type! {
	pub struct IdiomExpr{
		pub left: NodeId<Expr>,
		pub op: Spanned<IdiomOperator>,
	}
}

ast_type! {
	pub struct PrefixExpr {
		pub op: PrefixOperator,
		pub right: NodeId<Expr>,
	}
}

ast_type! {
	#[derive(Copy, Clone)]
	pub enum Expr {
		Covered(NodeId<Expr>),

		Builtin(NodeId<Builtin>),
		Float(NodeId<Spanned<f64>>),
		Integer(NodeId<Integer>),
		Decimal(NodeId<Spanned<Decimal>>),

		Point(NodeId<Point>),

		Path(NodeId<Path>),
		Param(NodeId<Param>),

		Binary(NodeId<BinaryExpr>),
		Postfix(NodeId<PostfixExpr>),
		Idiom(NodeId<IdiomExpr>),
		Prefix(NodeId<PrefixExpr>),

		Throw(NodeId<Expr>),
	}
}

ast_type! {
	pub struct Transaction {
		pub statements: Option<NodeListId<TopLevelExpr>>,
		pub commits: bool,
	}
}

#[derive(Debug)]
pub enum UseStatementKind {
	Namespace(NodeId<Ident>),
	NamespaceDatabase(NodeId<Ident>, NodeId<Ident>),
	Database(NodeId<Ident>),
}

ast_type! {
	pub struct UseStatement {
		pub kind: UseStatementKind,
	}
}

ast_type! {
	pub struct OptionStatement {
		pub name: NodeId<Ident>,
		pub value: bool,
	}
}

ast_type! {
	pub enum TopLevelExpr {
		Transaction(NodeId<Transaction>),
		Use(NodeId<UseStatement>),
		Option(NodeId<OptionStatement>),
		Expr(NodeId<Expr>),
	}
}

ast_type! {
	pub struct Point{
		pub x: f64,
		pub y: f64,
	}
}

ast_type! {
	pub struct Path{
		pub start: NodeId<Ident>,
		pub parts: Option<NodeListId<PathSegment>>,
	}
}

ast_type! {
	pub enum PathSegment{
		Ident(NodeId<Ident>),
		Version(Spanned<Version>),
	}
}

ast_type! {
	pub struct Version{
		pub major: u64,
		pub minor: u64,
		pub patch: u64,
	}
}
