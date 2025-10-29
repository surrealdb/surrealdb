use std::ops::{Deref, DerefMut};

use common::ids::IdSet;
use rust_decimal::Decimal;

mod span;
pub use span::Span;
mod types;
pub use types::Node;

pub use types::{NodeId, NodeList, NodeListId};

type NodeSet<T> = IdSet<u32, T>;

library! {
	Library{
		top_level_expr: Vec<TopLevelExpr>,
		top_level_exprs: Vec<NodeList<TopLevelExpr>>,
		exprs: Vec<Expr>,

		builtins: Vec<Spanned<Builtin>>,
		floats: Vec<Spanned<f64>>,
		integer: Vec<Integer>,
		decimals: Vec<Spanned<Decimal>>,

		binary: Vec<BinaryExpr>,
		postfix: Vec<PostfixExpr>,
		prefix: Vec<PrefixExpr>,

		params: Vec<Ident>,
		idents: Vec<Ident>,
		#[set]
		strings: NodeSet<String>,
	}
}

pub type Ast = types::Ast<Library>;

pub struct Ident {
	pub text: NodeId<String>,
	pub span: Span,
}
impl Node for Ident {}

pub struct Param {
	pub text: NodeId<String>,
	pub span: Span,
}
impl Node for Param {}

pub struct Spanned<T> {
	pub value: T,
	pub span: Span,
}

impl<T> Deref for Spanned<T> {
	type Target = T;

	fn deref(&self) -> &Self::Target {
		&self.value
	}
}
impl<T> DerefMut for Spanned<T> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.value
	}
}

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

pub enum MatchesOperator {
	And,
	Or,
}

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
	MoreThan,
	/// `>=`
	MoreThanEqual,

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

pub enum PostfixOperator {
	Range,
	RangeSkip,

	MethodCall(NodeId<String>, NodeId<Expr>),
	Call(NodeId<Expr>),
	All,
	Last,
	Field(NodeId<String>),
	Index(NodeId<Expr>),
}

pub enum PrefixOperator {
	Not,
	Negate,
	Positive,
	Range,
	RangeInclusive,
	Cast(NodeId<Kind>),
}

pub enum Kind {}

pub enum Builtin {
	True,
	False,
	None,
	Null,
}

pub struct Integer {
	pub value: u64,
	pub sign: bool,
	pub span: Span,
}

pub struct BinaryExpr {
	pub left: NodeId<Expr>,
	pub op: Spanned<BinaryOperator>,
	pub right: NodeId<Expr>,
}

pub struct PostfixExpr {
	pub left: NodeId<Expr>,
	pub op: Spanned<PostfixOperator>,
}

pub struct PrefixExpr {
	pub left: NodeId<Expr>,
	pub op: Spanned<PostfixOperator>,
}

pub enum Expr {
	Covered(NodeId<Expr>),

	Builtin(NodeId<Spanned<Builtin>>),
	Float(NodeId<Spanned<f64>>),
	Integer(NodeId<Integer>),

	Ident(NodeId<Ident>),
	Param(NodeId<Param>),

	Binary(NodeId<BinaryExpr>),
	Postfix(NodeId<PostfixExpr>),
	Prefix(NodeId<PrefixExpr>),

	Throw(NodeId<Expr>),
}

impl Node for Expr {}

pub struct Transaction {
	statements: Option<NodeListId<TopLevelExpr>>,
	span: Span,
}
impl Node for Transaction {}

pub enum UseStatementKind {
	Namespace(NodeId<Ident>),
	NamespaceDatabase(NodeId<Ident>, NodeId<Ident>),
	Database(NodeId<Ident>),
}

pub struct UseStatement {
	pub kind: UseStatementKind,
	pub span: Span,
}
impl Node for UseStatement {}

pub enum TopLevelExpr {
	Transaction(NodeId<Transaction>),
	Use(NodeId<UseStatement>),
	Expr(NodeId<Expr>),
}
