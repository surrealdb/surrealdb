use common::ids::IdSet;
use common::span::Span;
use rust_decimal::Decimal;

mod mac;
mod types;
pub mod vis;
mod visit;

pub use types::{Node, NodeId, NodeList, NodeListId, Spanned};

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

		builtins: Vec<Spanned<Builtin>>,
		floats: Vec<Spanned<f64>>,
		integer: Vec<Integer>,
		decimals: Vec<Spanned<Decimal>>,

		binary: Vec<BinaryExpr>,
		postfix: Vec<PostfixExpr>,
		prefix: Vec<PrefixExpr>,

		path: Vec<NodeListId<Ident>>,
		params: Vec<Ident>,
		idents: Vec<Ident>,
		#[set]
		strings: NodeSet<String>,
	}
}

pub type Ast = types::Ast<Library>;

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
		pub left: NodeId<Expr>,
	}
}

ast_type! {
	#[derive(Copy, Clone)]
	pub enum Expr {
		Covered(NodeId<Expr>),

		Builtin(NodeId<Builtin>),
		Float(NodeId<Spanned<f64>>),
		Integer(NodeId<Integer>),

		Path(NodeListId<Ident>),
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
