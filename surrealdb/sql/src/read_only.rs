//! Whether a statement can run inside a read-only transaction.
//!
//! The predicate is consulted *before* a statement executes, to choose the
//! transaction mode it runs under. It therefore has to answer from the syntax
//! alone, with no catalog, no bound parameters and no runtime values.
//!
//! # The approximation is one-sided
//!
//! `read_only` **over-approximates**: it may call a statement writable that
//! turns out only to read, and that costs nothing but a write transaction. It
//! must never call a writing statement read-only — a mutation that reaches a
//! read-only transaction fails partway through, after the statement has
//! already started producing results.
//!
//! Every construct whose effect is invisible from the syntax therefore answers
//! `false`. A closure invoked through a value the classifier cannot see into
//! (`$fn(...)`, `value.unregistered(...)`), a custom or scripted function, a
//! module or silo call, and the `eval::*` / `api::invoke` builtins that
//! evaluate arbitrary nested queries are all writable as far as this walk is
//! concerned.
//!
//! # Keeping this in step with the expression layer
//!
//! `surrealdb-expr` carries the same predicate over its own tree, and the two
//! enums evolve separately. The equivalence test in
//! `surrealdb-core`'s `dbs::read_only_test` asserts that for every fixture the
//! surface AST and the expression it converts to answer identically, which is
//! what catches a variant added to one side and not the other.

use std::ops::Bound;

use crate::ast::TopLevelExpr;
use crate::block::Block;
use crate::closure::Closure;
use crate::expression::Expr;
use crate::fetch::{Fetch, Fetchs};
use crate::field::{Field, Fields, Selector};
use crate::function::{Function, FunctionCall};
use crate::group::{Group, Groups};
use crate::idiom::Idiom;
use crate::limit::Limit;
use crate::literal::Literal;
use crate::lookup::{Lookup, LookupSubject};
use crate::method::is_builtin_method;
use crate::operator::PostfixOperator;
use crate::order::{OrderList, Ordering};
use crate::part::{DestructurePart, Part, RecurseInstruction};
use crate::record_id::{RecordIdKeyLit, RecordIdKeyRangeLit, RecordIdLit};
use crate::split::{Split, Splits};
use crate::start::Start;
use crate::statements::{
	ForeachStatement, IfelseStatement, InfoStatement, OutputStatement, SelectStatement,
	SetStatement,
};

impl TopLevelExpr {
	/// Whether this statement can run inside a read-only transaction.
	///
	/// See the [module docs](self) for the one-sided contract this upholds.
	pub fn read_only(&self) -> bool {
		match self {
			TopLevelExpr::Begin
			| TopLevelExpr::Cancel
			| TopLevelExpr::Commit
			| TopLevelExpr::Show(_) => true,
			TopLevelExpr::Kill(_)
			| TopLevelExpr::Live(_)
			| TopLevelExpr::Option(_)
			| TopLevelExpr::Use(_)
			| TopLevelExpr::Access(_) => false,
			TopLevelExpr::Expr(expr) => expr.read_only(),
		}
	}
}

impl Expr {
	/// Whether evaluating this expression does only reads.
	///
	/// See the [module docs](self) for the one-sided contract this upholds.
	pub fn read_only(&self) -> bool {
		match self {
			Expr::Param(_)
			| Expr::Table(_)
			| Expr::Mock(_)
			| Expr::Constant(_)
			| Expr::Break
			| Expr::Continue
			| Expr::Sleep(_) => true,

			Expr::Info(info) => info.read_only(),

			// Composite literals evaluate their element expressions in place.
			Expr::Literal(l) => l.read_only(),

			Expr::Idiom(x) => x.read_only(),
			Expr::Block(block) => block.read_only(),
			Expr::Prefix {
				expr,
				..
			} => expr.read_only(),
			Expr::Postfix {
				expr,
				op,
			} => {
				match op {
					// Calling a closure executes a body this expression can only
					// see when the target is a closure literal; anything else
					// (a param, a field) must over-approximate to writable.
					PostfixOperator::Call(args) => {
						matches!(&**expr, Expr::Closure(_))
							&& expr.read_only() && args.iter().all(Expr::read_only)
					}
					// A registered method dispatches to a builtin, so only the
					// receiver and arguments carry user expressions. Any other
					// name invokes a closure stored on the receiver — a runtime
					// value whose body is invisible here — and must
					// over-approximate to writable, mirroring `Part::Method`.
					PostfixOperator::MethodCall(name, args) => {
						is_builtin_method(name)
							&& expr.read_only() && args.iter().all(Expr::read_only)
					}
					PostfixOperator::Range | PostfixOperator::RangeSkip => expr.read_only(),
				}
			}
			Expr::Binary {
				left,
				right,
				..
			} => left.read_only() && right.read_only(),
			Expr::FunctionCall(function) => function.read_only(),
			Expr::Return(stmt) => stmt.read_only(),
			Expr::Throw(expr) => expr.read_only(),
			Expr::IfElse(s) => s.read_only(),
			Expr::Select(s) => s.read_only(),
			Expr::Let(s) => s.read_only(),
			Expr::Foreach(s) => s.read_only(),
			Expr::Explain {
				statement,
				..
			} => statement.read_only(),
			// A closure literal is inert as a value, but wherever it can flow
			// it can also be invoked (call operator, closure-taking builtins),
			// so a writing body makes the expression writable.
			Expr::Closure(c) => c.read_only(),
			Expr::Create(_)
			| Expr::Update(_)
			| Expr::Delete(_)
			| Expr::Relate(_)
			| Expr::Insert(_)
			| Expr::Define(_)
			| Expr::Remove(_)
			| Expr::Rebuild(_)
			| Expr::Upsert(_)
			| Expr::Alter(_) => false,
		}
	}
}

impl Closure {
	fn read_only(&self) -> bool {
		self.body.read_only()
	}
}

impl Block {
	pub fn read_only(&self) -> bool {
		self.0.iter().all(|x| x.read_only())
	}
}

impl Literal {
	pub fn read_only(&self) -> bool {
		match self {
			Literal::None
			| Literal::Null
			| Literal::UnboundedRange
			| Literal::Bool(_)
			| Literal::Float(_)
			| Literal::Integer(_)
			| Literal::Decimal(_)
			| Literal::String(_)
			| Literal::Bytes(_)
			| Literal::Regex(_)
			| Literal::Duration(_)
			| Literal::Datetime(_)
			| Literal::Uuid(_)
			| Literal::File(_)
			| Literal::Geometry(_) => true,
			Literal::RecordId(record_id_lit) => record_id_lit.read_only(),
			Literal::Array(exprs) => exprs.iter().all(|x| x.read_only()),
			Literal::Set(exprs) => exprs.iter().all(|x| x.read_only()),
			Literal::Object(items) => items.iter().all(|x| x.value.read_only()),
		}
	}
}

impl RecordIdLit {
	pub fn read_only(&self) -> bool {
		self.key.read_only()
	}
}

impl RecordIdKeyLit {
	pub fn read_only(&self) -> bool {
		match self {
			RecordIdKeyLit::Number(_)
			| RecordIdKeyLit::String(_)
			| RecordIdKeyLit::Uuid(_)
			| RecordIdKeyLit::Generate(_) => true,
			RecordIdKeyLit::Range(record_id_key_range_lit) => record_id_key_range_lit.read_only(),
			RecordIdKeyLit::Array(exprs) => exprs.iter().all(|x| x.read_only()),
			RecordIdKeyLit::Object(items) => items.iter().all(|x| x.value.read_only()),
		}
	}
}

impl RecordIdKeyRangeLit {
	pub fn read_only(&self) -> bool {
		let bound_read_only = |bound: &Bound<RecordIdKeyLit>| match bound {
			Bound::Included(x) | Bound::Excluded(x) => x.read_only(),
			Bound::Unbounded => true,
		};
		bound_read_only(&self.start) && bound_read_only(&self.end)
	}
}

impl Idiom {
	pub fn read_only(&self) -> bool {
		self.0.iter().all(|v| v.read_only())
	}
}

impl Part {
	pub fn read_only(&self) -> bool {
		match self {
			Part::Start(v) => v.read_only(),
			Part::Where(v) => v.read_only(),
			Part::Value(v) => v.read_only(),
			// A builtin method writes nothing beyond what its arguments
			// carry, but an unregistered name invokes a closure stored on
			// the receiver — a runtime value whose body is invisible here —
			// so it must over-approximate to writable.
			Part::Method(name, v) => is_builtin_method(name) && v.iter().all(Expr::read_only),
			Part::Graph(v) => v.read_only(),
			Part::Destructure(v) => v.iter().all(DestructurePart::read_only),
			Part::Recurse(_, alias, instruction) => {
				alias.as_ref().map(|x| x.read_only()).unwrap_or(true)
					&& instruction.as_ref().map(|x| x.read_only()).unwrap_or(true)
			}
			Part::All
			| Part::Flatten
			| Part::Last
			| Part::First
			| Part::Field(_)
			| Part::Optional
			| Part::Doc
			| Part::RepeatRecurse => true,
		}
	}
}

impl DestructurePart {
	pub fn read_only(&self) -> bool {
		match self {
			DestructurePart::All(_) | DestructurePart::Field(_) => true,
			DestructurePart::Aliased(_, v) => v.read_only(),
			DestructurePart::Destructure(_, v) => v.iter().all(DestructurePart::read_only),
		}
	}
}

impl RecurseInstruction {
	pub fn read_only(&self) -> bool {
		match self {
			RecurseInstruction::Path {
				..
			}
			| RecurseInstruction::Collect {
				..
			} => true,
			RecurseInstruction::Shortest {
				expects,
				..
			} => expects.read_only(),
		}
	}
}

impl Lookup {
	pub fn read_only(&self) -> bool {
		self.what.iter().all(LookupSubject::read_only)
			&& self.expr.as_ref().map(|x| x.read_only()).unwrap_or(true)
			&& self.cond.as_ref().map(|x| x.0.read_only()).unwrap_or(true)
			&& self.split.as_ref().map(|x| x.read_only()).unwrap_or(true)
			&& self.group.as_ref().map(|x| x.read_only()).unwrap_or(true)
			&& self.order.as_ref().map(|x| x.read_only()).unwrap_or(true)
			&& self.limit.as_ref().map(|x| x.read_only()).unwrap_or(true)
			&& self.start.as_ref().map(|x| x.read_only()).unwrap_or(true)
			&& self.alias.as_ref().map(|x| x.read_only()).unwrap_or(true)
	}
}

impl LookupSubject {
	/// A range subject's bounds are record-id keys, which carry arbitrary
	/// expressions in their array and object forms.
	pub fn read_only(&self) -> bool {
		match self {
			LookupSubject::Table {
				..
			} => true,
			LookupSubject::Range {
				range,
				..
			} => range.read_only(),
		}
	}
}

/// `INFO` reads catalog metadata, but its subject and `VERSION` clause are
/// expressions the statement evaluates first, so a write reaches the
/// transaction through them.
impl InfoStatement {
	pub fn read_only(&self) -> bool {
		match self {
			InfoStatement::Root(_, version)
			| InfoStatement::Ns(_, version)
			| InfoStatement::Db(_, version) => version.as_ref().is_none_or(|version| version.read_only()),
			InfoStatement::Tb(table, _, version) => {
				table.read_only() && version.as_ref().is_none_or(|version| version.read_only())
			}
			InfoStatement::User(user, _, _) => user.read_only(),
			InfoStatement::Index(index, table, _) => index.read_only() && table.read_only(),
		}
	}
}

impl Function {
	pub fn read_only(&self) -> bool {
		match self {
			Self::Custom(_)
			| Self::Script(_)
			| Self::Module(_, _)
			| Self::Silo {
				..
			} => false,
			Self::Normal(f) => !crate::method::is_writer_builtin(f),
			Self::Model(_) => true,
		}
	}
}

impl FunctionCall {
	pub fn read_only(&self) -> bool {
		self.receiver.read_only() && self.arguments.iter().all(|x| x.read_only())
	}
}

impl Fields {
	pub fn read_only(&self) -> bool {
		match self {
			Fields::Value(field) => field.read_only(),
			Fields::Select(fields) => fields.iter().all(|x| x.read_only()),
		}
	}
}

impl Field {
	pub fn read_only(&self) -> bool {
		match self {
			Field::All => true,
			Field::Single(x) => x.read_only(),
		}
	}
}

impl Selector {
	pub fn read_only(&self) -> bool {
		self.expr.read_only()
	}
}

impl Fetchs {
	pub fn read_only(&self) -> bool {
		self.0.iter().all(|x| x.read_only())
	}
}

impl Fetch {
	pub fn read_only(&self) -> bool {
		self.0.read_only()
	}
}

impl Groups {
	pub fn read_only(&self) -> bool {
		self.0.iter().all(|x| x.read_only())
	}
}

impl Group {
	pub fn read_only(&self) -> bool {
		self.0.read_only()
	}
}

impl Splits {
	pub fn read_only(&self) -> bool {
		self.0.iter().all(|x| x.read_only())
	}
}

impl Split {
	pub fn read_only(&self) -> bool {
		self.0.read_only()
	}
}

impl Ordering {
	pub fn read_only(&self) -> bool {
		match self {
			Ordering::Random => true,
			Ordering::Order(list) => list.read_only(),
		}
	}
}

impl OrderList {
	pub fn read_only(&self) -> bool {
		self.0.iter().all(|x| x.value.read_only())
	}
}

impl Limit {
	pub fn read_only(&self) -> bool {
		self.0.read_only()
	}
}

impl Start {
	pub fn read_only(&self) -> bool {
		self.0.read_only()
	}
}

impl SelectStatement {
	pub fn read_only(&self) -> bool {
		!self.for_update
			&& self.fields.read_only()
			&& self.omit.iter().all(|v| v.read_only())
			&& self.what.iter().all(|v| v.read_only())
			&& self.cond.as_ref().map(|x| x.0.read_only()).unwrap_or(true)
			&& self.split.as_ref().map(|x| x.read_only()).unwrap_or(true)
			&& self.group.as_ref().map(|x| x.read_only()).unwrap_or(true)
			&& self.order.as_ref().map(|x| x.read_only()).unwrap_or(true)
			&& self.limit.as_ref().map(|x| x.read_only()).unwrap_or(true)
			&& self.start.as_ref().map(|x| x.read_only()).unwrap_or(true)
			&& self.fetch.as_ref().map(|x| x.read_only()).unwrap_or(true)
			&& self.version.read_only()
			&& self.timeout.read_only()
	}
}

impl SetStatement {
	pub fn read_only(&self) -> bool {
		self.what.read_only()
	}
}

impl OutputStatement {
	pub fn read_only(&self) -> bool {
		self.what.read_only() && self.fetch.as_ref().map(|x| x.read_only()).unwrap_or(true)
	}
}

impl IfelseStatement {
	pub fn read_only(&self) -> bool {
		self.exprs.iter().all(|x| x.0.read_only() && x.1.read_only())
			&& self.close.as_ref().map(|x| x.read_only()).unwrap_or(true)
	}
}

impl ForeachStatement {
	pub fn read_only(&self) -> bool {
		self.range.read_only() && self.block.read_only()
	}
}
