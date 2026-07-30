use revision::{DeserializeRevisioned, Revisioned, SerializeRevisioned};
use surrealdb_types::{RecordId, SqlFormat, ToSql};

use super::SleepStatement;
use crate::expr::closure::ClosureExpr;
use crate::expr::statements::info::InfoStructure;
use crate::expr::statements::{
	AlterStatement, CreateStatement, DefineStatement, DeleteStatement, ForeachStatement,
	IfelseStatement, InfoStatement, InsertStatement, OutputStatement, RebuildStatement,
	RelateStatement, RemoveStatement, SelectStatement, SetStatement, UpdateStatement,
	UpsertStatement,
};
use crate::expr::{
	BinaryOperator, Block, Constant, FunctionCall, Idiom, Literal, Mock, ObjectEntry, Param,
	PostfixOperator, PrefixOperator, RecordIdKeyLit, RecordIdLit,
};
use crate::types::PublicValue;
use crate::val::table_name_public::IntoTableName;
use crate::val::{TableName, Value};

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug, Default)]
pub enum ExplainFormat {
	#[default]
	Text,
	Json,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Expr {
	Literal(Literal),
	Param(Param),
	Idiom(Idiom),
	// Maybe move into Literal?
	Table(TableName),
	// This type can probably be removed in favour of range expressions.
	Mock(Mock),
	Block(Box<Block>),
	Constant(Constant),
	Prefix {
		op: PrefixOperator,
		expr: Box<Expr>,
	},
	Postfix {
		expr: Box<Expr>,
		op: PostfixOperator,
	},
	Binary {
		left: Box<Expr>,
		op: BinaryOperator,
		right: Box<Expr>,
	},
	// TODO: Factor out the call from the function expression.
	FunctionCall(Box<FunctionCall>),

	Closure(Box<ClosureExpr>),

	Break,
	Continue,
	Return(Box<OutputStatement>),
	Throw(Box<Expr>),

	IfElse(Box<IfelseStatement>),
	Select(Box<SelectStatement>),
	Create(Box<CreateStatement>),
	Update(Box<UpdateStatement>),
	Upsert(Box<UpsertStatement>),
	Delete(Box<DeleteStatement>),
	Relate(Box<RelateStatement>),
	Insert(Box<InsertStatement>),
	Define(Box<DefineStatement>),
	Remove(Box<RemoveStatement>),
	Rebuild(Box<RebuildStatement>),
	Alter(Box<AlterStatement>),
	Info(Box<InfoStatement>),
	Foreach(Box<ForeachStatement>),
	Let(Box<SetStatement>),
	Sleep(Box<SleepStatement>),
	Explain {
		format: ExplainFormat,
		analyze: bool,
		statement: Box<Expr>,
	},
	/// An GQL `MATCH` query, lowered to its declarative binding-table plan.
	///
	/// Only constructed by the GQL lowering at top level. It runs exclusively
	/// under the streaming execution planner; see the `compute` and
	/// `From<expr::Expr> for sql::Expr` arms for the invariants it relies on.
	// Constructed by the GQL lowering, which lands as a sibling piece of PR-A.
	#[cfg(feature = "gql")]
	#[allow(dead_code)]
	Match(Box<crate::expr::match_plan::MatchPlan>),
}

impl Expr {
	/// Check if this expression does only reads.
	pub fn read_only(&self) -> bool {
		match self {
			Expr::Literal(_)
			| Expr::Param(_)
			| Expr::Table(_)
			| Expr::Mock(_)
			| Expr::Constant(_)
			| Expr::Break
			| Expr::Continue
			| Expr::Info(_)
			| Expr::Sleep(_) => true,

			Expr::Idiom(x) => x.read_only(),
			Expr::Block(block) => block.read_only(),
			Expr::Prefix {
				expr,
				..
			}
			| Expr::Postfix {
				expr,
				..
			} => expr.read_only(),
			Expr::Binary {
				left,
				right,
				..
			} => left.read_only() && right.read_only(),
			Expr::FunctionCall(function) => function.read_only(),
			Expr::Return(expr) => expr.read_only(),
			Expr::Throw(expr) => expr.read_only(),
			Expr::IfElse(s) => s.read_only(),
			Expr::Select(s) => s.read_only(),
			Expr::Let(s) => s.read_only(),
			Expr::Foreach(s) => s.read_only(),
			Expr::Explain {
				statement,
				..
			} => statement.read_only(),
			Expr::Closure(_) => true,
			// A GQL query is read-only unless it carries mutation stages; a
			// mutation-bearing plan must run under a write transaction.
			#[cfg(feature = "gql")]
			Expr::Match(plan) => !plan.has_mutations(),
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

	/// Check whether this expression's own tree *directly* contains a
	/// data-modifying statement (CREATE/UPDATE/DELETE/RELATE/INSERT/UPSERT or
	/// DDL).
	///
	/// Used to reject side-effecting `PERMISSIONS` clauses at definition time
	/// (GHSA-66r2-5gwj-gxm2). Unlike [`Expr::read_only`], a function or closure
	/// *call* is treated as opaque — a custom function may itself write, but
	/// that is enforced at runtime via `Options::new_for_permission_predicate`
	/// — so common read-only helper predicates such as
	/// `PERMISSIONS WHERE fn::is_owner()` remain valid. Writes buried inside a
	/// subquery or idiom are likewise left to the runtime guard.
	pub fn has_direct_write(&self) -> bool {
		match self {
			// Data-modifying statements: a direct write.
			Expr::Create(_)
			| Expr::Update(_)
			| Expr::Delete(_)
			| Expr::Relate(_)
			| Expr::Insert(_)
			| Expr::Define(_)
			| Expr::Remove(_)
			| Expr::Rebuild(_)
			| Expr::Upsert(_)
			| Expr::Alter(_) => true,

			// Combinators: recurse into nested expressions.
			Expr::Prefix {
				expr,
				..
			}
			| Expr::Postfix {
				expr,
				..
			}
			| Expr::Throw(expr) => expr.has_direct_write(),
			Expr::Binary {
				left,
				right,
				..
			} => left.has_direct_write() || right.has_direct_write(),
			Expr::Return(s) => s.what.has_direct_write(),
			Expr::Let(s) => s.what.has_direct_write(),
			Expr::Block(block) => block.has_direct_write(),
			Expr::IfElse(s) => s.has_direct_write(),
			Expr::Foreach(s) => s.has_direct_write(),
			Expr::Explain {
				statement,
				..
			} => statement.has_direct_write(),
			// Call arguments are evaluated in place, so inspect them; the callee
			// body is opaque and handled by the runtime guard.
			Expr::FunctionCall(function) => function.arguments.iter().any(|x| x.has_direct_write()),

			// Leaves, closures, subqueries and idioms contain no *direct* write.
			_ => false,
		}
	}

	pub fn from_public_value(value: PublicValue) -> Self {
		match value {
			surrealdb_types::Value::None => Expr::Literal(Literal::None),
			surrealdb_types::Value::Null => Expr::Literal(Literal::Null),
			surrealdb_types::Value::Bool(b) => Expr::Literal(Literal::Bool(b)),
			surrealdb_types::Value::Number(n) => match n {
				surrealdb_types::Number::Int(i) => Expr::Literal(Literal::Integer(i)),
				surrealdb_types::Number::Float(f) => Expr::Literal(Literal::Float(f)),
				surrealdb_types::Number::Decimal(d) => Expr::Literal(Literal::Decimal(d)),
			},
			surrealdb_types::Value::String(s) => Expr::Literal(Literal::String(s.into())),
			surrealdb_types::Value::Bytes(b) => {
				Expr::Literal(Literal::Bytes(crate::val::Bytes(b.into_inner())))
			}
			surrealdb_types::Value::Duration(d) => {
				Expr::Literal(Literal::Duration(crate::val::Duration(d.into_inner())))
			}
			surrealdb_types::Value::Datetime(d) => {
				Expr::Literal(Literal::Datetime(crate::val::Datetime(d.into_inner())))
			}
			surrealdb_types::Value::Uuid(u) => {
				Expr::Literal(Literal::Uuid(crate::val::Uuid(u.into_inner())))
			}
			surrealdb_types::Value::Array(a) => {
				Expr::Literal(Literal::Array(a.into_iter().map(Expr::from_public_value).collect()))
			}
			surrealdb_types::Value::Set(s) => {
				Expr::Literal(Literal::Array(s.into_iter().map(Expr::from_public_value).collect()))
			}
			surrealdb_types::Value::Object(o) => Expr::Literal(Literal::Object(
				o.into_iter()
					.map(|(k, v)| ObjectEntry {
						key: k.into(),
						value: Expr::from_public_value(v),
					})
					.collect(),
			)),
			surrealdb_types::Value::Table(t) => Expr::Table(t.into_table_name()),
			surrealdb_types::Value::RecordId(RecordId {
				table,
				key,
			}) => {
				let key_lit = match key {
					surrealdb_types::RecordIdKey::Number(n) => RecordIdKeyLit::Number(n),
					surrealdb_types::RecordIdKey::String(s) => RecordIdKeyLit::String(s.into()),
					surrealdb_types::RecordIdKey::Uuid(u) => {
						RecordIdKeyLit::Uuid(crate::val::Uuid(u.into_inner()))
					}
					surrealdb_types::RecordIdKey::Array(a) => {
						RecordIdKeyLit::Array(a.into_iter().map(Expr::from_public_value).collect())
					}
					surrealdb_types::RecordIdKey::Object(o) => RecordIdKeyLit::Object(
						o.into_iter()
							.map(|(k, v)| ObjectEntry {
								key: k.into(),
								value: Expr::from_public_value(v),
							})
							.collect(),
					),
					_ => return Expr::Literal(Literal::None), // For unsupported key types
				};
				Expr::Literal(Literal::RecordId(RecordIdLit {
					table: table.into_table_name(),
					key: key_lit,
				}))
			}
			surrealdb_types::Value::Geometry(g) => Expr::Literal(Literal::Geometry(g.into())),
			surrealdb_types::Value::File(f) => {
				Expr::Literal(Literal::File(crate::val::File::new(f.bucket, f.key)))
			}
			surrealdb_types::Value::Range(r) => Expr::from(*r),
			surrealdb_types::Value::Regex(r) => {
				Expr::Literal(Literal::Regex(crate::val::Regex(r.into_inner())))
			}
		}
	}
}

impl From<surrealdb_types::Range> for Expr {
	fn from(r: surrealdb_types::Range) -> Self {
		use std::ops::Bound;
		match r.into_inner() {
			// Unbounded range: ..
			(Bound::Unbounded, Bound::Unbounded) => Expr::Literal(Literal::UnboundedRange),
			// Prefix ranges: ..end or ..=end
			(Bound::Unbounded, Bound::Excluded(end)) => Expr::Prefix {
				op: PrefixOperator::Range,
				expr: Box::new(Expr::from_public_value(end)),
			},
			(Bound::Unbounded, Bound::Included(end)) => Expr::Prefix {
				op: PrefixOperator::RangeInclusive,
				expr: Box::new(Expr::from_public_value(end)),
			},
			// Binary ranges with inclusive start
			(Bound::Included(start), Bound::Excluded(end)) => Expr::Binary {
				left: Box::new(Expr::from_public_value(start)),
				op: BinaryOperator::Range,
				right: Box::new(Expr::from_public_value(end)),
			},
			(Bound::Included(start), Bound::Included(end)) => Expr::Binary {
				left: Box::new(Expr::from_public_value(start)),
				op: BinaryOperator::RangeInclusive,
				right: Box::new(Expr::from_public_value(end)),
			},
			// Binary ranges with excluded start (skip)
			(Bound::Excluded(start), Bound::Excluded(end)) => Expr::Binary {
				left: Box::new(Expr::from_public_value(start)),
				op: BinaryOperator::RangeSkip,
				right: Box::new(Expr::from_public_value(end)),
			},
			(Bound::Excluded(start), Bound::Included(end)) => Expr::Binary {
				left: Box::new(Expr::from_public_value(start)),
				op: BinaryOperator::RangeSkipInclusive,
				right: Box::new(Expr::from_public_value(end)),
			},
			// Invalid ranges with unbounded start but bounded in a way we can't represent
			// start>.. (excluded start with no end) - not valid in SurrealQL
			(Bound::Excluded(_), Bound::Unbounded) | (Bound::Included(_), Bound::Unbounded) => {
				Expr::Literal(Literal::None)
			}
		}
	}
}

impl Expr {
	/// Checks if a expression is 'pure' i.e. does not rely on the environment.
	pub fn is_static(&self) -> bool {
		match self {
			Expr::Literal(literal) => literal.is_static(),
			Expr::Constant(_) => true,
			Expr::Prefix {
				expr,
				..
			} => expr.is_static(),
			Expr::Postfix {
				expr,
				..
			} => expr.is_static(),
			Expr::Binary {
				left,
				right,
				..
			} => left.is_static() && right.is_static(),
			Expr::FunctionCall(x) => {
				// This is not correct as functions like http::get are not 'pure' but this is
				// replicating previous behavior.
				//
				// FIXME: Fix this discrepency and weird static/non-static behavior.
				x.arguments.iter().all(|x| x.is_static())
			}
			Expr::Param(_)
			| Expr::Idiom(_)
			| Expr::Table(_)
			| Expr::Mock(_)
			| Expr::Block(_)
			| Expr::Closure(_)
			| Expr::Break
			| Expr::Continue
			| Expr::Return(_)
			| Expr::Throw(_)
			| Expr::IfElse(_)
			| Expr::Select(_)
			| Expr::Create(_)
			| Expr::Update(_)
			| Expr::Delete(_)
			| Expr::Relate(_)
			| Expr::Insert(_)
			| Expr::Define(_)
			| Expr::Remove(_)
			| Expr::Rebuild(_)
			| Expr::Upsert(_)
			| Expr::Alter(_)
			| Expr::Info(_)
			| Expr::Foreach(_)
			| Expr::Let(_)
			| Expr::Sleep(_)
			| Expr::Explain {
				..
			} => false,
			// GQL MATCH reads from the datastore, so it is never static.
			#[cfg(feature = "gql")]
			Expr::Match(_) => false,
		}
	}

	pub fn to_idiom(&self) -> Idiom {
		match self {
			Expr::Idiom(i) => i.simplify(),
			Expr::Param(i) => Idiom::field(i.clone().into_strand()),
			Expr::FunctionCall(x) => x.receiver.to_idiom(),
			Expr::Literal(l) => match l {
				Literal::String(s) => Idiom::field(s.clone()),
				Literal::Datetime(d) => Idiom::field(d.to_string()),
				x => Idiom::field(x.to_sql()),
			},
			x => Idiom::field(x.to_sql()),
		}
	}

	pub fn to_raw_string(&self) -> String {
		match self {
			Expr::Idiom(idiom) => idiom.to_raw_string(),
			Expr::Table(ident) => ident.as_str().to_string(),
			_ => self.to_sql(),
		}
	}

	// NOTE: Changes to this function also likely require changes to
	// crate::sql::Expr::needs_parentheses
	/// Returns if this expression needs to be parenthesized when inside another expression.
	#[allow(dead_code)]
	fn needs_parentheses(&self) -> bool {
		match self {
			Expr::Literal(Literal::UnboundedRange | Literal::RecordId(_))
			| Expr::Closure(_)
			| Expr::Break
			| Expr::Continue
			| Expr::Throw(_)
			| Expr::Return(_)
			| Expr::IfElse(_)
			| Expr::Select(_)
			| Expr::Create(_)
			| Expr::Update(_)
			| Expr::Delete(_)
			| Expr::Relate(_)
			| Expr::Insert(_)
			| Expr::Define(_)
			| Expr::Remove(_)
			| Expr::Rebuild(_)
			| Expr::Upsert(_)
			| Expr::Alter(_)
			| Expr::Info(_)
			| Expr::Foreach(_)
			| Expr::Let(_)
			| Expr::Sleep(_)
			| Expr::Explain {
				..
			} => true,

			// GQL MATCH renders as a multi-clause statement; parenthesize it
			// when nested.
			#[cfg(feature = "gql")]
			Expr::Match(_) => true,

			Expr::Literal(_)
			| Expr::Param(_)
			| Expr::Idiom(_)
			| Expr::Table(_)
			| Expr::Mock(_)
			| Expr::Block(_)
			| Expr::Constant(_)
			| Expr::Prefix {
				..
			}
			| Expr::Postfix {
				..
			}
			| Expr::Binary {
				..
			}
			| Expr::FunctionCall(_) => false,
		}
	}
}

impl ToSql for Expr {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		// `Expr::Match` cannot round-trip through `sql::Expr` (it has no SurrealQL
		// surface). Render it directly via the dedicated `MatchPlan` renderer
		// before the conversion would replace it with a placeholder.
		#[cfg(feature = "gql")]
		if let Expr::Match(plan) = self {
			plan.fmt_sql(f, fmt);
			return;
		}
		let sql_expr: crate::sql::Expr = self.clone().into();
		sql_expr.fmt_sql(f, fmt);
	}
}

impl Expr {
	/// Renders the canonical SurrealQL text a catalog definition stores for
	/// this expression (e.g. a `StoredFieldDefinition.value`), matching exactly
	/// what the old `sql::Expr`-embedding definition used to render when
	/// nested after a clause keyword (`VALUE`, `ASSERT`, `WHEN`, ...) —
	/// including the parenthesization `CoverStmts` applies to statement-shaped
	/// sub-expressions (e.g. a nested `SELECT`), so the stored text is safe to
	/// splice back in after that keyword with no further wrapping.
	///
	/// `Expr::Match` never reaches a stored definition (see `ToSql`'s carve-out
	/// above), so unlike `ToSql::fmt_sql` this does not special-case it.
	pub fn to_stored_sql(&self) -> String {
		let sql_expr: crate::sql::Expr = self.clone().into();
		crate::sql::CoverStmts(&sql_expr).to_sql()
	}
}

impl InfoStructure for Expr {
	fn structure(self) -> Value {
		self.to_sql().into()
	}
}

impl Revisioned for Expr {
	fn revision() -> u16 {
		1
	}
}

impl SerializeRevisioned for Expr {
	fn serialize_revisioned<W: std::io::Write>(
		&self,
		writer: &mut W,
	) -> Result<(), revision::Error> {
		// `Expr::Match` renders GQL-ish text via `to_sql()`, which the
		// SurrealQL-only `deserialize_revisioned` path below cannot round-trip.
		// The invariant (V2_DESIGN §2; SECURITY_GUIDE §15a) is that `Expr::Match`
		// is only ever the top-level expr of a `PreparedGqlQuery` consumed
		// directly by the planner — it never nests into a `sql::Ast`, the
		// catalog, or a cached/revisioned `Expr`, so this path is unreachable by
		// construction. Mirror the `From<expr::Expr> for sql::Expr` arm and fail
		// loud (in debug) rather than silently emit unparseable bytes, so a
		// future regression that nests `Expr::Match` is caught here.
		#[cfg(feature = "gql")]
		if matches!(self, Expr::Match(_)) {
			tracing::error!(
				"Expr::Match reached Revisioned serialization; it must never enter a \
				 sql::Ast, the catalog, or Revisioned serialization"
			);
			debug_assert!(false, "Expr::Match must not be Revisioned-serialized");
		}
		SerializeRevisioned::serialize_revisioned(&self.to_sql(), writer)
	}
}

impl DeserializeRevisioned for Expr {
	fn deserialize_revisioned<R: std::io::Read>(reader: &mut R) -> Result<Self, revision::Error> {
		let query: String = DeserializeRevisioned::deserialize_revisioned(reader)?;

		let expr = crate::syn::parse_with_settings(
			query.as_bytes(),
			// The wire format is engine-rendered SurrealQL text; see the
			// constant for why decode is capability- and limit-independent.
			crate::syn::parser::ParserSettings::STORED_TEXT,
			// The whole of the stored text must be consumed, for the reason
			// `syn::expr_for_definition` documents: the Pratt parser stops at
			// the first token it cannot continue with, so trailing content
			// would decode to a prefix and be evaluated as if that were the
			// stored expression.
			async |p, stk| {
				let expr = p.parse_expr(stk).await?;
				p.assert_finished()?;
				Ok(expr)
			},
		)
		.map_err(|err| revision::Error::Conversion(err.to_string()))?;
		Ok(expr.into())
	}
}

impl revision::SkipRevisioned for Expr {
	fn skip_revisioned<R: std::io::Read>(reader: &mut R) -> Result<(), revision::Error> {
		// Wire format is the SurrealQL source string. Skip its bytes without
		// re-parsing into an `Expr`.
		<String as revision::SkipRevisioned>::skip_revisioned(reader)
	}
}

impl revision::WalkRevisioned for Expr {
	type Walker<'r, R: revision::BorrowedReader + 'r> = revision::LeafWalker<'r, Expr, R>;

	fn walk_revisioned<'r, R: revision::BorrowedReader>(
		reader: &'r mut R,
	) -> Result<Self::Walker<'r, R>, revision::Error> {
		Ok(revision::LeafWalker::new(reader))
	}
}

impl revision::LengthPrefixedBytes for Expr {}

#[cfg(test)]
mod length_prefixed_bytes_tests {
	use revision::{SerializeRevisioned, WalkRevisioned};
	use surrealdb_types::ToSql;

	use crate::expr::Expr;
	use crate::expr::literal::Literal;

	#[test]
	fn expr_with_bytes_matches_serialize() {
		let expr = Expr::Literal(Literal::Integer(42));
		let mut bytes = Vec::new();
		expr.serialize_revisioned(&mut bytes).unwrap();
		let wire_text = expr.to_sql();
		let mut r = bytes.as_slice();
		let walker = Expr::walk_revisioned(&mut r).unwrap();
		let observed = walker.with_bytes(|raw| raw.to_vec()).unwrap();
		assert_eq!(observed.as_slice(), wire_text.as_bytes());
		assert!(r.is_empty());
	}
}
