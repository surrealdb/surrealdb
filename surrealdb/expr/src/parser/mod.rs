//! Compiles parser-next's flat tree into logical plans and native payloads.
//!
//! Requests and stored clauses lower without an intermediate SQL AST. Request
//! passwords remain inert until execution prepares credentials; stored-text
//! entry points prepare them for the catalog's durable representation.
//!
//! [`api`] holds the entry points (`execution_query_with_settings`,
//! `parse`, `expr_for_definition`, `value`,
//! …) with their [`ParserSettings`] and [`ParseError`]; [`StatementStream`]
//! materializes statements incrementally. Underneath, [`entry`] parses with
//! parser-next and lowers the tree, and [`api::render`] words a rejection.
//!
//! Leaves are decoded here, not in the parser: numbers, durations, strings,
//! datetimes, uuids, bytes, regexes, files, builtin paths and kinds. So are
//! the semantic checks a flat tree cannot make on its own, such as builtin
//! names and `GROUP BY` against the selection.
//!
//! <section class="warning">
//! <h3>Unstable!</h3>
//! This crate is <b>SurrealDB internal API</b>. It does not adhere to SemVer and its API is
//! free to change and break code even between patch versions. If you are looking for a stable
//! interface to the SurrealDB library please have a look at
//! <a href="https://crates.io/crates/surrealdb">the Rust SDK</a>.
//! </section>

use std::cell::Cell;
use std::ops::Bound;
use std::sync::atomic::{self, AtomicU32};
use std::time::Duration;

use common::range::TypedRange;
use surrealdb_parser_next::ast::{self, Ast, Binary, NONE, Node, NodeId, Prefix, Tag};
use surrealdb_parser_next::decode;
use surrealdb_parser_next::kw::Kw;
use surrealdb_parser_next::lex::{Lexer, T, Tok, decode_duration};
use surrealdb_strand::{Strand, TableName};
use surrealdb_types::ToSql;

use crate::expr::data::Assignment;
use crate::expr::field::Selector;
use crate::expr::kind::{GeometryKind, KindLiteral};
use crate::expr::literal::ObjectEntry;
use crate::expr::lookup::{LookupKind, LookupSubject};
use crate::expr::operator::{
	AssignOperator, BinaryOperator, BooleanOperator, MatchesOperator, NearestNeighbor,
	PostfixOperator, PrefixOperator,
};
use crate::expr::order::{Order, OrderList, Ordering};
use crate::expr::output::Output;
use crate::expr::part::{DestructurePart, Recurse, RecurseInstruction};
use crate::expr::statements::live::LiveFields;
use crate::expr::statements::rebuild::RebuildIndexStatement;
use crate::expr::statements::show::ShowSince;
use crate::expr::statements::{
	CreateStatement, DeleteStatement, ForeachStatement, IfelseStatement, InfoStatement,
	InsertStatement, KillStatement, LiveStatement, OptionStatement, OutputStatement,
	RebuildStatement, RelateStatement, SelectStatement, SetStatement, ShowStatement,
	SleepStatement, UpdateStatement, UpsertStatement, UseStatement,
};
use crate::expr::{
	Base, Block, ClosureExpr as Closure, Cond, Data, Dir, Explain, ExplainFormat, Expr, ExtPrefix,
	Fetch, Fetchs, Field, Fields, Function, FunctionCall, Group, Groups, Idiom, KeyNames, Kind,
	Limit, Literal, Lookup, Mock, Model, Param, Part, RecordIdKeyGen, RecordIdKeyLit,
	RecordIdKeyRangeLit, RecordIdLit, Script, Split, Splits, Start, With,
};

pub mod api;
mod execution;
mod query;
pub use execution::{ExecutionStatement, LogicalStatement, StatementKind, StatementText};
pub use query::{Query, TopLevelExpr};
mod define;
pub mod entry;
mod extension;
pub(crate) mod geometry;
pub mod lit;
mod logical;

pub mod diagnostic;
pub mod settings;
mod stream;
mod support;

pub use api::*;
pub use lit::{is_builtin, is_regex};
pub use settings::{ParserConfig, ParserSettings};
pub use stream::StatementStream;
pub mod value;

thread_local! {
	/// Whether builtin names are checked; the benchmark turns this off so
	/// that only the lowering itself is timed.
	pub static CHECK_NAMES: Cell<bool> = const { Cell::new(true) };
}

pub struct Lower<'a> {
	pub ast: &'a Ast,
	pub src: &'a str,
	pub opts: Options,
}

pub(crate) const FILES_DISABLED: &str = "the experimental files feature to be enabled";
pub(crate) const SURREALISM_DISABLED: &str = "Surrealism extensions are not enabled";

/// The deprecation warning for each spelling in `bits`, a set of
/// [`ast::ext`] bits. `MODULE` and `mod::` mean exactly what `EXTENSION` and
/// `ext::` mean, so they are reported and never refused. An unnamed silo
/// name (`silo::org::pkg::<v>`) is reported wherever it is accepted: in a
/// call, `ALTER` and `REMOVE` always, and in `DEFINE` only in an import; a
/// `DEFINE` outside one is refused.
pub fn deprecated_extension_spellings(bits: u32) -> impl Iterator<Item = &'static str> {
	[
		(ast::ext::MODULE_KEYWORD, "`MODULE` is deprecated, use `EXTENSION`"),
		(ast::ext::MOD_PREFIX, "The `mod::` prefix is deprecated, use `ext::`"),
		(
			ast::ext::SILO_NAME,
			"Addressing an unnamed silo extension as `silo::org::pkg<version>` is deprecated; \
			 define the package with a name (`DEFINE EXTENSION ext::name FROM silo::…`) and use \
			 `ext::name`",
		),
	]
	.into_iter()
	.filter(move |(bit, _)| bits & bit != 0)
	.map(|(_, message)| message)
}

/// The [`ast::ext`] bits already reported by this process.
static WARNED_EXTENSION_SPELLINGS: AtomicU32 = AtomicU32::new(0);

impl Lower<'_> {
	/// Logs the warning of each deprecated spelling in `bits` (see
	/// [`deprecated_extension_spellings`]) that this process has not reported
	/// yet, so a spelling is reported once however often statements using it,
	/// stored definitions among them, are lowered. `at` is the byte offset of
	/// the spelling in the source; the warning carries its line and column and
	/// no source text.
	pub(crate) fn warn_deprecated_extension(&self, bits: u32, at: u32) {
		if bits == 0 {
			return;
		}
		let new = bits & !WARNED_EXTENSION_SPELLINGS.fetch_or(bits, atomic::Ordering::Relaxed);
		if new == 0 {
			return;
		}
		let before = &self.src[..(at as usize).min(self.src.len())];
		let line = before.matches('\n').count() + 1;
		let column = before.rsplit('\n').next().map_or(0, |l| l.chars().count()) + 1;
		for message in deprecated_extension_spellings(new) {
			tracing::warn!(line, column, "{message}");
		}
	}
}

pub use support::{Options, Reject};
type R<T> = Result<T, Reject>;

impl<'a> Lower<'a> {
	/// A lowering of `ast` over `src` with every optional grammar enabled.
	pub fn new(ast: &'a Ast, src: &'a str) -> Self {
		Lower {
			ast,
			src,
			opts: Options::ALL,
		}
	}

	fn n(&self, id: NodeId) -> &'a Node {
		&self.ast.nodes[id as usize]
	}

	pub(crate) fn text(&self, id: NodeId) -> &'a str {
		Ast::text(self.n(id), self.src)
	}

	fn list(&self, idx: u32) -> &'a [u32] {
		self.ast.list(idx)
	}

	/// A parameter's name without the `$`, unescaped when quoted.
	fn param_name(&self, id: NodeId) -> R<String> {
		let t = &self.text(id)[1..];
		if t.starts_with('`') || t.starts_with('⟨') {
			let mut s = String::new();
			if !decode::unescape(decode::strip_delims(t), &mut s) {
				return Err(Reject::new("invalid escape"));
			}
			return Ok(s);
		}
		Ok(t.to_owned())
	}

	/// An identifier's name: unescaped when quoted.
	pub(crate) fn name(&self, id: NodeId) -> R<String> {
		let t = self.text(id);
		let quoted = t.starts_with(['`', '⟨', '\'', '"'])
			|| (t.starts_with('s') && t[1..].starts_with(['\'', '"']));
		if quoted {
			let mut s = String::new();
			if !decode::unescape(decode::strip_delims(t), &mut s) {
				return Err(Reject::new("invalid escape"));
			}
			Ok(s)
		} else {
			Ok(t.to_owned())
		}
	}

	/// A top-level statement.
	pub fn top(&self, id: NodeId) -> R<TopLevelExpr> {
		let n = self.n(id);
		Ok(match n.tag {
			Tag::Begin => TopLevelExpr::Begin,
			Tag::Cancel => TopLevelExpr::Cancel,
			Tag::Commit => TopLevelExpr::Commit,
			Tag::Use => TopLevelExpr::Use(match n.op {
				0 => UseStatement::Ns(self.expr(n.a)?),
				1 => UseStatement::Db(self.expr(n.a)?),
				2 => UseStatement::NsDb(self.expr(n.a)?, self.expr(n.b)?),
				_ => UseStatement::Default,
			}),
			Tag::OptionStmt => TopLevelExpr::Option(OptionStatement {
				name: Strand::new(self.name(n.a)?),
				what: n.op != 0,
			}),
			Tag::Kill => TopLevelExpr::Kill(KillStatement {
				id: self.expr(n.a)?,
			}),
			Tag::Live => {
				let rec = &self.ast.extra[n.a as usize..n.a as usize + 5];
				TopLevelExpr::Live(Box::new(LiveStatement {
					id: uuid::Uuid::nil(),
					node: uuid::Uuid::nil(),
					fields: match rec[4] {
						1 => LiveFields::Diff,
						2 => LiveFields::Select(Fields::Value(Box::new(
							self.selector(self.n(rec[0]))?,
						))),
						_ => LiveFields::Select(self.fields(rec[0])?),
					},
					what: self.expr(rec[1])?,
					cond: self.opt(rec[2])?.map(Cond),
					fetch: self.fetchs(rec[3])?,
				}))
			}
			Tag::Show => {
				let rec = &self.ast.extra[n.a as usize..n.a as usize + 3];
				let since = if self.n(rec[1]).tag == Tag::Int {
					ShowSince::Versionstamp(Ast::uint(self.n(rec[1])))
				} else {
					match self.expr(rec[1])? {
						Expr::Literal(Literal::Datetime(d)) => ShowSince::Timestamp(d),
						_ => return Err(Reject::new("not a datetime")),
					}
				};
				TopLevelExpr::Show(ShowStatement {
					table: if rec[0] == NONE {
						None
					} else {
						Some(TableName::new(Strand::new(self.name(rec[0])?)))
					},
					since,
					limit: if rec[2] == NONE {
						None
					} else {
						Some(
							u32::try_from(Ast::int(self.n(rec[2])))
								.map_err(|_| Reject::new("u32"))?,
						)
					},
				})
			}
			Tag::AccessStmt => {
				use crate::expr::statements::access::{
					AccessStatement, AccessStatementGrant, AccessStatementPurge,
					AccessStatementRevoke, AccessStatementShow, PurgeKind, Subject,
				};
				let rec = &self.ast.extra[n.a as usize..n.a as usize + 4];
				let ac = Strand::new(self.name(rec[0])?);
				let base = (rec[1] != 0).then(|| Self::base(rec[1]));
				let gr = || -> R<Option<Strand>> {
					Ok(if rec[3] == 1 {
						Some(Strand::new(self.name(rec[2])?))
					} else {
						None
					})
				};
				let cond = || -> R<Option<Cond>> {
					Ok(if rec[3] == 2 {
						Some(Cond(self.expr(rec[2])?))
					} else {
						None
					})
				};
				TopLevelExpr::Access(Box::new(match n.op {
					0 => AccessStatement::Grant(AccessStatementGrant {
						ac,
						base,
						subject: if rec[3] == 1 {
							Subject::Record(self.record_id(rec[2])?)
						} else {
							Subject::User(Strand::new(self.name(rec[2])?))
						},
					}),
					1 => AccessStatement::Show(AccessStatementShow {
						ac,
						base,
						gr: gr()?,
						cond: cond()?,
					}),
					2 => AccessStatement::Revoke(AccessStatementRevoke {
						ac,
						base,
						gr: gr()?,
						cond: cond()?,
					}),
					_ => AccessStatement::Purge(AccessStatementPurge {
						ac,
						base,
						kind: match rec[3] {
							1 => PurgeKind::Expired,
							2 => PurgeKind::Revoked,
							_ => PurgeKind::Both,
						},
						grace: if rec[2] == NONE {
							Duration::default().into()
						} else {
							let (s, ns) = decode_duration(self.text(rec[2]).as_bytes())
								.ok_or(Reject::new("duration overflow"))?;
							Duration::new(s, ns).into()
						},
					}),
				}))
			}
			_ => TopLevelExpr::Expr(self.expr(id)?),
		})
	}

	fn opt(&self, id: NodeId) -> R<Option<Expr>> {
		if id == NONE {
			Ok(None)
		} else {
			self.expr(id).map(Some)
		}
	}

	fn base(code: u32) -> Base {
		match code {
			1 => Base::Ns,
			2 => Base::Db,
			_ => Base::Root,
		}
	}

	/// Lowers the expression at `id`.
	///
	/// Values, blocks and operators lower in a function of their own, so each
	/// level of their nesting costs a small frame on the call stack rather
	/// than the frame of every construct: stored values are lowered at the
	/// full depth the storage layer accepts.
	pub fn expr(&self, id: NodeId) -> R<Expr> {
		let n = self.n(id);
		match n.tag {
			Tag::Array
			| Tag::Object
			| Tag::Prefix
			| Tag::Cast
			| Tag::Binary
			| Tag::Block
			| Tag::Set => self.nesting(n),
			_ => self.other(id),
		}
	}

	/// [`Self::expr`] for the constructs that nest.
	#[inline(never)]
	fn nesting(&self, n: &Node) -> R<Expr> {
		Ok(match n.tag {
			Tag::Array => Expr::Literal(Literal::Array(
				self.list(n.a).iter().map(|&e| self.expr(e)).collect::<R<_>>()?,
			)),
			Tag::Object => Expr::Literal(Literal::Object(self.entries(n)?)),
			Tag::Prefix => {
				let op = match n.op {
					x if x == Prefix::Not as u8 => PrefixOperator::Not,
					x if x == Prefix::Neg as u8 => PrefixOperator::Negate,
					x if x == Prefix::Pos as u8 => PrefixOperator::Positive,
					x if x == Prefix::Range as u8 => PrefixOperator::Range,
					_ => PrefixOperator::RangeInclusive,
				};
				Expr::Prefix {
					op,
					expr: Box::new(self.expr(n.a)?),
				}
			}
			Tag::Cast => Expr::Prefix {
				op: PrefixOperator::Cast(self.kind(n.a)?),
				expr: Box::new(self.expr(n.b)?),
			},
			Tag::Binary => Expr::Binary {
				left: Box::new(self.expr(n.a)?),
				op: binary(Binary::from_u8(n.op)),
				right: Box::new(self.expr(n.b)?),
			},
			Tag::Block => Expr::Block(Box::new(Block(self.exprs(n.a)?))),
			Tag::Set => Expr::Literal(Literal::Set(self.exprs(n.a)?)),
			_ => unreachable!("only nesting constructs are lowered here"),
		})
	}

	/// [`Self::expr`] for everything else.
	#[inline(never)]
	fn other(&self, id: NodeId) -> R<Expr> {
		let n = self.n(id);
		Ok(match n.tag {
			Tag::None => Expr::Literal(Literal::None),
			Tag::Null => Expr::Literal(Literal::Null),
			Tag::True => Expr::Literal(Literal::Bool(true)),
			Tag::False => Expr::Literal(Literal::Bool(false)),
			Tag::Int => Expr::Literal(Literal::Integer(Ast::int(n))),
			Tag::Float => Expr::Literal(Literal::Float(Ast::float(n))),
			Tag::NaN => Expr::Literal(Literal::Float(f64::NAN)),
			Tag::Infinity => Expr::Literal(Literal::Float(f64::INFINITY)),
			Tag::UnboundedRange => Expr::Literal(Literal::UnboundedRange),
			Tag::Decimal => Expr::Literal(lit::decimal(self.text(id))?),
			Tag::Regex => Expr::Literal(lit::regex_at(self.text(id), n.start, n.end)?),
			Tag::Path => match lit::builtin(&self.path_name(id)?, n.start, n.end)? {
				lit::Builtin::Constant(c) => Expr::Constant(c),
				lit::Builtin::Function(_) => return Err(self.unexpected_after(n.end, "(")),
			},
			Tag::Duration => {
				let (s, ns) = decode_duration(self.text(id).as_bytes())
					.ok_or(Reject::new("duration overflow"))?;
				Expr::Literal(Literal::Duration(Duration::new(s, ns).into()))
			}
			Tag::Str => match n.op {
				0 | b's' => {
					let mut s = String::new();
					if !decode::unescape(decode::strip_delims(self.text(id)), &mut s) {
						return Err(Reject::new("invalid escape"));
					}
					if self.opts.legacy_strands {
						return Ok(Expr::Literal(lit::legacy_strand(s)));
					}
					Expr::Literal(Literal::String(Strand::new(s)))
				}
				b'f' => {
					if !self.opts.files {
						return Err(Reject::new(FILES_DISABLED));
					}
					let mut s = String::new();
					if !decode::unescape(decode::strip_delims(self.text(id)), &mut s) {
						return Err(Reject::new("invalid escape"));
					}
					let f = lit::file(&s).map_err(|r| lit::in_token(r, self.text(id), n.start))?;
					Expr::Literal(Literal::File(f.into()))
				}
				_ => self.prefixed(id)?,
			},
			Tag::Param => Expr::Param(Param::from(Strand::new(self.param_name(id)?))),
			Tag::Field => Expr::Idiom(Idiom(vec![Part::Field(Strand::new(self.name(id)?))])),
			Tag::Table => Expr::Table(TableName::new(Strand::new(self.name(id)?))),
			Tag::RecordId => Expr::Literal(Literal::RecordId(self.record_id(id)?)),
			Tag::BinaryX => {
				let r = &self.ast.extra[n.b as usize..];
				Expr::Binary {
					left: Box::new(self.expr(n.a)?),
					op: self.binary_extended(n)?,
					right: Box::new(self.expr(r[0])?),
				}
			}
			Tag::PostfixRange => Expr::Postfix {
				expr: Box::new(self.expr(n.a)?),
				op: if n.op == 0 {
					PostfixOperator::Range
				} else {
					PostfixOperator::RangeSkip
				},
			},
			Tag::Call => {
				let callee = self.n(n.a);
				let receiver = match callee.op {
					0 => {
						// Builtin names are resolved against the function
						// registry when the tree is lowered, not while parsing.
						let path = self.path_name(n.a)?;
						if !CHECK_NAMES.with(|c| c.get()) {
							Function::Normal(path.to_lowercase())
						} else {
							match lit::builtin(&path, callee.start, callee.end)? {
								lit::Builtin::Function(f) => Function::Normal(f),
								// A constant followed by arguments is a call of
								// its value.
								lit::Builtin::Constant(c) => {
									return Ok(Expr::Postfix {
										expr: Box::new(Expr::Constant(c)),
										op: PostfixOperator::Call(self.exprs(n.b)?),
									});
								}
							}
						}
					}
					_ => self.function_ref(n.a)?,
				};
				Expr::FunctionCall(Box::new(FunctionCall {
					receiver,
					arguments: self.exprs(n.b)?,
				}))
			}
			Tag::Script => Expr::FunctionCall(Box::new(FunctionCall {
				receiver: Function::Script(Script(self.text(n.a).to_owned())),
				arguments: self.exprs(n.b)?,
			})),
			Tag::Point => Expr::Literal(Literal::Geometry(
				surrealdb_types::Geometry::Point(
					(self.coordinate(n.a), self.coordinate(n.b)).into(),
				)
				.into(),
			)),
			Tag::CallExpr => Expr::Postfix {
				expr: Box::new(self.expr(n.a)?),
				op: PostfixOperator::Call(self.exprs(n.b)?),
			},
			Tag::Idiom => Expr::Idiom(self.idiom(id)?),
			Tag::Select => Expr::Select(Box::new(self.select(n)?)),
			Tag::Create => Expr::Create(Box::new(self.create(n)?)),
			Tag::Update => Expr::Update(Box::new(self.update(n)?)),
			Tag::Upsert => {
				let u = self.update(n)?;
				Expr::Upsert(Box::new(UpsertStatement {
					only: u.only,
					what: u.what,
					with: u.with,
					data: u.data,
					cond: u.cond,
					output: u.output,
					timeout: u.timeout,
					explain: u.explain,
				}))
			}
			Tag::Delete => {
				let u = self.update(n)?;
				Expr::Delete(Box::new(DeleteStatement {
					only: u.only,
					what: u.what,
					with: u.with,
					cond: u.cond,
					output: u.output,
					timeout: u.timeout,
					explain: u.explain,
				}))
			}
			Tag::Return => Expr::Return(Box::new(OutputStatement {
				what: self.expr(n.a)?,
				fetch: self.fetchs(n.b)?,
			})),
			Tag::Let => {
				let rec = &self.ast.extra[n.a as usize..n.a as usize + 3];
				let kind = if rec[1] == NONE {
					None
				} else {
					Some(self.kind(rec[1])?)
				};
				Expr::Let(Box::new(SetStatement {
					name: Strand::new(self.param_name(rec[0])?),
					what: self.expr(rec[2])?,
					kind,
				}))
			}
			Tag::If => {
				let l = self.list(n.a);
				Expr::IfElse(Box::new(IfelseStatement {
					exprs: l
						.chunks(2)
						.map(|cb| Ok((self.expr(cb[0])?, self.expr(cb[1])?)))
						.collect::<R<_>>()?,
					close: self.opt(n.b)?,
				}))
			}
			Tag::For => {
				let rec = &self.ast.extra[n.a as usize..n.a as usize + 3];
				Expr::Foreach(Box::new(ForeachStatement {
					param: Param::from(Strand::new(self.param_name(rec[0])?)),
					range: self.expr(rec[1])?,
					block: match self.expr(rec[2])? {
						Expr::Block(b) => *b,
						_ => return Err(Reject::new("not a block")),
					},
				}))
			}
			Tag::Relate => Expr::Relate(Box::new(self.relate(n)?)),
			Tag::Insert => Expr::Insert(Box::new(self.insert(n)?)),
			Tag::Explain => Expr::Explain {
				format: if n.op & 2 != 0 {
					ExplainFormat::Json
				} else {
					ExplainFormat::Text
				},
				analyze: n.op & 1 != 0,
				statement: Box::new(self.expr(n.a)?),
			},
			Tag::Alter => Expr::Alter(Box::new(self.alter(n)?)),
			Tag::Remove => Expr::Remove(Box::new(self.remove(n)?)),
			Tag::Define => Expr::Define(Box::new(self.define(n)?)),
			Tag::Closure => {
				let r = &self.ast.extra[n.a as usize..n.a as usize + 3];
				let args = self
					.list(r[0])
					.chunks(2)
					.map(|pk| {
						Ok((
							Param::from(Strand::new(self.param_name(pk[0])?)),
							if pk[1] == NONE {
								Kind::Any
							} else {
								self.kind(pk[1])?
							},
						))
					})
					.collect::<R<_>>()?;
				Expr::Closure(Box::new(Closure {
					args,
					returns: if r[1] == NONE {
						None
					} else {
						Some(self.kind(r[1])?)
					},
					body: self.expr(r[2])?,
				}))
			}
			Tag::Mock => {
				let r = &self.ast.extra[n.b as usize..n.b as usize + 3];
				let table = TableName::new(Strand::new(self.name(n.a)?));
				let int = |id: u32| Ast::int(self.n(id));
				Expr::Mock(if r[2] & 1 == 0 {
					Mock::Count(table, int(r[0]))
				} else {
					Mock::Range(
						table,
						TypedRange {
							start: if r[0] == NONE {
								Bound::Unbounded
							} else if r[2] & 2 != 0 {
								Bound::Excluded(int(r[0]))
							} else {
								Bound::Included(int(r[0]))
							},
							end: if r[1] == NONE {
								Bound::Unbounded
							} else if r[2] & 4 != 0 {
								Bound::Included(int(r[1]))
							} else {
								Bound::Excluded(int(r[1]))
							},
						},
					)
				})
			}
			Tag::Sleep => {
				let (s, ns) = decode_duration(self.text(n.a).as_bytes())
					.ok_or(Reject::new("duration overflow"))?;
				Expr::Sleep(Box::new(SleepStatement {
					duration: Duration::new(s, ns).into(),
				}))
			}
			Tag::Throw => Expr::Throw(Box::new(self.expr(n.a)?)),
			Tag::Break => Expr::Break,
			Tag::Continue => Expr::Continue,
			Tag::Info => {
				use surrealdb_parser_next::ast::{InfoTarget as I, info};
				let rec = &self.ast.extra[n.a as usize..n.a as usize + info::LEN];
				let structure = rec[info::FLAGS] & 1 != 0;
				let version = self.opt(rec[info::VERSION])?;
				let what = || self.expr(rec[info::WHAT]);
				Expr::Info(Box::new(match n.op {
					x if x == I::Root as u8 => InfoStatement::Root(structure, version),
					x if x == I::Ns as u8 => InfoStatement::Ns(structure, version),
					x if x == I::Db as u8 => InfoStatement::Db(structure, version),
					x if x == I::Table as u8 => InfoStatement::Tb(what()?, structure, version),
					x if x == I::User as u8 => {
						let base = rec[info::FLAGS] >> 1;
						InfoStatement::User(
							what()?,
							(base != 0).then(|| Self::base(base)),
							structure,
						)
					}
					_ => InfoStatement::Index(what()?, self.expr(rec[info::TABLE])?, structure),
				}))
			}
			Tag::Rebuild => {
				let rec = &self.ast.extra[n.a as usize..n.a as usize + 3];
				Expr::Rebuild(Box::new(RebuildStatement::Index(RebuildIndexStatement {
					name: Strand::new(self.name(rec[0])?),
					table: TableName::new(Strand::new(self.name(rec[1])?)),
					if_exists: rec[2] & 1 != 0,
					concurrently: rec[2] & 2 != 0,
				})))
			}
			t => panic!("not an expression: {t:?}"),
		})
	}

	pub fn kind(&self, id: NodeId) -> R<Kind> {
		use surrealdb_parser_next::ast::kind as k;
		let n = self.n(id);
		let names = |list: u32| -> R<Vec<String>> {
			self.list(list).iter().map(|&t| self.name(t)).collect()
		};
		let kinds =
			|list: u32| -> R<Vec<Kind>> { self.list(list).iter().map(|&t| self.kind(t)).collect() };
		Ok(match n.op {
			k::ANY => Kind::Any,
			k::NONE => Kind::None,
			k::NULL => Kind::Null,
			k::BOOL => Kind::Bool,
			k::BYTES => Kind::Bytes,
			k::DATETIME => Kind::Datetime,
			k::DECIMAL => Kind::Decimal,
			k::DURATION => Kind::Duration,
			k::FLOAT => Kind::Float,
			k::INT => Kind::Int,
			k::NUMBER => Kind::Number,
			k::OBJECT => Kind::Object,
			k::POINT => Kind::Geometry(vec![GeometryKind::Point]),
			k::STRING => Kind::String,
			k::UUID => Kind::Uuid,
			k::RANGE => Kind::Range,
			k::REGEX => Kind::Regex,
			k::FUNCTION => Kind::Function(None, None),
			k::RECORD => Kind::Record(
				names(n.a)?.into_iter().map(|t| TableName::new(Strand::new(t))).collect(),
			),
			k::TABLE => Kind::Table(
				names(n.a)?.into_iter().map(|t| TableName::new(Strand::new(t))).collect(),
			),
			k::GEOMETRY => Kind::Geometry(
				names(n.a)?
					.iter()
					.map(|g| match g.to_ascii_uppercase().as_str() {
						"POINT" => GeometryKind::Point,
						"LINE" => GeometryKind::Line,
						"POLYGON" => GeometryKind::Polygon,
						"MULTIPOINT" => GeometryKind::MultiPoint,
						"MULTILINE" => GeometryKind::MultiLine,
						"MULTIPOLYGON" => GeometryKind::MultiPolygon,
						_ => GeometryKind::Collection,
					})
					.collect(),
			),
			k::FILE => Kind::File(names(n.a)?),
			k::ARRAY | k::SET => {
				let inner = if n.a == NONE {
					Kind::Any
				} else {
					self.kind(n.a)?
				};
				let size = if n.b == NONE {
					None
				} else {
					Some(Ast::uint(self.n(n.b)))
				};
				if n.op == k::ARRAY {
					Kind::Array(Box::new(inner), size)
				} else {
					Kind::Set(Box::new(inner), size)
				}
			}
			k::EITHER => Kind::either(kinds(n.a)?),
			k::OPTION => {
				let mut v = vec![Kind::None];
				v.extend(kinds(n.a)?);
				Kind::either(v)
			}
			k::LITERAL if self.n(n.a).tag == Tag::Str => {
				// Read as written: `legacy_strands` does not apply to kinds.
				let mut s = String::new();
				if !decode::unescape(decode::strip_delims(self.text(n.a)), &mut s) {
					return Err(Reject::new("invalid escape"));
				}
				Kind::Literal(KindLiteral::String(Strand::new(s)))
			}
			k::LITERAL => Kind::Literal(match self.expr(n.a)? {
				Expr::Literal(Literal::Integer(i)) => KindLiteral::Integer(i),
				Expr::Literal(Literal::Float(f)) => KindLiteral::Float(f),
				Expr::Literal(Literal::Decimal(d)) => KindLiteral::Decimal(d),
				Expr::Literal(Literal::Duration(d)) => KindLiteral::Duration(d),
				Expr::Literal(Literal::Bool(b)) => KindLiteral::Bool(b),
				_ => return Err(Reject::new("not a literal kind")),
			}),
			k::LIT_OBJECT => Kind::Literal(KindLiteral::Object(
				self.list(n.a)
					.chunks(2)
					.map(|kv| Ok((Strand::new(self.name(kv[0])?), self.kind(kv[1])?)))
					.collect::<R<_>>()?,
			)),
			k::LIT_ARRAY => Kind::Literal(KindLiteral::Array(kinds(n.a)?)),
			// A named tuple is read only as the whole type of the `id` field, by
			// [`Self::field_type`]; anywhere else, as in `[a: x] | none`, it is refused.
			k::LIT_KEYED => {
				return Err(Reject::new(
					"component names are only allowed when they name the whole TYPE of the `id` field",
				)
				.at(n.start, n.end));
			}
			c => panic!("not a kind code: {c}"),
		})
	}

	/// The `TYPE` of a field and, for the whole type of the field `id` written as a
	/// named tuple, the names of its components. `field_is_id` is whether the field
	/// is `id`.
	///
	/// A component name that is reserved or repeated is refused at the span of the
	/// name.
	pub fn field_type(&self, id: NodeId, field_is_id: bool) -> R<(Kind, Option<KeyNames>)> {
		self.keyed_type(id, field_is_id, false)
	}

	/// The `TYPE` of the field `id` as the text a catalog definition stores for it:
	/// [`Self::field_type`] for the field `id`, except that component names that do
	/// not form a valid [`KeyNames`] are dropped with a warning instead of refused.
	/// The type is then the plain tuple, as if it had been written without names.
	///
	/// Reading the catalog must not fail over names, because the whole field list of
	/// the table would then be unreadable; whether names are acceptable is a check
	/// made when a definition is written.
	pub fn stored_id_type(&self, id: NodeId) -> R<(Kind, Option<KeyNames>)> {
		self.keyed_type(id, true, true)
	}

	fn keyed_type(
		&self,
		id: NodeId,
		field_is_id: bool,
		stored: bool,
	) -> R<(Kind, Option<KeyNames>)> {
		use surrealdb_parser_next::ast::kind as k;
		let n = self.n(id);
		if !field_is_id || n.tag != Tag::Kind || n.op != k::LIT_KEYED {
			return Ok((self.kind(id)?, None));
		}
		let list = self.list(n.a);
		let mut names: Vec<Strand> = Vec::with_capacity(list.len() / 2);
		let mut kinds = Vec::with_capacity(list.len() / 2);
		for pair in list.chunks(2) {
			let at = self.n(pair[0]);
			let name = self.name(pair[0])?;
			if !stored {
				if KeyNames::is_reserved(&name) {
					return Err(Reject::new(format!(
						"the record id component name `{name}` is reserved"
					))
					.at(at.start, at.end));
				}
				if names.iter().any(|earlier| earlier.as_str() == name) {
					return Err(Reject::new(format!(
						"the record id component `{name}` is named twice"
					))
					.at(at.start, at.end));
				}
			}
			names.push(Strand::new(name));
			kinds.push(self.kind(pair[1])?);
		}
		let names = match KeyNames::new(names, kinds.len()) {
			Ok(names) => Some(names),
			Err(e) if stored => {
				tracing::warn!(
					"the component names of a stored record id type are not valid and are ignored: {e}"
				);
				None
			}
			Err(e) => return Err(Reject::new(e.to_string()).at(n.start, n.end)),
		};
		Ok((Kind::Literal(KindLiteral::Array(kinds)), names))
	}

	/// A `::` path `Name` as the joined, unescaped segment names.
	/// The established unexpected-token error for the token after byte `at`.
	fn unexpected_after(&self, at: u32, expected: &str) -> Reject {
		let mut l = Lexer::new(self.src.as_bytes());
		l.pos = at;
		let t = l.next_token();
		if t.kind == T::Eof {
			return Reject::new(format!("Unexpected end of file, expected {expected}"))
				.at(t.start, t.end);
		}
		Reject::new(format!(
			"Unexpected token `{}`, expected {expected}",
			diagnostic::token_name(self.src, t.start)
		))
		.at(t.start, t.end)
	}

	/// The token at or after byte `at`.
	fn tok_at(&self, at: u32) -> Tok {
		let mut l = Lexer::new(self.src.as_bytes());
		l.pos = at;
		l.next_token()
	}

	/// The span of the last token that ends by `end`, lexing from `start`.
	fn last_tok(&self, start: u32, end: u32) -> (u32, u32) {
		let mut l = Lexer::new(self.src.as_bytes());
		l.pos = start;
		let mut last = (start, end);
		loop {
			let t = l.next_token();
			if t.kind == T::Eof || t.end > end {
				return last;
			}
			last = (t.start, t.end);
		}
	}

	/// The first keyword `kw` among the tokens in `from..to`.
	fn find_kw(&self, from: u32, to: u32, kw: Kw) -> Option<Tok> {
		let mut l = Lexer::new(self.src.as_bytes());
		l.pos = from;
		loop {
			let t = l.next_token();
			if t.kind == T::Eof || t.start >= to {
				return None;
			}
			if t.kind == T::Ident && t.kw == kw {
				return Some(t);
			}
		}
	}

	/// The function a `Name` callee names, its arguments aside. A builtin path
	/// (`op` 0) is returned as written, unchecked against the registry, as
	/// the function-name entry point returns it.
	pub(crate) fn function_ref(&self, id: NodeId) -> R<Function> {
		let callee = self.n(id);
		Ok(match callee.op {
			0 => Function::Normal(self.path_name(id)?),
			1 => Function::Custom(self.path_name(id)?),
			2 => {
				if !self.opts.surrealism {
					return Err(Reject::new(SURREALISM_DISABLED));
				}
				self.warn_deprecated_extension(callee.b, callee.start);
				let p = &self.ast.extra[callee.a as usize + 1..];
				Function::Module(
					self.name(p[0])?,
					if p[1] == NONE {
						None
					} else {
						Some(self.path_name(p[1])?)
					},
					if callee.b & ast::ext::MOD_PREFIX != 0 {
						ExtPrefix::Mod
					} else {
						ExtPrefix::Ext
					},
				)
			}
			3 => {
				if !self.opts.surrealism {
					return Err(Reject::new(SURREALISM_DISABLED));
				}
				self.warn_deprecated_extension(callee.b, callee.start);
				let p = &self.ast.extra[callee.a as usize + 1..];
				let v = |i: usize| {
					u32::try_from(Ast::int(self.n(p[i]))).map_err(|_| Reject::new("u32"))
				};
				Function::Silo {
					org: self.name(p[0])?,
					pkg: self.name(p[1])?,
					major: v(2)?,
					minor: v(3)?,
					patch: v(4)?,
					sub: if p[5] == NONE {
						None
					} else {
						Some(self.path_name(p[5])?)
					},
				}
			}
			_ => {
				let p = &self.ast.extra[callee.a as usize + 1..];
				let v = |i: usize| Ast::int(self.n(p[i]));
				Function::Model(Model {
					name: Strand::new(self.path_name(p[0])?),
					version: Strand::new(format!("{}.{}.{}", v(1), v(2), v(3))),
				})
			}
		})
	}

	pub(crate) fn path_name(&self, id: NodeId) -> R<String> {
		lit::path(self.text(id)).ok_or_else(|| Reject::new("invalid escape"))
	}

	/// A point coordinate: a float, integer, `NaN` or `Infinity` node.
	fn coordinate(&self, id: NodeId) -> f64 {
		let n = self.n(id);
		match n.tag {
			Tag::Int => Ast::int(n) as f64,
			Tag::NaN => f64::NAN,
			Tag::Infinity => f64::INFINITY,
			_ => Ast::float(n),
		}
	}

	fn exprs(&self, list: u32) -> R<Vec<Expr>> {
		self.list(list).iter().map(|&e| self.expr(e)).collect()
	}

	fn entries(&self, n: &Node) -> R<Vec<ObjectEntry>> {
		self.list(n.a)
			.chunks(2)
			.map(|kv| {
				Ok(ObjectEntry {
					key: Strand::new(self.name(kv[0])?),
					value: self.expr(kv[1])?,
				})
			})
			.collect()
	}

	fn binary_extended(&self, n: &Node) -> R<BinaryOperator> {
		let r = &self.ast.extra[n.b as usize..];
		let op = if Binary::from_u8(n.op) == Binary::Matches {
			BinaryOperator::Matches(MatchesOperator {
				rf: if r[1] == NONE {
					None
				} else {
					Some(u8::try_from(Ast::int(self.n(r[1]))).map_err(|_| Reject::new("u8"))?)
				},
				operator: match r[2] {
					1 => BooleanOperator::And,
					2 => BooleanOperator::Or,
					_ => BooleanOperator::And,
				},
			})
		} else {
			let k = u32::try_from(Ast::int(self.n(r[2]))).map_err(|_| Reject::new("u32"))?;
			BinaryOperator::NearestNeighbor(Box::new(match r[1] {
				0 => NearestNeighbor::KTree(k),
				1 => NearestNeighbor::K(k, self.distance_of(r[3], r[4])?),
				_ => NearestNeighbor::Approximate(
					k,
					u32::try_from(Ast::int(self.n(r[3]))).map_err(|_| Reject::new("u32"))?,
				),
			}))
		};
		Ok(op)
	}

	fn record_id(&self, id: NodeId) -> R<RecordIdLit> {
		let n = self.n(id);
		Ok(RecordIdLit {
			table: TableName::new(Strand::new(self.name(n.a)?)),
			key: self.key(n.b)?,
		})
	}

	fn key(&self, id: NodeId) -> R<RecordIdKeyLit> {
		let n = self.n(id);
		Ok(match n.tag {
			Tag::KeyInt => RecordIdKeyLit::Number(Ast::int(n)),
			Tag::KeyStr if n.op == 2 => {
				RecordIdKeyLit::String(Strand::new(format!("-{}", self.text(id))))
			}
			Tag::KeyStr if n.op == 3 => RecordIdKeyLit::String(Strand::new(self.text(id))),
			Tag::KeyStr => RecordIdKeyLit::String(Strand::new(self.name(id)?)),
			Tag::KeyUuid => match lit::uuid(&lit::string_body(self.text(id))?)
				.map_err(|r| lit::in_token(r, self.text(id), self.n(id).start))?
			{
				Literal::Uuid(u) => RecordIdKeyLit::Uuid(u),
				_ => return Err(Reject::new("not a uuid")),
			},
			Tag::KeyGen => RecordIdKeyLit::Generate(match n.op {
				0 => RecordIdKeyGen::Ulid,
				1 => RecordIdKeyGen::Uuid,
				_ => RecordIdKeyGen::Rand,
			}),
			Tag::Array => RecordIdKeyLit::Array(self.exprs(n.a)?),
			Tag::Object => RecordIdKeyLit::Object(self.entries(n)?),
			Tag::KeyRange => {
				let start = if n.a == NONE {
					Bound::Unbounded
				} else if n.op & 1 != 0 {
					Bound::Excluded(self.key(n.a)?)
				} else {
					Bound::Included(self.key(n.a)?)
				};
				let end = if n.b == NONE {
					Bound::Unbounded
				} else if n.op & 2 != 0 {
					Bound::Included(self.key(n.b)?)
				} else {
					Bound::Excluded(self.key(n.b)?)
				};
				RecordIdKeyLit::Range(Box::new(RecordIdKeyRangeLit {
					start,
					end,
				}))
			}
			t => panic!("not a key: {t:?}"),
		})
	}

	/// A `Field` leaf or an `Idiom` node, as an idiom.
	pub fn idiom(&self, id: NodeId) -> R<Idiom> {
		let n = self.n(id);
		if n.tag == Tag::Field {
			// The empty head of a recursion path with no parts, as in `.{1}()`.
			if n.op == 1 {
				return Ok(Idiom(Vec::new()));
			}
			return Ok(Idiom(vec![Part::Field(Strand::new(self.name(id)?))]));
		}
		assert_eq!(n.tag, Tag::Idiom);
		let mut parts = Vec::new();
		let head = self.n(n.a);
		match head.tag {
			Tag::Field if head.op == 1 => {}
			Tag::Doc => parts.push(Part::Doc),
			Tag::Field | Tag::Table if head.depth & ast::COVERED == 0 => {
				parts.push(Part::Field(Strand::new(self.name(n.a)?)))
			}
			_ => match self.expr(n.a)? {
				Expr::Idiom(Idiom(p)) if head.depth & ast::COVERED == 0 => parts.extend(p),
				e => parts.push(Part::Start(e)),
			},
		}
		for &p in self.list(n.b) {
			let pn = self.n(p);
			parts.push(match pn.tag {
				Tag::PartField => Part::Field(Strand::new(self.name(p)?)),
				Tag::PartAll => Part::All,
				Tag::PartLast => Part::Last,
				Tag::PartFlatten => Part::Flatten,
				Tag::PartOptional => Part::Optional,
				Tag::PartIndex => Part::Value(self.expr(pn.a)?),
				Tag::PartWhere => Part::Where(self.expr(pn.a)?),
				Tag::PartMethod => Part::Method(Strand::new(self.name(pn.a)?), self.exprs(pn.b)?),
				Tag::PartGraph => Part::Lookup(Box::new(self.lookup(pn)?)),
				Tag::PartRepeat => Part::RepeatRecurse,
				Tag::PartRecurse => {
					let r = &self.ast.extra[pn.a as usize..pn.a as usize + 7];
					let num = |id: u32| -> R<Option<u32>> {
						if id == NONE {
							return Ok(None);
						}
						u32::try_from(Ast::int(self.n(id)))
							.map(Some)
							.map_err(|_| Reject::new("u32"))
					};
					let recurse = if r[0] == 0 {
						Recurse::Fixed(num(r[1])?.expect("fixed recursion has a count"))
					} else {
						Recurse::Range(num(r[1])?, num(r[2])?)
					};
					let inclusive = r[5] == 1;
					let instruction = match r[3] {
						0 => None,
						1 => Some(RecurseInstruction::Path {
							inclusive,
						}),
						2 => Some(RecurseInstruction::Collect {
							inclusive,
						}),
						_ => Some(RecurseInstruction::Shortest {
							expects: self.expr(r[4])?,
							inclusive,
						}),
					};
					let nest = if r[6] == NONE {
						None
					} else {
						Some(self.idiom(r[6])?)
					};
					Part::Recurse(recurse, nest, instruction)
				}
				Tag::PartDestructure => Part::Destructure(self.destructure(pn)?),
				t => panic!("not a part: {t:?}"),
			});
		}
		Ok(Idiom(parts))
	}

	fn ordering(&self, list: u32) -> R<Option<Ordering>> {
		if list == NONE {
			return Ok(None);
		}
		let items = self.list(list);
		if items.len() == 1 && self.n(items[0]).tag == Tag::OrderRand {
			return Ok(Some(Ordering::Random));
		}
		Ok(Some(Ordering::Order(OrderList(
			items
				.iter()
				.map(|&o| {
					let on = self.n(o);
					Ok(Order {
						value: self.idiom(on.a)?,
						collate: on.op & 2 != 0,
						numeric: on.op & 4 != 0,
						direction: on.op & 1 == 0,
					})
				})
				.collect::<R<_>>()?,
		))))
	}

	/// A graph or reference lookup part.
	fn lookup(&self, pn: &Node) -> R<Lookup> {
		use ast::select as s;
		let rec = &self.ast.extra[pn.a as usize..pn.a as usize + s::LEN];
		let flags = rec[s::FLAGS];
		let kind = match pn.op {
			0 => LookupKind::Graph(Dir::Out),
			1 => LookupKind::Graph(Dir::In),
			2 => LookupKind::Graph(Dir::Both),
			_ => LookupKind::Reference,
		};
		let mut what = Vec::new();
		for &sj in self.list(rec[s::WHAT]) {
			let sn = self.n(sj);
			let table = TableName::new(Strand::new(self.name(sn.a)?));
			let x = &self.ast.extra[sn.b as usize..sn.b as usize + 2];
			let referencing_field = if x[1] == NONE {
				None
			} else {
				Some(self.name(x[1])?)
			};
			what.push(if x[0] == NONE {
				LookupSubject::Table {
					table,
					referencing_field,
				}
			} else {
				let RecordIdKeyLit::Range(range) = self.key(x[0])? else {
					return Err(Reject::new("not a range"));
				};
				LookupSubject::Range {
					table,
					range: *range,
					referencing_field,
				}
			});
		}
		let expr = if flags & 128 == 0 {
			None
		} else if rec[s::VALUE] != NONE {
			Some(Fields::Value(Box::new(self.selector(self.n(rec[s::VALUE]))?)))
		} else {
			Some(self.fields(rec[s::FIELDS])?)
		};
		let (split, group) = match &expr {
			Some(fields) => self.split_group(fields, rec, pn.start, false)?,
			None => (None, None),
		};
		Ok(Lookup {
			kind,
			expr,
			only: flags & 1 != 0,
			what,
			cond: self.opt(rec[s::COND])?.map(Cond),
			split,
			group,
			order: self.ordering(rec[s::ORDER])?,
			limit: self.opt(rec[s::LIMIT])?.map(Limit),
			start: self.opt(rec[s::START])?.map(Start),
			alias: if rec[s::FETCH] == NONE {
				None
			} else {
				Some(self.idiom(rec[s::FETCH])?)
			},
		})
	}

	fn destructure(&self, n: &Node) -> R<Vec<DestructurePart>> {
		let mut out = Vec::new();
		for &e in self.list(n.a) {
			let en = self.n(e);
			let name = || -> R<Strand> { Ok(Strand::new(self.name(en.a)?)) };
			out.push(match en.op {
				0 => DestructurePart::Field(name()?),
				1 => DestructurePart::Aliased(
					name()?,
					match self.expr(en.b)? {
						Expr::Idiom(i) => i,
						v => Idiom(vec![Part::Start(v)]),
					},
				),
				2 => DestructurePart::All(name()?),
				3 => DestructurePart::Destructure(name()?, self.destructure(self.n(en.b))?),
				_ => DestructurePart::Rest,
			});
		}
		Ok(out)
	}

	/// A [`Tag::Fields`] node from [`surrealdb_parser_next::Entry::Fields`].
	pub(crate) fn fields_node(&self, n: &Node) -> R<Fields> {
		if n.a == NONE {
			Ok(Fields::Value(Box::new(self.selector(self.n(n.b))?)))
		} else {
			self.fields(n.a)
		}
	}

	fn fields(&self, list: u32) -> R<Fields> {
		let mut out = Vec::new();
		for &f in self.list(list) {
			let n = self.n(f);
			out.push(match n.tag {
				Tag::FieldAll => Field::All,
				_ => Field::Single(self.selector(n)?),
			});
		}
		Ok(Fields::Select(out))
	}

	fn selector(&self, n: &Node) -> R<Selector> {
		Ok(Selector {
			expr: self.expr(n.a)?,
			alias: if n.b == NONE {
				None
			} else {
				Some(self.idiom(n.b)?)
			},
		})
	}

	fn opt_expr(&self, id: u32) -> R<Expr> {
		if id == NONE {
			Ok(Expr::Literal(Literal::None))
		} else {
			self.expr(id)
		}
	}

	fn select(&self, n: &Node) -> R<SelectStatement> {
		use ast::select as s;
		let rec = &self.ast.extra[n.a as usize..n.a as usize + s::LEN];
		let fields = if rec[s::VALUE] != NONE {
			Fields::Value(Box::new(self.selector(self.n(rec[s::VALUE]))?))
		} else {
			self.fields(rec[s::FIELDS])?
		};
		let flags = rec[s::FLAGS];
		let (split, group) = self.split_group(&fields, rec, n.start, false)?;
		let order = self.ordering(rec[s::ORDER])?;
		Ok(SelectStatement {
			fields,
			omit: self.exprs(rec[s::OMIT])?,
			only: flags & 1 != 0,
			what: self.exprs(rec[s::WHAT])?,
			with: self.with(rec[s::WITH])?,
			cond: if rec[s::COND] == NONE {
				None
			} else {
				Some(Cond(self.expr(rec[s::COND])?))
			},
			split,
			group,
			order,
			limit: if rec[s::LIMIT] == NONE {
				None
			} else {
				Some(Limit(self.expr(rec[s::LIMIT])?))
			},
			start: if rec[s::START] == NONE {
				None
			} else {
				Some(Start(self.expr(rec[s::START])?))
			},
			fetch: self.fetchs(rec[s::FETCH])?,
			version: self.opt_expr(rec[s::VERSION])?,
			timeout: self.opt_expr(rec[s::TIMEOUT])?,
			explain: explain(flags),
			tempfiles: flags & 8 != 0,
			for_update: flags & 64 != 0,
		})
	}

	/// The `GROUP` clause of a `SELECT` record, checked against its fields.
	/// The `SPLIT` and `GROUP` clauses of a select-shaped record, checked
	/// against the projection `fields` as the parse-time checks do, with
	/// their spans. `from` is at or before the `SELECT` keyword; in a
	/// view the projection's span runs to the `FROM` keyword.
	fn split_group(
		&self,
		fields: &Fields,
		rec: &[u32],
		from: u32,
		view: bool,
	) -> R<(Option<Splits>, Option<Groups>)> {
		use ast::select as s;
		let group_all = rec[s::FLAGS] & 2 != 0;
		if rec[s::SPLIT] == NONE && rec[s::GROUP] == NONE && !group_all {
			return Ok((None, None));
		}
		let fields_span = || {
			let select = self.find_kw(from, u32::MAX, Kw::Select).map_or(from, |t| t.end);
			let start = self.tok_at(select).start;
			let last = if rec[s::VALUE] != NONE {
				rec[s::VALUE]
			} else {
				*self.list(rec[s::FIELDS]).last().expect("a projection is never empty")
			};
			let mut end = self.n(last).end;
			if view {
				end = self.tok_at(end).end;
			}
			(start, end)
		};
		let missing = |msg: String, id: NodeId| {
			let n = self.n(id);
			let (a, b) = fields_span();
			let mut r = Reject::new(msg).at(n.start, n.end);
			r.extra = Some((a, b, "Idiom missing here".into()));
			r
		};
		// Clauses before `SPLIT` and `GROUP` hold no such keyword after here.
		let mut after = self.list(rec[s::WHAT]).last().map_or(from, |&w| self.n(w).end);
		for at in [rec[s::COND], rec[s::WITH]] {
			if at != NONE {
				after = after.max(self.n(at).end);
			}
		}
		let mut split_span = None;
		let split = if rec[s::SPLIT] == NONE {
			None
		} else {
			let ids = self.list(rec[s::SPLIT]);
			let mut splits = Vec::with_capacity(ids.len());
			for &id in ids {
				let g = self.idiom(id)?;
				if !projected(fields, &g, false) {
					let msg =
						format!("Missing split idiom `{:?}` in statement selection", g.to_sql());
					return Err(missing(msg, id));
				}
				splits.push(Split(g));
			}
			let first = self.n(ids[0]).start;
			let kw = self.find_kw(after, first, Kw::Split).map_or(first, |t| t.start);
			after = self.n(*ids.last().expect("a split list is never empty")).end;
			split_span = Some((kw, after));
			Some(Splits(splits))
		};
		let exclusive = |group: (u32, u32)| -> Reject {
			let (a, b) = split_span.expect("a split span");
			let mut r = Reject::new("SPLIT and GROUP are mutually exclusive")
				.at(a, b)
				.with_label("SPLIT cannot be used with GROUP");
			r.extra = Some((group.0, group.1, "GROUP cannot be used with SPLIT".into()));
			r
		};
		let group = if group_all {
			if split.is_some() {
				let kw = self.find_kw(after, u32::MAX, Kw::Group).map_or(after, |t| t.start);
				let all = self.find_kw(kw, u32::MAX, Kw::All).map_or(kw, |t| t.end);
				return Err(exclusive((kw, all)));
			}
			Some(Groups(Vec::new()))
		} else if rec[s::GROUP] != NONE {
			let ids = self.list(rec[s::GROUP]);
			let mut groups = Vec::with_capacity(ids.len());
			for &id in ids {
				let g = self.idiom(id)?;
				if !projected(fields, &g, true) {
					let msg =
						format!("Missing group idiom `{}` in statement selection", g.to_sql());
					return Err(missing(msg, id));
				}
				groups.push(Group(g));
			}
			if split.is_some() {
				let first = self.n(ids[0]).start;
				let kw = self.find_kw(after, first, Kw::Group).map_or(first, |t| t.start);
				let end = self.n(*ids.last().expect("a group list is never empty")).end;
				return Err(exclusive((kw, end)));
			}
			Some(Groups(groups))
		} else {
			None
		};
		Ok((split, group))
	}

	fn fetchs(&self, list: u32) -> R<Option<Fetchs>> {
		if list == NONE {
			return Ok(None);
		}
		Ok(Some(Fetchs::new(
			self.list(list)
				.iter()
				.map(|&f| {
					let fe = self.expr(f)?;
					Ok(Fetch(fe))
				})
				.collect::<R<_>>()?,
		)))
	}

	fn data(&self, rec: &[u32]) -> R<Option<Data>> {
		use ast::write as w;
		let payload = rec[w::DATA];
		Ok(Some(match rec[w::DATA_KIND] {
			0 => return Ok(None),
			1 => Data::SetExpression(self.assigns(payload)?),
			2 => Data::UnsetExpression(
				self.list(payload).iter().map(|&i| self.idiom(i)).collect::<R<_>>()?,
			),
			3 => Data::ContentExpression(self.expr(payload)?),
			4 => Data::MergeExpression(self.expr(payload)?),
			5 => Data::PatchExpression(self.expr(payload)?),
			_ => Data::ReplaceExpression(self.expr(payload)?),
		}))
	}

	fn output(&self, rec: &[u32]) -> R<Option<Output>> {
		use ast::write as w;
		Ok(Some(match rec[w::OUTPUT_KIND] {
			0 => return Ok(None),
			1 => Output::None,
			2 => Output::Null,
			3 => Output::Diff,
			4 => Output::After,
			5 => Output::Before,
			7 => Output::Fields(Fields::Value(Box::new(self.selector(self.n(rec[w::OUTPUT]))?))),
			_ => Output::Fields(self.fields(rec[w::OUTPUT])?),
		}))
	}

	fn create(&self, n: &Node) -> R<CreateStatement> {
		use ast::write as w;
		let rec = &self.ast.extra[n.a as usize..n.a as usize + w::LEN];
		Ok(CreateStatement {
			only: rec[w::FLAGS] & 1 != 0,
			what: self.exprs(rec[w::WHAT])?,
			data: self.data(rec)?,
			output: self.output(rec)?,
			timeout: self.opt_expr(rec[w::TIMEOUT])?,
		})
	}

	fn update(&self, n: &Node) -> R<UpdateStatement> {
		use ast::write as w;
		let rec = &self.ast.extra[n.a as usize..n.a as usize + w::LEN];
		Ok(UpdateStatement {
			only: rec[w::FLAGS] & 1 != 0,
			what: self.exprs(rec[w::WHAT])?,
			with: self.with(rec[w::WITH])?,
			data: self.data(rec)?,
			cond: if rec[w::COND] == NONE {
				None
			} else {
				Some(Cond(self.expr(rec[w::COND])?))
			},
			output: self.output(rec)?,
			timeout: self.opt_expr(rec[w::TIMEOUT])?,
			explain: explain(rec[w::FLAGS]),
		})
	}

	fn assigns(&self, list: u32) -> R<Vec<Assignment>> {
		self.list(list)
			.iter()
			.map(|&a| {
				let an = self.n(a);
				Ok(Assignment {
					place: self.idiom(an.a)?,
					operator: match an.op {
						0 => AssignOperator::Assign,
						1 => AssignOperator::Add,
						2 => AssignOperator::Subtract,
						_ => AssignOperator::Extend,
					},
					value: self.expr(an.b)?,
				})
			})
			.collect()
	}

	fn relate(&self, n: &Node) -> R<RelateStatement> {
		use ast::{relate as r, write as w};
		let rec = &self.ast.extra[n.a as usize..n.a as usize + r::LEN];
		Ok(RelateStatement {
			only: rec[w::FLAGS] & 1 != 0,
			or_update: rec[w::FLAGS] & 4 != 0,
			through: self.expr(rec[r::THROUGH])?,
			from: self.expr(rec[r::FROM])?,
			to: self.expr(rec[r::TO])?,
			data: self.data(rec)?,
			output: self.output(rec)?,
			timeout: self.opt_expr(rec[w::TIMEOUT])?,
		})
	}

	fn insert(&self, n: &Node) -> R<InsertStatement> {
		use ast::{insert as i, write as w};
		let rec = &self.ast.extra[n.a as usize..n.a as usize + i::LEN];
		let data = if rec[w::DATA_KIND] == 1 {
			let v = &self.ast.extra[rec[w::DATA] as usize..rec[w::DATA] as usize + 2];
			let idioms = self.list(v[0]).iter().map(|&x| self.idiom(x)).collect::<R<Vec<_>>>()?;
			let mut rows = Vec::new();
			for &row in self.list(v[1]) {
				let vals = self.exprs(row)?;
				if vals.len() != idioms.len() {
					return Err(Reject::new("invalid number of values to insert"));
				}
				rows.push(idioms.iter().cloned().zip(vals).collect());
			}
			Data::ValuesExpression(rows)
		} else {
			Data::SingleExpression(self.expr(rec[w::DATA])?)
		};
		Ok(InsertStatement {
			into: self.opt(rec[i::INTO])?,
			data,
			ignore: rec[w::FLAGS] & 4 != 0,
			update: if rec[i::UPDATE] == NONE {
				None
			} else {
				Some(Data::UpdateExpression(self.assigns(rec[i::UPDATE])?))
			},
			output: self.output(rec)?,
			timeout: self.opt_expr(rec[w::TIMEOUT])?,
			relation: rec[w::FLAGS] & 2 != 0,
		})
	}

	fn with(&self, id: NodeId) -> R<Option<With>> {
		if id == NONE {
			return Ok(None);
		}
		let n = self.n(id);
		Ok(Some(if n.op == 0 {
			With::NoIndex
		} else {
			With::Index(self.list(n.a).iter().map(|&i| self.name(i)).collect::<R<_>>()?)
		}))
	}
}

fn explain(flags: u32) -> Option<Explain> {
	(flags & 16 != 0).then_some(Explain(flags & 32 != 0))
}

fn binary(op: Binary) -> BinaryOperator {
	use BinaryOperator as B;
	match op {
		Binary::Or => B::Or,
		Binary::And => B::And,
		Binary::Tco => B::TenaryCondition,
		Binary::Nco => B::NullCoalescing,
		Binary::Eq => B::Equal,
		Binary::ExactEq => B::ExactEqual,
		Binary::Ne => B::NotEqual,
		Binary::AllEq => B::AllEqual,
		Binary::AnyEq => B::AnyEqual,
		Binary::Lt => B::LessThan,
		Binary::Le => B::LessThanEqual,
		Binary::Gt => B::MoreThan,
		Binary::Ge => B::MoreThanEqual,
		Binary::Add => B::Add,
		Binary::Sub => B::Subtract,
		Binary::Mul => B::Multiply,
		Binary::Div => B::Divide,
		Binary::Rem => B::Remainder,
		Binary::Pow => B::Power,
		Binary::Contain => B::Contain,
		Binary::NotContain => B::NotContain,
		Binary::ContainAll => B::ContainAll,
		Binary::ContainAny => B::ContainAny,
		Binary::ContainNone => B::ContainNone,
		Binary::Inside => B::Inside,
		Binary::NotInside => B::NotInside,
		Binary::AllInside => B::AllInside,
		Binary::AnyInside => B::AnyInside,
		Binary::NoneInside => B::NoneInside,
		Binary::Outside => B::Outside,
		Binary::Intersects => B::Intersects,
		Binary::Range => B::Range,
		Binary::RangeInclusive => B::RangeInclusive,
		Binary::RangeSkip => B::RangeSkip,
		Binary::RangeSkipInclusive => B::RangeSkipInclusive,
		Binary::Matches => B::Matches(MatchesOperator {
			operator: BooleanOperator::And,
			rf: None,
		}),
		Binary::Like | Binary::NotLike | Binary::AllLike | Binary::AnyLike | Binary::Knn => {
			unreachable!("not produced by the parser")
		}
	}
}

/// Whether the projection holds `g`, as a `SPLIT` idiom (exactly) or a
/// `GROUP BY` idiom (or a prefix of a projected idiom).
fn projected(fields: &Fields, g: &Idiom, group: bool) -> bool {
	let prefix = |a: &Idiom, b: &Idiom| b.0.len() >= a.0.len() && b.0[..a.0.len()] == a.0[..];
	let ok = |sel: &Selector| {
		if sel.alias.as_ref() == Some(g) {
			return true;
		}
		let same = |vi: &Idiom| g == vi || (group && prefix(g, vi));
		match &sel.expr {
			Expr::Idiom(x) => same(x),
			v => same(&v.to_idiom()),
		}
	};
	match fields {
		Fields::Value(sel) => ok(sel),
		Fields::Select(fs) => fs.iter().any(|f| match f {
			Field::All => true,
			Field::Single(sel) => ok(sel),
		}),
	}
}
