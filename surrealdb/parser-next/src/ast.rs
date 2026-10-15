//! The syntax tree: one flat arena of fixed-size nodes.
//!
//! Every node is a 20-byte [`Node`] in [`Ast::nodes`], addressed by a `u32`
//! [`NodeId`]. Variable-length children (array elements, call arguments, idiom
//! parts, statement clauses) live in [`Ast::extra`] as `[len, item…]` runs, and
//! a node refers to a run by its index. This is the layout of Zig's
//! `std.zig.Ast`.
//!
//! Consequences the rest of the crate relies on:
//! - A parse allocates only when `nodes` or `extra` outgrow their capacity, and an `Ast` reused
//!   across parses stops allocating once warm.
//! - Dropping a tree is two frees, never a recursive walk, so a deep tree cannot overflow the stack
//!   when it is dropped.
//! - Identifiers, strings and numbers are not copied: leaves hold source spans and are decoded when
//!   the tree is lowered, by which time the lexer has validated their syntax.
//! - Each node records its depth, so consumers that recurse over the tree can rely on
//!   [`crate::Limits::expr_depth`] bounding that recursion.

/// A node index. `NONE` marks an absent optional child.
pub type NodeId = u32;
pub const NONE: NodeId = u32::MAX;

/// A node tag. The meaning of `op`, `a` and `b` per tag is documented on each
/// variant; `start..end` is always the node's source span.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Tag {
	/// A statement that failed to parse; its span covers the skipped source.
	Error,
	None,
	Null,
	True,
	False,
	/// `a | b << 32` is the `i64` value; with `op` 1, the `u64` value of an
	/// unsigned-integer position.
	Int,
	/// `a | b << 32` is the `f64` bit pattern.
	Float,
	NaN,
	Infinity,
	/// Value decoded from the span.
	Decimal,
	/// Value decoded from the span.
	Duration,
	/// A string; `op` is its prefix byte (`0`, `s`, `d`, `u`, `b`, `r`, `f`).
	Str,
	Regex,
	/// `..` with no bounds.
	UnboundedRange,
	Param,
	/// A bare identifier resolved as a field of the current document.
	Field,
	/// A bare identifier resolved as a table name.
	Table,
	/// A `a::b::c` path that is not followed by arguments: a constant.
	Path,
	/// An object key or a part name; the span is the identifier or string.
	Name,
	/// `a`: table `Name`, `b`: key node.
	RecordId,
	/// Integer key: `a | b << 32`.
	KeyInt,
	/// Identifier-like key; the span is the text (`op` 1 escaped, 2 the
	/// digits of an out-of-range negative number, to which `-` is added, 3
	/// taken verbatim).
	KeyStr,
	/// A `u"…"` key; the span is the string.
	KeyUuid,
	/// `op`: 0 ulid, 1 uuid, 2 rand.
	KeyGen,
	/// `a`: start key or `NONE`, `b`: end key or `NONE`; `op` bit 0: start
	/// excluded, bit 1: end included.
	KeyRange,
	/// `a`: list of elements.
	Array,
	/// `a`: list of alternating key `Name` and value nodes.
	Object,
	/// `op`: [`Prefix`], `a`: operand.
	Prefix,
	/// `a`: kind node, `b`: operand.
	Cast,
	/// A type. `op` is a [`kind`] code; `a` and `b` are documented there.
	Kind,
	/// `op`: [`Binary`], `a`, `b`: operands.
	Binary,
	/// A binary operator with a payload: `op` [`Binary::Matches`] or
	/// [`Binary::Knn`], `a` the left operand, `b` index into `extra` of a
	/// record whose first slot is the right operand: `[rhs, reference or NONE,
	/// 0 | 1 AND | 2 OR]` for matches, `[rhs, 0 k-tree | 1 distance | 2
	/// approximate, k, distance Name or ef, Minkowski order]` for KNN.
	BinaryX,
	/// `op`: 0 `a..`, 1 `a>..`; `a`: operand.
	PostfixRange,
	/// `a`: the callee `Name`, `b`: argument list. `op` 0 a builtin path, 1
	/// `fn::` (the `Name` spans the path after `fn::`), 2 `ext::` (or its
	/// alias `mod::`), 3 `silo::`, 4 `ml::`; for 2–4 the `Name`'s `a` indexes
	/// `extra` for its parts: `[name, sub path or NONE]`, `[org, pkg, major,
	/// minor, patch, sub or NONE]`, `[path, major, minor, patch]`. For 2 and 3
	/// the `Name`'s `b` holds the [`ext`] bits of a deprecated spelling.
	Call,
	/// `a`: callee expression, `b`: argument list.
	CallExpr,
	/// `a`: start node, `b`: list of parts.
	Idiom,
	/// `.name`; the span is the name.
	PartField,
	PartAll,
	PartLast,
	PartFlatten,
	PartOptional,
	/// `[a]`.
	PartIndex,
	/// `[WHERE a]`.
	PartWhere,
	/// `.a(b…)`: `a` the method `Name`, `b` the argument list.
	PartMethod,
	/// A lookup: `op` 0 `->`, 1 `<-`, 2 `<->`, 3 `<~`; `a` index into
	/// `extra` of a [`select`] record whose `WHAT` is the list of
	/// [`Tag::Subject`]s, whose `FETCH` slot holds the `AS` alias idiom, and
	/// whose flags bit 7 marks a `SELECT` projection.
	PartGraph,
	/// `(x, y)`: `a` and `b` the coordinates.
	Point,
	/// `function(args) { … }`: `a` a `Name` spanning the body, `b` the
	/// argument list.
	Script,
	/// A closure: `a` index into `extra` of `[argument list, return kind or
	/// NONE, body]`; the argument list alternates parameters and kinds (`NONE`
	/// for no kind).
	Closure,
	/// `|table:…|`: `a` the table `Name`, `b` index into `extra` of `[from,
	/// to, flags]`: flags bit 0 a range, bit 1 start excluded, bit 2 end
	/// included; absent bounds `NONE`.
	Mock,
	/// `.@`.
	PartRepeat,
	/// `.{…}` recursion: `a` index into `extra` of `[0 fixed | 1 range, min
	/// Int or NONE, max Int or NONE, instruction 0 none | 1 path | 2 collect |
	/// 3 shortest, shortest target or NONE, inclusive, nested idiom or NONE]`.
	PartRecurse,
	/// `.{ … }` destructuring: `a` the list of [`Tag::DestructEntry`]s.
	PartDestructure,
	/// `op` 0 `name`, 1 `name: b` (an expression), 2 `name.*`, 3 `name.{…}`
	/// (`b` a `PartDestructure`), 4 `...`; `a` the `Name` (`op` 0–3).
	DestructEntry,
	/// `@`, the current document, as the head of an idiom.
	Doc,
	/// A lookup subject: `a` the table `Name`; `b` index into `extra` of
	/// `[KeyRange or NONE, referencing field Name or NONE]`.
	Subject,
	/// `a`: index into `extra` of the [`select`] record.
	Select,
	/// `a`: index into `extra` of the [`write`](mod@write) record.
	Create,
	/// `a`: index into `extra` of the [`write`](mod@write) record.
	Update,
	/// `a`: index into `extra` of the [`write`](mod@write) record.
	Upsert,
	/// `a`: index into `extra` of the [`write`](mod@write) record (no data).
	Delete,
	/// `a`: value, `b`: fetch list or `NONE`.
	Return,
	/// `a`: index into `extra` of `[param, kind or NONE, value]`.
	Let,
	/// `SELECT` field: `a` expression, `b` alias idiom or `NONE`.
	Selector,
	/// `*` in a field list.
	FieldAll,
	/// A field list parsed alone: `a` the list of `Selector` and `FieldAll`
	/// nodes, or `NONE` when `b` is the `Selector` after `VALUE`.
	Fields,
	/// `a`: idiom, `op`: [`Assign`], `b`: value.
	Assign,
	/// `ORDER BY` item: `a` idiom, `op` bit 0 descending, bit 1 collate, bit 2
	/// numeric.
	OrderItem,
	/// `ORDER BY RAND()`.
	OrderRand,
	/// `a`: list of alternating conditions and bodies; `b`: the `ELSE` body
	/// or `NONE`.
	If,
	/// `a`: index into `extra` of `[param, range, block]`.
	For,
	/// `a`: index into `extra` of the [`relate`] record.
	Relate,
	/// `a`: index into `extra` of the [`insert`] record.
	Insert,
	/// `WITH` index hint: `op` 0 `NOINDEX`, 1 `INDEX a` with `a` the list of
	/// index `Name`s.
	With,
	/// `EXPLAIN [ANALYZE] [FORMAT …] stmt`: `a` the statement; `op` bit 0
	/// `ANALYZE`, bit 1 `FORMAT JSON`.
	Explain,
	/// `{ stmt; … }`: `a` the list of statements.
	Block,
	/// `{ a, b }`: `a` the list of elements.
	Set,
	Begin,
	Cancel,
	Commit,
	/// `op`: 0 `NS a`, 1 `DB a`, 2 `NS a DB b`, 3 `DEFAULT`.
	Use,
	/// `a`: option `Name`; `op`: the value.
	OptionStmt,
	/// `a`: a uuid string or a parameter.
	Kill,
	/// `a`: index into `extra` of `[fields, what, cond, fetch list, flags]`:
	/// flags 1 `DIFF` (fields `NONE`), 2 `VALUE` (fields a `Selector`), else
	/// fields a field list.
	Live,
	/// `a`: index into `extra` of `[table Name or NONE, since, limit]`;
	/// `since` is an `Int` versionstamp or a datetime string.
	Show,
	/// `ACCESS …`: `op` 0 grant, 1 show, 2 revoke, 3 purge; `a` index into
	/// `extra` of `[access Name, base code or 0, x, y]`. Grant: `x` the user
	/// `Name` (`y` 0) or record id (`y` 1). Show and revoke: `y` 0 `ALL`, 1
	/// `GRANT x`, 2 `WHERE x`. Purge: `x` the grace duration or `NONE`, `y`
	/// bit 0 `EXPIRED`, bit 1 `REVOKED`.
	AccessStmt,
	/// `a`: duration.
	Sleep,
	/// `a`: value.
	Throw,
	Break,
	Continue,
	/// `op`: [`InfoTarget`]; `a`: index into `extra` of [`info`] slots.
	Info,
	/// `a`: index into `extra` of `[index Name, table Name, flags]`; flags
	/// bit 0 `IF EXISTS`, bit 1 `CONCURRENTLY`.
	Rebuild,
	/// `op`: [`DefineKind`]; `a`: index into `extra` of the [`define`] record;
	/// `b`: for [`DefineKind::Module`], the [`ext`] bits of a deprecated
	/// spelling, otherwise 0.
	Define,
	/// `op`: the [`DefineKind`] removed; `a`: index into `extra` of the
	/// [`remove`] record; `b` as for [`Tag::Define`].
	Remove,
	/// `op`: the [`DefineKind`] altered, or [`ALTER_SYSTEM`]; `a`: index into
	/// `extra` of `[if exists, name, on, clause list]`, as in [`remove`] for
	/// the name and `on` slots; `b` as for [`Tag::Define`]. `COMMENT` clauses
	/// hold a string, and duration clauses a duration or `NONE` for `NONE`.
	Alter,
	/// A clause of a definition: `op` a [`Cl`] code, `a` and `b` its payload as
	/// documented there. Clauses are kept in source order; a later clause of
	/// the same kind overrides an earlier one, as earlier releases read it.
	Clause,
}

/// What a `DEFINE` defines.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DefineKind {
	Namespace,
	Database,
	Function,
	User,
	Param,
	Table,
	Api,
	Event,
	Field,
	Index,
	Analyzer,
	Access,
	Config,
	Bucket,
	Sequence,
	/// `EXTENSION`, or its alias `MODULE`.
	Module,
}

/// Deprecated spellings of an extension, as bits of the `b` of a
/// [`Tag::Define`], [`Tag::Alter`] or [`Tag::Remove`] node of
/// [`DefineKind::Module`], or of an extension call's callee `Name`. Each
/// spelling means exactly what its replacement means; the bits only let the
/// consumer report the deprecation.
pub mod ext {
	/// The `MODULE` keyword, for `EXTENSION`.
	pub const MODULE_KEYWORD: u32 = 1;
	/// The `mod::` prefix, for `ext::`.
	pub const MOD_PREFIX: u32 = 2;
	/// An unnamed silo package addressed by its coordinates,
	/// `silo::org::pkg::<1.2.3>`, rather than by an `ext::` name.
	pub const SILO_NAME: u32 = 4;
}

/// Slots of the `DEFINE` record in `extra`. Absent parts are `NONE`.
pub mod define {
	/// 0 plain, 1 `IF NOT EXISTS`, 2 `OVERWRITE`.
	pub const EXIST: usize = 0;
	/// The name: an expression, or a `Name` (`fn::` path of a function, the
	/// name after `ext::` of an extension), or a `Param` (`DEFINE PARAM`).
	/// `NONE` for `CONFIG`.
	pub const NAME: usize = 1;
	/// `ON` target: a table expression (events, fields, indexes), or a base
	/// code (users, accesses). For `CONFIG`, 0 `API`, 1 `GRAPHQL`, 2 `DEFAULT`.
	pub const ON: usize = 2;
	/// Function: list of alternating parameter and kind. Extension: the
	/// executable (a list `[org Name, pkg Name, major, minor, patch]` with
	/// the version as `Int`s, or a file string).
	pub const X1: usize = 3;
	/// Function: the return kind. Extension: 1 when `X1` is a silo package,
	/// `NONE` when it is a file string.
	pub const X2: usize = 4;
	/// Function: the body block.
	pub const X3: usize = 5;
	pub const CLAUSES: usize = 6;
	pub const LEN: usize = 7;
}

/// The `op` of a [`Tag::Alter`] node for `ALTER SYSTEM`.
pub const ALTER_SYSTEM: u8 = 16;

/// Slots of the `REMOVE` record in `extra`.
pub mod remove {
	/// Bit 0 `IF EXISTS`, bit 1 `AND EXPUNGE`.
	pub const FLAGS: usize = 0;
	/// The name: an expression, a function `Name`, a `Param`, the `Name`
	/// after `ext::` of an extension, or for an unnamed silo extension a list
	/// `[org, pkg, major, minor, patch]`.
	pub const NAME: usize = 1;
	/// The `ON` table expression or base code; for `CONFIG` 0 `GRAPHQL`, 1
	/// `API`, 2 `DEFAULT`; for an extension 1 when it is an unnamed silo.
	pub const ON: usize = 2;
	pub const LEN: usize = 3;
}

/// Clause codes of [`Tag::Clause`]. `a`/`b` payloads are given per code;
/// numbers are `Int` or `Float` nodes, names `Name` leaves, strings `Str`.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Cl {
	/// `a`: expression.
	Comment,
	/// `a`: duration; `b`: 1 with `INCLUDE ORIGINAL`.
	Changefeed,
	Strict,
	/// `PERMISSIONS NONE | FULL | WHERE …`: `a` a [`Cl::Perm`] clause.
	PermValue,
	/// Table or field `PERMISSIONS`: `a` list of [`Cl::PermFor`] clauses; `b`
	/// 1 for a field (whose unspecified actions default to `FULL`).
	Permissions,
	/// One permission value: `a` 0 `NONE`, 1 `FULL`, 2 `WHERE b`.
	Perm,
	/// `FOR actions value`: `a` the actions (1 select, 2 create, 4 update, 8
	/// delete; 15 for a bare `NONE` or `FULL`), `b` the [`Cl::Perm`] clause.
	PermFor,
	/// `a`: string.
	GraphqlAlias,
	/// `a`: string.
	GraphqlDeprecated,
	/// `a`: string.
	Password,
	/// `a`: string.
	Passhash,
	/// `a`: string.
	Passscram,
	/// `a`: list of role `Name`s.
	Roles,
	/// `DURATION FOR TOKEN a`.
	DurToken,
	/// `DURATION FOR SESSION a`.
	DurSession,
	/// `DURATION FOR GRANT a`.
	DurGrant,
	/// `a`: expression.
	Value,
	Drop,
	TypeNormal,
	/// `a`: index into `extra` of `[from list or NONE, to list or NONE,
	/// flags]`; lists hold table `Name`s, flags bit 0 `ENFORCED`, bit 1
	/// `LIGHTWEIGHT`.
	TypeRelation,
	TypeAny,
	Schemaless,
	Schemafull,
	/// `a`: the view's `Select` node.
	View,
	/// `a`: integer.
	InlineEdges,
	/// `a`: integer.
	InlineRefs,
	/// `a`: expression.
	When,
	/// `a`: list of expressions.
	Then,
	Async,
	/// `a`: integer.
	Retry,
	/// `a`: integer.
	MaxDepth,
	/// `a`: kind, a [`kind::LIT_KEYED`] node when the field is `id` and its
	/// type names the components of the key; `b`: 1 with `FLEXIBLE`.
	FieldType,
	Readonly,
	Inline,
	/// `a`: expression.
	Assert,
	/// `a`: expression; `b`: 1 with `ALWAYS`.
	Default,
	/// `a`: 0 reject, 1 cascade, 2 ignore, 3 unset, 4 `THEN b`, 5 no `ON
	/// DELETE`.
	Reference,
	/// `a`: expression.
	Computed,
	/// `a`: list of expressions.
	Fields,
	Unique,
	/// `a`: condition or `NONE`.
	Count,
	/// `a`: analyzer `Name` or `NONE`; `b`: index into `extra` of `[k1, b]`
	/// for `BM25(k1, b)`, `NONE` for no `BM25`, or `NONE - 1` for a bare
	/// `BM25`. Highlights follow as [`Cl::Highlights`].
	Fulltext,
	Highlights,
	/// `a`: index into `extra` of the [`vector`] record.
	Hnsw,
	/// `a`: index into `extra` of the [`vector`] record.
	Diskann,
	Concurrently,
	/// `a`: list of filter clauses ([`Cl::Filter`]).
	Filters,
	/// `a`: 0 ascii, 1 lowercase, 2 uppercase, 3 edgengram, 4 ngram, 5
	/// snowball, 6 mapper; `b`: for the n-grams an index into `extra` of the
	/// two integers, for snowball the language `Name`, for mapper the path
	/// string.
	Filter,
	/// `a`: list of tokenizer `Name`s; a `SEGMENT(lang)` tokenizer is a
	/// `Name` whose `op` is 1 and whose `a` is the language `Name`.
	Tokenizers,
	/// `a`: function `Name` (`op` 1 `fn::`, 2 `ext::` or its alias `mod::`,
	/// with the [`ext`] bits in `b`).
	Function,
	/// `a`: expression.
	Authenticate,
	/// `a`: expression.
	Context,
	/// `TYPE JWT`: `a` the [`Cl::Jwt`] clause.
	AccessJwt,
	/// `TYPE RECORD`: `a` list of record-access clauses (`Signup`, `Signin`,
	/// `WithJwt`, `WithRefresh`).
	AccessRecord,
	/// `TYPE BEARER FOR USER|RECORD`: `a` 0 user, 1 record; `b` the `Jwt`
	/// clause or `NONE`.
	AccessBearer,
	/// `a`: expression.
	Signup,
	/// `a`: expression.
	Signin,
	/// `WITH JWT`: `a` the `Jwt` clause.
	WithJwt,
	WithRefresh,
	/// A JWT definition: `a` index into `extra` of the [`jwt`] record.
	Jwt,
	/// `a`: expression.
	Backend,
	/// `a`: expression.
	Batch,
	/// `a`: expression.
	Start,
	/// `a`: expression.
	Timeout,
	Unsigned,
	/// `FOR ANY`: `a` the [`Cl::ApiConfig`] clause, `b` the fallback or `NONE`.
	ApiAny,
	/// `FOR methods`: `a` index into `extra` of `[list of method Names,
	/// config clause, action]`.
	ApiMethods,
	/// API configuration: `a` list of `PermValue` and `Middleware` clauses.
	ApiConfig,
	/// `a`: list of middleware `Call`s.
	Middleware,
	/// `DEFINE CONFIG GRAPHQL` settings: `a` list of `Gql…` clauses.
	GraphqlConfig,
	/// `a`: 0 none, 1 auto, 2 include, 3 exclude; `b`: list of names.
	GqlTables,
	/// As [`Cl::GqlTables`], with function names.
	GqlFunctions,
	/// `NONE` or `AUTO` for both: `a` 0 none, 1 auto.
	GqlBoth,
	/// `a`: integer.
	GqlDepth,
	/// `a`: integer.
	GqlComplexity,
	/// `a`: 0 none, 1 auto.
	GqlIntrospection,
	/// `DEFINE CONFIG DEFAULT`: `a` expression.
	DefaultNs,
	/// `DEFINE CONFIG DEFAULT`: `a` expression.
	DefaultDb,
	Compact,
	/// `QUERY_TIMEOUT a`.
	QueryTimeout,
	/// `PREPARE [REMOVE]`.
	PrepareRemove,
	/// `FLEXIBLE` on its own, in `ALTER FIELD`.
	Flexible,
	/// `ALTER FUNCTION`'s `(args) [-> kind] { … }`: `a` index into `extra` of
	/// `[argument list, return kind or NONE, block]`.
	Signature,
	/// `DROP x`: `a` the [`Cl`] code of the clause dropped.
	AlterDrop,
	/// `FOR methods DROP THEN`: `a` list of method `Name`s.
	ApiDropAction,
}

/// Slots of an HNSW or DiskANN record in `extra`. Absent parts are `NONE`.
pub mod vector {
	pub const DIMENSION: usize = 0;
	/// Distance `Name`; for `MINKOWSKI` its order is in [`MINKOWSKI`].
	pub const DISTANCE: usize = 1;
	pub const MINKOWSKI: usize = 2;
	/// Vector type `Name`.
	pub const TYPE: usize = 3;
	/// HNSW `M`, DiskANN `DEGREE`.
	pub const M: usize = 4;
	/// HNSW `M0`, DiskANN `L_BUILD`.
	pub const M0: usize = 5;
	/// HNSW `LM`, DiskANN `ALPHA`.
	pub const LM: usize = 6;
	/// HNSW `EFC`.
	pub const EFC: usize = 7;
	/// Bit 0 `EXTEND_CANDIDATES`, bit 1 `KEEP_PRUNED_CONNECTIONS`, bit 2
	/// `HASHED_VECTOR`.
	pub const FLAGS: usize = 8;
	pub const LEN: usize = 9;
}

/// Slots of a JWT record in `extra`. Absent parts are `NONE`.
pub mod jwt {
	/// Verification algorithm `Name` (with `KEY`), or `NONE` with `URL`.
	pub const ALG: usize = 0;
	/// The `KEY` or `URL` expression.
	pub const KEY: usize = 1;
	/// List of audience expressions.
	pub const AUDIENCE: usize = 2;
	/// 1 when `WITH ISSUER` is present.
	pub const ISSUER: usize = 3;
	/// Issuer algorithm `Name`.
	pub const ISSUER_ALG: usize = 4;
	/// Issuer key expression.
	pub const ISSUER_KEY: usize = 5;
	pub const LEN: usize = 6;
}

/// The target of an `INFO` statement.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum InfoTarget {
	Root,
	Ns,
	Db,
	Table,
	User,
	Index,
}

/// Slots of the `INFO` record in `extra`. Absent parts are `NONE`.
pub mod info {
	/// Table, user or index name expression.
	pub const WHAT: usize = 0;
	/// The table of `INFO FOR INDEX`.
	pub const TABLE: usize = 1;
	pub const VERSION: usize = 2;
	/// Bit 0 `STRUCTURE`; bits 1–2 the `ON` base of `INFO FOR USER`: 0 none,
	/// then [`super::BASE_NS`], [`super::BASE_DB`], [`super::BASE_ROOT`].
	pub const FLAGS: usize = 3;
	pub const LEN: usize = 4;
}

/// Base codes: `NAMESPACE`, `DATABASE`, `ROOT`.
pub const BASE_NS: u32 = 1;
pub const BASE_DB: u32 = 2;
pub const BASE_ROOT: u32 = 3;

/// [`Tag::Kind`] codes. Kinds without a payload have `a` and `b` unused.
pub mod kind {
	pub const ANY: u8 = 0;
	pub const NONE: u8 = 1;
	pub const NULL: u8 = 2;
	pub const BOOL: u8 = 3;
	pub const BYTES: u8 = 4;
	pub const DATETIME: u8 = 5;
	pub const DECIMAL: u8 = 6;
	pub const DURATION: u8 = 7;
	pub const FLOAT: u8 = 8;
	pub const INT: u8 = 9;
	pub const NUMBER: u8 = 10;
	pub const OBJECT: u8 = 11;
	/// `point`: a geometry kind of points only.
	pub const POINT: u8 = 12;
	pub const STRING: u8 = 13;
	pub const UUID: u8 = 14;
	pub const RANGE: u8 = 15;
	pub const REGEX: u8 = 16;
	pub const FUNCTION: u8 = 17;
	/// `a`: list of table `Name`s, or `NONE` when no `<…>` was given.
	pub const RECORD: u8 = 18;
	/// As [`RECORD`].
	pub const TABLE: u8 = 19;
	/// `a`: list of geometry-kind `Name`s, or `NONE`.
	pub const GEOMETRY: u8 = 20;
	/// `a`: element kind or `NONE`; `b`: size `Int` or `NONE`.
	pub const ARRAY: u8 = 21;
	/// As [`ARRAY`].
	pub const SET: u8 = 22;
	/// `a`: list of bucket `Name`s, or `NONE`.
	pub const FILE: u8 = 23;
	/// `a | b`: `a` the list of alternatives.
	pub const EITHER: u8 = 24;
	/// `option<…>`: `a` the list of alternatives, to which `none` is added.
	pub const OPTION: u8 = 25;
	/// A literal kind: `a` the literal node (string, number, duration, bool).
	pub const LITERAL: u8 = 26;
	/// `{ k: kind, … }`: `a` the list of alternating key `Name`s and kinds.
	pub const LIT_OBJECT: u8 = 27;
	/// `[kind, …]`: `a` the list of kinds.
	pub const LIT_ARRAY: u8 = 28;
	/// `[name: kind, …]`, the whole `TYPE` of an `id` field: `a` the list of
	/// alternating component `Name`s and kinds. Only [`crate::Entry::IdKind`]
	/// and the `TYPE` clause of a field named `id` produce it.
	pub const LIT_KEYED: u8 = 29;
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Prefix {
	Not,
	Neg,
	Pos,
	Range,
	RangeInclusive,
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Binary {
	Or,
	And,
	Tco,
	Nco,
	Eq,
	ExactEq,
	Ne,
	AllEq,
	AnyEq,
	Lt,
	Le,
	Gt,
	Ge,
	Add,
	Sub,
	Mul,
	Div,
	Rem,
	Pow,
	Contain,
	NotContain,
	ContainAll,
	ContainAny,
	ContainNone,
	Inside,
	NotInside,
	AllInside,
	AnyInside,
	NoneInside,
	Outside,
	Intersects,
	Range,
	RangeInclusive,
	RangeSkip,
	RangeSkipInclusive,
	Matches,
	Like,
	NotLike,
	AllLike,
	AnyLike,
	/// `<|…|>`, a nearest-neighbour search; only in [`Tag::BinaryX`].
	Knn,
}

impl Binary {
	pub(crate) const COUNT: usize = Binary::Knn as usize + 1;
	pub fn from_u8(v: u8) -> Binary {
		debug_assert!((v as usize) < Self::COUNT);
		// SAFETY: `Binary` is `repr(u8)` with contiguous discriminants from
		// zero, and every caller passes a value read back from a node this
		// crate wrote from a `Binary`.
		unsafe { core::mem::transmute::<u8, Binary>(v) }
	}
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Assign {
	Set,
	Add,
	Sub,
	Ext,
}

/// Field offsets of the `SELECT` clause record in `extra`. Absent clauses
/// are `NONE`.
pub mod select {
	/// Field list, or `NONE` when the projection is `VALUE`.
	pub const FIELDS: usize = 0;
	/// `SELECT VALUE` selector, or `NONE`.
	pub const VALUE: usize = 1;
	pub const OMIT: usize = 2;
	pub const WHAT: usize = 3;
	pub const COND: usize = 4;
	pub const GROUP: usize = 5;
	pub const ORDER: usize = 6;
	pub const LIMIT: usize = 7;
	pub const START: usize = 8;
	pub const FETCH: usize = 9;
	pub const VERSION: usize = 10;
	pub const TIMEOUT: usize = 11;
	/// Bit 0 `ONLY`, bit 1 `GROUP ALL`, bit 3 `TEMPFILES`, bit 4 `EXPLAIN`,
	/// bit 5 `EXPLAIN FULL`, bit 6 `FOR UPDATE`.
	pub const FLAGS: usize = 12;
	/// The [`super::Tag::With`] node or `NONE`.
	pub const WITH: usize = 13;
	/// `SPLIT` idiom list.
	pub const SPLIT: usize = 14;
	pub const LEN: usize = 15;
}

/// Field offsets of the `CREATE`/`UPDATE` clause record in `extra`.
pub mod write {
	pub const WHAT: usize = 0;
	/// `op` of [`super::DataKind`] in the low byte; the payload node or list in
	/// [`DATA`].
	pub const DATA_KIND: usize = 1;
	pub const DATA: usize = 2;
	pub const COND: usize = 3;
	/// Output kind (0 none given, 1 NONE, 2 NULL, 3 DIFF, 4 AFTER, 5 BEFORE,
	/// 6 fields, 7 `VALUE` selector).
	pub const OUTPUT_KIND: usize = 4;
	pub const OUTPUT: usize = 5;
	pub const TIMEOUT: usize = 6;
	/// Bit 0 `ONLY`, bit 1 `RELATION` (insert), bit 2 `OR UPDATE` (relate)
	/// or `IGNORE` (insert), bit 4 `EXPLAIN`, bit 5 `EXPLAIN FULL`.
	pub const FLAGS: usize = 7;
	/// The [`super::Tag::With`] node or `NONE`.
	pub const WITH: usize = 8;
	pub const LEN: usize = 9;
}

/// Slots of the `RELATE` record: the [`write`](mod@write) slots, then the relation.
pub mod relate {
	pub const FROM: usize = super::write::LEN;
	pub const THROUGH: usize = super::write::LEN + 1;
	pub const TO: usize = super::write::LEN + 2;
	pub const LEN: usize = super::write::LEN + 3;
}

/// Slots of the `INSERT` record: the [`write`](mod@write) slots (`DATA_KIND` 0 for a
/// single expression, 1 for `(fields) VALUES …`, whose `DATA` is an index
/// into `extra` of `[idiom list, list of row lists]`), then these.
pub mod insert {
	/// A `Param` or `Table`, or `NONE`.
	pub const INTO: usize = super::write::LEN;
	/// `ON DUPLICATE KEY UPDATE` assignment list.
	pub const UPDATE: usize = super::write::LEN + 1;
	pub const LEN: usize = super::write::LEN + 2;
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DataKind {
	None,
	Set,
	Unset,
	Content,
	Merge,
	Patch,
	Replace,
}

/// Bit set in [`Node::depth`] when the node was written in parentheses.
pub const COVERED: u16 = 0x8000;

/// One node: 20 bytes.
#[derive(Clone, Copy, Debug)]
pub struct Node {
	pub tag: Tag,
	pub op: u8,
	/// Depth of the subtree rooted here, saturating.
	pub depth: u16,
	pub a: u32,
	pub b: u32,
	pub start: u32,
	pub end: u32,
}

const _: () = assert!(core::mem::size_of::<Node>() == 20);

/// The arena. Reuse one across parses to stop allocating.
#[derive(Default)]
pub struct Ast {
	pub nodes: Vec<Node>,
	pub extra: Vec<u32>,
	/// The top-level statements of the last parse.
	pub stmts: Vec<NodeId>,
	/// The source span of each top-level statement.
	pub spans: Vec<(u32, u32)>,
}

impl Ast {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn clear(&mut self) {
		self.nodes.clear();
		self.extra.clear();
		self.stmts.clear();
		self.spans.clear();
	}

	#[inline]
	pub fn node(&self, id: NodeId) -> &Node {
		&self.nodes[id as usize]
	}

	/// The items of the list at `idx` in `extra`.
	#[inline]
	pub fn list(&self, idx: u32) -> &[u32] {
		if idx == NONE {
			return &[];
		}
		let i = idx as usize;
		let len = self.extra[i] as usize;
		&self.extra[i + 1..i + 1 + len]
	}

	pub fn int(n: &Node) -> i64 {
		(n.a as u64 | (n.b as u64) << 32) as i64
	}

	/// An `Int` node read as unsigned: the value of an unsigned-integer
	/// position, which may exceed `i64::MAX`.
	pub fn uint(n: &Node) -> u64 {
		n.a as u64 | (n.b as u64) << 32
	}

	pub fn float(n: &Node) -> f64 {
		f64::from_bits(n.a as u64 | (n.b as u64) << 32)
	}

	pub fn text<'s>(n: &Node, src: &'s str) -> &'s str {
		&src[n.start as usize..n.end as usize]
	}
}
