//! The attribute types of the statement-family ops: writes, DDL, `INFO`, `ACCESS`, `KILL`,
//! `LIVE`, `SHOW`, `OPTION` and `MATCH`.
//!
//! Every field of a statement is classified once:
//!
//! - an expression the statement evaluates once when it runs is an **operand** of the op, named
//!   here by an [`OperandSlot`];
//! - an expression evaluated per subject or row is a `Clause` region of the op, named by a
//!   [`RegionSlot`];
//! - an expression the statement stores and never runs is a `StoredClause` region, named by a
//!   [`RegionSlot`];
//! - a place a statement writes to is a [`Place`], whose expression-holding parts name an operand
//!   or a region;
//! - everything else is a typed attribute, reusing the language's own types where they hold no
//!   expression.
//!
//! No type here holds an expression, a logical plan or a statement: every one of them is plain
//! data, so an op built from them is printed, parsed, digested and compared structurally.
//!
//! A slot indexes the operand list or the region list of the op that carries the attribute, in the
//! order the op's contract states. A slot is meaningful only beside its op. The binder states
//! every slot. The verifier checks only the slots of a tuple row's places and of a key-range
//! scan's operand bounds. A slot of any other attribute names the operand or region the binder
//! put there.

use std::time::Duration;

use smallvec::SmallVec;
use surrealdb_strand::{Strand, TableName};

use super::region_kind::{ClauseRole, RegionKind, RegionKinds};
use crate::exec::operators::{
	EdgeBinding, EndpointField, ExpandDir, JoinType, PathMode, ShortestSelector,
};
use crate::exec::vm::program::FieldRefusal;
use crate::expr::access_type::{BearerAccessSubject, BearerAccessType};
use crate::expr::graphql_config::GraphQLConfig;
use crate::expr::index::{DiskAnnParams, FullTextParams, HnswParams};
use crate::expr::match_plan::{
	BindingDef, BindingId, DetachMode, EdgeStep, NodeStep, PathPrefixPlan,
};
use crate::expr::module::{ModuleExecutable, ModuleName};
use crate::expr::statements::access::PurgeKind;
use crate::expr::statements::alter::AlterKind;
use crate::expr::statements::alter::table::TableMigrateClause;
use crate::expr::statements::define::MigrateClause;
use crate::expr::statements::remove::SqlTypeObject;
pub(crate) use crate::expr::statements::show::ShowSince;
use crate::expr::table_type::TableType;
use crate::expr::{Algorithm, ApiMethod, Base, ChangeFeed, EventKind, Filter, Kind, Tokenizer};
use crate::iam::{Action, ConfigKind, ResourceKind, ScramCredential};
use crate::val::Value;

/// The index of an operand in the op's operand list.
pub(crate) type OperandSlot = u16;

/// The index of a region in the op's region list.
pub(crate) type RegionSlot = u16;

// ---------------------------------------------------------------------------------------------
// Writes
// ---------------------------------------------------------------------------------------------

/// The statement form a write op performs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum WriteForm {
	Create,
	Update,
	Upsert,
	Delete,
	Insert,
	Relate,
}

/// What a write returns for each record it writes.
///
/// `Default` is a statement with no `RETURN` clause: the record after the write where the write's
/// order leaves one, and nothing where it does not. `After` is `RETURN AFTER`, which shows the
/// record after the write whether or not the order leaves one. `Fields` leaves the projection to
/// the op's `Clause(ReturnClause)` region.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum OutputShape {
	Default,
	None,
	Null,
	Diff,
	After,
	Before,
	Fields,
}

/// The boolean modifiers of a write.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct WriteFlags {
	/// `INSERT IGNORE`.
	pub(crate) ignore: bool,
	/// `INSERT RELATION`.
	pub(crate) relation: bool,
	/// The statement updates the record that is already there instead of failing: an `INSERT`
	/// that carries an `ON DUPLICATE KEY UPDATE` clause, held as a `Clause(DuplicateUpdate)`
	/// region of the subject loop's body, and `RELATE OR UPDATE`, which has no clause.
	pub(crate) or_update: bool,
	/// The statement is an `UPDATE` or a `DELETE` whose `WHERE` condition is a literal that no
	/// record satisfies (`false`, `NONE` or `NULL`), so that no record passes the `WHERE` stage
	/// and the stages after it run for none. Stated at bind from the written condition alone; it
	/// says nothing of the table, which the physical phase reads to decide whether the stages
	/// after the `WHERE` stage are removed.
	pub(crate) where_never: bool,
}

/// What a `logical.content` op writes the record from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ContentMode {
	Content,
	Merge,
	Patch,
	Replace,
	/// `INSERT INTO t { ... }`: the row itself rather than a clause applied to a subject.
	Single,
}

/// A path a statement writes to: `SET a.b[$i] = ...`, `UNSET`, an `INSERT` column, an index column.
///
/// The parts are the language's own, except that a part holding an expression names the operand
/// or region that holds it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Place {
	pub(crate) parts: Box<[PlacePart]>,
}

/// One step of a [`Place`].
///
/// A part that embeds an expression holds it where the op that carries the place can read it: a
/// subscript is an operand, evaluated with no record in scope, and a filter is a region whose
/// argument is the element it judges. A part no place addresses embeds nothing, so nothing it
/// writes is evaluated.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum PlacePart {
	/// `[*]`
	All,
	/// `...`
	Flatten,
	/// `[$]`
	Last,
	/// `[0]`
	First,
	/// `.field`
	Field(Strand),
	/// `[WHERE p]`: a `Clause(Where)` region whose predicate selects the elements.
	Filter(RegionSlot),
	/// `[expr]`: the operand that holds the subscript.
	Dynamic(OperandSlot),
	/// `[n]` where the statement wrote a number: the operand that holds it. It is evaluated as a
	/// [`PlacePart::Dynamic`] is, and differs in how a step that follows a filter counts and in
	/// which element a removal cuts, both of which read the written number.
	Index(OperandSlot),
	/// `.?`
	Optional,
	/// `@`
	Doc,
	/// `.@`
	RepeatRecurse,
	/// A part a write walking the path cannot address: it stops at it, as it does for a path that
	/// names nothing. The text is the part's canonical SurrealQL, kept so the path prints as it
	/// was written. A [`UnaddressedPart::Lookup`] is the exception: the path addresses the key
	/// that the text spells.
	Unaddressed(UnaddressedPart, Box<str>),
}

/// The kinds of part that carry their written text instead of an address.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum UnaddressedPart {
	/// A graph or reference lookup, addressed as the constant key its text spells.
	Lookup,
	/// A destructure.
	Destructure,
	/// A recursion.
	Recurse,
	/// `[START n]`. What it embeds is never evaluated.
	Start,
	/// `.name(args)`. What it embeds is never evaluated.
	Method,
	/// `[WHERE p]` in the path a schema statement writes, which the statement keeps as written.
	/// What it embeds is never evaluated.
	Filter,
}

// ---------------------------------------------------------------------------------------------
// DDL
// ---------------------------------------------------------------------------------------------

/// The authority check that precedes a DDL statement's evaluated operands or its definition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DdlGuard {
	/// The statement refuses to run in a frame that may not write.
	RefuseNoWrite,
	/// The statement requires `action` on `resource` at `base`.
	Allow {
		action: Action,
		resource: ResourceKind,
		base: Base,
	},
}

/// The field of a stored definition that an operand fills when the definition is written.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum TemplateHole {
	Name,
	Comment,
	Value,
	AuthLimit,
	/// A field of one definition type that no other type shares, named as the stored type names
	/// it.
	Other(&'static str),
}

/// A stored definition encoded as it is written, with the fields that are filled at run time
/// left as holes.
///
/// `value` is the encoding of the stored type named by `ty`; every other field is final. Each hole
/// names the field an operand of the op fills before the definition is written.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DefinitionTemplate {
	pub(crate) ty: &'static str,
	pub(crate) value: Value,
	pub(crate) holes: SmallVec<[TemplateHole; 4]>,
}

/// What an `ALTER` replaces in a stored definition: the definition the statement forms by itself,
/// encoded as the stored type encodes it, and the fields of it that the statement sets or drops.
///
/// `value` is the encoding of the stored type named by `ty`, holding the clauses the statement
/// writes and the stored type's defaults elsewhere; the statement replaces exactly the fields in
/// `fields` of the definition it alters with those of `value`. A field the statement drops is in
/// `fields` and holds its default in `value`. `ALTER CONFIG` replaces the whole definition, so its
/// `fields` is empty and `value` is the config as stored; `ALTER API` applies clause by clause, so
/// its `fields` is empty and `value` is an array with one object of the stored values each clause
/// sets.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AlterTemplate {
	pub(crate) ty: &'static str,
	pub(crate) value: Value,
	pub(crate) fields: Box<[Strand]>,
}

/// A permission clause, with its predicate held as a stored region.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum PermissionShape {
	None,
	Full,
	/// A `StoredClause(Permissions)` region holding the predicate.
	Specific(RegionSlot),
}

/// The four permission clauses of a table or a field.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct PermissionsShape {
	pub(crate) select: PermissionShape,
	pub(crate) create: PermissionShape,
	pub(crate) update: PermissionShape,
	pub(crate) delete: PermissionShape,
}

/// A table's materialised view: the tables it reads, the stored query and its backfill.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ViewShape {
	pub(crate) what: Box<[TableName]>,
	/// The `StoredClause(View)` region holding the view's projection, condition and grouping.
	pub(crate) query: RegionSlot,
	/// The `Subquery` region whose rows fill the view once, in the definer's frame: `None` for a
	/// view that is computed by its own select whenever it is read, which holds no rows.
	pub(crate) backfill: Option<RegionSlot>,
	/// For a grouped view, one `Clause(Projection)` region per field of the view, in the order the
	/// view's aggregation analysis lists them. Each takes the document a group's keys and
	/// aggregate values make as its row and yields the field's value. Empty for any other view.
	pub(crate) group_fields: Box<[RegionSlot]>,
}

/// What a `DEFINE FIELD ... REFERENCE` does when the referenced record is deleted.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum OnDeleteShape {
	Reject,
	Ignore,
	Cascade,
	Unset,
	/// A stored region holding the code that runs instead.
	Custom(RegionSlot),
}

/// A field's reference declaration.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ReferenceShape {
	pub(crate) on_delete: OnDeleteShape,
}

/// A field's `DEFAULT` clause.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum DefaultShape {
	None,
	/// `DEFAULT ALWAYS`: a `StoredClause(Default)` region.
	Always(RegionSlot),
	/// `DEFAULT`: a `StoredClause(Default)` region.
	Set(RegionSlot),
}

/// An `ALTER FIELD`'s `DEFAULT` clause.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum AlterDefaultShape {
	None,
	Drop,
	Always(RegionSlot),
	Set(RegionSlot),
}

/// One column of an index.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum IndexColumn {
	/// A plain path, whose subscripts are operands the statement evaluates when it runs.
	Path(Place),
	/// Any other expression: a `StoredClause(IndexColumn)` region.
	Stored(RegionSlot),
}

/// An index's kind and parameters.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum IndexSpec {
	Idx,
	Uniq,
	Hnsw(HnswParams),
	FullText(FullTextParams),
	/// `COUNT`, with the optional condition held as a stored region.
	Count(Option<RegionSlot>),
	DiskAnn(DiskAnnParams),
	Spatial,
}

/// The operands that hold a user's token and session durations.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct UserDurationShape {
	pub(crate) token: OperandSlot,
	pub(crate) session: OperandSlot,
}

/// The operands that hold an access method's durations.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct AccessDurationShape {
	pub(crate) grant: OperandSlot,
	pub(crate) token: OperandSlot,
	pub(crate) session: OperandSlot,
}

/// How a JWT access method verifies a token.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum JwtVerifyShape {
	Key {
		alg: Algorithm,
		key: OperandSlot,
	},
	Jwks {
		url: OperandSlot,
	},
}

/// How a JWT access method issues a token.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct JwtIssueShape {
	pub(crate) alg: Algorithm,
	pub(crate) key: OperandSlot,
}

/// The JWT half of an access method.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct JwtShape {
	pub(crate) verify: JwtVerifyShape,
	pub(crate) issue: Option<JwtIssueShape>,
	pub(crate) audience: Option<Box<[OperandSlot]>>,
}

/// The bearer half of an access method.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct BearerShape {
	pub(crate) kind: BearerAccessType,
	pub(crate) subject: BearerAccessSubject,
	pub(crate) jwt: JwtShape,
}

/// A record access method.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RecordAccessShape {
	/// `StoredClause(Signup)`.
	pub(crate) signup: Option<RegionSlot>,
	/// `StoredClause(Signin)`.
	pub(crate) signin: Option<RegionSlot>,
	pub(crate) jwt: JwtShape,
	pub(crate) bearer: Option<BearerShape>,
}

/// The kind of an access method and everything specific to it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum AccessTypeShape {
	Record(Box<RecordAccessShape>),
	Jwt(JwtShape),
	Bearer(BearerShape),
}

/// One middleware of an API route.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct MiddlewareShape {
	pub(crate) name: Strand,
	/// The operands that hold the arguments.
	pub(crate) args: Box<[OperandSlot]>,
}

/// An API route's middleware and permission.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ApiConfigShape {
	pub(crate) middleware: Box<[MiddlewareShape]>,
	pub(crate) permissions: PermissionShape,
}

/// The handler of one set of methods of an API route.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ApiActionShape {
	pub(crate) methods: Box<[ApiMethod]>,
	/// `StoredClause(Action)`.
	pub(crate) action: RegionSlot,
	pub(crate) config: ApiConfigShape,
}

/// The `DEFAULT` config's namespace and database, held as operands.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct DefaultConfigShape {
	pub(crate) namespace: OperandSlot,
	pub(crate) database: OperandSlot,
}

/// The body of a `DEFINE CONFIG` or `ALTER CONFIG`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ConfigShape {
	GraphQL(GraphQLConfig),
	Api(ApiConfigShape),
	Default(DefaultConfigShape),
}

/// The credentials of a user definition.
///
/// The plaintext password is never an attribute: it reaches the op only as an operand. The
/// derived values are held as written to the definition and never printed.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct UserSecrets {
	pub(crate) hash: String,
	/// Whether the statement supplied `hash`, as opposed to deriving it from the password.
	pub(crate) hash_is_set: bool,
	pub(crate) code: String,
	pub(crate) scram: Option<ScramCredential>,
}

impl ::core::fmt::Debug for UserSecrets {
	fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
		f.debug_struct("UserSecrets")
			.field("hash_is_set", &self.hash_is_set)
			.field("scram", &self.scram.is_some())
			.finish_non_exhaustive()
	}
}

/// The keys and URLs of an access method that its statement writes as text.
///
/// A written credential is an operand that holds a `NONE` marker, and its text is held here, by
/// the slot of the operand it stands for, so that it reaches the stored definition without being
/// an operand, a template value or a printed attribute. Each entry is the text the clause
/// converts to. A credential the statement evaluates, or writes as a value that does not convert,
/// is an ordinary operand and has no entry.
#[derive(Clone, PartialEq, Eq, Hash, Default)]
pub(crate) struct AccessSecrets {
	pub(crate) written: Box<[(OperandSlot, String)]>,
}

impl AccessSecrets {
	/// The text the credential in operand `slot` is written as, if it is written.
	pub(crate) fn written(&self, slot: OperandSlot) -> Option<&str> {
		self.written.iter().find(|(held, _)| *held == slot).map(|(_, text)| text.as_str())
	}
}

impl ::core::fmt::Debug for AccessSecrets {
	fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
		f.debug_struct("AccessSecrets").field("held", &self.written.len()).finish_non_exhaustive()
	}
}

/// Which `DEFINE` form an op defines, with the attributes of that form.
///
/// One variant per form of the language's `DEFINE`. Operands are named by [`OperandSlot`] and
/// stored clauses by [`RegionSlot`]; `comment` is always an operand, and every `StoredClause`
/// region is bound in the definer's frame.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum DefineObject {
	Namespace {
		id: Option<u32>,
		name: OperandSlot,
		comment: OperandSlot,
	},
	Database {
		id: Option<u32>,
		name: OperandSlot,
		strict: bool,
		changefeed: Option<ChangeFeed>,
		comment: OperandSlot,
		/// `COMPATIBILITY <level>`, when the statement writes it.
		compatibility: Option<u8>,
	},
	Function(Box<DefineFunction>),
	Analyzer {
		name: OperandSlot,
		function: Option<Strand>,
		tokenizers: Option<Box<[Tokenizer]>>,
		filters: Option<Box<[Filter]>>,
		comment: OperandSlot,
	},
	Param {
		name: Strand,
		value: OperandSlot,
		comment: OperandSlot,
		/// `StoredClause(Permissions)` when specific.
		permissions: PermissionShape,
	},
	Table(Box<DefineTable>),
	Event {
		name: OperandSlot,
		target_table: OperandSlot,
		/// `StoredClause(When)`.
		when: RegionSlot,
		/// One `StoredClause(Then)` region per expression of the body, in order.
		then: Box<[RegionSlot]>,
		comment: OperandSlot,
		event_kind: EventKind,
	},
	Field(Box<DefineField>),
	Index {
		name: OperandSlot,
		/// The statement's own text of the name, which an existing index's refusal shows.
		name_text: Box<str>,
		what: OperandSlot,
		columns: Box<[IndexColumn]>,
		index: IndexSpec,
		comment: OperandSlot,
		concurrently: bool,
	},
	User(Box<DefineUser>),
	Model {
		hash: Strand,
		name: Strand,
		version: Strand,
		comment: OperandSlot,
		permissions: PermissionShape,
	},
	Module {
		name: Option<String>,
		executable: ModuleExecutable,
		/// `UNSIGNED`.
		unsigned: bool,
		/// Whether the statement names a `SIGNER`.
		signed: bool,
		comment: OperandSlot,
		permissions: PermissionShape,
	},
	Access(Box<DefineAccess>),
	Config(Box<ConfigShape>),
	Api(Box<DefineApi>),
	Bucket {
		name: OperandSlot,
		/// The level the bucket lives at.
		base: Base,
		backend: Option<OperandSlot>,
		permissions: PermissionShape,
		readonly: bool,
		comment: OperandSlot,
	},
	Sequence {
		name: OperandSlot,
		batch: OperandSlot,
		start: OperandSlot,
		timeout: OperandSlot,
	},
	/// A named type, whose whole definition but its id and comment is the op's template.
	Type {
		name: Strand,
		comment: OperandSlot,
		permissions: PermissionShape,
	},
}

/// One change `ALTER TYPE` makes to the labels of a union type.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum EnumEdit {
	/// `ADD VALUE`: `value` is placed before or after `anchor` when one is named (`after`
	/// choosing the side), and last otherwise.
	Add {
		value: String,
		anchor: Option<String>,
		after: bool,
		if_not_exists: bool,
	},
	/// `RENAME VALUE`.
	Rename {
		from: String,
		to: String,
	},
}

/// `DEFINE FUNCTION`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct DefineFunction {
	pub(crate) name: Strand,
	pub(crate) args: Box<[(String, Kind)]>,
	pub(crate) returns: Option<Kind>,
	pub(crate) graphql_alias: Option<String>,
	pub(crate) graphql_deprecated: Option<String>,
	pub(crate) comment: OperandSlot,
	/// `StoredClause(Body)`.
	pub(crate) body: RegionSlot,
	pub(crate) permissions: PermissionShape,
}

/// `DEFINE TABLE`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct DefineTable {
	pub(crate) id: Option<u32>,
	pub(crate) name: OperandSlot,
	pub(crate) drop: bool,
	pub(crate) full: bool,
	pub(crate) view: Option<ViewShape>,
	pub(crate) permissions: PermissionsShape,
	pub(crate) changefeed: Option<ChangeFeed>,
	pub(crate) comment: OperandSlot,
	pub(crate) table_type: TableType,
	pub(crate) graphql_alias: Option<String>,
	pub(crate) graphql_deprecated: Option<String>,
	pub(crate) inline_edges_cap: Option<u32>,
	pub(crate) inline_refs_cap: Option<u32>,
}

/// `DEFINE FIELD`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct DefineField {
	pub(crate) name: OperandSlot,
	pub(crate) what: OperandSlot,
	pub(crate) field_kind: Option<Kind>,
	pub(crate) flexible: bool,
	pub(crate) readonly: bool,
	pub(crate) inline: bool,
	/// `StoredClause(Value)`.
	pub(crate) value: Option<RegionSlot>,
	/// `StoredClause(Assert)`.
	pub(crate) assert: Option<RegionSlot>,
	/// `StoredClause(Computed)`.
	pub(crate) computed: Option<RegionSlot>,
	pub(crate) default: DefaultShape,
	pub(crate) permissions: PermissionsShape,
	pub(crate) comment: OperandSlot,
	pub(crate) reference: Option<ReferenceShape>,
	pub(crate) graphql_alias: Option<String>,
	pub(crate) graphql_deprecated: Option<String>,
	/// `MIGRATE EXISTING ROWS`, as written: its transform is compiled, and its `FALLBACK` read
	/// as a constant, where the statement commits the migration.
	pub(crate) migrate: Option<Box<MigrateClause>>,
	/// The `SQL TYPE` clause, as written.
	pub(crate) sql_type: Option<crate::expr::statements::define::SqlType>,
}

/// `DEFINE USER`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct DefineUser {
	pub(crate) name: OperandSlot,
	pub(crate) base: Base,
	/// The operand that holds the plaintext password, when the statement supplies one.
	pub(crate) password: Option<OperandSlot>,
	pub(crate) secrets: UserSecrets,
	pub(crate) roles: Box<[String]>,
	pub(crate) duration: UserDurationShape,
	pub(crate) comment: OperandSlot,
}

/// `DEFINE ACCESS`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct DefineAccess {
	pub(crate) name: OperandSlot,
	pub(crate) base: Base,
	pub(crate) access_type: AccessTypeShape,
	/// The keys and URLs the statement writes.
	pub(crate) secrets: AccessSecrets,
	/// `StoredClause(Authenticate)`.
	pub(crate) authenticate: Option<RegionSlot>,
	/// `StoredClause(Context)`.
	pub(crate) context: Option<RegionSlot>,
	pub(crate) duration: AccessDurationShape,
	pub(crate) comment: OperandSlot,
}

/// `DEFINE API`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct DefineApi {
	pub(crate) path: OperandSlot,
	pub(crate) actions: Box<[ApiActionShape]>,
	/// `StoredClause(Fallback)`.
	pub(crate) fallback: Option<RegionSlot>,
	pub(crate) config: ApiConfigShape,
	pub(crate) comment: OperandSlot,
}

/// Which `REMOVE` form an op removes, and the operands that name the object.
///
/// One variant per form of the language's `REMOVE`. `IF EXISTS` and `EXPUNGE` are attributes of
/// the op.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum RemoveObject {
	Namespace {
		name: OperandSlot,
	},
	Database {
		name: OperandSlot,
	},
	Function {
		name: Strand,
	},
	Analyzer {
		name: OperandSlot,
	},
	Access {
		name: OperandSlot,
		base: Base,
	},
	Param {
		name: Strand,
	},
	Table {
		name: OperandSlot,
	},
	Event {
		name: OperandSlot,
		table: OperandSlot,
	},
	Field {
		name: OperandSlot,
		table: OperandSlot,
	},
	Index {
		name: OperandSlot,
		table: OperandSlot,
	},
	User {
		name: OperandSlot,
		base: Base,
	},
	Model {
		name: Strand,
		version: Strand,
	},
	Api {
		name: OperandSlot,
	},
	Bucket {
		name: OperandSlot,
		/// The level the bucket lives at.
		base: Base,
	},
	Sequence {
		name: OperandSlot,
	},
	Module {
		name: ModuleName,
	},
	Config {
		kind: ConfigKind,
	},
	Type {
		name: Strand,
		/// The kind of SQL object the stored type must be; `None` removes a type of any kind.
		object: Option<SqlTypeObject>,
	},
}

/// One clause of `ALTER API`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum AlterApiClauseShape {
	ForAny {
		config: Option<ApiConfigShape>,
		/// `StoredClause(Fallback)` when set.
		fallback: AlterKind<RegionSlot>,
	},
	SetAction(ApiActionShape),
	DropAction {
		methods: Box<[ApiMethod]>,
	},
}

/// Which `ALTER` form an op alters, with the attributes of that form.
///
/// One variant per form of the language's `ALTER`. A clause that the matching `DEFINE` stores is
/// a `StoredClause` region; a clause that `DEFINE` evaluates is an operand. `IF EXISTS` is an
/// attribute of the op.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum AlterObject {
	System {
		query_timeout: AlterKind<OperandSlot>,
		compact: bool,
		/// `COMPATIBILITY <level>`: the level a database created without one takes.
		compatibility: Option<u8>,
	},
	Namespace {
		compact: bool,
	},
	Database {
		/// The database the statement names, or `None` for the session's.
		name: Option<OperandSlot>,
		compact: bool,
		/// `COMPATIBILITY <level>`, when the statement writes it.
		compatibility: Option<u8>,
	},
	Table(Box<AlterTable>),
	Api {
		path: OperandSlot,
		clauses: Box<[AlterApiClauseShape]>,
		/// Whether the statement sets the handler security mode.
		security: bool,
		comment: AlterKind<String>,
	},
	Event(Box<AlterEvent>),
	Index {
		name: OperandSlot,
		table: OperandSlot,
		prepare_remove: bool,
		comment: AlterKind<String>,
	},
	Sequence {
		name: OperandSlot,
		timeout: Option<OperandSlot>,
	},
	Field(Box<AlterField>),
	Param {
		name: Strand,
		value: Option<OperandSlot>,
		comment: AlterKind<String>,
		permissions: Option<PermissionShape>,
	},
	Bucket {
		name: OperandSlot,
		/// The level the bucket lives at.
		base: Base,
		backend: AlterKind<String>,
		permissions: Option<PermissionShape>,
		readonly: AlterKind<()>,
		comment: AlterKind<String>,
		revoke_signatures: bool,
	},
	Config {
		inner: Box<ConfigShape>,
		comment: AlterKind<String>,
	},
	Analyzer {
		name: OperandSlot,
		function: AlterKind<String>,
		tokenizers: AlterKind<Box<[Tokenizer]>>,
		filters: AlterKind<Box<[Filter]>>,
		comment: AlterKind<String>,
	},
	Function(Box<AlterFunction>),
	User(Box<AlterUser>),
	Access(Box<AlterAccess>),
	Module {
		name: ModuleName,
		comment: AlterKind<String>,
		permissions: Option<PermissionShape>,
	},
	Type {
		name: Strand,
		edits: Box<[EnumEdit]>,
	},
}

/// `ALTER TABLE`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct AlterTable {
	pub(crate) name: OperandSlot,
	pub(crate) schemafull: AlterKind<()>,
	pub(crate) permissions: Option<PermissionsShape>,
	pub(crate) changefeed: AlterKind<ChangeFeed>,
	pub(crate) comment: AlterKind<String>,
	pub(crate) compact: bool,
	pub(crate) kind: Option<TableType>,
	pub(crate) inline_edges_cap: AlterKind<u32>,
	pub(crate) inline_refs_cap: AlterKind<u32>,
	/// `MIGRATE EXISTING ROWS`, as written.
	pub(crate) migrate: Option<Box<TableMigrateClause>>,
}

/// `ALTER EVENT`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct AlterEvent {
	pub(crate) name: OperandSlot,
	pub(crate) what: OperandSlot,
	/// `StoredClause(When)` when set.
	pub(crate) when: AlterKind<RegionSlot>,
	/// One `StoredClause(Then)` region per expression when set.
	pub(crate) then: AlterKind<Box<[RegionSlot]>>,
	pub(crate) comment: AlterKind<String>,
	pub(crate) kind: AlterKind<EventKind>,
}

/// `ALTER FIELD`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct AlterField {
	pub(crate) name: OperandSlot,
	pub(crate) what: OperandSlot,
	pub(crate) kind: AlterKind<Kind>,
	pub(crate) flexible: AlterKind<()>,
	pub(crate) readonly: AlterKind<()>,
	pub(crate) inline: AlterKind<()>,
	/// `StoredClause(Value)` when set.
	pub(crate) value: AlterKind<RegionSlot>,
	/// `StoredClause(Assert)` when set.
	pub(crate) assert: AlterKind<RegionSlot>,
	pub(crate) default: AlterDefaultShape,
	pub(crate) permissions: Option<PermissionsShape>,
	pub(crate) comment: AlterKind<String>,
	pub(crate) reference: AlterKind<ReferenceShape>,
	/// `MIGRATE EXISTING ROWS`, as written.
	pub(crate) migrate: Option<Box<MigrateClause>>,
	/// Set or drop the `SQL TYPE` clause.
	pub(crate) sql_type: AlterKind<crate::expr::statements::define::SqlType>,
}

/// `ALTER FUNCTION`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct AlterFunction {
	pub(crate) name: Strand,
	pub(crate) args: AlterKind<Box<[(String, Kind)]>>,
	/// `StoredClause(Body)` when set.
	pub(crate) block: AlterKind<RegionSlot>,
	pub(crate) comment: AlterKind<String>,
	pub(crate) permissions: Option<PermissionShape>,
	pub(crate) returns: AlterKind<Kind>,
}

/// `ALTER USER`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct AlterUser {
	pub(crate) name: OperandSlot,
	pub(crate) base: Base,
	/// The operand that holds the plaintext password, when the statement supplies one.
	pub(crate) password: Option<OperandSlot>,
	pub(crate) hash: Option<String>,
	pub(crate) scram: Option<Option<ScramCredential>>,
	pub(crate) roles: AlterKind<Box<[String]>>,
	pub(crate) token_duration: AlterKind<Option<Duration>>,
	pub(crate) session_duration: AlterKind<Option<Duration>>,
	pub(crate) comment: AlterKind<String>,
}

/// `ALTER ACCESS`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct AlterAccess {
	pub(crate) name: OperandSlot,
	pub(crate) base: Base,
	/// `StoredClause(Authenticate)` when set.
	pub(crate) authenticate: AlterKind<RegionSlot>,
	/// `StoredClause(Context)` when set.
	pub(crate) context: AlterKind<RegionSlot>,
	pub(crate) grant_duration: AlterKind<Option<Duration>>,
	pub(crate) token_duration: AlterKind<Option<Duration>>,
	pub(crate) session_duration: AlterKind<Option<Duration>>,
	pub(crate) comment: AlterKind<String>,
}

// ---------------------------------------------------------------------------------------------
// INFO, ACCESS, LIVE, SHOW
// ---------------------------------------------------------------------------------------------

/// What an `INFO` statement describes.
///
/// The operands, in order: the table (`Table`), the user (`User`), the index and its table
/// (`Index`), then the `VERSION` value when the statement carries one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum InfoSubject {
	Root,
	Ns,
	Db,
	Table,
	User {
		base: Option<Base>,
	},
	Index,
}

/// Who an access grant is issued to.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum AccessSubjectShape {
	User(Strand),
	/// The record the grant is issued to: the operand holds its record id.
	Record,
}

/// The operation an `ACCESS` statement performs.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum AccessOp {
	Grant {
		subject: AccessSubjectShape,
	},
	/// A `Clause(Where)` region filters the grants.
	Show {
		gr: Option<Strand>,
	},
	/// A `Clause(Where)` region filters the grants.
	Revoke {
		gr: Option<Strand>,
	},
	Purge {
		kind: PurgeKind,
		grace: Duration,
	},
}

/// What a live query delivers for each change.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum LiveShape {
	/// A diff of the record.
	Diff,
	/// A projection held as a `StoredClause(LiveFields)` region.
	Select,
}

// ---------------------------------------------------------------------------------------------
// MATCH
// ---------------------------------------------------------------------------------------------

/// The bindings a `MATCH` plan declares, indexed by their binding id.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct MatchBindings(pub(crate) Box<[BindingDef]>);

/// One path pattern of a `MATCH` read stage.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct MatchPattern {
	pub(crate) path_var: Option<BindingId>,
	pub(crate) search: Option<PathPrefixPlan>,
	pub(crate) start: NodeStep,
	pub(crate) steps: Box<[(EdgeStep, NodeStep)]>,
}

/// What a `MATCH` update stage writes.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum MatchData {
	/// One `Clause(SetValue)` region per name, in order.
	Set(Box<[String]>),
	Unset(Box<[String]>),
	/// A `Clause(Content)` region.
	Content,
}

/// A node a `MATCH` insert stage creates. Its properties are a `Clause(Content)` region.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct MatchInsertNode {
	pub(crate) binding: BindingId,
	pub(crate) label: TableName,
}

/// An edge a `MATCH` insert stage creates. Its properties are a `Clause(Content)` region.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct MatchInsertEdge {
	pub(crate) binding: BindingId,
	pub(crate) label: TableName,
	pub(crate) from: BindingId,
	pub(crate) to: BindingId,
}

/// One output column of a `MATCH` plan. Its expression is a `Clause(Projection)` region.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct MatchColumnShape {
	pub(crate) name: String,
	pub(crate) hidden: bool,
}

/// The attributes of a `MATCH` plan's output stage.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct MatchOutputShape {
	pub(crate) columns: Box<[MatchColumnShape]>,
	pub(crate) distinct: bool,
	/// Ascending (`true`) or descending, one per `Clause(OrderKey)` region.
	pub(crate) order: Box<[bool]>,
}

/// A single-hop expansion off the node bound at `source`. The predicate a candidate row must
/// satisfy, when there is one, is a `Clause(Where)` region evaluated against the candidate row.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct BindExpandShape {
	pub(crate) source: String,
	pub(crate) direction: ExpandDir,
	/// The edge tables traversed; none means every edge table.
	pub(crate) edge_tables: Box<[TableName]>,
	pub(crate) edge_binding: EdgeBinding,
	pub(crate) target_binding: String,
	pub(crate) target_label: Option<TableName>,
	pub(crate) predicate: bool,
	/// An input row with no survivor yields one row with the edge and the target bound to `NULL`.
	pub(crate) optional: bool,
}

/// A quantified expansion off the node bound at `source`: every path the pattern admits, or the
/// ones a shortest-path selector keeps.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct BindPathShape {
	pub(crate) source: String,
	pub(crate) direction: ExpandDir,
	pub(crate) edge_tables: Box<[TableName]>,
	pub(crate) min: u32,
	pub(crate) max: Option<u32>,
	pub(crate) target_binding: String,
	pub(crate) target_label: Option<TableName>,
	/// The binding the edges of a path are bound to, as an array.
	pub(crate) group_binding: Option<String>,
	/// The binding the alternating node and edge array of a path is bound to.
	pub(crate) path_binding: Option<String>,
	pub(crate) mode: PathMode,
	/// `None` enumerates every path.
	pub(crate) selector: Option<ShortestSelector>,
	/// The pattern was anchored on its far node, so the paths are reversed to read in the order
	/// the pattern is written.
	pub(crate) reversed: bool,
}

/// A join of the rows of the probe operand with the rows of the build operand. The residual
/// predicate, when there is one, is a `Clause(Where)` region evaluated against the merged row.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct BindJoinShape {
	/// The node bindings both sides share, on which the join is keyed.
	pub(crate) keys: Box<[String]>,
	pub(crate) join_type: JoinType,
	/// The bindings the build side introduces, which an unmatched probe row of a left join is
	/// filled with `NULL` for.
	pub(crate) null_template: Box<[String]>,
	pub(crate) residual: bool,
}

/// A node an insert step creates; its properties are a `Clause(Content)` region.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct BindInsertNode {
	pub(crate) name: String,
	pub(crate) table: TableName,
}

/// An edge an insert step creates; its properties are a `Clause(Content)` region.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct BindInsertEdge {
	pub(crate) name: String,
	pub(crate) table: TableName,
	pub(crate) from: String,
	pub(crate) to: String,
}

/// One step of a `MATCH` plan's binding table: a node that turns each binding row of its operand
/// into none, one or several rows. A binding row is an object keyed by binding name, and every
/// clause region is evaluated against the row the step produces or consumes, as the step
/// documents.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum BindStepShape {
	/// Each row wrapped as `{ name: row }`.
	Bind {
		name: String,
	},
	/// The node at the `in` or `out` of the edge bound at `edge`, bound as `name` beside it.
	Endpoint {
		edge: String,
		field: EndpointField,
		name: String,
		label: Option<TableName>,
	},
	Expand(Box<BindExpandShape>),
	Path(Box<BindPathShape>),
	/// Keeps the rows in which no edge record is bound twice across the bindings `edges`.
	DistinctEdges {
		edges: Box<[String]>,
	},
	/// Two operands: the build rows, then the probe rows.
	Join(Box<BindJoinShape>),
	/// The first of each distinct row.
	Distinct,
	/// Runs its operand to completion and answers no rows.
	Drain,
	/// `SET`, `REMOVE` or `CONTENT` of the record bound at `target`: the clause regions of
	/// [`MatchData`].
	Update {
		target: String,
		data: MatchData,
	},
	/// `[DETACH|NODETACH] DELETE` of the record bound at `target`. An edge has no connected edges
	/// to refuse.
	Delete {
		target: String,
		detach: DetachMode,
		is_edge: bool,
	},
	/// `INSERT` of new nodes and edges: one `Clause(Content)` region per node, then per edge.
	Insert {
		nodes: Box<[BindInsertNode]>,
		edges: Box<[BindInsertEdge]>,
	},
}

impl BindStepShape {
	/// How many operands the step takes: the rows it extends, and for a join the build rows first.
	pub(crate) fn operands(&self) -> u16 {
		match self {
			BindStepShape::Join(_) => 2,
			BindStepShape::Bind {
				..
			}
			| BindStepShape::Endpoint {
				..
			}
			| BindStepShape::Expand(_)
			| BindStepShape::Path(_)
			| BindStepShape::DistinctEdges {
				..
			}
			| BindStepShape::Distinct
			| BindStepShape::Drain
			| BindStepShape::Update {
				..
			}
			| BindStepShape::Delete {
				..
			}
			| BindStepShape::Insert {
				..
			} => 1,
		}
	}

	/// How many `Subquery` regions the step holds after its clause regions, one per write the step
	/// applies to a record: an update and a delete one, an insert one per node, then one per edge.
	pub(crate) fn write_regions(&self) -> usize {
		match self {
			BindStepShape::Update {
				..
			}
			| BindStepShape::Delete {
				..
			} => 1,
			BindStepShape::Insert {
				nodes,
				edges,
			} => nodes.len() + edges.len(),
			BindStepShape::Bind {
				..
			}
			| BindStepShape::Endpoint {
				..
			}
			| BindStepShape::Expand(_)
			| BindStepShape::Path(_)
			| BindStepShape::DistinctEdges {
				..
			}
			| BindStepShape::Join(_)
			| BindStepShape::Distinct
			| BindStepShape::Drain => 0,
		}
	}

	/// The kinds of the regions the step holds, in order: its clause regions, then the
	/// `Subquery` region of each write it applies ([`Self::write_regions`]).
	pub(crate) fn region_kinds(&self) -> RegionKinds {
		let mut kinds = self.clause_kinds();
		kinds.extend(std::iter::repeat_n(RegionKind::Subquery, self.write_regions()));
		kinds
	}

	/// The kinds of the clause regions the step holds, in order.
	fn clause_kinds(&self) -> RegionKinds {
		let clause = RegionKind::Clause;
		match self {
			BindStepShape::Expand(expand) => {
				expand.predicate.then(|| clause(ClauseRole::Where)).into_iter().collect()
			}
			BindStepShape::Join(join) => {
				join.residual.then(|| clause(ClauseRole::Where)).into_iter().collect()
			}
			BindStepShape::Update {
				data,
				..
			} => match data {
				MatchData::Set(names) => {
					std::iter::repeat_n(clause(ClauseRole::SetValue), names.len()).collect()
				}
				MatchData::Unset(_) => SmallVec::new(),
				MatchData::Content => std::iter::once(clause(ClauseRole::Content)).collect(),
			},
			BindStepShape::Insert {
				nodes,
				edges,
			} => std::iter::repeat_n(clause(ClauseRole::Content), nodes.len() + edges.len())
				.collect(),
			BindStepShape::Bind {
				..
			}
			| BindStepShape::Endpoint {
				..
			}
			| BindStepShape::Path(_)
			| BindStepShape::DistinctEdges {
				..
			}
			| BindStepShape::Distinct
			| BindStepShape::Drain
			| BindStepShape::Delete {
				..
			} => SmallVec::new(),
		}
	}

	/// Whether the step reads records or their edges from storage.
	#[deny(clippy::wildcard_enum_match_arm)]
	pub(crate) fn reads(&self) -> bool {
		match self {
			BindStepShape::Endpoint {
				..
			}
			| BindStepShape::Expand(_)
			| BindStepShape::Path(_) => true,
			BindStepShape::Bind {
				..
			}
			| BindStepShape::DistinctEdges {
				..
			}
			| BindStepShape::Join(_)
			| BindStepShape::Distinct
			| BindStepShape::Drain
			| BindStepShape::Update {
				..
			}
			| BindStepShape::Delete {
				..
			}
			| BindStepShape::Insert {
				..
			} => false,
		}
	}

	/// Whether the step writes records.
	#[deny(clippy::wildcard_enum_match_arm)]
	pub(crate) fn writes(&self) -> bool {
		match self {
			BindStepShape::Update {
				..
			}
			| BindStepShape::Delete {
				..
			}
			| BindStepShape::Insert {
				..
			} => true,
			BindStepShape::Bind {
				..
			}
			| BindStepShape::Endpoint {
				..
			}
			| BindStepShape::Expand(_)
			| BindStepShape::Path(_)
			| BindStepShape::DistinctEdges {
				..
			}
			| BindStepShape::Join(_)
			| BindStepShape::Distinct
			| BindStepShape::Drain => false,
		}
	}
}

/// What a schema statement's body checks it against in the catalog, and what those checks decide.
/// The planner fills it when the statement is planned, and lowering takes it from the statement's
/// op and reads no catalog. A statement whose names are evaluated is checked by its program against
/// the catalog it finds when it runs, and has none.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) enum DdlChecks {
	#[default]
	None,
	/// `DEFINE TABLE`: the refusal of the definition once the table is read,
	/// and the definition's type as its checks leave it.
	DefineTable {
		/// The refusal of the view the statement defines, which the catalog raises as it forms the
		/// view's definition from the statement; it is raised after the authority check, unless
		/// the statement's own clauses are refused first.
		view_refusal: Option<FieldRefusal>,
		definition_refusal: Option<FieldRefusal>,
		table_type: crate::catalog::TableType,
		/// For a table whose name is written, the definition its clauses
		/// form, which the statement judges against the table it finds when
		/// it runs.
		redefined: Option<std::sync::Arc<crate::catalog::TableDefinition>>,
	},
	/// `DEFINE FIELD`: the table and the field as the catalog holds them, the
	/// table's fields (`None` where they could not be read), and the
	/// refusals of the definition's checks: `refused` of those the body makes
	/// before `FLEXIBLE`'s, and `flexible` of `FLEXIBLE`'s. The record id
	/// components an `id` definition names are not judged here: what they
	/// are judged against includes the rows the table's views hold, which
	/// the statements run before this one may change, so the statement judges
	/// them when it runs (`FieldCheck::KeyComponents`), between the two.
	DefineField {
		existing_table: Option<std::sync::Arc<crate::catalog::TableDefinition>>,
		existing_field: Option<std::sync::Arc<crate::catalog::FieldDefinition>>,
		fields: Option<std::sync::Arc<[crate::catalog::FieldDefinition]>>,
		indexed: Option<FieldRefusal>,
		refused: Option<FieldRefusal>,
		flexible: Option<FieldRefusal>,
	},
	/// `REMOVE TABLE`, which checks nothing of the catalog when it is
	/// planned: the tables a view reads from, the views that read the table
	/// and the edges of the relations over it are read when it runs
	/// (`StoreInstr::ViewSources`, `StoreInstr::RefuseRemovingViewed`,
	/// `StoreInstr::RefuseRemovingLightweight`).
	RemoveTable,
	/// `REMOVE FIELD` and `ALTER FIELD`: the table, the field and the
	/// table's fields as the catalog holds them — the fields nested in the
	/// field, which `REMOVE FIELD` removes with it — and the refusal of the
	/// field's name against the record id components of the table: a
	/// component name has no definition of its own to remove or alter.
	/// Whether a definition still reads a component of the `id` a `REMOVE
	/// FIELD` removes is judged when the statement runs
	/// (`FieldCheck::IdUnread`), against the rows the views over the table
	/// hold then.
	Field {
		existing_table: Option<std::sync::Arc<crate::catalog::TableDefinition>>,
		existing_field: Option<std::sync::Arc<crate::catalog::FieldDefinition>>,
		fields: std::sync::Arc<[crate::catalog::FieldDefinition]>,
		key_refusal: Option<FieldRefusal>,
	},
}
