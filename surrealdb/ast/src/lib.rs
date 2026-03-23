//! # Surrealdb AST
//!
//! This crate is the internal library of SurrealDB. It contains the implemention of the surrealql
//! abstract syntax tree.
//!
//! <section class="warning">
//! <h3>Unstable!</h3>
//! This crate is <b>SurrealDB internal API</b>. It does not adhere to SemVer and its API is
//! free to change and break code even between patch versions. If you are looking for a stable
//! interface to the SurrealDB library please have a look at
//! <a href="https://crates.io/crates/surrealdb">the Rust SDK</a>.
//! </section>

use std::ops::Bound;
use std::time::Duration;

use chrono::{DateTime as BaseDateTime, Utc};
use common::ids::IdSet;
use common::span::Span;
pub use rust_decimal::Decimal;
pub use uuid::Uuid;
pub type DateTime = BaseDateTime<Utc>;

mod mac;
mod types;
pub mod vis;
//mod visit;

pub use types::{AstSpan, Node, NodeId, NodeList, NodeListId, Spanned, UniqueNode};

use crate::mac::{ast_type, impl_vis_debug, impl_vis_type};
use crate::types::library;

type NodeSet<T> = IdSet<u32, T>;

// Macro implementing the syntax sugar for working with the ast.
library! {
	/// Type containing all the possible node types.
	#[derive(Debug)]
	Library{
		top_level_expr: Vec<TopLevelExpr>,
		top_level_exprs: Vec<NodeList<TopLevelExpr>>,

		transactions: Vec<Transaction>,

		kill: Vec<Kill>,
		use_stmt: Vec<Use>,
		option_stmt: Vec<OptionStmt>,

		if_stmt: Vec<If>,
		for_stmt: Vec<For>,
		let_stmt: Vec<Let>,
		return_stmt: Vec<Return>,
		info_stmt: Vec<Info>,
		show_stmt: Vec<Show>,
		create_stmt: Vec<Create>,
		update_stmt: Vec<Update>,
		upsert_stmt: Vec<Upsert>,
		delete_stmt: Vec<Delete>,
		relate_stmt: Vec<Relate>,
		select_stmt: Vec<Select>,
		insert_stmt: Vec<Insert>,
		rebuild_stmt: Vec<Rebuild>,
		access_stmt: Vec<Access>,

		define_ns_stmt: Vec<DefineNamespace>,
		define_db_stmt: Vec<DefineDatabase>,
		define_function_stmt: Vec<DefineFunction>,
		define_table_stmt: Vec<DefineTable>,
		define_module: Vec<DefineModule>,
		define_param: Vec<DefineParam>,
		define_event: Vec<DefineEvent>,
		define_field: Vec<DefineField>,
		define_index: Vec<DefineIndex>,
		define_bucket: Vec<DefineBucket>,
		define_sequence: Vec<DefineSequence>,
		define_config: Vec<DefineConfig>,
		define_user: Vec<DefineUser>,
		define_access: Vec<DefineAccess>,

		remove_ns_stmt: Vec<RemoveNamespace>,
		remove_db_stmt: Vec<RemoveDatabase>,
		remove_function_stmt: Vec<RemoveFunction>,
		remove_table_stmt: Vec<RemoveTable>,
		remove_module: Vec<RemoveModule>,
		remove_param: Vec<RemoveParam>,
		remove_event: Vec<RemoveEvent>,
		remove_field: Vec<RemoveField>,
		remove_index: Vec<RemoveIndex>,
		remove_bucket: Vec<RemoveBucket>,
		remove_sequence: Vec<RemoveSequence>,
		remove_user: Vec<RemoveUser>,
		remove_access: Vec<RemoveAccess>,
		remove_analyzer: Vec<RemoveAnalyzer>,
		remove_api: Vec<RemoveApi>,

		alter_system: Vec<AlterSystem>,
		alter_ns_stmt: Vec<AlterNamespace>,
		alter_db_stmt: Vec<AlterDatabase>,
		alter_table_stmt: Vec<AlterTable>,
		alter_field_stmt: Vec<AlterField>,
		alter_index_stmt: Vec<AlterIndex>,
		alter_sequence_stmt: Vec<AlterSequence>,

		explain_stmt: Vec<Explain>,

		order: Vec<Order>,
		orders: Vec<NodeList<Order>>,

		filter: Vec<Filter>,
		filters: Vec<NodeList<Filter>>,
		define_analyzer: Vec<DefineAnalyzer>,

		api_action: Vec<ApiAction>,
		api_middleware: Vec<ApiMiddleware>,
		api_middlewares: Vec<NodeList<ApiMiddleware>>,
		define_api: Vec<DefineApi>,

		expr: Vec<Expr>,
		exprs: Vec<NodeList<Expr>>,
		exprs_id: Vec<NodeListId<Expr>>,
		exprs_ids: Vec<NodeList<NodeListId<Expr>>>,

		block: Vec<Block>,

		builtin: Vec<Builtin>,
		floats: Vec<Spanned<f64>>,
		integer: Vec<Integer>,
		decimal: Vec<Spanned<Decimal>>,
		uuid: Vec<Spanned<Uuid>>,
		datetime: Vec<Spanned<DateTime>>,
		duration: Vec<Spanned<Duration>>,
		str: Vec<StringLit>,
		bytes: Vec<BytesLit>,
		files: Vec<FileLit>,
		regex: Vec<Regex>,
		js_function: Vec<JsFunction>,

		record_id: Vec<RecordId>,
		record_id_key: Vec<RecordIdKey>,
		record_id_key_range: Vec<RecordIdKeyRange>,

		mock: Vec<Mock>,
		closure: Vec<Closure>,

		point: Vec<Point>,
		array: Vec<Array>,
		set: Vec<Set>,
		object: Vec<Object>,
		object_entry: Vec<ObjectEntry>,
		object_entrys: Vec<NodeList<ObjectEntry>>,

		binary: Vec<BinaryExpr>,
		postfix: Vec<PostfixExpr>,
		prefix: Vec<PrefixExpr>,

		lookup_subject: Vec<LookupSubject>,
		lookup_subjects: Vec<NodeList<LookupSubject>>,
		lookup: Vec<Lookup>,
		recurse: Vec<Recurse>,
		idiom: Vec<IdiomExpr>,

		destructure: Vec<Destructure>,
		destructures: Vec<NodeList<Destructure>>,

		place: Vec<Place>,
		places: Vec<NodeList<Place>>,
		present_place: Vec<PresentPlace>,
		present_places: Vec<NodeList<PresentPlace>>,

		assignment: Vec<Assignment>,
		assignments: Vec<NodeList<Assignment>>,

		field: Vec<Fields>,
		list_selector: Vec<ListSelector>,
		list_selectors: Vec<NodeList<ListSelector>>,
		selector: Vec<Selector>,

		output: Vec<Output>,

		parameter: Vec<Parameter>,
		parameters: Vec<NodeList<Parameter>>,

		type_: Vec<Type>,
		types: Vec<NodeList<Type>>,
		prime_type: Vec<PrimeType>,
		prime_types: Vec<NodeList<PrimeType>>,
		lit_array_type: Vec<LitArrayType>,
		lit_object_type: Vec<LitObjectType>,
		lit_object_entry_type: Vec<LitObjectTypeEntry>,
		lit_object_entry_types: Vec<NodeList<LitObjectTypeEntry>>,
		array_like_type: Vec<ArrayLikeType>,
		geometry_type: Vec<GeometryType>,
		geometry_sub_type: Vec<GeometrySubType>,
		geometry_sub_types: Vec<NodeList<GeometrySubType>>,
		record_table_type: Vec<IdentListType>,

		path: Vec<Path>,
		paths: Vec<NodeList<Path>>,
		path_segment: Vec<PathSegment>,
		path_segments: Vec<NodeList<PathSegment>>,

		params: Vec<Param>,
		ident: Vec<Ident>,
		idents: Vec<NodeList<Ident>>,

		span: Vec<Span>,

		#[set]
		strings: NodeSet<String>,
	}
}
pub type Ast = types::Ast<Library>;

impl UniqueNode for String {}
impl Node for f64 {}
impl Node for Decimal {}
impl Node for Uuid {}
impl Node for DateTime {}
impl Node for Duration {}

ast_type! {
	pub struct Query {
		pub exprs: Option<NodeListId<TopLevelExpr>>,
	}
}

ast_type! {
	pub enum TopLevelExpr {
		Transaction(NodeId<Transaction>),
		Use(NodeId<Use>),
		Option(NodeId<OptionStmt>),
		Kill(NodeId<Kill>),
		Show(NodeId<Show>),
		Expr(NodeId<Expr>),
	}
}

ast_type! {
	pub struct Transaction {
		pub statements: Option<NodeListId<TopLevelExpr>>,
		pub commits: bool,
	}
}

ast_type! {
	pub enum KillKind {
		Uuid(NodeId<Spanned<Uuid>>),
		Param(NodeId<Param>)
	}
}

ast_type! {
	pub struct Kill {
		pub kind: KillKind,
	}
}

impl_vis_type! {
	#[derive(Debug)]
	pub enum UseKind {
		Namespace(NodeId<Ident>),
		NamespaceDatabase{
			namespace: NodeId<Ident>,
			database: NodeId<Ident>
		},
		Database(NodeId<Ident>),
	}
}

ast_type! {
	pub struct Use {
		pub kind: UseKind,
	}
}

ast_type! {
	pub struct OptionStmt {
		pub name: NodeId<Ident>,
		pub value: bool,
	}
}

ast_type! {
	pub struct If{
		pub condition: NodeId<Expr>,
		pub then: NodeId<Expr>,
		pub otherwise: Option<NodeId<Expr>>,
	}
}

ast_type! {
	pub struct For{
		pub param: NodeId<Param>,
		pub range: NodeId<Expr>,
		pub body: NodeId<Block>,
	}
}

ast_type! {
	pub struct Let{
		pub param: NodeId<Param>,
		pub ty: Option<NodeId<Type>>,
		pub expr: NodeId<Expr>,
	}
}

ast_type! {
	pub struct Return{
		pub expr: NodeId<Expr>,
		pub fetch: Option<NodeListId<Expr>>,
	}
}

#[derive(Debug)]
pub enum Base {
	Namespace,
	Database,
	Root,
}

impl_vis_type! {
	#[derive(Debug)]
	pub enum InfoKind {
		Root,
		Namespace,
		Database {
			version: Option<NodeId<Expr>>,
		},
		Table {
			name: NodeId<Expr>,
			version: Option<NodeId<Expr>>,
		},
		User {
			name: NodeId<Expr>,
			base: Option<Base>,
		},
		Index {
			name: NodeId<Expr>,
			table: NodeId<Expr>,
		},
	}
}

ast_type! {
	pub struct Info{
		pub kind: InfoKind,
		pub structure: bool,
	}
}

ast_type! {
	pub enum ShowTarget{
		Database(Span),
		Table(NodeId<Ident>)
	}
}

ast_type! {
	pub enum ShowSince{
		Timestamp(NodeId<Spanned<DateTime>>),
		VersionStamp(NodeId<Integer>),
	}
}

ast_type! {
	pub enum ExplainClause{
		Base(Span),
		Full(Span),
	}
}

ast_type! {
	pub struct Show{
		pub target: ShowTarget,
		pub since: ShowSince,
		pub limit: Option<NodeId<Expr>>,
	}
}

ast_type! {
	pub enum WithIndex{
		None(Span),
		Some(NodeListId<Ident>)
	}
}

ast_type! {
	pub enum AssignmentOp{
		Assign(Span),
		Add(Span),
		Subtract(Span),
		Extend(Span),
	}
}

ast_type! {
	pub struct Assignment{
		pub place: NodeId<Place>,
		pub op: AssignmentOp,
		pub value: NodeId<Expr>,
	}
}

ast_type! {
	pub enum RecordData{
		Set(NodeListId<Assignment>),
		Unset(NodeListId<Place>),
		Content(NodeId<Expr>),
		Patch(NodeId<Expr>),
		Merge(NodeId<Expr>),
		Replace(NodeId<Expr>),
	}
}

ast_type! {
	pub struct Selector{
		pub expr: NodeId<Expr>,
		pub alias: Option<NodeId<Place>>,
	}
}

ast_type! {
	pub enum ListSelector{
		All(Span),
		Selector(Selector),
	}
}

ast_type! {
	pub enum Fields{
		Value(NodeId<Selector>),
		List(NodeListId<ListSelector>),
	}
}

ast_type! {
	pub enum Output{
		None(Span),
		Null(Span),
		Diff(Span),
		After(Span),
		Before(Span),
		Fields(NodeId<Fields>),
	}
}

ast_type! {
	pub struct Create{
		pub only: bool,
		pub targets: NodeListId<Expr>,
		pub data: Option<RecordData>,
		pub output: Option<NodeId<Output>>,
		pub version: Option<NodeId<Expr>>,
		pub timeout: Option<NodeId<Expr>>,
	}
}

ast_type! {
	pub struct Delete{
		pub only: bool,
		pub targets: NodeListId<Expr>,
		pub with_index: Option<WithIndex>,
		pub condition: Option<NodeId<Expr>>,
		pub output: Option<NodeId<Output>>,
		pub timeout: Option<NodeId<Expr>>,
		pub explain: Option<ExplainClause>,
	}
}

ast_type! {
	pub struct Update{
		pub only: bool,
		pub targets: NodeListId<Expr>,
		pub with_index: Option<WithIndex>,
		pub data: Option<RecordData>,
		pub condition: Option<NodeId<Expr>>,
		pub output: Option<NodeId<Output>>,
		pub timeout: Option<NodeId<Expr>>,
		pub explain: Option<ExplainClause>,
	}
}

ast_type! {
	pub struct Upsert{
		pub only: bool,
		pub targets: NodeListId<Expr>,
		pub with_index: Option<WithIndex>,
		pub data: Option<RecordData>,
		pub condition: Option<NodeId<Expr>>,
		pub output: Option<NodeId<Output>>,
		pub timeout: Option<NodeId<Expr>>,
		pub explain: Option<ExplainClause>,
	}
}

ast_type! {
	pub struct Relate{
		pub only: bool,
		pub from: NodeId<Expr>,
		pub through: NodeId<Expr>,
		pub to: NodeId<Expr>,
		pub data: Option<RecordData>,
		pub output: Option<NodeId<Output>>,
		pub timeout: Option<NodeId<Expr>>,
	}
}

impl_vis_type! {
	#[derive(Debug)]
	pub enum OrderDirection{
		Ascending,
		Descending,
	}
}

ast_type! {
	pub struct Order{
		pub expr: NodeId<Expr>,
		pub collate: bool,
		pub numeric: bool,
		pub direction: Option<OrderDirection>,
	}
}

ast_type! {
	pub enum OrderBy{
		Rand(Span),
		List(NodeListId<Order>),
	}
}

impl_vis_type! {
	#[derive(Debug)]
	pub enum Group{
		All,
		Fields(NodeListId<Expr>),
	}
}

ast_type! {
	pub struct Select{
		pub fields: NodeId<Fields>,
		pub omit: Option<NodeListId<Expr>>,
		pub only: bool,
		pub from: NodeListId<Expr>,
		pub with_index: Option<WithIndex>,
		pub condition: Option<NodeId<Expr>>,
		pub split: Option<NodeListId<Expr>>,
		pub group: Option<Group>,
		pub order: Option<OrderBy>,
		pub start: Option<NodeId<Expr>>,
		pub limit: Option<NodeId<Expr>>,
		pub version: Option<NodeId<Expr>>,
		pub timeout: Option<NodeId<Expr>>,
		pub fetch: Option<NodeListId<Expr>>,
		pub tempfiles: bool,
		pub explain: Option<ExplainClause>,
	}
}

impl_vis_type! {
	#[derive(Debug)]
	pub enum InsertInto{
		Param(NodeId<Param>),
		Table(NodeId<Ident>),
	}
}

ast_type! {
	pub struct InsertTuples {
		pub places: NodeListId<Place>,
		pub values: NodeListId<NodeListId<Expr>>,
	}
}

impl_vis_type! {
	#[derive(Debug)]
	pub enum InsertData{
		Expr(NodeId<Expr>),
		Tuples(InsertTuples),
	}
}

ast_type! {
	pub struct Insert{
		pub relation: bool,
		pub ignore: bool,
		pub into: Option<InsertInto>,
		pub data: InsertData,
		pub on_duplicate: Option<NodeListId<Assignment>>,
		pub output: Option<NodeId<Output>>,
		pub version: Option<NodeId<Expr>>,
		pub timeout: Option<NodeId<Expr>>,
	}
}

ast_type! {
	pub struct Rebuild{
		pub if_exists: bool,
		pub name: NodeId<Expr>,
		pub table: NodeId<Expr>,
		pub concurrently: bool,
	}
}

ast_type! {
	pub enum AccessSubject{
		User(NodeId<Ident>),
		Subject(NodeId<RecordId>),
	}
}

ast_type! {
	pub struct AccessGrant{
		pub subject: AccessSubject,
	}
}

impl_vis_type! {
	#[derive(Debug)]
	pub enum AccessShowKind{
		All,
		Condition(NodeId<Expr>),
		Grant(NodeId<Ident>),
	}
}

ast_type! {
	pub struct AccessShow{
		pub kind: AccessShowKind,
	}
}

impl_vis_type! {
	#[derive(Debug)]
	pub enum AccessRevokeKind{
		All,
		Condition(NodeId<Expr>),
		Grant(NodeId<Ident>),
	}
}

ast_type! {
	pub struct AccessRevoke{
		pub kind: AccessRevokeKind,
	}
}

impl_vis_type! {
	#[derive(Debug)]
	pub enum PurgeKind{
		Expired,
		Revoked,
		Both,
	}
}

ast_type! {
	pub struct AccessPurge{
		pub kind: PurgeKind,
		pub grace: NodeId<Spanned<Duration>>,
	}
}

ast_type! {
	pub enum AccessKind{
		Grant(AccessGrant),
		Show(AccessShow),
		Revoke(AccessRevoke),
		Purge(AccessPurge),
	}
}

ast_type! {
	pub struct Access{
		pub access: NodeId<Ident>,
		pub base : Option<Base>,
		pub kind: AccessKind,
	}
}

#[derive(Debug)]
pub enum DefineKind {
	Create,
	IfNotExists,
	Overwrite,
}
impl_vis_debug!(DefineKind);

ast_type! {
	#[derive(Clone)]
	pub enum Permission{
		None(Span),
		Full(Span),
		Where(NodeId<Expr>),
	}
}

ast_type! {
	pub struct DefineNamespace{
		pub kind: DefineKind,
		pub name: NodeId<Expr>,
		pub comment: Option<NodeId<Expr>>,
	}
}

ast_type! {
	pub enum ChangeFeed{
		Base(NodeId<Spanned<Duration>>),
		WithOriginal(NodeId<Spanned<Duration>>),
	}
}

ast_type! {
	pub struct DefineDatabase{
		pub kind: DefineKind,
		pub name: NodeId<Expr>,
		pub strict: bool,
		pub changefeed: Option<ChangeFeed>,
		pub comment: Option<NodeId<Expr>>,
	}
}

ast_type! {
	pub struct Parameter{
		pub name: NodeId<Param>,
		pub ty: Option<NodeId<Type>>,
	}
}

ast_type! {
	pub struct DefineFunction{
		pub kind: DefineKind,
		pub name: NodeId<Path>,
		pub parameters: Option<NodeListId<Parameter>>,
		pub return_ty: Option<NodeId<Type>>,
		pub body: NodeId<Block>,
		pub comment: Option<NodeId<Expr>>,
		pub permission: Option<Permission>,
	}
}
ast_type! {
	pub enum ModuleName{
		File(NodeId<FileLit>),
		Path(NodeId<Path>),
	}
}

ast_type! {
	pub struct DefineModule{
		pub kind: DefineKind,
		pub subject: ModuleName,
		pub alias: Option<NodeId<Path>>,
		pub comment: Option<NodeId<Expr>>,
		pub permission: Option<Permission>,
	}
}

ast_type! {
	pub struct DefineParam{
		pub kind: DefineKind,
		pub param: NodeId<Param>,
		pub value: Option<NodeId<Expr>>,
		pub comment: Option<NodeId<Expr>>,
		pub permission: Option<Permission>,
	}
}

ast_type! {
	pub struct RelationTable{
		pub from: Option<NodeListId<Ident>>,
		pub to: Option<NodeListId<Ident>>,
		pub enforced: bool,
	}
}

ast_type! {
	pub enum TableKind{
		Normal(Span),
		Relation(RelationTable),
		Any(Span),
	}
}

#[derive(Debug)]
pub enum Schema {
	Less,
	Full,
}
impl_vis_debug!(Schema);

impl_vis_type! {
	#[derive(Debug)]
	pub struct TablePermissions{
		pub create: Option<Permission>,
		pub delete: Option<Permission>,
		pub update: Option<Permission>,
		pub select: Option<Permission>,
	}
}

ast_type! {
	pub struct DefineTable{
		pub kind: DefineKind,
		pub name: NodeId<Expr>,
		pub comment: Option<NodeId<Expr>>,
		pub drop: bool,
		pub schema: Option<Schema>,
		pub table_kind: Option<TableKind>,
		pub permission: Option<TablePermissions>,
		pub changefeed: Option<ChangeFeed>,
		pub view: Option<NodeId<Select>>,
	}
}

ast_type! {
	pub struct ApiMiddleware{
		pub path: NodeId<Path>,
		pub args: Option<NodeListId<Expr>>,
	}
}

ast_type! {
	pub struct ApiAction{
		pub middleware: Option<NodeListId<ApiMiddleware>>,
		pub permission: Option<Permission>,
		pub action: NodeId<Expr>,
	}
}

impl_vis_type! {
	#[derive(Debug)]
	pub struct DefineMethodApiActions {
		pub get: Option<NodeId<ApiAction>>,
		pub delete: Option<NodeId<ApiAction>>,
		pub patch: Option<NodeId<ApiAction>>,
		pub post: Option<NodeId<ApiAction>>,
		pub put: Option<NodeId<ApiAction>>,
		pub trace: Option<NodeId<ApiAction>>,
	}
}

ast_type! {
	pub struct DefineApi{
		pub kind: DefineKind,
		pub path: NodeId<Expr>,
		pub base_middleware: Option<NodeListId<ApiMiddleware>>,
		pub base_permission: Option<Permission>,
		pub fallback: Option<NodeId<Expr>>,
		pub methods: DefineMethodApiActions,
		pub comment: Option<NodeId<Expr>>,
	}
}

ast_type! {
	pub struct DefineEventAsync{
		pub retry: Option<NodeId<Integer>>,
		pub max_depth: Option<NodeId<Integer>>,
	}
}

ast_type! {
	pub struct DefineEvent{
		pub kind: DefineKind,
		pub name: NodeId<Expr>,
		pub table: NodeId<Expr>,
		pub condition: Option<NodeId<Expr>>,
		pub then: NodeListId<Expr>,
		pub comment: Option<NodeId<Expr>>,
		pub async_: Option<DefineEventAsync>,
	}
}

ast_type! {
	pub enum FieldDefault{
		Always(NodeId<Expr>),
		Some(NodeId<Expr>),
	}
}

impl_vis_type! {
	#[derive(Debug)]
	pub enum OnDelete{
		Reject,
		Ignore,
		Cascade,
		Unset,
		Then(NodeId<Expr>),
	}
}

impl_vis_type! {
	#[derive(Debug)]
	pub struct FieldPermissions{
		pub create: Option<Permission>,
		pub update: Option<Permission>,
		pub select: Option<Permission>,
	}
}

ast_type! {
	pub struct DefineField{
		pub kind: DefineKind,
		pub name: NodeId<Expr>,
		pub table: NodeId<Expr>,
		pub ty: Option<NodeId<Type>>,
		pub flexible: bool,
		pub readonly: bool,
		pub value: Option<NodeId<Expr>>,
		pub assert: Option<NodeId<Expr>>,
		pub computed: Option<NodeId<Expr>>,
		pub default: Option<FieldDefault>,
		pub permissions: Option<FieldPermissions>,
		pub comment: Option<NodeId<Expr>>,
		// NOTE: maybe move into own struct if `REFERENCE` gets more subclauses.
		/// `REFERENCE ON DELETE` clause
		pub on_delete: Option<OnDelete>
	}
}

ast_type! {
	pub struct CountIndex{
		pub condition: Option<NodeId<Expr>>,
	}
}

impl_vis_type! {
	#[derive(Debug)]
	pub enum FullTextScoring {
		VectorSearch,
		Bm25 {
			k1: NodeId<Spanned<f64>>,
			b: NodeId<Spanned<f64>>,
		},
	}
}

ast_type! {
	pub struct FullTextIndex{
		pub analyzer: Option<NodeId<Ident>>,
		pub highlights: bool,
		pub scoring: Option<FullTextScoring>,
	}
}

#[derive(Debug)]
pub enum VectorType {
	F64,
	F32,
	I64,
	I32,
	I16,
}
impl_vis_debug!(VectorType);

ast_type! {
	pub struct HnswIndex{
		pub dimension: NodeId<Integer>,
		pub distance: Option<Distance>,
		pub ty: Option<VectorType>,
		pub m: Option<NodeId<Integer>>,
		pub m0: Option<NodeId<Integer>>,
		pub ml: Option<NodeId<Integer>>,
		pub ef_construction: Option<NodeId<Integer>>,
		pub extend_candidates: bool,
		pub keep_pruned_connections: bool,
		pub use_hashed_vector: bool,
	}
}

ast_type! {
	pub enum Index{
		Unique(Span),
		Count(CountIndex),
		FullText(FullTextIndex),
		Hnsw(HnswIndex)
	}
}

ast_type! {
	pub struct DefineIndex{
		pub kind: DefineKind,
		pub name: NodeId<Expr>,
		pub table: NodeId<Expr>,
		pub fields: Option<NodeListId<Expr>>,
		pub comment: Option<NodeId<Expr>>,
		pub index: Option<Index>,
		pub concurrently: bool,
	}
}

ast_type! {
	pub struct NgramMapper{
		pub min: NodeId<Integer>,
		pub max: NodeId<Integer>,
	}
}

ast_type! {
	pub enum Filter{
		Ascii(Span),
		Lowercase(Span),
		Uppercase(Span),
		EdgeNgram(NgramMapper),
		Ngram(NgramMapper),
		Snowball(NodeId<Ident>),
		Mapper(NodeId<StringLit>),
	}
}

ast_type! {
	pub struct DefineAnalyzer{
		pub kind: DefineKind,
		pub name: NodeId<Expr>,
		pub filters: Option<NodeListId<Filter>>,
		pub tokenizer: Option<NodeListId<Ident>>,
		pub function: Option<NodeId<Path>>,
		pub comment: Option<NodeId<Expr>>,
	}
}

ast_type! {
	pub struct DefineBucket{
		pub kind: DefineKind,
		pub name: NodeId<Expr>,
		pub backend: Option<NodeId<Expr>>,
		pub permission: Option<Permission>,
		pub comment: Option<NodeId<Expr>>,
		pub readonly: bool,
	}
}

ast_type! {
	pub struct DefineSequence{
		pub kind: DefineKind,
		pub name: NodeId<Expr>,
		pub batch: Option<NodeId<Expr>>,
		pub start: Option<NodeId<Expr>>,
		pub timeout: Option<NodeId<Expr>>,
	}
}

ast_type! {
	pub struct DefineConfigApi{
		pub permission: Option<Permission>,
		pub middleware: Option<NodeListId<ApiMiddleware>>,
	}
}

#[derive(Debug)]
pub enum GraphqlIntrospection {
	None,
	Auto,
}
impl_vis_debug!(GraphqlIntrospection);

impl_vis_type! {
	#[derive(Debug)]
	pub enum TablesConfig{
		None,
		Auto,
		Include(NodeListId<Ident>),
		Exclude(NodeListId<Ident>),
	}
}

impl_vis_type! {
	#[derive(Debug)]
	pub enum FunctionConfig{
		None,
		Auto,
		Include(NodeListId<Path>),
		Exclude(NodeListId<Path>),
	}
}

ast_type! {
	pub struct DefineConfigGraphql{
		pub introspection: Option<GraphqlIntrospection>,
		pub table_config: Option<TablesConfig>,
		pub function_config: Option<FunctionConfig>,
		pub depth_limit: Option<NodeId<Integer>>,
		pub complexity_limit: Option<NodeId<Integer>>,
	}
}

ast_type! {
	pub struct DefineConfigDefault{
		pub namespace: Option<NodeId<Expr>>,
		pub database: Option<NodeId<Expr>>,
	}
}

ast_type! {
	pub enum DefineConfigKind{
		Api(DefineConfigApi),
		Graphql(DefineConfigGraphql),
		Default(DefineConfigDefault),
	}
}

ast_type! {
	pub struct DefineConfig{
		pub kind: DefineKind,
		pub inner: DefineConfigKind,
	}
}

ast_type! {
	pub enum UserSecret{
		PassHash(NodeId<StringLit>),
		PassWord(NodeId<StringLit>),
	}
}

ast_type! {
	pub struct DefineUser {
		pub kind: DefineKind,
		pub name: NodeId<Expr>,
		pub base: Base,
		pub secret: Option<UserSecret>,
		pub roles: Option<NodeListId<Ident>>,
		pub session_duration: Option<NodeId<Expr>>,
		pub token_duration: Option<NodeId<Expr>>,
		pub comment: Option<NodeId<Expr>>,
	}
}

impl_vis_type! {
	#[derive(Debug)]
	pub enum Algorithm{
		EdDSA,
		Es256,
		Es384,
		Es512,
		Hs256,
		Hs384,
		Hs512,
		Ps256,
		Ps384,
		Ps512,
		Rs256,
		Rs384,
		Rs512,
	}
}

impl_vis_type! {
	#[derive(Debug)]
	pub enum JwtVerify{
		Key{
			algorithm: Algorithm,
			key: NodeId<Expr>,
		},
		Jwks{
			url: NodeId<Expr>,
		},
	}
}

impl_vis_type! {
	#[derive(Debug)]
	pub struct JwtIssue{
		pub algorithm: Option<Algorithm>,
		pub key: Option<NodeId<Expr>>,
	}
}

impl_vis_type! {
	#[derive(Debug)]
	pub struct Jwt{
		pub verify: JwtVerify,
		pub issue: Option<JwtIssue>,
	}
}

impl_vis_type! {
	#[derive(Debug)]
	pub enum BearerAccessSubject{
		User,
		Record,
	}
}

impl_vis_type! {
	#[derive(Debug)]
	pub struct BearerAccess{
		pub subject: BearerAccessSubject,
		pub jwt: Option<Jwt>
	}
}

impl_vis_type! {
	#[derive(Debug)]
	pub struct RecordAccess{
		pub signup: Option<NodeId<Expr>>,
		pub signin: Option<NodeId<Expr>>,
		pub jwt: Option<Jwt>,
		pub refresh: bool,
	}
}

impl_vis_type! {
	#[derive(Debug)]
	pub enum AccessType{
		Jwt(Jwt),
		Record(RecordAccess),
		Bearer(BearerAccess),
	}
}

ast_type! {
	pub struct DefineAccess{
		pub kind: DefineKind,
		pub name: NodeId<Expr>,
		pub base: Base,
		pub comment: Option<NodeId<Expr>>,
		pub duration_session: Option<NodeId<Expr>>,
		pub duration_token: Option<NodeId<Expr>>,
		pub duration_grant: Option<NodeId<Expr>>,
		pub authenticate: Option<NodeId<Expr>>,
		pub ty: Option<AccessType>,
	}
}

ast_type! {
	pub struct RemoveNamespace{
		pub if_exists: bool,
		pub expunge: bool,
		pub name: NodeId<Expr>,
	}
}

ast_type! {
	pub struct RemoveDatabase{
		pub if_exists: bool,
		pub expunge: bool,
		pub name: NodeId<Expr>,
	}
}

ast_type! {
	pub struct RemoveFunction{
		pub if_exists: bool,
		pub name: NodeId<Path>,
	}
}

ast_type! {
	pub struct RemoveModule{
		pub if_exists: bool,
		pub name: NodeId<Path>,
	}
}

ast_type! {
	pub struct RemoveAccess{
		pub if_exists: bool,
		pub name: NodeId<Expr>,
		pub base: Base,
	}
}

ast_type! {
	pub struct RemoveParam{
		pub if_exists: bool,
		pub name: NodeId<Param>,
	}
}

ast_type! {
	pub struct RemoveTable{
		pub expunge: bool,
		pub if_exists: bool,
		pub name: NodeId<Expr>,
	}
}

ast_type! {
	pub struct RemoveEvent{
		pub if_exists: bool,
		pub name: NodeId<Expr>,
		pub table: NodeId<Expr>,
	}
}

ast_type! {
	pub struct RemoveField{
		pub if_exists: bool,
		pub name: NodeId<Expr>,
		pub table: NodeId<Expr>,
	}
}

ast_type! {
	pub struct RemoveIndex{
		pub if_exists: bool,
		pub name: NodeId<Expr>,
		pub table: NodeId<Expr>,
	}
}

ast_type! {
	pub struct RemoveAnalyzer{
		pub if_exists: bool,
		pub name: NodeId<Expr>,
	}
}

ast_type! {
	pub struct RemoveSequence{
		pub if_exists: bool,
		pub name: NodeId<Expr>,
	}
}

ast_type! {
	pub struct RemoveUser{
		pub if_exists: bool,
		pub name: NodeId<Expr>,
		pub base: Base,
	}
}

ast_type! {
	pub struct RemoveApi{
		pub if_exists: bool,
		pub name: NodeId<Expr>,
	}
}

ast_type! {
	pub struct RemoveBucket{
		pub if_exists: bool,
		pub name: NodeId<Expr>,
	}
}

#[derive(Debug)]
pub enum AlterKind<T> {
	Drop(Span),
	Set(T),
}

ast_type! {
	pub struct AlterSystem{
		pub query_timeout: Option<AlterKind<NodeId<Expr>>>,
		pub compact: bool,
	}
}

ast_type! {
	pub struct AlterNamespace{
		pub if_exists: bool,
		pub name: NodeId<Expr>,
		pub compact: bool,
	}
}

ast_type! {
	pub struct AlterDatabase{
		pub if_exists: bool,
		pub name: NodeId<Expr>,
		pub compact: bool,
	}
}

ast_type! {
	pub struct AlterTable{
		pub if_exists: bool,
		pub name: NodeId<Expr>,
		pub comment: Option<AlterKind<NodeId<Expr>>>,
		pub changefeed: Option<AlterKind<ChangeFeed>>,
		pub schema: Option<Schema>,
		pub table_kind: Option<TableKind>,
		pub compact: bool,
		pub permissions: Option<TablePermissions>,
	}
}

ast_type! {
	pub struct AlterField{
		pub if_exists: bool,
		pub name: NodeId<Expr>,
		pub table: NodeId<Expr>,
		pub ty: Option<AlterKind<NodeId<Type>>>,
		pub flexible: Option<AlterKind<()>>,
		pub readonly: Option<AlterKind<()>>,
		pub value: Option<AlterKind<NodeId<Expr>>>,
		pub assert: Option<AlterKind<NodeId<Expr>>>,
		pub default: Option<AlterKind<FieldDefault>>,
		// NOTE: maybe move into own struct if `REFERENCE` gets more subclauses.
		/// `REFERENCE ON DELETE` clause
		pub on_delete: Option<AlterKind<OnDelete>>,
		pub comment: Option<AlterKind<NodeId<Expr>>>,
		pub permissions: Option<FieldPermissions>,
	}
}

ast_type! {
	pub struct AlterIndex{
		pub if_exists: bool,
		pub name: NodeId<Expr>,
		pub table: NodeId<Expr>,
		pub comment: Option<AlterKind<NodeId<Expr>>>,
		pub prepare_remove: bool,
	}
}

ast_type! {
	pub struct AlterSequence{
		pub if_exists: bool,
		pub name: NodeId<Expr>,
		pub timeout: Option<AlterKind<NodeId<Expr>>>,
	}
}

impl_vis_type! {
	#[derive(Debug)]
	pub enum ExplainFormat{
		Json,
		Text,
	}
}

ast_type! {
	pub struct Explain{
		pub analyze: bool,
		pub format: Option<ExplainFormat>,
		pub expr: NodeId<Expr>,
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
		String(NodeId<StringLit>),

		// `@` or ommited when parsing `<-`,`<~` or similar.
		Document(NodeId<Span>),

		Regex(NodeId<Regex>),

		Uuid(NodeId<Spanned<Uuid>>),
		DateTime(NodeId<Spanned<DateTime>>),
		Duration(NodeId<Spanned<Duration>>),
		File(NodeId<FileLit>),
		Bytes(NodeId<BytesLit>),

		Point(NodeId<Point>),

		Array(NodeId<Array>),
		Object(NodeId<Object>),
		Set(NodeId<Set>),

		Mock(NodeId<Mock>),
		Closure(NodeId<Closure>),
		UnboundedRange(NodeId<Span>),

		JsFunction(NodeId<JsFunction>),

		RecordId(NodeId<RecordId>),

		Block(NodeId<Block>),

		Path(NodeId<Path>),
		Param(NodeId<Param>),

		Binary(NodeId<BinaryExpr>),
		Postfix(NodeId<PostfixExpr>),
		Idiom(NodeId<IdiomExpr>),
		Prefix(NodeId<PrefixExpr>),

		Throw(NodeId<Expr>),
		If(NodeId<If>),
		For(NodeId<For>),
		Let(NodeId<Let>),
		Return(NodeId<Return>),
		Info(NodeId<Info>),
		Create(NodeId<Create>),
		Update(NodeId<Update>),
		Upsert(NodeId<Upsert>),
		Delete(NodeId<Delete>),
		Relate(NodeId<Relate>),
		Select(NodeId<Select>),
		Insert(NodeId<Insert>),
		Rebuild(NodeId<Rebuild>),
		Access(NodeId<Access>),

		Sleep(NodeId<Spanned<Duration>>),

		Continue(NodeId<Span>),
		Break(NodeId<Span>),

		DefineNamespace(NodeId<DefineNamespace>),
		DefineDatabase(NodeId<DefineDatabase>),
		DefineTable(NodeId<DefineTable>),
		DefineFunction(NodeId<DefineFunction>),
		DefineModule(NodeId<DefineModule>),
		DefineParam(NodeId<DefineParam>),
		DefineApi(NodeId<DefineApi>),
		DefineEvent(NodeId<DefineEvent>),
		DefineField(NodeId<DefineField>),
		DefineIndex(NodeId<DefineIndex>),
		DefineAnalyzer(NodeId<DefineAnalyzer>),
		DefineBucket(NodeId<DefineBucket>),
		DefineSequence(NodeId<DefineSequence>),
		DefineConfig(NodeId<DefineConfig>),
		DefineUser(NodeId<DefineUser>),
		DefineAccess(NodeId<DefineAccess>),

		RemoveNamespace(NodeId<RemoveNamespace>),
		RemoveDatabase(NodeId<RemoveDatabase>),
		RemoveTable(NodeId<RemoveTable>),
		RemoveFunction(NodeId<RemoveFunction>),
		RemoveModule(NodeId<RemoveModule>),
		RemoveParam(NodeId<RemoveParam>),
		RemoveApi(NodeId<RemoveApi>),
		RemoveEvent(NodeId<RemoveEvent>),
		RemoveField(NodeId<RemoveField>),
		RemoveIndex(NodeId<RemoveIndex>),
		RemoveAnalyzer(NodeId<RemoveAnalyzer>),
		RemoveBucket(NodeId<RemoveBucket>),
		RemoveSequence(NodeId<RemoveSequence>),
		RemoveUser(NodeId<RemoveUser>),
		RemoveAccess(NodeId<RemoveAccess>),

		AlterSystem(NodeId<AlterSystem>),
		AlterNamespace(NodeId<AlterNamespace>),
		AlterDatabase(NodeId<AlterDatabase>),
		AlterTable(NodeId<AlterTable>),
		AlterField(NodeId<AlterField>),
		AlterIndex(NodeId<AlterIndex>),
		AlterSequence(NodeId<AlterSequence>),

		Explain(NodeId<Explain>)
	}
}

ast_type! {
	pub struct Block{
		pub exprs: Option<NodeListId<Expr>>,
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

ast_type! {
	pub struct Ident {
		pub text: NodeId<String>,
	}
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
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

impl Integer {
	pub fn with_sign(mut self, sign: Sign) -> Self {
		self.sign = sign;
		self
	}

	pub fn into_f64(&self) -> f64 {
		let v = self.value as f64;
		if let Sign::Minus = self.sign {
			-v
		} else {
			v
		}
	}

	pub fn into_i64(&self) -> Option<i64> {
		const I64_MAX: u64 = i64::MAX as u64;
		const I64_NEG_MIN: u64 = (i64::MAX as u64) + 1;

		match (self.sign, self.value) {
			(Sign::Plus, 0..=I64_MAX) => Some(self.value as i64),
			(Sign::Minus, 0..=I64_MAX) => Some(-(self.value as i64)),
			(Sign::Minus, I64_NEG_MIN) => Some(i64::MIN),
			_ => None,
		}
	}
}

ast_type! {
	pub struct StringLit {
		pub text: NodeId<String>,
	}
}

ast_type! {
	pub struct BytesLit{
		pub text: Vec<u8>,
	}
}

ast_type! {
	pub struct FileLit{
		pub path: NodeId<String>,
	}
}

ast_type! {
	pub struct Regex {
		pub source: NodeId<String>,
	}
}

ast_type! {
	pub struct JsFunctionBody{
		pub source: NodeId<String>,
	}
}

ast_type! {
	pub struct JsFunction{
		pub args: Option<NodeListId<Expr>>,
		pub body: JsFunctionBody,
	}
}

ast_type! {
	#[derive(Copy, Clone)]
	pub struct RecordId{
		pub name: NodeId<Ident>,
		pub key: NodeId<RecordIdKey>,
	}
}

ast_type! {
	#[derive(Copy, Clone)]
	pub enum RecordIdKey{
		String(NodeId<StringLit>),
		Number(Spanned<i64>),
		Uuid(NodeId<Spanned<Uuid>>),
		Object(NodeId<Object>),
		Array(NodeId<Array>),
		Range(NodeId<RecordIdKeyRange>),
		Generate(Spanned<RecordIdKeyGenerate>),
	}
}

ast_type! {
	pub struct RecordIdKeyRange{
		pub start: Bound<NodeId<RecordIdKey>>,
		pub end: Bound<NodeId<RecordIdKey>>,
	}
}

#[derive(Copy, Clone, Debug)]
pub enum RecordIdKeyGenerate {
	Ulid,
	Uuid,
	Rand,
}
impl_vis_debug!(RecordIdKeyGenerate);

impl_vis_type! {
	#[derive(Debug)]
	pub enum MockKind {
		Integer(NodeId<Integer>),
		Range {
			start: Bound<NodeId<Integer>>,
			end: Bound<NodeId<Integer>>,
		},
	}
}

ast_type! {
	pub struct Mock{
		pub name: NodeId<Ident>,
		pub kind: MockKind
	}
}

ast_type! {
	pub struct Closure{
		pub parameters: Option<NodeListId<Parameter>>,
		pub output_ty: Option<NodeId<Type>>,
		pub body: NodeId<Expr>,
	}
}

ast_type! {
	pub struct Point{
		pub x: f64,
		pub y: f64,
	}
}

ast_type! {
	#[derive(Copy, Clone)]
	pub struct Array{
		pub entries: Option<NodeListId<Expr>>,
	}
}

ast_type! {
	#[derive(Copy, Clone)]
	pub struct Set{
		pub entries: Option<NodeListId<Expr>>,
	}
}

ast_type! {
	#[derive(Copy, Clone)]
	pub struct Object{
		pub entries: Option<NodeListId<ObjectEntry>>,
	}
}

ast_type! {
	#[derive(Copy, Clone)]
	pub struct ObjectEntry{
		pub key: NodeId<String>,
		pub value: NodeId<Expr>,
	}
}

ast_type! {
	pub struct Param {
		pub text: NodeId<String>,
	}
}

impl_vis_type! {
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
		reference: Option<NodeId<Integer>>,
		operator: Option<MatchesOperator>,
	},
	KNearestNeighbour {
		k: NodeId<Integer>,
		distance: Distance,
	},
	KTree {
		k: NodeId<Integer>,
	},
	KApproximate {
		k: NodeId<Integer>,
		ef: NodeId<Integer>,
	},
}

ast_type! {
	pub struct BinaryExpr {
		pub left: NodeId<Expr>,
		pub op: Spanned<BinaryOperator>,
		pub right: NodeId<Expr>,
	}
}

impl_vis_type! {
	#[derive(Debug)]
	pub enum PostfixOperator {
		Range,
		RangeSkip,

		/// .field(EXPR*)
		MethodCall{
			name: NodeId<String>,
			arguments: Option<NodeListId<Expr>>
		},
		/// (EXPR*)
		Call(Option<NodeListId<Expr>>),
	}
}

ast_type! {
	pub struct PostfixExpr {
		pub left: NodeId<Expr>,
		pub op: Spanned<PostfixOperator>,
	}
}

ast_type! {
	pub enum PrefixOperator {
		Not(Span),
		Negate(Span),
		Positive(Span),
		Range(Span),
		RangeInclusive(Span),
		Cast(NodeId<Type>),
	}
}

ast_type! {
	pub struct PrefixExpr {
		pub op: PrefixOperator,
		pub right: NodeId<Expr>,
	}
}

impl_vis_type! {
	#[derive(Debug)]
	pub enum DestructureOperator {
		/// { field.* }
		All,
		/// { field : EXPR }
		Expr(NodeId<Expr>),
		/// { field.{ .. } }
		Destructure(Option<NodeListId<Destructure>>),
	}
}

ast_type! {
	pub struct Destructure {
		pub field: NodeId<Ident>,
		pub op: Option<Spanned<DestructureOperator>>,
	}
}

#[derive(Debug)]
pub enum Direction {
	// `<-` or `<~`
	In,
	// `->` or `~>`
	Out,
	// `<->` or `<~>`
	Both,
}
impl_vis_debug!(Direction);

ast_type! {
	pub struct LookupSubjectRange{
		pub table: NodeId<Ident>,
		pub range: NodeId<RecordIdKeyRange>,
		pub field: Option<NodeId<Ident>>,
	}
}

ast_type! {
	pub struct LookupSubjectTable{
		pub table: NodeId<Ident>,
		pub field: Option<NodeId<Ident>>,
	}
}

ast_type! {
	pub enum LookupSubject{
		Range(LookupSubjectRange),
		Table(LookupSubjectTable),
	}
}

ast_type! {
	pub struct BasicLookup{
		pub from: Option<NodeListId<LookupSubject>>,
		pub condition:  Option<NodeId<Expr>>,
		pub limit: Option<NodeId<Expr>>,
		pub start: Option<NodeId<Expr>>,
		pub alias: Option<NodeId<PresentPlace>>,
	}
}

ast_type! {
	pub struct SelectLookup{
		pub fields: NodeId<Fields>,
		pub from: Option<NodeListId<LookupSubject>>,
		pub condition:  Option<NodeId<Expr>>,
		pub split: Option<NodeListId<Expr>>,
		pub group: Option<Group>,
		pub order: Option<OrderBy>,
		pub limit: Option<NodeId<Expr>>,
		pub start: Option<NodeId<Expr>>,
		pub alias: Option<NodeId<PresentPlace>>,
	}
}

ast_type! {
	pub enum Lookup{
		Any(Span),
		Subject(NodeId<LookupSubject>),
		Basic(BasicLookup),
		Select(SelectLookup),
	}
}

impl_vis_type! {
	#[derive(Debug)]
	pub enum RecurseKind{
		Path{
			inclusive: bool,
		},
		Collect{
			inclusive: bool,
		},
		Shortest{
			expects: NodeId<Expr>,
			inclusive: bool,
		},
	}
}

ast_type! {
	pub struct Recurse{
		pub start: Bound<NodeId<Integer>>,
		pub end: Bound<NodeId<Integer>>,
		pub kind: Option<RecurseKind>,
		pub expr: NodeId<Expr>,
	}
}

impl_vis_type! {
	#[derive(Debug)]
	pub enum IdiomOperator {
		/// `[*]` | `.*`
		All,
		/// `[$]`
		Last,
		/// `...`
		Flatten,
		/// `.field`
		Field(NodeId<Ident>),
		/// `[EXPR]`
		Index(NodeId<Expr>),
		/// `[? EXPR]` | `[WHERE EXPR\]`
		Where(NodeId<Expr>),
		/// `.?`
		Option,
		/// `.@`
		Repeat,
		/// `.{ .. }`
		Destructure(Option<NodeListId<Destructure>>),
		/// (1, $bar)
		Call(Option<NodeListId<Expr>>),
		/// `<-`, `->` or `<->`
		Graph{
			direction: Direction,
			lookup: NodeId<Lookup>,
		},
		/// `<~`
		Reference(NodeId<Lookup>),
		Recurse(NodeId<Recurse>),
	}
}

ast_type! {
	pub struct IdiomExpr{
		pub left: NodeId<Expr>,
		pub op: Spanned<IdiomOperator>,
	}
}

ast_type! {
	pub struct MemberPlace{
		pub lhs: NodeId<Place>,
		pub name: NodeId<Ident>,
	}
}

ast_type! {
	pub struct IndexPlace{
		pub lhs: NodeId<Place>,
		pub index: NodeId<Expr>,
	}
}

ast_type! {
	pub enum Place{
		Field(NodeId<Ident>),
		Member(MemberPlace),
		Index(IndexPlace),
	}
}

ast_type! {
	pub struct MemberPresentPlace{
		pub lhs: NodeId<PresentPlace>,
		pub name: NodeId<Ident>,
	}
}

ast_type! {
	pub struct IndexPresentPlace{
		pub lhs: NodeId<PresentPlace>,
		pub index: NodeId<Expr>,
	}
}

ast_type! {
	pub struct AllPresentPlace{
		pub lhs: NodeId<PresentPlace>,
	}
}

ast_type! {
	pub struct LastPresentPlace{
		pub lhs: NodeId<PresentPlace>,
	}
}

ast_type! {
	pub enum PresentPlace{
		Field(NodeId<Ident>),
		Member(MemberPresentPlace),
		Index(IndexPresentPlace),
		All(AllPresentPlace),
		Last(LastPresentPlace),
	}
}

ast_type! {
	pub struct IdentListType{
		pub idents: Option<NodeListId<Ident>>,
	}
}

ast_type! {
	pub enum GeometrySubType {
		Point(Span),
		Line(Span),
		Polygon(Span),
		MultiPoint(Span),
		MultiLine(Span),
		MultiPolygon(Span),
		Collection(Span),
	}
}

ast_type! {
	pub struct GeometryType{
		pub types: Option<NodeListId<GeometrySubType>>,
	}
}

ast_type! {
	pub struct ArrayLikeType{
		pub ty: Option<NodeId<Type>>,
		pub size : Option<NodeId<Integer>>,
	}
}

ast_type! {
	pub struct LitObjectType{
		pub entries: Option<NodeListId<LitObjectTypeEntry>>
	}
}

ast_type! {
	pub struct LitObjectTypeEntry{
		pub name: NodeId<Ident>,
		pub ty: NodeId<Type>,
	}
}

ast_type! {
	pub struct LitArrayType{
		pub entries: Option<NodeListId<Type>>,
	}
}

ast_type! {
	pub enum PrimeType {
		None(Span),
		Null(Span),
		Bool(Span),
		Bytes(Span),
		DateTime(Span),
		Duration(Span),
		Decimal(Span),
		Number(Span),
		Float(Span),
		Integer(Span),
		Object(Span),
		String(Span),
		Regex(Span),
		Uuid(Span),
		Range(Span),
		Function(Span),
		Record(NodeId<IdentListType>),
		Table(NodeId<IdentListType>),
		Geometry(NodeId<GeometryType>),
		Array(NodeId<ArrayLikeType>),
		Set(NodeId<ArrayLikeType>),
		File(NodeId<IdentListType>),
		LitBuiltin(NodeId<Builtin>),
		LitFloat(NodeId<Spanned<f64>>),
		LitInteger(NodeId<Integer>),
		LitString(NodeId<StringLit>),
		LitDecimal(NodeId<Spanned<Decimal>>),
		LitObject(NodeId<LitObjectType>),
		LitArray(NodeId<LitArrayType>),
		LitDuration(NodeId<Spanned<Duration>>),
	}
}

ast_type! {
	pub enum Type {
		Any(Span),
		Prime(NodeListId<PrimeType>),
	}
}

ast_type! {
	pub struct Version{
		pub major: u64,
		pub minor: u64,
		pub patch: u64,
	}
}

ast_type! {
	pub enum PathSegment{
		Ident(NodeId<Ident>),
		Version(Version),
	}
}

ast_type! {
	// `field`, `math::max`, `silo::module::pkg::<1.2.3>`
	pub struct Path{
		pub start: NodeId<Ident>,
		pub parts: Option<NodeListId<PathSegment>>,
	}
}
