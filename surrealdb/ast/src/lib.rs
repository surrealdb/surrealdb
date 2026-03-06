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
mod visit;

pub use types::{AstSpan, Node, NodeId, NodeList, NodeListId, Spanned, UniqueNode};

use crate::mac::{ast_type, impl_vis_debug, impl_vis_type};

type NodeSet<T> = IdSet<u32, T>;

library! {
	/// Type containing all the possible node types.
	#[derive(Debug)]
	Library{
		query: Vec<Query>,

		top_level_expr: Vec<TopLevelExpr>,
		top_level_exprs: Vec<NodeList<TopLevelExpr>>,

		transactions: Vec<Transaction>,

		kill: Vec<Kill>,
		use_stmt: Vec<Use>,
		option_stmt: Vec<OptionStmt>,

		if_stmt: Vec<If>,
		let_stmt: Vec<Let>,
		info_stmt: Vec<Info>,
		show_stmt: Vec<Show>,
		create_stmt: Vec<Create>,
		update_stmt: Vec<Update>,
		upsert_stmt: Vec<Upsert>,
		delete_stmt: Vec<Delete>,
		relate_stmt: Vec<Relate>,
		select_stmt: Vec<Select>,
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
		define_access: Vec<DefineAccess>,

		filter: Vec<Filter>,
		filters: Vec<NodeList<Filter>>,
		define_analyzer: Vec<DefineAnalyzer>,

		api_action: Vec<ApiAction>,
		api_middleware: Vec<ApiMiddleware>,
		api_middlewares: Vec<NodeList<ApiMiddleware>>,
		define_api: Vec<DefineApi>,

		expr: Vec<Expr>,
		exprs: Vec<NodeList<Expr>>,

		block: Vec<Block>,

		builtin: Vec<Builtin>,
		floats: Vec<Spanned<f64>>,
		integer: Vec<Integer>,
		decimal: Vec<Spanned<Decimal>>,
		uuid: Vec<Spanned<Uuid>>,
		datetime: Vec<Spanned<DateTime>>,
		duration: Vec<Spanned<Duration>>,
		str: Vec<StringLit>,
		files: Vec<FileLit>,
		regex: Vec<Regex>,
		js_function: Vec<JsFunction>,

		record_id: Vec<RecordId>,
		record_id_key: Vec<RecordIdKey>,
		record_id_key_range: Vec<RecordIdKeyRange>,

		mock: Vec<Mock>,

		point: Vec<Point>,
		array: Vec<Array>,
		set: Vec<Set>,
		object: Vec<Object>,
		object_entry: Vec<ObjectEntry>,
		object_entrys: Vec<NodeList<ObjectEntry>>,

		binary: Vec<BinaryExpr>,
		postfix: Vec<PostfixExpr>,
		prefix: Vec<PrefixExpr>,
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

		fetch: Vec<Fetch>,
		fetchs: Vec<NodeList<Fetch>>,

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
	pub struct Let{
		pub param: NodeId<Param>,
		pub ty: Option<NodeId<Type>>,
		// TODO: Kind,
		pub expr: NodeId<Expr>,
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
	pub enum Explain{
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
		pub timeout: Option<NodeId<Expr>>,
		pub explain: Option<Explain>,
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
		pub explain: Option<Explain>,
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
		pub explain: Option<Explain>,
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

ast_type! {
	pub enum OrderBy{
		Rand(Span),
		Places(NodeListId<PresentPlace>),
	}
}

ast_type! {
	pub struct FetchTypeFields{
		pub args: Option<NodeListId<Expr>>,
	}
}

ast_type! {
	pub struct FetchTypeField{
		pub arg: NodeId<Expr>,
	}
}

ast_type! {
	pub enum Fetch{
		Param(NodeId<Param>),
		Place(NodeId<PresentPlace>),
		TypeField(FetchTypeField),
		TypeFields(FetchTypeFields),
	}
}

ast_type! {
	pub struct Select{
		pub fields: NodeId<Fields>,
		pub only: bool,
		pub from: NodeListId<Expr>,
		pub with_index: Option<WithIndex>,
		pub condition: Option<NodeId<Expr>>,
		pub split: Option<NodeListId<PresentPlace>>,
		pub group: Option<NodeListId<PresentPlace>>,
		pub order: Option<OrderBy>,
		pub start: Option<NodeId<Expr>>,
		pub limit: Option<NodeId<Expr>>,
		pub version: Option<NodeId<Expr>>,
		pub timeout: Option<NodeId<Expr>>,
		pub fetch: Option<NodeListId<Fetch>>,
		pub tempfiles: bool,
		pub explain: Option<Explain>,
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
		pub ty: NodeId<Type>,
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
	pub enum DefineFieldDefault{
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
		pub default: Option<DefineFieldDefault>,
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
	#[derive(Copy, Clone)]
	pub enum Expr {
		Covered(NodeId<Expr>),

		Builtin(NodeId<Builtin>),
		Float(NodeId<Spanned<f64>>),
		Integer(NodeId<Integer>),
		Decimal(NodeId<Spanned<Decimal>>),
		String(NodeId<StringLit>),

		Regex(NodeId<Regex>),

		Uuid(NodeId<Spanned<Uuid>>),
		DateTime(NodeId<Spanned<DateTime>>),
		Duration(NodeId<Spanned<Duration>>),
		File(NodeId<FileLit>),

		Point(NodeId<Point>),

		Array(NodeId<Array>),
		Object(NodeId<Object>),
		Set(NodeId<Set>),

		Mock(NodeId<Mock>),

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
		Let(NodeId<Let>),
		Info(NodeId<Info>),
		Create(NodeId<Create>),
		Update(NodeId<Update>),
		Upsert(NodeId<Upsert>),
		Delete(NodeId<Delete>),
		Relate(NodeId<Relate>),
		Select(NodeId<Select>),
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
		DefineAccess(NodeId<DefineAccess>),
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
	pub struct StringLit {
		pub text: NodeId<String>,
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
		Number(NodeId<Integer>),
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

impl_vis_type! {
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
		/// (1, $bar)
		Call(Option<NodeListId<Expr>>),
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
		LitDecimal(NodeId<Spanned<Decimal>>),
		LitObject(NodeId<LitObjectType>),
		LitArray(NodeId<LitArrayType>),
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
	pub struct Path{
		pub start: NodeId<Ident>,
		pub parts: Option<NodeListId<PathSegment>>,
	}
}
