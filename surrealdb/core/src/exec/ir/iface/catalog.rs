//! The catalog views an op's analysis may consult.
//!
//! Planning reads no data: neither view holds a storage handle. Definitions are read at the
//! catalog version the request is bound to.
//!
//! The split is a layering rule. Definitions (tables, fields including `COMPUTED` ones, events,
//! functions, parameters, permissions, sequences) are the logical layer's. Indexes, layout
//! generations and statistics are physical, and are reachable only through [`PhysCx`], which the
//! conversion driver hands to physical patterns. Expression, logical and analysis code never names
//! [`PhysicalCatalog`] or [`PhysCx`].
//!
//! Every method is synchronous and answers from what the per-planning memo already holds. A name
//! the memo does not hold answers `NotLoaded` and becomes pending. After a catalog change, or
//! while the statement reads at a `VERSION` stamp, a definition lookup answers `Unknown`: the
//! definitions held predate the change, so a consumer plans the code for run time. A default body
//! answers as an implementor that holds no catalog does: `Unknown` for a definition, `RunTime` for
//! a free name, `None` for a fact that does not exist.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use super::choose::IndexRef;
use super::fold::FoldConsumer;
use super::frame::FrameTable;
use super::ids::FrameId;
use crate::catalog::{
	DatabaseDefinition, EventDefinition, FunctionDefinition, IndexDefinition, ParamDefinition,
	TableDefinition,
};
use crate::exec::catalog_deps::Referencing;
use crate::exec::config::ExecConfig;
use crate::exec::function::FunctionRegistry;
pub(crate) use crate::exec::ir::cache::answer::{
	CatalogAnswer, ExactAnswer, FreeName, LayoutFacts,
};
use crate::exec::ir::phys::access::{ReadVersion, TableAnswers};
use crate::expr::logical::{LogicalExpr, LogicalFields, Schema};
use crate::expr::with::With;
use crate::kvs::cache::ds::TableSnapshot;
use crate::val::{Datetime, TableName};

/// What decides a table read's [`TableAnswers`]: the table, the `WITH` hint of the read and the
/// version it observes. Two reads with one key have one set of answers.
pub(crate) type SourceKey = (TableName, Option<With>, ReadVersion);

/// What the datastore underneath offers, a static fact of the datastore and the build and never of
/// one transaction. Every flag is conservative when `false`: a fan-out that needs the capability
/// stays serial.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct BackendCaps {
	/// A read-only transaction may hand out an owned read view.
	pub(crate) offers_read_views: bool,
	/// A plain read joins the transaction's conflict set.
	pub(crate) reads_tracked: bool,
	/// A writable transaction may hand out an owned read view.
	pub(crate) offers_write_views: bool,
}

/// How the datastore's reads behave, a static fact of the datastore and never of one transaction.
/// Every flag is conservative when `false`: a read that needs the capability is not widened.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct IoCaps {
	/// A plain read never joins the transaction's conflict set, so a read issued speculatively
	/// (a key the serial run may not reach) widens no conflict.
	pub(crate) untracked_reads: bool,
	/// The next chunk of a scan is worth reading on another task while the caller works on the
	/// current one, and the transaction may be read from that task.
	pub(crate) scan_read_ahead: bool,
	/// A multi-get and a multi-range read cost less than the same reads one by one, so a plan may
	/// batch the random key reads of a row stream into windows.
	pub(crate) multi_read: bool,
	/// A read-only transaction may hand out an owned read view, so workers can read parts of a
	/// key range the statement's task does not.
	pub(crate) read_views: bool,
}

/// How the field view of a table is applied to an image of one of its records, for the principal
/// a request is planned for.
#[derive(Clone, Debug)]
pub(crate) enum FieldView {
	/// The resolver layers the planner binds over the table's source, bottom first, each with the
	/// schema of its input. Empty when the view changes nothing for the principal.
	Layers(Vec<(LogicalFields, Schema)>),
	/// The view has no layer form: the planner's plan over the table is not a stack of layers
	/// above a source whose scan enforces the declarations.
	Enforced,
}

/// Definitions at the bound catalog version: tables, fields, events, functions, parameters,
/// permissions and sequences. Lookups are by name.
pub(crate) trait SemanticCatalog: Send + Sync {
	/// The database definition, or `None` when none is held or the statement reads at a `VERSION`
	/// stamp.
	fn database(&self) -> Option<Arc<DatabaseDefinition>> {
		None
	}

	/// Whether a write of the container's request code that names its one table may run the bundle
	/// of that table's stages the compile links, rather than binding them as its own ops.
	fn links_stages(&self) -> bool {
		false
	}

	/// The table whose records, or which itself, the request variable `name` holds in every run of
	/// the plan, where a request's statement reads it as the one target of its write and the plan
	/// is kept for that class of value. `None` for any other name.
	fn target_table(&self, _name: &str) -> Option<TableName> {
		None
	}

	/// The table `name` with its fields and indexes. `Absent` for a table the catalog does not
	/// define.
	fn table(&self, _name: &TableName) -> CatalogAnswer<Arc<TableSnapshot>> {
		CatalogAnswer::Unknown
	}

	/// The events of the table `table`. `Absent` for a table the catalog does not define.
	fn events(&self, _table: &TableName) -> CatalogAnswer<Arc<[EventDefinition]>> {
		CatalogAnswer::Unknown
	}

	/// The definitions of the view tables the views of the table `table` fill: the tables whose
	/// definition selects from it, which a write to `table` maintains. `Absent` for a table the
	/// catalog does not define.
	fn views(&self, _table: &TableName) -> CatalogAnswer<Arc<[TableDefinition]>> {
		CatalogAnswer::Unknown
	}

	/// The `REFERENCE` fields, of any table of the database, that can name a record of the table
	/// `table`, by the table that defines them: what a delete of such a record repairs. A
	/// database-wide read, so a consumer asks only when it deletes. `Absent` for a table the
	/// catalog does not define.
	fn referencing(&self, _table: &TableName) -> CatalogAnswer<Referencing> {
		CatalogAnswer::Unknown
	}

	/// Whether a table of the database that can hold edges stores code a delete of one of its
	/// records runs: an event, a view, a `REFERENCE` field of its own, or a `REFERENCE` field of
	/// any table that can name one of its records. A purge deletes the edges of the record it
	/// deletes, which may be records of any such table. A table that can hold edges is one not
	/// declared `TYPE NORMAL`: a table holds no edge while it is `NORMAL`, because a change of its
	/// type to `NORMAL` is refused while it holds one. A database-wide read, so a consumer asks
	/// only when it deletes.
	fn edge_code(&self) -> CatalogAnswer<bool> {
		CatalogAnswer::Unknown
	}

	/// The field view of the table `table` for the principal the request is planned for: the
	/// layers the planner binds over `SELECT * FROM t`. A consumer asks only for a table whose
	/// field declarations derive a value or cut a field. `Absent` for a table the catalog does not
	/// define.
	fn field_view(&self, _table: &TableName) -> CatalogAnswer<Arc<FieldView>> {
		CatalogAnswer::Unknown
	}

	/// The custom function `name`, without the `fn::` prefix. `Absent` for a function the
	/// request's capabilities deny. The definition is answered whatever its `PERMISSIONS` and
	/// `AUTH LIMIT`: a consumer that inlines or folds the body checks those itself.
	fn function(&self, _name: &str) -> CatalogAnswer<Arc<FunctionDefinition>> {
		CatalogAnswer::Unknown
	}

	/// Whether the request is planned at a `VERSION` stamp, so that a definition read here is the
	/// one the stamp holds rather than the one the transaction holds now.
	fn plans_at_stamp(&self) -> bool {
		false
	}

	/// Whether running the function `definition` under its `AUTH LIMIT` leaves the authority of the
	/// principal the request is planned for as it is. Without a principal only a function defined
	/// with no limit qualifies, since a limit would narrow some caller.
	fn function_limit_is_inert(&self, definition: &FunctionDefinition) -> bool {
		definition.auth_limit == crate::catalog::auth::AuthLimit::new_no_limit()
	}

	/// Whether each call of a user-defined function the request makes runs the definition the
	/// transaction reads when the call is made: the statement may change the catalog as it runs,
	/// so no body planned with it is run
	/// ([`CatalogWithin::calls_current`](crate::exec::planner::CatalogWithin)).
	fn calls_current(&self) -> bool {
		false
	}

	/// The stored parameter `name`, without the `$`.
	fn param(&self, _name: &str) -> CatalogAnswer<Arc<ParamDefinition>> {
		CatalogAnswer::Unknown
	}

	/// How the free name `name` read by an op in frame `at` is bound. Records nothing.
	fn free_name(&self, _name: &str, _at: FrameId) -> FreeName {
		FreeName::RunTime
	}

	/// As [`Self::free_name`], for a consumer that branches on the class of the value: a bound name
	/// records the dependence.
	fn free_kind(&self, _name: &str, _at: FrameId, _consumer: FoldConsumer) -> FreeName {
		FreeName::RunTime
	}

	/// The exact value of the free name `name` read in frame `at`, for a decision that plans from
	/// it. A `Value` answer has recorded the dependence.
	fn free_value_exact(&self, _name: &str, _at: FrameId, _consumer: FoldConsumer) -> ExactAnswer {
		ExactAnswer::RunTime
	}

	/// What tells this catalog apart from the other catalogs a fact cache meets while it lives,
	/// with its type. The default is its address, which holds only for a catalog that outlives
	/// every cache that files its answers. A catalog dropped while such a cache lives answers a
	/// lineage of [`next_lineage`] taken when it was made, so a catalog made later at its address
	/// never shares its identity.
	fn memo_identity(&self) -> (u64, &'static str) {
		(std::ptr::from_ref(self).cast::<()>().addr() as u64, std::any::type_name::<Self>())
	}
}

/// Indexes and their readiness, layout generations, statistics epochs, backend capabilities and
/// the answers an access-path decision reads about one table.
pub(crate) trait PhysicalCatalog: Send + Sync {
	/// The indexes of the table `table`. `Absent` for a table the catalog does not define.
	fn indexes(&self, _table: &TableName) -> CatalogAnswer<Arc<[IndexDefinition]>> {
		CatalogAnswer::Unknown
	}

	/// The layout facts of `table`.
	fn layout(&self, _table: &TableName) -> Option<LayoutFacts> {
		None
	}

	/// The statistics epoch.
	fn stats_epoch(&self) -> Option<u64> {
		None
	}

	/// Whether the index is built and serves reads at the bound version.
	fn index_ready(&self, _index: &IndexRef) -> CatalogAnswer<bool> {
		CatalogAnswer::Unknown
	}

	/// The version stamp the datetime `at` names, or `None` when the datastore cannot resolve it at
	/// plan time. A read at such a version is planned for run time.
	fn version_stamp(&self, _at: &Datetime) -> Option<u64> {
		None
	}

	/// The `VERSION` stamp the compile reads the catalog at, where its planner reads at one: a unit
	/// compiled for a call or a closure run under a stamp. A read the container makes under no
	/// `VERSION` of its own is a read at this stamp.
	fn read_stamp(&self) -> Option<u64> {
		None
	}

	/// What the datastore offers.
	fn backend(&self) -> BackendCaps {
		BackendCaps::default()
	}

	/// How the datastore's reads behave.
	fn io_caps(&self) -> IoCaps {
		IoCaps::default()
	}

	/// Whether the container is planned as a statement of a request, which runs in the frame its
	/// own transaction opens and inherits no `VERSION` from a caller. `false` for a unit compiled
	/// on its own: a read it states at the current version is read at whatever version the frame
	/// that calls it holds.
	fn at_request_root(&self) -> bool {
		false
	}

	/// What an access-path decision reads about the table a read of `key` names. `Absent` is never
	/// answered: a table that is not defined is a `Found` answer with `defined` false.
	fn table_answers(&self, _key: &SourceKey) -> CatalogAnswer<Arc<TableAnswers>> {
		CatalogAnswer::Unknown
	}

	/// The conjunct `conjunct` of a predicate, as an access-path decision reads it, with its
	/// row-independent record paths replaced by the values the planning transaction holds for
	/// them and simplified again; `None` where nothing was folded. The filter itself keeps the
	/// conjunct as written.
	fn record_folded(&self, _conjunct: &LogicalExpr) -> Option<LogicalExpr> {
		None
	}

	/// Whether a read whose table answers are `Unknown` is planned as one that knows nothing of
	/// its table, scanning it and enforcing the definitions it meets when it runs. Otherwise the
	/// statement is planned again late-bound when it is reached.
	fn plans_unknown_reads(&self) -> bool {
		false
	}
}

/// Everything an op's local effect and fallibility answer may consult.
#[derive(Clone, Copy)]
pub(crate) struct EffectCx<'a> {
	pub(crate) registry: &'a FunctionRegistry,
	/// Definitions at the bound version.
	pub(crate) catalog: &'a dyn SemanticCatalog,
	pub(crate) frames: &'a FrameTable,
}

impl EffectCx<'_> {
	/// The key a fact cache files an answer computed under this context by: the identities of
	/// its registry, catalog and frame table. Two contexts with one key answer alike for as long
	/// as the cache lives; two keys may still answer alike, which costs a recompute.
	pub(crate) fn memo_key(&self) -> CxKey {
		CxKey {
			registry: std::ptr::from_ref(self.registry).addr(),
			catalog: self.catalog.memo_identity(),
			frames: self.frames.memo_identity(),
		}
	}
}

/// The identity of the [`EffectCx`] a cached effect answer was computed under
/// ([`EffectCx::memo_key`]). The registry is named by its address: a registry is built once and
/// lives for the process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CxKey {
	pub(crate) registry: usize,
	pub(crate) catalog: (u64, &'static str),
	pub(crate) frames: (u64, usize),
}

/// A lineage no earlier call answered: the identity a catalog or frame table takes when it is
/// made.
pub(crate) fn next_lineage() -> u64 {
	static NEXT: AtomicU64 = AtomicU64::new(1);
	NEXT.fetch_add(1, Ordering::Relaxed)
}

/// What a physical pattern may consult: everything of [`EffectCx`], the physical catalog and the
/// executor configuration the plan's budgets derive from.
#[derive(Clone, Copy)]
pub(crate) struct PhysCx<'a> {
	pub(crate) effects: EffectCx<'a>,
	pub(crate) physical: &'a dyn PhysicalCatalog,
	pub(crate) config: &'a ExecConfig,
}

/// The configuration of a planning session that is given none: every field at its default.
pub(crate) fn default_config() -> &'static ExecConfig {
	static CONFIG: LazyLock<ExecConfig> = LazyLock::new(ExecConfig::default);
	&CONFIG
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::ir::iface::ids::FrameId;

	struct Silent;

	impl SemanticCatalog for Silent {}
	impl PhysicalCatalog for Silent {}

	/// An implementor that holds no catalog answers as one that knows nothing: a definition is
	/// `Unknown`, a free name is read at run time, and a fact that does not exist is `None`.
	#[test]
	fn an_empty_implementor_answers_the_conservative_top() {
		let table = TableName::from("t");
		assert!(Silent.database().is_none());
		assert!(matches!(Silent.table(&table), CatalogAnswer::Unknown));
		assert!(matches!(Silent.events(&table), CatalogAnswer::Unknown));
		assert!(matches!(Silent.function("f"), CatalogAnswer::Unknown));
		assert!(matches!(Silent.param("p"), CatalogAnswer::Unknown));
		assert_eq!(Silent.free_name("x", FrameId::new(0)), FreeName::RunTime);
		assert_eq!(
			Silent.free_kind("x", FrameId::new(0), FoldConsumer::IndexChoice),
			FreeName::RunTime
		);
		assert_eq!(
			Silent.free_value_exact("x", FrameId::new(0), FoldConsumer::IndexChoice),
			ExactAnswer::RunTime
		);
		assert!(matches!(Silent.indexes(&table), CatalogAnswer::Unknown));
		assert!(Silent.layout(&table).is_none());
		assert!(Silent.stats_epoch().is_none());
		assert_eq!(Silent.backend(), BackendCaps::default());
		assert!(Silent.version_stamp(&Datetime::now()).is_none());
		let key: SourceKey = (table, None, ReadVersion::Current);
		assert!(matches!(Silent.table_answers(&key), CatalogAnswer::Unknown));
	}

	/// A context's key changes with the contents of its frame table, not with where the table
	/// stands: a table reassigned in place with more frames, and a table made where a dropped one
	/// stood, never take the key of the earlier table.
	#[test]
	fn a_context_key_follows_the_frame_table_not_its_address() {
		use crate::exec::ir::iface::frame::AuthorityFrame;
		use crate::exec::vm::program::UnitFrame;
		use crate::expr::logical::EvalFrame;

		let registry = crate::exec::ir::analysis::cx::builtin_registry();
		let key = |frames: &FrameTable| {
			EffectCx {
				registry,
				catalog: &Silent,
				frames,
			}
			.memo_key()
		};
		let mut frames = FrameTable::default();
		let first = key(&frames);
		let mut grown = frames.clone();
		assert_eq!(key(&grown), first, "a clone answers alike");
		grown.frames.push(AuthorityFrame {
			eval: EvalFrame::caller(),
			unit: UnitFrame::unprincipled(),
		});
		frames = grown;
		assert_ne!(key(&frames), first, "a grown table in the same place");
		let mut seen = Vec::new();
		for _ in 0..2 {
			let made = FrameTable::default();
			seen.push(key(&made));
		}
		assert_ne!(seen[0], seen[1], "a table made where a dropped one stood");
	}
}
