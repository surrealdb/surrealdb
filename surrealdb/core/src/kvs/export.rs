use std::collections::{HashMap, HashSet};
use std::fmt;

use anyhow::Result;
use async_channel::Sender;
use surrealdb_rpc::export::Config;
use surrealdb_types::ToSql;

use super::Transaction;
use crate::catalog::providers::{
	ApiProvider, AuthorisationProvider, BucketProvider, DatabaseProvider, TableProvider,
	UserProvider,
};
use crate::catalog::{
	DatabaseId, Error, NamespaceId, Record, TableDefinition, TableType, ViewDefinition,
};
use crate::expr::access::AccessDuration;
use crate::expr::access_type::{
	AccessType, BearerAccess, BearerAccessSubject, BearerAccessType, JwtAccess, JwtAccessIssue,
	JwtAccessVerify, JwtAccessVerifyJwks, JwtAccessVerifyKey, RecordAccess,
};
use crate::expr::paths::{IN, OUT};
use crate::expr::statements::define::{DefineAccessStatement, DefineKind, DefineUserStatement};
use crate::expr::user::UserDuration;
use crate::expr::{Algorithm, Base, DefineAnalyzerStatement, Expr, Idiom, Literal};
use crate::idx::IndexKeyBase;
use crate::idx::ft::fulltext::mean_tokens_per_document;
use crate::key::schema::{RecordKey, RecordPrefix};
use crate::key::{KVKeyDecode, KVSubspace, KVValue};
use crate::kvs::sequences::next_unissued_value;
use crate::sql::statements::OptionStatement;
use crate::sql::statements::define::{DefineDatabaseStatement, DefineKind as SqlDefineKind};
use crate::sql::{Expr as SqlExpr, Idiom as SqlIdiom, Literal as SqlLiteral, Param, Part};
use crate::val::TableName;
use crate::{catalog, val};

struct InlineCommentWriter<'a, F>(&'a mut F);
impl<F: fmt::Write> fmt::Write for InlineCommentWriter<'_, F> {
	fn write_str(&mut self, s: &str) -> fmt::Result {
		for c in s.chars() {
			self.write_char(c)?
		}
		Ok(())
	}

	fn write_char(&mut self, c: char) -> fmt::Result {
		match c {
			'\n' => self.0.write_str("\\n"),
			'\r' => self.0.write_str("\\r"),
			// NEL/Next Line
			'\u{0085}' => self.0.write_str("\\u{0085}"),
			// line separator
			'\u{2028}' => self.0.write_str("\\u{2028}"),
			// Paragraph separator
			'\u{2029}' => self.0.write_str("\\u{2029}"),
			_ => self.0.write_char(c),
		}
	}
}

struct InlineCommentDisplay<F>(F);
impl<F: fmt::Display> fmt::Display for InlineCommentDisplay<F> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		fmt::Write::write_fmt(&mut InlineCommentWriter(f), format_args!("{}", self.0))
	}
}

/// Renders a config definition as the `DEFINE CONFIG` statement that recreates it.
///
/// A config definition's own `ToSql` is its body alone — `GRAPHQL …`, `API …` —
/// which is the form `INFO FOR DB` reports and is not a statement. An export is
/// replayed as SurrealQL, so it carries the statement instead; the body on its own
/// fails to parse, and because the config section precedes every table, a dump
/// carrying one restores nothing at all.
struct DefineConfig<'a>(&'a catalog::ConfigDefinition);

impl ToSql for DefineConfig<'_> {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		f.push_str("DEFINE CONFIG OVERWRITE ");
		self.0.fmt_sql(f, fmt);
	}
}

/// Writes the full database contents as binary SQL.
///
/// Runs as a job a transport has already begun streaming, so a failure raised in
/// here cannot change the status the caller was given. `Datastore::export_with_config`
/// rejects a config this cannot honour — `versions`, which a dump has no grammar
/// for — before handing the job over, and that is where a new such rejection goes.
pub(crate) async fn export(
	tx: &Transaction,
	ns: &str,
	db: &str,
	cfg: Config,
	batch_size: u32,
	chn: Sender<Vec<u8>>,
) -> Result<()> {
	let db = tx.get_db_by_name(ns, db, None).await?.ok_or_else(|| {
		anyhow::Error::new(Error::DbNotFound {
			name: db.to_owned(),
		})
	})?;

	// Output OPTIONS, DATABASE, USERS, ACCESSES, PARAMS, FUNCTIONS, ANALYZERS
	export_metadata(tx, &cfg, &chn, &db).await?;
	// Output TABLES
	export_tables(tx, &cfg, &chn, db.namespace_id, db.database_id, batch_size).await?;
	Ok(())
}

/// The `DEFINE DATABASE` line carrying the clauses held by the database itself,
/// or `None` for a database that holds none of them.
///
/// Emitted only when there is a clause to carry, because `DEFINE DATABASE` is
/// authorized at `Edit` on `Database` in the namespace: a dump holding the line
/// can only be restored by a namespace-level principal — a database-level
/// `OWNER` is refused it — where one without it restores under database-level
/// rights. Gating on the clauses confines that floor to the dumps that would
/// otherwise lose one.
///
/// The cost of the gate is that a clause is cleared on the target only by a dump
/// that carries some other clause. Restoring a database that holds none of them
/// over a target that holds one leaves the target's in place, so the two differ
/// in a clause the source does not have. Emitting the line unconditionally would
/// settle that and put every restore behind namespace-level rights, including
/// the one this exists for: a database-level principal restoring their own
/// database.
///
/// The name is the session's own database rather than the exported name, because
/// a restore must not write outside its target: `DEFINE DATABASE` does not
/// switch the session, so a literal name would reconfigure whichever database
/// bears that name in the target namespace instead of the one being restored
/// into. `$session.db` is a context value rather than a function call, so it
/// resolves whatever the capability policy allows, a `DEFINE PARAM` in the
/// dump's own parameters section cannot shadow it, and the name reaches the
/// statement as a value, needing no escaping.
fn define_database_statement_from_definition(
	def: &catalog::DatabaseDefinition,
) -> Option<DefineDatabaseStatement> {
	if def.comment.is_none() && def.changefeed.is_none() && !def.strict {
		return None;
	}
	Some(DefineDatabaseStatement {
		kind: SqlDefineKind::Overwrite,
		id: None,
		name: SqlExpr::Idiom(SqlIdiom(vec![
			Part::Start(SqlExpr::Param(Param::new("session"))),
			Part::Field("db".into()),
		])),
		strict: def.strict,
		comment: def
			.comment
			.clone()
			.map(|v| SqlExpr::Literal(SqlLiteral::String(v.into())))
			.unwrap_or(SqlExpr::Literal(SqlLiteral::None)),
		changefeed: def.changefeed.map(|v| v.into()),
	})
}

async fn export_metadata(
	tx: &Transaction,
	cfg: &Config,
	chn: &Sender<Vec<u8>>,
	def: &catalog::DatabaseDefinition,
) -> Result<()> {
	let (ns, db) = (def.namespace_id, def.database_id);

	// Output OPTIONS
	export_section("OPTION", [OptionStatement::import()].into_iter(), chn).await?;

	// Output DATABASE
	//
	// Gated like every other section: the statement rewrites the target
	// database's own clauses, so an export that was asked for a few tables must
	// not carry it.
	if cfg.database_definition
		&& let Some(stmt) = define_database_statement_from_definition(def)
	{
		export_section("DATABASE", std::iter::once(stmt), chn).await?;
	}

	// Output USERS
	if cfg.users {
		let users = tx.all_db_users(ns, db, None).await?;
		export_section(
			"USERS",
			users.iter().map(|x| define_user_statement_from_definition(Base::Db, x)),
			chn,
		)
		.await?;
	}

	// Output ACCESSES
	if cfg.accesses {
		let accesses = tx.all_db_accesses(ns, db, None).await?;
		export_section(
			"ACCESSES",
			accesses.iter().map(|x| define_access_statement_from_definition(Base::Db, x).redact()),
			chn,
		)
		.await?;
	}

	// Output PARAMS
	if cfg.params {
		let params = tx.all_db_params(ns, db, None).await?;
		export_section("PARAMS", params.iter(), chn).await?;
	}

	// Output FUNCTIONS
	if cfg.functions {
		let functions = tx.all_db_functions(ns, db, None).await?;
		export_section("FUNCTIONS", functions.iter(), chn).await?;
	}

	// Output ANALYZERS
	if cfg.analyzers {
		let analyzers = tx.all_db_analyzers(ns, db, None).await?;
		export_section(
			"ANALYZERS",
			analyzers.iter().map(define_analyzer_statement_from_definition),
			chn,
		)
		.await?;
	}

	// Output APIS
	if cfg.apis {
		let apis = tx.all_db_apis(ns, db, None).await?;
		export_section("APIS", apis.iter(), chn).await?;
	}

	// Output BUCKETS
	if cfg.buckets {
		let buckets = tx.all_db_buckets(ns, db, None).await?;
		export_section("BUCKETS", buckets.iter(), chn).await?;
	}

	// Output MODULES
	if cfg.modules {
		let modules = tx.all_db_modules(ns, db, None).await?;
		export_section("MODULES", modules.iter(), chn).await?;
	}

	// Output CONFIGS
	if cfg.configs {
		let configs = tx.all_db_configs(ns, db, None).await?;
		export_section("CONFIGS", configs.iter().map(DefineConfig), chn).await?;
	}

	// Output SEQUENCES
	//
	// A definition carries the `START` the sequence was declared with, which is
	// only where it began. Emitting that verbatim would rewind the sequence on
	// import while the exported records keep the ids already allocated from it,
	// so the first `sequence::nextval` after a restore would re-issue a value
	// that is in use. Each sequence is therefore exported at the position it
	// reached, which is what an import has to re-establish.
	if cfg.sequences {
		let sequences = tx.all_db_sequences(ns, db, None).await?;
		let mut positioned = Vec::with_capacity(sequences.len());
		for sq in sequences.iter() {
			let mut sq = sq.clone();
			sq.start = next_unissued_value(tx, ns, db, sq.name.as_str(), sq.start, None).await?;
			positioned.push(sq);
		}
		export_section("SEQUENCES", positioned.iter(), chn).await?;
	}

	Ok(())
}

async fn export_section<T>(
	title: &str,
	items: impl ExactSizeIterator<Item = T>,
	chn: &Sender<Vec<u8>>,
) -> Result<()>
where
	T: ToSql,
{
	if items.len() == 0 {
		return Ok(());
	}

	chn.send(bytes!("-- ------------------------------")).await?;
	chn.send(bytes!(format!("-- {}", InlineCommentDisplay(title)))).await?;
	chn.send(bytes!("-- ------------------------------")).await?;
	chn.send(bytes!("")).await?;

	for item in items {
		chn.send(bytes!(format!("{};", item.to_sql()))).await?;
	}

	chn.send(bytes!("")).await?;
	Ok(())
}

async fn export_tables(
	tx: &Transaction,
	cfg: &Config,
	chn: &Sender<Vec<u8>>,
	ns: NamespaceId,
	db: DatabaseId,
	batch_size: u32,
) -> Result<()> {
	// Check if tables are included in the export config
	if !cfg.tables.is_any() {
		return Ok(());
	}
	// Fetch all of the tables for this NS / DB
	let tables = tx.all_tb(ns, db, None).await?;
	// Warn if any specified table names don't match existing tables
	if let Some(names) = cfg.tables.names() {
		let existing: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
		for name in names {
			if !existing.contains(&name.as_str()) {
				warn!("Table '{name}' does not exist in the database");
			}
		}
	}
	// Split the tables the export config selects into the ones holding records
	// of their own and the views derived from them.
	let (views, stored): (Vec<&TableDefinition>, Vec<&TableDefinition>) = tables
		.iter()
		.filter(|t| cfg.tables.includes(t.name.as_str()))
		.partition(|t| t.view.is_some());
	// The table names this dump goes on to define. A view reading a name that is
	// not among them restores against whatever the target already holds under
	// it, and fails outright when the target holds nothing.
	let emitted: HashSet<&str> =
		stored.iter().chain(views.iter()).map(|t| t.name.as_str()).collect();
	// The stored tables the emitted views recompute from.
	//
	// A view's `DEFINE TABLE ... AS SELECT` clears its table's whole key range
	// before recomputing, and that range holds more than the view's own rows: a
	// `REFERENCE` field naming the view writes its backlink there, and a graph
	// edge with one end in the view writes that end there. Both are written by
	// another table's records, and clearing them is silent — the definition
	// succeeded, and what it cleared was never its own.
	//
	// So a stored table is emitted before the view definitions when a definition
	// reads it, and after them when none does. What no order settles is a table
	// that is both: one a view selects from and one holding a reference or an
	// edge into a view. Its records have to come first, and the link they write
	// is cleared.
	//
	// First is not a preference there. An import runs no view maintenance —
	// `process_table_views` returns early under `OPTION IMPORT` — so a
	// definition placed ahead of its source's records recomputes over an empty
	// table, and no record written afterwards ever fills it. The rows a view
	// restores with are the ones its definition recomputes at the moment it
	// runs, or the ones this dump carried under the plain table when it does
	// not run at all.
	let view_sources: HashSet<&str> = views
		.iter()
		.filter_map(|table| table.view.as_ref())
		.flat_map(|view| view.source_tables())
		.map(|name| name.as_str())
		.collect();
	let (read_by_a_view, unread): (Vec<&TableDefinition>, Vec<&TableDefinition>) =
		stored.into_iter().partition(|table| view_sources.contains(table.name.as_str()));
	// Emit the tables the views read first, structure then data, in name order.
	for table in &read_by_a_view {
		export_table_structure(tx, ns, db, table, chn).await?;
		if cfg.records {
			export_table_data(tx, ns, db, table, chn, batch_size).await?;
		}
	}
	// A view is derived state: replaying its `DEFINE TABLE` recomputes it from
	// the tables it selects from, and fails outright when one of them is
	// absent. So a view's definition follows every table it reads, and a view
	// this dump can replay carries no records — the definition writes those rows
	// itself, and an `INSERT` carrying the same ids is refused as a duplicate
	// record. The read-only rule on view tables does not stand in the way of
	// that `INSERT`: a restore runs under `OPTION IMPORT`, which exempts it, so
	// the statement reaches the record and fails on the id.
	//
	// Recomputation reproduces the exported rows exactly when the projection is
	// a function of its source rows, and not when it reads anything else —
	// `time::now()`, `rand()`, a value from outside the database. Those rows
	// come back recomputed, and that is the trade this makes rather than an
	// oversight: what the restore holds is the view, recomputed.
	for (table, deferral) in order_views(&views) {
		// A reason the definition is already known not to replay is reported, in
		// the log and in the dump alike — the log belongs to the server, and an
		// operator running `surreal export` reads the file.
		if let Some(reason) = unreplayable_view(table, &emitted, deferral) {
			warn!("{reason}");
			chn.send(bytes!("-- ------------------------------")).await?;
			chn.send(bytes!(format!("-- NOTE: {}.", InlineCommentDisplay(&reason)))).await?;
			chn.send(bytes!("-- ------------------------------")).await?;
			chn.send(bytes!("")).await?;
		}
		// Every view is emitted as the same table without its `AS SELECT`, then
		// its records, then the definition, whether or not anything here can see
		// a reason the definition will not replay.
		//
		// The three statements settle every outcome between them: a definition
		// that replays begins by clearing the table, so its recomputed rows
		// replace these and nothing collides, and one that fails takes its own
		// writes back with it, leaving the plain table and these rows behind
		// under that name.
		//
		// It is unconditional because whether the definition replays is not a
		// property of the database being exported. Some reasons it will not are
		// visible here — an analysis that no longer accepts the clauses, a
		// definition cycle, a source this dump does not carry — but others
		// belong to the target alone: recomputing a view is one transaction, so
		// a target enforcing `transaction_max_write_keys` rejects a view whose
		// rows outgrow the limit, even where the source built those same rows
		// through writes that were each within it. An export that carried the
		// rows only for the reasons it can name would still lose a view to the
		// ones it cannot.
		//
		// The plain table goes first whether or not a row follows, because it is
		// what makes the name exist at all. A dump that leaves the name to an
		// `INSERT` loses an empty view outright — and with it every view reading
		// that name, whose own definition then finds nothing there.
		//
		// It keeps the view's name and nothing that would refuse the view's
		// rows, because maintaining a view runs the table's triggers and
		// nothing else: its stored rows never met the table's own rules, and
		// writing them back through an ordinary `INSERT` is the first time
		// anything asks them to.
		//
		// - `SCHEMAFULL` goes, and the fields are left to the structure after the records: a row
		//   can carry a field the view does not define, hold a value its declared type rejects,
		//   ignore a `VALUE` clause and fail its `ASSERT`. A `REFERENCE` among those definitions
		//   would also write a backlink under the table it points at, which the exported database
		//   never had and no `DEFINE TABLE ... AS SELECT` clears.
		// - `DROP` goes, or the table discards every record written to it and reports nothing — the
		//   one loss here that leaves no trace for the import to report.
		// - The type becomes `ANY`, because a relation table rejects what a plain `INSERT` writes
		//   it, `in` and `out` or not. `ANY` stores those two as the fields they were and writes no
		//   edge, which is what maintaining the view did.
		//
		// So a fallback restore is left holding a plain, permissive table
		// carrying the view's rows and its field definitions. A definition that
		// replays supersedes all of it: it states the table's own mode, type
		// and `DROP`, and recomputes the rows under them.
		let fallback = TableDefinition {
			view: None,
			schemafull: false,
			drop: false,
			table_type: TableType::Any,
			..table.clone()
		};
		chn.send(bytes!(format!("{};", fallback.to_sql()))).await?;
		chn.send(bytes!("")).await?;
		// KNOWN GAP: a view whose source this dump leaves out can still lose
		// its carried rows on restore, silently.
		//
		// `DEFINE TABLE ... AS SELECT` clears the table before recomputing. The
		// reasoning above covers a target holding *nothing* under the source's
		// name, where the statement fails and takes its own writes back. It
		// does not cover a target holding something *else* under that name: the
		// statement then succeeds, deletes the rows this dump wrote and
		// replaces them with a recomputation over data this database never had,
		// while the NOTE above called the outcome merely open.
		//
		// Emitting the definition before the records for the flagged cases
		// makes that loud instead of silent, but it also makes the legitimate
		// restore loud: against a target that holds the *right* source, the
		// recomputation lands first and the records that follow collide with
		// it, so a correct restore reports a failed statement and answers 422.
		// Measured, not assumed — `a_restore_with_the_source_recomputes_over_the_rows`
		// fails that way under the reordering.
		//
		// The two cases are indistinguishable from here: an export cannot tell
		// a target's right source from a same-named wrong one. Which failure to
		// have is a product decision rather than an exporter's, so the order
		// is unchanged until it is made.
		if cfg.records {
			export_table_data(tx, ns, db, table, chn, batch_size).await?;
		}
		export_table_structure(tx, ns, db, table, chn).await?;
	}
	// The stored tables no view reads, structure and data together, in name
	// order — after every view definition, so a reference or an edge into a view
	// is written into a range nothing clears again.
	//
	// The structure goes here with the records rather than ahead of the views,
	// because a definition that reads a name outside the tables it selects from
	// is one nothing here can order: it finds the name undefined and fails, and
	// the rows this dump carried stay under the plain table it fell back to. A
	// dump that defined every name up front would let that definition succeed
	// against an empty table and recompute the rows away instead.
	for table in &unread {
		export_table_structure(tx, ns, db, table, chn).await?;
		if cfg.records {
			export_table_data(tx, ns, db, table, chn, batch_size).await?;
		}
	}

	Ok(())
}

/// Why replaying this view's `DEFINE TABLE` may not put its rows back, or
/// `None` when the dump can replay it.
///
/// Three things stop a replay. [`ViewDefinition::Select`] is the classification
/// a stored view degrades to when the aggregation analysis rejects its clauses:
/// no write to a source updates such a view, and the same analysis rejects the
/// `DEFINE TABLE` that would recreate it, so its stored rows are state nothing
/// can recompute. A definition cycle is the second: the catalog stores one, and
/// whichever member an order emits first reads a name the dump has not defined
/// yet. A source this dump does not define is the third: the definition reads
/// whatever the target holds under that name, and raises `TbNotFound` when the
/// target holds nothing.
///
/// Only the first two are settled here. The third depends on the target, so what
/// this reports is that the outcome is open, not that the replay will fail.
fn unreplayable_view(
	table: &TableDefinition,
	emitted: &HashSet<&str>,
	deferral: Deferral,
) -> Option<String> {
	let view = table.view.as_ref()?;
	if matches!(view, ViewDefinition::Select { .. }) {
		return Some(format!(
			"Table '{}' is a view this version can no longer maintain, so its definition cannot \
			 be replayed",
			InlineCommentDisplay(&table.name)
		));
	}
	match deferral {
		Deferral::Cycle => {
			return Some(format!(
				"Table '{}' is a view in a definition cycle, which no order of this dump can \
				 replay",
				InlineCommentDisplay(&table.name)
			));
		}
		Deferral::BehindCycle => {
			return Some(format!(
				"Table '{}' is a view reading a definition cycle, so this dump defines a table it \
				 selects from after it",
				InlineCommentDisplay(&table.name)
			));
		}
		Deferral::None => {}
	}
	let missing: Vec<String> = view
		.source_tables()
		.iter()
		.filter(|t| !emitted.contains(t.as_str()))
		.map(|t| format!("'{}'", InlineCommentDisplay(t)))
		.collect();
	if missing.is_empty() {
		return None;
	}
	Some(format!(
		"Table '{}' is a view over {}, which this export does not carry",
		InlineCommentDisplay(&table.name),
		missing.join(", ")
	))
}

/// Orders views so each one follows the views it selects from, pairing each with
/// whether it is one this order cannot satisfy.
fn order_views<'a>(views: &[&'a TableDefinition]) -> Vec<(&'a TableDefinition, Deferral)> {
	let graph: Vec<(&str, &[TableName])> = views
		.iter()
		.map(|t| (t.name.as_str(), t.view.as_ref().map_or(&[][..], |v| v.source_tables())))
		.collect();
	let order = view_order(&graph);
	order
		.order
		.iter()
		.map(|&i| {
			let deferral = if order.cyclic.contains(&i) {
				Deferral::Cycle
			} else if order.behind_cycle.contains(&i) {
				Deferral::BehindCycle
			} else {
				Deferral::None
			};
			(views[i], deferral)
		})
		.collect()
}

/// Whether the emission order put a view after every table it reads, and if not,
/// why it could not.
///
/// The two failures are distinguished because they say different things about
/// the schema: a cycle is a property of the definitions themselves, whereas a
/// view behind one is ordinary and unreplayable only by association.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Deferral {
	/// Emitted after every table it reads.
	None,
	/// On a definition cycle: whichever member is emitted first reads a name the
	/// dump has not defined yet.
	Cycle,
	/// Reads a view on a cycle, directly or through others, so the dump defines
	/// one of its sources after it.
	BehindCycle,
}

/// An emission order for a set of views, and the members of it that order cannot
/// satisfy.
struct ViewOrder {
	/// Positions in the order they are to be emitted.
	order: Vec<usize>,
	/// Positions lying on a definition cycle. Named apart because no order
	/// replays them: whichever member is emitted first reads a name the dump has
	/// not defined yet.
	cyclic: HashSet<usize>,
	/// Positions that read a cycle without being on one. Equally unorderable —
	/// their sources are never placed — but a cycle is not what their own
	/// definition says, so they are reported for what they are.
	behind_cycle: HashSet<usize>,
}

/// The positions of `views` — each a name and the tables it selects from — in an
/// order where every view follows the views it reads.
///
/// Replaying a view's `DEFINE TABLE` recomputes it from its source tables, so a
/// view reading another view has to be defined after it. A source that is not
/// itself one of `views` — a stored table, already emitted, or a table the
/// export config excludes — places no constraint on the order.
///
/// Views that cannot be ordered — a definition cycle, which the catalog stores
/// and no single-pass order can satisfy — come last in their original order, and
/// are named in [`ViewOrder::cyclic`] so the caller can carry what their
/// definitions will not restore rather than the export looping or hiding it.
fn view_order(views: &[(&str, &[TableName])]) -> ViewOrder {
	let names: HashSet<&str> = views.iter().map(|(name, _)| *name).collect();
	let mut emitted: HashSet<&str> = HashSet::new();
	let mut ordered: Vec<usize> = Vec::with_capacity(views.len());
	let mut pending: Vec<usize> = (0..views.len()).collect();
	while !pending.is_empty() {
		let placed = ordered.len();
		let mut deferred = Vec::new();
		for i in pending {
			let (name, sources) = views[i];
			let ready =
				sources.iter().all(|s| !names.contains(s.as_str()) || emitted.contains(s.as_str()));
			if ready {
				emitted.insert(name);
				ordered.push(i);
			} else {
				deferred.push(i);
			}
		}
		if ordered.len() == placed {
			// Nothing became ready, so nothing left can be ordered after all of
			// its sources. Some of those lie on a cycle and the rest only read
			// one, and the two are reported apart.
			let cyclic = cyclic_members(views, &deferred);
			let behind_cycle = deferred.iter().copied().filter(|i| !cyclic.contains(i)).collect();
			ordered.extend(deferred);
			return ViewOrder {
				order: ordered,
				cyclic,
				behind_cycle,
			};
		}
		pending = deferred;
	}
	ViewOrder {
		order: ordered,
		cyclic: HashSet::new(),
		behind_cycle: HashSet::new(),
	}
}

/// The members of `stuck` that lie on a definition cycle.
///
/// Every position in `stuck` is unorderable, but only one with a path back to
/// itself through the others is in a cycle. The rest merely read one, and
/// reporting those as cyclic tells an operator something untrue about their
/// schema. `stuck` holds what one pass of [`view_order`] could not place, which
/// is at most the dump's views, so the walk is bounded by that.
fn cyclic_members(views: &[(&str, &[TableName])], stuck: &[usize]) -> HashSet<usize> {
	let position: HashMap<&str, usize> = stuck.iter().map(|&i| (views[i].0, i)).collect();
	let mut cyclic = HashSet::new();
	for &start in stuck {
		// Walk the sources that are themselves still stuck. `start` is on a
		// cycle exactly when the walk arrives back at it.
		let mut seen: HashSet<usize> = HashSet::new();
		let mut frontier = vec![start];
		while let Some(i) = frontier.pop() {
			for source in views[i].1 {
				let Some(&next) = position.get(source.as_str()) else {
					continue;
				};
				if next == start {
					cyclic.insert(start);
					frontier.clear();
					break;
				}
				if seen.insert(next) {
					frontier.push(next);
				}
			}
		}
	}
	cyclic
}

async fn export_table_structure(
	tx: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableDefinition,
	chn: &Sender<Vec<u8>>,
) -> Result<()> {
	chn.send(bytes!("-- ------------------------------")).await?;
	chn.send(bytes!(format!("-- TABLE: {}", InlineCommentDisplay(&table.name)))).await?;
	chn.send(bytes!("-- ------------------------------")).await?;
	chn.send(bytes!("")).await?;
	chn.send(bytes!(format!("{};", table.to_sql()))).await?;
	chn.send(bytes!("")).await?;
	let tb_name = table.name.clone();
	// Export all table field definitions with OVERWRITE to ensure
	// idempotent re-import (relation tables auto-generate in/out fields,
	// and array types generate sub-field definitions that would conflict).
	// A lightweight relation's only fields are the auto in/out pair, which
	// its table definition regenerates and whose explicit redefinition the
	// importer would reject — so no field lines are emitted for it.
	if crate::kvs::lightweight::lightweight_relation(&table.table_type).is_none() {
		let fields = tx.all_tb_fields(ns, db, &tb_name, None).await?;
		for field in fields.iter() {
			chn.send(bytes!(format!("{};", field.to_sql_overwrite()))).await?;
		}
	}
	chn.send(bytes!("")).await?;
	// Export all table index definitions for this table
	let indexes = tx.all_tb_indexes(ns, db, &tb_name, None).await?;
	for index in indexes.iter() {
		chn.send(bytes!(format!("{};", index.to_sql()))).await?;
	}
	chn.send(bytes!("")).await?;
	// Export all table event definitions for this table
	let events = tx.all_tb_events(ns, db, &tb_name, None).await?;
	for event in events.iter() {
		chn.send(bytes!(format!("{};", event.to_sql()))).await?;
	}
	chn.send(bytes!("")).await?;
	// Everything ok
	Ok(())
}

/// Target number of KV keys the records of one emitted `INSERT` should write on
/// re-import.
///
/// An import executes one statement per transaction, so how many records an
/// `INSERT` line carries decides how large a transaction is on the way back in.
/// What a transaction costs is not its record count but the number of keys those
/// records write, and a table's index set moves that by two orders of magnitude:
/// a bare record writes 2 keys, eight b-tree indexes take it to 12, and a single
/// full-text index roughly one per distinct term in the document. Sizing by keys
/// keeps a transaction's cost roughly constant across index sets instead of
/// leaving it to vary with the schema.
///
/// The value is chosen so a table whose records write few keys keeps using the
/// whole `export_batch_size` — the cap below — and only a table carrying an
/// index whose per-record key count is large groups its records more tightly.
const INSERT_KEY_BUDGET: usize = 20_000;

/// Keys a statement writes once rather than per record: an index whose maintenance
/// is batched per transaction — the full-text and count families — contributes a
/// delta entry and a compaction-queue entry at commit whatever the group's size.
/// Plain and unique indexes contribute neither, so this is an allowance for the
/// tables that have them and a small overcharge for the tables that do not.
///
/// Flat rather than per-index, because it is taken off the budget before the index
/// set is known. What it costs is a group's last few records: one for a table whose
/// records are expensive enough to group tightly, more for a cheap table already
/// grouping near the whole batch, where a handful of records either way is not what
/// decides transaction size.
const KEYS_PER_STATEMENT: usize = 32;

/// Keys a record writes for itself, independent of any index.
const KEYS_PER_RECORD: usize = 2;

/// Keys a record writes per index whose per-record key count is fixed and small:
/// plain, unique, and count indexes each write a bounded number of entries.
const KEYS_PER_VALUE_INDEX: usize = 2;

/// Keys a record writes per record id held in a `REFERENCE` field.
///
/// Each referenced id costs a reference key naming the back-link, and, when the
/// referenced table caps its back-references with `INLINE REFERENCES`, a rewrite
/// of that record's reference cache. The cache write happens only for a capped
/// target, so two is the ceiling rather than the going rate — the direction to
/// err in, since a group sized as though references were free is one whose
/// re-import can exceed the write-cardinality guard.
const KEYS_PER_REFERENCE: usize = 2;

/// Keys a full-text index writes per indexed term: its share of the delta log.
///
/// A record's postings are one key, not one per term — a document's whole term
/// map is a single value. So the only cost that scales with term count is the
/// delta log, and because that is batched per transaction, a group's delta cost
/// is its *distinct vocabulary*: at most one key per posting, when no record in
/// the group shares a word with another, and less the more they overlap.
///
/// One key per term is therefore the ceiling rather than the going rate, which is
/// the direction to err in: over-sizing groups more tightly than needed and costs
/// throughput that is flat across small groupings, where under-sizing leaves the
/// oversized transaction that grouping exists to avoid.
///
/// It covers the delta log and nothing else. The keys that do not scale with term
/// count get [`KEYS_PER_FULLTEXT_RECORD`], because at the ceiling — a group whose
/// records share no vocabulary — there is no slack in this allowance to absorb
/// them, and a group sized as though there were is one that fails its own
/// re-import against the write-cardinality guard.
const KEYS_PER_FULLTEXT_TERM: usize = 1;

/// Keys a record writes for a full-text index however long its document is: the
/// term map, the document length, and the table's forward and reverse doc-id
/// mapping.
///
/// The doc-id pair is table-scoped rather than per index, so a table carrying
/// several full-text indexes is charged for it more than once. That is the safe
/// direction, and the alternative — accounting for it once per table — would put
/// per-record cost somewhere other than the per-index loop that derives it.
const KEYS_PER_FULLTEXT_RECORD: usize = 4;

/// Fallback allowance for one index whose per-record key count scales with the
/// indexed content rather than being fixed, used when the index carries no
/// statistic to derive it from — an index with no documents yet, or a vector
/// index, whose per-record cost follows graph connectivity rather than a length
/// the index records.
///
/// Deliberately generous. Overestimating only groups records more tightly, and
/// throughput is flat across a wide range of small groupings, whereas
/// underestimating leaves the oversized transaction in place — so the error that
/// costs nothing is the one to make.
const KEYS_PER_CONTENT_INDEX_FALLBACK: usize = 256;

/// Records sampled from a batch to price a value index's fan-out.
///
/// A table's records are usually alike in shape, so a handful is enough to tell
/// a scalar field from an array one and to size the array within an order of
/// magnitude. The sample is taken per batch, so a table whose shape varies down
/// its key range is re-priced as the export walks it.
const FAN_OUT_SAMPLE: usize = 8;

/// The number of index entries one record produces for an index over `cols`.
///
/// A value index over an array-valued field does not write one entry: the
/// indexer expands a non-flattened array into one entry per element, so a
/// record with a 500-element `tags` array writes 500 entries to a single index.
/// Ignoring that would leave exactly the schema this budget exists for — records
/// that write far more keys than their count suggests — producing oversized
/// transactions.
///
/// Counted over the positions the column's path reaches, so a path with a
/// wildcard is priced across every one of them, and a container at a position
/// by its length. Nested containers therefore price at the total rather than the
/// outer length, overstating what the indexer expands: overstating groups
/// records more tightly and costs throughput that is flat across small
/// groupings, where understating leaves the oversized transaction that grouping
/// exists to avoid.
///
/// Takes the largest fan-out across the index's columns rather than their
/// product: the combinator advances one column per step, so the entry count
/// tracks the longest array rather than every combination of them.
///
/// The positions are visited by reference. A length is all this needs, and
/// reading them out to take it copies a whole array per column, per sampled
/// record, per batch.
fn index_fan_out(data: &val::Value, cols: &[Idiom]) -> usize {
	fn entries(v: &val::Value) -> usize {
		match v {
			val::Value::Array(a) => a.len().max(1),
			val::Value::Set(set) => set.len().max(1),
			_ => 1,
		}
	}
	cols.iter()
		.map(|col| {
			let mut entries_for_column = 0;
			data.walk_ref(col, &mut |v| entries_for_column += entries(v));
			entries_for_column.max(1)
		})
		.max()
		.unwrap_or(1)
}

/// The number of reference keys one record produces for a `REFERENCE` field.
///
/// A reference field holds record ids, and one key is written per distinct id it
/// names — so an `array<record<person>>` holding 20 ids writes 20 keys from a
/// single field. The count is taken over the field's walked occurrences, the way
/// the write path collects them, so a path with a wildcard is priced across every
/// position it reaches.
///
/// Floors at one. Unlike an index, a reference field can legitimately hold no id
/// in every sampled record while later records in the same table hold many, and a
/// sample that prices those at zero is one that sizes their group as though the
/// keys were free.
///
/// The occurrences are visited by reference. A count is all this needs, and
/// reading them out to take it copies every id the field holds — hundreds, for
/// the array-valued field this exists to price — per sampled record, per batch.
fn reference_fan_out(data: &val::Value, name: &Idiom) -> usize {
	fn record_ids(v: &val::Value) -> usize {
		match v {
			val::Value::Array(a) => a.iter().filter(|v| v.is_record()).count(),
			val::Value::Set(set) => set.iter().filter(|v| v.is_record()).count(),
			val::Value::RecordId(_) => 1,
			_ => 0,
		}
	}
	let mut ids = 0;
	data.walk_ref(name, &mut |v| ids += record_ids(v));
	ids.max(1)
}

/// Estimates the keys one record of `table` writes, given its index set and a
/// sample of the records about to be emitted.
///
/// Two index kinds cost more per record than the schema alone reveals, and both
/// are resolved from data rather than guessed:
///
/// - A full-text index writes per distinct term, so a table of 8 KB documents costs an order of
///   magnitude more per record than one of 700 B documents. Derived from the mean document length
///   the index already maintains for BM25 scoring, using the token mean as a stand-in for the
///   distinct-term count. Usually above it, since documents repeat words, which is the direction
///   that costs nothing — but not a bound: the mean is truncated, and a mean is not a ceiling for a
///   group whose vocabulary sits in a few long documents.
/// - A value index over an array field writes one entry per element. Derived from the sampled
///   records, since no statistic records it.
///
/// Falls back to a fixed allowance where neither is available: a full-text index
/// with no documents to average, and the vector indexes, whose per-record cost
/// follows graph connectivity rather than anything visible here.
///
/// A `REFERENCE` field is the third such kind and is charged alongside the index
/// set: its keys are maintained on the import path like any other write, and a
/// field holding many ids writes one key each from a record the schema makes look
/// ordinary.
async fn keys_per_record(
	tx: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	indexes: &[catalog::IndexDefinition],
	fields: &[catalog::FieldDefinition],
	sample: &[val::Value],
) -> Result<usize> {
	let mut keys = KEYS_PER_RECORD;
	for fd in fields.iter().filter(|fd| fd.reference.is_some()) {
		let fan_out =
			sample.iter().map(|data| reference_fan_out(data, &fd.name)).max().unwrap_or(1);
		keys = keys.saturating_add(KEYS_PER_REFERENCE.saturating_mul(fan_out));
	}
	for ix in indexes {
		// Saturating, so a table whose statistics are implausible sizes its group at
		// one record rather than wrapping to a large one.
		keys = keys.saturating_add(match ix.index {
			catalog::Index::FullText(_) => {
				let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
				match mean_tokens_per_document(tx, &ikb).await? {
					Some(mean) => KEYS_PER_FULLTEXT_RECORD
						.saturating_add((mean as usize).saturating_mul(KEYS_PER_FULLTEXT_TERM)),
					None => KEYS_PER_CONTENT_INDEX_FALLBACK,
				}
			}
			catalog::Index::Hnsw(_) | catalog::Index::DiskAnn(_) => KEYS_PER_CONTENT_INDEX_FALLBACK,
			catalog::Index::Idx | catalog::Index::Uniq | catalog::Index::Count(_) => {
				let fan_out =
					sample.iter().map(|data| index_fan_out(data, &ix.cols)).max().unwrap_or(1);
				KEYS_PER_VALUE_INDEX.saturating_mul(fan_out)
			}
		});
	}
	Ok(keys)
}

/// Decodes up to `limit` records from the head of a scan batch, for sizing.
///
/// Sizing has to look at record content, and the alternative to decoding a few
/// twice is threading decoded records through the emit path — which would hold a
/// whole batch's `Value`s alive for the sake of a handful used to measure.
fn decode_sample(batch: &[(Vec<u8>, Vec<u8>)], limit: usize) -> Result<Vec<val::Value>> {
	batch
		.iter()
		.take(limit)
		.map(|(k, v)| {
			let k = RecordKey::decode_key(k)?;
			let rid = crate::val::RecordId {
				table: k.tb.into_owned(),
				key: k.id.into_owned(),
			};
			Ok(Record::kv_decode_value(v, rid)?.data)
		})
		.collect()
}

/// Chooses how many records to put in one emitted `INSERT`, from the keys the
/// table's index set makes each record write.
///
/// Never exceeds `batch_size`, so `export_batch_size` remains the bound a
/// deployment can already configure and this only ever groups more tightly than
/// it asks for.
///
/// The budget is spent net of what a statement writes once however many records it
/// carries, so the per-record division has the whole of what is left. Without that
/// the estimate lands exactly on the budget for a group whose records fill it, and
/// the statement's own entries carry it over.
fn records_per_insert(keys_per_record: usize, batch_size: u32) -> usize {
	let budget = INSERT_KEY_BUDGET.saturating_sub(KEYS_PER_STATEMENT);
	(budget / keys_per_record.max(1)).clamp(1, batch_size.max(1) as usize)
}

async fn export_table_data(
	tx: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableDefinition,
	chn: &Sender<Vec<u8>>,
	batch_size: u32,
) -> Result<()> {
	chn.send(bytes!("-- ------------------------------")).await?;
	chn.send(bytes!(format!("-- TABLE DATA: {}", InlineCommentDisplay(&table.name)))).await?;
	chn.send(bytes!("-- ------------------------------")).await?;
	chn.send(bytes!("")).await?;

	let tb_name = table.name.clone();

	// A lightweight relation has no record range; its edges are enumerated
	// from its IN tables' adjacency and emitted as the `RELATE` statements
	// that recreate them — deterministic canonical ids make the round trip
	// idempotent.
	if let Some(rel) = crate::kvs::lightweight::lightweight_relation(&table.table_type) {
		let mut scanner = crate::kvs::lightweight::LightweightEdgeScanner::new(
			tx,
			ns,
			db,
			&tb_name,
			rel,
			surrealdb_kvs::Direction::Forward,
			None,
			None,
		);
		loop {
			let batch = scanner.next_batch(batch_size).await?;
			if batch.is_empty() {
				break;
			}
			let mut statements = String::new();
			for edge in &batch {
				let Some((l, r)) = crate::kvs::lightweight::edge_parts(&edge.key) else {
					continue;
				};
				statements.push_str(&format!(
					"RELATE {} -> {} -> {};\n",
					l.to_sql(),
					surrealdb_types::ToSql::to_sql(&crate::val::Value::Table(tb_name.clone())),
					r.to_sql()
				));
			}
			chn.send(bytes!(statements)).await?;
		}
		chn.send(bytes!("")).await?;
		return Ok(());
	}

	// How many records one `INSERT` carries is a separate question from how many
	// keys one scan reads: the scan size trades KV round trips against memory,
	// while the `INSERT` size sets the size of a transaction on re-import. The
	// index set only bears on the second.
	let indexes = tx.all_tb_indexes(ns, db, &tb_name, None).await?;
	// A `REFERENCE` field writes back-reference keys on every write path, a
	// restore included, so its fan-out is part of what one record costs.
	let fields = tx.all_tb_fields(ns, db, &tb_name, None).await?;
	// A record's value decodes only with its own key's record id, so the table's
	// records are read as bytes and decoded per key by `export_regular_data`.
	let records = RecordPrefix {
		ns,
		db,
		tb: std::borrow::Cow::Borrowed(&tb_name),
	};
	let mut next = Some(records.range()?);

	while let Some(rng) = next {
		let batch = tx.batch_keys_vals_raw(rng, batch_size, None).await?;
		// What a batch leaves unread is a tail of the same region, so the bound that
		// produced the range is the one that wraps the continuation.
		next = batch.next.map(|rng| records.raw(rng));
		// If there are no values, return early.
		if batch.result.is_empty() {
			break;
		}
		// Price this batch from its own records. Sizing needs to see the data
		// because a value index over an array field writes one entry per
		// element, which the schema does not state.
		let sample = decode_sample(&batch.result, FAN_OUT_SAMPLE)?;
		let per_insert = records_per_insert(
			keys_per_record(tx, ns, db, &tb_name, &indexes, &fields, &sample).await?,
			batch_size,
		);
		for group in batch.result.chunks(per_insert) {
			export_regular_data(group, chn).await?;
		}
	}

	chn.send(bytes!("")).await?;
	Ok(())
}

/// Processes a record and categorizes it for SQL export.
///
/// This function processes a record, categorizing it into either normal
/// records or graph edge records, and writes it to the appropriate string
/// buffer for later SQL generation.
///
/// Note: Only the latest version of each record is exported. Historical
/// versions must be exported at the KV level.
///
/// # Arguments
///
/// * `record` - The record to be processed. The `id` field must already be present in `data` (this
///   is the case when the record was produced by [`Record::kv_decode_value_with_id`]).
/// * `records_relate` - A mutable reference to a string buffer for graph edge records.
/// * `records_normal` - A mutable reference to a string buffer for normal records.
fn process_record(record: &Record, records_relate: &mut String, records_normal: &mut String) {
	// Match on the value to determine if it is a graph edge record or a normal record.
	if record.is_edge()
		&& let crate::val::Value::RecordId(_) = record.data.pick(&IN)
		&& let crate::val::Value::RecordId(_) = record.data.pick(&OUT)
	{
		// If the value is a graph edge record (indicated by EDGE, IN, and OUT fields):
		// Write the value to the records_relate string.
		if !records_relate.is_empty() {
			records_relate.push_str(", ");
		}
		records_relate.push_str(&record.data.to_sql());
	} else {
		// If the value is a normal record, write it to the records_normal string.
		if !records_normal.is_empty() {
			records_normal.push_str(", ");
		}
		records_normal.push_str(&record.data.to_sql());
	}
}

/// Exports regular data to the provided channel.
///
/// This function processes a list of regular values, converting them into
/// SQL commands and sending them to the provided channel. It handles both
/// normal records and graph edge records, and ensures that the appropriate
/// SQL commands are generated for each type of record.
///
/// # Arguments
///
/// * `regular_values` - A vector of tuples containing the regular values to be exported. Each tuple
///   consists of a key and a value.
/// * `chn` - A reference to the channel to which the SQL commands will be sent.
///
/// # Returns
///
/// * `Result<()>` - Returns `Ok(())` if the operation is successful, or an `Error` if an error
///   occurs.
async fn export_regular_data(
	regular_values: &[(Vec<u8>, Vec<u8>)],
	chn: &Sender<Vec<u8>>,
) -> Result<()> {
	// Initialize strings to hold normal records and graph edge records.
	// Write directly to strings to avoid unnecessary allocations.
	let mut records_normal = String::new();
	let mut records_relate = String::new();

	// Process each regular value.
	for (k, v) in regular_values {
		let k = RecordKey::decode_key(k)?;
		let rid = crate::val::RecordId {
			table: k.tb.into_owned(),
			key: k.id.into_owned(),
		};
		let v = Record::kv_decode_value(v, rid)?;
		// Process the value and categorize it into records_relate or records_normal.
		process_record(&v, &mut records_relate, &mut records_normal);
	}

	// If there are normal records, generate and send the INSERT SQL command.
	if !records_normal.is_empty() {
		let sql = format!("INSERT [ {} ];", records_normal);
		chn.send(bytes!(sql)).await?;
	}

	// If there are graph edge records, generate and send the INSERT RELATION SQL
	// command.
	if !records_relate.is_empty() {
		let sql = format!("INSERT RELATION [ {} ];", records_relate);
		chn.send(bytes!(sql)).await?;
	}

	Ok(())
}

pub(crate) fn define_access_statement_from_definition(
	base: Base,
	def: &catalog::AccessDefinition,
) -> DefineAccessStatement {
	fn convert_algorithm(access: catalog::Algorithm) -> Algorithm {
		match &access {
			catalog::Algorithm::EdDSA => Algorithm::EdDSA,
			catalog::Algorithm::Es256 => Algorithm::Es256,
			catalog::Algorithm::Es384 => Algorithm::Es384,
			catalog::Algorithm::Es512 => Algorithm::Es512,
			catalog::Algorithm::Hs256 => Algorithm::Hs256,
			catalog::Algorithm::Hs384 => Algorithm::Hs384,
			catalog::Algorithm::Hs512 => Algorithm::Hs512,
			catalog::Algorithm::Ps256 => Algorithm::Ps256,
			catalog::Algorithm::Ps384 => Algorithm::Ps384,
			catalog::Algorithm::Ps512 => Algorithm::Ps512,
			catalog::Algorithm::Rs256 => Algorithm::Rs256,
			catalog::Algorithm::Rs384 => Algorithm::Rs384,
			catalog::Algorithm::Rs512 => Algorithm::Rs512,
		}
	}

	fn convert_jwt_access(access: &catalog::JwtAccess) -> JwtAccess {
		JwtAccess {
			verify: match &access.verify {
				catalog::JwtAccessVerify::Key(k) => JwtAccessVerify::Key(JwtAccessVerifyKey {
					alg: convert_algorithm(k.alg),
					key: Expr::Literal(Literal::String(k.key.as_str().into())),
				}),
				catalog::JwtAccessVerify::Jwks(j) => JwtAccessVerify::Jwks(JwtAccessVerifyJwks {
					url: Expr::Literal(Literal::String(j.url.as_str().into())),
				}),
			},
			issue: access.issue.as_ref().map(|x| JwtAccessIssue {
				alg: convert_algorithm(x.alg),
				key: Expr::Literal(Literal::String(x.key.as_str().into())),
			}),
			audience: access.audience.as_ref().map(|a| {
				a.iter().map(|s| Expr::Literal(Literal::String(s.as_str().into()))).collect()
			}),
		}
	}

	fn convert_bearer_access(access: &catalog::BearerAccess) -> BearerAccess {
		BearerAccess {
			kind: match access.kind {
				catalog::BearerAccessType::Bearer => BearerAccessType::Bearer,
				catalog::BearerAccessType::Refresh => BearerAccessType::Refresh,
			},
			subject: match access.subject {
				catalog::BearerAccessSubject::Record => BearerAccessSubject::Record,
				catalog::BearerAccessSubject::User => BearerAccessSubject::User,
			},
			jwt: convert_jwt_access(&access.jwt),
		}
	}

	DefineAccessStatement {
		kind: DefineKind::Default,
		base,
		name: Expr::Idiom(Idiom::field(def.name.clone())),
		duration: AccessDuration {
			grant: def
				.grant_duration
				.map(|v| Expr::Literal(Literal::Duration(val::Duration(v))))
				.unwrap_or(Expr::Literal(Literal::None)),
			token: def
				.token_duration
				.map(|v| Expr::Literal(Literal::Duration(val::Duration(v))))
				.unwrap_or(Expr::Literal(Literal::None)),
			session: def
				.session_duration
				.map(|v| Expr::Literal(Literal::Duration(val::Duration(v))))
				.unwrap_or(Expr::Literal(Literal::None)),
		},
		comment: def
			.comment
			.clone()
			.map(|x| Expr::Literal(Literal::String(x.into())))
			.unwrap_or(Expr::Literal(Literal::None)),
		authenticate: def.authenticate.clone(),
		context: def.context.clone(),
		access_type: match &def.access_type {
			catalog::AccessType::Record(record_access) => {
				AccessType::Record(Box::new(RecordAccess {
					signup: def.signup.clone(),
					signin: def.signin.clone(),
					jwt: convert_jwt_access(&record_access.jwt),
					bearer: record_access.bearer.as_ref().map(convert_bearer_access),
				}))
			}
			catalog::AccessType::Jwt(jwt_access) => AccessType::Jwt(convert_jwt_access(jwt_access)),
			catalog::AccessType::Bearer(bearer_access) => {
				AccessType::Bearer(convert_bearer_access(bearer_access))
			}
		},
	}
}

pub(crate) fn define_analyzer_statement_from_definition(
	def: &catalog::AnalyzerDefinition,
) -> DefineAnalyzerStatement {
	DefineAnalyzerStatement {
		kind: DefineKind::Default,
		name: Expr::Idiom(Idiom::field(def.name.clone())),
		function: def.function.clone(),
		tokenizers: def.tokenizers.clone(),
		filters: def.filters.clone(),
		comment: def
			.comment
			.as_ref()
			.map(|x| Expr::Literal(Literal::String(x.as_str().into())))
			.unwrap_or(Expr::Literal(Literal::None)),
	}
}

pub(crate) fn define_user_statement_from_definition(
	base: Base,
	def: &catalog::UserDefinition,
) -> DefineUserStatement {
	DefineUserStatement {
		kind: DefineKind::Default,
		base,
		name: Expr::Idiom(Idiom::field(def.name.clone())),
		hash: def.hash.clone(),
		code: def.code.clone(),
		roles: def.roles.clone(),
		duration: UserDuration {
			token: def
				.token_duration
				.map(|x| Expr::Literal(Literal::Duration(val::Duration(x))))
				.unwrap_or(Expr::Literal(Literal::None)),
			session: def
				.session_duration
				.map(|x| Expr::Literal(Literal::Duration(val::Duration(x))))
				.unwrap_or(Expr::Literal(Literal::None)),
		},
		comment: def
			.comment
			.as_ref()
			.map(|x| Expr::Literal(Literal::String(x.as_str().into())))
			.unwrap_or(Expr::Literal(Literal::None)),
		scram: def.scram.clone(),
	}
}

#[cfg(test)]
mod tests {
	use super::{HashSet, INSERT_KEY_BUDGET, TableName, records_per_insert, view_order};

	/// The fewest keys a statement writes once, whatever its group size: one index
	/// whose maintenance is batched per transaction contributes a delta entry and a
	/// compaction-queue entry at commit. Stated here rather than read from the
	/// allowance, so this measures the grouping against what a statement costs
	/// instead of against the number the grouping already used.
	const STATEMENT_FLOOR: usize = 2;

	/// A group's records must leave room for what the statement writes once, or a
	/// group whose per-record cost divides the budget evenly lands exactly on it and
	/// is carried over by the statement's own entries.
	#[test]
	fn a_group_leaves_room_for_what_the_statement_itself_writes() {
		// A per-record cost that divides the whole budget evenly, which is the only
		// shape where the last record and the statement's entries compete.
		let per_record = 100;
		assert_eq!(INSERT_KEY_BUDGET % per_record, 0, "the case only bites on an exact division");
		let group = records_per_insert(per_record, u32::MAX);
		assert!(
			group * per_record + STATEMENT_FLOOR <= INSERT_KEY_BUDGET,
			"a group of {group} at {per_record} keys each writes {} plus the statement's own, \
			 over a budget of {INSERT_KEY_BUDGET}",
			group * per_record
		);
	}

	/// The scan batch stays the bound a deployment configures, and a record too
	/// expensive to share a statement still gets one of its own.
	#[test]
	fn grouping_respects_the_batch_and_never_reaches_zero() {
		assert_eq!(records_per_insert(1, 10), 10, "a cheap record groups at the batch");
		assert_eq!(
			records_per_insert(INSERT_KEY_BUDGET * 2, 10),
			1,
			"a record costing more than the whole budget still gets a statement"
		);
		assert_eq!(records_per_insert(0, 10), 10, "a zero estimate must not divide by zero");
	}

	/// A view's definition recomputes it from its sources, so a view reading
	/// another view has to come after it however the two sort by name.
	#[test]
	fn a_view_follows_the_views_it_reads() {
		let person = [TableName::from("person")];
		let zz = [TableName::from("zz_view")];
		// Name order puts the dependent first, which is the case a plain walk
		// of the table list gets wrong.
		let views = [("bb_vv", &zz[..]), ("zz_view", &person[..])];
		assert_eq!(view_order(&views).order, vec![1, 0]);
	}

	/// A source that is not itself being emitted here — a stored table, or one
	/// the export config excludes — places no constraint on the order.
	#[test]
	fn a_source_outside_the_set_does_not_defer_a_view() {
		let person = [TableName::from("person")];
		let views = [("aa_view", &person[..]), ("zz_view", &person[..])];
		assert_eq!(view_order(&views).order, vec![0, 1]);
	}

	/// A view that only reads a cycle is not itself in one. Both are emitted and
	/// both carry their rows, but the reason the dump states has to be true of
	/// the table it names.
	#[test]
	fn a_view_behind_a_cycle_is_not_reported_as_being_in_it() {
		let b = [TableName::from("b")];
		let c = [TableName::from("c")];
		let d = [TableName::from("d")];
		let c_again = [TableName::from("c")];
		// `a` reads `b` reads `c`, and `c` and `d` read each other.
		let views = [("a", &b[..]), ("b", &c[..]), ("c", &d[..]), ("d", &c_again[..])];
		let order = view_order(&views);
		assert_eq!(order.order.len(), 4, "every view is emitted");
		assert_eq!(
			order.cyclic,
			HashSet::<usize>::from([2, 3]),
			"only the two views that read each other are in the cycle"
		);
		assert_eq!(
			order.behind_cycle,
			HashSet::<usize>::from([0, 1]),
			"the chain feeding the cycle is unorderable without being part of it"
		);
	}

	/// A cycle cannot be ordered, so its members are emitted rather than
	/// dropped or spun on.
	#[test]
	fn a_cycle_is_emitted_last_rather_than_looping() {
		let a = [TableName::from("a")];
		let b = [TableName::from("b")];
		let base = [TableName::from("base")];
		let views = [("a", &b[..]), ("b", &a[..]), ("ok", &base[..])];
		let order = view_order(&views);
		assert_eq!(order.order.len(), 3, "every view is emitted");
		assert_eq!(order.order[0], 2, "the orderable view comes first");
		assert_eq!(order.order[1..], [0, 1], "the cycle keeps its original order");
		assert_eq!(
			order.cyclic,
			HashSet::<usize>::from([0, 1]),
			"the cycle's members are named, and the view that can be ordered is not"
		);
	}
}
