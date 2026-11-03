//! What an earlier release would refuse in a datastore: the rollback check.
//!
//! [`Datastore::rollback_check`] reads what a datastore records and lists what a build of an
//! earlier release (the *target*) could not read or maintain. It is a report: it changes nothing,
//! and it takes no part in opening a datastore.
//!
//! # What it reads
//!
//! - **The ledger** (`/!mg`). An entry the target lacks is one whose migration the target does not
//!   ship (its release is above the target) or whose id the running build has no migration for. The
//!   class of an entry lives in its id, so the report classes every finding without a decoder.
//! - **Layout descriptors** (`!lg`), read through the table's generation ring (`!mi`). A table
//!   without a ring has none, and a descriptor outside its table's ring is damage that no release
//!   reads. A generation whose descriptor needs a release above the target is one whose rows the
//!   target refuses; a table whose newest generation is listed refuses writes.
//! - **Catalog entries**, from a raw walk of every key family that holds a catalog type. Each value
//!   is judged by the decode the datastore's loader runs for its key. A stored entry records the
//!   oldest release able to read it: one above the target is a definition the target refuses, and
//!   its body is not decoded. Any other value is decoded whole, and one that does not decode is
//!   unreadable. A type whose every entry has the first stored release as its floor (a floor-fixed
//!   type) with a floor above that release is damage, since no release writes one. A table's
//!   conformance record (`!fc`) is one of the entries, and never blocks writes.
//! - **Unknown keys**: keys under a catalog band (the root, a namespace, a database or a table)
//!   that no key family of this build decodes.
//! - **The version history**: the downward notes show which releases below the stamp have written
//!   to the datastore.
//!
//! Entries and keys that do not parse are counted, not named, because a malformed value has no
//! trustworthy name.
//!
//! # The verdict
//!
//! [`RollbackCheck::verdict`] follows the ledger alone, which is what an earlier release itself
//! applies when it opens a datastore: it refuses on an incompatible or reserved-class entry it does
//! not know, and opens read-only on a read-only compatible one. Generation and entry findings do
//! not change the verdict. They list what the target refuses once it has opened the datastore, one
//! statement or row at a time. A store whose `in_use` is above the target is reported as a flag,
//! because lowering the feature level to the target is refused while it holds.

use std::borrow::Cow;

use anyhow::Result;
use surrealdb_catalog::stored::{CATALOG_MARKER, EntryError, StoredType, parse_header};
use surrealdb_catalog::{
	DatabaseDefinition, Generation, LayoutDescriptor, NamespaceDefinition, NodeLiveQuery,
	StoredFeatureLevel, TableConformance, TableMigrations, ViewState,
};
use surrealdb_strand::TableName;
use surrealdb_types::ToSql;

use crate::key::schema::{
	AnalyzerKey, AnalyzerPrefix, AnyKey, ApiKey, ApiPrefix, BucketKey, BucketPrefix, DatabaseKey,
	DatabasePrefix, DbAccessMethodKey, DbAccessMethodPrefix, DbAccessPrefix, DbConfigKey,
	DbConfigPrefix, DbGrantKey, DbPrefix, DbRoot, DbUserKey, DbUserPrefix, EventKey, EventPrefix,
	FeatureLevelKey, FieldKey, FieldPrefix, ForeignTableKey, ForeignTablePrefix, FunctionKey,
	FunctionPrefix, GenerationMarkKey, GenerationMarkPrefix, IndexDefKey, IndexDefPrefix,
	LayoutKey, LayoutPrefix, MlModelKey, MlModelPrefix, ModuleKey, ModulePrefix, NamespaceKey,
	NamespacePrefix, NodeEventQueueKey, NodeKey, NodeLiveQueryKey, NodePrefix, NodeReleaseKey,
	NodeReleasePrefix, NodeStatePrefix, NsAccessMethodKey, NsAccessMethodPrefix, NsAccessPrefix,
	NsBucketKey, NsBucketPrefix, NsGrantKey, NsPrefix, NsRoot, NsUserKey, NsUserPrefix, ParamKey,
	ParamPrefix, RootAccessMethodKey, RootAccessMethodPrefix, RootAccessPrefix, RootBucketKey,
	RootBucketPrefix, RootConfigKey, RootConfigPrefix, RootGrantKey, RootUserKey, RootUserPrefix,
	SequenceKey, SequencePrefix, SubscriptionKey, SubscriptionPrefix, TableConformanceKey,
	TableKey, TableMigrationsKey, TablePrefix, TaskLeaseKey, TaskLeasePrefix, TblPrefix, TblRoot,
	UserTypeKey, UserTypePrefix, ViewStateKey, ViewStatePrefix,
};
use crate::key::{
	AnyRange, KVKeyDecode, KVSubspace, KVValue, Key, RawRange, Resumable, TypedRange,
};
use crate::kvs::ds::Datastore;
use crate::kvs::migration::{self, is_downward_note, release_of};
use crate::kvs::version::VersionHistoryEntry;
use crate::kvs::version::ledger::{LedgerClass, LedgerId};
use crate::kvs::walk::{database_subtree, decode_after, holds, namespace_subtree};
use crate::kvs::{Direction, FeatureLevelState, Release, Transaction, TransactionType};

/// The number of keys one page of the catalog walk reads.
const PAGE: u32 = 1024;

/// What `target` would refuse in a datastore. See the module docs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RollbackCheck {
	/// The release the check was made for.
	pub target: Release,
	/// The feature level the datastore records.
	pub level: FeatureLevelState,
	/// Whether the datastore's `in_use` is above `target`: lowering the level to `target` is
	/// refused while it holds. The target still opens the datastore, and refuses what uses a newer
	/// feature.
	pub in_use_above_target: bool,
	/// Ledger entries the datastore records that `target` lacks.
	pub ledger: Vec<LedgerFinding>,
	/// Generations inside their table's ring whose descriptor needs a release above `target`.
	pub generations: Vec<GenerationFinding>,
	/// Catalog entries whose read floor is above `target`. A conformance record appears as an
	/// entry of its type, and never blocks writes. A generation mark or a ring never appears: no
	/// release writes one above the first stored release, so a refusal of one is damage and is
	/// counted in `unreadable`.
	pub entries: Vec<EntryFinding>,
	/// Catalog entries and layout keys the walk could not read, counted and not named, and layout
	/// keys outside their table's generation ring.
	pub unreadable: usize,
	/// Keys under a catalog band that no key family of this build decodes. Counted and not named,
	/// because a key no family decodes has no name this build can give it.
	pub unknown: usize,
	/// The downward notes in the version history: releases below the stamp that wrote to the
	/// datastore.
	pub notes: Vec<VersionHistoryEntry>,
}

/// A ledger entry the target lacks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LedgerFinding {
	/// The ledger id, as the key carries it.
	pub id: u32,
	/// The recorded name, or the name of the running build's migration. `None` when the entry is
	/// unreadable and this build does not know the id.
	pub name: Option<String>,
	/// What a release that does not know the entry does with the datastore, read from the id.
	pub class: LedgerClass,
	/// The release that introduced the entry, when this build has a migration for the id.
	pub since: Option<Release>,
}

/// A table generation whose descriptor the target cannot read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenerationFinding {
	/// The namespace's name.
	pub namespace: String,
	/// The database's name.
	pub database: String,
	/// The table.
	pub table: TableName,
	/// The generation the descriptor describes.
	pub generation: u64,
	/// The release the descriptor needs.
	pub needs: Release,
	/// Whether this is the head of the table's ring, the generation its writes stamp rows with, so
	/// the target refuses writes to the table.
	pub current: bool,
}

/// A catalog entry the target cannot read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntryFinding {
	/// The catalog type the entry holds.
	pub kind: &'static str,
	/// The namespace's name, for an entry beneath a namespace.
	pub namespace: Option<String>,
	/// The database's name, for an entry beneath a database.
	pub database: Option<String>,
	/// The table, for an entry beneath a table. A table's own definition is named by `name`.
	pub table: Option<TableName>,
	/// The entry's name as its key spells it. A grant is named `access/grant` and a machine
	/// learning model `name<version>`.
	pub name: String,
	/// The release the entry needs.
	pub needs: Release,
	/// Whether a write to the entry's table has to consult the entry, so the target refuses writes
	/// to the table: a field, an index, an event or a foreign table. A view's group state is
	/// consulted by every write that maintains the view, and blocks those writes.
	pub blocks_writes: bool,
}

/// What the target does with a datastore, by the ledger alone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RollbackVerdict {
	/// The target opens the datastore read-write.
	Opens,
	/// The target opens the datastore read-only.
	OpensReadOnly,
	/// The target refuses the datastore.
	Refused,
}

impl RollbackCheck {
	/// What the target does with the datastore: refuses it for an incompatible or reserved-class
	/// ledger entry, opens it read-only for a read-only compatible one, and otherwise opens it.
	///
	/// Generation and entry findings never change the verdict; they list what the target refuses
	/// after it has opened the datastore.
	pub fn verdict(&self) -> RollbackVerdict {
		let has = |wanted: &[LedgerClass]| self.ledger.iter().any(|f| wanted.contains(&f.class));
		if has(&[LedgerClass::Incompatible, LedgerClass::Reserved]) {
			RollbackVerdict::Refused
		} else if has(&[LedgerClass::ReadOnlyCompatible]) {
			RollbackVerdict::OpensReadOnly
		} else {
			RollbackVerdict::Opens
		}
	}
}

impl Datastore {
	/// What a build of release `target` would refuse in this datastore.
	///
	/// A report only: nothing is written, and the datastore keeps serving. Entries are read from
	/// storage rather than the catalog cache, so the answer is what is stored. The walk visits
	/// every namespace, database and table, one read transaction per database, and reads each
	/// catalog key family in pages.
	///
	/// # Errors
	///
	/// - `DatastoreError::FeatureLevelAboveRelease` when `target` is newer than this build, and
	///   `DatastoreError::FeatureLevelBelowFirst` when it precedes the first release with a feature
	///   level;
	/// - a storage error from reading the datastore.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(
			err,
			level = "trace",
			target = "surrealdb::core::kvs::rollback",
			skip(self)
		)
	)]
	pub async fn rollback_check(&self, target: Release) -> Result<RollbackCheck> {
		Self::check_feature_level_target(target)?;

		let mut audit = Audit::new(target);
		let level = self.read_level().await?;
		let ledger = self.ledger_findings(target).await?;

		let txn = self.transaction(TransactionType::Read).await?;
		let walked = audit.root(&txn).await;
		let _ = txn.cancel().await;
		let namespaces = walked?;

		for (ns, ns_name) in &namespaces {
			let txn = self.transaction(TransactionType::Read).await?;
			let walked = audit.namespace(&txn, *ns, ns_name).await;
			let _ = txn.cancel().await;
			for (db, db_name) in walked? {
				let txn = self.transaction(TransactionType::Read).await?;
				let walked = audit.database(&txn, *ns, ns_name, db, &db_name).await;
				let _ = txn.cancel().await;
				walked?;
			}
		}

		let notes = self.version_history().await?.into_iter().filter(is_downward_note).collect();

		Ok(RollbackCheck {
			target,
			level,
			in_use_above_target: level.in_use.is_some_and(|in_use| in_use > target),
			ledger,
			generations: audit.generations,
			entries: audit.entries,
			unreadable: audit.unreadable,
			unknown: audit.unknown,
			notes,
		})
	}

	/// The datastore's recorded feature level, as stored.
	async fn read_level(&self) -> Result<FeatureLevelState> {
		let txn = self.transaction(TransactionType::Read).await?;
		let level = txn.feature_level_state().await;
		let _ = txn.cancel().await;
		level
	}

	/// The ledger entries `target` lacks.
	///
	/// An entry this build has a migration for is lacking when that migration's release is above
	/// `target`. An entry this build has no migration for is lacking, with its introducing release
	/// unknown, because a build that does not know it cannot be assumed to ship it.
	async fn ledger_findings(&self, target: Release) -> Result<Vec<LedgerFinding>> {
		let mut findings = Vec::new();
		for (id, record) in self.applied_migration_ids().await? {
			let class = LedgerId::from_raw(id).class();
			match migration::LEDGER.iter().find(|m| m.id == id) {
				Some(known) => {
					let since = release_of(known.version);
					if since > target {
						findings.push(LedgerFinding {
							id,
							name: Some(known.name.to_owned()),
							class,
							since: Some(since),
						});
					}
				}
				None => findings.push(LedgerFinding {
					id,
					name: record.map(|record| record.name),
					class,
					since: None,
				}),
			}
		}
		Ok(findings)
	}
}

/// How the target judges one stored value of the catalog type `V`.
enum Judgement<V> {
	/// The target reads it, and this is its decoded value.
	Reads(V),
	/// The target refuses it: it needs this release.
	Needs(Release),
	/// The bytes do not decode as a value of the type, or the type's floor rule rules them out.
	Unreadable,
}

/// Judges `bytes`, a value of the catalog type `V`, for a build of release `target`.
///
/// A stored entry whose floor is above `target` is `Needs`, judged on its header alone. A floor
/// above the first stored release on a floor-fixed type is `Unreadable`, since no release writes
/// one. Every other value is decoded whole with [`KVValue::kv_decode_value`], the decode the
/// datastore's loader runs for its key, so a value that the loader refuses is never judged readable
/// here.
fn judge<V>(bytes: &[u8], target: Release) -> Judgement<V>
where
	V: KVValue<KeyContext = ()> + StoredType,
{
	let schema = <V as StoredType>::SCHEMA;
	if bytes.first() == Some(&CATALOG_MARKER) {
		let floor = match parse_header(schema.name, bytes) {
			Ok((floor, _)) => floor,
			Err(EntryError::NeedsRelease {
				needed,
				..
			}) => needed,
			Err(_) => return Judgement::Unreadable,
		};
		if schema.floor_fixed && floor > Release::FIRST_STORED {
			return Judgement::Unreadable;
		}
		if floor > target {
			return Judgement::Needs(floor);
		}
	}
	match V::kv_decode_value(bytes, ()) {
		Ok(value) => Judgement::Reads(value),
		Err(_) => Judgement::Unreadable,
	}
}

/// Reads `range` in pages of [`PAGE`] keys and passes each key and value to `visit`.
///
/// Memory is bounded by the page, not the range.
async fn scan_pages<R>(
	txn: &Transaction,
	range: R,
	mut visit: impl FnMut(&[u8], &[u8]),
) -> Result<()>
where
	R: AnyRange + Resumable + Clone,
{
	let mut range = range;
	loop {
		let rows = txn.scan_raw(range.clone(), PAGE, 0, None).await?;
		for (key, value) in &rows {
			visit(key, value);
		}
		match rows.last() {
			Some((last, _)) if rows.len() == PAGE as usize => {
				range = range.resume_after(last, Direction::Forward);
			}
			_ => return Ok(()),
		}
	}
}

/// The subtrees under `band`, found by reading one key per subtree and stepping past it.
/// `subtree` names the subtree that holds a key and the first key past that subtree; a key it
/// names no subtree for is counted in `unreadable` and stepped over.
async fn subtrees<T>(
	txn: &Transaction,
	band: RawRange,
	unreadable: &mut usize,
	subtree: impl Fn(&[u8]) -> Option<(T, Key<'static>)>,
) -> Result<Vec<T>> {
	let mut found = Vec::new();
	let mut range = band.clone();
	loop {
		let rows = txn.scan_raw(range.clone(), 1, 0, None).await?;
		let Some((key, _)) = rows.first() else {
			return Ok(found);
		};
		match subtree(key) {
			Some((id, past)) => {
				found.push(id);
				range = band.part(past.as_slice(), band.bounds().1);
			}
			None => {
				*unreadable += 1;
				range = range.resume_after(key, Direction::Forward);
			}
		}
	}
}

/// The subtrees `found`, each named by the definition in `named` with its id, or by its id when
/// no definition read.
fn named_by<Id: Copy + PartialEq + std::fmt::Display>(
	found: Vec<Id>,
	named: &[(Id, String)],
) -> Vec<(Id, String)> {
	found
		.into_iter()
		.map(|id| {
			let name = named.iter().find(|(n, _)| *n == id).map(|(_, name)| name.clone());
			(id, name.unwrap_or_else(|| format!("#{id}")))
		})
		.collect()
}

/// The name of a value under a key that carries none.
fn unnamed(_: &[u8]) -> Result<String> {
	Ok(String::new())
}

/// Where an entry sits in the catalog, for naming it in a finding.
#[derive(Clone, Copy, Default)]
struct Scope<'a> {
	namespace: Option<&'a str>,
	database: Option<&'a str>,
	table: Option<&'a TableName>,
}

/// Decodes a key into the name its finding carries.
type Namer = fn(&[u8]) -> Result<String>;

/// Builds a [`Namer`] from a key type and the expression that names a decoded key.
macro_rules! namer {
	($key:ident, $k:ident => $name:expr) => {
		(|bytes: &[u8]| -> Result<String> {
			let $k = $key::decode_key(bytes)?;
			Ok($name)
		}) as Namer
	};
}

/// What the walk has found so far.
struct Audit {
	target: Release,
	entries: Vec<EntryFinding>,
	generations: Vec<GenerationFinding>,
	unreadable: usize,
	unknown: usize,
}

impl Audit {
	fn new(target: Release) -> Self {
		Self {
			target,
			entries: Vec::new(),
			generations: Vec::new(),
			unreadable: 0,
			unknown: 0,
		}
	}

	/// Records the judgement of the value at `key`, and answers the decoded value when the target
	/// reads it.
	///
	/// An entry whose key does not decode is counted once, whatever its value, and its value still
	/// answers when it reads, so the subtree a definition names is still named.
	fn record<V>(
		&mut self,
		judgement: Judgement<V>,
		kind: &'static str,
		blocks_writes: bool,
		scope: &Scope<'_>,
		key: &[u8],
		name: Namer,
	) -> Option<V> {
		let name = match name(key) {
			Ok(name) => name,
			Err(_) => {
				self.unreadable += 1;
				return match judgement {
					Judgement::Reads(value) => Some(value),
					Judgement::Needs(_) | Judgement::Unreadable => None,
				};
			}
		};
		match judgement {
			Judgement::Reads(value) => Some(value),
			Judgement::Unreadable => {
				self.unreadable += 1;
				None
			}
			Judgement::Needs(needs) => {
				self.entries.push(EntryFinding {
					kind,
					namespace: scope.namespace.map(str::to_owned),
					database: scope.database.map(str::to_owned),
					table: scope.table.cloned(),
					name,
					needs,
					blocks_writes,
				});
				None
			}
		}
	}

	/// Judges one catalog entry at a point key, which holds a single value of its type.
	fn point<V>(&mut self, bytes: &[u8], scope: &Scope<'_>) -> Option<V>
	where
		V: KVValue<KeyContext = ()> + StoredType,
	{
		let judgement = judge::<V>(bytes, self.target);
		self.record(judgement, <V as StoredType>::SCHEMA.name, false, scope, &[], unnamed)
	}

	/// Walks one key family: every value in `range` is a catalog entry of the type `V`. A value the
	/// target reads is handed to `visit`; any other is recorded as its judgement says.
	async fn family<V>(
		&mut self,
		txn: &Transaction,
		range: TypedRange<V>,
		blocks_writes: bool,
		scope: &Scope<'_>,
		name: Namer,
		mut visit: impl FnMut(V),
	) -> Result<()>
	where
		V: KVValue<KeyContext = ()> + StoredType,
	{
		let target = self.target;
		let kind = <V as StoredType>::SCHEMA.name;
		scan_pages(txn, range, |key, value| {
			let judgement = judge::<V>(value, target);
			if let Some(read) = self.record(judgement, kind, blocks_writes, scope, key, name) {
				visit(read);
			}
		})
		.await
	}

	/// Counts the keys under the catalog band of `level` that no key family of this build decodes.
	///
	/// The band is `level` followed by `!`. A key that decodes belongs to a family, and the walk
	/// steps past the whole family: its first `level.len() + 3` bytes are the level, the `!` and
	/// the family's two letters. A key that does not decode is counted and stepped over.
	async fn unknown_families(&mut self, txn: &Transaction, level: &[u8]) -> Result<()> {
		let mut bound = level.to_vec();
		bound.push(b'!');
		let Some(band) = RawRange::prefix_of(Key::from(bound)) else {
			return Ok(());
		};
		let depth = level.len() + 3;
		let mut range = band.clone();
		loop {
			let rows = txn.scan_raw(range.clone(), 1, 0, None).await?;
			let Some((key, _)) = rows.first() else {
				return Ok(());
			};
			let bytes = key.as_slice();
			range = match (AnyKey::decode(bytes), bytes.get(..depth)) {
				(Some(_), Some(family)) => {
					let past = Key::from(family.to_vec()).next_neighbour_expect();
					band.part(past.as_slice(), band.bounds().1)
				}
				_ => {
					self.unknown += 1;
					range.resume_after(bytes, Direction::Forward)
				}
			};
		}
	}

	/// The root level, and the namespaces found there.
	async fn root(
		&mut self,
		txn: &Transaction,
	) -> Result<Vec<(crate::catalog::NamespaceId, String)>> {
		let scope = Scope::default();

		if let Some(bytes) = txn.get_key_raw(&FeatureLevelKey {}, None).await? {
			self.point::<StoredFeatureLevel>(&bytes, &scope);
		}

		let mut namespaces = Vec::new();
		self.family(
			txn,
			NamespacePrefix {}.range()?,
			false,
			&scope,
			namer!(NamespaceKey, k => k.ns.into_owned()),
			|definition: NamespaceDefinition| {
				namespaces.push((definition.namespace_id, definition.name.as_str().to_owned()));
			},
		)
		.await?;

		self.family(
			txn,
			RootUserPrefix {}.range()?,
			false,
			&scope,
			namer!(RootUserKey, k => k.user.into_owned()),
			|_| {},
		)
		.await?;
		self.family(
			txn,
			RootConfigPrefix {}.range()?,
			false,
			&scope,
			namer!(RootConfigKey, k => k.ty.into_owned()),
			|_| {},
		)
		.await?;
		self.family(
			txn,
			TaskLeasePrefix {}.range()?,
			false,
			&scope,
			namer!(TaskLeaseKey, k => format!("{:?}", k.task_id)),
			|_| {},
		)
		.await?;

		self.family(
			txn,
			NodePrefix {}.range()?,
			false,
			&scope,
			namer!(NodeKey, k => k.nd.to_string()),
			|_| {},
		)
		.await?;
		self.family(
			txn,
			NodeReleasePrefix {}.range()?,
			false,
			&scope,
			namer!(NodeReleaseKey, k => k.nd.to_string()),
			|_| {},
		)
		.await?;
		self.family(
			txn,
			RootAccessMethodPrefix {}.range()?,
			false,
			&scope,
			namer!(RootAccessMethodKey, k => k.ac.into_owned()),
			|_| {},
		)
		.await?;
		self.family(
			txn,
			RootBucketPrefix {}.range()?,
			false,
			&scope,
			namer!(RootBucketKey, k => k.bu.into_owned()),
			|_| {},
		)
		.await?;
		// Every key under an access method is a grant: the band is walked whole, so a grant whose
		// access method is missing or malformed is still judged.
		self.family(
			txn,
			RootAccessPrefix {}.range()?,
			false,
			&scope,
			namer!(RootGrantKey, k => format!("{}/{}", k.ac, k.gr)),
			|_| {},
		)
		.await?;
		// Every key under a node's state is a live query or a queued asynchronous event: the band
		// is walked whole, so a live query whose node row is missing or malformed is still judged.
		// A queued event is a `revision` container holding its definition, not a catalog entry
		// of its own, and is passed over as the root-level queue is. Neither its place nor its
		// revision needs a release the target lacks: every target this check accepts is at or
		// above `Release::FIRST_STORED`, the first release whose drain reads per-node queues and
		// decodes the record at its current revision. A release below that is refused as a target
		// rather than judged.
		{
			let target = self.target;
			let kind = <NodeLiveQuery as StoredType>::SCHEMA.name;
			let name = namer!(NodeLiveQueryKey, k => k.lq.to_string());
			scan_pages(txn, NodeStatePrefix {}.range()?, |key, value| {
				if NodeEventQueueKey::decode_key(key).is_ok() {
					return;
				}
				let judgement = judge::<NodeLiveQuery>(value, target);
				self.record(judgement, kind, false, &scope, key, name);
			})
			.await?;
		}

		// Namespaces are found from the keys under the namespaces' band, so the subtree of one
		// whose definition is missing or malformed is still walked. It is named by its id.
		let band = NsPrefix {};
		let prefix = KVSubspace::encode_bound(&band)?;
		let found = subtrees(txn, band.range()?, &mut self.unreadable, |key| {
			namespace_subtree(&prefix, key)
		})
		.await?;
		self.unknown_families(txn, b"/").await?;
		Ok(named_by(found, &namespaces))
	}

	/// One namespace's level, and the databases found there.
	async fn namespace(
		&mut self,
		txn: &Transaction,
		ns: crate::catalog::NamespaceId,
		ns_name: &str,
	) -> Result<Vec<(crate::catalog::DatabaseId, String)>> {
		let scope = Scope {
			namespace: Some(ns_name),
			..Scope::default()
		};

		let mut databases = Vec::new();
		self.family(
			txn,
			DatabasePrefix {
				ns,
			}
			.range()?,
			false,
			&scope,
			namer!(DatabaseKey, k => k.db.into_owned()),
			|definition: DatabaseDefinition| {
				databases.push((definition.database_id, definition.name.as_str().to_owned()));
			},
		)
		.await?;

		self.family(
			txn,
			NsUserPrefix {
				ns,
			}
			.range()?,
			false,
			&scope,
			namer!(NsUserKey, k => k.user.into_owned()),
			|_| {},
		)
		.await?;
		self.family(
			txn,
			NsAccessMethodPrefix {
				ns,
			}
			.range()?,
			false,
			&scope,
			namer!(NsAccessMethodKey, k => k.ac.into_owned()),
			|_| {},
		)
		.await?;
		self.family(
			txn,
			NsBucketPrefix {
				ns,
			}
			.range()?,
			false,
			&scope,
			namer!(NsBucketKey, k => k.bu.into_owned()),
			|_| {},
		)
		.await?;
		// Every key under an access method is a grant: the band is walked whole, so a grant whose
		// access method is missing or malformed is still judged.
		self.family(
			txn,
			NsAccessPrefix {
				ns,
			}
			.range()?,
			false,
			&scope,
			namer!(NsGrantKey, k => format!("{}/{}", k.ac, k.gr)),
			|_| {},
		)
		.await?;

		// Databases are found from the keys under the namespace's databases' band, as namespaces
		// are.
		let band = DbPrefix {
			ns,
		};
		let prefix = KVSubspace::encode_bound(&band)?;
		let found = subtrees(txn, band.range()?, &mut self.unreadable, |key| {
			database_subtree(ns, &prefix, key)
		})
		.await?;
		let level = KVSubspace::encode_bound(&NsRoot {
			ns,
		})?;
		self.unknown_families(txn, level.as_slice()).await?;
		Ok(named_by(found, &databases))
	}

	/// One database's level, and every table beneath it.
	async fn database(
		&mut self,
		txn: &Transaction,
		ns: crate::catalog::NamespaceId,
		ns_name: &str,
		db: crate::catalog::DatabaseId,
		db_name: &str,
	) -> Result<()> {
		let scope = Scope {
			namespace: Some(ns_name),
			database: Some(db_name),
			table: None,
		};

		self.family(
			txn,
			TablePrefix {
				ns,
				db,
			}
			.range()?,
			false,
			&scope,
			namer!(TableKey, k => k.tb.as_str().to_owned()),
			|_| {},
		)
		.await?;

		self.family(
			txn,
			DbUserPrefix {
				ns,
				db,
			}
			.range()?,
			false,
			&scope,
			namer!(DbUserKey, k => k.user.into_owned()),
			|_| {},
		)
		.await?;
		self.family(
			txn,
			ApiPrefix {
				ns,
				db,
			}
			.range()?,
			false,
			&scope,
			namer!(ApiKey, k => k.ap.into_owned()),
			|_| {},
		)
		.await?;
		self.family(
			txn,
			AnalyzerPrefix {
				ns,
				db,
			}
			.range()?,
			false,
			&scope,
			namer!(AnalyzerKey, k => k.az.into_owned()),
			|_| {},
		)
		.await?;
		self.family(
			txn,
			BucketPrefix {
				ns,
				db,
			}
			.range()?,
			false,
			&scope,
			namer!(BucketKey, k => k.bu.into_owned()),
			|_| {},
		)
		.await?;
		self.family(
			txn,
			UserTypePrefix {
				ns,
				db,
			}
			.range()?,
			false,
			&scope,
			namer!(UserTypeKey, k => k.ty.into_owned()),
			|_| {},
		)
		.await?;
		self.family(
			txn,
			DbConfigPrefix {
				ns,
				db,
			}
			.range()?,
			false,
			&scope,
			namer!(DbConfigKey, k => k.ty.into_owned()),
			|_| {},
		)
		.await?;
		self.family(
			txn,
			FunctionPrefix {
				ns,
				db,
			}
			.range()?,
			false,
			&scope,
			namer!(FunctionKey, k => k.fc.into_owned()),
			|_| {},
		)
		.await?;
		self.family(
			txn,
			ModulePrefix {
				ns,
				db,
			}
			.range()?,
			false,
			&scope,
			namer!(ModuleKey, k => k.md.into_owned()),
			|_| {},
		)
		.await?;
		self.family(
			txn,
			MlModelPrefix {
				ns,
				db,
			}
			.range()?,
			false,
			&scope,
			namer!(MlModelKey, k => format!("{}<{}>", k.ml, k.vn)),
			|_| {},
		)
		.await?;
		self.family(
			txn,
			ParamPrefix {
				ns,
				db,
			}
			.range()?,
			false,
			&scope,
			namer!(ParamKey, k => k.pa.into_owned()),
			|_| {},
		)
		.await?;
		self.family(
			txn,
			SequencePrefix {
				ns,
				db,
			}
			.range()?,
			false,
			&scope,
			namer!(SequenceKey, k => k.sq.into_owned()),
			|_| {},
		)
		.await?;

		self.family(
			txn,
			DbAccessMethodPrefix {
				ns,
				db,
			}
			.range()?,
			false,
			&scope,
			namer!(DbAccessMethodKey, k => k.ac.into_owned()),
			|_| {},
		)
		.await?;
		// Every key under an access method is a grant: the band is walked whole, so a grant whose
		// access method is missing or malformed is still judged.
		self.family(
			txn,
			DbAccessPrefix {
				ns,
				db,
			}
			.range()?,
			false,
			&scope,
			namer!(DbGrantKey, k => format!("{}/{}", k.ac, k.gr)),
			|_| {},
		)
		.await?;

		self.family(
			txn,
			GenerationMarkPrefix {
				ns,
				db,
			}
			.range()?,
			false,
			&scope,
			namer!(GenerationMarkKey, k => k.tb.as_str().to_owned()),
			|_| {},
		)
		.await?;

		let level = KVSubspace::encode_bound(&DbRoot {
			ns,
			db,
		})?;
		self.unknown_families(txn, level.as_slice()).await?;

		// Tables are found from the keys under the tables' band, not from their definitions, so the
		// subtree of a table whose definition is missing or malformed is still walked. One key is
		// read per table, then the walk steps past that table's subtree.
		let band = TblPrefix {
			ns,
			db,
		};
		let prefix = KVSubspace::encode_bound(&band)?;
		let tables = subtrees(txn, band.range()?, &mut self.unreadable, |key| {
			let table = decode_after::<Cow<'_, TableName>>(&prefix, key)?;
			let root = TblRoot {
				ns,
				db,
				tb: Cow::Borrowed(table.as_ref()),
			};
			holds(&root.range_subtree().ok()?, key)?;
			Some((table.clone().into_owned(), root.skip_extensions().ok()?))
		})
		.await?;
		for table in &tables {
			self.table(txn, ns, db, &scope, table).await?;
		}
		Ok(())
	}

	/// One table's definitions, conformance record and layout descriptors.
	///
	/// A field, an index, an event and a foreign table are consulted by every write to the table,
	/// and a view's group state by every write that maintains the view, so an entry of one of
	/// those the target cannot read makes the target refuse those writes.
	async fn table(
		&mut self,
		txn: &Transaction,
		ns: crate::catalog::NamespaceId,
		db: crate::catalog::DatabaseId,
		parent: &Scope<'_>,
		table: &TableName,
	) -> Result<()> {
		let scope = Scope {
			table: Some(table),
			..*parent
		};

		self.family(
			txn,
			FieldPrefix {
				ns,
				db,
				tb: Cow::Borrowed(table),
			}
			.range()?,
			true,
			&scope,
			namer!(FieldKey, k => k.fd.into_owned()),
			|_| {},
		)
		.await?;
		self.family(
			txn,
			IndexDefPrefix {
				ns,
				db,
				tb: Cow::Borrowed(table),
			}
			.range()?,
			true,
			&scope,
			namer!(IndexDefKey, k => k.ix.into_owned()),
			|_| {},
		)
		.await?;
		self.family(
			txn,
			EventPrefix {
				ns,
				db,
				tb: Cow::Borrowed(table),
			}
			.range()?,
			true,
			&scope,
			namer!(EventKey, k => k.ev.into_owned()),
			|_| {},
		)
		.await?;
		self.family(
			txn,
			ForeignTablePrefix {
				ns,
				db,
				tb: Cow::Borrowed(table),
			}
			.range()?,
			true,
			&scope,
			namer!(ForeignTableKey, k => k.ft.as_str().to_owned()),
			|_| {},
		)
		.await?;
		self.family(
			txn,
			SubscriptionPrefix {
				ns,
				db,
				tb: Cow::Borrowed(table),
			}
			.range()?,
			false,
			&scope,
			namer!(SubscriptionKey, k => k.lq.to_string()),
			|_| {},
		)
		.await?;
		self.family::<ViewState>(
			txn,
			ViewStatePrefix {
				ns,
				db,
				tb: Cow::Borrowed(table),
			}
			.range()?,
			true,
			&scope,
			namer!(ViewStateKey, k => k.id.to_sql()),
			|_| {},
		)
		.await?;

		// A conformance record never blocks writes: a target that cannot read it reads no claims,
		// and the target's own field changes and imports remove it.
		let conformance = TableConformanceKey {
			ns,
			db,
			tb: Cow::Borrowed(table),
		};
		if let Some(bytes) = txn.get_key_raw(&conformance, None).await? {
			self.point::<TableConformance>(&bytes, &scope);
		}

		let level = KVSubspace::encode_bound(&TblRoot {
			ns,
			db,
			tb: Cow::Borrowed(table),
		})?;
		self.unknown_families(txn, level.as_slice()).await?;

		self.layouts(txn, ns, db, &scope, table).await
	}

	/// One table's generation ring and the descriptors inside it, judged as the datastore reads
	/// them: every descriptor of a table without a readable ring counted as damage, a ring the
	/// target cannot read counted as damage (no release writes one it refuses: the type is
	/// `floor_fixed`), and only the descriptors inside the ring. The table's head generation is
	/// the one its writes stamp rows with.
	///
	/// A key that does not decode, or names a generation outside the ring (generation 0 included),
	/// is counted and not judged: the datastore never decodes it. A descriptor is read only under
	/// the table it names, as the layout loader reads it. A ring whose head has no descriptor is
	/// counted too: every write to the table stamps the head, and fails without its descriptor.
	async fn layouts(
		&mut self,
		txn: &Transaction,
		ns: crate::catalog::NamespaceId,
		db: crate::catalog::DatabaseId,
		scope: &Scope<'_>,
		table: &TableName,
	) -> Result<()> {
		let migrations = TableMigrationsKey {
			ns,
			db,
			tb: Cow::Borrowed(table),
		};
		let Some(bytes) = txn.get_key_raw(&migrations, None).await? else {
			// With no ring every descriptor is outside it, which no write leaves: each is damage.
			return self.descriptors_outside_a_ring(txn, ns, db, table).await;
		};
		// A ring is `floor_fixed`: a value the target does not read is damage, and so is one that
		// does not validate as a ring. No descriptor is read through a ring that is not read, so
		// each is damage too.
		let ring = match judge::<TableMigrations>(&bytes, self.target) {
			Judgement::Reads(migrations) => migrations.ring().ok(),
			Judgement::Needs(_) | Judgement::Unreadable => None,
		};
		let Some(ring) = ring else {
			self.unreadable += 1;
			return self.descriptors_outside_a_ring(txn, ns, db, table).await;
		};

		let mut found: Vec<GenerationFinding> = Vec::new();
		let mut head_listed = false;
		let target = self.target;
		let unreadable = &mut self.unreadable;
		scan_pages(
			txn,
			LayoutPrefix {
				ns,
				db,
				tb: Cow::Borrowed(table),
			}
			.range()?,
			|key, value| {
				let generation = match LayoutKey::decode_key(key)
					.ok()
					.and_then(|layout| Generation::new(layout.generation))
				{
					Some(generation) if ring.contains(generation) => generation,
					_ => {
						*unreadable += 1;
						return;
					}
				};
				if ring.head() == Some(generation) {
					head_listed = true;
				}
				match judge::<LayoutDescriptor>(value, target) {
					// The layout loader reads a descriptor only when it names the table it is
					// stored under.
					Judgement::Reads(descriptor) => {
						if descriptor.table != *table {
							*unreadable += 1;
						}
					}
					Judgement::Unreadable => *unreadable += 1,
					Judgement::Needs(needs) => found.push(GenerationFinding {
						namespace: scope.namespace.unwrap_or_default().to_owned(),
						database: scope.database.unwrap_or_default().to_owned(),
						table: table.clone(),
						generation: u64::from(generation.get()),
						needs,
						current: ring.head() == Some(generation),
					}),
				}
			},
		)
		.await?;
		if ring.head().is_some() && !head_listed {
			self.unreadable += 1;
		}
		self.generations.extend(found);
		Ok(())
	}

	/// Counts every layout descriptor of `table` unreadable: the table has no ring the datastore
	/// reads them through.
	async fn descriptors_outside_a_ring(
		&mut self,
		txn: &Transaction,
		ns: crate::catalog::NamespaceId,
		db: crate::catalog::DatabaseId,
		table: &TableName,
	) -> Result<()> {
		let unreadable = &mut self.unreadable;
		scan_pages(
			txn,
			LayoutPrefix {
				ns,
				db,
				tb: Cow::Borrowed(table),
			}
			.range()?,
			|_, _| *unreadable += 1,
		)
		.await
	}
}

#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod tests {
	use std::borrow::Cow;

	use surrealdb_catalog::stored::StoredType;
	use surrealdb_catalog::{StoredFieldDefinition, ViewState};
	use uuid::Uuid;

	use crate::catalog::providers::DatabaseProvider;
	use crate::dbs::Session;
	use crate::key::schema::{
		DbRoot, FieldKey, GenerationMarkKey, LayoutKey, NamespaceKey, NodeKey, NsRoot,
		TableMigrationsKey, ViewStateKey,
	};
	use crate::key::{KVKey, KVSubspace, KVValue, Key};
	use crate::kvs::compat::v3_3_0;
	use crate::kvs::ds::Datastore;
	use crate::kvs::test_support::with_future_floor;
	use crate::kvs::{DatastoreError, Release, TransactionType};
	use crate::val::{Array, RecordIdKey, TableName, Value};

	/// A datastore holding table `person` with field `name`, and its namespace and database ids.
	async fn with_person()
	-> (std::sync::Arc<Datastore>, crate::catalog::NamespaceId, crate::catalog::DatabaseId) {
		let ds = Datastore::new("memory").await.unwrap();
		let session = Session::owner().with_ns("test").with_db("test");
		for response in ds
			.execute(
				"DEFINE NAMESPACE test; USE NS test; DEFINE DATABASE test; USE DB test; \
				 DEFINE TABLE person; DEFINE FIELD name ON person TYPE string; \
				 CREATE person:1 SET name = 'a';",
				&session,
				None,
			)
			.await
			.unwrap()
		{
			response.result.unwrap();
		}
		let txn = ds.transaction(TransactionType::Read).await.unwrap();
		let db = txn.expect_db_by_name("test", "test").await.unwrap();
		txn.cancel().await.unwrap();
		(ds, db.namespace_id, db.database_id)
	}

	/// Writes `value` at `key` and commits.
	async fn put(ds: &Datastore, key: crate::key::Key<'static>, value: &[u8]) {
		let txn = ds.transaction(TransactionType::Write).await.unwrap();
		txn.set(key, value.to_vec()).await.unwrap();
		txn.commit().await.unwrap();
	}

	/// A release before the first stored one drains neither per-node event queues nor the
	/// record revision they hold, so the check refuses it as a target instead of reporting a
	/// rollback to it.
	#[tokio::test]
	async fn a_release_before_the_first_stored_one_is_not_a_target() {
		let (ds, _, _) = with_person().await;
		let err = ds.rollback_check(Release::new(3, 3)).await.unwrap_err();
		assert!(
			matches!(
				err.downcast_ref::<DatastoreError>(),
				Some(DatastoreError::FeatureLevelBelowFirst { .. })
			),
			"{err}"
		);
	}

	#[tokio::test]
	async fn a_written_catalog_is_readable() {
		let (ds, _, _) = with_person().await;
		let check = ds.rollback_check(Release::CURRENT).await.unwrap();
		assert_eq!(check.unreadable, 0, "{check:?}");
		assert_eq!(check.unknown, 0, "{check:?}");
	}

	#[tokio::test]
	async fn a_well_formed_revision_definition_is_readable() {
		let (ds, ns, db) = with_person().await;
		let field = FieldKey::new(
			ns,
			db,
			Cow::Owned(TableName::new("person")),
			Cow::Owned("name".to_owned()),
		);
		put(&ds, field.encode_key().unwrap(), v3_3_0::FIELD_WITH_TYPE).await;
		let check = ds.rollback_check(Release::CURRENT).await.unwrap();
		assert_eq!(check.unreadable, 0, "{check:?}");
	}

	#[tokio::test]
	async fn a_malformed_revision_definition_is_unreadable() {
		let (ds, ns, db) = with_person().await;
		let field = FieldKey::new(
			ns,
			db,
			Cow::Owned(TableName::new("person")),
			Cow::Owned("name".to_owned()),
		);
		let mut truncated = v3_3_0::FIELD_WITH_TYPE.to_vec();
		truncated.truncate(truncated.len() / 2);
		put(&ds, field.encode_key().unwrap(), &truncated).await;
		let check = ds.rollback_check(Release::CURRENT).await.unwrap();
		assert_eq!(check.unreadable, 1, "{check:?}");
	}

	/// A revision definition whose string is not UTF-8 is unreadable: the judge decodes it whole,
	/// as the loader does, rather than skipping it.
	#[tokio::test]
	async fn a_revision_definition_with_invalid_utf8_is_unreadable() {
		let (ds, ns, db) = with_person().await;
		let mut bytes = v3_3_0::FIELD_WITH_TYPE.to_vec();
		let at = bytes.windows(4).position(|w| w == b"User").expect("the comment names User");
		bytes[at] = 0xff;
		assert!(StoredFieldDefinition::kv_decode_value(&bytes, ()).is_err());
		let field = FieldKey::new(
			ns,
			db,
			Cow::Owned(TableName::new("person")),
			Cow::Owned("name".to_owned()),
		);
		put(&ds, field.encode_key().unwrap(), &bytes).await;
		let check = ds.rollback_check(Release::CURRENT).await.unwrap();
		assert_eq!(check.unreadable, 1, "{check:?}");
		assert!(check.entries.is_empty(), "{check:?}");
	}

	/// A stored entry whose header the target reads is judged by its body too: one cut short is
	/// unreadable, in a family of definitions and in a datastore-only record alike.
	#[tokio::test]
	async fn a_stored_entry_with_a_truncated_body_is_unreadable() {
		let (ds, ns, db) = with_person().await;
		let field = FieldKey::new(
			ns,
			db,
			Cow::Owned(TableName::new("person")),
			Cow::Owned("name".to_owned()),
		)
		.encode_key()
		.unwrap();
		let ring = TableMigrationsKey::new(ns, db, Cow::Owned(TableName::new("person")))
			.encode_key()
			.unwrap();
		for key in [field, ring] {
			let txn = ds.transaction(TransactionType::Read).await.unwrap();
			let written = txn.get(key.clone(), None).await.unwrap().expect("written");
			txn.cancel().await.unwrap();
			assert_eq!(written.first(), Some(&surrealdb_catalog::stored::CATALOG_MARKER));
			let (_, body) = surrealdb_catalog::stored::parse_header("any", &written).unwrap();
			assert!(!body.is_empty());
			let header = written[..written.len() - body.len()].to_vec();
			put(&ds, key.clone(), &header).await;
			let check = ds.rollback_check(Release::CURRENT).await.unwrap();
			assert!(check.unreadable >= 1, "{check:?}");
			put(&ds, key, &written).await;
			let check = ds.rollback_check(Release::CURRENT).await.unwrap();
			assert_eq!(check.unreadable, 0, "{check:?}");
		}
	}

	/// A floor-fixed type writes no floor above its first stored release, so a node record with a
	/// future floor is damage: counted unreadable and never a finding.
	#[tokio::test]
	async fn a_floor_fixed_entry_with_a_future_floor_is_unreadable() {
		let (ds, _, _) = with_person().await;
		let key = NodeKey::new(Uuid::from_u128(1)).encode_key().unwrap();
		put(&ds, key, &with_future_floor(&[0x00])).await;
		let check = ds.rollback_check(Release::CURRENT).await.unwrap();
		assert_eq!(check.unreadable, 1, "{check:?}");
		assert!(check.entries.is_empty(), "{check:?}");
	}

	/// A namespace definition whose floor is above this build is one finding, and its database
	/// and table still walk: the definition is refused, not damage.
	#[tokio::test]
	async fn a_namespace_with_a_future_floor_is_one_finding() {
		let (ds, _, _) = with_person().await;
		let key = NamespaceKey::new(Cow::Borrowed("test")).encode_key().unwrap();
		let txn = ds.transaction(TransactionType::Read).await.unwrap();
		let written = txn.get(key.clone(), None).await.unwrap().expect("written");
		txn.cancel().await.unwrap();
		let (_, body) = surrealdb_catalog::stored::parse_header("any", &written).unwrap();
		put(&ds, key, &with_future_floor(body)).await;
		let check = ds.rollback_check(Release::CURRENT).await.unwrap();
		assert_eq!(check.entries.len(), 1, "{check:?}");
		assert_eq!(check.unreadable, 0, "{check:?}");
	}

	/// A descriptor is read only under the table it names: one copied under another table's
	/// generation is unreadable there, as the layout loader refuses it.
	#[tokio::test]
	async fn a_descriptor_naming_another_table_is_unreadable() {
		let (ds, ns, db) = with_person().await;
		let session = Session::owner().with_ns("test").with_db("test");
		for response in ds.execute("DEFINE TABLE other", &session, None).await.unwrap() {
			response.result.unwrap();
		}
		let person =
			LayoutKey::new(ns, db, Cow::Owned(TableName::new("person")), 1).encode_key().unwrap();
		let other =
			LayoutKey::new(ns, db, Cow::Owned(TableName::new("other")), 1).encode_key().unwrap();
		let txn = ds.transaction(TransactionType::Read).await.unwrap();
		let descriptor = txn.get(person, None).await.unwrap().expect("person has generation 1");
		assert!(txn.get(other.clone(), None).await.unwrap().is_some(), "other has generation 1");
		txn.cancel().await.unwrap();
		let check = ds.rollback_check(Release::CURRENT).await.unwrap();
		assert_eq!(check.unreadable, 0, "{check:?}");
		put(&ds, other, &descriptor).await;
		let check = ds.rollback_check(Release::CURRENT).await.unwrap();
		assert_eq!(check.unreadable, 1, "{check:?}");
	}

	/// A ring whose head has no descriptor is unreadable: every write to the table stamps the head
	/// and fails without its descriptor.
	#[tokio::test]
	async fn a_ring_whose_head_has_no_descriptor_is_unreadable() {
		let (ds, ns, db) = with_person().await;
		let head =
			LayoutKey::new(ns, db, Cow::Owned(TableName::new("person")), 1).encode_key().unwrap();
		let txn = ds.transaction(TransactionType::Write).await.unwrap();
		assert!(txn.get(head.clone(), None).await.unwrap().is_some(), "person has generation 1");
		txn.del(head).await.unwrap();
		txn.commit().await.unwrap();
		let check = ds.rollback_check(Release::CURRENT).await.unwrap();
		assert_eq!(check.unreadable, 1, "{check:?}");
	}

	#[tokio::test]
	async fn an_entry_under_a_table_with_no_definition_is_judged() {
		let (ds, ns, db) = with_person().await;
		let field = FieldKey::new(
			ns,
			db,
			Cow::Owned(TableName::new("ghost")),
			Cow::Owned("name".to_owned()),
		);
		put(&ds, field.encode_key().unwrap(), &[0xee]).await;
		let check = ds.rollback_check(Release::CURRENT).await.unwrap();
		assert_eq!(check.unreadable, 1, "{check:?}");
	}

	#[tokio::test]
	async fn a_descriptor_under_an_unreadable_ring_is_unreadable() {
		let (ds, ns, db) = with_person().await;
		let ring = TableMigrationsKey::new(ns, db, Cow::Owned(TableName::new("ghost")));
		put(&ds, ring.encode_key().unwrap(), &[0xee]).await;
		let layout = LayoutKey::new(ns, db, Cow::Owned(TableName::new("ghost")), 1);
		put(&ds, layout.encode_key().unwrap(), &[0xee]).await;
		let check = ds.rollback_check(Release::CURRENT).await.unwrap();
		assert_eq!(check.unreadable, 2, "{check:?}");
	}

	#[tokio::test]
	async fn a_descriptor_of_a_table_with_no_ring_is_unreadable() {
		let (ds, ns, db) = with_person().await;
		let layout = LayoutKey::new(ns, db, Cow::Owned(TableName::new("ghost")), 1);
		put(&ds, layout.encode_key().unwrap(), &[0xee]).await;
		let check = ds.rollback_check(Release::CURRENT).await.unwrap();
		assert_eq!(check.unreadable, 1, "{check:?}");
	}

	/// A record its validator refuses is unreadable: a generation mark whose head is 0 decodes but
	/// fails validation.
	#[tokio::test]
	async fn a_record_its_validator_refuses_is_unreadable() {
		let (ds, ns, db) = with_person().await;
		let key = GenerationMarkKey {
			ns,
			db,
			tb: Cow::Owned(TableName::new("person")),
		}
		.encode_key()
		.unwrap();
		// `GenerationMark { head: 7 }` with its generation's value replaced by 0.
		let invalid: Vec<u8> = vec![254, 0, 128, 128, 16, 38, 1, 0, 2, 128];
		put(&ds, key, &invalid).await;
		let check = ds.rollback_check(Release::CURRENT).await.unwrap();
		assert_eq!(check.unreadable, 1, "{check:?}");
	}

	/// A key of no declared family under the level `level` (its bytes followed by `!zz`).
	fn unknown_under(level: &Key<'static>) -> Key<'static> {
		let mut bytes = level.as_slice().to_vec();
		bytes.extend_from_slice(b"!zz");
		Key::from(bytes)
	}

	/// A view's group state is judged under its view table: one the target reads is no finding,
	/// and one whose floor is above the target is a finding that blocks writes, since every write
	/// that maintains the view reads it.
	#[tokio::test]
	async fn a_view_state_with_a_future_floor_is_one_finding() {
		let (ds, ns, db) = with_person().await;
		let session = Session::owner().with_ns("test").with_db("test");
		for response in ds
			.execute(
				"DEFINE TABLE by_name AS SELECT name, count() AS n FROM person GROUP BY name; \
				 CREATE person:2 SET name = 'a';",
				&session,
				None,
			)
			.await
			.unwrap()
		{
			response.result.unwrap();
		}
		let state = ViewStateKey {
			ns,
			db,
			tb: Cow::Owned(TableName::new("by_name")),
			id: Cow::Owned(RecordIdKey::Array(Array(vec![Value::from("a")]))),
		}
		.encode_key()
		.unwrap();
		let txn = ds.transaction(TransactionType::Read).await.unwrap();
		let written = txn.get(state.clone(), None).await.unwrap().expect("the group has a state");
		txn.cancel().await.unwrap();
		let check = ds.rollback_check(Release::CURRENT).await.unwrap();
		assert!(check.entries.is_empty(), "{check:?}");
		assert_eq!(check.unreadable, 0, "{check:?}");
		assert_eq!(check.unknown, 0, "{check:?}");

		let (_, body) = surrealdb_catalog::stored::parse_header("any", &written).unwrap();
		put(&ds, state, &with_future_floor(body)).await;
		let check = ds.rollback_check(Release::CURRENT).await.unwrap();
		let [finding] = check.entries.as_slice() else {
			panic!("the state is one finding: {check:?}");
		};
		assert_eq!(finding.kind, <ViewState as StoredType>::SCHEMA.name);
		assert_eq!(finding.table, Some(TableName::new("by_name")));
		assert!(finding.blocks_writes, "{finding:?}");
		assert_eq!(check.unreadable, 0, "{check:?}");
	}

	#[tokio::test]
	async fn an_unknown_key_under_a_catalog_band_is_counted() {
		let (ds, ns, db) = with_person().await;
		put(&ds, Key::from(b"/!zz".to_vec()), b"x").await;
		let check = ds.rollback_check(Release::CURRENT).await.unwrap();
		assert_eq!(check.unknown, 1, "{check:?}");
		put(
			&ds,
			unknown_under(
				&KVSubspace::encode_bound(&NsRoot {
					ns,
				})
				.unwrap(),
			),
			b"x",
		)
		.await;
		put(
			&ds,
			unknown_under(
				&KVSubspace::encode_bound(&DbRoot {
					ns,
					db,
				})
				.unwrap(),
			),
			b"x",
		)
		.await;
		let check = ds.rollback_check(Release::CURRENT).await.unwrap();
		assert_eq!(check.unknown, 3, "{check:?}");
	}
}
