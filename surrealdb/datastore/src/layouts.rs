//! The generation check that a reader of stored records runs before it reads any field of a row.
//!
//! A table's rows are stamped with a generation, and every generation above 0 has a descriptor
//! under the table's `!lg` keys ([`surrealdb_catalog::TableLayouts`]). A reader that scans or
//! fetches rows of one table holds that table's layouts for the whole read and checks each row's
//! envelope and generation against them before the row is filtered, projected or decoded:
//!
//! - [`RowGate`] is the check of one reader of one table, run in a synchronous visitor over raw
//!   bytes.
//! - [`LayoutSet`] is the same check for a read over records of several tables, where each table's
//!   layouts are loaded on the first row that needs them.
//!
//! A row of a generation that is unlisted, or outside the table's ring, costs one reload of the
//! table's layouts at the reader's version, because the layouts a reader holds may predate the
//! transaction that minted the generation. After that reload such a generation is a refusal. A
//! reader reloads at most once per layout epoch: it reads one snapshot of the store, so a second
//! reload could not list more.
//!
//! A generation number names one descriptor at a time, but within one transaction a save point
//! rollback can undo a mint, and the transaction can then mint the same number with other bytes. A
//! current gate ([`RowGate`] with no version) therefore holds the transaction's layout epoch
//! ([`Transaction::layout_epoch`]) as it read it before it loaded its layouts, and
//! [`RowGate::refresh`] takes the transaction's layouts again when the epoch has moved. A reader
//! over batches refreshes once per batch, before the batch is admitted; [`LayoutSet`] refreshes per
//! row. A gate at a version never refreshes: a read at a version does not see the transaction's own
//! mints or rollbacks.
//!
//! A packed row is admitted like a document: its envelope parses and its generation is listed. Its
//! body is read through its generation's descriptor by the decode, with the layouts the gate holds
//! ([`RowGate::layouts`], [`LayoutSet::layouts_of`]).
//!
//! Rows written in the `revision` encoding, and rows of generation 0, are admitted by one byte
//! compare and one varint read, with no allocation.

use std::sync::Arc;

use anyhow::Result;
use surrealdb_catalog::{
	DatabaseId, Error as CatalogError, Generation, NamespaceId, RecordLayoutError, StoredRecord,
	TableLayouts,
};
use surrealdb_expr::val::TableName;

use crate::tx::Transaction;

/// What a reader does with one stored row.
pub enum Admission {
	/// The row's generation is listed and its descriptor is readable: read it.
	Admit,
	/// The row names a generation the reader's layouts do not list or hold in their ring, and
	/// the reader has not yet reloaded them: call [`RowGate::reload`], then admit the row again.
	Reload,
	/// The row is refused: its envelope is malformed or not readable, its generation's
	/// descriptor is refused, or its generation is unlisted or outside the ring after the
	/// reload.
	Refuse(anyhow::Error),
}

/// The per-row generation check of one reader of one table.
///
/// Built once per reader, from layouts that are checked once to belong to the table read
/// ([`RowGate::new`]). Each row is then checked by [`RowGate::admit`] before any field of it is
/// read, filtered or decoded. A current gate re-resolves its layouts when the transaction's layout
/// epoch moves ([`RowGate::refresh`]).
#[derive(Debug)]
pub struct RowGate {
	layouts: Arc<TableLayouts>,
	version: Option<u64>,
	reloaded: bool,
	/// The transaction's layout epoch, read before `layouts` were loaded. A current gate whose
	/// epoch has moved takes the transaction's layouts again before it admits another row
	/// ([`RowGate::refresh`]).
	epoch: u64,
}

impl RowGate {
	/// A gate over `layouts` for a read of `table` in database `db` of namespace `ns`, at
	/// `version` (`None` for the transaction's current state).
	///
	/// `epoch` is [`Transaction::layout_epoch`] read before `layouts` were loaded; a gate at a
	/// version ignores it.
	///
	/// Layouts of another table are refused with [`RecordLayoutError::ForeignLayouts`] before
	/// any row is read.
	pub fn new(
		layouts: Arc<TableLayouts>,
		ns: NamespaceId,
		db: DatabaseId,
		table: &TableName,
		version: Option<u64>,
		epoch: u64,
	) -> Result<Self> {
		if !layouts.belongs_to(ns, db, table) {
			return Err(CatalogError::Layout(RecordLayoutError::ForeignLayouts {
				layouts: layouts.table().clone(),
				table: table.clone(),
			})
			.into());
		}
		Ok(Self {
			layouts,
			version,
			reloaded: false,
			epoch,
		})
	}

	/// Checks the stored bytes of one row.
	///
	/// - A `revision` row is admitted.
	/// - A v1 row is parsed, and its generation is looked up in the layouts: a listed readable
	///   generation is admitted. A packed row is admitted like a document: its body is read through
	///   its generation's descriptor by the decode.
	/// - A generation that is unlisted or outside the table's ring answers [`Admission::Reload`]
	///   until the gate has reloaded, and a refusal after.
	/// - An envelope error (a packed row at generation 0 included) and a refused generation answer
	///   [`Admission::Refuse`].
	///
	/// A row is checked before its body is read.
	#[inline]
	pub fn admit(&self, value: &[u8]) -> Admission {
		if value.first() != Some(&StoredRecord::MARKER) {
			return Admission::Admit;
		}
		let generation = match StoredRecord::parse(value) {
			Ok(Some(stored)) => stored.generation(),
			Ok(None) => return Admission::Admit,
			Err(e) => return Admission::Refuse(e.into()),
		};
		let generation = match generation {
			Ok(None) => return Admission::Admit,
			Ok(Some(generation)) => generation,
			Err(stamp) => {
				return Admission::Refuse(
					CatalogError::Layout(self.layouts.stamp_refusal(stamp)).into(),
				);
			}
		};
		match self.layouts.admit(generation) {
			Ok(_) => Admission::Admit,
			Err(
				RecordLayoutError::UnknownGeneration {
					..
				}
				| RecordLayoutError::OutsideWindow {
					..
				},
			) if !self.reloaded => Admission::Reload,
			Err(e) => Admission::Refuse(CatalogError::Layout(e).into()),
		}
	}

	/// Reads the table's layouts again at the reader's version, so that a row naming a
	/// generation minted after the first read can be admitted.
	///
	/// The transaction's layout epoch is read before the load and recorded with it. After this call
	/// [`RowGate::admit`] never answers [`Admission::Reload`], until a [`refresh`](Self::refresh)
	/// finds the epoch moved.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn reload(&mut self, txn: &Transaction) -> Result<()> {
		let epoch = txn.layout_epoch();
		let layouts = txn
			.reload_table_layouts(
				self.layouts.namespace(),
				self.layouts.database(),
				self.layouts.table(),
				self.version,
			)
			.await?;
		self.layouts = layouts;
		self.epoch = epoch;
		self.reloaded = true;
		Ok(())
	}

	/// Brings a current gate's layouts up to the transaction's view when the transaction's layout
	/// epoch has moved since they were loaded: a mint, a floor move, or a save point rollback that
	/// undid one may have changed what a generation number names. It costs one atomic load when
	/// nothing moved. A gate at a version never refreshes.
	///
	/// A refresh takes the transaction's own view of the table's layouts (its memo, or one load
	/// when the memo was cleared) and restores the gate's one reload. The epoch is read before the
	/// layouts are taken, so a move during the load is seen by the next refresh. Call it before
	/// admitting a batch of rows; no statement may run between the call and the batch's
	/// admissions.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn refresh(&mut self, txn: &Transaction) -> Result<()> {
		if self.version.is_some() {
			return Ok(());
		}
		let now = txn.layout_epoch();
		if now == self.epoch {
			return Ok(());
		}
		let layouts = txn
			.table_layouts(
				self.layouts.namespace(),
				self.layouts.database(),
				self.layouts.table(),
				None,
			)
			.await?;
		self.layouts = layouts;
		self.epoch = now;
		self.reloaded = false;
		Ok(())
	}

	/// The layouts the gate checks against.
	pub fn layouts(&self) -> &Arc<TableLayouts> {
		&self.layouts
	}
}

/// The gates of one read over records of any tables of one database.
///
/// A gate is made on the first row of its table that names a nonzero generation, so a read of
/// `revision` rows and generation-0 rows loads no layouts and allocates nothing.
#[derive(Debug)]
pub struct LayoutSet {
	ns: NamespaceId,
	db: DatabaseId,
	version: Option<u64>,
	gates: Vec<(TableName, RowGate)>,
}

impl LayoutSet {
	/// A set for a read of database `db` of namespace `ns` at `version`.
	pub fn new(ns: NamespaceId, db: DatabaseId, version: Option<u64>) -> Self {
		Self {
			ns,
			db,
			version,
			gates: Vec::new(),
		}
	}

	/// Admits `value`, the stored bytes of a record of `table`, loading and reloading the
	/// table's layouts as needed, and answers the row's generation: `None` for a `revision` row and
	/// for stamp 0, and the generation otherwise.
	///
	/// Returns the refusal of [`RowGate::admit`], with a generation that is unlisted or outside the
	/// table's ring refused after the one reload the table's gate allows. A stamp that names no
	/// generation is refused with the table's stamp refusal ([`TableLayouts::stamp_refusal`]).
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn admit(
		&mut self,
		txn: &Transaction,
		table: &TableName,
		value: &[u8],
	) -> Result<Option<Generation>> {
		self.admit_with_layouts(txn, table, value).await.map(|(generation, _)| generation)
	}

	/// Admits `value` as [`LayoutSet::admit`] does, and answers the layouts that admitted it with
	/// the row's generation: `Some` for a nonzero generation (as reloaded, when the row made the
	/// gate reload), `None` for a `revision` row and for stamp 0. A packed row is decoded with
	/// them ([`surrealdb_catalog::Record::decode_admitted`]), so one search serves the admission
	/// and the decode.
	///
	/// The table's gate refreshes before it admits ([`RowGate::refresh`]), one atomic load per row
	/// when nothing moved, so a set held across statements, or across a save point rollback that
	/// undid a mint, never admits a row against layouts the transaction no longer holds.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn admit_with_layouts(
		&mut self,
		txn: &Transaction,
		table: &TableName,
		value: &[u8],
	) -> Result<(Option<Generation>, Option<&Arc<TableLayouts>>)> {
		if value.first() != Some(&StoredRecord::MARKER) {
			return Ok((None, None));
		}
		let stored = match StoredRecord::parse(value)? {
			None => return Ok((None, None)),
			Some(stored) => stored,
		};
		let generation = match stored.generation() {
			Ok(None) => return Ok((None, None)),
			Ok(Some(generation)) => generation,
			Err(stamp) => {
				let layouts = txn.table_layouts(self.ns, self.db, table, self.version).await?;
				return Err(CatalogError::Layout(layouts.stamp_refusal(stamp)).into());
			}
		};
		let at = match self.gates.iter().position(|(held, _)| held == table) {
			Some(at) => at,
			None => {
				let epoch = txn.layout_epoch();
				let layouts = txn.table_layouts(self.ns, self.db, table, self.version).await?;
				let gate = RowGate::new(layouts, self.ns, self.db, table, self.version, epoch)?;
				self.gates.push((table.clone(), gate));
				self.gates.len() - 1
			}
		};
		let Some((_, gate)) = self.gates.get_mut(at) else {
			anyhow::bail!("a layout gate was lost between its creation and its use");
		};
		gate.refresh(txn).await?;
		match gate.admit(value) {
			Admission::Admit => {}
			Admission::Refuse(e) => return Err(e),
			Admission::Reload => {
				gate.reload(txn).await?;
				match gate.admit(value) {
					Admission::Admit => {}
					Admission::Refuse(e) => return Err(e),
					Admission::Reload => {
						return Err(CatalogError::Layout(RecordLayoutError::UnknownGeneration {
							table: table.clone(),
							generation,
						})
						.into());
					}
				}
			}
		}
		let layouts = self.gates.get(at).map(|(_, gate)| gate.layouts());
		Ok((Some(generation), layouts))
	}

	/// The layouts this set holds for `table`: those of the gate a row of `table` naming a nonzero
	/// generation made, as last reloaded or refreshed.
	pub fn layouts_of(&self, table: &TableName) -> Option<&Arc<TableLayouts>> {
		self.gates.iter().find(|(held, _)| held == table).map(|(_, gate)| gate.layouts())
	}
}
