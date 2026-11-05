//! The foreign views a write maintains.
//!
//! One stage with three arms, because a
//! `DEFINE TABLE … AS SELECT` produces three different obligations:
//!
//! - a view nothing maintains, which a write does nothing for;
//! - a **materialized** view, which holds one row per source record, under that record's own key;
//! - an **aggregated** view, which holds one row per group, whose stored stats a write adjusts by
//!   the delta the record made rather than by recomputing the group.
//!
//! # A view row is a record
//!
//! Both maintained arms end the same way: the row they wrote or deleted is
//! driven through [the view-maintenance
//! order](super::super::plan::WritePlan::view_maintenance), which is the view
//! table's own indexes, views, events, subscriptions and change feed. A view
//! table is a real table — a second-order view can read it, a subscription can
//! watch it, an index can cover it — and `DEFINE TABLE … AS SELECT`
//! runs that lifecycle over every row of its initial build.
//!
//! # The frame
//!
//! Every clause here runs with permission enforcement off and writes allowed,
//! under the *writer's* authority: a view definition records no `AUTH LIMIT`,
//! so there is nothing to narrow to. See
//! [`view_frame`](crate::exec::permission::view_frame).

use std::borrow::Cow;

use surrealdb_datastore::WriteBatch;
use surrealdb_kvs::Key;
use surrealdb_kvs::key::KVKey;
use surrealdb_kvs::value::KVValue;
use surrealdb_types::ToSql;

use crate::catalog::aggregation::{AggregationAnalysis, AggregationStat, Recalculation};
use crate::catalog::{
	DatabaseId, Metadata, NamespaceId, Record, RecordType, RecordWriter, StoredRecord, ViewState,
};
use crate::exec::Error as ExecError;
use crate::exec::operators::write::ctx::WriteCtx;
use crate::expr::field::Selector;
use crate::expr::logical::LogicalOp;
use crate::expr::statements::SelectStatement;
use crate::expr::{
	BinaryOperator, Cond, ControlFlow, Expr, Fields, Function, FunctionCall, Groups, Literal, Param,
};
use crate::key::schema::{RecordKey, ViewStateKey};
use crate::kvs::Transaction;
use crate::val::{Array, RecordId, RecordIdKey, TableName, Value};

#[cfg(test)]
thread_local! {
	/// How many times this thread ran the views stage's body over a record, for
	/// tests that must know a write maintained its views as instructions.
	pub(crate) static VIEW_BODIES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// What an aggregated view's stored definition says it is over: the pieces a
/// group's row and its recalculation read beside the view's clauses.
pub(crate) struct ViewSources<'a> {
	/// The stored analysis, which says how many stats a new group starts with
	/// and how the recalculation names them.
	pub(crate) analysis: &'a AggregationAnalysis,
	/// The tables the view selects from, which a recalculation re-reads.
	pub(crate) tables: &'a [TableName],
	/// The view's condition in its stored form, which a recalculation ANDs
	/// into its own.
	pub(crate) condition: Option<&'a Expr>,
}

// ===========================================================================
// One row per group
// ===========================================================================

/// The extremes `analysis`'s stats keep, each as the aggregate function that recomputes it and
/// the aggregate argument it reads, once each, in the order of the aggregations that first keep
/// it: the columns of a recalculation before its count.
pub(crate) fn extremes_of(analysis: &AggregationAnalysis) -> Vec<(String, usize)> {
	let mut extremes: Vec<(String, usize)> = Vec::new();
	for aggregation in &analysis.aggregations {
		let extreme = match aggregation.to_stat() {
			AggregationStat::NumberMax {
				arg,
				..
			} => ("math::max", arg),
			AggregationStat::NumberMin {
				arg,
				..
			} => ("math::min", arg),
			AggregationStat::TimeMax {
				arg,
				..
			}
			| AggregationStat::TemporalMax {
				arg,
				..
			} => ("time::max", arg),
			AggregationStat::TimeMin {
				arg,
				..
			}
			| AggregationStat::TemporalMin {
				arg,
				..
			} => ("time::min", arg),
			AggregationStat::Count {
				..
			}
			| AggregationStat::CountValue {
				..
			}
			| AggregationStat::Sum {
				..
			}
			| AggregationStat::Mean {
				..
			}
			| AggregationStat::StdDev {
				..
			}
			| AggregationStat::Variance {
				..
			}
			| AggregationStat::Accumulate {
				..
			} => continue,
		};
		let extreme = (extreme.0.to_owned(), extreme.1);
		if !extremes.contains(&extreme) {
			extremes.push(extreme);
		}
	}
	extremes
}

/// The names a recalculation reads the key values of its group from, one per group expression
/// of `analysis`, in order.
pub(crate) fn group_names(analysis: &AggregationAnalysis) -> Vec<String> {
	(0..analysis.group_expressions.len()).map(|index| format!("__view_group_{index}")).collect()
}

/// The statement [`recalculate_aggregates`] runs: `SELECT VALUE [extremes…,
/// count()]` over the view's sources, narrowed to the group whose key values
/// are the parameters `names` and by the view's own condition, `GROUP ALL`.
pub(crate) fn recalculation_of(
	target: &ViewSources<'_>,
	recalculations: &[Recalculation],
	names: &[String],
) -> Result<LogicalOp, ControlFlow> {
	let analysis = target.analysis;
	let mut exprs: Vec<Expr> = recalculations
		.iter()
		.map(|recalculation| {
			Expr::FunctionCall(Box::new(FunctionCall {
				receiver: Function::Normal(recalculation.function.clone()),
				arguments: vec![analysis.aggregate_arguments[recalculation.arg].clone()],
			}))
		})
		.collect();
	exprs.push(Expr::FunctionCall(Box::new(FunctionCall {
		receiver: Function::Normal("count".to_string()),
		arguments: Vec::new(),
	})));

	// The group-key equalities lead and the view's condition is ANDed last:
	// `&&` short-circuits left to right, so a row of another group is rejected
	// on a key comparison without the view's condition being evaluated for it.
	let mut cond: Option<Expr> = None;
	for (index, expr) in analysis.group_expressions.iter().enumerate() {
		let equality = Expr::Binary {
			left: Box::new(expr.clone()),
			op: BinaryOperator::Equal,
			right: Box::new(Expr::Param(Param::from(names[index].clone()))),
		};
		cond = Some(conjoin(cond, equality));
	}
	if let Some(view_condition) = target.condition.cloned() {
		cond = Some(conjoin(cond, view_condition));
	}

	let statement = SelectStatement {
		// SELECT VALUE [recalc1, .., count()]
		fields: Fields::Value(Box::new(Selector {
			expr: Expr::Literal(Literal::Array(exprs)),
			alias: None,
		})),
		// FROM source1, source2, .. — `ONLY` is rejected for several targets
		// without a limit, so the single `GROUP ALL` row is taken below.
		only: false,
		what: target.tables.iter().cloned().map(Expr::Table).collect(),
		cond: cond.map(Cond),
		group: Some(Groups(Vec::new())),
		omit: vec![],
		with: None,
		split: None,
		order: None,
		limit: None,
		start: None,
		fetch: None,
		version: Expr::Literal(Literal::None),
		timeout: Expr::Literal(Literal::None),
		explain: None,
		tempfiles: false,
		for_update: false,
	};
	crate::expr::logical::lower_select(statement)
		.map_err(|err| ControlFlow::Err(anyhow::Error::new(err)))
}

/// `left && right`, or `right` alone when there is no left.
fn conjoin(left: Option<Expr>, right: Expr) -> Expr {
	match left {
		Some(left) => Expr::Binary {
			left: Box::new(left),
			op: BinaryOperator::And,
			right: Box::new(right),
		},
		None => right,
	}
}

// ===========================================================================
// Shared
// ===========================================================================

/// The statistics a group no record was in yet starts from: the analysis's
/// own.
pub(crate) fn new_stats(analysis: &AggregationAnalysis) -> Vec<AggregationStat> {
	analysis.aggregations.iter().map(|aggregation| aggregation.to_stat()).collect()
}

/// A view row holding `data` and, where the store keeps a group's statistics
/// inline, `stats`.
pub(crate) fn row_of(keyed: bool, data: Value, stats: &[AggregationStat]) -> Record {
	Record {
		data,
		metadata: (!keyed).then(|| Metadata {
			record_type: RecordType::Table,
			aggregation_stats: stats.to_vec(),
		}),
	}
}

// ===========================================================================
// One record's reads and writes
// ===========================================================================

/// Where a key a [`ViewBatch`] reads sits in its answer.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Slot(usize);

/// The two keys of an aggregated view's group: its row, and its state.
#[derive(Clone, Copy, Debug)]
pub(crate) struct GroupSlots {
	row: Slot,
	state: Slot,
}

/// An aggregated view's group as storage holds it.
pub(crate) struct Group {
	/// The row as stored. Its metadata is there only for a row that keeps the
	/// group's statistics inline and is written back the same way.
	pub(crate) row: Record,
	/// The group's running statistics.
	pub(crate) stats: Vec<AggregationStat>,
}

/// The storage one record's view maintenance reads and writes.
///
/// Maintenance first names every key it will read, then reads them all with
/// one multi-get ([`Self::fetch`]), applies the record's change to each view
/// in memory, staging what it writes, and issues the staged writes as one
/// multi-delete and one multi-write ([`Self::flush`]): a record feeding `V`
/// views costs one round trip to read and one to write where reading and
/// writing each group on its own costs up to `2V` of each. Keys named after a
/// flush are read by the next fetch.
///
/// A key is written at most once per batch: a group is touched by one view's
/// one change (a record that moves between two groups whose keys encode alike
/// is a change within one group, [`Self::is_group`]), and the rows of different
/// views are under different tables. Staging a key takes it from the batch.
pub(crate) struct ViewBatch {
	ns: NamespaceId,
	db: DatabaseId,
	/// Whether a group new to the view keeps its statistics at its own key
	/// ([`Transaction::keyed_view_state`]), decided once for the batch. A group
	/// that already has a state keeps it whatever this says.
	keyed: bool,
	wanted: Vec<Key<'static>>,
	found: Vec<Option<Vec<u8>>>,
	staged: WriteBatch,
	/// The writer of each view table the batch wrote a row of.
	writers: Vec<(TableName, RecordWriter)>,
}

impl ViewBatch {
	pub(crate) fn new(ctx: &WriteCtx) -> Self {
		Self::of(ctx.ns.namespace_id, ctx.db.database_id, ctx.txn().keyed_view_state())
	}

	/// A batch over the database `db` of `ns`, whose new groups keep their
	/// statistics at their own key when `keyed`.
	pub(crate) fn of(ns: NamespaceId, db: DatabaseId, keyed: bool) -> Self {
		Self {
			ns,
			db,
			keyed,
			wanted: Vec::new(),
			found: Vec::new(),
			staged: WriteBatch::default(),
			writers: Vec::new(),
		}
	}

	/// Whether the group read at `slots` keeps its statistics at their own
	/// key: it holds a state, or it is new to the view and the store keeps
	/// states.
	pub(crate) fn keyed(&self, slots: GroupSlots) -> bool {
		self.keyed || self.bytes(slots.state).is_some()
	}

	fn want(&mut self, key: Key<'static>) -> Slot {
		let slot = Slot(self.wanted.len());
		self.wanted.push(key);
		slot
	}

	fn row_key(&self, table: &TableName, key: &RecordIdKey) -> Result<Key<'static>, ControlFlow> {
		RecordKey {
			ns: self.ns,
			db: self.db,
			tb: Cow::Borrowed(table),
			id: Cow::Borrowed(key),
		}
		.encode_key()
		.map_err(ControlFlow::Err)
	}

	fn state_key(&self, table: &TableName, key: &RecordIdKey) -> Result<Key<'static>, ControlFlow> {
		ViewStateKey {
			ns: self.ns,
			db: self.db,
			tb: Cow::Borrowed(table),
			id: Cow::Borrowed(key),
		}
		.encode_key()
		.map_err(ControlFlow::Err)
	}

	/// Name the row of `table` under `key` as a key to read.
	pub(crate) fn want_row(
		&mut self,
		table: &TableName,
		key: &RecordIdKey,
	) -> Result<Slot, ControlFlow> {
		let key = self.row_key(table, key)?;
		Ok(self.want(key))
	}

	/// Name the row of `table` under `key`, and its state, as keys to read.
	///
	/// The state is read whatever the store's feature level says, so that a
	/// node whose view of the level lags behind the node that wrote a group's
	/// state still reads the group as stored.
	pub(crate) fn want_group(
		&mut self,
		table: &TableName,
		key: &RecordIdKey,
	) -> Result<GroupSlots, ControlFlow> {
		let row = self.want_row(table, key)?;
		let state = self.state_key(table, key)?;
		Ok(GroupSlots {
			row,
			state: self.want(state),
		})
	}

	/// Whether the group read at `slots` is the group of `table` under `key`:
	/// two group values that differ may still encode to one key.
	pub(crate) fn is_group(
		&self,
		slots: GroupSlots,
		table: &TableName,
		key: &RecordIdKey,
	) -> Result<bool, ControlFlow> {
		Ok(self.wanted[slots.row.0] == self.row_key(table, key)?)
	}

	/// Read every key named since the last fetch with one multi-get.
	pub(crate) async fn fetch(&mut self, ctx: &WriteCtx) -> Result<(), ControlFlow> {
		let unread = &self.wanted[self.found.len()..];
		if !unread.is_empty() {
			let found = ctx.txn().getm_encoded(unread, None).await.map_err(ControlFlow::Err)?;
			self.found.extend(found);
		}
		Ok(())
	}

	fn bytes(&self, slot: Slot) -> Option<&[u8]> {
		self.found.get(slot.0).and_then(|found| found.as_deref())
	}

	/// The key read at `slot`, taken from the batch to be written.
	fn take_key(&mut self, slot: Slot) -> Key<'static> {
		std::mem::replace(&mut self.wanted[slot.0], Key::from(Vec::new()))
	}

	/// The row under `key` of `table`, decoded as it was written.
	pub(crate) async fn read_row(
		&self,
		txn: &Transaction,
		slot: Slot,
		table: &TableName,
		key: &RecordIdKey,
	) -> Result<Option<Record>, ControlFlow> {
		match self.bytes(slot) {
			Some(bytes) => Ok(Some(
				txn.decode_record_as_written(
					self.ns,
					self.db,
					table,
					bytes,
					RecordId {
						table: table.clone(),
						key: key.clone(),
					},
					None,
				)
				.await
				.map_err(ControlFlow::Err)?,
			)),
			None => Ok(None),
		}
	}

	/// The group under `key` of the aggregated view `table`, or `None` for a
	/// group nothing holds.
	///
	/// A group's state, where it has one, is authoritative, and the row is
	/// read without metadata. A group with no state keeps its statistics
	/// inline: its row must be in the `revision` encoding with them. When the
	/// store keeps states, the write that touches such a group stores its
	/// state and the row without them ([`Self::write_group`]). A row in the
	/// current record format with no state, and a state with no row, are
	/// refused as corrupt: neither is a group that starts over.
	pub(crate) async fn read_group(
		&self,
		txn: &Transaction,
		slots: GroupSlots,
		table: &TableName,
		key: &RecordIdKey,
	) -> Result<Option<Group>, ControlFlow> {
		let corrupt = |reason: &'static str| {
			ControlFlow::Err(anyhow::Error::new(ExecError::CorruptView {
				table: table.to_string(),
				reason,
			}))
		};
		let row = self.read_row(txn, slots.row, table, key).await?;
		match (row, self.bytes(slots.state)) {
			(None, None) => Ok(None),
			(None, Some(_)) => Err(corrupt("a group holds a state but no row")),
			(Some(mut row), Some(state)) => {
				let stats =
					ViewState::kv_decode_value(state, ()).map_err(ControlFlow::Err)?.into_stats();
				row.metadata = None;
				Ok(Some(Group {
					row,
					stats,
				}))
			}
			(Some(mut row), None) => {
				let inline = !self.is_envelope(slots.row);
				let stats = match row.metadata.as_ref() {
					Some(metadata) if inline && !metadata.aggregation_stats.is_empty() => {
						metadata.aggregation_stats.clone()
					}
					_ if self.keyed => return Err(corrupt("a group holds a row but no state")),
					_ => return Err(corrupt("a row of an aggregated view carries no stats")),
				};
				// A group moving its statistics to their own key holds a row
				// without them from this write on.
				if self.keyed {
					row.metadata = None;
				}
				Ok(Some(Group {
					row,
					stats,
				}))
			}
		}
	}

	/// Whether the bytes read for `slot` are a record in the current format,
	/// which holds no statistics.
	fn is_envelope(&self, slot: Slot) -> bool {
		self.bytes(slot).is_some_and(|bytes| matches!(StoredRecord::parse(bytes), Ok(Some(_))))
	}

	/// Stage `record` as the row of `table` under `key`, read at `slot`, as
	/// the view's maintenance stores the row it computed. `created` says the
	/// view held no row under `key`, so a write reading the view passes over
	/// it ([`Transaction::note_creation`]).
	pub(crate) async fn write_row(
		&mut self,
		txn: &Transaction,
		slot: Slot,
		table: &TableName,
		key: &RecordIdKey,
		record: &Record,
		created: bool,
	) -> Result<(), ControlFlow> {
		let at = match self.writers.iter().position(|(t, _)| t == table) {
			Some(at) => at,
			None => {
				// The view table is not the statement's table, so its writer is
				// resolved from the transaction, which answers from its memo.
				let writer =
					txn.record_writer(self.ns, self.db, table).await.map_err(ControlFlow::Err)?;
				self.writers.push((table.clone(), writer));
				self.writers.len() - 1
			}
		};
		// A view row's data is stored whole: no `id` is left out for its key to
		// restore, so the row names no key and is never packed. The record's
		// encoding refuses a value nested past the cap.
		let value = self.writers[at].1.encode_whole(record, None).map_err(ControlFlow::Err)?;
		let key_bytes = self.take_key(slot);
		self.staged.set_record(table.clone(), key.clone(), key_bytes, value, created);
		Ok(())
	}

	/// Stage the group of an aggregated view read at `slots`: its `row`
	/// (built by [`row_of`] for [`Self::keyed`]), and its statistics at their
	/// own key where the group keeps them there; otherwise the row carries
	/// them inline.
	#[expect(clippy::too_many_arguments, reason = "the group's place, its row and its stats")]
	pub(crate) async fn write_group(
		&mut self,
		txn: &Transaction,
		slots: GroupSlots,
		table: &TableName,
		key: &RecordIdKey,
		row: &Record,
		stats: &[AggregationStat],
		created: bool,
	) -> Result<(), ControlFlow> {
		let keyed = self.keyed(slots);
		self.write_row(txn, slots.row, table, key, row, created).await?;
		if keyed {
			let state = ViewState::of(stats).map_err(ControlFlow::Err)?;
			let value = txn
				.encode_for_write(&state, self.bytes(slots.state))
				.await
				.map_err(ControlFlow::Err)?;
			let key_bytes = self.take_key(slots.state);
			self.staged.set(key_bytes, value);
		}
		Ok(())
	}

	/// Stage the removal of the row of `table` under `key`, read at `slot`.
	pub(crate) fn delete_row(&mut self, slot: Slot, table: &TableName, key: &RecordIdKey) {
		let key_bytes = self.take_key(slot);
		self.staged.del_record(table.clone(), key.clone(), key_bytes);
	}

	/// Stage the removal of a group's row and of its state, where it has one.
	pub(crate) fn delete_group(&mut self, slots: GroupSlots, table: &TableName, key: &RecordIdKey) {
		let stated = self.bytes(slots.state).is_some();
		self.delete_row(slots.row, table, key);
		if stated {
			let key_bytes = self.take_key(slots.state);
			self.staged.del(key_bytes);
		}
	}

	/// Write everything staged since the last flush
	/// ([`Transaction::write_batch`]).
	pub(crate) async fn flush(&mut self, ctx: &WriteCtx) -> Result<(), ControlFlow> {
		self.flush_to(&ctx.txn()).await
	}

	/// [`Self::flush`] into `txn`.
	pub(crate) async fn flush_to(&mut self, txn: &Transaction) -> Result<(), ControlFlow> {
		let staged = std::mem::take(&mut self.staged);
		if staged.is_empty() {
			return Ok(());
		}
		txn.write_batch(self.ns, self.db, staged).await.map_err(ControlFlow::Err)
	}
}

/// The failure of a record leaving `group` of `table`, which holds no row for
/// the group.
pub(crate) fn left_no_row(group: &[Value], table: &TableName) -> ControlFlow {
	ControlFlow::Err(anyhow::anyhow!(ExecError::InvalidStatement(format!(
		"a record left the group {} of the view {}, which holds no row for it",
		Value::Array(Array(group.to_vec())).to_sql(),
		table
	))))
}

/// The failure of a record changing within `group` of `table`, which holds no
/// row for the group.
pub(crate) fn changed_no_row(group: &[Value], table: &TableName) -> ControlFlow {
	ControlFlow::Err(anyhow::anyhow!(ExecError::InvalidStatement(format!(
		"a record changed within the group {} of the view {}, which holds no row for it",
		Value::Array(Array(group.to_vec())).to_sql(),
		table
	))))
}

/// The failure of a row of `table` whose stats hold no record count.
pub(crate) fn no_record_count(table: &TableName) -> ControlFlow {
	ControlFlow::Err(anyhow::anyhow!(ExecError::InvalidStatement(format!(
		"the view {table} holds a row with no record count"
	))))
}

/// The group values a view row's key is made of.
///
/// An aggregated view keys each group row by the array of its group values, so
/// a key of any other shape belongs to no group and names none.
pub(crate) fn group_values(key: &RecordIdKey) -> Vec<Value> {
	if let RecordIdKey::Array(Array(values)) = key {
		values.clone()
	} else {
		Vec::new()
	}
}
