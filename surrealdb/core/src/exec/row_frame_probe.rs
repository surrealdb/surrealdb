//! A measurement entry point for the output-frame transcoder, for the
//! allocation harness (`tests/vm_alloc.rs`), which links the crate from
//! outside and so reaches only public items. Not an API.

use std::borrow::Cow;

use crate::catalog::TableLayouts;
use crate::catalog::providers::DatabaseProvider as _;
use crate::key::schema::RecordPrefix;
use crate::kvs::{Admission, Datastore, RowGate, TransactionType};

/// The rows a frame holds before it is handed on and cleared, as the
/// executor's output batches are flushed.
const FLUSH_AT: usize = 1000;

/// What [`scan_table`] does with each record it reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Probe {
	/// Admit it and nothing more: the cost of reading the table.
	Scan,
	/// Decode it whole and encode the value into the frame.
	Decoded,
	/// Transcode it into the frame from its stored bytes.
	Transcoded,
	/// Write it into a shaped frame from its stored bytes: a packed row by its generation's copy
	/// plan.
	#[cfg(feature = "shaped")]
	Shaped,
}

/// Read every record of table `tb` in `ns`/`db` and transcode each into one
/// reused output frame, flushed every [`FLUSH_AT`] rows: what a `SELECT *`
/// over the table writes for the wire with frames as its output. Answers
/// the rows and the bytes written.
pub async fn transcode_table(
	ds: &Datastore,
	ns: &str,
	db: &str,
	tb: &str,
) -> anyhow::Result<(usize, usize)> {
	scan_table(ds, ns, db, tb, Probe::Transcoded).await
}

/// Read every record of table `tb` in `ns`/`db` as [`transcode_table`] does, doing what `probe`
/// says with each. Answers the rows and the bytes written.
pub async fn scan_table(
	ds: &Datastore,
	ns: &str,
	db: &str,
	tb: &str,
	probe: Probe,
) -> anyhow::Result<(usize, usize)> {
	let txn = ds.transaction(TransactionType::Read).await?;
	let def = txn
		.get_db_by_name(ns, db, None)
		.await?
		.ok_or_else(|| anyhow::anyhow!("no database {ns}/{db}"))?;
	let table = crate::val::TableName::from(tb);
	let range = RecordPrefix {
		ns: def.namespace_id,
		db: def.database_id,
		tb: Cow::Borrowed(&table),
	}
	.range()?;
	let epoch = txn.layout_epoch();
	let layouts = txn.table_layouts(def.namespace_id, def.database_id, &table, None).await?;
	let mut gate = RowGate::new(layouts, def.namespace_id, def.database_id, &table, None, epoch)?;
	let mut cursor =
		txn.open_vals_cursor_raw(range, crate::kvs::Direction::Forward, 0, None).await?;
	let mut frame = match probe {
		#[cfg(feature = "shaped")]
		Probe::Shaped => {
			let mut frame =
				super::vm::row_frame::RowFrame::with_format(surrealdb_rpc::RowFormat::Shaped);
			// The seed shape a statement over the table is compiled with, for an owner.
			use crate::catalog::providers::TableProvider as _;
			let fields = txn.all_tb_fields(def.namespace_id, def.database_id, &table, None).await?;
			frame.seed(tb, super::shaped::seed_shape(&fields, tb, false)?);
			frame
		}
		_ => super::vm::row_frame::RowFrame::new(),
	};
	let (mut rows, mut bytes) = (0usize, 0usize);
	let mut scanned = 0usize;
	loop {
		let mut failed = None;
		// The row that named a generation the gate does not list yet; the
		// cursor has consumed it and it is the last row the visit read.
		let mut held: Option<(Vec<u8>, Vec<u8>)> = None;
		gate.refresh(&txn).await?;
		let layouts: &TableLayouts = gate.layouts();
		let stats = cursor
			.for_each(crate::kvs::NORMAL_BATCH_SIZE, &mut |key, value| {
				match gate.admit(value) {
					Admission::Admit => {}
					Admission::Reload => {
						held = Some((key.to_vec(), value.to_vec()));
						return Ok(std::ops::ControlFlow::Break(()));
					}
					Admission::Refuse(e) => {
						failed = Some(e);
						return Ok(std::ops::ControlFlow::Break(()));
					}
				}
				let pushed = match probe {
					Probe::Scan => {
						scanned += 1;
						Ok(())
					}
					Probe::Decoded => crate::exec::operators::scan::pipeline::decode_record_value(
						key,
						value,
						Some(layouts),
					)
					.and_then(|row| frame.push_value(&row)),
					Probe::Transcoded => frame.push_stored(key, value, Some(layouts)),
					#[cfg(feature = "shaped")]
					Probe::Shaped => frame.push_stored(key, value, Some(layouts)),
				};
				if let Err(e) = pushed {
					failed = Some(e);
					return Ok(std::ops::ControlFlow::Break(()));
				}
				if frame.len() >= FLUSH_AT {
					rows += frame.len();
					bytes += frame.data().len();
					frame.clear();
				}
				Ok(std::ops::ControlFlow::Continue(()))
			})
			.await?;
		if let Some(e) = failed {
			return Err(e);
		}
		if let Some((key, value)) = held {
			gate.reload(&txn).await?;
			match gate.admit(&value) {
				Admission::Admit => {}
				Admission::Refuse(e) => return Err(e),
				Admission::Reload => anyhow::bail!("a generation was unlisted after the reload"),
			}
			frame.push_stored(&key, &value, Some(&**gate.layouts()))?;
		}
		if stats.rows == 0 {
			break;
		}
	}
	rows += frame.len() + scanned;
	bytes += frame.data().len();
	txn.cancel().await?;
	Ok((rows, bytes))
}
