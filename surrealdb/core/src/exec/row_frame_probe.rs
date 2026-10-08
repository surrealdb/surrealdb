//! A measurement entry point for the output-frame transcoder, for the
//! allocation harness (`tests/vm_alloc.rs`), which links the crate from
//! outside and so reaches only public items. Not an API.

use std::borrow::Cow;

use crate::catalog::providers::DatabaseProvider as _;
use crate::key::schema::RecordPrefix;
use crate::kvs::{Datastore, TransactionType};

/// The rows a frame holds before it is handed on and cleared, as the
/// executor's output batches are flushed.
const FLUSH_AT: usize = 1000;

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
	let mut cursor =
		txn.open_vals_cursor_raw(range, crate::kvs::Direction::Forward, 0, None).await?;
	let mut frame = super::vm::row_frame::RowFrame::new();
	let (mut rows, mut bytes) = (0usize, 0usize);
	loop {
		let mut failed = None;
		let stats = cursor
			.for_each(crate::kvs::NORMAL_BATCH_SIZE, &mut |key, value| {
				if let Err(e) = frame.push_stored(key, value) {
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
		if stats.rows == 0 {
			break;
		}
	}
	rows += frame.len();
	bytes += frame.data().len();
	txn.cancel().await?;
	Ok((rows, bytes))
}
