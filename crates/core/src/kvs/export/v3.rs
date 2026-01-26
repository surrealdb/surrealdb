use core::fmt;

use async_channel::Sender;
use chrono::{TimeZone, Utc};
use fmt::Write;

use crate::{
	cnf::EXPORT_BATCH_SIZE,
	err::Error,
	key,
	kvs::{
		export::{Config, InlineCommentDisplay},
		version::v3::{MigrationIssue, MigratorPass, PassState},
		KeyDecode, Transaction,
	},
	sql::{escape::EscapeIdent, statements::DefineTableStatement, visit::Visit, Value},
};

const CHUNK_SIZE: usize = 1024 * 16;

struct Inner {
	buffer: Vec<u8>,
	channel: Sender<Vec<u8>>,
}

// Writer which buffers writen data into larger chunks.
// This avoids stressing the channel as well as the consumer of the receiver of the channel's data.
#[repr(transparent)]
struct ChannelWriter(Inner);

impl ChannelWriter {
	pub fn new(channel: Sender<Vec<u8>>) -> Self {
		ChannelWriter(Inner {
			buffer: Vec::with_capacity(CHUNK_SIZE),
			channel,
		})
	}

	fn is_closed(&self) -> bool {
		self.0.channel.is_closed()
	}

	async fn write_bytes(&mut self, mut bytes: &[u8]) {
		loop {
			if bytes.is_empty() {
				return;
			}

			let remaining_cap = CHUNK_SIZE - self.0.buffer.len();

			let Some((head, tail)) = bytes.split_at_checked(remaining_cap) else {
				self.0.buffer.extend_from_slice(bytes);
				return;
			};

			self.0.buffer.extend_from_slice(head);
			let buffer = std::mem::replace(&mut self.0.buffer, Vec::with_capacity(CHUNK_SIZE));
			if self.0.channel.send(buffer).await.is_err() {
				return;
			};
			bytes = tail;
		}
	}

	pub async fn flush(self) {
		// Safe because of `#[repr(transparent)]`;
		let this = unsafe { std::mem::transmute::<Self, Inner>(self) };
		if !this.buffer.is_empty() {
			let _ = this.channel.send(this.buffer).await;
		}
	}
}

pub async fn export_v3(
	tx: &Transaction,
	cfg: &Config,
	chn: Sender<Vec<u8>>,
	ns: &str,
	db: &str,
	issue_buffer: &mut Vec<MigrationIssue>,
) -> Result<(), Error> {
	let mut fmt_buffer = String::new();
	let mut writer = ChannelWriter::new(chn);
	writer.write_bytes(b"OPTION IMPORT;\n\n").await;
	let mut path = vec![Value::from("ns"), Value::from(ns), Value::from("db"), Value::from(db)];

	if cfg.users {
		let users = tx.all_db_users(ns, db).await?;
		export_section_header("USERS", &mut writer, &mut fmt_buffer).await;
		for i in users.as_ref() {
			write_visit(issue_buffer, &mut path, &mut fmt_buffer, &mut writer, i).await
		}

		writer.write_bytes(b"\n").await;
	}

	if cfg.accesses {
		let accesses = tx.all_db_accesses(ns, db).await?;
		export_section_header("ACCESSES", &mut writer, &mut fmt_buffer).await;
		for i in accesses.as_ref() {
			write_visit(issue_buffer, &mut path, &mut fmt_buffer, &mut writer, i).await
		}

		writer.write_bytes(b"\n").await;
	}

	if cfg.params {
		let params = tx.all_db_params(ns, db).await?;
		export_section_header("PARAMS", &mut writer, &mut fmt_buffer).await;
		for i in params.as_ref() {
			write_visit(issue_buffer, &mut path, &mut fmt_buffer, &mut writer, i).await
		}
	}

	if cfg.functions {
		let functions = tx.all_db_functions(ns, db).await?;
		export_section_header("FUNCTIONS", &mut writer, &mut fmt_buffer).await;
		for i in functions.as_ref() {
			write_visit(issue_buffer, &mut path, &mut fmt_buffer, &mut writer, i).await
		}
	}

	if cfg.analyzers {
		let analyzers = tx.all_db_analyzers(ns, db).await?;
		export_section_header("ANALYZERS", &mut writer, &mut fmt_buffer).await;
		for i in analyzers.as_ref() {
			write_visit(issue_buffer, &mut path, &mut fmt_buffer, &mut writer, i).await
		}
	}

	if !cfg.tables.is_any() {
		return Ok(());
	}

	let tables = tx.all_tb(ns, db, None).await?;

	for t in tables.iter() {
		if writer.is_closed() {
			break;
		}

		if !cfg.tables.includes(&t.name) {
			continue;
		}

		export_table_structure(
			tx,
			ns,
			db,
			&mut writer,
			&mut fmt_buffer,
			issue_buffer,
			&mut path,
			t,
		)
		.await?;

		if cfg.records {
			export_table_data(
				tx,
				cfg.versions,
				ns,
				db,
				&mut writer,
				&mut fmt_buffer,
				issue_buffer,
				&mut path,
				t,
			)
			.await?;
		}
	}

	writer.flush().await;

	Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn export_table_structure(
	tx: &Transaction,
	ns: &str,
	db: &str,
	writer: &mut ChannelWriter,
	fmt_buf: &mut String,
	issue_buffer: &mut Vec<MigrationIssue>,
	path: &mut Vec<Value>,
	t: &DefineTableStatement,
) -> Result<(), Error> {
	writer.write_bytes(b"-- ------------------------------\n").await;
	write_fmt(fmt_buf, writer, |s| writeln!(s, "-- TABLE: {}", InlineCommentDisplay(&t.name)))
		.await;
	writer.write_bytes(b"-- ------------------------------\n\n").await;

	write_visit(issue_buffer, path, fmt_buf, writer, t).await;
	writer.write_bytes(b";\n").await;

	{
		let fields = tx.all_tb_fields(ns, db, &t.name, None).await?;
		for f in fields.as_ref() {
			write_visit(issue_buffer, path, fmt_buf, writer, f).await;
			writer.write_bytes(b";\n").await;
		}
	}
	{
		let indexes = tx.all_tb_indexes(ns, db, &t.name).await?;
		for i in indexes.as_ref() {
			write_visit(issue_buffer, path, fmt_buf, writer, i).await;
			writer.write_bytes(b";\n").await;
		}
	}
	{
		let events = tx.all_tb_events(ns, db, &t.name).await?;
		for e in events.as_ref() {
			write_visit(issue_buffer, path, fmt_buf, writer, e).await;
			writer.write_bytes(b";\n").await;
		}
	}
	writer.write_bytes(b";\n").await;

	Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn export_table_data(
	tx: &Transaction,
	versions: bool,
	ns: &str,
	db: &str,
	writer: &mut ChannelWriter,
	fmt_buf: &mut String,
	issue_buffer: &mut Vec<MigrationIssue>,
	path: &mut Vec<Value>,
	t: &DefineTableStatement,
) -> Result<(), Error> {
	writer.write_bytes(b"-- ------------------------------\n").await;
	write_fmt(fmt_buf, writer, |s| writeln!(s, "-- TABLE DATA: {}", InlineCommentDisplay(&t.name)))
		.await;
	writer.write_bytes(b"-- ------------------------------\n\n").await;

	let beg = crate::key::thing::prefix(ns, db, &t.name)?;
	let end = crate::key::thing::suffix(ns, db, &t.name)?;
	let mut next = Some(beg..end);

	while let Some(rng) = next {
		if writer.is_closed() {
			break;
		}

		if versions {
			let batch = tx.batch_keys_vals_versions(rng, *EXPORT_BATCH_SIZE).await?;
			next = batch.next;
			// If there are no versioned values, return early.
			if batch.result.is_empty() {
				break;
			}
			export_versioned_data(&batch.result, writer, fmt_buf, issue_buffer, path).await?;
		} else {
			let batch = tx.batch_keys_vals(rng, *EXPORT_BATCH_SIZE, None).await?;
			next = batch.next;
			// If there are no values, return early.
			if batch.result.is_empty() {
				break;
			}
			export_data(&batch.result, writer, fmt_buf, issue_buffer, path).await?;
		}
		// Fetch more records
		continue;
	}

	writer.write_bytes(b"\n").await;

	Ok(())
}

async fn export_versioned_data(
	data: &[(Vec<u8>, Vec<u8>, u64, bool)],
	writer: &mut ChannelWriter,
	fmt_buf: &mut String,
	issue_buffer: &mut Vec<MigrationIssue>,
	path: &mut Vec<Value>,
) -> Result<(), Error> {
	let mut count = 0usize;

	for (k, v, version, tombstone) in data {
		if count % *EXPORT_BATCH_SIZE as usize == 0 {
			writer.write_bytes(b"BEGIN;\n").await;
		}

		let v = if v.is_empty() {
			Value::None
		} else {
			revision::from_slice(&v)?
		};

		let ts = Utc.timestamp_nanos(*version as i64);

		if *tombstone {
			let k = key::thing::Thing::decode(k)?;
			writer.write_bytes(b"DELETE ").await;
			write_fmt(fmt_buf, writer, |s| write!(s, "{}", EscapeIdent(&k.tb))).await;
			writer.write_bytes(b":").await;
			write_visit(issue_buffer, path, fmt_buf, writer, &k.id).await;
			writer.write_bytes(b";\n").await;
		} else if is_edge(&v) {
			writer.write_bytes(b"INSERT RELATION ").await;
			write_visit(issue_buffer, path, fmt_buf, writer, &v).await;
			writer.write_bytes(b" VERSION d").await;
			write_fmt(fmt_buf, writer, |s| write!(s, "{:?}", ts)).await;
			writer.write_bytes(b";\n").await;
		} else {
			writer.write_bytes(b"INSERT ").await;
			write_visit(issue_buffer, path, fmt_buf, writer, &v).await;
			writer.write_bytes(b" VERSION d").await;
			write_fmt(fmt_buf, writer, |s| write!(s, "{:?}", ts)).await;
			writer.write_bytes(b";\n").await;
		}

		count += 1;

		if count % *EXPORT_BATCH_SIZE as usize == 0 {
			writer.write_bytes(b"COMMIT;\n").await;
		}
	}

	if count % *EXPORT_BATCH_SIZE as usize != 0 {
		writer.write_bytes(b"COMMIT;\n").await;
	}

	Ok(())
}

async fn export_data(
	data: &[(Vec<u8>, Vec<u8>)],
	writer: &mut ChannelWriter,
	fmt_buf: &mut String,
	issue_buffer: &mut Vec<MigrationIssue>,
	path: &mut Vec<Value>,
) -> Result<(), Error> {
	let mut inserting_relation = None;

	for (_, v) in data {
		let v = if v.is_empty() {
			Value::None
		} else {
			revision::from_slice(&v)?
		};

		if is_edge(&v) {
			match inserting_relation {
				Some(false) => {
					writer.write_bytes(b"];\nINSERT RELATION [").await;
					inserting_relation = Some(true);
				}
				Some(true) => {}
				None => {
					writer.write_bytes(b"\nINSERT RELATION [").await;
					inserting_relation = Some(true);
				}
			}
			write_visit(issue_buffer, path, fmt_buf, writer, &v).await;
		} else {
			match inserting_relation {
				Some(true) => {
					writer.write_bytes(b"];\nINSERT [").await;
					inserting_relation = Some(true);
				}
				Some(false) => {}
				None => {
					writer.write_bytes(b"\nINSERT [").await;
					inserting_relation = Some(true);
				}
			}
			write_visit(issue_buffer, path, fmt_buf, writer, &v).await;
		}
	}

	if inserting_relation.is_some() {
		writer.write_bytes(b"];\n").await;
	}
	writer.write_bytes(b"\n").await;

	Ok(())
}

fn is_edge(v: &Value) -> bool {
	let Value::Object(o) = v else {
		return false;
	};

	let Some(Value::Bool(true)) = o.get("__") else {
		return false;
	};

	let Some(Value::Thing(_)) = o.get("in") else {
		return false;
	};

	let Some(Value::Thing(_)) = o.get("out") else {
		return false;
	};
	true
}

async fn write_fmt<F: FnOnce(&mut String) -> fmt::Result>(
	buffer: &mut String,
	writer: &mut ChannelWriter,
	cb: F,
) {
	buffer.clear();
	let _ = cb(buffer);
	writer.write_bytes(buffer.as_bytes()).await
}

async fn write_visit<T: for<'a> Visit<MigratorPass<'a>>>(
	issue_buffer: &mut Vec<MigrationIssue>,
	path: &mut Vec<Value>,
	buf: &mut String,
	writer: &mut ChannelWriter,
	t: &T,
) {
	buf.clear();
	{
		let mut pass = MigratorPass::new(issue_buffer, buf, path, PassState::default());
		let _ = t.visit_self(&mut pass);
	}
	writer.write_bytes(&buf.as_bytes()).await;
}

async fn export_section_header(title: &str, writer: &mut ChannelWriter, fmt_buffer: &mut String) {
	writer.write_bytes(b"-- ------------------------------\n").await;
	write_fmt(fmt_buffer, writer, |s| writeln!(s, "-- {}", InlineCommentDisplay(title))).await;
	writer.write_bytes(b"-- ------------------------------\n").await;
	writer.write_bytes(b"\n").await;
}
