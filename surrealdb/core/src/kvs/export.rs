use std::fmt;

use anyhow::Result;
use async_channel::Sender;
use surrealdb_types::{SurrealValue, ToSql};

use super::Transaction;
use crate::catalog::providers::{
	AuthorisationProvider, DatabaseProvider, TableProvider, UserProvider,
};
use crate::catalog::{DatabaseId, NamespaceId, Record, TableDefinition};
use crate::cnf::EXPORT_BATCH_SIZE;
use crate::err::Error;
use crate::expr::paths::{IN, OUT};
use crate::expr::statements::define::{DefineAccessStatement, DefineUserStatement};
use crate::expr::{Base, DefineAnalyzerStatement};
use crate::key::record;
use crate::kvs::KVValue;
use crate::sql::statements::OptionStatement;

#[derive(Clone, Debug, SurrealValue)]
#[surreal(default)]
pub struct Config {
	pub users: bool,
	pub accesses: bool,
	pub params: bool,
	pub functions: bool,
	pub analyzers: bool,
	pub tables: TableConfig,
	pub versions: bool,
	pub records: bool,
	pub sequences: bool,
}

impl Default for Config {
	fn default() -> Config {
		Config {
			users: true,
			accesses: true,
			params: true,
			functions: true,
			analyzers: true,
			tables: TableConfig::default(),
			versions: false,
			records: true,
			sequences: true,
		}
	}
}

#[derive(Clone, Debug, Default, SurrealValue)]
#[surreal(untagged)]
pub enum TableConfig {
	#[default]
	#[surreal(value = true)]
	All,
	#[surreal(value = false)]
	None,
	Some(Vec<String>),
}

// TODO: This should probably be removed
// This is not a good from implementation,
// It is not direct: What true and false mean when converted to a table config?
impl From<bool> for TableConfig {
	fn from(value: bool) -> Self {
		match value {
			true => TableConfig::All,
			false => TableConfig::None,
		}
	}
}

impl From<Vec<String>> for TableConfig {
	fn from(value: Vec<String>) -> Self {
		TableConfig::Some(value)
	}
}

impl From<Vec<&str>> for TableConfig {
	fn from(value: Vec<&str>) -> Self {
		TableConfig::Some(value.into_iter().map(ToOwned::to_owned).collect())
	}
}

impl TableConfig {
	/// Check if we should export tables
	pub(crate) fn is_any(&self) -> bool {
		matches!(self, Self::All | Self::Some(_))
	}
	// Check if we should export a specific table
	pub(crate) fn includes(&self, table: &str) -> bool {
		match self {
			Self::All => true,
			Self::None => false,
			Self::Some(v) => v.iter().any(|v| v.eq(table)),
		}
	}
}

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
			// line seperator
			'\u{2028}' => self.0.write_str("\\u{2028}"),
			// Paragraph seperator
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

impl Transaction {
	/// Writes the full database contents as binary SQL.
	pub async fn export(
		&self,
		ns: &str,
		db: &str,
		cfg: Config,
		chn: Sender<Vec<u8>>,
	) -> Result<()> {
		let db = self.get_db_by_name(ns, db).await?.ok_or_else(|| {
			anyhow::Error::new(Error::DbNotFound {
				name: db.to_owned(),
			})
		})?;

		// Output USERS, ACCESSES, PARAMS, FUNCTIONS, ANALYZERS
		self.export_metadata(&cfg, &chn, db.namespace_id, db.database_id).await?;
		// Output TABLES
		self.export_tables(&cfg, &chn, db.namespace_id, db.database_id).await?;
		Ok(())
	}

	async fn export_metadata(
		&self,
		cfg: &Config,
		chn: &Sender<Vec<u8>>,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<()> {
		// Output OPTIONS
		self.export_section("OPTION", [OptionStatement::import()].into_iter(), chn).await?;

		// Output USERS
		if cfg.users {
			let users = self.all_db_users(ns, db).await?;
			self.export_section(
				"USERS",
				users.iter().map(|x| DefineUserStatement::from_definition(Base::Db, x)),
				chn,
			)
			.await?;
		}

		// Output ACCESSES
		if cfg.accesses {
			let accesses = self.all_db_accesses(ns, db).await?;
			self.export_section(
				"ACCESSES",
				accesses
					.iter()
					.map(|x| DefineAccessStatement::from_definition(Base::Db, x).redact()),
				chn,
			)
			.await?;
		}

		// Output PARAMS
		if cfg.params {
			let params = self.all_db_params(ns, db).await?;
			self.export_section("PARAMS", params.iter(), chn).await?;
		}

		// Output FUNCTIONS
		if cfg.functions {
			let functions = self.all_db_functions(ns, db).await?;
			self.export_section("FUNCTIONS", functions.iter(), chn).await?;
		}

		// Output ANALYZERS
		if cfg.analyzers {
			let analyzers = self.all_db_analyzers(ns, db).await?;
			self.export_section(
				"ANALYZERS",
				analyzers.iter().map(DefineAnalyzerStatement::from_definition),
				chn,
			)
			.await?;
		}

		// Output SEQUENCES
		if cfg.sequences {
			let sequences = self.all_db_sequences(ns, db).await?;
			self.export_section("SEQUENCES", sequences.iter(), chn).await?;
		}

		Ok(())
	}

	async fn export_section<T>(
		&self,
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
		&self,
		cfg: &Config,
		chn: &Sender<Vec<u8>>,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<()> {
		// Check if tables are included in the export config
		if !cfg.tables.is_any() {
			return Ok(());
		}
		// Fetch all of the tables for this NS / DB
		let tables = self.all_tb(ns, db, None).await?;
		// Loop over all of the tables in order
		for table in tables.iter() {
			// Check if this table is included in the export config
			if !cfg.tables.includes(&table.name) {
				continue;
			}
			// Export the table definition structure first
			self.export_table_structure(ns, db, table, chn).await?;
			// Then export the table data if its desired
			if cfg.records {
				self.export_table_data(ns, db, table, chn).await?;
			}
		}

		Ok(())
	}

	async fn export_table_structure(
		&self,
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
		// Export all table field definitions for this table
		let fields = self.all_tb_fields(ns, db, &table.name, None).await?;
		for field in fields.iter() {
			chn.send(bytes!(format!("{};", field.to_sql()))).await?;
		}
		chn.send(bytes!("")).await?;
		// Export all table index definitions for this table
		let indexes = self.all_tb_indexes(ns, db, &table.name).await?;
		for index in indexes.iter() {
			chn.send(bytes!(format!("{};", index.to_sql()))).await?;
		}
		chn.send(bytes!("")).await?;
		// Export all table event definitions for this table
		let events = self.all_tb_events(ns, db, &table.name).await?;
		for event in events.iter() {
			chn.send(bytes!(format!("{};", event.to_sql()))).await?;
		}
		chn.send(bytes!("")).await?;
		// Everything ok
		Ok(())
	}

	async fn export_table_data(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		table: &TableDefinition,
		chn: &Sender<Vec<u8>>,
	) -> Result<()> {
		chn.send(bytes!("-- ------------------------------")).await?;
		chn.send(bytes!(format!("-- TABLE DATA: {}", InlineCommentDisplay(&table.name)))).await?;
		chn.send(bytes!("-- ------------------------------")).await?;
		chn.send(bytes!("")).await?;

		let beg = crate::key::record::prefix(ns, db, &table.name)?;
		let end = crate::key::record::suffix(ns, db, &table.name)?;
		let mut next = Some(beg..end);

		while let Some(rng) = next {
			let batch = self.batch_keys_vals(rng, *EXPORT_BATCH_SIZE, None).await?;
			next = batch.next;
			// If there are no values, return early.
			if batch.result.is_empty() {
				break;
			}
			self.export_regular_data(batch.result, chn).await?;
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
	/// * `k` - The record key.
	/// * `record` - The record to be processed.
	/// * `records_relate` - A mutable reference to a string buffer for graph edge records.
	/// * `records_normal` - A mutable reference to a string buffer for normal records.
	fn process_record(
		k: record::RecordKey,
		mut record: Record,
		records_relate: &mut String,
		records_normal: &mut String,
	) {
		// Inject the id field into the document before processing.
		let rid = crate::val::RecordId {
			table: k.tb.into_owned(),
			key: k.id,
		};
		record.data.def(rid);
		// Match on the value to determine if it is a graph edge record or a normal record.
		if record.is_edge()
			&& let crate::val::Value::RecordId(_) = record.data.pick(&*IN)
			&& let crate::val::Value::RecordId(_) = record.data.pick(&*OUT)
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
	/// * `regular_values` - A vector of tuples containing the regular values to be exported. Each
	///   tuple consists of a key and a value.
	/// * `chn` - A reference to the channel to which the SQL commands will be sent.
	///
	/// # Returns
	///
	/// * `Result<()>` - Returns `Ok(())` if the operation is successful, or an `Error` if an error
	///   occurs.
	async fn export_regular_data(
		&self,
		regular_values: Vec<(Vec<u8>, Vec<u8>)>,
		chn: &Sender<Vec<u8>>,
	) -> Result<()> {
		// Initialize strings to hold normal records and graph edge records.
		// Write directly to strings to avoid unnecessary allocations.
		let mut records_normal = String::new();
		let mut records_relate = String::new();

		// Process each regular value.
		for (k, v) in regular_values {
			let k = record::RecordKey::decode_key(&k)?;
			let v = Record::kv_decode_value(v)?;
			// Process the value and categorize it into records_relate or records_normal.
			Self::process_record(k, v, &mut records_relate, &mut records_normal);
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
}
