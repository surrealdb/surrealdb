use std::fmt;

use anyhow::Result;
use async_channel::Sender;
use chrono::TimeZone;
use chrono::prelude::Utc;

use super::Transaction;
use crate::catalog::{DatabaseId, NamespaceId, TableDefinition};
use crate::cnf::EXPORT_BATCH_SIZE;
use crate::err::Error;
use crate::expr::paths::{IN, OUT};
use crate::expr::statements::define::{DefineAccessStatement, DefineUserStatement};
use crate::expr::{Base, DefineAnalyzerStatement};
use crate::key::thing;
use crate::kvs::KVValue;
use crate::sql::ToSql;
use crate::val::record::Record;
use crate::val::{RecordId, Strand, Value};

#[derive(Clone, Debug)]
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

impl Config {
	pub fn from_value(value: &Value) -> Result<Self, anyhow::Error> {
		match value {
			Value::Object(obj) => {
				let mut config = Config::default();

				macro_rules! bool_prop {
					($prop:ident) => {{
						match obj.get(stringify!($prop)) {
							Some(Value::Bool(v)) => {
								config.$prop = v.to_owned();
							}
							Some(v) => {
								return Err(anyhow::Error::new(Error::InvalidExportConfig(
									v.to_owned(),
									"a bool".into(),
								)));
							}
							_ => (),
						}
					}};
				}

				bool_prop!(users);
				bool_prop!(accesses);
				bool_prop!(params);
				bool_prop!(functions);
				bool_prop!(analyzers);
				bool_prop!(versions);
				bool_prop!(records);

				if let Some(v) = obj.get("tables") {
					config.tables = v.try_into()?;
				}

				Ok(config)
			}
			v => Err(anyhow::Error::new(Error::InvalidExportConfig(
				v.to_owned(),
				"an object".into(),
			))),
		}
	}
}

impl From<Config> for Value {
	fn from(config: Config) -> Value {
		let obj = map!(
			"users" => config.users.into(),
			"accesses" => config.accesses.into(),
			"params" => config.params.into(),
			"functions" => config.functions.into(),
			"analyzers" => config.analyzers.into(),
			"versions" => config.versions.into(),
			"records" => config.records.into(),
			"sequences" => config.sequences.into(),
			"tables" => match config.tables {
				TableConfig::All => true.into(),
				TableConfig::None => false.into(),
				// TODO: Null byte validity
				TableConfig::Some(v) => v.into_iter().map(|x| Value::Strand(Strand::new(x).unwrap())).collect::<Vec<_>>().into()
			},
		);

		obj.into()
	}
}

#[derive(Clone, Debug, Default)]
pub enum TableConfig {
	#[default]
	All,
	None,
	Some(Vec<String>),
}

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

impl TryFrom<&Value> for TableConfig {
	type Error = anyhow::Error;
	fn try_from(value: &Value) -> Result<Self, Self::Error> {
		match value {
			Value::Bool(b) => {
				if *b {
					Ok(TableConfig::All)
				} else {
					Ok(TableConfig::None)
				}
			}
			Value::None | Value::Null => Ok(TableConfig::None),
			Value::Array(v) => v
				.iter()
				.cloned()
				.map(|v| match v {
					Value::Strand(str) => Ok(str.into_string()),
					v => Err(anyhow::Error::new(Error::InvalidExportConfig(
						v.clone(),
						"a string".into(),
					))),
				})
				.collect::<Result<Vec<String>>>()
				.map(TableConfig::Some),
			v => Err(anyhow::Error::new(Error::InvalidExportConfig(
				v.to_owned(),
				"a bool, none, null or array<string>".into(),
			))),
		}
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
		self.export_section("OPTION", ["OPTION IMPORT"].iter(), chn).await?;

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
				self.export_table_data(ns, db, table, cfg, chn).await?;
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
		cfg: &Config,
		chn: &Sender<Vec<u8>>,
	) -> Result<()> {
		chn.send(bytes!("-- ------------------------------")).await?;
		chn.send(bytes!(format!("-- TABLE DATA: {}", InlineCommentDisplay(&table.name)))).await?;
		chn.send(bytes!("-- ------------------------------")).await?;
		chn.send(bytes!("")).await?;

		let beg = crate::key::thing::prefix(ns, db, &table.name)?;
		let end = crate::key::thing::suffix(ns, db, &table.name)?;
		let mut next = Some(beg..end);

		while let Some(rng) = next {
			if cfg.versions {
				let batch = self.batch_keys_vals_versions(rng, *EXPORT_BATCH_SIZE).await?;
				next = batch.next;
				// If there are no versioned values, return early.
				if batch.result.is_empty() {
					break;
				}
				self.export_versioned_data(batch.result, chn).await?;
			} else {
				let batch = self.batch_keys_vals(rng, *EXPORT_BATCH_SIZE, None).await?;
				next = batch.next;
				// If there are no values, return early.
				if batch.result.is_empty() {
					break;
				}
				self.export_regular_data(batch.result, chn).await?;
			}
			// Fetch more records
			continue;
		}

		chn.send(bytes!("")).await?;
		Ok(())
	}

	/// Processes a value and generates the appropriate SQL command.
	///
	/// This function processes a value, categorizing it into either normal
	/// records or graph edge records, and generates the appropriate SQL
	/// command based on the type of record and the presence of a version.
	///
	/// # Arguments
	///
	/// * `v` - The value to be processed.
	/// * `records_relate` - A mutable reference to a vector that holds graph edge records.
	/// * `records_normal` - A mutable reference to a vector that holds normal records.
	/// * `is_tombstone` - An optional boolean indicating if the record is a tombstone.
	/// * `version` - An optional version number for the record.
	///
	/// # Returns
	///
	/// * `String` - Returns the generated SQL command as a string. If no command is generated,
	///   returns an empty string.
	fn process_record(
		k: thing::ThingKey,
		mut record: Record,
		records_relate: &mut Vec<String>,
		records_normal: &mut Vec<String>,
		is_tombstone: Option<bool>,
		version: Option<u64>,
	) -> String {
		// Inject the id field into the document before processing.
		let rid = RecordId {
			table: k.tb.to_owned(),
			key: k.id.clone(),
		};
		record.data.to_mut().def(&rid);
		// Match on the value to determine if it is a graph edge record or a normal
		// record.
		match (record.is_edge(), record.data.as_ref().pick(&*IN), record.data.as_ref().pick(&*OUT))
		{
			// If the value is a graph edge record (indicated by EDGE, IN, and OUT fields):
			(true, Value::RecordId(_), Value::RecordId(_)) => {
				if let Some(version) = version {
					// If a version exists, format the value as an INSERT RELATION VERSION command.
					let ts = Utc.timestamp_nanos(version as i64);
					let sql =
						format!("INSERT RELATION {} VERSION d'{:?}';", record.data.as_ref(), ts);
					records_relate.push(sql);
					String::new()
				} else {
					// If no version exists, push the value to the records_relate vector.
					records_relate.push(record.data.as_ref().to_string());
					String::new()
				}
			}
			// If the value is a normal record:
			_ => {
				if let Some(is_tombstone) = is_tombstone {
					if is_tombstone {
						// If the record is a tombstone, format it as a DELETE command.
						format!("DELETE {}:{};", k.tb, k.id)
					} else {
						// If the record is not a tombstone and a version exists, format it as an
						// INSERT VERSION command.
						let ts = Utc.timestamp_nanos(version.unwrap() as i64);
						format!("INSERT {} VERSION d'{:?}';", record.data.as_ref(), ts)
					}
				} else {
					// If no tombstone or version information is provided, push the value to the
					// records_normal vector.
					records_normal.push(record.data.as_ref().to_string());
					String::new()
				}
			}
		}
	}

	/// Exports versioned data to the provided channel.
	///
	/// This function processes a list of versioned values, converting them into
	/// SQL commands and sending them to the provided channel. It handles both
	/// normal records and graph edge records, and ensures that the appropriate
	/// SQL commands are generated for each type of record.
	///
	/// # Arguments
	///
	/// * `versioned_values` - A vector of tuples containing the versioned values to be exported.
	///   Each tuple consists of a key, value, version, and a boolean indicating if the record is a
	///   tombstone.
	/// * `chn` - A reference to the channel to which the SQL commands will be sent.
	///
	/// # Returns
	///
	/// * `Result<()>` - Returns `Ok(())` if the operation is successful, or an `Error` if an error
	///   occurs.
	async fn export_versioned_data(
		&self,
		versioned_values: Vec<(Vec<u8>, Vec<u8>, u64, bool)>,
		chn: &Sender<Vec<u8>>,
	) -> Result<()> {
		// Initialize a vector to hold graph edge records.
		let mut records_relate = Vec::with_capacity(*EXPORT_BATCH_SIZE as usize);

		// Initialize a counter for the number of processed records.
		let mut counter = 0;

		// Process each versioned value.
		for (k, v, version, is_tombstone) in versioned_values {
			// Begin a new transaction at the beginning of each batch.
			if counter % *EXPORT_BATCH_SIZE == 0 {
				chn.send(bytes!("BEGIN;")).await?;
			}

			let k = thing::ThingKey::decode_key(&k)?;
			let v: Record = if v.is_empty() {
				Default::default()
			} else {
				KVValue::kv_decode_value(v)?
			};
			// Process the value and generate the appropriate SQL command.
			let sql = Self::process_record(
				k,
				v,
				&mut records_relate,
				&mut Vec::new(),
				Some(is_tombstone),
				Some(version),
			);
			// If the SQL command is not empty, send it to the channel.
			if !sql.is_empty() {
				chn.send(bytes!(sql)).await?;
			}

			// Increment the counter.
			counter += 1;

			// Commit the transaction at the end of each batch.
			if counter % *EXPORT_BATCH_SIZE == 0 {
				chn.send(bytes!("COMMIT;")).await?;
			}
		}

		// Commit any remaining records if the last batch was not full.
		if counter % *EXPORT_BATCH_SIZE != 0 {
			chn.send(bytes!("COMMIT;")).await?;
		}

		// If there are no graph edge records, return early.
		if records_relate.is_empty() {
			return Ok(());
		}

		// Begin a new transaction for graph edge records.
		chn.send(bytes!("BEGIN;")).await?;

		// If there are graph edge records, send them to the channel.
		if !records_relate.is_empty() {
			for record in records_relate.iter() {
				chn.send(bytes!(record)).await?;
			}
		}

		// Commit the transaction for graph edge records.
		chn.send(bytes!("COMMIT;")).await?;

		Ok(())
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
		// Initialize vectors to hold normal records and graph edge records.
		let mut records_normal = Vec::with_capacity(*EXPORT_BATCH_SIZE as usize);
		let mut records_relate = Vec::with_capacity(*EXPORT_BATCH_SIZE as usize);

		// Process each regular value.
		for (k, v) in regular_values {
			let k = thing::ThingKey::decode_key(&k)?;
			let v = Record::kv_decode_value(v)?;
			// Process the value and categorize it into records_relate or records_normal.
			Self::process_record(k, v, &mut records_relate, &mut records_normal, None, None);
		}

		// If there are normal records, generate and send the INSERT SQL command.
		if !records_normal.is_empty() {
			let values = records_normal.join(", ");
			let sql = format!("INSERT [ {} ];", values);
			chn.send(bytes!(sql)).await?;
		}

		// If there are graph edge records, generate and send the INSERT RELATION SQL
		// command.
		if !records_relate.is_empty() {
			let values = records_relate.join(", ");
			let sql = format!("INSERT RELATION [ {} ];", values);
			chn.send(bytes!(sql)).await?;
		}

		Ok(())
	}
}
