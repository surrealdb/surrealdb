use super::Transaction;
use crate::cnf::EXPORT_BATCH_SIZE;
use crate::err::Error;
use crate::key::thing;
use crate::sql::paths::EDGE;
use crate::sql::paths::IN;
use crate::sql::paths::OUT;
use crate::sql::statements::DefineTableStatement;
use crate::sql::Value;
use channel::Sender;
use chrono::prelude::Utc;
use chrono::TimeZone;

#[derive(Clone, Debug)]
pub struct Config {
	pub users: bool,
	pub accesses: bool,
	pub params: bool,
	pub functions: bool,
	pub analyzers: bool,
	pub tables: TableConfig,
	pub versions: bool,
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
		}
	}
}

impl TryFrom<&Value> for Config {
	type Error = Error;
	fn try_from(value: &Value) -> Result<Self, Self::Error> {
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
								return Err(Error::InvalidExportConfig(
									v.to_owned(),
									"a bool".into(),
								))
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

				if let Some(v) = obj.get("tables") {
					config.tables = v.try_into()?;
				}

				Ok(config)
			}
			v => Err(Error::InvalidExportConfig(v.to_owned(), "an object".into())),
		}
	}
}

#[derive(Clone, Debug, Default)]
pub enum TableConfig {
	#[default]
	All,
	None,
	Some(Vec<String>),
}

impl TryFrom<&Value> for TableConfig {
	type Error = Error;
	fn try_from(value: &Value) -> Result<Self, Self::Error> {
		match value {
			Value::Bool(b) => match b {
				true => Ok(TableConfig::All),
				false => Ok(TableConfig::None),
			},
			Value::None | Value::Null => Ok(TableConfig::None),
			Value::Array(v) => v
				.to_owned()
				.into_iter()
				.map(|v| match v {
					Value::Strand(str) => Ok(str.0),
					v => Err(Error::InvalidExportConfig(v.to_owned(), "a string".into())),
				})
				.collect::<Result<Vec<String>, Error>>()
				.map(TableConfig::Some),
			v => Err(Error::InvalidExportConfig(
				v.to_owned(),
				"a bool, none, null or array<string>".into(),
			)),
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

impl Transaction {
	/// Writes the full database contents as binary SQL.
	pub async fn export(
		&self,
		ns: &str,
		db: &str,
		cfg: Config,
		chn: Sender<Vec<u8>>,
	) -> Result<(), Error> {
		// Output USERS, ACCESSES, PARAMS, FUNCTIONS, ANALYZERS
		self.export_metadata(&cfg, &chn, ns, db).await?;
		// Output TABLES
		self.export_tables(ns, db, &cfg, &chn).await?;
		Ok(())
	}

	async fn export_metadata(
		&self,
		cfg: &Config,
		chn: &Sender<Vec<u8>>,
		ns: &str,
		db: &str,
	) -> Result<(), Error> {
		// Output OPTIONS
		self.export_section("OPTION", vec!["OPTION IMPORT;"], chn).await?;

		// Output USERS
		if cfg.users {
			let users = self.all_db_users(ns, db).await?;
			self.export_section("USERS", users.to_vec(), chn).await?;
		}

		// Output ACCESSES
		if cfg.accesses {
			let accesses = self.all_db_accesses(ns, db).await?;
			self.export_section("ACCESSES", accesses.to_vec(), chn).await?;
		}

		// Output PARAMS
		if cfg.params {
			let params = self.all_db_params(ns, db).await?;
			self.export_section("PARAMS", params.to_vec(), chn).await?;
		}

		// Output FUNCTIONS
		if cfg.functions {
			let functions = self.all_db_functions(ns, db).await?;
			self.export_section("FUNCTIONS", functions.to_vec(), chn).await?;
		}

		// Output ANALYZERS
		if cfg.analyzers {
			let analyzers = self.all_db_analyzers(ns, db).await?;
			self.export_section("ANALYZERS", analyzers.to_vec(), chn).await?;
		}

		Ok(())
	}

	async fn export_section<T: ToString>(
		&self,
		title: &str,
		items: Vec<T>,
		chn: &Sender<Vec<u8>>,
	) -> Result<(), Error> {
		if items.is_empty() {
			return Ok(());
		}

		chn.send(bytes!("-- ------------------------------")).await?;
		chn.send(bytes!(format!("-- {}", title))).await?;
		chn.send(bytes!("-- ------------------------------")).await?;
		chn.send(bytes!("")).await?;

		for item in items {
			chn.send(bytes!(format!("{};", item.to_string()))).await?;
		}

		chn.send(bytes!("")).await?;
		Ok(())
	}

	async fn export_tables(
		&self,
		ns: &str,
		db: &str,
		cfg: &Config,
		chn: &Sender<Vec<u8>>,
	) -> Result<(), Error> {
		if !cfg.tables.is_any() {
			return Ok(());
		}

		let tables = self.all_tb(ns, db, None).await?;
		for table in tables.iter() {
			if !cfg.tables.includes(&table.name) {
				continue;
			}

			self.export_table_structure(ns, db, table, chn).await?;
			self.export_table_data(ns, db, table, cfg, chn).await?;
		}

		Ok(())
	}

	async fn export_table_structure(
		&self,
		ns: &str,
		db: &str,
		table: &DefineTableStatement,
		chn: &Sender<Vec<u8>>,
	) -> Result<(), Error> {
		chn.send(bytes!("-- ------------------------------")).await?;
		chn.send(bytes!(format!("-- TABLE: {}", table.name))).await?;
		chn.send(bytes!("-- ------------------------------")).await?;
		chn.send(bytes!("")).await?;
		chn.send(bytes!(format!("{};", table))).await?;
		chn.send(bytes!("")).await?;

		let fields = self.all_tb_fields(ns, db, &table.name, None).await?;
		for field in fields.iter() {
			chn.send(bytes!(format!("{};", field))).await?;
		}
		chn.send(bytes!("")).await?;

		let indexes = self.all_tb_indexes(ns, db, &table.name).await?;
		for index in indexes.iter() {
			chn.send(bytes!(format!("{};", index))).await?;
		}
		chn.send(bytes!("")).await?;

		let events = self.all_tb_events(ns, db, &table.name).await?;
		for event in events.iter() {
			chn.send(bytes!(format!("{};", event))).await?;
		}
		chn.send(bytes!("")).await?;

		Ok(())
	}

	async fn export_table_data(
		&self,
		ns: &str,
		db: &str,
		table: &DefineTableStatement,
		cfg: &Config,
		chn: &Sender<Vec<u8>>,
	) -> Result<(), Error> {
		chn.send(bytes!("-- ------------------------------")).await?;
		chn.send(bytes!(format!("-- TABLE DATA: {}", table.name))).await?;
		chn.send(bytes!("-- ------------------------------")).await?;
		chn.send(bytes!("")).await?;

		let beg = crate::key::thing::prefix(ns, db, &table.name);
		let end = crate::key::thing::suffix(ns, db, &table.name);
		let mut next = Some(beg..end);

		while let Some(rng) = next {
			if cfg.versions {
				let batch = self.batch_versions(rng, *EXPORT_BATCH_SIZE).await?;
				next = batch.next;
				let values = batch.versioned_values;
				// If there are no versioned values, return early.
				if values.is_empty() {
					break;
				}
				self.export_versioned_data(values, chn).await?;
			} else {
				let batch = self.batch(rng, *EXPORT_BATCH_SIZE, true, None).await?;
				next = batch.next;
				// If there are no values, return early.
				let values = batch.values;
				if values.is_empty() {
					break;
				}
				self.export_regular_data(values, chn).await?;
			}
			// Fetch more records
			continue;
		}

		chn.send(bytes!("")).await?;
		Ok(())
	}

	/// Processes a value and generates the appropriate SQL command.
	///
	/// This function processes a value, categorizing it into either normal records or graph edge records,
	/// and generates the appropriate SQL command based on the type of record and the presence of a version.
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
	/// * `String` - Returns the generated SQL command as a string. If no command is generated, returns an empty string.
	fn process_value(
		k: thing::Thing,
		v: Value,
		records_relate: &mut Vec<String>,
		records_normal: &mut Vec<String>,
		is_tombstone: Option<bool>,
		version: Option<u64>,
	) -> String {
		// Match on the value to determine if it is a graph edge record or a normal record.
		match (v.pick(&*EDGE), v.pick(&*IN), v.pick(&*OUT)) {
			// If the value is a graph edge record (indicated by EDGE, IN, and OUT fields):
			(Value::Bool(true), Value::Thing(_), Value::Thing(_)) => {
				if let Some(version) = version {
					// If a version exists, format the value as an INSERT RELATION VERSION command.
					let ts = Utc.timestamp_nanos(version as i64);
					let sql = format!("INSERT RELATION {} VERSION d'{:?}';", v, ts);
					records_relate.push(sql);
					String::new()
				} else {
					// If no version exists, push the value to the records_relate vector.
					records_relate.push(v.to_string());
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
						// If the record is not a tombstone and a version exists, format it as an INSERT VERSION command.
						let ts = Utc.timestamp_nanos(version.unwrap() as i64);
						format!("INSERT {} VERSION d'{:?}';", v, ts)
					}
				} else {
					// If no tombstone or version information is provided, push the value to the records_normal vector.
					records_normal.push(v.to_string());
					String::new()
				}
			}
		}
	}

	/// Exports versioned data to the provided channel.
	///
	/// This function processes a list of versioned values, converting them into SQL commands
	/// and sending them to the provided channel. It handles both normal records and graph edge records,
	/// and ensures that the appropriate SQL commands are generated for each type of record.
	///
	/// # Arguments
	///
	/// * `versioned_values` - A vector of tuples containing the versioned values to be exported.
	///   Each tuple consists of a key, value, version, and a boolean indicating if the record is a tombstone.
	/// * `chn` - A reference to the channel to which the SQL commands will be sent.
	///
	/// # Returns
	///
	/// * `Result<(), Error>` - Returns `Ok(())` if the operation is successful, or an `Error` if an error occurs.
	async fn export_versioned_data(
		&self,
		versioned_values: Vec<(Vec<u8>, Vec<u8>, u64, bool)>,
		chn: &Sender<Vec<u8>>,
	) -> Result<(), Error> {
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

			let k: thing::Thing = (&k).into();
			let v: Value = if v.is_empty() {
				Value::None
			} else {
				(&v).into()
			};
			// Process the value and generate the appropriate SQL command.
			let sql = Self::process_value(
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
	/// This function processes a list of regular values, converting them into SQL commands
	/// and sending them to the provided channel. It handles both normal records and graph edge records,
	/// and ensures that the appropriate SQL commands are generated for each type of record.
	///
	/// # Arguments
	///
	/// * `regular_values` - A vector of tuples containing the regular values to be exported.
	///   Each tuple consists of a key and a value.
	/// * `chn` - A reference to the channel to which the SQL commands will be sent.
	///
	/// # Returns
	///
	/// * `Result<(), Error>` - Returns `Ok(())` if the operation is successful, or an `Error` if an error occurs.
	async fn export_regular_data(
		&self,
		regular_values: Vec<(Vec<u8>, Vec<u8>)>,
		chn: &Sender<Vec<u8>>,
	) -> Result<(), Error> {
		// Initialize vectors to hold normal records and graph edge records.
		let mut records_normal = Vec::with_capacity(*EXPORT_BATCH_SIZE as usize);
		let mut records_relate = Vec::with_capacity(*EXPORT_BATCH_SIZE as usize);

		// Process each regular value.
		for (k, v) in regular_values {
			let k: thing::Thing = (&k).into();
			let v: Value = (&v).into();
			// Process the value and categorize it into records_relate or records_normal.
			Self::process_value(k, v, &mut records_relate, &mut records_normal, None, None);
		}

		// If there are normal records, generate and send the INSERT SQL command.
		if !records_normal.is_empty() {
			let values = records_normal.join(", ");
			let sql = format!("INSERT [ {} ];", values);
			chn.send(bytes!(sql)).await?;
		}

		// If there are graph edge records, generate and send the INSERT RELATION SQL command.
		if !records_relate.is_empty() {
			let values = records_relate.join(", ");
			let sql = format!("INSERT RELATION [ {} ];", values);
			chn.send(bytes!(sql)).await?;
		}

		Ok(())
	}
}
