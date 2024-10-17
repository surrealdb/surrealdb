use super::Transaction;
use crate::cnf::EXPORT_BATCH_SIZE;
use crate::err::Error;
use crate::sql::paths::EDGE;
use crate::sql::paths::ID;
use crate::sql::paths::IN;
use crate::sql::paths::OUT;
use crate::sql::statements::DefineTableStatement;
use crate::sql::Duration;
use crate::sql::Value;
use channel::Sender;

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

#[derive(Clone, Debug, Default)]
pub enum TableConfig {
	#[default]
	All,
	None,
	Some(Vec<String>),
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

// impl Transaction {
// 	/// Writes the full database contents as binary SQL.
// 	pub async fn export(
// 		&self,
// 		ns: &str,
// 		db: &str,
// 		cfg: Config,
// 		chn: Sender<Vec<u8>>,
// 	) -> Result<(), Error> {
// 		// Output OPTIONS
// 		{
// 			chn.send(bytes!("-- ------------------------------")).await?;
// 			chn.send(bytes!("-- OPTION")).await?;
// 			chn.send(bytes!("-- ------------------------------")).await?;
// 			chn.send(bytes!("")).await?;
// 			chn.send(bytes!("OPTION IMPORT;")).await?;
// 			chn.send(bytes!("")).await?;
// 		}
// 		// Output USERS
// 		if cfg.users {
// 			let dus = self.all_db_users(ns, db).await?;
// 			if !dus.is_empty() {
// 				chn.send(bytes!("-- ------------------------------")).await?;
// 				chn.send(bytes!("-- USERS")).await?;
// 				chn.send(bytes!("-- ------------------------------")).await?;
// 				chn.send(bytes!("")).await?;
// 				for us in dus.iter() {
// 					chn.send(bytes!(format!("{us};"))).await?;
// 				}
// 				chn.send(bytes!("")).await?;
// 			}
// 		}
// 		// Output ACCESSES
// 		if cfg.accesses {
// 			let dts = self.all_db_accesses(ns, db).await?;
// 			if !dts.is_empty() {
// 				chn.send(bytes!("-- ------------------------------")).await?;
// 				chn.send(bytes!("-- ACCESSES")).await?;
// 				chn.send(bytes!("-- ------------------------------")).await?;
// 				chn.send(bytes!("")).await?;
// 				for dt in dts.iter() {
// 					chn.send(bytes!(format!("{dt};"))).await?;
// 				}
// 				chn.send(bytes!("")).await?;
// 			}
// 		}
// 		// Output PARAMS
// 		if cfg.params {
// 			let pas = self.all_db_params(ns, db).await?;
// 			if !pas.is_empty() {
// 				chn.send(bytes!("-- ------------------------------")).await?;
// 				chn.send(bytes!("-- PARAMS")).await?;
// 				chn.send(bytes!("-- ------------------------------")).await?;
// 				chn.send(bytes!("")).await?;
// 				for pa in pas.iter() {
// 					chn.send(bytes!(format!("{pa};"))).await?;
// 				}
// 				chn.send(bytes!("")).await?;
// 			}
// 		}
// 		// Output FUNCTIONS
// 		if cfg.functions {
// 			let fcs = self.all_db_functions(ns, db).await?;
// 			if !fcs.is_empty() {
// 				chn.send(bytes!("-- ------------------------------")).await?;
// 				chn.send(bytes!("-- FUNCTIONS")).await?;
// 				chn.send(bytes!("-- ------------------------------")).await?;
// 				chn.send(bytes!("")).await?;
// 				for fc in fcs.iter() {
// 					chn.send(bytes!(format!("{fc};"))).await?;
// 				}
// 				chn.send(bytes!("")).await?;
// 			}
// 		}
// 		// Output ANALYZERS
// 		if cfg.analyzers {
// 			let azs = self.all_db_analyzers(ns, db).await?;
// 			if !azs.is_empty() {
// 				chn.send(bytes!("-- ------------------------------")).await?;
// 				chn.send(bytes!("-- ANALYZERS")).await?;
// 				chn.send(bytes!("-- ------------------------------")).await?;
// 				chn.send(bytes!("")).await?;
// 				for az in azs.iter() {
// 					chn.send(bytes!(format!("{az};"))).await?;
// 				}
// 				chn.send(bytes!("")).await?;
// 			}
// 		}
// 		// Output TABLES
// 		if cfg.tables.is_any() {
// 			let tbs = self.all_tb(ns, db, None).await?;
// 			if !tbs.is_empty() {
// 				for tb in tbs.iter() {
// 					// Check table
// 					if !cfg.tables.includes(&tb.name) {
// 						continue;
// 					}
// 					// Output TABLE
// 					chn.send(bytes!("-- ------------------------------")).await?;
// 					chn.send(bytes!(format!("-- TABLE: {}", tb.name))).await?;
// 					chn.send(bytes!("-- ------------------------------")).await?;
// 					chn.send(bytes!("")).await?;
// 					chn.send(bytes!(format!("{tb};"))).await?;
// 					chn.send(bytes!("")).await?;
// 					// Output FIELDS
// 					let fds = self.all_tb_fields(ns, db, &tb.name, None).await?;
// 					if !fds.is_empty() {
// 						for fd in fds.iter() {
// 							chn.send(bytes!(format!("{fd};"))).await?;
// 						}
// 						chn.send(bytes!("")).await?;
// 					}
// 					// Output INDEXES
// 					let ixs = self.all_tb_indexes(ns, db, &tb.name).await?;
// 					if !ixs.is_empty() {
// 						for ix in ixs.iter() {
// 							chn.send(bytes!(format!("{ix};"))).await?;
// 						}
// 						chn.send(bytes!("")).await?;
// 					}
// 					// Output EVENTS
// 					let evs = self.all_tb_events(ns, db, &tb.name).await?;
// 					if !evs.is_empty() {
// 						for ev in evs.iter() {
// 							chn.send(bytes!(format!("{ev};"))).await?;
// 						}
// 						chn.send(bytes!("")).await?;
// 					}
// 				}
// 				// Records to be exported, categorised by the type of INSERT statement
// 				let mut records_normal: Vec<String> =
// 					Vec::with_capacity(*EXPORT_BATCH_SIZE as usize);
// 				let mut records_relate: Vec<String> =
// 					Vec::with_capacity(*EXPORT_BATCH_SIZE as usize);

// 				// Output TABLE data
// 				for tb in tbs.iter() {
// 					// Start records
// 					chn.send(bytes!("-- ------------------------------")).await?;
// 					chn.send(bytes!(format!("-- TABLE DATA: {}", tb.name))).await?;
// 					chn.send(bytes!("-- ------------------------------")).await?;
// 					chn.send(bytes!("")).await?;
// 					// Fetch records
// 					let beg = crate::key::thing::prefix(ns, db, &tb.name);
// 					let end = crate::key::thing::suffix(ns, db, &tb.name);
// 					let mut next = Some(beg..end);
// 					while let Some(rng) = next {
// 						// Get the next batch of records
// 						let batch = if cfg.versions {
// 							self.batch_versions(rng, *EXPORT_BATCH_SIZE).await?
// 						} else {
// 							self.batch(rng, *EXPORT_BATCH_SIZE, true, None).await?
// 						};
// 						// Set the next scan range
// 						next = batch.next;
// 						// Check there are records
// 						let values = if cfg.versions {
// 							BatchValues::Versioned(batch.versioned_values)
// 						} else {
// 							BatchValues::Regular(batch.values)
// 						};
// 						match values {
// 							BatchValues::Versioned(versioned_values) => {
// 								if versioned_values.is_empty() {
// 									break;
// 								}

// 								chn.send(bytes!("BEGIN")).await?;

// 								// Categorize the record types
// 								for (_, v, ts, is_tombstone) in versioned_values.into_iter() {
// 									// Parse the key and the value
// 									let v: Value = (&v).into();
// 									// Check if this is a graph edge
// 									match (v.pick(&*EDGE), v.pick(&*IN), v.pick(&*OUT)) {
// 										// This is a graph edge record
// 										(Value::Bool(true), Value::Thing(_), Value::Thing(_)) => {
// 											records_relate.push(v.to_string());
// 										}
// 										// This is a normal record
// 										_ => {
// 											let rec = v.to_string();

// 											if is_tombstone{
// 												let rec_id = v.pick(&*ID).to_string();
// 												let sql = format!(
// 													"DELETE {rec_id};",
// 												);
// 												println!("dsql: {:?}", sql);
// 												chn.send(bytes!(sql)).await?;
// 											} else {
// 												let sql = format!(
// 													"INSERT {rec} VERSION d'{:?}';",
// 													Duration::from_nanos(ts)
// 												);
// 												println!("isql: {:?}", sql);
// 												chn.send(bytes!(sql)).await?;
// 											}
// 										}
// 									}
// 								}

// 								chn.send(bytes!("COMMIT")).await?;
// 							}
// 							BatchValues::Regular(regular_values) => {
// 								if regular_values.is_empty() {
// 									break;
// 								}
// 								// Categorize the record types
// 								for (_, v) in regular_values.into_iter() {
// 									// Parse the key and the value
// 									let v: Value = (&v).into();
// 									// Check if this is a graph edge
// 									match (v.pick(&*EDGE), v.pick(&*IN), v.pick(&*OUT)) {
// 										// This is a graph edge record
// 										(Value::Bool(true), Value::Thing(_), Value::Thing(_)) => {
// 											records_relate.push(v.to_string());
// 										}
// 										// This is a normal record
// 										_ => {
// 											println!("v: {:?}", v.to_string());
// 											println!("id: {:?}", v.pick(&*ID).to_string());
// 											records_normal.push(v.to_string());
// 										}
// 									}
// 								}

// 								// Add batches of INSERT statements
// 								if !records_normal.is_empty() {
// 									let values = records_normal.join(", ");
// 									let sql = format!("INSERT [ {values} ];");
// 									println!("sql: {:?}", sql);
// 									chn.send(bytes!(sql)).await?;
// 									records_normal.clear();
// 								}
// 							}
// 						}

// 						// Add batches of INSERT RELATION statements
// 						if !records_relate.is_empty() {
// 							let values = records_relate.join(", ");
// 							let sql = format!("INSERT RELATION [ {values} ];");
// 							chn.send(bytes!(sql)).await?;
// 							records_relate.clear()
// 						}
// 						// Fetch more records
// 						continue;
// 					}

// 					chn.send(bytes!("")).await?;
// 				}
// 			}
// 		}
// 		// Everything exported
// 		Ok(())
// 	}
// }

impl Transaction {
	/// Writes the full database contents as binary SQL.
	pub async fn export(
		&self,
		ns: &str,
		db: &str,
		cfg: Config,
		chn: Sender<Vec<u8>>,
	) -> Result<(), Error> {
		self.export_header_sections(&cfg, &chn, &ns, &db).await?;
		// Output TABLES
		self.export_tables(ns, db, &cfg, &chn).await?;
		Ok(())
	}

	async fn export_header_sections(
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

			self.export_table_structure(ns, db, &table, chn).await?;
			self.export_table_data(ns, db, &table, cfg, chn).await?;
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
				self.export_versioned_data(values, chn).await?;
				if next.is_none() {
					break;
				}
			} else {
				let batch = self.batch(rng, *EXPORT_BATCH_SIZE, true, None).await?;
				next = batch.next;
				let values = batch.values;
				self.export_regular_data(values, chn).await?;
				if next.is_none() {
					break;
				}
			}
		}

		chn.send(bytes!("")).await?;
		Ok(())
	}

	fn process_value(v: Value, records_relate: &mut Vec<String>, records_normal: &mut Vec<String>, is_tombstone: Option<bool>, ts: Option<u64>) -> String {
		match (v.pick(&*EDGE), v.pick(&*IN), v.pick(&*OUT)) {
			(Value::Bool(true), Value::Thing(_), Value::Thing(_)) => {
				records_relate.push(v.to_string());
				String::new()
			}
			_ => {
				if let Some(is_tombstone) = is_tombstone {
					if is_tombstone {
						format!("DELETE {};", v.pick(&*ID))
					} else {
						format!("INSERT {} VERSION d'{:?}';", v, Duration::from_nanos(ts.unwrap()))
					}
				} else {
					records_normal.push(v.to_string());
					String::new()
				}
			}
		}
	}

	async fn export_versioned_data(
		&self,
		versioned_values: Vec<(Vec<u8>, Vec<u8>, u64, bool)>,
		chn: &Sender<Vec<u8>>,
	) -> Result<(), Error> {
		if versioned_values.is_empty() {
			return Ok(());
		}
	
		let mut records_relate = Vec::with_capacity(*EXPORT_BATCH_SIZE as usize);
	
		chn.send(bytes!("BEGIN")).await?;
		
		for (_, v, ts, is_tombstone) in versioned_values {
			let v: Value = (&v).into();
			let sql = Self::process_value(v, &mut records_relate, &mut Vec::new(), Some(is_tombstone), Some(ts));
			if !sql.is_empty() {
				chn.send(bytes!(sql)).await?;
			}
		}
	
		chn.send(bytes!("COMMIT")).await?;
	
		if !records_relate.is_empty() {
			let values = records_relate.join(", ");
			let sql = format!("INSERT RELATION [ {} ];", values);
			chn.send(bytes!(sql)).await?;
		}
	
		Ok(())
	}

async fn export_regular_data(
    &self,
    regular_values: Vec<(Vec<u8>, Vec<u8>)>,
    chn: &Sender<Vec<u8>>,
) -> Result<(), Error> {
    if regular_values.is_empty() {
        return Ok(());
    }

    let mut records_normal = Vec::with_capacity(*EXPORT_BATCH_SIZE as usize);
    let mut records_relate = Vec::with_capacity(*EXPORT_BATCH_SIZE as usize);

    for (_, v) in regular_values {
        let v: Value = (&v).into();
        Self::process_value(v, &mut records_relate, &mut records_normal, None, None);
    }

    if !records_normal.is_empty() {
        let values = records_normal.join(", ");
        let sql = format!("INSERT [ {} ];", values);
		chn.send(bytes!(sql)).await?;
    }

    if !records_relate.is_empty() {
        let values = records_relate.join(", ");
        let sql = format!("INSERT RELATION [ {} ];", values);
		chn.send(bytes!(sql)).await?;
    }

    Ok(())
}
}
