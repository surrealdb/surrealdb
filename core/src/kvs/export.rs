use super::Transaction;
use crate::cnf::EXPORT_BATCH_SIZE;
use crate::err::Error;
use crate::sql::paths::EDGE;
use crate::sql::paths::IN;
use crate::sql::paths::OUT;
use crate::sql::Value;
use channel::Sender;

impl Transaction {
	/// Writes the full database contents as binary SQL.
	pub async fn export(&self, ns: &str, db: &str, chn: Sender<Vec<u8>>) -> Result<(), Error> {
		// Output OPTIONS
		{
			chn.send(bytes!("-- ------------------------------")).await?;
			chn.send(bytes!("-- OPTION")).await?;
			chn.send(bytes!("-- ------------------------------")).await?;
			chn.send(bytes!("")).await?;
			chn.send(bytes!("OPTION IMPORT;")).await?;
			chn.send(bytes!("")).await?;
		}
		// Output USERS
		{
			let dus = self.all_db_users(ns, db).await?;
			if !dus.is_empty() {
				chn.send(bytes!("-- ------------------------------")).await?;
				chn.send(bytes!("-- USERS")).await?;
				chn.send(bytes!("-- ------------------------------")).await?;
				chn.send(bytes!("")).await?;
				for us in dus.iter() {
					chn.send(bytes!(format!("{us};"))).await?;
				}
				chn.send(bytes!("")).await?;
			}
		}
		// Output ACCESSES
		{
			let dts = self.all_db_accesses(ns, db).await?;
			if !dts.is_empty() {
				chn.send(bytes!("-- ------------------------------")).await?;
				chn.send(bytes!("-- ACCESSES")).await?;
				chn.send(bytes!("-- ------------------------------")).await?;
				chn.send(bytes!("")).await?;
				for dt in dts.iter() {
					chn.send(bytes!(format!("{dt};"))).await?;
				}
				chn.send(bytes!("")).await?;
			}
		}
		// Output PARAMS
		{
			let pas = self.all_db_params(ns, db).await?;
			if !pas.is_empty() {
				chn.send(bytes!("-- ------------------------------")).await?;
				chn.send(bytes!("-- PARAMS")).await?;
				chn.send(bytes!("-- ------------------------------")).await?;
				chn.send(bytes!("")).await?;
				for pa in pas.iter() {
					chn.send(bytes!(format!("{pa};"))).await?;
				}
				chn.send(bytes!("")).await?;
			}
		}
		// Output FUNCTIONS
		{
			let fcs = self.all_db_functions(ns, db).await?;
			if !fcs.is_empty() {
				chn.send(bytes!("-- ------------------------------")).await?;
				chn.send(bytes!("-- FUNCTIONS")).await?;
				chn.send(bytes!("-- ------------------------------")).await?;
				chn.send(bytes!("")).await?;
				for fc in fcs.iter() {
					chn.send(bytes!(format!("{fc};"))).await?;
				}
				chn.send(bytes!("")).await?;
			}
		}
		// Output ANALYZERS
		{
			let azs = self.all_db_analyzers(ns, db).await?;
			if !azs.is_empty() {
				chn.send(bytes!("-- ------------------------------")).await?;
				chn.send(bytes!("-- ANALYZERS")).await?;
				chn.send(bytes!("-- ------------------------------")).await?;
				chn.send(bytes!("")).await?;
				for az in azs.iter() {
					chn.send(bytes!(format!("{az};"))).await?;
				}
				chn.send(bytes!("")).await?;
			}
		}
		// Output TABLES
		{
			let tbs = self.all_tb(ns, db).await?;
			if !tbs.is_empty() {
				for tb in tbs.iter() {
					// Output TABLE
					chn.send(bytes!("-- ------------------------------")).await?;
					chn.send(bytes!(format!("-- TABLE: {}", tb.name))).await?;
					chn.send(bytes!("-- ------------------------------")).await?;
					chn.send(bytes!("")).await?;
					chn.send(bytes!(format!("{tb};"))).await?;
					chn.send(bytes!("")).await?;
					// Output FIELDS
					let fds = self.all_tb_fields(ns, db, &tb.name).await?;
					if !fds.is_empty() {
						for fd in fds.iter() {
							chn.send(bytes!(format!("{fd};"))).await?;
						}
						chn.send(bytes!("")).await?;
					}
					// Output INDEXES
					let ixs = self.all_tb_indexes(ns, db, &tb.name).await?;
					if !ixs.is_empty() {
						for ix in ixs.iter() {
							chn.send(bytes!(format!("{ix};"))).await?;
						}
						chn.send(bytes!("")).await?;
					}
					// Output EVENTS
					let evs = self.all_tb_events(ns, db, &tb.name).await?;
					if !evs.is_empty() {
						for ev in evs.iter() {
							chn.send(bytes!(format!("{ev};"))).await?;
						}
						chn.send(bytes!("")).await?;
					}
				}
				// Records to be exported, categorised by the type of INSERT statement
				let mut records_normal: Vec<String> =
					Vec::with_capacity(*EXPORT_BATCH_SIZE as usize);
				let mut records_relate: Vec<String> =
					Vec::with_capacity(*EXPORT_BATCH_SIZE as usize);
				// Output TABLE data
				for tb in tbs.iter() {
					// Start records
					chn.send(bytes!("-- ------------------------------")).await?;
					chn.send(bytes!(format!("-- TABLE DATA: {}", tb.name))).await?;
					chn.send(bytes!("-- ------------------------------")).await?;
					chn.send(bytes!("")).await?;
					// Fetch records
					let beg = crate::key::thing::prefix(ns, db, &tb.name);
					let end = crate::key::thing::suffix(ns, db, &tb.name);
					let mut next = Some(beg..end);
					while let Some(rng) = next {
						// Get the next batch of records
						let batch = self.batch(rng, *EXPORT_BATCH_SIZE, true).await?;
						// Set the next scan range
						next = batch.next;
						// Check there are records
						if batch.values.is_empty() {
							break;
						}
						// Categorize the record types
						for (_, v) in batch.values.into_iter() {
							// Parse the key and the value
							let v: Value = (&v).into();
							// Check if this is a graph edge
							match (v.pick(&*EDGE), v.pick(&*IN), v.pick(&*OUT)) {
								// This is a graph edge record
								(Value::Bool(true), Value::Thing(_), Value::Thing(_)) => {
									records_relate.push(v.to_string());
								}
								// This is a normal record
								_ => {
									records_normal.push(v.to_string());
								}
							}
						}
						// Add batches of INSERT statements
						if !records_normal.is_empty() {
							let values = records_normal.join(", ");
							let sql = format!("INSERT [ {values} ];");
							chn.send(bytes!(sql)).await?;
							records_normal.clear();
						}
						// Add batches of INSERT RELATION statements
						if !records_relate.is_empty() {
							let values = records_relate.join(", ");
							let sql = format!("INSERT RELATION [ {values} ];");
							chn.send(bytes!(sql)).await?;
							records_relate.clear()
						}
						// Fetch more records
						continue;
					}
					chn.send(bytes!("")).await?;
				}
			}
		}
		// Everything exported
		Ok(())
	}
}
