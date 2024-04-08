use super::ScanPage;
use super::Transaction;
use crate::err::Error;
use crate::sql::paths::EDGE;
use crate::sql::paths::IN;
use crate::sql::paths::OUT;
use crate::cnf::EXPORT_BATCH_SIZE;
use crate::sql::Value;
use channel::Sender;

impl Transaction {
	/// Writes the full database contents as binary SQL.
	pub async fn export(&mut self, ns: &str, db: &str, chn: Sender<Vec<u8>>) -> Result<(), Error> {
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
            // Start transaction
            chn.send(bytes!("-- ------------------------------")).await?;
            chn.send(bytes!("-- TRANSACTION")).await?;
            chn.send(bytes!("-- ------------------------------")).await?;
            chn.send(bytes!("")).await?;
            chn.send(bytes!("BEGIN TRANSACTION;")).await?;
            chn.send(bytes!("")).await?;
            // Records to be exported, categorised by the type of INSERT statement
            let mut exported_normal: Vec<String> =
                Vec::with_capacity(*EXPORT_BATCH_SIZE as usize);
            let mut exported_relation: Vec<String> =
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
                let mut nxt: Option<ScanPage<Vec<u8>>> = Some(ScanPage::from(beg..end));
                while nxt.is_some() {
                    let res = self.scan_paged(nxt.unwrap(), *EXPORT_BATCH_SIZE).await?;
                    nxt = res.next_page;
                    let res = res.values;
                    if res.is_empty() {
                        break;
                    }

                    // Categorize results
                    for (_, v) in res.into_iter() {
                        // Parse the key and the value
                        let v: Value = (&v).into();
                        // Check if this is a graph edge
                        match (v.pick(&*EDGE), v.pick(&*IN), v.pick(&*OUT)) {
                            // This is a graph edge record
                            (Value::Bool(true), Value::Thing(_), Value::Thing(_)) => {
                                exported_relation.push(v.to_string());
                            }
                            // This is a normal record
                            _ => {
                                exported_normal.push(v.to_string());
                            }
                        }
                    }

                    // Add batches of INSERT statements
                    // No need to chunk here, the scan it limited to 1000
                    if !exported_normal.is_empty() {
                        let values = exported_normal.join(", ");
                        let sql = format!("INSERT [ {values} ];");
                        chn.send(bytes!(sql)).await?;
                        exported_normal.clear();
                    }

                    // Add batches of INSERT RELATION statements
                    // No need to chunk here, the scan it limited to 1000
                    if !exported_relation.is_empty() {
                        let values = exported_relation.join(", ");
                        let sql = format!("INSERT RELATION [ {values} ];");
                        chn.send(bytes!(sql)).await?;
                        exported_relation.clear()
                    }

                    continue;
                }
                chn.send(bytes!("")).await?;
            }
            // Commit transaction
            chn.send(bytes!("-- ------------------------------")).await?;
            chn.send(bytes!("-- TRANSACTION")).await?;
            chn.send(bytes!("-- ------------------------------")).await?;
            chn.send(bytes!("")).await?;
            chn.send(bytes!("COMMIT TRANSACTION;")).await?;
            chn.send(bytes!("")).await?;
        }
    }
    // Everything exported
    Ok(())
}
}
