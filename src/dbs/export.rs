use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::err::Error;
use bytes::Bytes;
use hyper::body::Sender;

macro_rules! output {
	($expression:expr) => {
		Bytes::from(format!("{}\n", $expression))
	};
}

impl Executor {
	pub async fn export(
		&mut self,
		ctx: Runtime,
		opt: Options,
		mut chn: Sender,
	) -> Result<(), Error> {
		// Start a new transaction
		let txn = self.dbs.transaction(false, false).await?;
		// Output OPTIONS
		chn.send_data(output!("-- ------------------------------")).await?;
		chn.send_data(output!("-- OPTION")).await?;
		chn.send_data(output!("-- ------------------------------")).await?;
		chn.send_data(output!("")).await?;
		chn.send_data(output!("OPTION IMPORT;")).await?;
		chn.send_data(output!("")).await?;
		// Output LOGINS
		chn.send_data(output!("-- ------------------------------")).await?;
		chn.send_data(output!("-- LOGINS")).await?;
		chn.send_data(output!("-- ------------------------------")).await?;
		chn.send_data(output!("")).await?;
		// Output TOKENS
		chn.send_data(output!("-- ------------------------------")).await?;
		chn.send_data(output!("-- TOKENS")).await?;
		chn.send_data(output!("-- ------------------------------")).await?;
		chn.send_data(output!("")).await?;
		// Output SCOPES
		chn.send_data(output!("-- ------------------------------")).await?;
		chn.send_data(output!("-- SCOPES")).await?;
		chn.send_data(output!("-- ------------------------------")).await?;
		chn.send_data(output!("")).await?;
		// Output TABLES
		for v in 0..1 {
			chn.send_data(output!("-- ------------------------------")).await?;
			chn.send_data(output!(format!("-- TABLE: {}", v))).await?;
			chn.send_data(output!("-- ------------------------------")).await?;
			chn.send_data(output!("")).await?;
		}
		// Start transaction
		chn.send_data(output!("-- ------------------------------")).await?;
		chn.send_data(output!("-- TRANSACTION")).await?;
		chn.send_data(output!("-- ------------------------------")).await?;
		chn.send_data(output!("")).await?;
		chn.send_data(output!("BEGIN TRANSACTION;")).await?;
		chn.send_data(output!("")).await?;
		// Output TABLE data
		for v in 0..1 {
			chn.send_data(output!("-- ------------------------------")).await?;
			chn.send_data(output!(format!("-- TABLE DATA: {}", v))).await?;
			chn.send_data(output!("-- ------------------------------")).await?;
			chn.send_data(output!("")).await?;
		}
		// Commit transaction
		chn.send_data(output!("-- ------------------------------")).await?;
		chn.send_data(output!("-- TRANSACTION")).await?;
		chn.send_data(output!("-- ------------------------------")).await?;
		chn.send_data(output!("")).await?;
		chn.send_data(output!("COMMIT TRANSACTION;")).await?;
		chn.send_data(output!("")).await?;
		// Everything ok
		Ok(())
	}
}
