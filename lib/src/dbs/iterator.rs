use crate::cnf::ID_CHARS;
use crate::cnf::MAX_CONCURRENT_TASKS;
use crate::ctx::Canceller;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::statements::create::CreateStatement;
use crate::sql::statements::delete::DeleteStatement;
use crate::sql::statements::insert::InsertStatement;
use crate::sql::statements::relate::RelateStatement;
use crate::sql::statements::select::SelectStatement;
use crate::sql::statements::update::UpdateStatement;
use crate::sql::table::Table;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use nanoid::nanoid;
use std::mem;
use std::sync::Arc;

#[derive(Default)]
pub struct Iterator {
	// Iterator status
	run: Canceller,
	// Iterator statement
	stm: Statement,
	// Iterator run option
	parallel: bool,
	// Iterator runtime error
	error: Option<Error>,
	// Iterator input values
	readies: Vec<Value>,
	// Iterator output results
	results: Vec<Value>,
}

impl From<Arc<SelectStatement>> for Iterator {
	fn from(v: Arc<SelectStatement>) -> Self {
		Iterator {
			parallel: v.parallel,
			stm: Statement::from(v),
			..Iterator::default()
		}
	}
}

impl From<Arc<CreateStatement>> for Iterator {
	fn from(v: Arc<CreateStatement>) -> Self {
		Iterator {
			parallel: v.parallel,
			stm: Statement::from(v),
			..Iterator::default()
		}
	}
}

impl From<Arc<UpdateStatement>> for Iterator {
	fn from(v: Arc<UpdateStatement>) -> Self {
		Iterator {
			parallel: v.parallel,
			stm: Statement::from(v),
			..Iterator::default()
		}
	}
}

impl From<Arc<RelateStatement>> for Iterator {
	fn from(v: Arc<RelateStatement>) -> Self {
		Iterator {
			parallel: v.parallel,
			stm: Statement::from(v),
			..Iterator::default()
		}
	}
}

impl From<Arc<DeleteStatement>> for Iterator {
	fn from(v: Arc<DeleteStatement>) -> Self {
		Iterator {
			parallel: v.parallel,
			stm: Statement::from(v),
			..Iterator::default()
		}
	}
}

impl From<Arc<InsertStatement>> for Iterator {
	fn from(v: Arc<InsertStatement>) -> Self {
		Iterator {
			parallel: v.parallel,
			stm: Statement::from(v),
			..Iterator::default()
		}
	}
}

impl Iterator {
	// Prepares a value for processing
	pub fn prepare(&mut self, val: Value) {
		self.readies.push(val)
	}

	// Create a new record for processing
	pub fn produce(&mut self, val: Table) {
		self.prepare(Value::Thing(Thing {
			tb: val.name,
			id: nanoid!(20, &ID_CHARS),
		}))
	}

	// Process the records and output
	pub async fn output(
		&mut self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
	) -> Result<Value, Error> {
		// Log the statement
		trace!("Iterating: {}", self.stm);
		// Enable context override
		let mut ctx = Context::new(ctx);
		self.run = ctx.add_cancel();
		let ctx = ctx.freeze();
		// Process prepared values
		self.iterate(&ctx, opt, txn).await?;
		// Return any document errors
		if let Some(e) = self.error.take() {
			return Err(e);
		}
		// Process any SPLIT clause
		self.output_split(&ctx, opt, txn);
		// Process any GROUP clause
		self.output_group(&ctx, opt, txn);
		// Process any ORDER clause
		self.output_order(&ctx, opt, txn);
		// Process any START clause
		self.output_start(&ctx, opt, txn);
		// Process any LIMIT clause
		self.output_limit(&ctx, opt, txn);
		// Output the results
		Ok(mem::take(&mut self.results).into())
	}

	#[inline]
	fn output_split(&mut self, _ctx: &Runtime, _opt: &Options, _txn: &Transaction) {
		if self.stm.split().is_some() {
			// Ignore
		}
	}

	#[inline]
	fn output_group(&mut self, _ctx: &Runtime, _opt: &Options, _txn: &Transaction) {
		if self.stm.group().is_some() {
			// Ignore
		}
	}

	#[inline]
	fn output_order(&mut self, _ctx: &Runtime, _opt: &Options, _txn: &Transaction) {
		if self.stm.order().is_some() {
			// Ignore
		}
	}

	#[inline]
	fn output_start(&mut self, _ctx: &Runtime, _opt: &Options, _txn: &Transaction) {
		if let Some(v) = self.stm.start() {
			self.results = mem::take(&mut self.results).into_iter().skip(v.0).collect();
		}
	}

	#[inline]
	fn output_limit(&mut self, _ctx: &Runtime, _opt: &Options, _txn: &Transaction) {
		if let Some(v) = self.stm.limit() {
			self.results = mem::take(&mut self.results).into_iter().take(v.0).collect();
		}
	}

	#[cfg(not(feature = "parallel"))]
	async fn iterate(
		&mut self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
	) -> Result<(), Error> {
		// Process all prepared values
		for v in mem::take(&mut self.readies) {
			v.iterate(ctx, opt, txn, self).await?;
		}
		// Everything processed ok
		Ok(())
	}

	#[cfg(feature = "parallel")]
	async fn iterate(
		&mut self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
	) -> Result<(), Error> {
		match self.parallel {
			// Run statements sequentially
			false => {
				// Process all prepared values
				for v in mem::take(&mut self.readies) {
					v.iterate(ctx, opt, txn, self).await?;
				}
				// Everything processed ok
				Ok(())
			}
			// Run statements in parallel
			true => {
				let mut rcv = {
					// Create an unbounded channel
					let (chn, rx) = tokio::sync::mpsc::channel(MAX_CONCURRENT_TASKS);
					// Process all prepared values
					for v in mem::take(&mut self.readies) {
						if ctx.is_ok() {
							tokio::spawn(v.channel(
								ctx.clone(),
								opt.clone(),
								txn.clone(),
								chn.clone(),
							));
						}
					}
					// Return the receiver
					rx
				};
				let mut rcv = {
					// Clone the send values
					let ctx = ctx.clone();
					let opt = opt.clone();
					let txn = txn.clone();
					let stm = self.stm.clone();
					// Create an unbounded channel
					let (chn, rx) = tokio::sync::mpsc::channel(MAX_CONCURRENT_TASKS);
					// Process all received values
					tokio::spawn(async move {
						while let Some((k, v)) = rcv.recv().await {
							if ctx.is_ok() {
								tokio::spawn(Document::compute(
									ctx.clone(),
									opt.clone(),
									txn.clone(),
									chn.clone(),
									stm.clone(),
									k,
									v,
								));
							}
						}
					});
					// Return the receiver
					rx
				};
				// Process all processed values
				while let Some(r) = rcv.recv().await {
					self.result(r);
				}
				// Everything processed ok
				Ok(())
			}
		}
	}

	// Process a new record Thing and Value
	pub async fn process(
		&mut self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		thg: Option<Thing>,
		val: Value,
	) {
		// Check current context
		if ctx.is_done() {
			return;
		}
		// Setup a new document
		let mut doc = Document::new(thg, &val);
		// Process the document
		let res = match self.stm {
			Statement::Select(_) => doc.select(ctx, opt, txn, &self.stm).await,
			Statement::Create(_) => doc.create(ctx, opt, txn, &self.stm).await,
			Statement::Update(_) => doc.update(ctx, opt, txn, &self.stm).await,
			Statement::Relate(_) => doc.relate(ctx, opt, txn, &self.stm).await,
			Statement::Delete(_) => doc.delete(ctx, opt, txn, &self.stm).await,
			Statement::Insert(_) => doc.insert(ctx, opt, txn, &self.stm).await,
			_ => unreachable!(),
		};
		// Process the result
		self.result(res);
	}

	// Accept a processed record result
	fn result(&mut self, res: Result<Value, Error>) {
		// Process the result
		match res {
			Err(Error::Ignore) => {
				return;
			}
			Err(e) => {
				self.error = Some(e);
				self.run.cancel();
				return;
			}
			Ok(v) => self.results.push(v),
		}
		// Check if we can exit
		if self.stm.group().is_none() && self.stm.order().is_none() {
			if let Some(l) = self.stm.limit() {
				if let Some(s) = self.stm.start() {
					if self.results.len() == l.0 + s.0 {
						self.run.cancel()
					}
				} else if self.results.len() == l.0 {
					self.run.cancel()
				}
			}
		}
	}
}
