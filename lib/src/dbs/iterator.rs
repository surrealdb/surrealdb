use crate::cnf::MAX_CONCURRENT_TASKS;
use crate::ctx::Canceller;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::array::Array;
use crate::sql::field::Field;
use crate::sql::id::Id;
use crate::sql::part::Part;
use crate::sql::statements::create::CreateStatement;
use crate::sql::statements::delete::DeleteStatement;
use crate::sql::statements::insert::InsertStatement;
use crate::sql::statements::relate::RelateStatement;
use crate::sql::statements::select::SelectStatement;
use crate::sql::statements::update::UpdateStatement;
use crate::sql::table::Table;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use rand::Rng;
use std::cmp::Ordering;
use std::collections::BTreeMap;
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
			id: Id::rand(),
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
		self.output_split(&ctx, opt, txn).await?;
		// Process any GROUP clause
		self.output_group(&ctx, opt, txn).await?;
		// Process any ORDER clause
		self.output_order(&ctx, opt, txn).await?;
		// Process any START clause
		self.output_start(&ctx, opt, txn).await?;
		// Process any LIMIT clause
		self.output_limit(&ctx, opt, txn).await?;
		// Process any FETCH clause
		self.output_fetch(&ctx, opt, txn).await?;
		// Output the results
		Ok(mem::take(&mut self.results).into())
	}

	#[inline]
	async fn output_split(
		&mut self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
	) -> Result<(), Error> {
		if let Some(splits) = self.stm.split() {
			// Loop over each split clause
			for split in splits.iter() {
				// Get the query result
				let res = mem::take(&mut self.results);
				// Loop over each value
				for obj in &res {
					// Get the value at the path
					let val = obj.pick(&split.split);
					// Set the value at the path
					match val {
						Value::Array(v) => {
							for val in v.value {
								// Make a copy of object
								let mut obj = obj.clone();
								// Set the value at the path
								obj.set(ctx, opt, txn, &split.split, val).await?;
								// Add the object to the results
								self.results.push(obj);
							}
						}
						_ => {
							// Make a copy of object
							let mut obj = obj.clone();
							// Set the value at the path
							obj.set(ctx, opt, txn, &split.split, val).await?;
							// Add the object to the results
							self.results.push(obj);
						}
					}
				}
			}
		}
		Ok(())
	}

	#[inline]
	async fn output_group(
		&mut self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
	) -> Result<(), Error> {
		if let Some(fields) = self.stm.expr() {
			if let Some(groups) = self.stm.group() {
				// Create the new grouped collection
				let mut grp: BTreeMap<Array, Array> = BTreeMap::new();
				// Get the query result
				let res = mem::take(&mut self.results);
				// Loop over each value
				for obj in res {
					// Create a new column set
					let mut arr = Array::with_capacity(groups.len());
					// Loop over each group clause
					for group in groups.iter() {
						// Get the value at the path
						let val = obj.pick(&group.group);
						// Set the value at the path
						arr.value.push(val);
					}
					// Add to grouped collection
					match grp.get_mut(&arr) {
						Some(v) => v.value.push(obj),
						None => {
							grp.insert(arr, Array::from(obj));
						}
					}
				}
				// Loop over each grouped collection
				for (_, vals) in grp {
					// Create a new value
					let mut obj = Value::base();
					// Save the collected values
					let vals = Value::from(vals);
					// Loop over each group clause
					for field in fields.other() {
						// Process it if it is a normal field
						if let Field::Alone(v) = field {
							match v {
								Value::Function(f) if f.is_aggregate() => {
									let x = vals
										.all()
										.get(ctx, opt, txn, v.to_idiom().as_ref())
										.await?;
									let x = f.aggregate(x).compute(ctx, opt, txn, None).await?;
									obj.set(ctx, opt, txn, v.to_idiom().as_ref(), x).await?;
								}
								_ => {
									let x = vals.first();
									let x = v.compute(ctx, opt, txn, Some(&x)).await?;
									obj.set(ctx, opt, txn, v.to_idiom().as_ref(), x).await?;
								}
							}
						}
						// Process it if it is a aliased field
						if let Field::Alias(v, i) = field {
							match v {
								Value::Function(f) if f.is_aggregate() => {
									let x = vals.all().get(ctx, opt, txn, i).await?;
									let x = f.aggregate(x).compute(ctx, opt, txn, None).await?;
									obj.set(ctx, opt, txn, i, x).await?;
								}
								_ => {
									let x = vals.first();
									let x = v.compute(ctx, opt, txn, Some(&x)).await?;
									obj.set(ctx, opt, txn, i, x).await?;
								}
							}
						}
					}
					// Add the object to the results
					self.results.push(obj);
				}
			}
		}
		Ok(())
	}

	#[inline]
	async fn output_order(
		&mut self,
		_ctx: &Runtime,
		_opt: &Options,
		_txn: &Transaction,
	) -> Result<(), Error> {
		if let Some(orders) = self.stm.order() {
			// Sort the full result set
			self.results.sort_by(|a, b| {
				// Loop over each order clause
				for order in orders.iter() {
					// Reverse the ordering if DESC
					let o = match order.random {
						true => {
							let a = rand::random::<f64>();
							let b = rand::random::<f64>();
							a.partial_cmp(&b)
						}
						false => match order.direction {
							true => a.compare(b, &order.order, order.collate, order.numeric),
							false => b.compare(a, &order.order, order.collate, order.numeric),
						},
					};
					//
					match o {
						Some(Ordering::Greater) => return Ordering::Greater,
						Some(Ordering::Equal) => continue,
						Some(Ordering::Less) => return Ordering::Less,
						None => continue,
					}
				}
				Ordering::Equal
			})
		}
		Ok(())
	}

	#[inline]
	async fn output_start(
		&mut self,
		_ctx: &Runtime,
		_opt: &Options,
		_txn: &Transaction,
	) -> Result<(), Error> {
		if let Some(v) = self.stm.start() {
			self.results = mem::take(&mut self.results).into_iter().skip(v.0).collect();
		}
		Ok(())
	}

	#[inline]
	async fn output_limit(
		&mut self,
		_ctx: &Runtime,
		_opt: &Options,
		_txn: &Transaction,
	) -> Result<(), Error> {
		if let Some(v) = self.stm.limit() {
			self.results = mem::take(&mut self.results).into_iter().take(v.0).collect();
		}
		Ok(())
	}

	#[inline]
	async fn output_fetch(
		&mut self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
	) -> Result<(), Error> {
		if let Some(fetchs) = self.stm.fetch() {
			for fetch in &fetchs.0 {
				// Loop over each value
				for obj in &mut self.results {
					// Get the value at the path
					let val = obj.get(ctx, opt, txn, &fetch.fetch).await?;
					// Set the value at the path
					match val {
						Value::Array(v) => {
							// Fetch all remote records
							let val = Value::Array(v).get(ctx, opt, txn, &[Part::All]).await?;
							// Set the value at the path
							obj.set(ctx, opt, txn, &fetch.fetch, val).await?;
						}
						Value::Thing(v) => {
							// Fetch all remote records
							let val = Value::Thing(v).get(ctx, opt, txn, &[Part::All]).await?;
							// Set the value at the path
							obj.set(ctx, opt, txn, &fetch.fetch, val).await?;
						}
						_ => {
							// Set the value at the path
							obj.set(ctx, opt, txn, &fetch.fetch, val).await?;
						}
					}
				}
			}
		}
		Ok(())
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
					// Get current statement
					let stm = &self.stm;
					// Create an unbounded channel
					let (chn, rx) = tokio::sync::mpsc::channel(MAX_CONCURRENT_TASKS);
					// Process all prepared values
					for v in mem::take(&mut self.readies) {
						if ctx.is_ok() {
							tokio::spawn(v.channel(
								ctx.clone(),
								opt.clone(),
								stm.clone(),
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
