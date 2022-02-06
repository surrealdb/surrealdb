use crate::ctx::Canceller;
use crate::ctx::Context;
use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::group::Groups;
use crate::sql::limit::Limit;
use crate::sql::order::Orders;
use crate::sql::split::Splits;
use crate::sql::start::Start;
use crate::sql::table::Table;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use crate::sql::version::Version;
use std::mem;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use xid;

pub type Channel = UnboundedSender<Value>;

#[derive(Default)]
pub struct Iterator<'a> {
	// Iterator status
	run: Canceller,
	// Iterator runtime error
	error: Option<Error>,
	// Iterator input values
	readies: Vec<Value>,
	// Iterator output results
	results: Vec<Value>,
	// Iterate options
	pub parallel: bool,
	// Underlying statement
	pub stmt: Statement<'a>,
	// Iterator options
	pub split: Option<&'a Splits>,
	pub group: Option<&'a Groups>,
	pub order: Option<&'a Orders>,
	pub limit: Option<&'a Limit>,
	pub start: Option<&'a Start>,
	pub version: Option<&'a Version>,
}

impl<'a> Iterator<'a> {
	pub fn new() -> Iterator<'a> {
		Iterator::default()
	}

	// Prepares a value for processing
	pub fn prepare(&mut self, val: Value) {
		self.readies.push(val)
	}

	// Create a new record for processing
	pub fn produce(&mut self, val: Table) {
		self.prepare(Value::Thing(Thing {
			tb: val.name.to_string(),
			id: xid::new().to_string(),
		}))
	}

	// Process the records and output
	pub async fn output(
		&mut self,
		ctx: &Runtime,
		opt: &Options,
		exe: &Executor<'_>,
	) -> Result<Value, Error> {
		// Log the statement
		trace!("Iterating {}", self.stmt);
		// Enable context override
		let mut ctx = Context::new(&ctx);
		self.run = ctx.add_cancel();
		let ctx = ctx.freeze();
		// Process prepared values
		self.iterate(&ctx, opt, exe).await?;
		// Return any document errors
		if let Some(e) = self.error.take() {
			return Err(e);
		}
		// Process any SPLIT clause
		self.output_split(&ctx, opt, exe);
		// Process any GROUP clause
		self.output_group(&ctx, opt, exe);
		// Process any ORDER clause
		self.output_order(&ctx, opt, exe);
		// Process any START clause
		self.output_start(&ctx, opt, exe);
		// Process any LIMIT clause
		self.output_limit(&ctx, opt, exe);
		// Output the results
		Ok(mem::take(&mut self.results).into())
	}

	#[inline]
	fn output_split(&mut self, ctx: &Runtime, opt: &Options, exe: &Executor) {
		if self.split.is_some() {
			// Ignore
		}
	}

	#[inline]
	fn output_group(&mut self, ctx: &Runtime, opt: &Options, exe: &Executor) {
		if self.group.is_some() {
			// Ignore
		}
	}

	#[inline]
	fn output_order(&mut self, ctx: &Runtime, opt: &Options, exe: &Executor) {
		if self.order.is_some() {
			// Ignore
		}
	}

	#[inline]
	fn output_start(&mut self, ctx: &Runtime, opt: &Options, exe: &Executor) {
		if let Some(v) = self.start {
			self.results = mem::take(&mut self.results).into_iter().skip(v.0).collect();
		}
	}

	#[inline]
	fn output_limit(&mut self, ctx: &Runtime, opt: &Options, exe: &Executor) {
		if let Some(v) = self.limit {
			self.results = mem::take(&mut self.results).into_iter().take(v.0).collect();
		}
	}

	async fn iterate(
		&mut self,
		ctx: &Runtime,
		opt: &Options,
		exe: &Executor<'_>,
	) -> Result<(), Error> {
		match self.parallel {
			// Run statements in parallel
			true => {
				// Create an unbounded channel
				let (_, mut rx) = mpsc::unbounded_channel();
				// Process all prepared values
				for _ in mem::take(&mut self.readies) {
					todo!();
				}
				// Process all processed values
				while let Some(v) = rx.recv().await {
					self.process(&ctx, opt, exe, None, v).await;
				}
				// Everything processed ok
				Ok(())
			}
			// Run statements sequentially
			false => {
				// Process all prepared values
				for v in mem::take(&mut self.readies) {
					v.iterate(ctx, opt, exe, self).await?;
				}
				// Everything processed ok
				Ok(())
			}
		}
	}

	pub async fn process(
		&mut self,
		ctx: &Runtime,
		opt: &Options,
		exe: &Executor<'_>,
		thg: Option<Thing>,
		val: Value,
	) {
		// Check current context
		if ctx.is_done() {
			return;
		}
		// Setup a new document
		let mut doc = Document::new(thg, val);

		// Process the document
		let res = match self.stmt {
			Statement::Select(_) => doc.select(ctx, opt, exe, &self.stmt).await,
			Statement::Create(_) => doc.create(ctx, opt, exe, &self.stmt).await,
			Statement::Update(_) => doc.update(ctx, opt, exe, &self.stmt).await,
			Statement::Relate(_) => doc.relate(ctx, opt, exe, &self.stmt).await,
			Statement::Delete(_) => doc.delete(ctx, opt, exe, &self.stmt).await,
			Statement::Insert(_) => doc.insert(ctx, opt, exe, &self.stmt).await,
			_ => unreachable!(),
		};

		// Process the result
		match res {
			Err(Error::IgnoreError) => {
				self.run.cancel();
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
		if self.group.is_none() {
			if self.order.is_none() {
				if let Some(l) = self.limit {
					if let Some(s) = self.start {
						if self.results.len() == l.0 + s.0 {
							self.run.cancel()
						}
					} else {
						if self.results.len() == l.0 {
							self.run.cancel()
						}
					}
				}
			}
		}
	}
}
