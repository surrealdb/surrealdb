use crate::dbs::Executor;
use crate::dbs::Runtime;
use crate::err::Error;
use crate::sql::array::Array;
use crate::sql::cond::Cond;
use crate::sql::data::Data;
use crate::sql::field::Fields;
use crate::sql::group::Groups;
use crate::sql::limit::Limit;
use crate::sql::model::Model;
use crate::sql::object::Object;
use crate::sql::order::Orders;
use crate::sql::split::Splits;
use crate::sql::start::Start;
use crate::sql::table::Table;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use crate::sql::version::Version;
use std::mem;
use xid;

#[derive(Default)]
pub struct Iterator<'a> {
	ok: bool,
	// Iterator options
	pub into: Option<&'a Table>,
	pub expr: Option<&'a Fields>,
	pub data: Option<&'a Data>,
	pub cond: Option<&'a Cond>,
	pub split: Option<&'a Splits>,
	pub group: Option<&'a Groups>,
	pub order: Option<&'a Orders>,
	pub limit: Option<&'a Limit>,
	pub start: Option<&'a Start>,
	pub version: Option<&'a Version>,
	// Iterator runtime error
	error: Option<Error>,
	// Iterator output results
	results: Vec<Value>,
}

impl<'a> Iterator<'a> {
	pub fn new() -> Iterator<'a> {
		Iterator {
			ok: true,
			..Iterator::default()
		}
	}

	fn check(&self, ctx: &Runtime) -> bool {
		self.ok && ctx.is_ok()
	}

	pub fn process_table(&mut self, ctx: &Runtime, exe: &mut Executor, val: Table) {
		// Check basic permissions
		self.process_perms(ctx, exe);
		// Loop over all table keys
		// - Process record
	}

	pub fn process_thing(&mut self, ctx: &Runtime, exe: &mut Executor, val: Thing) {
		// Check basic permissions
		self.process_perms(ctx, exe);
		// Check current context
		if self.check(ctx) {
			// Process record
			// self.process(ctx, exe);
		}
	}

	pub fn process_model(&mut self, ctx: &Runtime, exe: &mut Executor, val: Model) {
		// Check basic permissions
		self.process_perms(ctx, exe);
		// Process count based model
		if val.count.is_some() {
			let c = val.count.unwrap();
			for _ in 0..c {
				// Check current context
				if self.check(ctx) {
					// Process record
					self.process(
						ctx,
						exe,
						Value::from(Thing {
							tb: val.table.to_string(),
							id: xid::new().to_string(),
						}),
					);
				}
			}
		}
		// Process range based model
		if val.range.is_some() {
			let r = val.range.unwrap();
			for x in r.0..r.1 {
				// Check current context
				if self.check(ctx) {
					// Process record
					self.process(
						ctx,
						exe,
						Value::from(Thing {
							tb: val.table.to_string(),
							id: x.to_string(),
						}),
					);
				}
			}
		}
	}

	pub fn process_array(&mut self, ctx: &Runtime, exe: &mut Executor, val: Array) {
		// Check basic permissions
		self.process_perms(ctx, exe);
		// Loop over query result array
		for v in val.value.into_iter() {
			// Check current context
			if self.check(ctx) {
				// Process item
				match v {
					Value::Thing(v) => self.process_thing(ctx, exe, v),
					Value::Object(v) => self.process_object(ctx, exe, v),
					v => self.process(ctx, exe, v),
				}
			}
		}
	}

	pub fn process_object(&mut self, ctx: &Runtime, exe: &mut Executor, val: Object) {
		// Check basic permissions
		self.process_perms(ctx, exe);
		// Check current context
		if self.check(ctx) {
			// Loop over query result array
			self.process(ctx, exe, val.into())
		}
	}

	pub fn process_value(&mut self, ctx: &Runtime, exe: &mut Executor, val: Value) {
		// Check basic permissions
		self.process_perms(ctx, exe);
		// Loop over query result array
		// self.process(ctx, exe, val)
		// - IF value is THING then process record
		// - IF value.id is THING then process record
		// - ELSE process as object
		match val {
			Value::Thing(v) => self.process_thing(ctx, exe, v),
			Value::Object(v) => self.process_object(ctx, exe, v),
			v => self.process(ctx, exe, v),
		}
	}

	fn process(&mut self, ctx: &Runtime, exe: &mut Executor, val: Value) {
		// 1. Setup a new document
		// 2. Check for any errors
		// 3. Append the result

		let res = Some(val);

		// If an error was received from the
		// worker, then set the error if no
		// previous iterator error has occured.

		if self.check(ctx) == false {
			return;
		}

		// Otherwise add the received result
		// to the iterator result slice so
		// that it is ready for processing.

		if let Some(r) = res {
			self.results.push(r);
		}

		// The statement does not have a limit
		// expression specified, so therefore
		// we need to load all data before
		// stopping the iterator.

		if self.limit.is_none() {
			return;
		}

		// If the statement specified a GROUP
		// BY expression, then we need to load
		// all data from all sources before
		// stopping the iterator.

		if self.group.is_some() {
			return;
		}

		// If the statement specified an ORDER
		// BY expression, then we need to load
		// all data from all sources before
		// stopping the iterator.

		if self.order.is_some() {
			return;
		}

		// Otherwise we can stop the iterator
		// early, if we have the necessary
		// number of records specified in the
		// query statement.

		if let Some(l) = self.limit {
			if let Some(s) = self.start {
				if self.results.len() == l.0 + s.0 {
					self.ok = false
				}
			} else {
				if self.results.len() == l.0 {
					self.ok = false
				}
			}
		}
	}

	fn process_perms(&self, ctx: &Runtime, exe: &Executor) {}

	fn process_split(&mut self, ctx: &Runtime, exe: &Executor) {
		if self.split.is_some() {
			// Ignore
		}
	}

	fn process_group(&mut self, ctx: &Runtime, exe: &Executor) {
		if self.group.is_some() {
			// Ignore
		}
	}

	fn process_order(&mut self, ctx: &Runtime, exe: &Executor) {
		if self.order.is_some() {
			// Ignore
		}
	}

	fn process_start(&mut self, ctx: &Runtime, exe: &Executor) {
		if let Some(v) = self.start {
			let s = v.0 as usize;
			self.results = mem::take(&mut self.results).into_iter().skip(s).collect();
		}
	}

	fn process_limit(&mut self, ctx: &Runtime, exe: &Executor) {
		if let Some(v) = self.limit {
			let l = v.0 as usize;
			self.results = mem::take(&mut self.results).into_iter().take(l).collect();
		}
	}

	pub fn output(&mut self, ctx: &Runtime, exe: &Executor) -> Result<Value, Error> {
		// Return any errors
		if let Some(e) = self.error.take() {
			return Err(e);
		}
		// Process SPLIT clause
		self.process_split(ctx, exe);
		// Process GROUP clause
		self.process_group(ctx, exe);
		// Process ORDER clause
		self.process_order(ctx, exe);
		// Process START clause
		self.process_start(ctx, exe);
		// Process LIMIT clause
		self.process_limit(ctx, exe);
		// Output the results
		Ok(mem::take(&mut self.results).into())
	}
}
