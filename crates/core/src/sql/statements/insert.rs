use crate::ctx::{Context, MutableContext};
use crate::dbs::{Iterable, Iterator, Options, Statement};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::paths::IN;
use crate::sql::paths::OUT;
use crate::sql::{Data, Id, Output, Table, Thing, Timeout, Value, Version};
use derive::Store;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct InsertStatement {
	pub into: Option<Value>,
	pub data: Data,
	pub ignore: bool,
	pub update: Option<Data>,
	pub output: Option<Output>,
	pub timeout: Option<Timeout>,
	pub parallel: bool,
	#[revision(start = 2)]
	pub relation: bool,
	#[revision(start = 3)]
	pub version: Option<Version>,
}

impl InsertStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		true
	}
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		// Valid options?
		opt.valid_for_db()?;
		// Create a new iterator
		let mut i = Iterator::new();
		// Propagate the version to the underlying datastore
		let version = match &self.version {
			Some(v) => Some(v.compute(stk, ctx, opt, doc).await?),
			_ => None,
		};
		// Ensure futures are stored
		let opt = &opt.new_with_futures(false).with_version(version);
		// Check if there is a timeout
		let ctx = match self.timeout.as_ref() {
			Some(timeout) => {
				let mut ctx = MutableContext::new(ctx);
				ctx.add_timeout(*timeout.0)?;
				ctx.freeze()
			}
			None => ctx.clone(),
		};
		// Parse the INTO expression
		let into = match &self.into {
			None => None,
			Some(into) => match into.compute(stk, &ctx, opt, doc).await? {
				Value::Table(into) => Some(into),
				v => {
					return Err(Error::InsertStatement {
						value: v.to_string(),
					})
				}
			},
		};
		// Parse the data expression
		match &self.data {
			// Check if this is a traditional statement
			Data::ValuesExpression(v) => {
				for v in v {
					// Create a new empty base object
					let mut o = Value::base();
					// Set each field from the expression
					for (k, v) in v.iter() {
						let v = v.compute(stk, &ctx, opt, None).await?;
						o.set(stk, &ctx, opt, k, v).await?;
					}
					// Specify the new table record id
					let id = gen_id(&o, &into)?;
					// Pass the value to the iterator
					i.ingest(iterable(id, o, self.relation)?)
				}
			}
			// Check if this is a modern statement
			Data::SingleExpression(v) => {
				let v = v.compute(stk, &ctx, opt, doc).await?;
				match v {
					Value::Array(v) => {
						for v in v {
							// Specify the new table record id
							let id = gen_id(&v, &into)?;
							// Pass the value to the iterator
							i.ingest(iterable(id, v, self.relation)?)
						}
					}
					Value::Object(_) => {
						// Specify the new table record id
						let id = gen_id(&v, &into)?;
						// Pass the value to the iterator
						i.ingest(iterable(id, v, self.relation)?)
					}
					v => {
						return Err(Error::InsertStatement {
							value: v.to_string(),
						})
					}
				}
			}
			v => return Err(fail!("Unknown data clause type in INSERT statement: {v:?}")),
		}
		// Assign the statement
		let stm = Statement::from(self);
		// Process the statement
		let res = i.output(stk, &ctx, opt, &stm).await?;
		// Catch statement timeout
		if ctx.is_timedout() {
			return Err(Error::QueryTimedout);
		}
		// Output the results
		Ok(res)
	}
}

impl fmt::Display for InsertStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("INSERT")?;
		if self.relation {
			f.write_str(" RELATION")?
		}
		if self.ignore {
			f.write_str(" IGNORE")?
		}
		if let Some(into) = &self.into {
			write!(f, " INTO {}", into)?;
		}
		write!(f, " {}", self.data)?;
		if let Some(ref v) = self.update {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.output {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.version {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.timeout {
			write!(f, " {v}")?
		}
		if self.parallel {
			f.write_str(" PARALLEL")?
		}
		Ok(())
	}
}

fn iterable(id: Thing, v: Value, relation: bool) -> Result<Iterable, Error> {
	match relation {
		false => Ok(Iterable::Mergeable(id, v)),
		true => {
			let f = match v.pick(&*IN) {
				Value::Thing(v) => v,
				v => {
					return Err(Error::InsertStatementIn {
						value: v.to_string(),
					})
				}
			};
			let w = match v.pick(&*OUT) {
				Value::Thing(v) => v,
				v => {
					return Err(Error::InsertStatementOut {
						value: v.to_string(),
					})
				}
			};
			Ok(Iterable::Relatable(f, id, w, Some(v)))
		}
	}
}

fn gen_id(v: &Value, into: &Option<Table>) -> Result<Thing, Error> {
	match into {
		Some(into) => v.rid().generate(into, true),
		None => match v.rid() {
			Value::Thing(v) => match v {
				Thing {
					id: Id::Generate(_),
					..
				} => Err(Error::InsertStatementId {
					value: v.to_string(),
				}),
				v => Ok(v),
			},
			v => Err(Error::InsertStatementId {
				value: v.to_string(),
			}),
		},
	}
}
