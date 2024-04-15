use crate::ctx::Context;
use crate::dbs::{Iterable, Iterator, Options, Statement, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::{Data, Output, Timeout, Value};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[revisioned(revision = 1)]
#[non_exhaustive]
pub struct InsertStatement {
	pub into: Value,
	pub data: Data,
	pub ignore: bool,
	pub update: Option<Data>,
	pub output: Option<Output>,
	pub timeout: Option<Timeout>,
	pub parallel: bool,
}

impl InsertStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		true
	}
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Valid options?
		opt.valid_for_db()?;
		// Create a new iterator
		let mut i = Iterator::new();
		// Ensure futures are stored
		let opt = &opt.new_with_futures(false).with_projections(false);
		// Parse the expression
		match self.into.compute(ctx, opt, txn, doc).await? {
			Value::Table(into) => match &self.data {
				// Check if this is a traditional statement
				Data::ValuesExpression(v) => {
					for v in v {
						// Create a new empty base object
						let mut o = Value::base();
						// Set each field from the expression
						for (k, v) in v.iter() {
							let v = v.compute(ctx, opt, txn, None).await?;
							o.set(ctx, opt, txn, k, v).await?;
						}
						// Specify the new table record id
						let id = o.rid().generate(&into, true)?;
						// Pass the mergeable to the iterator
						i.ingest(Iterable::Mergeable(id, o));
					}
				}
				// Check if this is a modern statement
				Data::SingleExpression(v) => {
					let v = v.compute(ctx, opt, txn, doc).await?;
					match v {
						Value::Array(v) => {
							for v in v {
								// Specify the new table record id
								let id = v.rid().generate(&into, true)?;
								// Pass the mergeable to the iterator
								i.ingest(Iterable::Mergeable(id, v));
							}
						}
						Value::Object(_) => {
							// Specify the new table record id
							let id = v.rid().generate(&into, true)?;
							// Pass the mergeable to the iterator
							i.ingest(Iterable::Mergeable(id, v));
						}
						v => {
							return Err(Error::InsertStatement {
								value: v.to_string(),
							})
						}
					}
				}
				_ => unreachable!(),
			},
			v => {
				return Err(Error::InsertStatement {
					value: v.to_string(),
				})
			}
		}
		// Assign the statement
		let stm = Statement::from(self);
		// Output the results
		i.output(ctx, opt, txn, &stm).await
	}
}

impl fmt::Display for InsertStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("INSERT")?;
		if self.ignore {
			f.write_str(" IGNORE")?
		}
		write!(f, " INTO {} {}", self.into, self.data)?;
		if let Some(ref v) = self.update {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.output {
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
