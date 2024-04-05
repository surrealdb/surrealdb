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
#[revisioned(revision = 2)]
#[non_exhaustive]
pub struct InsertStatement {
	pub into: Value,
	pub data: Data,
	pub ignore: bool,
	pub update: Option<Data>,
	pub output: Option<Output>,
	pub timeout: Option<Timeout>,
	pub parallel: bool,
	#[revision(start = 2)]
	pub relation: bool,
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
		match (self.relation, self.into.compute(ctx, opt, txn, doc).await?) {
			(false, Value::Table(into)) => match &self.data {
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
				v => {
					return Err(Error::InsertStatement {
						value: v.to_string(),
					})
				}
			},
			(true, val) => {
				println!("Got to (true, val)");
				let into = match val {
					Value::None => None,
					Value::Table(into) => Some(into),
					_ => {
						return Err(Error::InsertStatement {
							value: val.to_string(),
						})
					}
				};

				match &self.data {
					Data::SingleExpression(Value::Array(v)) => {
						for r in v.iter() {
							let Value::Object(o) = r else {
								return Err(Error::InsertStatement {
									value: r.to_string(),
								});
							};
							let Some(Value::Thing(in_id)) = o.get("in") else {
								return Err(Error::Thrown("No in specified".to_string()));
							};
							let Some(Value::Thing(out_id)) = o.get("out") else {
								return Err(Error::Thrown("No out specified".to_string()));
							};
							let id = match (&into, o.get("id")) {
								(_, Some(Value::Thing(id))) => id.clone(),
								(Some(tb), _) => tb.generate(),
								(_, _) => {
									return Err(Error::Thrown(
										"No id or table specified".to_string(),
									))
								}
							};
							// i.ingest(Iterable::Mergeable(id.clone(), Value::Object(o.to_owned())));
							println!("\nIterable::Relate({in_id}, {id}, {out_id})\n");
							i.ingest(Iterable::Relatable(in_id.clone(), id, out_id.clone()))
						}
					}
					e => {
						return Err(Error::InsertStatement {
							value: e.to_string(),
						})
					}
				}
			}
			// not relation and not table is error
			(false, v) => {
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
