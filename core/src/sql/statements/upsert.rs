use crate::ctx::Context;
use crate::dbs::{Iterable, Iterator, Options, Statement};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::{Cond, Data, Id, Output, Table, Thing, Timeout, Value, Values};
use derive::Store;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct UpsertStatement {
	pub only: bool,
	pub what: Values,
	pub data: Option<Data>,
	pub cond: Option<Cond>,
	pub output: Option<Output>,
	pub timeout: Option<Timeout>,
	pub parallel: bool,
}

impl UpsertStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		true
	}
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Valid options?
		opt.valid_for_db()?;
		// Create a new iterator
		let mut i = Iterator::new();
		// Ensure futures are stored
		let opt = &opt.new_with_futures(false).with_projections(false);

		let mut is_bulk = false;
		if let Some(Data::ContentExpression(v))
		| Some(Data::MergeExpression(v))
		| Some(Data::PatchExpression(v)) = &self.data
		{
			if let Ok(tables) = self.get_tables(stk, ctx, opt, doc).await {
				match v.compute(stk, ctx, opt, doc).await? {
					Value::Array(v) => {
						for v in v {
							iterable(&mut i, &tables, &v)?;
						}
						is_bulk = true;
					}
					Value::Object(_) if !matches!(v.rid(), Value::None) => {
						iterable(&mut i, &tables, v)?;
						is_bulk = true;
					}
					_ => {}
				}
			}
		}
		// Assign the statement
		let stm = Statement::from(self);
		if !is_bulk {
			// Loop over the upsert targets
			for w in self.what.0.iter() {
				let v = w.compute(stk, ctx, opt, doc).await?;
				i.prepare(stk, ctx, opt, &stm, v).await.map_err(|e| match e {
					Error::InvalidStatementTarget {
						value: v,
					} => Error::UpsertStatement {
						value: v,
					},
					e => e,
				})?;
			}
		}
		// Output the results
		match i.output(stk, ctx, opt, &stm).await? {
			// This is a single record result
			Value::Array(mut a) if self.only => match a.len() {
				// There was exactly one result
				1 => Ok(a.remove(0)),
				// There were no results
				_ => Err(Error::SingleOnlyOutput),
			},
			// This is standard query result
			v => Ok(v),
		}
	}

	async fn get_tables(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Vec<Table>, Error> {
		let mut accum = vec![];
		for w in self.what.0.iter() {
			if let Value::Table(t) = w.compute(stk, ctx, opt, doc).await? {
				accum.push(t);
			} else {
				return Err(Error::UpsertStatement {
					value: "Targets contains Thing".to_string(),
				});
			}
		}
		Ok(accum)
	}
}

impl fmt::Display for UpsertStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "UPSERT")?;
		if self.only {
			f.write_str(" ONLY")?
		}
		write!(f, " {}", self.what)?;
		if let Some(ref v) = self.data {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.cond {
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

fn gen_id(id: Value, tables: &[Table], selected: &Table) -> Result<Option<Thing>, Error> {
	match id {
		Value::Thing(v) => match v {
			Thing {
				id: Id::Generate(_),
				..
			} => Err(Error::UpsertStatementId {
				value: v.to_string(),
			}),
			Thing {
				tb,
				..
			} if tb != selected.0 => {
				if tables.iter().any(|x| x.0 == tb) {
					Ok(None)
				} else {
					Err(Error::UpsertStatementId {
						value: tb.to_string(),
					})
				}
			}
			v => Ok(Some(v)),
		},
		Value::None => Err(Error::UpsertStatementId {
			value: "not specified".to_string(),
		}),
		v => v.generate(selected, false).map(Into::into),
	}
}

fn iterable(i: &mut Iterator, tables: &[Table], v: &Value) -> Result<(), Error> {
	let id = v.rid();
	for table in tables.iter() {
		let Some(id) = gen_id(id.clone(), tables, table)? else {
			continue;
		};
		i.ingest(Iterable::Mergeable(id, v.clone()));
	}
	Ok(())
}
