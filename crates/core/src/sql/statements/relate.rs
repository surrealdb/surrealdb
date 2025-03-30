use crate::ctx::{Context, MutableContext};
use crate::dbs::{Iterable, Iterator, Options, Statement};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::idx::planner::RecordStrategy;
use crate::sql::{Data, FlowResultExt as _, Kind, Output, Timeout, Value, Literal};

use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RelateStatement {
	#[revision(start = 2)]
	pub only: bool,
	pub kind: Kind,
	pub from: Value,
	pub with: Value,
	pub uniq: bool,
	pub data: Option<Data>,
	pub output: Option<Output>,
	pub timeout: Option<Timeout>,
	pub parallel: bool,
}

impl RelateStatement {
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
		// Ensure futures are stored
		let opt = &opt.new_with_futures(false);
		// Check if there is a timeout
		let ctx = match self.timeout.as_ref() {
			Some(timeout) => {
				let mut ctx = MutableContext::new(ctx);
				ctx.add_timeout(*timeout.0)?;
				ctx.freeze()
			}
			None => ctx.clone(),
		};

		// Validate custom types in relationship data
		if let Some(ref data) = self.data {
			if let Kind::Literal(Literal::Object(fields)) = &self.kind {
				match data {
					Data::SetExpression(v) => {
						for (key, _, val) in v {
							if let Some(field_kind) = fields.get(&key.to_string()) {
								val.validate_custom_type(field_kind, &ctx).await?;
							}
						}
					}
					Data::MergeExpression(v) | Data::ReplaceExpression(v) | Data::ContentExpression(v) => {
						if let Value::Object(obj) = v {
							for (key, val) in obj.iter() {
								if let Some(field_kind) = fields.get(key) {
									val.validate_custom_type(field_kind, &ctx).await?;
								}
							}
						}
					}
					_ => {}
				}
			}
		}

		// Loop over the from targets
		let from = {
			let mut out = Vec::new();
			match self.from.compute(stk, &ctx, opt, doc).await.catch_return()? {
				Value::Thing(v) => out.push(v),
				Value::Array(v) => {
					for v in v {
						match v {
							Value::Thing(v) => out.push(v),
							Value::Object(v) => match v.rid() {
								Some(v) => out.push(v),
								_ => {
									return Err(Error::RelateStatementIn {
										value: v.to_string(),
									})
								}
							},
							v => {
								return Err(Error::RelateStatementIn {
									value: v.to_string(),
								})
							}
						}
					}
				}
				Value::Object(v) => match v.rid() {
					Some(v) => out.push(v),
					None => {
						return Err(Error::RelateStatementIn {
							value: v.to_string(),
						})
					}
				},
				v => {
					return Err(Error::RelateStatementIn {
						value: v.to_string(),
					})
				}
			};
			// }
			out
		};
		// Loop over the with targets
		let with = {
			let mut out = Vec::new();
			match self.with.compute(stk, &ctx, opt, doc).await.catch_return()? {
				Value::Thing(v) => out.push(v),
				Value::Array(v) => {
					for v in v {
						match v {
							Value::Thing(v) => out.push(v),
							Value::Object(v) => match v.rid() {
								Some(v) => out.push(v),
								None => {
									return Err(Error::RelateStatementId {
										value: v.to_string(),
									})
								}
							},
							v => {
								return Err(Error::RelateStatementId {
									value: v.to_string(),
								})
							}
						}
					}
				}
				Value::Object(v) => match v.rid() {
					Some(v) => out.push(v),
					None => {
						return Err(Error::RelateStatementId {
							value: v.to_string(),
						})
					}
				},
				v => {
					return Err(Error::RelateStatementId {
						value: v.to_string(),
					})
				}
			};
			out
		};
		//
		for f in from.iter() {
			for w in with.iter() {
				let f = f.clone();
				let w = w.clone();
				match &self.kind {
					Kind::Record(tables) if tables.len() == 1 => {
						let tb = &tables[0];
						match &self.data {
							Some(data) => {
								let id = match data.rid(stk, &ctx, opt).await? {
									Some(id) => id.generate(tb, false)?,
									None => tb.generate(),
								};
								i.ingest(Iterable::Relatable(f, id, w, None))
							}
							None => i.ingest(Iterable::Relatable(f, tb.generate(), w, None)),
						}
					}
					_ => {
						return Err(Error::RelateStatementOut {
							value: self.kind.to_string(),
						})
					}
				};
			}
		}
		// Assign the statement
		let stm = Statement::from(self);
		// Process the statement
		let res = i.output(stk, &ctx, opt, &stm, RecordStrategy::KeysAndValues).await?;
		// Catch statement timeout
		if ctx.is_timedout() {
			return Err(Error::QueryTimedout);
		}
		// Output the results
		match res {
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
}

impl fmt::Display for RelateStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "RELATE")?;
		if self.only {
			f.write_str(" ONLY")?
		}
		write!(f, " {} -> {} -> {}", self.from, self.kind, self.with)?;
		if self.uniq {
			f.write_str(" UNIQUE")?
		}
		if let Some(ref v) = self.data {
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
