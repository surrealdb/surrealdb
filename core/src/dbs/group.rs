use crate::ctx::Context;
use crate::dbs::store::StoreCollector;
use crate::dbs::{Options, Statement, Transaction};
use crate::err::Error;
use crate::sql::value::Value;
use crate::sql::{Array, Field};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::mem;

#[derive(Default)]
pub(super) struct GroupsCollector {
	grp: BTreeMap<Array, Array>,
}
impl GroupsCollector {
	pub(super) fn push(&mut self, stm: &Statement<'_>, obj: Value) {
		if let Some(groups) = stm.group() {
			// Create a new column set
			let mut arr = Array::with_capacity(groups.len());
			// Loop over each group clause
			for group in groups.iter() {
				// Get the value at the path
				let val = obj.pick(group);
				// Set the value at the path
				arr.push(val);
			}
			// Add to grouped collection
			match self.grp.get_mut(&arr) {
				Some(v) => v.push(obj),
				None => {
					self.grp.insert(arr, Array::from(obj));
				}
			}
		}
	}

	pub(super) fn len(&self) -> usize {
		self.grp.len()
	}

	pub(super) async fn output(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
	) -> Result<StoreCollector, Error> {
		let mut results = StoreCollector::default();
		if let Some(fields) = stm.expr() {
			let grp = mem::take(&mut self.grp);
			// Loop over each grouped collection
			for (_, vals) in grp {
				// Create a new value
				let mut obj = Value::base();
				// Save the collected values
				let vals = Value::from(vals);
				// Loop over each group clause
				for field in fields.other() {
					// Process the field
					if let Field::Single {
						expr,
						alias,
					} = field
					{
						let idiom = alias
							.as_ref()
							.map(Cow::Borrowed)
							.unwrap_or_else(|| Cow::Owned(expr.to_idiom()));
						match expr {
							Value::Function(f) if f.is_aggregate() => {
								let x = vals.all().get(ctx, opt, txn, None, idiom.as_ref()).await?;
								let x = f.aggregate(x).compute(ctx, opt, txn, None).await?;
								obj.set(ctx, opt, txn, idiom.as_ref(), x).await?;
							}
							_ => {
								let x = vals.first();
								let x = if let Some(alias) = alias {
									let cur = (&x).into();
									alias.compute(ctx, opt, txn, Some(&cur)).await?
								} else {
									let cur = (&x).into();
									expr.compute(ctx, opt, txn, Some(&cur)).await?
								};
								obj.set(ctx, opt, txn, idiom.as_ref(), x).await?;
							}
						}
					}
				}
				// Add the object to the results
				results.push(obj);
			}
		}
		Ok(results)
	}
}
