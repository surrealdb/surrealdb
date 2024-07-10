use crate::{
	ctx::Context,
	dbs::Options,
	doc::CursorDoc,
	err::Error,
	iam::Action,
	sql::{ident::Ident, value::Value, Permission},
};
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::{fmt, ops::Deref, str};

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Param";

/// https://surrealdb.com/docs/surrealdb/surrealql/parameters#reserved-variable-names
const RESERVED: [&str; 11] = [
	"before", "after", "auth", "event", "input", "parent", "this", "scope", "session", "token",
	"value",
];

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Param")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Param(pub Ident);

impl From<Ident> for Param {
	fn from(v: Ident) -> Self {
		Self(v)
	}
}

impl From<String> for Param {
	fn from(v: String) -> Self {
		Self(v.into())
	}
}

impl From<&str> for Param {
	fn from(v: &str) -> Self {
		Self(v.into())
	}
}

impl Deref for Param {
	type Target = Ident;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Param {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Find the variable by name
		match self.as_str() {
			// This is a special param
			"this" | "self" => match doc {
				// The base document exists
				Some(v) => v.doc.compute(stk, ctx, opt, doc).await,
				// The base document does not exist
				None => Ok(Value::None),
			},
			// This is a normal param
			v => match ctx.value(v) {
				// The param has been set locally
				Some(v) => v.compute(stk, ctx, opt, doc).await,
				// The param has not been set locally
				None => {
					let val = {
						// Claim transaction
						let mut run = ctx.tx_lock().await;
						// Get the param definition
						run.get_and_cache_db_param(opt.ns()?, opt.db()?, v).await
					};
					// Check if the param has been set globally
					match val {
						// The param has been set globally
						Ok(val) => {
							// Check permissions
							if opt.check_perms(Action::View)? {
								match &val.permissions {
									Permission::Full => (),
									Permission::None => {
										return Err(Error::ParamPermissions {
											name: v.to_owned(),
										})
									}
									Permission::Specific(e) => {
										// Disable permissions
										let opt = &opt.new_with_perms(false);
										// Process the PERMISSION clause
										if !e.compute(stk, ctx, opt, doc).await?.is_truthy() {
											return Err(Error::ParamPermissions {
												name: v.to_owned(),
											});
										}
									}
								}
							}
							// Return the computed value
							val.value.compute(stk, ctx, opt, doc).await
						}
						// The param has not been set globally
						Err(_) => Ok(Value::None),
					}
				}
			},
		}
	}

	/// Evaluate a param if it is not reserved
	pub(crate) async fn partially_compute(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Check if the param is reserved
		if RESERVED.contains(&self.as_str().to_lowercase().as_str()) {
			// Return the param as a value
			Ok(Value::Param(self.clone()))
		} else {
			// Evaluate the param
			self.compute(stk, ctx, opt, doc).await
		}
	}
}

impl fmt::Display for Param {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "${}", &self.0 .0)
	}
}

#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod test {
	use crate::dbs::Options;
	use crate::kvs::{Datastore, LockType, TransactionType};
	use crate::sql::Param;

	#[tokio::test]
	async fn params_evaluate_except_reserved_in_partial_compute() {
		use crate::ctx::Context;
		use crate::sql::Value;
		use reblessive::TreeStack;

		let ds = Datastore::new("memory").await.unwrap();

		struct Case {
			param: Param,
			expected: Value,
			val: Option<(&'static str, Value)>,
		};
		let cases = map! {
			"normal params get evaluated" => Case{param: Param::from("test"),
				expected: Value::Number(1.0.into()),
				val: Some(("test", Value::Number(1.0.into())))},
			"normal params missing context" => Case{param: Param::from("test"),
				expected: Value::None,
				val: None},
			"reserved - this" => Case{param: "this".into(),
				expected: Value::Param(Param::from("this")),
				val: Some(("this", Value::Number(1.0.into())))},
			"reserved - before" => Case{param: "before".into(),
				expected: Value::Param(Param::from("before")),
				val: Some(("before", Value::Number(1.0.into())))},
			"reserved - after" => Case{param: Param::from("after"),
				expected: Value::Param(Param::from("after")),
				val: Some(("after", Value::Number(1.0.into())))},
			"reserved - auth" => Case{param: Param::from("auth"),
				expected: Value::Param(Param::from("auth")),
				val: Some(("auth", Value::Number(1.0.into())))},
			"reserved - event" => Case{param: Param::from("event"),
				expected: Value::Param(Param::from("event")),
				val: Some(("event", Value::Number(1.0.into())))},
			"reserved - input" => Case{param: Param::from("input"),
				expected: Value::Param(Param::from("input")),
				val: Some(("input", Value::Number(1.0.into())))},
			"reserved - parent" => Case{param: Param::from("parent"),
				expected: Value::Param(Param::from("parent")),
				val: Some(("parent", Value::Number(1.0.into())))},
			"reserved - this" => Case{param: Param::from("this"),
				expected: Value::Param(Param::from("this")),
				val: Some(("this", Value::Number(1.0.into())))},
			"reserved - scope" => Case{param: Param::from("scope"),
				expected: Value::Param(Param::from("scope")),
				val: Some(("scope", Value::Number(1.0.into())))},
			"reserved - session" => Case{param: Param::from("session"),
				expected: Value::Param(Param::from("session")),
				val: Some(("session", Value::Number(1.0.into())))},
			"reserved - token" => Case{param: Param::from("token"),
				expected: Value::Param(Param::from("token")),
				val: Some(("token", Value::Number(1.0.into())))},
			"reserved - value" => Case{param: Param::from("value"),
				expected: Value::Param(Param::from("value")),
				val: Some(("value", Value::Number(1.0.into())))},
		};

		let tx =
			ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap().enclose();
		let mut stack = TreeStack::new();
		for (name, case) in cases {
			let mut ctx = Context::default().set_transaction(tx.clone());
			let opt = Options::new().with_ns(Some("test".into())).with_db(Some("test".into()));
			if let Some((k, v)) = case.val {
				ctx.add_value(k, v);
			}
			let param = stack
				.enter(|stk| async {
					case.param.partially_compute(stk, &ctx, &opt, None).await.unwrap()
				})
				.finish()
				.await;
			assert_eq!(param, case.expected, "{}", name);
		}
		tx.lock().await.commit().await.unwrap();
	}
}
