use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::statements::info::InfoStructure;
use crate::sql::{Base, Ident, Object, Strand, Value};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineNamespaceStatement {
	pub id: Option<u32>,
	pub name: Ident,
	pub comment: Option<Strand>,
	#[revision(start = 2)]
	pub if_not_exists: bool,
}

impl DefineNamespaceStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Namespace, &Base::Root)?;
		// Process the statement
		let key = crate::key::root::ns::new(&self.name);
		// Claim transaction
		let mut run = ctx.tx_lock().await;
		// Clear the cache
		run.clear_cache();
		// Check if namespace already exists
		if run.get_ns(&self.name).await.is_ok() {
			if self.if_not_exists {
				return Ok(Value::None);
			} else {
				return Err(Error::NsAlreadyExists {
					value: self.name.to_string(),
				});
			}
		}
		if self.id.is_none() {
			// Set the id
			let ns = DefineNamespaceStatement {
				id: Some(run.get_next_ns_id().await?),
				if_not_exists: false,
				..self.clone()
			};
			run.set(key, ns).await?;
		} else {
			run.set(
				key,
				DefineNamespaceStatement {
					if_not_exists: false,
					..self.clone()
				},
			)
			.await?;
		}
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineNamespaceStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE NAMESPACE")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		write!(f, " {}", self.name)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		Ok(())
	}
}

impl InfoStructure for DefineNamespaceStatement {
	fn structure(self) -> Value {
		let Self {
			name,
			comment,
			..
		} = self;
		let mut acc = Object::default();

		acc.insert("name".to_string(), name.structure());

		if let Some(comment) = comment {
			acc.insert("comment".to_string(), comment.into());
		}

		Value::Object(acc)
	}
}
