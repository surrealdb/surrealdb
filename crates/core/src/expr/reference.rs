use std::fmt;

use anyhow::{Result, bail};
use revision::revisioned;
use serde::{Deserialize, Serialize};

use super::statements::info::InfoStructure;
use super::{Idiom, Value};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::{Expr, Ident};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Reference")]
pub struct Reference {
	pub on_delete: ReferenceDeleteStrategy,
}

impl fmt::Display for Reference {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "ON DELETE {}", &self.on_delete)
	}
}

impl InfoStructure for Reference {
	fn structure(self) -> Value {
		map! {
			"on_delete" => self.on_delete.structure(),
		}
		.into()
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::ReferenceDeleteStrategy")]
pub enum ReferenceDeleteStrategy {
	Reject,
	Ignore,
	Cascade,
	Unset,
	Custom(Expr),
}

impl fmt::Display for ReferenceDeleteStrategy {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			ReferenceDeleteStrategy::Reject => write!(f, "REJECT"),
			ReferenceDeleteStrategy::Ignore => write!(f, "IGNORE"),
			ReferenceDeleteStrategy::Cascade => write!(f, "CASCADE"),
			ReferenceDeleteStrategy::Unset => write!(f, "UNSET"),
			ReferenceDeleteStrategy::Custom(v) => write!(f, "THEN {}", v),
		}
	}
}

impl InfoStructure for ReferenceDeleteStrategy {
	fn structure(self) -> Value {
		self.to_string().into()
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Refs")]
pub struct Refs(pub Vec<(Option<Ident>, Option<Idiom>)>);

impl Refs {
	// remove once reintroducing refences
	#[allow(dead_code)]
	pub(crate) async fn compute(
		&self,
		_ctx: &Context,
		_opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value> {
		bail!(Error::Unimplemented("Refs::compute not yet implemented".to_owned()))
		/*
		if !ctx.get_capabilities().allows_experimental(&ExperimentalTarget::RecordReferences) {
			return Ok(Value::Array(Default::default()));
		}

		// Collect an array of references
		let arr = match doc {
			// Check if the current document has specified an ID
			Some(doc) => {
				// Obtain a record id from the document
				let rid = match &doc.rid {
					Some(id) => id.as_ref().to_owned(),
					None => match &doc.doc.rid() {
						Value::RecordId(id) => id.to_owned(),
						_ => bail!(Error::InvalidRefsContext),
					},
				};

				let mut ids: Vec<RecordId> = Vec::new();

				// Map over all input pairs
				for (ft, ff) in self.0.iter() {
					// Collect the references
					ids.append(&mut rid.refs(ctx, opt, ft.as_ref(), ff.as_ref()).await?);
				}

				// Convert the references into values
				Array(ids.into_iter().map(Value::RecordId).collect::<Vec<_>>())
			}
			None => bail!(Error::InvalidRefsContext),
		};

		Ok(Value::Array(arr.uniq()))
		*/
	}
}

impl fmt::Display for Refs {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "[]")
	}
}
