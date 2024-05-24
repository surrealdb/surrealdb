use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::statements::info::InfoStructure;
use crate::sql::statements::DefineTableStatement;
use crate::sql::{
	fmt::is_pretty, fmt::pretty_indent, Base, Ident, Idiom, Kind, Permissions, Strand, Value,
};
use crate::sql::{Object, Part};
use crate::sql::{Relation, TableType};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Write};

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineFieldStatement {
	pub name: Idiom,
	pub what: Ident,
	pub flex: bool,
	pub kind: Option<Kind>,
	#[revision(start = 2)]
	pub readonly: bool,
	pub value: Option<Value>,
	pub assert: Option<Value>,
	pub default: Option<Value>,
	pub permissions: Permissions,
	pub comment: Option<Strand>,
	#[revision(start = 3)]
	pub if_not_exists: bool,
}

impl DefineFieldStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Field, &Base::Db)?;
		// Claim transaction
		let mut run = ctx.transaction()?.lock().await;
		// Clear the cache
		run.clear_cache();
		// Check if field already exists
		let fd = self.name.to_string();
		if self.if_not_exists && run.get_tb_field(opt.ns(), opt.db(), &self.what, &fd).await.is_ok()
		{
			return Err(Error::FdAlreadyExists {
				value: fd,
			});
		}
		// Process the statement
		run.add_ns(opt.ns(), opt.strict).await?;
		run.add_db(opt.ns(), opt.db(), opt.strict).await?;

		let tb = run.add_tb(opt.ns(), opt.db(), &self.what, opt.strict).await?;
		let key = crate::key::table::fd::new(opt.ns(), opt.db(), &self.what, &fd);
		run.set(
			key,
			DefineFieldStatement {
				if_not_exists: false,
				..self.clone()
			},
		)
		.await?;

		// find existing field definitions.
		let fields = run.all_tb_fields(opt.ns(), opt.db(), &self.what).await.ok();

		// Process possible recursive_definitions.
		if let Some(mut cur_kind) = self.kind.as_ref().and_then(|x| x.inner_kind()) {
			let mut name = self.name.clone();
			loop {
				let new_kind = cur_kind.inner_kind();
				name.0.push(Part::All);

				let fd = name.to_string();
				let key = crate::key::table::fd::new(opt.ns(), opt.db(), &self.what, &fd);
				run.add_ns(opt.ns(), opt.strict).await?;
				run.add_db(opt.ns(), opt.db(), opt.strict).await?;

				// merge the new definition with possible existing definitions.
				let statement = if let Some(existing) =
					fields.as_ref().and_then(|x| x.iter().find(|x| x.name == name))
				{
					DefineFieldStatement {
						kind: Some(cur_kind),
						if_not_exists: false,
						..existing.clone()
					}
				} else {
					DefineFieldStatement {
						name: name.clone(),
						what: self.what.clone(),
						flex: self.flex,
						kind: Some(cur_kind),
						..Default::default()
					}
				};

				run.set(key, statement).await?;

				if let Some(new_kind) = new_kind {
					cur_kind = new_kind;
				} else {
					break;
				}
			}
		}

		let new_tb = match (fd.as_str(), tb.kind.clone(), self.kind.clone()) {
			("in", TableType::Relation(rel), Some(dk)) => {
				if !matches!(dk, Kind::Record(_)) {
					return Err(Error::Thrown("in field on a relation must be a record".into()));
				};
				if rel.from.as_ref() != Some(&dk) {
					Some(DefineTableStatement {
						kind: TableType::Relation(Relation {
							from: Some(dk),
							..rel
						}),
						..tb
					})
				} else {
					None
				}
			}
			("out", TableType::Relation(rel), Some(dk)) => {
				if !matches!(dk, Kind::Record(_)) {
					return Err(Error::Thrown("out field on a relation must be a record".into()));
				};
				if rel.to.as_ref() != Some(&dk) {
					Some(DefineTableStatement {
						kind: TableType::Relation(Relation {
							to: Some(dk),
							..rel
						}),
						..tb
					})
				} else {
					None
				}
			}
			_ => None,
		};
		if let Some(tb) = new_tb {
			let key = crate::key::database::tb::new(opt.ns(), opt.db(), &self.what);
			run.set(key, &tb).await?;
			let key = crate::key::table::ft::prefix(opt.ns(), opt.db(), &self.what);
			run.clr(key).await?;
		}

		// Clear the cache
		let key = crate::key::table::fd::prefix(opt.ns(), opt.db(), &self.what);
		run.clr(key).await?;
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineFieldStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE FIELD")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		write!(f, " {} ON {}", self.name, self.what)?;
		if self.flex {
			write!(f, " FLEXIBLE")?
		}
		if let Some(ref v) = self.kind {
			write!(f, " TYPE {v}")?
		}
		if let Some(ref v) = self.default {
			write!(f, " DEFAULT {v}")?
		}
		if self.readonly {
			write!(f, " READONLY")?
		}
		if let Some(ref v) = self.value {
			write!(f, " VALUE {v}")?
		}
		if let Some(ref v) = self.assert {
			write!(f, " ASSERT {v}")?
		}
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		let _indent = if is_pretty() {
			Some(pretty_indent())
		} else {
			f.write_char(' ')?;
			None
		};
		write!(f, "{}", self.permissions)?;
		Ok(())
	}
}

impl InfoStructure for DefineFieldStatement {
	fn structure(self) -> Value {
		let Self {
			name,
			what,
			flex,
			kind,
			readonly,
			value,
			assert,
			default,
			permissions,
			comment,
			..
		} = self;
		let mut acc = Object::default();

		acc.insert("name".to_string(), name.structure());

		acc.insert("what".to_string(), what.structure());

		acc.insert("flex".to_string(), flex.into());

		if let Some(kind) = kind {
			acc.insert("kind".to_string(), kind.structure());
		}

		acc.insert("readonly".to_string(), readonly.into());

		if let Some(value) = value {
			acc.insert("value".to_string(), value.structure());
		}

		if let Some(assert) = assert {
			acc.insert("assert".to_string(), assert.structure());
		}

		if let Some(default) = default {
			acc.insert("default".to_string(), default.structure());
		}

		acc.insert("permissions".to_string(), permissions.structure());

		if let Some(comment) = comment {
			acc.insert("comment".to_string(), comment.into());
		}

		Value::Object(acc)
	}
}
