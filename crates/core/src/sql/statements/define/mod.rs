mod access;
mod analyzer;
mod api;
pub mod config;
mod database;
mod deprecated;
mod event;
mod field;
mod function;
mod index;
mod model;
mod namespace;
mod param;
mod table;
mod user;

pub use access::DefineAccessStatement;
pub use analyzer::DefineAnalyzerStatement;
pub use api::DefineApiStatement;
pub use config::DefineConfigStatement;
pub use database::DefineDatabaseStatement;
pub use event::DefineEventStatement;
pub use field::DefineFieldStatement;
pub use function::DefineFunctionStatement;
pub use index::DefineIndexStatement;
pub use model::DefineModelStatement;
pub use namespace::DefineNamespaceStatement;
pub use param::DefineParamStatement;
pub use table::DefineTableStatement;
pub use user::DefineUserStatement;

pub use deprecated::scope::DefineScopeStatement;
pub use deprecated::token::DefineTokenStatement;

pub use api::ApiAction;
pub use api::ApiDefinition;
pub use api::FindApi;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::statements::info::InfoStructure;
use crate::sql::{Ident, Kind, Strand, Value};
use crate::dbs::type_def::TypeDefinition;

use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum DefineStatement {
	Namespace(DefineNamespaceStatement),
	Database(DefineDatabaseStatement),
	Function(DefineFunctionStatement),
	Analyzer(DefineAnalyzerStatement),
	#[revision(
		end = 2,
		convert_fn = "convert_token_to_access",
		fields_name = "DefineTokenStatementFields"
	)]
	Token(DefineTokenStatement),
	#[revision(
		end = 2,
		convert_fn = "convert_scope_to_access",
		fields_name = "DefineScopeStatementFields"
	)]
	Scope(DefineScopeStatement),
	Param(DefineParamStatement),
	Table(DefineTableStatement),
	Event(DefineEventStatement),
	Field(DefineFieldStatement),
	Index(DefineIndexStatement),
	User(DefineUserStatement),
	Model(DefineModelStatement),
	#[revision(start = 2)]
	Access(DefineAccessStatement),
	Config(DefineConfigStatement),
	#[revision(start = 3)]
	Api(DefineApiStatement),
	#[revision(start = 3)]
	Type(DefineTypeStatement),
}

// Revision implementations
impl DefineStatement {
	fn convert_token_to_access(
		fields: DefineTokenStatementFields,
		_revision: u16,
	) -> Result<Self, revision::Error> {
		Ok(DefineStatement::Access(fields.0.into()))
	}

	fn convert_scope_to_access(
		fields: DefineScopeStatementFields,
		_revision: u16,
	) -> Result<Self, revision::Error> {
		Ok(DefineStatement::Access(fields.0.into()))
	}
}

impl DefineStatement {
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
		match self {
			Self::Namespace(ref v) => v.compute(ctx, opt, doc).await,
			Self::Database(ref v) => v.compute(ctx, opt, doc).await,
			Self::Function(ref v) => v.compute(ctx, opt, doc).await,
			Self::Param(ref v) => v.compute(stk, ctx, opt, doc).await,
			Self::Table(ref v) => v.compute(stk, ctx, opt, doc).await,
			Self::Event(ref v) => v.compute(ctx, opt, doc).await,
			Self::Field(ref v) => v.compute(ctx, opt, doc).await,
			Self::Index(ref v) => v.compute(stk, ctx, opt, doc).await,
			Self::Analyzer(ref v) => v.compute(ctx, opt, doc).await,
			Self::User(ref v) => v.compute(ctx, opt, doc).await,
			Self::Model(ref v) => v.compute(ctx, opt, doc).await,
			Self::Access(ref v) => v.compute(ctx, opt, doc).await,
			Self::Config(ref v) => v.compute(ctx, opt, doc).await,
			Self::Api(ref v) => v.compute(stk, ctx, opt, doc).await,
			Self::Type(ref v) => v.compute(ctx, opt, doc).await,
		}
	}
}

impl Display for DefineStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Namespace(v) => Display::fmt(v, f),
			Self::Database(v) => Display::fmt(v, f),
			Self::Function(v) => Display::fmt(v, f),
			Self::User(v) => Display::fmt(v, f),
			Self::Param(v) => Display::fmt(v, f),
			Self::Table(v) => Display::fmt(v, f),
			Self::Event(v) => Display::fmt(v, f),
			Self::Field(v) => Display::fmt(v, f),
			Self::Index(v) => Display::fmt(v, f),
			Self::Analyzer(v) => Display::fmt(v, f),
			Self::Model(v) => Display::fmt(v, f),
			Self::Access(v) => Display::fmt(v, f),
			Self::Config(v) => Display::fmt(v, f),
			Self::Api(v) => Display::fmt(v, f),
			Self::Type(v) => Display::fmt(v, f),
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::Ident;

	#[test]
	fn check_define_serialize() {
		let stm = DefineStatement::Namespace(DefineNamespaceStatement {
			name: Ident::from("test"),
			..Default::default()
		});
		let enc: Vec<u8> = revision::to_vec(&stm).unwrap();
		assert_eq!(13, enc.len());
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineTypeStatement {
	pub name: Ident,
	pub kind: Kind,
	pub comment: Option<Strand>,
	pub if_not_exists: bool,
	pub overwrite: bool,
}

impl DefineTypeStatement {
	pub(crate) async fn compute(
		&self,
		ctx: &Context,
		_opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		// Check if the type already exists
		if !self.if_not_exists {
			// Check if type exists in the database
			let key = format!("type:{}", self.name);
			let txn = ctx.tx();
			if let Some(_) = txn.get(key.as_str(), None).await? {
				return Err(Error::TypeExists(self.name.to_string()));
			}
		}

		// Create the type definition
		let def = TypeDefinition {
			name: self.name.clone(),
			kind: self.kind.clone(),
			comment: self.comment.clone(),
		};

		// Store the type definition
		let key = format!("type:{}", self.name);
		let txn = ctx.tx();
		txn.set(key.as_str(), revision::to_vec(&def)?, None).await?;

		Ok(Value::None)
	}
}

impl Display for DefineTypeStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE TYPE")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		if self.overwrite {
			write!(f, " OVERWRITE")?
		}
		write!(f, " {} AS {}", self.name, self.kind)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		Ok(())
	}
}

impl InfoStructure for DefineTypeStatement {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.structure(),
			"kind".to_string() => self.kind.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}
