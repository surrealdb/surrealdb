use std::fmt::{self, Write as _};

use revision::revisioned;

use crate::expr::escape::QuoteStr;
use crate::expr::fmt::{is_pretty, pretty_indent};
use crate::expr::reference::Reference;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Expr, Idiom, Kind, Permissions};
use crate::kvs::impl_kv_value_revisioned;
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum DefineDefault {
	#[default]
	None,
	Always(Expr),
	Set(Expr),
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct FieldDefinition {
	// TODO: Needs to be it's own type.
	// Idiom::Value/Idiom::Start are for example not allowed.
	pub name: Idiom,
	pub what: String,
	/// Whether the field is marked as flexible.
	/// Flexible allows the field to be schemaless even if the table is marked as schemafull.
	pub flex: bool,
	// TODO: Optionally also be a seperate type from expr::Kind
	pub field_kind: Option<Kind>,
	pub readonly: bool,
	pub value: Option<Expr>,
	pub assert: Option<Expr>,
	pub default: DefineDefault,
	pub permissions: Permissions,
	pub comment: Option<String>,
	pub reference: Option<Reference>,
}
impl_kv_value_revisioned!(FieldDefinition);

impl fmt::Display for FieldDefinition {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE FIELD")?;
		write!(f, " {} ON {}", self.name, self.what)?;
		if self.flex {
			write!(f, " FLEXIBLE")?
		}
		if let Some(ref v) = self.field_kind {
			write!(f, " TYPE {v}")?
		}
		match self.default {
			DefineDefault::None => {}
			DefineDefault::Always(ref expr) => write!(f, " DEFAULT ALWAYS {expr}")?,
			DefineDefault::Set(ref expr) => write!(f, " DEFAULT {expr}")?,
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
		if let Some(ref v) = self.reference {
			write!(f, " REFERENCE {v}")?
		}
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {}", QuoteStr(v.as_str()))?
		}
		let _indent = if is_pretty() {
			Some(pretty_indent())
		} else {
			f.write_char(' ')?;
			None
		};
		// Alternate permissions display implementation ignores delete permission
		// This display is used to show field permissions, where delete has no effect
		// Displaying the permission could mislead users into thinking it has an effect
		// Additionally, including the permission will cause a parsing error in 3.0.0
		write!(f, "{:#}", self.permissions)?;
		Ok(())
	}
}

impl InfoStructure for FieldDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.structure(),
			"what".to_string() => Value::from(self.what.clone()),
			"flex".to_string() => self.flex.into(),
			"kind".to_string(), if let Some(v) = self.field_kind => v.structure(),
			"value".to_string(), if let Some(v) = self.value => v.structure(),
			"assert".to_string(), if let Some(v) = self.assert => v.structure(),
			"default_always".to_string(), if matches!(&self.default, DefineDefault::Always(_) | DefineDefault::Set(_)) => Value::Bool(matches!(self.default,DefineDefault::Always(_))), // Only reported if DEFAULT is also enabled for this field
			"default".to_string(), if let DefineDefault::Always(v) | DefineDefault::Set(v) = self.default => v.structure(),
			"reference".to_string(), if let Some(v) = self.reference => v.structure(),
			"readonly".to_string() => self.readonly.into(),
			"permissions".to_string() => self.permissions.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}
