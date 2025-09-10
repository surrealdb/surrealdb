use std::fmt::Display;
use std::str::FromStr;

use anyhow::{Result, bail};
use revision::revisioned;

use super::Permission;
use crate::expr::reference::Reference;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Expr, Idiom, Kind, Literal};
use crate::kvs::impl_kv_value_revisioned;
use crate::sql::{DefineFieldStatement, ToSql};
use crate::val::{Strand, Value};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum DefineDefault {
	#[default]
	None,
	Always(Expr),
	Set(Expr),
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct FieldName(pub(crate) Vec<FieldNamePart>);

impl FieldName {
	pub fn from_idiom(idiom: Idiom) -> Result<Self> {
		let mut parts = vec![];
		for part in &idiom.0 {
			match part {
				crate::expr::Part::Field(field) => {
					parts.push(FieldNamePart::Field(field.to_raw_string()))
				}
				crate::expr::Part::Value(value) => {
					let Expr::Literal(Literal::Integer(index)) = value else {
						bail!("Expected an integer literal for index");
					};
					parts.push(FieldNamePart::Index(*index as u32))
				}
				crate::expr::Part::All => {
					parts.push(FieldNamePart::All)
				}
				unexpected => bail!("Idiom {idiom} contains unexpected part: {unexpected:?}"),
			}
		}
		Ok(FieldName(parts))
	}

	pub fn to_idiom(&self) -> Idiom {
		Idiom(
			self.0
				.iter()
				.map(|part: &FieldNamePart| match part {
					FieldNamePart::Field(field) => crate::expr::Part::Field(unsafe {
						crate::expr::Ident::new_unchecked(field.clone())
					}),
					FieldNamePart::Index(index) => {
						crate::expr::Part::Value(Expr::Literal(Literal::Integer(*index as i64)))
					}
					FieldNamePart::All => crate::expr::Part::All,
				})
				.collect(),
		)
	}

	pub(crate) fn len(&self) -> usize {
		self.0.len()
	}

	/// Check if this FieldName starts with a specific path part
	pub(crate) fn starts_with(&self, other: &FieldName) -> bool {
		self.0.starts_with(&other.0)
	}
}

impl std::ops::Index<std::ops::RangeFrom<usize>> for FieldName {
	type Output = [FieldNamePart];
	fn index(&self, index: std::ops::RangeFrom<usize>) -> &Self::Output {
		&self.0[index]
	}
}

impl FromStr for FieldName {
	type Err = anyhow::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let idiom = Idiom::from_str(s)?;
		Self::from_idiom(idiom)
	}
}

impl Display for FieldName {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		for (i, part) in self.0.iter().enumerate() {
			if i == 0 {
				match part {
					FieldNamePart::Field(field) => write!(f, "{field}")?,
					FieldNamePart::Index(index) => write!(f, "[{index}]")?,
					FieldNamePart::All => write!(f, "[*]")?,
				};
			} else {
				match part {
					FieldNamePart::Field(field) => write!(f, ".{field}")?,
					FieldNamePart::Index(index) => write!(f, "[{index}]")?,
					FieldNamePart::All => write!(f, "[*]")?,
				};
			}
		}
		Ok(())
	}
}

impl InfoStructure for FieldName {
	fn structure(self) -> Value {
		self.to_string().into()
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum FieldNamePart {
	All,
	Field(String),
	Index(u32),
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct FieldDefinition {
	pub name: FieldName,
	pub what: String,
	/// Whether the field is marked as flexible.
	/// Flexible allows the field to be schemaless even if the table is marked as schemafull.
	pub flexible: bool,
	// TODO: Optionally also be a seperate type from expr::Kind
	pub field_kind: Option<Kind>,
	pub readonly: bool,
	pub value: Option<Expr>,
	pub assert: Option<Expr>,
	pub computed: Option<Expr>,
	pub default: DefineDefault,

	pub select_permission: Permission,
	pub create_permission: Permission,
	pub update_permission: Permission,

	pub comment: Option<String>,
	pub reference: Option<Reference>,
}
impl_kv_value_revisioned!(FieldDefinition);

impl FieldDefinition {
	pub fn new(
		name: FieldName,
		what: String,
		flexible: bool,
		field_kind: Option<Kind>,
		reference: Option<Reference>,
	) -> Self {
		Self {
			name,
			what,
			flexible,
			field_kind,
			readonly: false,
			value: None,
			assert: None,
			computed: None,
			default: DefineDefault::default(),
			select_permission: Permission::default(),
			create_permission: Permission::default(),
			update_permission: Permission::default(),
			comment: None,
			reference,
		}
	}

	pub fn to_sql_definition(&self) -> DefineFieldStatement {
		DefineFieldStatement {
			kind: crate::sql::statements::define::DefineKind::Default,
			name: self.name.clone(),
			what: unsafe { crate::sql::Ident::new_unchecked(self.what.clone()) },
			flex: self.flexible,
			field_kind: self.field_kind.clone().map(|x| x.into()),
			readonly: self.readonly,
			value: self.value.clone().map(|x| x.into()),
			assert: self.assert.clone().map(|x| x.into()),
			computed: self.computed.clone().map(|x| x.into()),
			default: match &self.default {
				DefineDefault::None => crate::sql::statements::define::DefineDefault::None,
				DefineDefault::Set(x) => {
					crate::sql::statements::define::DefineDefault::Set(x.clone().into())
				}
				DefineDefault::Always(x) => {
					crate::sql::statements::define::DefineDefault::Always(x.clone().into())
				}
			},
			permissions: crate::sql::Permissions {
				select: self.select_permission.to_sql_definition(),
				create: self.create_permission.to_sql_definition(),
				update: self.update_permission.to_sql_definition(),
				delete: crate::sql::Permission::Full,
			},
			comment: self.comment.clone().map(|x| unsafe { Strand::new_unchecked(x) }),
			reference: self.reference.clone().map(|x| x.into()),
		}
	}
}

impl InfoStructure for FieldDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.structure(),
			"what".to_string() => Value::from(self.what.clone()),
			"flex".to_string() => self.flexible.into(),
			"kind".to_string(), if let Some(v) = self.field_kind => v.structure(),
			"value".to_string(), if let Some(v) = self.value => v.structure(),
			"assert".to_string(), if let Some(v) = self.assert => v.structure(),
			"computed".to_string(), if let Some(v) = self.computed => v.structure(),
			"default_always".to_string(), if matches!(&self.default, DefineDefault::Always(_) | DefineDefault::Set(_)) => Value::Bool(matches!(self.default,DefineDefault::Always(_))), // Only reported if DEFAULT is also enabled for this field
			"default".to_string(), if let DefineDefault::Always(v) | DefineDefault::Set(v) = self.default => v.structure(),
			"reference".to_string(), if let Some(v) = self.reference => v.structure(),
			"readonly".to_string() => self.readonly.into(),
			"permissions".to_string() => Value::from(map!{
				"select".to_string() => self.select_permission.structure(),
				"create".to_string() => self.create_permission.structure(),
				"update".to_string() => self.update_permission.structure(),
			}),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}

impl ToSql for FieldDefinition {
	fn to_sql(&self) -> String {
		self.to_sql_definition().to_string()
	}
}
