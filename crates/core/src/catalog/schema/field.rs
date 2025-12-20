use revision::revisioned;
use surrealdb_types::{SqlFormat, ToSql};

use super::Permission;
use crate::expr::reference::Reference;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Expr, Idiom, Kind};
use crate::kvs::impl_kv_value_revisioned;
use crate::sql::{self, DefineFieldStatement};
use crate::val::{TableName, Value};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) enum DefineDefault {
	#[default]
	None,
	Always(Expr),
	Set(Expr),
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct FieldDefinition {
	// TODO: Needs to be it's own type.
	// Idiom::Value/Idiom::Start are for example not allowed.
	pub(crate) name: Idiom,
	pub(crate) table: TableName,
	// TODO: Optionally also be a seperate type from expr::Kind
	pub(crate) field_kind: Option<Kind>,
	pub(crate) flexible: bool,
	pub(crate) readonly: bool,
	pub(crate) value: Option<Expr>,
	pub(crate) assert: Option<Expr>,
	pub(crate) computed: Option<Expr>,
	pub(crate) default: DefineDefault,

	pub(crate) select_permission: Permission,
	pub(crate) create_permission: Permission,
	pub(crate) update_permission: Permission,

	pub(crate) comment: Option<String>,
	pub(crate) reference: Option<Reference>,
}
impl_kv_value_revisioned!(FieldDefinition);

impl FieldDefinition {
	pub fn to_sql_definition(&self) -> DefineFieldStatement {
		DefineFieldStatement {
			kind: sql::statements::define::DefineKind::Default,
			name: Expr::Idiom(self.name.clone()).into(),
			what: sql::Expr::Table(self.table.clone().into_string()),
			field_kind: self.field_kind.clone().map(|x| x.into()),
			flexible: self.flexible,
			readonly: self.readonly,
			value: self.value.clone().map(|x| x.into()),
			assert: self.assert.clone().map(|x| x.into()),
			computed: self.computed.clone().map(|x| x.into()),
			default: match &self.default {
				DefineDefault::None => sql::statements::define::DefineDefault::None,
				DefineDefault::Set(x) => {
					sql::statements::define::DefineDefault::Set(x.clone().into())
				}
				DefineDefault::Always(x) => {
					sql::statements::define::DefineDefault::Always(x.clone().into())
				}
			},
			permissions: sql::Permissions {
				select: self.select_permission.to_sql_definition(),
				create: self.create_permission.to_sql_definition(),
				update: self.update_permission.to_sql_definition(),
				delete: sql::Permission::Full,
			},
			comment: self
				.comment
				.clone()
				.map(|x| sql::Expr::Literal(sql::Literal::String(x)))
				.unwrap_or(sql::Expr::Literal(sql::Literal::None)),
			reference: self.reference.clone().map(|x| x.into()),
		}
	}
}

impl InfoStructure for FieldDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.structure(),
			"table".to_string() => Value::String(self.table.into_string().into()),
			"kind".to_string(), if let Some(v) = self.field_kind => v.structure(),
			"flexible".to_string(), if self.flexible => true.into(),
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
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_sql_definition().fmt_sql(f, fmt)
	}
}
