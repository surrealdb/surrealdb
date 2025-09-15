use revision::revisioned;

use crate::expr::Expr;
use crate::expr::statements::info::InfoStructure;
use crate::kvs::impl_kv_value_revisioned;
use crate::sql::statements::define::DefineKind;
use crate::sql::{Ident, ToSql};
use crate::val::{Strand, Value};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct EventDefinition {
	pub name: String,
	pub target_table: String,
	pub when: Expr,
	pub then: Vec<Expr>,
	pub comment: Option<String>,
}

impl_kv_value_revisioned!(EventDefinition);

impl EventDefinition {
	pub fn to_sql_definition(&self) -> crate::sql::DefineEventStatement {
		crate::sql::DefineEventStatement {
			kind: DefineKind::Default,
			name: unsafe { Ident::new_unchecked(self.name.clone()) },
			target_table: unsafe { Ident::new_unchecked(self.target_table.clone()) },
			when: self.when.clone().into(),
			then: self.then.iter().cloned().map(Into::into).collect(),
			comment: self.comment.clone().map(Strand::new_lossy),
		}
	}
}

impl InfoStructure for EventDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.into(),
			"what".to_string() => self.target_table.into(),
			"when".to_string() => self.when.structure(),
			"then".to_string() => self.then.into_iter().map(|x| x.structure()).collect(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}

impl ToSql for EventDefinition {
	fn to_sql(&self) -> String {
		self.to_sql_definition().to_string()
	}
}
