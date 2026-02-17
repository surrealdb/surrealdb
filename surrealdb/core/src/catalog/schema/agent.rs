use revision::revisioned;
use surrealdb_types::{SqlFormat, ToSql};

use crate::ai::agent::types::{AgentConfig, AgentGuardrails, AgentMemory, AgentModel, AgentTool};
use crate::catalog::Permission;
use crate::expr::statements::info::InfoStructure;
use crate::kvs::impl_kv_value_revisioned;
use crate::sql::statements::define::DefineKind;
use crate::sql::{self, DefineAgentStatement};
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct AgentDefinition {
	pub(crate) name: String,
	pub(crate) model: AgentModel,
	pub(crate) prompt: String,
	pub(crate) config: Option<AgentConfig>,
	pub(crate) tools: Vec<AgentTool>,
	pub(crate) memory: Option<AgentMemory>,
	pub(crate) guardrails: Option<AgentGuardrails>,
	pub(crate) comment: Option<String>,
	pub(crate) permissions: Permission,
}

impl_kv_value_revisioned!(AgentDefinition);

impl AgentDefinition {
	fn to_sql_definition(&self) -> DefineAgentStatement {
		DefineAgentStatement {
			kind: DefineKind::Default,
			name: self.name.clone(),
			model: self.model.clone(),
			prompt: self.prompt.clone(),
			config: self.config.clone(),
			tools: self.tools.clone(),
			memory: self.memory.clone(),
			guardrails: self.guardrails.clone(),
			permissions: self.permissions.clone().into(),
			comment: self
				.comment
				.clone()
				.map(|x| sql::Expr::Literal(sql::Literal::String(x)))
				.unwrap_or(sql::Expr::Literal(sql::Literal::None)),
		}
	}
}

impl InfoStructure for AgentDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.into(),
			"model".to_string() => self.model.model_id.into(),
			"prompt".to_string() => self.prompt.into(),
			"tools".to_string() => self.tools
				.into_iter()
				.map(|t| Value::from(map! {
					"name".to_string() => t.name.into(),
					"description".to_string() => t.description.into(),
					"args".to_string() => t.args
						.into_iter()
						.map(|(n, k)| vec![n.into(), k.to_sql().into()].into())
						.collect::<Vec<Value>>()
						.into(),
				}))
				.collect::<Vec<Value>>()
				.into(),
			"permissions".to_string() => self.permissions.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.to_sql().into(),
		})
	}
}

impl ToSql for &AgentDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_sql_definition().fmt_sql(f, fmt)
	}
}
