use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::ai::agent::types::{AgentConfig, AgentGuardrails, AgentMemory, AgentModel, AgentTool};
use crate::sql::{Expr, Literal, Permission};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct DefineAgentStatement {
	pub kind: DefineKind,
	pub name: String,
	pub model: AgentModel,
	pub prompt: String,
	pub config: Option<AgentConfig>,
	pub tools: Vec<AgentTool>,
	pub memory: Option<AgentMemory>,
	pub guardrails: Option<AgentGuardrails>,
	pub comment: Expr,
	pub permissions: Permission,
}

impl ToSql for DefineAgentStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "DEFINE AGENT");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write_sql!(f, fmt, " OVERWRITE"),
			DefineKind::IfNotExists => write_sql!(f, fmt, " IF NOT EXISTS"),
		}
		write_sql!(f, fmt, " {}", self.name);
		write_sql!(f, fmt, " MODEL '{}'", self.model.model_id);
		write_sql!(f, fmt, " PROMPT '{}'", self.prompt.replace('\'', "\\'"));
		if let Some(ref config) = self.config {
			write_sql!(f, fmt, " CONFIG {{");
			let mut first = true;
			if let Some(ref t) = config.temperature {
				write_sql!(f, fmt, " temperature: {}", t.0);
				first = false;
			}
			if let Some(ref t) = config.max_tokens {
				if !first {
					f.push(',');
				}
				write_sql!(f, fmt, " max_tokens: {t}");
				first = false;
			}
			if let Some(ref t) = config.top_p {
				if !first {
					f.push(',');
				}
				write_sql!(f, fmt, " top_p: {}", t.0);
				first = false;
			}
			if let Some(ref stops) = config.stop {
				if !first {
					f.push(',');
				}
				write_sql!(f, fmt, " stop: [");
				for (i, s) in stops.iter().enumerate() {
					if i > 0 {
						f.push_str(", ");
					}
					write_sql!(f, fmt, "'{}'", s.replace('\'', "\\'"));
				}
				f.push(']');
				first = false;
			}
			if let Some(ref r) = config.max_rounds {
				if !first {
					f.push(',');
				}
				write_sql!(f, fmt, " max_rounds: {r}");
				first = false;
			}
			if let Some(ref t) = config.timeout {
				if !first {
					f.push(',');
				}
				write_sql!(f, fmt, " timeout: {t}s");
			}
			write_sql!(f, fmt, " }}");
		}
		if !self.tools.is_empty() {
			write_sql!(f, fmt, " TOOLS [{{ ");
			for (i, tool) in self.tools.iter().enumerate() {
				if i > 0 {
					f.push_str(" }}, {{ ");
				}
				write_sql!(
					f,
					fmt,
					"name: '{}', description: '{}', function(",
					tool.name,
					tool.description.replace('\'', "\\'"),
				);
				for (j, (name, kind)) in tool.args.iter().enumerate() {
					if j > 0 {
						f.push_str(", ");
					}
					write_sql!(f, fmt, "${name}: {kind}");
				}
				f.push_str(") ");
				tool.block.fmt_sql(f, fmt);
				if !tool.param_descriptions.is_empty() {
					write_sql!(f, fmt, ", parameters: [");
					for (j, (name, desc)) in tool.param_descriptions.iter().enumerate() {
						if j > 0 {
							f.push_str(", ");
						}
						write_sql!(
							f,
							fmt,
							"{{ name: '{name}', description: '{}' }}",
							desc.replace('\'', "\\'")
						);
					}
					f.push(']');
				}
			}
			f.push_str(" }}]");
		}
		if !matches!(self.comment, Expr::Literal(Literal::None)) {
			write_sql!(f, fmt, " COMMENT {}", self.comment);
		}
		let fmt = fmt.increment();
		write_sql!(f, fmt, " PERMISSIONS {}", self.permissions);
	}
}

impl From<DefineAgentStatement> for crate::expr::statements::DefineAgentStatement {
	fn from(v: DefineAgentStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name,
			model: v.model,
			prompt: v.prompt,
			config: v.config,
			tools: v.tools,
			memory: v.memory,
			guardrails: v.guardrails,
			comment: v.comment.into(),
			permissions: v.permissions.into(),
		}
	}
}

impl From<crate::expr::statements::DefineAgentStatement> for DefineAgentStatement {
	fn from(v: crate::expr::statements::DefineAgentStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name,
			model: v.model,
			prompt: v.prompt,
			config: v.config,
			tools: v.tools,
			memory: v.memory,
			guardrails: v.guardrails,
			comment: v.comment.into(),
			permissions: v.permissions.into(),
		}
	}
}
