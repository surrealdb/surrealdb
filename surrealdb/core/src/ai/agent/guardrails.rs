//! Guardrail enforcement for agent execution.
//!
//! Validates tool calls against permission rules and enforces rate limits.
use anyhow::Result;

use crate::ai::agent::types::AgentGuardrails;

/// Default maximum LLM round-trips per invocation.
const DEFAULT_MAX_LLM_ROUNDS: u32 = 15;

/// Checks guardrail constraints during agent execution.
pub struct GuardrailChecker<'a> {
	config: &'a Option<AgentGuardrails>,
}

impl<'a> GuardrailChecker<'a> {
	/// Create a new guardrail checker with the given configuration.
	pub fn new(config: &'a Option<AgentGuardrails>) -> Self {
		Self {
			config,
		}
	}

	/// Get the maximum number of LLM rounds allowed.
	pub fn max_llm_rounds(&self) -> u32 {
		self.config.as_ref().and_then(|g| g.max_llm_rounds).unwrap_or(DEFAULT_MAX_LLM_ROUNDS)
	}

	/// Check whether a tool call is allowed by guardrails.
	pub fn check_tool_allowed(&self, tool_name: &str) -> Result<()> {
		if let Some(guardrails) = self.config {
			// Check deny list
			if guardrails.deny_tools.iter().any(|t| t == tool_name) {
				anyhow::bail!("Tool '{tool_name}' is denied by agent guardrails");
			}

			// Check approval requirement
			if guardrails.require_approval.iter().any(|t| t == tool_name) {
				// TODO: Implement approval flow (create approval record, pause)
				anyhow::bail!("Tool '{tool_name}' requires approval (not yet implemented)");
			}
		}
		Ok(())
	}
}
