//! Core types for agent configuration.
//!
//! These types are shared across the parser, AST, catalog storage, and runtime.
//! They define the schema for `DEFINE AGENT` statements.
use std::collections::BTreeMap;

use revision::revisioned;
use serde::{Deserialize, Serialize};

/// Model identifier for an agent.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct AgentModel {
	/// Provider-prefixed model identifier (e.g. `"openai:gpt-4-turbo"`).
	pub model_id: String,
}

/// Configuration for agent generation and behavior.
///
/// Passed as the optional `CONFIG { ... }` block on `DEFINE AGENT`.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct AgentConfig {
	/// Sampling temperature (0.0 = deterministic, higher = more random).
	pub temperature: Option<OrderedFloat>,
	/// Maximum number of tokens to generate per LLM call.
	pub max_tokens: Option<u64>,
	/// Nucleus sampling threshold.
	pub top_p: Option<OrderedFloat>,
	/// Stop sequences: generation stops when any of these strings is produced.
	pub stop: Option<Vec<String>>,
	/// Maximum LLM round-trips per invocation (prevents infinite loops).
	pub max_rounds: Option<u32>,
	/// Maximum wall-clock time per invocation (in seconds).
	pub timeout: Option<u64>,
}

/// Wrapper for f64 that implements Eq and Hash via total ordering.
///
/// Required because `f64` doesn't implement `Eq`/`Hash`, but we need
/// these for AST node comparisons and catalog storage.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct OrderedFloat(pub f64);

impl PartialEq for OrderedFloat {
	fn eq(&self, other: &Self) -> bool {
		self.0.to_bits() == other.0.to_bits()
	}
}

impl Eq for OrderedFloat {}

impl std::hash::Hash for OrderedFloat {
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
		self.0.to_bits().hash(state);
	}
}

impl From<f64> for OrderedFloat {
	fn from(v: f64) -> Self {
		Self(v)
	}
}

impl From<OrderedFloat> for f64 {
	fn from(v: OrderedFloat) -> f64 {
		v.0
	}
}

/// A tool that the agent can invoke.
///
/// Each tool has an inline function body (args + block) that is executed
/// when the LLM requests it. The tool is presented to the LLM with a
/// name, description, and auto-generated parameter schema.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct AgentTool {
	/// Tool name as presented to the LLM.
	pub name: String,
	/// Human-readable description of what the tool does.
	pub description: String,
	/// Typed parameters for the inline function (same format as FunctionDefinition).
	pub args: Vec<(String, crate::expr::Kind)>,
	/// The function body to execute when the tool is called.
	pub(crate) block: crate::expr::Block,
	/// Optional per-parameter descriptions for the LLM's JSON Schema.
	/// Keyed by parameter name (without `$` prefix).
	pub param_descriptions: BTreeMap<String, String>,
}

/// Memory configuration for an agent.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct AgentMemory {
	/// Short-term conversation memory settings.
	pub short_term: Option<ShortTermMemory>,
	/// Long-term semantic memory settings.
	pub long_term: Option<LongTermMemory>,
}

/// Short-term (conversation) memory configuration.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct ShortTermMemory {
	/// Maximum number of messages to keep in context.
	pub max_messages: Option<u32>,
	/// Maximum token count for conversation history.
	pub max_tokens: Option<u32>,
	/// Strategy for handling overflow.
	pub strategy: OverflowStrategy,
	/// Time-to-live for conversation sessions (in seconds).
	pub ttl: Option<u64>,
}

/// Strategy for handling conversation memory overflow.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum OverflowStrategy {
	/// Drop the oldest messages when the buffer is full.
	#[default]
	SlidingWindow,
	/// Summarize older messages into a condensed form.
	Summarize,
}

/// Long-term (semantic/vector) memory configuration.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct LongTermMemory {
	/// Embedding model for semantic retrieval.
	pub embed_model: Option<String>,
	/// Number of memories to retrieve per invocation.
	pub top_k: u32,
	/// Minimum similarity threshold for retrieval.
	pub similarity_threshold: OrderedFloat,
	/// Time-to-live for memories (in seconds).
	pub ttl: Option<u64>,
}

impl Default for LongTermMemory {
	fn default() -> Self {
		Self {
			embed_model: None,
			top_k: 5,
			similarity_threshold: OrderedFloat(0.7),
			ttl: None,
		}
	}
}

/// Guardrails configuration for an agent.
///
/// Controls rate limits, tool permissions, and safety boundaries.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct AgentGuardrails {
	/// Maximum agent invocations per minute.
	pub max_invocations_per_minute: Option<u32>,
	/// Maximum tool calls allowed per single invocation.
	pub max_tool_calls_per_invocation: Option<u32>,
	/// Maximum LLM round-trips per invocation (prevents infinite loops).
	pub max_llm_rounds: Option<u32>,
	/// Maximum total tokens per invocation.
	pub max_tokens_per_invocation: Option<u64>,
	/// Tool names that require human approval before execution.
	pub require_approval: Vec<String>,
	/// Tool names that are explicitly denied.
	pub deny_tools: Vec<String>,
}
