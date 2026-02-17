//! Agent execution engine.
//!
//! Orchestrates the LLM tool-calling loop: prompt construction,
//! LLM invocation, tool execution, and response assembly.
use anyhow::Result;
use surrealdb_types::ToSql;

use super::guardrails::GuardrailChecker;
use super::memory::MemoryManager;
use super::tools::ToolExecutor;
use crate::ai::provider::{ChatMessage, ChatResponse, GenerationConfig};
use crate::catalog::AgentDefinition;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::val::Value;

/// Input to an agent invocation.
#[derive(Debug, Clone)]
pub struct AgentInput {
	/// The user's message.
	pub message: String,
	/// Optional session ID for stateful conversations.
	pub session_id: Option<String>,
}

/// Output from an agent invocation.
#[derive(Debug, Clone)]
pub struct AgentOutput {
	/// The agent's response message.
	pub message: String,
	/// Tool calls that were made during the invocation.
	pub tools_used: Vec<String>,
	/// Number of LLM round-trips.
	pub llm_rounds: u32,
}

impl From<AgentOutput> for Value {
	fn from(output: AgentOutput) -> Self {
		Value::from(map! {
			"message".to_string() => Value::from(output.message),
			"tools_used".to_string() => output.tools_used.into_iter()
				.map(Value::from)
				.collect::<Vec<Value>>()
				.into(),
			"llm_rounds".to_string() => Value::from(output.llm_rounds as i64),
		})
	}
}

/// Default timeout for agent execution (in seconds).
const DEFAULT_AGENT_TIMEOUT_SECS: u64 = 120;

/// Runs an agent with the given input.
///
/// This is the main entry point for agent execution. It:
/// 1. Loads memory (conversation history, long-term memories)
/// 2. Builds the initial message list
/// 3. Enters the LLM decision loop (call LLM -> execute tools -> repeat)
/// 4. Stores conversation to memory
/// 5. Returns the agent's response
///
/// The entire execution is bounded by a timeout (from `AgentConfig.timeout`
/// or a default of 120 seconds).
pub async fn run(
	ctx: &FrozenContext,
	opt: &Options,
	agent: &AgentDefinition,
	input: AgentInput,
) -> Result<AgentOutput> {
	let agent_config = agent.config.as_ref();
	let timeout_secs = agent_config.and_then(|c| c.timeout).unwrap_or(DEFAULT_AGENT_TIMEOUT_SECS);
	let timeout = std::time::Duration::from_secs(timeout_secs);

	tokio::time::timeout(timeout, run_inner(ctx, opt, agent, input))
		.await
		.map_err(|_| anyhow::anyhow!("Agent '{}' timed out after {timeout_secs}s", agent.name))?
}

/// Inner agent execution, called by `run()` under a timeout.
async fn run_inner(
	ctx: &FrozenContext,
	opt: &Options,
	agent: &AgentDefinition,
	input: AgentInput,
) -> Result<AgentOutput> {
	let guardrails = GuardrailChecker::new(&agent.guardrails);
	let tool_executor = ToolExecutor::new(ctx, opt, &agent.tools);
	let memory_manager = MemoryManager::new(&agent.memory);

	// Build tool definitions for the LLM
	let tool_defs = tool_executor.tool_definitions();

	// Build initial messages
	let mut messages: Vec<ChatMessage> = Vec::new();

	// System prompt
	messages.push(ChatMessage {
		role: "system".to_string(),
		content: Some(agent.prompt.clone()),
		tool_calls: None,
		tool_call_id: None,
	});

	// Load conversation history from memory
	if let Some(ref session_id) = input.session_id {
		let history = memory_manager.load_short_term(ctx, session_id).await?;
		messages.extend(history);
	}

	// User message
	messages.push(ChatMessage {
		role: "user".to_string(),
		content: Some(input.message.clone()),
		tool_calls: None,
		tool_call_id: None,
	});

	// Parse model ID into provider and model name
	let (provider_name, model_name) = crate::ai::chat::parse_model_id(&agent.model.model_id)?;

	// Build generation config from agent config
	let agent_config = agent.config.as_ref();
	let config = GenerationConfig {
		temperature: agent_config.and_then(|c| c.temperature.as_ref().map(|t| t.0)),
		max_tokens: agent_config.and_then(|c| c.max_tokens),
		top_p: agent_config.and_then(|c| c.top_p.as_ref().map(|t| t.0)),
		stop: agent_config.and_then(|c| c.stop.clone()),
	};

	let mut tools_used = Vec::new();
	let mut llm_rounds = 0u32;
	let max_rounds =
		agent_config.and_then(|c| c.max_rounds).unwrap_or_else(|| guardrails.max_llm_rounds());

	// Decision loop
	loop {
		llm_rounds += 1;
		if llm_rounds > max_rounds {
			anyhow::bail!("Agent '{}' exceeded maximum LLM rounds ({max_rounds})", agent.name);
		}

		// Call the LLM with tools
		let provider = crate::ai::chat::get_provider(provider_name)?;
		let response = provider.chat_with_tools(model_name, &messages, &tool_defs, &config).await?;

		match response {
			ChatResponse::Message(text) => {
				// Store conversation to memory
				if let Some(ref session_id) = input.session_id {
					memory_manager.save_short_term(ctx, session_id, &messages).await?;
				}

				return Ok(AgentOutput {
					message: text,
					tools_used,
					llm_rounds,
				});
			}
			ChatResponse::ToolCalls(calls) => {
				// Add assistant's tool call message
				messages.push(ChatMessage {
					role: "assistant".to_string(),
					content: None,
					tool_calls: Some(calls.clone()),
					tool_call_id: None,
				});

				for call in &calls {
					// Check guardrails
					guardrails.check_tool_allowed(&call.name)?;

					// Execute the tool
					let result = tool_executor.execute(call).await;
					tools_used.push(call.name.clone());

					// Add tool result to messages
					let result_text = match result {
						Ok(Value::String(s)) => s.clone(),
						Ok(val) => val.to_sql(),
						Err(e) => format!("{{\"error\": \"{e}\"}}"),
					};

					messages.push(ChatMessage {
						role: "tool".to_string(),
						content: Some(result_text),
						tool_calls: None,
						tool_call_id: Some(call.id.clone()),
					});
				}
			}
		}
	}
}
