//! Defines the provider traits that all AI backends implement.
use anyhow::Result;
use serde::{Deserialize, Serialize};

/// A provider that can generate vector embeddings from text input.
///
/// Implementations must be thread-safe (`Send + Sync`) since they may be
/// shared across concurrent SurrealQL queries.
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
pub trait EmbeddingProvider: Send + Sync {
	/// Generate an embedding vector for a single text input.
	///
	/// # Arguments
	/// * `model` — The model name (without the provider prefix).
	/// * `input` — The text to embed.
	///
	/// # Returns
	/// A vector of `f64` values representing the embedding.
	async fn embed(&self, model: &str, input: &str) -> Result<Vec<f64>>;
}

/// Configuration for text generation requests.
#[derive(Debug, Clone, Default)]
pub struct GenerationConfig {
	/// Sampling temperature (0.0 = deterministic, higher = more random).
	pub temperature: Option<f64>,
	/// Maximum number of tokens to generate.
	pub max_tokens: Option<u64>,
	/// Nucleus sampling: only consider tokens with cumulative probability >= top_p.
	pub top_p: Option<f64>,
	/// Stop sequences: generation stops when any of these strings is produced.
	pub stop: Option<Vec<String>>,
}

/// A provider that can generate text from a prompt using an LLM.
///
/// Implementations must be thread-safe (`Send + Sync`) since they may be
/// shared across concurrent SurrealQL queries.
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
pub trait GenerationProvider: Send + Sync {
	/// Generate text from a prompt using the specified model.
	///
	/// # Arguments
	/// * `model` — The model name (without the provider prefix).
	/// * `prompt` — The user prompt to send to the model.
	/// * `config` — Optional generation parameters.
	///
	/// # Returns
	/// The generated text as a `String`.
	async fn generate(
		&self,
		model: &str,
		prompt: &str,
		config: &GenerationConfig,
	) -> Result<String>;
}

/// A single message in a chat conversation.
///
/// Supports plain text messages as well as tool call requests and tool
/// call results used by the agent tool-calling loop.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
	/// The role of the message sender (e.g. `"system"`, `"user"`, `"assistant"`, `"tool"`).
	pub role: String,
	/// The text content of the message (may be `None` for tool-call-only assistant messages).
	#[serde(skip_serializing_if = "Option::is_none")]
	pub content: Option<String>,
	/// Tool calls requested by the assistant (present when the LLM wants to invoke tools).
	#[serde(skip_serializing_if = "Option::is_none")]
	pub tool_calls: Option<Vec<ToolCall>>,
	/// The ID of the tool call this message is a response to (for `role: "tool"` messages).
	#[serde(skip_serializing_if = "Option::is_none")]
	pub tool_call_id: Option<String>,
}

impl ChatMessage {
	/// Create a simple text message (no tool calls).
	pub fn text(role: impl Into<String>, content: impl Into<String>) -> Self {
		Self {
			role: role.into(),
			content: Some(content.into()),
			tool_calls: None,
			tool_call_id: None,
		}
	}
}

/// A tool definition that is presented to the LLM.
///
/// Contains the tool's name, description, and a JSON Schema describing
/// the expected parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolDefinition {
	/// The name of the tool.
	pub name: String,
	/// A description of what the tool does.
	pub description: String,
	/// JSON Schema for the tool's parameters.
	pub parameters: serde_json::Value,
}

/// A tool invocation requested by the LLM.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
	/// Unique identifier for this tool call.
	pub id: String,
	/// The name of the tool to invoke.
	pub name: String,
	/// The arguments to pass to the tool, as a JSON object.
	pub arguments: serde_json::Value,
}

/// The response from a chat-with-tools request.
///
/// Either the LLM produced a final text response, or it is requesting
/// one or more tool calls.
#[derive(Debug, Clone)]
pub enum ChatResponse {
	/// A final text response from the assistant.
	Message(String),
	/// The assistant is requesting tool invocations.
	ToolCalls(Vec<ToolCall>),
}

/// A provider that can conduct multi-turn chat conversations using an LLM.
///
/// Implementations must be thread-safe (`Send + Sync`) since they may be
/// shared across concurrent SurrealQL queries.
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
pub trait ChatProvider: Send + Sync {
	/// Generate a response from a list of chat messages using the specified model.
	///
	/// # Arguments
	/// * `model` — The model name (without the provider prefix).
	/// * `messages` — The conversation history as a slice of [`ChatMessage`].
	/// * `config` — Optional generation parameters.
	///
	/// # Returns
	/// The assistant's response text as a `String`.
	async fn chat(
		&self,
		model: &str,
		messages: &[ChatMessage],
		config: &GenerationConfig,
	) -> Result<String>;

	/// Generate a response with tool-calling support.
	///
	/// The LLM may return either a text response or a request to invoke
	/// one or more tools. The caller is responsible for executing tools
	/// and feeding results back.
	///
	/// # Arguments
	/// * `model` — The model name (without the provider prefix).
	/// * `messages` — The conversation history.
	/// * `tools` — Available tool definitions.
	/// * `config` — Generation parameters.
	///
	/// # Returns
	/// A [`ChatResponse`] indicating either a final message or tool call requests.
	async fn chat_with_tools(
		&self,
		model: &str,
		messages: &[ChatMessage],
		_tools: &[ToolDefinition],
		config: &GenerationConfig,
	) -> Result<ChatResponse> {
		// Default implementation: ignore tools and fall back to plain chat
		let result = self.chat(model, messages, config).await?;
		Ok(ChatResponse::Message(result))
	}
}
