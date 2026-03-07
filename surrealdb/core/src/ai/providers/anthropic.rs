//! Anthropic AI generation and chat provider.
//!
//! Calls the Anthropic Messages API which uses a different format from
//! OpenAI-compatible providers.
//!
//! The `anthropic:` prefix is the canonical name. `claude:` is accepted as an alias.
//!
//! # Configuration
//!
//! - `SURREAL_AI_ANTHROPIC_API_KEY` — Required. An Anthropic API key.
//! - `SURREAL_AI_ANTHROPIC_BASE_URL` — Optional. Defaults to `https://api.anthropic.com/v1`.
//!
//! # Usage
//!
//! ```sql
//! ai::generate('anthropic:claude-sonnet-4-20250514', 'What is SurrealDB?')
//! ai::chat('claude:claude-sonnet-4-20250514', [{ role: 'user', content: 'Hello' }])
//! ```
//!
//! # Embeddings
//!
//! Anthropic does not offer an embeddings API. Calling `ai::embed` with an
//! `anthropic:` or `claude:` prefix returns an error suggesting `voyage:` instead.
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use crate::ai::provider::{
	ChatMessage as ProviderChatMessage, ChatProvider, ChatResponse, EmbeddingProvider,
	GenerationConfig, GenerationProvider, ToolCall as ProviderToolCall, ToolDefinition,
};

const DEFAULT_BASE_URL: &str = "https://api.anthropic.com/v1";

/// The Anthropic API version header value.
const ANTHROPIC_VERSION: &str = "2023-06-01";

/// Default max_tokens when the caller does not specify one.
/// Anthropic requires this field; 4096 is a safe default for most models.
const DEFAULT_MAX_TOKENS: u64 = 4096;

/// Default HTTP request timeout for provider API calls (in seconds).
const HTTP_TIMEOUT_SECS: u64 = 60;

/// An AI provider that calls the Anthropic Messages API.
pub struct AnthropicProvider {
	api_key: String,
	base_url: String,
	client: reqwest::Client,
}

impl AnthropicProvider {
	/// Create a new provider with explicit configuration.
	pub(crate) fn new(api_key: String, base_url: String) -> Self {
		let client = reqwest::Client::builder()
			.timeout(std::time::Duration::from_secs(HTTP_TIMEOUT_SECS))
			.build()
			.unwrap_or_default();
		Self {
			api_key,
			base_url,
			client,
		}
	}

	/// Create a new provider from environment variables.
	///
	/// Reads `SURREAL_AI_ANTHROPIC_API_KEY` (required) and
	/// `SURREAL_AI_ANTHROPIC_BASE_URL` (optional, defaults to Anthropic).
	pub fn from_env() -> Result<Self> {
		let api_key = std::env::var("SURREAL_AI_ANTHROPIC_API_KEY").map_err(|_| {
			anyhow::anyhow!(
				"SURREAL_AI_ANTHROPIC_API_KEY environment variable is not set. \
				 Set it to your Anthropic API key to use 'anthropic:' or 'claude:' models."
			)
		})?;

		let base_url = std::env::var("SURREAL_AI_ANTHROPIC_BASE_URL")
			.unwrap_or_else(|_| DEFAULT_BASE_URL.to_owned());

		let client = reqwest::Client::builder()
			.timeout(std::time::Duration::from_secs(HTTP_TIMEOUT_SECS))
			.build()
			.map_err(|e| anyhow::anyhow!("Failed to build HTTP client: {e}"))?;

		Ok(Self {
			api_key,
			base_url,
			client,
		})
	}

	/// Build the URL for the messages endpoint.
	fn messages_url(&self) -> String {
		let base = self.base_url.trim_end_matches('/');
		format!("{base}/messages")
	}
}

// =========================================================================
// Request types
// =========================================================================

/// Request body for the Anthropic Messages API.
#[derive(Serialize)]
struct MessagesRequest<'a> {
	model: &'a str,
	max_tokens: u64,
	messages: Vec<RequestMessage>,
	#[serde(skip_serializing_if = "Option::is_none")]
	system: Option<&'a str>,
	#[serde(skip_serializing_if = "Option::is_none")]
	temperature: Option<f64>,
	#[serde(skip_serializing_if = "Option::is_none")]
	top_p: Option<f64>,
	#[serde(skip_serializing_if = "Option::is_none")]
	stop_sequences: Option<Vec<String>>,
	#[serde(skip_serializing_if = "Option::is_none")]
	tools: Option<Vec<AnthropicTool>>,
}

/// A single message in the Anthropic Messages API format.
#[derive(Serialize)]
struct RequestMessage {
	role: String,
	content: RequestContent,
}

/// Content can be a simple string or an array of content blocks.
#[derive(Serialize)]
#[serde(untagged)]
enum RequestContent {
	Text(String),
	Blocks(Vec<ContentBlock>),
}

/// A content block within a message.
#[derive(Serialize)]
#[serde(tag = "type")]
enum ContentBlock {
	#[serde(rename = "text")]
	Text {
		text: String,
	},
	#[serde(rename = "tool_use")]
	ToolUse {
		id: String,
		name: String,
		input: serde_json::Value,
	},
	#[serde(rename = "tool_result")]
	ToolResult {
		tool_use_id: String,
		content: String,
	},
}

/// Anthropic tool definition format.
#[derive(Serialize)]
struct AnthropicTool {
	name: String,
	description: String,
	input_schema: serde_json::Value,
}

// =========================================================================
// Response types
// =========================================================================

/// Top-level response from the Anthropic Messages API.
#[derive(Deserialize)]
struct MessagesResponse {
	content: Vec<ResponseContentBlock>,
	#[allow(dead_code)]
	stop_reason: Option<String>,
}

/// A content block in the response.
#[derive(Deserialize)]
#[serde(tag = "type")]
enum ResponseContentBlock {
	#[serde(rename = "text")]
	Text {
		text: String,
	},
	#[serde(rename = "tool_use")]
	ToolUse {
		id: String,
		name: String,
		input: serde_json::Value,
	},
}

// =========================================================================
// Helper methods
// =========================================================================

impl AnthropicProvider {
	/// Convert provider messages to Anthropic format, extracting system messages.
	///
	/// Returns `(system_text, messages)` where `system_text` is the concatenated
	/// system message content (if any) and `messages` are the non-system messages
	/// in Anthropic's format.
	fn convert_messages(messages: &[ProviderChatMessage]) -> (Option<String>, Vec<RequestMessage>) {
		let system_parts: Vec<&str> = messages
			.iter()
			.filter(|m| m.role == "system")
			.filter_map(|m| m.content.as_deref())
			.collect();

		let system = if system_parts.is_empty() {
			None
		} else {
			Some(system_parts.join("\n"))
		};

		let msgs = messages
			.iter()
			.filter(|m| m.role != "system")
			.map(|m| {
				if m.role == "tool" {
					// Tool result messages use content blocks
					let tool_use_id = m.tool_call_id.clone().unwrap_or_default();
					let content_text = m.content.clone().unwrap_or_default();
					RequestMessage {
						role: "user".to_string(),
						content: RequestContent::Blocks(vec![ContentBlock::ToolResult {
							tool_use_id,
							content: content_text,
						}]),
					}
				} else if m.role == "assistant" && m.tool_calls.is_some() {
					// Assistant messages with tool calls use content blocks
					let mut blocks = Vec::new();
					if let Some(text) = &m.content
						&& !text.is_empty()
					{
						blocks.push(ContentBlock::Text {
							text: text.clone(),
						});
					}
					if let Some(calls) = &m.tool_calls {
						for call in calls {
							blocks.push(ContentBlock::ToolUse {
								id: call.id.clone(),
								name: call.name.clone(),
								input: call.arguments.clone(),
							});
						}
					}
					RequestMessage {
						role: "assistant".to_string(),
						content: RequestContent::Blocks(blocks),
					}
				} else {
					// Plain text messages
					RequestMessage {
						role: m.role.clone(),
						content: RequestContent::Text(m.content.clone().unwrap_or_default()),
					}
				}
			})
			.collect();

		(system, msgs)
	}

	/// Convert tool definitions to Anthropic format.
	fn convert_tools(tools: &[ToolDefinition]) -> Vec<AnthropicTool> {
		tools
			.iter()
			.map(|t| AnthropicTool {
				name: t.name.clone(),
				description: t.description.clone(),
				input_schema: t.parameters.clone(),
			})
			.collect()
	}

	/// Send a request to the Anthropic Messages API.
	async fn send_messages_request(
		&self,
		model: &str,
		messages: Vec<RequestMessage>,
		system: Option<&str>,
		config: &GenerationConfig,
		tools: Option<Vec<AnthropicTool>>,
	) -> Result<MessagesResponse> {
		let url = self.messages_url();

		let max_tokens = config.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS);

		let body = MessagesRequest {
			model,
			max_tokens,
			messages,
			system,
			temperature: config.temperature,
			top_p: config.top_p,
			stop_sequences: config.stop.clone(),
			tools,
		};

		let client = self.client.clone();
		let response = client
			.post(&url)
			.header("Content-Type", "application/json")
			.header("x-api-key", &self.api_key)
			.header("anthropic-version", ANTHROPIC_VERSION)
			.json(&body)
			.send()
			.await
			.map_err(|e| anyhow::anyhow!("Failed to call Anthropic Messages API: {e}"))?;

		if !response.status().is_success() {
			let status = response.status();
			let body = response.text().await.unwrap_or_default();
			bail!("Anthropic Messages API returned {status}: {body}");
		}

		let result: MessagesResponse = response
			.json()
			.await
			.map_err(|e| anyhow::anyhow!("Failed to parse Anthropic Messages response: {e}"))?;

		Ok(result)
	}

	/// Extract text from a messages response.
	fn extract_text(response: &MessagesResponse) -> Result<String> {
		let text: String = response
			.content
			.iter()
			.filter_map(|block| match block {
				ResponseContentBlock::Text {
					text,
				} => Some(text.as_str()),
				_ => None,
			})
			.collect::<Vec<_>>()
			.join("");

		if text.is_empty() {
			bail!("Anthropic Messages response contained no text");
		}

		Ok(text)
	}
}

// =========================================================================
// Trait implementations
// =========================================================================

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl EmbeddingProvider for AnthropicProvider {
	async fn embed(&self, _model: &str, _input: &str) -> Result<Vec<f64>> {
		bail!(
			"Anthropic does not provide an embeddings API. \
			 Use 'voyage:' models for embeddings (e.g. 'voyage:voyage-3.5')."
		)
	}
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl GenerationProvider for AnthropicProvider {
	async fn generate(
		&self,
		model: &str,
		prompt: &str,
		config: &GenerationConfig,
	) -> Result<String> {
		let messages = vec![RequestMessage {
			role: "user".to_string(),
			content: RequestContent::Text(prompt.to_string()),
		}];

		let response = self.send_messages_request(model, messages, None, config, None).await?;
		Self::extract_text(&response)
	}
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl ChatProvider for AnthropicProvider {
	async fn chat(
		&self,
		model: &str,
		messages: &[ProviderChatMessage],
		config: &GenerationConfig,
	) -> Result<String> {
		let (system, msgs) = Self::convert_messages(messages);

		let response =
			self.send_messages_request(model, msgs, system.as_deref(), config, None).await?;
		Self::extract_text(&response)
	}

	async fn chat_with_tools(
		&self,
		model: &str,
		messages: &[ProviderChatMessage],
		tools: &[ToolDefinition],
		config: &GenerationConfig,
	) -> Result<ChatResponse> {
		let (system, msgs) = Self::convert_messages(messages);

		let anthropic_tools = if tools.is_empty() {
			None
		} else {
			Some(Self::convert_tools(tools))
		};

		let response = self
			.send_messages_request(model, msgs, system.as_deref(), config, anthropic_tools)
			.await?;

		// Check for tool_use blocks in the response
		let tool_calls: Vec<ProviderToolCall> = response
			.content
			.iter()
			.filter_map(|block| match block {
				ResponseContentBlock::ToolUse {
					id,
					name,
					input,
				} => Some(ProviderToolCall {
					id: id.clone(),
					name: name.clone(),
					arguments: input.clone(),
				}),
				_ => None,
			})
			.collect();

		if !tool_calls.is_empty() {
			return Ok(ChatResponse::ToolCalls(tool_calls));
		}

		// No tool calls — extract text
		let text = Self::extract_text(&response)?;
		Ok(ChatResponse::Message(text))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::ai::provider::ChatMessage as ProviderChatMessage;

	#[test]
	fn messages_url_default() {
		let provider = AnthropicProvider::new("test-key".into(), DEFAULT_BASE_URL.into());
		assert_eq!(provider.messages_url(), "https://api.anthropic.com/v1/messages");
	}

	#[test]
	fn messages_url_custom_with_trailing_slash() {
		let provider =
			AnthropicProvider::new("test-key".into(), "https://custom.example.com/v1/".into());
		assert_eq!(provider.messages_url(), "https://custom.example.com/v1/messages");
	}

	#[test]
	fn messages_url_custom_no_trailing_slash() {
		let provider =
			AnthropicProvider::new("test-key".into(), "https://custom.example.com/v1".into());
		assert_eq!(provider.messages_url(), "https://custom.example.com/v1/messages");
	}

	#[test]
	fn convert_messages_extracts_system() {
		let messages = vec![
			ProviderChatMessage::text("system", "You are helpful."),
			ProviderChatMessage::text("user", "Hello"),
		];
		let (system, msgs) = AnthropicProvider::convert_messages(&messages);
		assert_eq!(system.as_deref(), Some("You are helpful."));
		assert_eq!(msgs.len(), 1);
		assert_eq!(msgs[0].role, "user");
	}

	#[test]
	fn convert_messages_no_system() {
		let messages = vec![ProviderChatMessage::text("user", "Hello")];
		let (system, msgs) = AnthropicProvider::convert_messages(&messages);
		assert!(system.is_none());
		assert_eq!(msgs.len(), 1);
	}

	#[test]
	fn deserialize_text_response() {
		let json = r#"{
			"id": "msg_abc",
			"type": "message",
			"role": "assistant",
			"content": [{"type": "text", "text": "Hello from Claude!"}],
			"model": "claude-sonnet-4-20250514",
			"stop_reason": "end_turn",
			"usage": {"input_tokens": 10, "output_tokens": 5}
		}"#;
		let response: MessagesResponse = serde_json::from_str(json).unwrap();
		assert_eq!(response.content.len(), 1);
		match &response.content[0] {
			ResponseContentBlock::Text {
				text,
			} => assert_eq!(text, "Hello from Claude!"),
			_ => panic!("Expected text block"),
		}
	}

	#[test]
	fn deserialize_tool_use_response() {
		let json = r#"{
			"id": "msg_abc",
			"type": "message",
			"role": "assistant",
			"content": [
				{"type": "text", "text": "Let me check."},
				{"type": "tool_use", "id": "toolu_123", "name": "get_weather", "input": {"location": "SF"}}
			],
			"model": "claude-sonnet-4-20250514",
			"stop_reason": "tool_use",
			"usage": {"input_tokens": 10, "output_tokens": 15}
		}"#;
		let response: MessagesResponse = serde_json::from_str(json).unwrap();
		assert_eq!(response.content.len(), 2);
		match &response.content[1] {
			ResponseContentBlock::ToolUse {
				id,
				name,
				input,
			} => {
				assert_eq!(id, "toolu_123");
				assert_eq!(name, "get_weather");
				assert_eq!(input["location"], "SF");
			}
			_ => panic!("Expected tool_use block"),
		}
	}

	#[test]
	fn embed_returns_error() {
		let provider = AnthropicProvider::new("test-key".into(), DEFAULT_BASE_URL.into());
		let result =
			tokio::runtime::Runtime::new().unwrap().block_on(provider.embed("some-model", "test"));
		assert!(result.is_err());
		let msg = result.unwrap_err().to_string();
		assert!(
			msg.contains("does not provide an embeddings API"),
			"Expected embeddings error, got: {msg}"
		);
	}

	#[tokio::test]
	async fn generate_returns_text_from_mock_api() {
		use wiremock::matchers::{header, method, path};
		use wiremock::{Mock, MockServer, ResponseTemplate};

		let server = MockServer::start().await;

		let response_body = serde_json::json!({
			"id": "msg_abc",
			"type": "message",
			"role": "assistant",
			"content": [{"type": "text", "text": "SurrealDB is a multi-model database."}],
			"model": "claude-sonnet-4-20250514",
			"stop_reason": "end_turn",
			"usage": {"input_tokens": 10, "output_tokens": 7}
		});

		Mock::given(method("POST"))
			.and(path("/messages"))
			.and(header("x-api-key", "test-key"))
			.and(header("anthropic-version", ANTHROPIC_VERSION))
			.and(header("Content-Type", "application/json"))
			.respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
			.expect(1)
			.mount(&server)
			.await;

		let provider = AnthropicProvider::new("test-key".into(), server.uri());
		let config = GenerationConfig::default();
		let result =
			provider.generate("claude-sonnet-4-20250514", "What is SurrealDB?", &config).await;

		let text = result.expect("generate should succeed with mock Anthropic server");
		assert_eq!(text, "SurrealDB is a multi-model database.");

		server.verify().await;
	}

	#[tokio::test]
	async fn generate_returns_error_on_401() {
		use wiremock::matchers::method;
		use wiremock::{Mock, MockServer, ResponseTemplate};

		let server = MockServer::start().await;

		Mock::given(method("POST"))
			.respond_with(ResponseTemplate::new(401).set_body_string(
				r#"{"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"}}"#,
			))
			.expect(1)
			.mount(&server)
			.await;

		let provider = AnthropicProvider::new("bad-key".into(), server.uri());
		let config = GenerationConfig::default();
		let result = provider.generate("claude-sonnet-4-20250514", "Hello", &config).await;

		assert!(result.is_err());
		let err_msg = result.unwrap_err().to_string();
		assert!(err_msg.contains("401"), "Expected 401 in error: {err_msg}");

		server.verify().await;
	}

	#[tokio::test]
	async fn generate_returns_error_on_empty_content() {
		use wiremock::matchers::method;
		use wiremock::{Mock, MockServer, ResponseTemplate};

		let server = MockServer::start().await;

		let response_body = serde_json::json!({
			"id": "msg_abc",
			"type": "message",
			"role": "assistant",
			"content": [],
			"model": "claude-sonnet-4-20250514",
			"stop_reason": "end_turn",
			"usage": {"input_tokens": 10, "output_tokens": 0}
		});

		Mock::given(method("POST"))
			.respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
			.expect(1)
			.mount(&server)
			.await;

		let provider = AnthropicProvider::new("test-key".into(), server.uri());
		let config = GenerationConfig::default();
		let result = provider.generate("claude-sonnet-4-20250514", "Hello", &config).await;

		assert!(result.is_err());
		let err_msg = result.unwrap_err().to_string();
		assert!(err_msg.contains("no text"), "Expected 'no text' in error: {err_msg}");

		server.verify().await;
	}

	#[tokio::test]
	async fn chat_returns_text_from_mock_api() {
		use wiremock::matchers::{header, method, path};
		use wiremock::{Mock, MockServer, ResponseTemplate};

		let server = MockServer::start().await;

		let response_body = serde_json::json!({
			"id": "msg_abc",
			"type": "message",
			"role": "assistant",
			"content": [{"type": "text", "text": "SurrealDB is a multi-model database."}],
			"model": "claude-sonnet-4-20250514",
			"stop_reason": "end_turn",
			"usage": {"input_tokens": 20, "output_tokens": 7}
		});

		Mock::given(method("POST"))
			.and(path("/messages"))
			.and(header("x-api-key", "test-key"))
			.and(header("anthropic-version", ANTHROPIC_VERSION))
			.respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
			.expect(1)
			.mount(&server)
			.await;

		let provider = AnthropicProvider::new("test-key".into(), server.uri());
		let config = GenerationConfig::default();
		let messages = vec![
			ProviderChatMessage::text("system", "You are a helpful assistant."),
			ProviderChatMessage::text("user", "What is SurrealDB?"),
		];
		let result = provider.chat("claude-sonnet-4-20250514", &messages, &config).await;

		let text = result.expect("chat should succeed with mock Anthropic server");
		assert_eq!(text, "SurrealDB is a multi-model database.");

		server.verify().await;
	}

	#[tokio::test]
	async fn chat_with_tools_returns_tool_calls() {
		use wiremock::matchers::{method, path};
		use wiremock::{Mock, MockServer, ResponseTemplate};

		let server = MockServer::start().await;

		let response_body = serde_json::json!({
			"id": "msg_abc",
			"type": "message",
			"role": "assistant",
			"content": [
				{"type": "text", "text": "Let me look that up."},
				{"type": "tool_use", "id": "toolu_123", "name": "search", "input": {"query": "SurrealDB"}}
			],
			"model": "claude-sonnet-4-20250514",
			"stop_reason": "tool_use",
			"usage": {"input_tokens": 30, "output_tokens": 15}
		});

		Mock::given(method("POST"))
			.and(path("/messages"))
			.respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
			.expect(1)
			.mount(&server)
			.await;

		let provider = AnthropicProvider::new("test-key".into(), server.uri());
		let config = GenerationConfig::default();
		let messages = vec![ProviderChatMessage::text("user", "Search for SurrealDB")];
		let tools = vec![ToolDefinition {
			name: "search".to_string(),
			description: "Search the web".to_string(),
			parameters: serde_json::json!({"type": "object", "properties": {"query": {"type": "string"}}}),
		}];

		let result =
			provider.chat_with_tools("claude-sonnet-4-20250514", &messages, &tools, &config).await;
		let response = result.expect("chat_with_tools should succeed");

		match response {
			ChatResponse::ToolCalls(calls) => {
				assert_eq!(calls.len(), 1);
				assert_eq!(calls[0].id, "toolu_123");
				assert_eq!(calls[0].name, "search");
				assert_eq!(calls[0].arguments["query"], "SurrealDB");
			}
			ChatResponse::Message(_) => panic!("Expected tool calls, got message"),
		}

		server.verify().await;
	}
}
