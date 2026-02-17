//! Memory manager for agent conversations.
//!
//! Handles short-term (conversation buffer) and long-term (semantic vector)
//! memory for agents.
use anyhow::Result;

use crate::ai::agent::types::AgentMemory;
use crate::ai::provider::ChatMessage;
use crate::ctx::FrozenContext;

/// Manages short-term and long-term memory for agent conversations.
pub struct MemoryManager<'a> {
	_config: &'a Option<AgentMemory>,
}

impl<'a> MemoryManager<'a> {
	/// Create a new memory manager with the given configuration.
	pub fn new(config: &'a Option<AgentMemory>) -> Self {
		Self {
			_config: config,
		}
	}

	/// Load short-term conversation history for a session.
	pub async fn load_short_term(
		&self,
		_ctx: &FrozenContext,
		_session_id: &str,
	) -> Result<Vec<ChatMessage>> {
		// TODO: Load from __agent_conversations table
		Ok(Vec::new())
	}

	/// Save conversation messages to short-term memory.
	pub async fn save_short_term(
		&self,
		_ctx: &FrozenContext,
		_session_id: &str,
		_messages: &[ChatMessage],
	) -> Result<()> {
		// TODO: Save to __agent_conversations table
		Ok(())
	}
}
