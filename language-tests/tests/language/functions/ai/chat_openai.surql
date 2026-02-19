/**
[test]
reason = "Test ai::chat with OpenAI provider returns a string"
wip = true

# To run this test manually:
# 1. Build with the 'ai' feature enabled
# 2. Set SURREAL_AI_OPENAI_API_KEY to a valid OpenAI API key
# 3. Run: cargo run run ai/chat_openai

[[test.results]]
# The result should be a non-empty string
match = "string::len($) > 0"

[env]
namespace = "test"
database = "test"

*/

-- Chat using OpenAI
RETURN ai::chat('openai:gpt-4-turbo', [
	{ role: 'system', content: 'You are a helpful assistant. Answer in one sentence.' },
	{ role: 'user', content: 'What is SurrealDB?' }
]);
