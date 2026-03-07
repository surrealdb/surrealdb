/**
[test]
reason = "Test ai::chat with Google Gemini provider returns a string"
wip = true

# To run this test manually:
# 1. Build with the 'ai' feature enabled
# 2. Set SURREAL_AI_GOOGLE_API_KEY to a valid Google AI API key
# 3. Run: cargo run run ai/chat_google

[[test.results]]
# The result should be a non-empty string
match = "string::len($) > 0"

[env]
namespace = "test"
database = "test"

*/

-- Chat using Google Gemini
RETURN ai::chat('google:gemini-2.0-flash', [
	{ role: 'user', content: 'What is SurrealDB? Answer in one sentence.' }
]);
