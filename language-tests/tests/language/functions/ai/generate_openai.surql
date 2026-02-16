/**
[test]
reason = "Test ai::generate with OpenAI provider returns a string"
wip = true

# To run this test manually:
# 1. Build with the 'ai' feature enabled
# 2. Set SURREAL_AI_OPENAI_API_KEY to a valid OpenAI API key
# 3. Run: cargo run run ai/generate_openai

[[test.results]]
# The result should be a non-empty string
match = "string::len($) > 0"

[env]
namespace = "test"
database = "test"

*/

-- Generate text using OpenAI
RETURN ai::generate('openai:gpt-4-turbo', 'What is SurrealDB? Answer in one sentence.');
