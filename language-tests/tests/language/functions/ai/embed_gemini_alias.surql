/**
[test]
reason = "Test ai::embed with gemini: alias routes to Google provider"
wip = true

# To run this test manually:
# 1. Build with the 'ai' feature enabled
# 2. Set SURREAL_AI_GOOGLE_API_KEY to a valid Google AI API key
# 3. Run: cargo run run ai/embed_gemini_alias

[[test.results]]
match = "array::len($) > 0"

[env]
namespace = "test"
database = "test"

*/

-- The gemini: prefix should route to Google AI
RETURN ai::embed('gemini:gemini-embedding-001', 'SurrealDB is a multi-model database');
