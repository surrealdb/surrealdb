/**
[test]
reason = "Test ai::embed with claude: alias routes to Voyage provider"
wip = true

# To run this test manually:
# 1. Build with the 'ai' feature enabled
# 2. Set SURREAL_AI_VOYAGE_API_KEY to a valid Voyage AI API key
# 3. Run: cargo run run ai/embed_claude_alias

[[test.results]]
match = "array::len($) > 0"

[env]
namespace = "test"
database = "test"

*/

-- The claude: prefix should route to Voyage AI
RETURN ai::embed('claude:voyage-3.5', 'SurrealDB is a multi-model database');
