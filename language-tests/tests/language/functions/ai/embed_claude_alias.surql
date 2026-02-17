/**
[test]
reason = "Test ai::embed with claude: alias routes to Anthropic provider"

[[test.results]]
error = "SURREAL_AI_ANTHROPIC_API_KEY environment variable is not set. Set it to use 'anthropic:' or 'claude:' models."

[env]
namespace = "test"
database = "test"

*/

-- The claude: prefix should route to Anthropic provider (not Voyage)
RETURN ai::embed('claude:claude-sonnet-4-20250514', 'SurrealDB is a multi-model database');
