/**
[test]
reason = "Test ai::embed with Voyage provider returns an array of floats"
wip = true

# To run this test manually:
# 1. Build with the 'ai' feature enabled
# 2. Set SURREAL_AI_VOYAGE_API_KEY to a valid Voyage AI API key
# 3. Run: cargo run run ai/embed_voyage

[[test.results]]
match = "array::len($) > 0"

[env]
namespace = "test"
database = "test"

*/

-- Generate an embedding using Voyage AI
RETURN ai::embed('voyage:voyage-3.5', 'SurrealDB is a multi-model database');
