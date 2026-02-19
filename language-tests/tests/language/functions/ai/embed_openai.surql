/**
[test]
reason = "Test ai::embed with OpenAI provider returns an array of floats"
wip = true

# To run this test manually:
# 1. Build with the 'ai' feature enabled
# 2. Set SURREAL_AI_OPENAI_API_KEY to a valid OpenAI API key
# 3. Run: cargo run run ai/embed_openai

[[test.results]]
# The embedding should be a non-empty array
match = "array::len($) > 0"

[env]
namespace = "test"
database = "test"

*/

-- Generate an embedding using OpenAI
RETURN ai::embed('openai:text-embedding-3-small', 'SurrealDB is a multi-model database');
