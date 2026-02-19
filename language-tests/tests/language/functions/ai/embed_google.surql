/**
[test]
reason = "Test ai::embed with Google provider returns an array of floats"
wip = true

# To run this test manually:
# 1. Build with the 'ai' feature enabled
# 2. Set SURREAL_AI_GOOGLE_API_KEY to a valid Google AI API key
# 3. Run: cargo run run ai/embed_google

[[test.results]]
match = "array::len($) > 0"

[env]
namespace = "test"
database = "test"

*/

-- Generate an embedding using Google Gemini API
RETURN ai::embed('google:text-embedding-005', 'SurrealDB is a multi-model database');
