/**
[test]
reason = "Test ai::embed with HuggingFace provider returns an array of floats"
wip = true

# To run this test manually:
# 1. Build with the 'ai' feature enabled
# 2. Set SURREAL_AI_HUGGINGFACE_API_KEY to a valid HuggingFace API token
# 3. Run: cargo run run ai/embed_huggingface

[[test.results]]
# The embedding should be a non-empty array
match = "array::len($) > 0"

[env]
namespace = "test"
database = "test"

*/

-- Generate an embedding using HuggingFace Inference API
RETURN ai::embed('huggingface:sentence-transformers/all-MiniLM-L6-v2', 'SurrealDB is a multi-model database');
