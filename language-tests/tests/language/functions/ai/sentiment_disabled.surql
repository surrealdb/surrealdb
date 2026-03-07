/**
[test]
reason = "Test that ai::sentiment returns appropriate error when the API key is not configured"

[[test.results]]
error = "SURREAL_AI_OPENAI_API_KEY environment variable is not set. Set it to your OpenAI API key to use 'openai:' models."

[env]
namespace = "test"
database = "test"

*/

-- Try to call ai::sentiment without configuring the API key
RETURN ai::sentiment('openai:gpt-4-turbo', 'I love this product!');
