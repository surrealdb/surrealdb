/**
[test]
reason = "Test that ai::sentiment returns appropriate error when the Anthropic API key is not configured"

[[test.results]]
error = "SURREAL_AI_ANTHROPIC_API_KEY environment variable is not set. Set it to use 'anthropic:' or 'claude:' models."

[env]
namespace = "test"
database = "test"

*/

-- Try to call ai::sentiment with an Anthropic model without configuring the API key
RETURN ai::sentiment('anthropic:claude-sonnet-4-20250514', 'I love this product!');
