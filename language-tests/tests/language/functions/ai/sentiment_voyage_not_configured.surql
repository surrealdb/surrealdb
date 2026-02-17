/**
[test]
reason = "Test that ai::sentiment returns appropriate error when the Voyage API key is not configured"

[[test.results]]
error = "SURREAL_AI_VOYAGE_API_KEY environment variable is not set. Set it to use 'voyage:' models."

[env]
namespace = "test"
database = "test"

*/

-- Try to call ai::sentiment with a Voyage model without configuring the API key
RETURN ai::sentiment('voyage:voyage-3.5', 'I love this product!');
