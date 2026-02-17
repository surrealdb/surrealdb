/**
[test]
reason = "Test that ai::chat returns appropriate error when the Voyage API key is not configured"

[[test.results]]
error = "SURREAL_AI_VOYAGE_API_KEY environment variable is not set. Set it to use 'voyage:', 'claude:', or 'anthropic:' models."

[env]
namespace = "test"
database = "test"

*/

-- Try to call ai::chat with a Voyage model without configuring the API key
RETURN ai::chat('voyage:voyage-3.5', [
	{ role: 'user', content: 'Hello' }
]);
