/**
[test]
reason = "Test that ai::generate returns appropriate error when the Voyage API key is not configured"

[[test.results]]
error = "SURREAL_AI_VOYAGE_API_KEY environment variable is not set. Set it to use 'voyage:' models."

[env]
namespace = "test"
database = "test"

*/

-- Try to call ai::generate with a Voyage model without configuring the API key
RETURN ai::generate('voyage:voyage-3.5', 'Hello');
