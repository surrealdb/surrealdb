/**
[test]
reason = "Test that ai::chat returns appropriate error when the API key is not configured"

[[test.results]]
error = "SURREAL_AI_OPENAI_API_KEY environment variable is not set. Set it to your OpenAI API key to use 'openai:' models."

[env]
namespace = "test"
database = "test"

*/

-- Try to call ai::chat without configuring the API key
RETURN ai::chat('openai:gpt-4-turbo', [
	{ role: 'user', content: 'Hello' }
]);
