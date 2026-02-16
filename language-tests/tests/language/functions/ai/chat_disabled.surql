/**
[test]
reason = "Test that ai::chat returns appropriate error when the AI feature is not enabled"

[[test.results]]
error = "AI functions are not enabled"

[env]
namespace = "test"
database = "test"

*/

-- Try to call ai::chat (AI feature not enabled in tests)
RETURN ai::chat('openai:gpt-4-turbo', [
	{ role: 'user', content: 'Hello' }
]);
