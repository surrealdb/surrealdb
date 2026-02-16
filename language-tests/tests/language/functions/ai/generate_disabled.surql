/**
[test]
reason = "Test that ai::generate returns appropriate error when the AI feature is not enabled"

[[test.results]]
error = "AI functions are not enabled"

[env]
namespace = "test"
database = "test"

*/

-- Try to call ai::generate (AI feature not enabled in tests)
RETURN ai::generate('openai:gpt-4-turbo', 'Hello');
