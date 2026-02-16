/**
[test]
reason = "Test that ai::embed returns appropriate error when the AI feature is not enabled"

[[test.results]]
error = "AI functions are not enabled"

[env]
namespace = "test"
database = "test"

*/

-- Try to call ai::embed (AI feature not enabled in tests)
RETURN ai::embed('openai:text-embedding-3-small', 'hello world');
