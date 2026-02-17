/**
[test]
reason = "Test that ai::embed returns appropriate error when the API key is not configured"

[[test.results]]
error = "SURREAL_AI_OPENAI_API_KEY environment variable is not set. Set it to your OpenAI API key to use 'openai:' models."

[env]
namespace = "test"
database = "test"

*/

-- Try to call ai::embed without configuring the API key
RETURN ai::embed('openai:text-embedding-3-small', 'hello world');
