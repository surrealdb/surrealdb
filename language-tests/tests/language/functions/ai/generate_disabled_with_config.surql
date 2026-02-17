/**
[test]
reason = "Test that ai::generate with config returns appropriate error when the API key is not configured"

[[test.results]]
error = "SURREAL_AI_OPENAI_API_KEY environment variable is not set. Set it to your OpenAI API key to use 'openai:' models."

[env]
namespace = "test"
database = "test"

*/

-- Try to call ai::generate with config without configuring the API key
RETURN ai::generate('openai:gpt-4-turbo', 'Hello', { temperature: 0.7, max_tokens: 100 });
