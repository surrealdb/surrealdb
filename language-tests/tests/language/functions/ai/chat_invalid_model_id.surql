/**
[test]
reason = "Test that ai::chat returns an error when the model ID is missing the provider prefix"

[[test.results]]
error = "Incorrect arguments for function ai::chat(). Model ID must be in 'provider:model' format (e.g. 'openai:gpt-4-turbo'), got: 'gpt-4-turbo'"

[env]
namespace = "test"
database = "test"

*/

-- Try to call ai::chat without a provider prefix
RETURN ai::chat('gpt-4-turbo', [
	{ role: 'user', content: 'Hello' }
]);
