/**
[test]
reason = "Test that ai::chat returns an error when the model ID is missing the provider prefix"

[[test.results]]
error = "Model ID must be in 'provider:model' format"

[env]
namespace = "test"
database = "test"

*/

-- Try to call ai::chat without a provider prefix
RETURN ai::chat('gpt-4-turbo', [
	{ role: 'user', content: 'Hello' }
]);
