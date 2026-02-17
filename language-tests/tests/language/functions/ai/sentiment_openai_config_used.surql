/**
[test]
reason = "Test that ai::sentiment uses the DEFINE CONFIG AI key for OpenAI (not env). An invalid config key should cause a provider error, not a 'not set' error."

[[test.results]]
value = "NONE"

[[test.results]]
match = "!string::contains($error, 'not set')"
error = true

[env]
namespace = true
database = true
auth = { level = "owner" }
timeout = 10000
context-timeout = 10000

*/

-- Define an invalid OpenAI key via DEFINE CONFIG AI
DEFINE CONFIG AI ON DATABASE OPENAI_API_KEY 'sk-invalid-test-key';

-- Call ai::sentiment; should attempt the request with the config key (not env)
RETURN ai::sentiment('openai:gpt-4-turbo', 'I love this product!');
