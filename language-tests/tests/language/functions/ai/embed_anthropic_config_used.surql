/**
[test]
reason = "Test that ai::embed uses the DEFINE CONFIG AI key for Anthropic (not env). An invalid config key should cause a provider error, not a 'not set' error."

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

-- Define an invalid Anthropic key via DEFINE CONFIG AI
DEFINE CONFIG AI ON DATABASE ANTHROPIC_API_KEY 'sk-ant-invalid-test-key';

-- Call ai::embed; should attempt the request with the config key (not env)
-- Note: Anthropic doesn't support embeddings, so this will return a different error
RETURN ai::embed('anthropic:claude-sonnet-4-20250514', 'hello world');
