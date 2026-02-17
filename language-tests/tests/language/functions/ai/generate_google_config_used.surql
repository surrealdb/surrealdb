/**
[test]
reason = "Test that ai::generate uses the DEFINE CONFIG AI key for Google (not env). An invalid config key should cause a provider error, not a 'not set' error."

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

-- Define an invalid Google key via DEFINE CONFIG AI
DEFINE CONFIG AI ON DATABASE GOOGLE_API_KEY 'invalid-google-test-key';

-- Call ai::generate; should attempt the request with the config key (not env)
RETURN ai::generate('google:gemini-2.0-flash', 'Hello');
