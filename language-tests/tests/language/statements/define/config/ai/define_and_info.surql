/**
[test]
reason = "Test DEFINE CONFIG AI ON DATABASE stores provider keys and appears in INFO FOR DB and INFO FOR DB STRUCTURE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ AI: \"AI ON DATABASE OPENAI_API_KEY 'sk-test' OPENAI_BASE_URL 'https://api.example.com/v1'\" }"

[[test.results]]
value = "[{ ai: { openai_api_key: 'sk-test', openai_base_url: 'https://api.example.com/v1' } }]"

[env]
namespace = true
database = true
auth = { level = "owner" }
*/

-- Define AI config for the current database with optional provider keys
DEFINE CONFIG AI ON DATABASE OPENAI_API_KEY 'sk-test' OPENAI_BASE_URL 'https://api.example.com/v1';

-- Verify the config is stored and formatted
(INFO FOR DB).configs;

-- Verify the structured config values
(INFO FOR DB STRUCTURE).configs;
