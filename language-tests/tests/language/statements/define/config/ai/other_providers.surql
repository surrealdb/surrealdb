/**
[test]
reason = "Test DEFINE CONFIG AI ON DATABASE with Google, Voyage, and Huggingface provider keys"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ AI: \"AI ON DATABASE GOOGLE_API_KEY 'google-key' GOOGLE_BASE_URL 'https://google.example.com' VOYAGE_API_KEY 'voyage-key' VOYAGE_BASE_URL 'https://voyage.example.com' HUGGINGFACE_API_KEY 'hf-key' HUGGINGFACE_BASE_URL 'https://hf.example.com'\" }"

[[test.results]]
value = "[{ ai: { google_api_key: 'google-key', google_base_url: 'https://google.example.com', voyage_api_key: 'voyage-key', voyage_base_url: 'https://voyage.example.com', huggingface_api_key: 'hf-key', huggingface_base_url: 'https://hf.example.com' } }]"

[env]
namespace = true
database = true
auth = { level = "owner" }
*/

-- Define AI config with Google, Voyage, and Huggingface keys only
DEFINE CONFIG AI ON DATABASE
	GOOGLE_API_KEY 'google-key'
	GOOGLE_BASE_URL 'https://google.example.com'
	VOYAGE_API_KEY 'voyage-key'
	VOYAGE_BASE_URL 'https://voyage.example.com'
	HUGGINGFACE_API_KEY 'hf-key'
	HUGGINGFACE_BASE_URL 'https://hf.example.com';

-- Verify the config is stored and formatted
(INFO FOR DB).configs;

-- Verify the structured config values
(INFO FOR DB STRUCTURE).configs;
