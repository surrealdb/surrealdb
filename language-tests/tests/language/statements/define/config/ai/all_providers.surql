/**
[test]
reason = "Test DEFINE CONFIG AI ON DATABASE with all provider keys (OpenAI, Google, Voyage, Huggingface)"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ AI: \"AI ON DATABASE OPENAI_API_KEY 'sk-openai' OPENAI_BASE_URL 'https://api.openai.example.com' GOOGLE_API_KEY 'sk-google' GOOGLE_BASE_URL 'https://google.example.com' VOYAGE_API_KEY 'sk-voyage' VOYAGE_BASE_URL 'https://voyage.example.com' HUGGINGFACE_API_KEY 'sk-hf' HUGGINGFACE_BASE_URL 'https://hf.example.com'\" }"

[[test.results]]
value = "[{ ai: { openai_api_key: 'sk-openai', openai_base_url: 'https://api.openai.example.com', google_api_key: 'sk-google', google_base_url: 'https://google.example.com', voyage_api_key: 'sk-voyage', voyage_base_url: 'https://voyage.example.com', huggingface_api_key: 'sk-hf', huggingface_base_url: 'https://hf.example.com' } }]"

[env]
namespace = true
database = true
auth = { level = "owner" }
*/

-- Define AI config with all provider keys
DEFINE CONFIG AI ON DATABASE
	OPENAI_API_KEY 'sk-openai'
	OPENAI_BASE_URL 'https://api.openai.example.com'
	GOOGLE_API_KEY 'sk-google'
	GOOGLE_BASE_URL 'https://google.example.com'
	VOYAGE_API_KEY 'sk-voyage'
	VOYAGE_BASE_URL 'https://voyage.example.com'
	HUGGINGFACE_API_KEY 'sk-hf'
	HUGGINGFACE_BASE_URL 'https://hf.example.com';

-- Verify the config is stored and formatted
(INFO FOR DB).configs;

-- Verify the structured config values
(INFO FOR DB STRUCTURE).configs;
