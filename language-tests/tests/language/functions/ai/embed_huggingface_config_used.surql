/**
[test]
reason = "Test that ai::embed uses the DEFINE CONFIG AI base URL for HuggingFace (not env). An invalid base URL should cause a connection error, proving the config is used."

[[test.results]]
value = "NONE"

[[test.results]]
match = "string::contains($error, 'invalid-hf-host.example.invalid')"
error = true

[env]
namespace = true
database = true
auth = { level = "owner" }
timeout = 10000
context-timeout = 10000

*/

-- Define a HuggingFace config with an unreachable base URL to prove config is used
DEFINE CONFIG AI ON DATABASE HUGGINGFACE_API_KEY 'hf-invalid' HUGGINGFACE_BASE_URL 'https://invalid-hf-host.example.invalid/v1';

-- Call ai::embed; should attempt the request to the config base URL and fail
RETURN ai::embed('huggingface:sentence-transformers/all-MiniLM-L6-v2', 'hello world');
