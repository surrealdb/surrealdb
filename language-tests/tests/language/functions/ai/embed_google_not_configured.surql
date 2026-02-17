/**
[test]
reason = "Test that ai::embed returns appropriate error when the Google API key is not configured"

[[test.results]]
error = "SURREAL_AI_GOOGLE_API_KEY environment variable is not set. Set it to use 'google:' or 'gemini:' models."

[env]
namespace = "test"
database = "test"

*/

-- Try to call ai::embed with a Google model without configuring the API key
RETURN ai::embed('google:text-embedding-005', 'hello world');
