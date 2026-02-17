/**
[test]
reason = "Test that ai::chat returns appropriate error when the Google API key is not configured"

[[test.results]]
error = "SURREAL_AI_GOOGLE_API_KEY environment variable is not set. Set it to use 'google:' or 'gemini:' models."

[env]
namespace = "test"
database = "test"

*/

-- Try to call ai::chat with a Google model without configuring the API key
RETURN ai::chat('google:gemini-2.0-flash', [
	{ role: 'user', content: 'Hello' }
]);
