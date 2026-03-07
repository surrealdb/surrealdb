/**
[test]
reason = "Test that ai::sentiment returns appropriate error when the Google API key is not configured"

[[test.results]]
error = "SURREAL_AI_GOOGLE_API_KEY environment variable is not set. Set it to use 'google:' or 'gemini:' models."

[env]
namespace = "test"
database = "test"

*/

-- Try to call ai::sentiment with a Google model without configuring the API key
RETURN ai::sentiment('google:gemini-2.0-flash', 'I love this product!');
