/**
[test]
reason = "Test ai::sentiment with OpenAI provider returns a valid sentiment object"
wip = true

# To run this test manually:
# 1. Build with the 'ai' feature enabled
# 2. Set SURREAL_AI_OPENAI_API_KEY to a valid OpenAI API key
# 3. Run: cargo run run ai/sentiment_openai

[[test.results]]
# The result should be an object with sentiment (number) and summary (string)
match = "type::is::object($) AND $['sentiment'] >= -1 AND $['sentiment'] <= 1 AND ($['summary'] = 'positive' OR $['summary'] = 'negative' OR $['summary'] = 'mixed')"

[env]
namespace = "test"
database = "test"

*/

-- Analyse sentiment using OpenAI
RETURN ai::sentiment('openai:gpt-4-turbo', 'I love this product! It is amazing and works perfectly.');
