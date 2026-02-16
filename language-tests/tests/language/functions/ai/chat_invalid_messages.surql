/**
[test]
reason = "Test that ai::chat returns an error when the messages argument is not a valid array of objects"

[[test.results]]
error = "The messages argument must be an array of objects"

[env]
namespace = "test"
database = "test"

*/

-- Try to call ai::chat with a string instead of an array
RETURN ai::chat('openai:gpt-4-turbo', 'not an array');
