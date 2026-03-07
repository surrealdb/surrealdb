/**
[test]
reason = "Test ai::generate returns error when model ID is missing provider prefix"
wip = true

# This test requires the 'ai' feature to be enabled.
# Without it, the error is 'AI functions are not enabled' instead.

[[test.results]]
error = true

[env]
namespace = "test"
database = "test"

*/

-- Missing provider prefix should error
RETURN ai::generate('gpt-4-turbo', 'Hello');
