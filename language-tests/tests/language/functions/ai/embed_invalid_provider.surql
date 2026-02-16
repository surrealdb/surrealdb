/**
[test]
reason = "Test ai::embed returns error for unknown provider prefix"
wip = true

# This test requires the 'ai' feature to be enabled.
# Without it, the error is 'AI functions are not enabled' instead.

[[test.results]]
error = true

[env]
namespace = "test"
database = "test"

*/

-- Unknown provider prefix should error
RETURN ai::embed('badprovider:some-model', 'hello world');
