/**
[test]
reason = "Test that ml:: model functions return appropriate error when ML feature is not enabled"

[[test.results]]
error = "Problem with machine learning computation. Machine learning computation is not enabled."

[env]
namespace = "test"
database = "test"

*/

-- Try to call an ML model (ML feature not enabled in tests)
RETURN ml::nonexistent<1.0.0>({ input: 1.0 });
