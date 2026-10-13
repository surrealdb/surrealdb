/**
[env]
namespace = true
database = true
auth = { namespace = "test", database = "test", level = "viewer" }

[test]
reason = """
SLEEP needs edit permission: a database viewer is denied it.
"""

[[test.results]]
error = "IAM error: Not enough permissions to perform this action"
*/

SLEEP 1ms;
