/**
[env]
namespace = "nonexistent_ns"
database = "nonexistent_db"
auth = { level = "owner" }

[test]
reason = "ALTER on non-existent namespace/database must fail instead of silently creating catalog entries"

[[test.results]]
error = true

[[test.results]]
error = true

*/

ALTER NAMESPACE COMPACT;
ALTER DATABASE COMPACT;
