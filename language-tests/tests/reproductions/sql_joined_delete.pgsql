/**
[env]
namespace = true
database = true

[test]
reason = "Joined writes are rejected until join-based write lowering exists; their sources must never become mutation targets."

[test.results]
request-error = "PostgreSQL DELETE lowering error: unsupported statement: DELETE USING requires join-based write lowering"
*/
DELETE FROM write_target USING write_source;
