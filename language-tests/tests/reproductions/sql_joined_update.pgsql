/**
[env]
namespace = true
database = true

[test]
reason = "Joined writes are rejected until join-based write lowering exists; their sources must never become mutation targets."

[test.results]
request-error = "PostgreSQL UPDATE lowering error: unsupported statement: UPDATE FROM requires join-based write lowering"
*/
UPDATE write_target SET active = true FROM write_source;
