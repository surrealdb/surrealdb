/**
[env]
namespace = "test"
database = "test"
clean = true
imports = ["reproductions/vuln_002_passhash_redact_info_import.surql"]
auth = { namespace = "test", database = "test", level = "viewer" }

[test]
reason = "VULN-002 (GHSA TBD): a Database-scope Viewer must not be able to exfiltrate the Argon2 PASSHASH of users at the same or higher scope via INFO FOR. The unstructured PASSHASH clause and the structured `hash` field must be redacted for any user whose password is set."

[[test.results]]
match = """
$result.users.db_secret = /DEFINE USER db_secret ON DATABASE PASSHASH '\\[REDACTED\\]' PASSSCRAM '\\[REDACTED\\]' ROLES VIEWER DURATION FOR TOKEN 1h, FOR SESSION NONE/
"""
error = false

[[test.results]]
match = """
$result.users[0].hash = '[REDACTED]'
"""
error = false

[[test.results]]
match = """
$result = /DEFINE USER db_secret ON DATABASE PASSHASH '\\[REDACTED\\]' PASSSCRAM '\\[REDACTED\\]' ROLES VIEWER DURATION FOR TOKEN 1h, FOR SESSION NONE/
"""
error = false

[[test.results]]
match = """
$result.hash = '[REDACTED]'
"""
error = false

*/

-- A Viewer at DB scope reading INFO FOR DB receives user definitions; the
-- PASSHASH clause and structured `hash` field must be redacted.
INFO FOR DB;
INFO FOR DB STRUCTURE;

-- Same surface via INFO FOR USER (unstructured + structured).
INFO FOR USER db_secret ON DATABASE;
INFO FOR USER db_secret ON DATABASE STRUCTURE;
