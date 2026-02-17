/**
[test]
reason = "Test that DEFINE AGENT correctly stores and roundtrips PERMISSIONS clauses"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
match = """
$result.agents.default_perms = /PERMISSIONS FULL/
AND $result.agents.no_perms = /PERMISSIONS NONE/
AND $result.agents.where_perms = /PERMISSIONS WHERE/
"""
error = false

*/

-- Agent with default permissions (FULL)
DEFINE AGENT default_perms MODEL 'openai:gpt-4' PROMPT 'Hello';

-- Agent with PERMISSIONS NONE
DEFINE AGENT no_perms MODEL 'openai:gpt-4' PROMPT 'Hello' PERMISSIONS NONE;

-- Agent with PERMISSIONS WHERE clause
DEFINE AGENT where_perms MODEL 'openai:gpt-4' PROMPT 'Hello' PERMISSIONS WHERE $auth.role = 'admin';

-- Verify permissions roundtrip via INFO FOR DB
INFO FOR DB;
