/**
[test]
reason = "Setup: a multi-tenant table whose permission compares `id.tenant` to `$auth.id.tenant`. Two records, both signed in as a tenant 'b' record user. Codex finding c67c7232: `id.tenant` must reference the immutable record-id component, NOT a mutable document field of the same name — otherwise the row's stored `tenant` field could be spoofed to grant cross-tenant access."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid );

DEFINE TABLE tenanted SCHEMALESS
	PERMISSIONS
		FOR select WHERE id.tenant = $auth.id.tenant
		FOR create FULL
		FOR update, delete NONE;

-- notes:{ tenant: 'a', key: 1 } has a stored document field `tenant` of 'b'
-- (a spoof). Without the fix, the SELECT permission compares the doc-level
-- `tenant` field (== 'b') with `$auth.id.tenant` (== 'b' for tenant b user)
-- and lets tenant 'b' read tenant 'a' data.
CREATE tenanted:{ tenant: 'a', key: 1 } SET tenant = 'b';
CREATE tenanted:{ tenant: 'b', key: 2 } SET tenant = 'b';
