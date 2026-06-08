/**
[env]
imports = ["reproductions/codex_b2a3f033_self_record_dereference_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "secrets:test" }

[test]
reason = "Self-record id dereference (e.g. SELECT id.secret) must apply field-level SELECT permissions; previously it bypassed them by fetching the raw stored record (Codex finding b2a3f033)"

[[test.results]]
value = "[{ leaked: NONE }]"

[[test.results]]
value = "[{ public: 'visible' }]"
*/

-- Field secret has PERMISSIONS FOR select NONE. Before the fix, the
-- self-record traversal `id.secret` re-fetched the raw stored record and
-- continued the path on it with `doc=None`, so the field permission was not
-- applied and the secret leaked under a different alias.
SELECT id.secret AS leaked FROM secrets:test;
SELECT public FROM secrets:test;
