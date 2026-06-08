/**
[env]
imports = ["reproductions/codex_c67c7232_composite_id_perm_spoofing_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "tenanted:{ tenant: 'b', key: 2 }" }

[test]
reason = "Codex finding c67c7232: `PERMISSIONS WHERE id.tenant = $auth.id.tenant` must check the immutable record-id `tenant` component, not the mutable document field of the same name. The first row's stored `tenant` field is spoofed to 'b' but its actual id-key tenant is 'a', so tenant-'b' user must NOT see it."

[[test.results]]
value = "[tenanted:{ key: 2, tenant: 'b' }]"

*/

SELECT VALUE id FROM tenanted ORDER BY id;
