/**
[env]
auth = { level = "viewer" }
imports = ["language/permissions/dml/viewer_select_allowed_import.surql"]

[test]
reason = "Viewer can SELECT data"

[[test.results]]
value = "[{ id: item:1, name: 'alpha' }, { id: item:2, name: 'beta' }]"

*/
SELECT * FROM item ORDER BY id;
