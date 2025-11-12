/**
[test]

[[test.results]]
value = "[{ id: something:o6h7x2ftw3utzggeud3q, time: d'1970-01-01T00:00:00Z', user: user:test }]"
skip-record-id-key = true

[env]
imports = ["reproductions/5945_create_permission_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:test" }
*/

CREATE something;
