/**
[env]
imports = ["./permissions_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:alice" }

[test]
reason = """
	A table a permission or a field permission governs is read through that permission: the rows an
	id bound would skip are judged by it, and a bound evaluated in the caller's frame would not be
	the definer's. A record user sees what the permission admits for every spelling of an id bound,
	so the bounds never narrow such a table.
"""

[[test.results]]
value = "[owned:3, owned:5, owned:6]"

[[test.results]]
value = "[owned:3]"

[[test.results]]
value = "[owned:6, owned:5, owned:3]"

[[test.results]]
value = "[owned:3]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[owned:3, owned:5, owned:6]"

[[test.results]]
value = "[guarded:2, guarded:3, guarded:4]"

[[test.results]]
value = "[{ id: guarded:2, secret: NONE }, { id: guarded:3, secret: 'c' }, { id: guarded:4, secret: NONE }]"

[[test.results]]
value = "[{ id: guarded:3, secret: 'c' }, { id: guarded:2, secret: NONE }, { id: guarded:1, secret: 'a' }]"

[[test.results]]
value = "[open:3, open:4, open:5]"

[[test.results]]
value = "[open:2, open:3]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[open:3, open:4, open:5]"

[[test.results]]
value = "[]"
*/

SELECT VALUE id FROM owned WHERE id > owned:2;
SELECT VALUE id FROM owned WHERE id >= owned:2 AND id < owned:5;
SELECT VALUE id FROM owned WHERE id > owned:2 ORDER BY id DESC;
SELECT VALUE id FROM owned WHERE id < owned:4 ORDER BY id DESC LIMIT 1;
SELECT VALUE id FROM owned WHERE id > owned:1 AND id < owned:3;
LET $c = owned:2;
SELECT VALUE id FROM owned WHERE id > $c;
SELECT VALUE id FROM guarded WHERE id > guarded:1;
SELECT id, secret FROM guarded WHERE id > guarded:1;
SELECT id, secret FROM guarded WHERE id >= guarded:1 AND id <= guarded:3 ORDER BY id DESC;
SELECT VALUE id FROM open WHERE id > open:2;
SELECT VALUE id FROM open WHERE id >= open:2 AND id < open:4;
LET $o = open:2;
SELECT VALUE id FROM open WHERE id > $o;
SELECT VALUE id FROM denied WHERE id > denied:1;
