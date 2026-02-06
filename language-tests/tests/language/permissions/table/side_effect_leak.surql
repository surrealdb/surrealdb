
/**
[env]
imports = ["./basic_import.surql"]
auth = { namespace = "test", database = "test", access = "test", rid = "user:test" }

[test]
reason = "Do not leak data from unselectable tables through side effects"
wip = true

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: full:1 }, { id: full:2 }, { id: full:3 }, { id: full:4 }]"
*/

// With use of $parent
UPDATE `update`:1 SET v = (CREATE full:2 SET data = $parent);
// With use of $this
UPDATE `update`:1 SET v = (SELECT VALUE (CREATE full:3 SET data = $parent) FROM $this);
// With use of block through param
UPDATE `update`:1 SET v = {
	let $secret = secret;
	CREATE full:4 SET data = $secret;
};
SELECT * FROM full;
