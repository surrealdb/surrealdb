/**
[env]
namespace = true
database = true
imports = ["pgsql/_join_lookup_restricted.surql"]
auth = { namespace = "test", database = "test", access = "test", rid = "users:alice" }

[test]
reason = "A join as a record user whose right key column has PERMISSIONS FOR select NONE: the pairing is judged over the field view, so the hidden key pairs nothing, whichever index the right input is read through. The unrestricted indexed column pairs, alone and as the second key pair of a join whose first pair is the hidden one."

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ n: 1, tag: NONE }, { n: 2, tag: NONE }, { n: 3, tag: NONE }]"

[[test.results]]
value = "[{ n: 1, tag: 'x' }, { n: 2, tag: 'y' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/

SELECT a.n, b.tag FROM a JOIN b ON a.k = b.k ORDER BY a.n;

SELECT a.n, b.tag FROM a LEFT JOIN b ON a.k = b.k ORDER BY a.n;

SELECT a.n, b.tag FROM a JOIN b ON a.j = b.j ORDER BY a.n;

SELECT a.n, b.tag FROM a JOIN b ON a.k = b.k AND a.j = b.j ORDER BY a.n;

SELECT a.n, b.tag FROM a JOIN b ON a.j = b.j AND a.k = b.k ORDER BY a.n;
