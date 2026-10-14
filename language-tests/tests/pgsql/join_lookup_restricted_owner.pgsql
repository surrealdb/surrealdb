/**
[env]
namespace = true
database = true
imports = ["pgsql/_join_lookup_restricted.surql"]
auth = { level = "owner" }

[test]
reason = "The joins of join_lookup_restricted_record.pgsql as the owner, who reads the column the record user may not: every pairing the stored values give is there."

[[test.results]]
value = "[{ n: 1, tag: 'x' }, { n: 2, tag: 'y' }, { n: 3, tag: 'z' }]"

[[test.results]]
value = "[{ n: 1, tag: 'x' }, { n: 2, tag: 'y' }, { n: 3, tag: 'z' }]"

[[test.results]]
value = "[{ n: 1, tag: 'x' }, { n: 2, tag: 'y' }]"

[[test.results]]
value = "[{ n: 1, tag: 'x' }, { n: 2, tag: 'y' }]"

[[test.results]]
value = "[{ n: 1, tag: 'x' }, { n: 2, tag: 'y' }]"
*/

SELECT a.n, b.tag FROM a JOIN b ON a.k = b.k ORDER BY a.n;

SELECT a.n, b.tag FROM a LEFT JOIN b ON a.k = b.k ORDER BY a.n;

SELECT a.n, b.tag FROM a JOIN b ON a.j = b.j ORDER BY a.n;

SELECT a.n, b.tag FROM a JOIN b ON a.k = b.k AND a.j = b.j ORDER BY a.n;

SELECT a.n, b.tag FROM a JOIN b ON a.j = b.j AND a.k = b.k ORDER BY a.n;
