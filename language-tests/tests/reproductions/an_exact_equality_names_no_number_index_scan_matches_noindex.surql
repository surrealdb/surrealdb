/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
wip = true
reason = """
A float and a decimal below one that print alike compare equal yet are keyed
by their exact values, so an index scan for `c = 0.1dec` reads a key that holds
neither `0.1f` nor the other way round. The expected answers are the rows the
comparison equates, which `WITH NOINDEX` returns; the index scan and the
index-only count read the `0.1dec` key alone and return fewer. An object holds
such a number too, and an equality to a regex is sought at the regex's own key,
where no string is stored: the scan errors where `WITH NOINDEX` returns the
strings it matches.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[ks:1, ks:2]"

[[test.results]]
value = "[ks:1, ks:2]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[ko:1, ko:2]"

[[test.results]]
value = "[ko:1, ko:2]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[ko:3]"

[[test.results]]
value = "[ko:3]"
*/
DEFINE INDEX idx_c ON ks FIELDS c;
CREATE ks:1 SET c = 0.1f RETURN NONE;
CREATE ks:2 SET c = 0.1dec RETURN NONE;
SELECT VALUE id FROM ks WHERE c = 0.1dec ORDER BY id;
SELECT VALUE id FROM ks WITH NOINDEX WHERE c = 0.1dec ORDER BY id;
SELECT count() FROM ks WHERE c = 0.1dec GROUP ALL;
SELECT count() FROM ks WITH NOINDEX WHERE c = 0.1dec GROUP ALL;
DEFINE INDEX idx_o ON ko FIELDS c;
CREATE ko:1 SET c = { a: 0.1f } RETURN NONE;
CREATE ko:2 SET c = { a: 0.1dec } RETURN NONE;
CREATE ko:3 SET c = 'zz' RETURN NONE;
SELECT VALUE id FROM ko WHERE c = { a: 0.1dec } ORDER BY id;
SELECT VALUE id FROM ko WITH NOINDEX WHERE c = { a: 0.1dec } ORDER BY id;
SELECT count() FROM ko WHERE c = { a: 0.1dec } GROUP ALL;
SELECT count() FROM ko WITH NOINDEX WHERE c = { a: 0.1dec } GROUP ALL;
SELECT VALUE id FROM ko WHERE c = /zz/ ORDER BY id;
SELECT VALUE id FROM ko WITH NOINDEX WHERE c = /zz/ ORDER BY id;
