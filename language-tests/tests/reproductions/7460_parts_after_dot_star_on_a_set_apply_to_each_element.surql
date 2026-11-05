/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
issue = 7460
reason = """
`.*` over a set yields its elements, and every part after it applies to each
element, as it does over an array, with the results collected back into a set.
Over `{[1, 2], [3]}`, `.*[0]` is `{1, 3}`, `.*[$]` is `{2, 3}`, `.*.len()` is
`{1, 2}` and `.*[WHERE $this > 1]` is `{[2], [3]}`. A predicate, a COMPUTED
field and an UPDATE's condition read the same value on every engine, so
`ss.*[0] = [1, 2]` matches nothing and `ss.*[0] CONTAINS 3` matches `n:1`.
"""

[[test.results]]
value = "[{ a: [[1, 2], [3]], id: n:1, ss: {[1, 2], [3]} }]"

[[test.results]]
value = "[{1, 3}]"

[[test.results]]
value = "[{2, 3}]"

[[test.results]]
value = "[{1, 2}]"

[[test.results]]
value = "[{[2], [3]}]"

[[test.results]]
value = "[[1, 3]]"

[[test.results]]
value = "[n:1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[n:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{1, 3}]"

[[test.results]]
value = "[n:1]"
*/
CREATE n:1 SET ss = <set>[[1, 2], [3]], a = [[1, 2], [3]];
SELECT VALUE ss.*[0] FROM n:1;
SELECT VALUE ss.*[$] FROM n:1;
SELECT VALUE ss.*.len() FROM n:1;
SELECT VALUE ss.*[WHERE $this > 1] FROM n:1;
-- The same part over an array, for comparison.
SELECT VALUE a.*[0] FROM n:1;
SELECT VALUE id FROM n WHERE ss.*[0] CONTAINS 3;
SELECT VALUE id FROM n WHERE ss.*[0] = [1, 2];
UPDATE n SET hit = true WHERE ss.*[0] CONTAINS 3 RETURN VALUE id;
DEFINE FIELD firsts ON n COMPUTED ss.*[0];
SELECT VALUE firsts FROM n:1;
SELECT VALUE id FROM n WHERE firsts CONTAINS 3;
