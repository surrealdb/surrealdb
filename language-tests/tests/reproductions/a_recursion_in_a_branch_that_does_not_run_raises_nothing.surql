/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A recursion is a value a write's targets may be read from, and its depth is checked when it is \
evaluated: a write or a recursion with a refused depth in a branch the run does not take answers \
what the branch would have, and raises its refusal only where it is reached.
"""

[[test.results]]
value = "[{ id: post:1, next: post:2 }]"

[[test.results]]
value = "[{ id: post:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1, 2]"

[[test.results]]
error = "Found 0 for bound but expected at least 1."

[[test.results]]
error = "Found 99999 for bound but expected 256 at most."

[[test.results]]
error = "Cannot execute UPDATE statement using value: NONE"

[[test.results]]
error = "Cannot execute UPDATE statement using value: NONE"

[[test.results]]
error = "Cannot execute CREATE statement using value: NONE"

[[test.results]]
error = "Cannot execute DELETE statement using value: NONE"

[[test.results]]
error = "Cannot execute UPSERT statement using value: NONE"

[[test.results]]
value = "[{ id: post:2, seen: true }]"

[[test.results]]
value = "[{ id: post:2, seen: true, twice: true }]"

[[test.results]]
value = "[{ id: post:2, nested: true, seen: true, twice: true }]"

[[test.results]]
value = "[{ id: post:1, next: post:2 }, { id: post:2, nested: true, seen: true, twice: true }]"
*/

CREATE post:1 SET next = post:2;
CREATE post:2;
IF $unset THEN UPDATE ONLY post.{0} END;
LET $c = false;
IF $c THEN UPDATE ONLY post.{0} END;
IF $c THEN RETURN post.{0} END;
IF $c THEN RETURN post.{99999} END;
[1, 2].map(|$v| IF $v > 5 THEN post.{0} ELSE $v END);
UPDATE ONLY post.{0};
RETURN post.{99999};
UPDATE post.{1};
UPDATE ONLY post.{1..2};
CREATE ONLY post.{1};
DELETE post.{1};
UPSERT ONLY post.{1};
UPDATE post:1.{1}.next SET seen = true;
UPDATE post:1.{1..2}.next SET twice = true;
UPDATE post:1.{1}(.next) SET nested = true;
SELECT * FROM post;
