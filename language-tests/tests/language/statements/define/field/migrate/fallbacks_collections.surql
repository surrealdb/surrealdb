/**
[test]
reason = "A coercion that loses a number inside a collection takes the FALLBACK. A set orders numbers of every variant as one line and merges those that order as equal, so an element is judged against what it became rather than against the element at its position, and a set must hold apart every element the value held apart. A number that converts back to the value it was keeps its place, and an array's duplicates merge as a set merges them. Each value is read through `<string>`, which pins the variant of every number."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['{,}', '{,}', '{1f, 2f}']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['{,}', '{1f,}', '{1f, 2f}']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['[]', '[{1f, 2f}]']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['{,}', '{[1f], [2f]}']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['{,}', '{1f, 2f}']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['-1f', '1.5f']"
*/
-- Two ints that round to one float would merge; one that rounds alone would change.
DEFINE TABLE s SCHEMAFULL;
DEFINE FIELD v ON s TYPE set<int>;
CREATE s:1 SET v = <set>[9007199254740992, 9007199254740993] RETURN NONE;
CREATE s:2 SET v = <set>[9007199254740993] RETURN NONE;
CREATE s:3 SET v = <set>[1, 2] RETURN NONE;
ALTER FIELD v ON s TYPE set<float> MIGRATE EXISTING ROWS AS v_float USING $value FALLBACK {,};
SELECT VALUE <string>v FROM s ORDER BY id;
-- An array that becomes a set: a merge the rounding causes is a loss, a duplicate the array held is not.
DEFINE TABLE a SCHEMAFULL;
DEFINE FIELD v ON a TYPE array<int>;
CREATE a:1 SET v = [9007199254740992, 9007199254740993] RETURN NONE;
CREATE a:2 SET v = [1, 1] RETURN NONE;
CREATE a:3 SET v = [2, 1] RETURN NONE;
ALTER FIELD v ON a TYPE set<float> MIGRATE EXISTING ROWS AS v_set USING $value FALLBACK {,};
SELECT VALUE <string>v FROM a ORDER BY id;
-- A set beneath an array.
DEFINE TABLE n SCHEMAFULL;
DEFINE FIELD v ON n TYPE array<set<int>>;
CREATE n:1 SET v = [<set>[9007199254740993]] RETURN NONE;
CREATE n:2 SET v = [<set>[1, 2]] RETURN NONE;
ALTER FIELD v ON n TYPE array<set<float>> MIGRATE EXISTING ROWS AS v_float USING $value FALLBACK [];
SELECT VALUE <string>v FROM n ORDER BY id;
-- An array beneath a set.
DEFINE TABLE sa SCHEMAFULL;
DEFINE FIELD v ON sa TYPE any;
CREATE sa:1 SET v = <set>[[9007199254740993], [1]] RETURN NONE;
CREATE sa:2 SET v = <set>[[2], [1]] RETURN NONE;
ALTER FIELD v ON sa TYPE set<array<float>> MIGRATE EXISTING ROWS AS v_float USING $value FALLBACK {,};
SELECT VALUE <string>v FROM sa ORDER BY id;
-- The member of a union the value coerces to.
DEFINE TABLE u SCHEMAFULL;
DEFINE FIELD v ON u TYPE set<int>;
CREATE u:1 SET v = <set>[9007199254740993] RETURN NONE;
CREATE u:2 SET v = <set>[1, 2] RETURN NONE;
ALTER FIELD v ON u TYPE option<set<float>> MIGRATE EXISTING ROWS AS v_float USING $value FALLBACK {,};
SELECT VALUE <string>v FROM u ORDER BY id;
-- A decimal that does not convert back from the float it becomes.
DEFINE TABLE d SCHEMAFULL;
DEFINE FIELD v ON d TYPE decimal;
CREATE d:1 SET v = 0.10000000000000000555dec RETURN NONE;
CREATE d:2 SET v = 1.5dec RETURN NONE;
ALTER FIELD v ON d TYPE float MIGRATE EXISTING ROWS AS v_float USING $value FALLBACK -1f;
SELECT VALUE <string>v FROM d ORDER BY id;
