/**
[env]
namespace = true
database = true

[test]
reason = "A range written in place compares an enum field by declaration order, as BETWEEN does in SQL: its bounds and the field are ranked by the enum, where the labels' spelling orders them differently"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

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
value = "['b', 'c']"

[[test.results]]
value = "['b', 'c']"

[[test.results]]
value = "['b']"

[[test.results]]
value = "['a']"

[[test.results]]
value = "['b', 'c']"

[[test.results]]
value = "['a', 'c']"

[[test.results]]
value = "['b']"

[[test.results]]
value = "['b', 'c']"

[[test.results]]
value = "['a', 'c']"

[[test.results]]
value = "['c']"

[[test.results]]
error = "'nope' is not a value of the type 'mood'; its values are 'sad', 'ok', 'happy'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['a', 'b', 'c']"
*/
DEFINE TYPE mood AS 'sad' | 'ok' | 'happy';
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
DEFINE FIELD mood ON person TYPE mood;
CREATE person:a SET name = 'a', mood = 'happy' RETURN NONE;
CREATE person:b SET name = 'b', mood = 'sad' RETURN NONE;
CREATE person:c SET name = 'c', mood = 'ok' RETURN NONE;
-- 'ok' sorts before 'sad' by spelling, so each range below is empty or inverted read lexically.
SELECT VALUE name FROM person WHERE mood IN 'sad'..='ok' ORDER BY name;
SELECT VALUE name FROM person WHERE mood INSIDE 'sad'..'happy' ORDER BY name;
SELECT VALUE name FROM person WHERE mood IN 'sad'..'ok' ORDER BY name;
SELECT VALUE name FROM person WHERE mood NOT IN 'sad'..='ok' ORDER BY name;
SELECT VALUE name FROM person WHERE ('sad'..='ok') CONTAINS mood ORDER BY name;
SELECT VALUE name FROM person WHERE mood IN 'ok'.. ORDER BY name;
SELECT VALUE name FROM person WHERE mood IN ..'ok' ORDER BY name;
SELECT VALUE name FROM person WHERE mood IN ..='ok' ORDER BY name;
SELECT VALUE name FROM person WHERE mood IN 'sad'>.. ORDER BY name;
SELECT VALUE name FROM person WHERE mood IN 'sad'>..='ok' ORDER BY name;
SELECT VALUE name FROM person WHERE mood IN 'nope'..='ok';
-- A range bound to a parameter is not written in place and compares the labels as written.
LET $r = 'happy'..='sad';
SELECT VALUE name FROM person WHERE mood IN $r ORDER BY name;
