/**
[test]
reason = "A unique-index, ASSERT or typed-LET failure in a block is the error the block reports, ahead of a later THROW, and a write to another table before it is undone"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, n: 1 }]"

[[test.results]]
error = "Database index `u` already contains 1, with record `t:1`"

[[test.results]]
error = "The table 'other' does not exist"

[[test.results]]
error = "Found -1 for field `n`, with record `s:2`, but field must conform to: $value > 0"

[[test.results]]
error = "The table 'w' does not exist"

[[test.results]]
value = "[]"

[[test.results]]
error = "Tried to set `$v`, but couldn't coerce value: Expected `int` but found `'a'`"

[[test.results]]
error = "The table 'x' does not exist"

[[test.results]]
value = "[{ id: y:1 }]"

[[test.results]]
error = "Tried to set `$v`, but couldn't coerce value: Expected `int` but found `'a'`"

[[test.results]]
error = "Database record `y:1` already exists"

[[test.results]]
error = "Could not cast into `int` using input `'q'`"

[[test.results]]
error = "An error occurred: later"

[[test.results]]
error = "The table 'c' does not exist"

[[test.results]]
value = "2"

[[test.results]]
value = "3"

[env]
namespace = "test"
database = "test"

*/

DEFINE INDEX u ON t FIELDS n UNIQUE;
DEFINE FIELD n ON s TYPE int ASSERT $value > 0;
CREATE t:1 SET n = 1;
{ CREATE other:1; CREATE t:2 SET n = 1; THROW 'later'; };
SELECT * FROM other;
{ CREATE s:1 SET n = 1; CREATE w:1; CREATE s:2 SET n = -1; THROW 'later'; };
SELECT * FROM w;
SELECT * FROM s;
{ CREATE x:1; LET $v: int = 'a'; THROW 'later'; };
SELECT * FROM x;
CREATE y:1;
{ LET $v: int = 'a'; CREATE y:1; };
{ CREATE y:1; LET $v: int = 'a'; };
{ IF <int> 'q' { CREATE z:1 }; THROW 'later'; };
{ LET $a = (CREATE c:1); THROW 'later'; };
SELECT * FROM c;
{ 1; 2 };
{ LET $a = 1; LET $b = 2; $a + $b };
