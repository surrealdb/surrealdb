/**
[env]
namespace = true
database = true

[test]
reason = "A statement of a function body that runs over rows of its own (a `SELECT` returned, bound by `LET` or standing as the body's last statement) reads the row the function is called for as `$parent`, whether the call is in a `SELECT` projection or in an `UPDATE` `SET`."

[[test.results]]
value = "[{ a: 1, id: u:1 }]"

[[test.results]]
value = "[{ a: 7, id: w:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[1]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[1]]"

[[test.results]]
value = "[[1]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[1]]"

[[test.results]]
value = "[[1]]"
*/
CREATE u:1 SET a = 1;
CREATE w:1 SET a = 7;
DEFINE FUNCTION fn::p() { RETURN (SELECT VALUE $parent.a FROM w) };
SELECT VALUE fn::p() FROM u:1;
DEFINE FUNCTION fn::q() { LET $x = (SELECT VALUE $parent.a FROM w); RETURN $x };
SELECT VALUE fn::q() FROM u:1;
UPDATE u:1 SET c = fn::q() RETURN VALUE c;
DEFINE FUNCTION fn::r() { SELECT VALUE $parent.a FROM w };
SELECT VALUE fn::r() FROM u:1;
UPDATE u:1 SET c = fn::r() RETURN VALUE c;
