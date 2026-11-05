/**
[env]
namespace = true
database = true

[test]
reason = "A FOR written in value position inside an expression, a SELECT projection, a SET value, a closure body or a field's VALUE clause, runs as a statement whose value is the expression's: NONE when its body returns nothing, the value of a RETURN it takes otherwise, and NONE when a BREAK ends it. Its body reads the row and the names the expression reads, and its writes are made. A FOR in value position that a statement holds directly runs where it stands."

[[test.results]]
value = "[{ id: qv:one, v: 1 }]"

[[test.results]]
value = "[{ id: w:1, v: 2 }]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[{ f: 1 }]"

[[test.results]]
value = "[[1]]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[11]"

[[test.results]]
value = "[[2]]"

[[test.results]]
value = "[2]"

[[test.results]]
value = "6"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[3]"

[[test.results]]
value = "1"
*/

CREATE qv:one SET v = 1;
CREATE w:1 SET v = 2;
SELECT VALUE (FOR $i IN [1, 2] { }) FROM w;
SELECT VALUE (FOR $i IN [1, 2] { RETURN $i + v }) FROM w;
SELECT (FOR $i IN [1, 2] { RETURN $i }) AS f FROM w;
SELECT VALUE (SELECT VALUE (FOR $i IN [1] { RETURN $parent.v }) FROM qv) FROM w;
SELECT VALUE (FOR $i IN [1] { RETURN $i }) FROM w ORDER BY id;
SELECT VALUE (FOR $i IN [1, 2] { IF $i = 2 { BREAK }; }) FROM w;
SELECT VALUE (FOR $i IN [1, 2] { CREATE m SET n = $i }) FROM w;
SELECT VALUE n FROM m ORDER BY n;
UPDATE w:1 SET z = (FOR $i IN [1] { RETURN $i + 10 }) RETURN VALUE z;
SELECT VALUE [1].map(|$x| (FOR $i IN [1] { RETURN $i + $x })) FROM w;
RETURN [1].map(|$x| (FOR $i IN [1] { RETURN $i + $x }));
RETURN (|$x| (FOR $i IN [1] { RETURN $i + $x }))(5);
DEFINE FIELD f ON fw VALUE (FOR $i IN [1] { RETURN 3 });
CREATE fw:1 RETURN VALUE f;
RETURN (FOR $i IN [1, 2] { RETURN $i });
