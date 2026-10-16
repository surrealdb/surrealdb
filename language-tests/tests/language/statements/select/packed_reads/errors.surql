/**
[env]
namespace = true
database = true

[test]
reason = "A filter over a SCHEMAFULL table that calls a function which fails on some rows raises the same first error, naming the same row, whichever fields it reads of the rows before them."

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
value = "NONE"

[[test.results]]
value = "[{ id: item:1, n: 1, o: 1, s: '10' }]"

[[test.results]]
value = "[{ id: item:2, n: 2, s: 'two' }]"

[[test.results]]
value = "[{ id: item:3, n: 3, o: 3, s: '30' }]"

[[test.results]]
value = "[{ id: item:4, n: 4, o: 4, s: 'four' }]"

[[test.results]]
error = "An error occurred: boom 3"

[[test.results]]
error = "An error occurred: boom 3"

[[test.results]]
error = "An error occurred: boom 3"

[[test.results]]
value = "[{ id: item:1, n: 1, o: 1, s: '10' }, { id: item:2, n: 2, s: 'two' }, { id: item:3, n: 3, o: 3, s: '30' }, { id: item:4, n: 4, o: 4, s: 'four' }]"

[[test.results]]
error = "Could not cast into `int` using input `'two'`"

[[test.results]]
error = "Could not cast into `int` using input `'two'`"

[[test.results]]
error = "Could not cast into `int` using input `'four'`"

[[test.results]]
error = "Could not cast into `int` using input `'two'`"

[[test.results]]
value = "[item:1, item:3, item:4]"

[[test.results]]
error = "Could not cast into `int` using input `'two'`"

[[test.results]]
error = "An error occurred: boom 3"

[[test.results]]
error = "An error occurred: boom 3"

[[test.results]]
value = "[{ id: item:4, n: 4, o: 4, s: 'four' }, { id: item:3, n: 3, o: 3, s: '30' }]"

[[test.results]]
value = "[{ id: item:1, n: 1, o: 1, s: '10' }, { id: item:2, n: 2, s: 'two' }]"
*/

DEFINE TABLE item SCHEMAFULL;
DEFINE FIELD n ON item TYPE int;
DEFINE FIELD s ON item TYPE string;
DEFINE FIELD o ON item TYPE option<int>;
DEFINE FUNCTION fn::boom($x: int) { IF $x > 2 { THROW 'boom ' + <string> $x }; RETURN true; };
DEFINE FUNCTION fn::fine($x: int) { RETURN $x > 0; };
CREATE item:1 CONTENT { n: 1, s: '10', o: 1 };
CREATE item:2 CONTENT { n: 2, s: 'two' };
CREATE item:3 CONTENT { n: 3, s: '30', o: 3 };
CREATE item:4 CONTENT { n: 4, s: 'four', o: 4 };

SELECT * FROM item WHERE fn::boom(n);
SELECT VALUE id FROM item WHERE fn::boom(n);
SELECT * FROM item WHERE n > 0 AND fn::boom(n);
SELECT * FROM item WHERE fn::fine(n);
SELECT * FROM item WHERE type::int(s) > 5;
SELECT VALUE id FROM item WHERE type::int(s) > 5;
SELECT * FROM item WHERE n < 3 OR type::int(s) > 5;
SELECT VALUE id FROM item WHERE n < 3 AND type::int(s) > 5;
SELECT VALUE id FROM item WHERE o != NONE AND math::sqrt(o - 2) > 0;
SELECT VALUE id FROM item WHERE type::int(s) > 5 OR n > 2;
SELECT count() FROM item WHERE fn::boom(n) GROUP ALL;
SELECT n, s FROM item WHERE fn::boom(n);
SELECT * FROM item ORDER BY n DESC LIMIT 2;
SELECT * FROM item WHERE n IN [1, 2];
