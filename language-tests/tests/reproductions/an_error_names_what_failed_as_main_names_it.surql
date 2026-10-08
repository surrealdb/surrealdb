/**
[env]
namespace = true
database = true

[test]
reason = "A user-defined function that does not exist is named with its reason in a read and by its reason alone in code a write reaches, and a computed field whose value does not coerce to its kind is named by the coercion where a statement reads it and by the field where a record is dereferenced."

[[test.results]]
error = "Function 'fn::x' not found: The function 'fn::x' does not exist"

[[test.results]]
error = "Function 'fn::x' not found: The function 'fn::x' does not exist"

[[test.results]]
error = "The function 'fn::x' does not exist"

[[test.results]]
value = "[{ id: a:2 }]"

[[test.results]]
error = "The function 'fn::x' does not exist"

[[test.results]]
error = "The function 'fn::x' does not exist"

[[test.results]]
error = "The function 'fn::x' does not exist"

[[test.results]]
error = "The function 'fn::x' does not exist"

[[test.results]]
error = "Function 'fn::x' not found: The function 'fn::x' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The function 'fn::x' does not exist"

[[test.results]]
value = "[{ id: b:1, n: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `computed` of `b:1`: Expected `int` but found `'x'`"

[[test.results]]
error = "Couldn't coerce value for field `computed` of `b:1`: Expected `int` but found `'x'`"

[[test.results]]
error = "Couldn't coerce value for field `computed` of `b:1`: Expected `int` but found `'x'`"

[[test.results]]
error = "Failed to coerce computed field 'computed'"

[[test.results]]
error = "Failed to coerce computed field 'computed'"

[[test.results]]
error = "Couldn't coerce value for field `computed` of `b:1`: Expected `int` but found `'x'`"

[[test.results]]
error = "Couldn't coerce value for field `computed` of `b:1`: Expected `int` but found `'x'`"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ n: 1 }]"

[[test.results]]
error = "Couldn't coerce value for field `computed` of `b:1`: Expected `int` but found `'x'`"

[[test.results]]
error = "Couldn't coerce value for field `computed` of `b:1`: Expected `int` but found `'x'`"

[[test.results]]
error = "Couldn't coerce value for field `computed` of `b:1`: Expected `int` but found `'x'`"

*/
RETURN fn::x();
SELECT fn::x() FROM 1;
CREATE a:1 SET v = fn::x();
CREATE a:2;
UPDATE a:2 SET v = fn::x();
UPSERT a:3 SET v = fn::x();
INSERT INTO a { id: a:4, v: fn::x() };
DELETE a:2 WHERE fn::x();
LET $v = fn::x();
DEFINE FIELD w ON a VALUE fn::x();
CREATE a:5;
CREATE b:1 SET n = 1;
DEFINE FIELD computed ON b COMPUTED 'x' TYPE int;
SELECT * FROM b;
SELECT computed FROM b;
SELECT VALUE computed FROM ONLY b:1;
RETURN b:1.computed;
RETURN b:1.*;
SELECT * FROM b:1;
SELECT * FROM b WHERE computed = 1;
SELECT count() FROM b GROUP ALL;
SELECT n FROM b;
UPDATE b:1 SET n = 2;
UPDATE b:1 SET n = 3 RETURN NONE;
DELETE b:1 RETURN BEFORE;
