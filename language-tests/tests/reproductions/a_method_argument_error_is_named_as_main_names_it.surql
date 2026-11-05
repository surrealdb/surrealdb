/**
[env]
namespace = true
database = true

[test]
reason = "An argument error raised in a method call is named as main names it. In a read (RETURN, LET, a block, a SELECT, a function body a read calls) the method names every argument error raised beneath it as its own, the body of a closure it runs included. In code a write reaches (a data clause, a WHERE, a field's VALUE or DEFAULT, a computed field, an event, a function body a write calls, a SELECT nested in a write) and in a schema statement's evaluated value, the method names its own argument error as the function it is, and an error a closure it runs raises keeps the name of the function that raised it."

[[test.results]]
value = "NONE"

[[test.results]]
error = "Incorrect arguments for method map(). Failed to coerce argument `$s`: Expected `string` but found `['a']`"

[[test.results]]
error = "Incorrect arguments for method map(). Argument 2 was the wrong type. Expected `function` but found `1`"

[[test.results]]
error = "Incorrect arguments for method len(). Expected 1 argument"

[[test.results]]
error = "Incorrect arguments for method map(). Argument 1 was the wrong type. Expected `string` but found `['a']`"

[[test.results]]
error = "Incorrect arguments for function fn::up(). Failed to coerce argument `$s`: Expected `string` but found `['a']`"

[[test.results]]
error = "Incorrect arguments for function map(). Argument 2 was the wrong type. Expected `function` but found `1`"

[[test.results]]
error = "Incorrect arguments for function len(). Expected 1 argument"

[[test.results]]
error = "Incorrect arguments for function string::uppercase(). Argument 1 was the wrong type. Expected `string` but found `['a']`"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Incorrect arguments for function map(). Argument 2 was the wrong type. Expected `function` but found `1`"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Incorrect arguments for function string::uppercase(). Argument 1 was the wrong type. Expected `string` but found `['a']`"

[[test.results]]
value = "[{ id: x:8, q: 1 }]"

[[test.results]]
error = "Incorrect arguments for function fn::up(). Failed to coerce argument `$s`: Expected `string` but found `['a']`"

[[test.results]]
error = "Incorrect arguments for method map(). Failed to coerce argument `$s`: Expected `string` but found `['a']`"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Incorrect arguments for method map(). Argument 2 was the wrong type. Expected `function` but found `1`"

[[test.results]]
error = "Incorrect arguments for method map(). Argument 2 was the wrong type. Expected `function` but found `1`"

[[test.results]]
error = "Incorrect arguments for method map(). Argument 2 was the wrong type. Expected `function` but found `1`"

[[test.results]]
error = "Incorrect arguments for method map(). Argument 2 was the wrong type. Expected `function` but found `1`"

[[test.results]]
error = "Incorrect arguments for method map(). Argument 2 was the wrong type. Expected `function` but found `1`"

[[test.results]]
error = "Incorrect arguments for method map(). Argument 2 was the wrong type. Expected `function` but found `1`"

[[test.results]]
error = "Incorrect arguments for method map(). Argument 2 was the wrong type. Expected `function` but found `1`"

[[test.results]]
error = "Incorrect arguments for function map(). Argument 2 was the wrong type. Expected `function` but found `1`"

[[test.results]]
error = "Incorrect arguments for function map(). Argument 2 was the wrong type. Expected `function` but found `1`"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Incorrect arguments for function map(). Argument 2 was the wrong type. Expected `function` but found `1`"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event ev: Incorrect arguments for function map(). Argument 2 was the wrong type. Expected `function` but found `1`"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Incorrect arguments for function map(). Argument 2 was the wrong type. Expected `function` but found `1`"

[[test.results]]
error = "The table 'c' does not exist"

[[test.results]]
error = "Incorrect arguments for function map(). Argument 2 was the wrong type. Expected `function` but found `1`"

[[test.results]]
error = "Incorrect arguments for function map(). Argument 2 was the wrong type. Expected `function` but found `1`"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Incorrect arguments for function map(). Argument 2 was the wrong type. Expected `function` but found `1`"

[[test.results]]
error = "Incorrect arguments for method map(). Argument 2 was the wrong type. Expected `function` but found `1`"

[[test.results]]
error = "Incorrect arguments for method map(). Argument 2 was the wrong type. Expected `function` but found `1`"

*/
DEFINE FUNCTION fn::up($s: string) { RETURN string::uppercase($s) };
RETURN [['a']].map(|$t| fn::up($t));
RETURN [1].map(1);
RETURN 'a'.len(5);
RETURN [['a']].map(|$t| string::uppercase($t));
CREATE x:1 SET v = [['a']].map(|$t| fn::up($t));
CREATE x:2 SET v = [1].map(1);
CREATE x:3 SET v = 'a'.len(5);
CREATE x:4 SET v = [['a']].map(|$t| string::uppercase($t));
DEFINE FIELD w ON y VALUE [1].map(1);
CREATE y:1;
DEFINE FIELD w ON z VALUE [['a']].map(|$t| string::uppercase($t));
CREATE z:1;
CREATE x:8 SET q = 1;
UPDATE x:8 SET v = [["a"]].map(|$t| fn::up($t)) WHERE [["a"]].map(|$t| fn::up($t));
SELECT * FROM [1] WHERE [['a']].map(|$t| fn::up($t));
DEFINE FUNCTION fn::g() { RETURN [1].map(1) };
LET $a = [1].map(1);
IF true { [1].map(1) };
FOR $i IN [1] { [1].map(1) };
{ [1].map(1) };
RETURN fn::g();
SELECT * FROM [1].map(1);
SELECT [1].map(1) FROM 1;
CREATE c:1 SET v = (SELECT VALUE [1].map(1) FROM 1);
CREATE c:2 SET v = fn::g();
DEFINE FIELD comp ON d COMPUTED [1].map(1);
CREATE d:1;
SELECT * FROM d;
DEFINE EVENT ev ON e THEN { CREATE log SET v = [1].map(1) };
CREATE e:1;
DEFINE FIELD dd ON g DEFAULT [1].map(1);
CREATE g:1;
DELETE c WHERE [1].map(1);
RELATE c:1->r->c:1 SET v = [1].map(1);
INSERT INTO h { v: [1].map(1) };
DEFINE TABLE pt PERMISSIONS FOR select WHERE [1].map(1);
DEFINE PARAM $pp VALUE [1].map(1);
RETURN { [1].map(1) };
SELECT * FROM 1 WHERE [1].map(1);
