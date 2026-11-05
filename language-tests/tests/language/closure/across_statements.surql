/**
[env]
namespace = true
database = true

[test]
reason = """
A closure made by one statement and invoked by later ones, as a value: called
directly, handed to closure-taking builtins, held in an object or array,
returned from a function, refused by a DEFINE PARAM, and invoked in a write's
clauses. Bodies that read the clock, define a table, and read a table.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, n: 1 }]"

[[test.results]]
value = "[{ id: t:2, n: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[2, [2]]"

[[test.results]]
value = "[2, 3, 4]"

[[test.results]]
value = "[2]"

[[test.results]]
value = "9"

[[test.results]]
value = "[3, 4]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[20, 6]"

[[test.results]]
value = "12"

[[test.results]]
value = "[5, 10]"

[[test.results]]
error = "A user generated conversion error occured: Closures cannot be stored on disk"

[[test.results]]
error = "There was a problem running the ANONYMOUS() function. 'none' is not a function"

[[test.results]]
error = "Incorrect arguments for function array::map(). Argument 2 was the wrong type. Expected `function` but found `NONE`"

[[test.results]]
value = "[2, 3]"

[[test.results]]
value = "[[1, 2], [2]]"

[[test.results]]
value = "[2, 3]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[1, 2], true]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[['a'], ['b']]"

[[test.results]]
value = "['a', 'b']"

[[test.results]]
error = "Incorrect arguments for function ANONYMOUS(). Expected a value of type 'int' for argument $x"

[[test.results]]
error = "Couldn't coerce return value from function `ANONYMOUS`: Expected `int` but found `'nope'`"
*/

DEFINE TABLE t SCHEMALESS;
CREATE t:1 SET n = 1;
CREATE t:2 SET n = 2;
DEFINE FUNCTION fn::mk($k: int) { RETURN |$x| $x * $k };
LET $f = |$x| $x + 1;
LET $read = |$min| (SELECT VALUE n FROM t WHERE n >= $min ORDER BY n);
RETURN [$f(1), $read(2)];
RETURN array::map([1, 2, 3], $f);
RETURN array::filter([1, 2, 3, 4], |$v| $v IN $read(2));
RETURN array::fold([1, 2, 3], 0, |$acc, $v| $acc + $f($v));
RETURN [1, 2, 3].map($f).filter(|$v| $v > 2);
LET $o = { g: |$x| $x * 10, h: [$f] };
RETURN [$o.g(2), $o.h[0](5)];
RETURN fn::mk(3)(4);
RETURN array::map([1, 2], fn::mk(5));
DEFINE PARAM $dbl VALUE |$x| $x * 2;
RETURN $dbl(21);
RETURN array::map([1, 2], $dbl);
UPDATE t SET m = $f(n) RETURN VALUE m;
SELECT VALUE $read(n) FROM t ORDER BY id;
SELECT VALUE (|$v| $v + n)(1) FROM t ORDER BY id;
LET $clock = || (SELECT VALUE n FROM t WHERE time::now() > d'2000-01-01' ORDER BY n);
RETURN [$clock(), (|| time::now() > d'2000-01-01')()];
LET $make = |$name| { DEFINE TABLE IF NOT EXISTS made SCHEMALESS; CREATE made SET name = $name RETURN VALUE name };
RETURN [$make('a'), $make('b')];
SELECT VALUE name FROM made ORDER BY name;
RETURN (|$x: int| $x)('a');
RETURN (|$x| -> int { $x })('nope');
