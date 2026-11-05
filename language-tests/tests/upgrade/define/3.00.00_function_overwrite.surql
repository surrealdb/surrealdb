/**
[env]
imports = ["upgrade/define/3.00.00_function_overwrite_import.surql"]

[test]
upgrade = true
reason = """
Functions a release before 3.3 wrote store their argument and return types as a
`Kind`, a revision the current checkout converts on read. `DEFINE FUNCTION …
OVERWRITE` and `ALTER FUNCTION` read the stored definition they replace, so each
must accept that revision: a call before the change runs the old body, a call
after it runs the new one, and the function left alone keeps its converted
signature."""
# ALTER FUNCTION and the current INFO rendering are the current checkout's.
version = ">=3.4.0-a"

[[test.results]]
value = "['hello x', 5, 0]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['hi x', 6, 4]"

[[test.results]]
value = """{ combine: "DEFINE FUNCTION fn::combine($a: int, $b: int) -> int { RETURN $a * $b } COMMENT 'product' PERMISSIONS FULL", greet: "DEFINE FUNCTION fn::greet($name: string) -> string { RETURN 'hi ' + $name } PERMISSIONS FULL", kept: 'DEFINE FUNCTION fn::kept($v: none | int) -> int { RETURN $v ?? 0 } PERMISSIONS FULL' }"""

[[test.results]]
error = "Incorrect arguments for function fn::combine(). Failed to coerce argument `$a`: Expected `int` but found `'a'`"

*/
-- Each function as the old binary wrote it.
RETURN [fn::greet('x'), fn::combine(2, 3), fn::kept(NONE)];
-- Replace a function's body and drop its comment.
DEFINE FUNCTION OVERWRITE fn::greet($name: string) -> string { RETURN 'hi ' + $name; };
-- Alter a function's body and add a comment.
ALTER FUNCTION fn::combine($a: int, $b: int) -> int { RETURN $a * $b; } COMMENT 'product';
-- Calls run the new bodies; `fn::kept` is unchanged.
RETURN [fn::greet('x'), fn::combine(2, 3), fn::kept(4)];
-- The definitions as they now stand.
RETURN (INFO FOR DB).functions;
-- A converted argument type is still enforced.
RETURN fn::combine('a', 1);
