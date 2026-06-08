/**
[test]
reason = "LET with a declared type inside a block coerces the RHS via the streaming LetPlan. Mismatched types raise SetCoerce; matching types bind unchanged."

[env]
planner-strategy = ["all-ro", "best-effort-ro"]

[[test.results]]
value = "42"

[[test.results]]
error = "Tried to set `$bar`, but couldn't coerce value: Expected `int` but found `'hello'`"

*/

{
    LET $foo: int = 42;
    RETURN $foo;
};

{
    LET $bar: int = 'hello';
    RETURN $bar;
};
