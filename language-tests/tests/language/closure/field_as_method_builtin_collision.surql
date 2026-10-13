/**
[env]
planner-strategy = ["compute-only", "all-ro", "best-effort-ro"]

[test]
reason = "Method-call syntax on an object resolves builtin methods first, by receiver type: a builtin with an implementation for objects (`len`) wins over a same-named closure field, while a builtin method name with no object implementation (`get` exists for files only) falls back to invoking the object's closure field of that name. A colliding name with no closure field is the unknown-method error. Every planner strategy must apply the same precedence and wording."

[[test.results]]
value = "NONE"

[[test.results]]
value = "['a']"

[[test.results]]
value = "'x!'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "2"

[[test.results]]
value = "NONE"

[[test.results]]
error = "There was a problem running the get() function. no such method found for the object type"

*/

LET $obj = { get: || ['a'] };

$obj.get();

{ get: |$k: string| $k + '!' }.get('x');

LET $builtin = { len: || 42, other: true };

$builtin.len();

LET $plain = { x: 1 };

$plain.get('x');
