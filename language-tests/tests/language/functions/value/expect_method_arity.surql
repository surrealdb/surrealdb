/**
[test]
reason = "Wrong arity on method syntax is reported against the bare method name, `expect()`"

[env]

[[test.results]]
error = "Incorrect arguments for method expect(). Expected 2 to 3 arguments"
*/

9.expect(|$n| $n = 9, "a", "b");