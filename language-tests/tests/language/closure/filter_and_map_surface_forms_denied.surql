/**
[env.capabilities]
deny-functions = ["array::filter", "array::map"]

[test]
reason = """
A caller denied `array::filter` and `array::map` is refused by both the call and
the method form of a closure-written filter or map, however the base and the body
read. The `[WHERE]` filter calls no function and runs.
"""

[[test.results]]
error = "Function 'array::filter' is not allowed to be executed"

[[test.results]]
error = "Function 'array::filter' is not allowed to be executed"

[[test.results]]
error = "Function 'array::map' is not allowed to be executed"

[[test.results]]
error = "Function 'array::map' is not allowed to be executed"

[[test.results]]
value = "[5, 7, 9]"

*/
array::filter([5, 7, 9], |$v| $v > 6);
[5, 7, 9].filter(|$v| $v > 6);
array::map([1, 2, 3], |$v| [$v]);
[1, 2, 3].map(|$v| [$v]);
[5, 7, 9][WHERE true];
