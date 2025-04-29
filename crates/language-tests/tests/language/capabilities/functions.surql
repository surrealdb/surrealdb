/**

[env.capabilities]
allow-functions = ["object::*", "<array>.*"]

[test]

[[test.results]]
value = "[['a', 1]]"

[[test.results]]
error = "Function 'object.entries' is not allowed to be executed"

[[test.results]]
error = "Function 'array::any' is not allowed to be executed"

[[test.results]]
value = "true"

*/

object::entries({ a: 1 });
{ a: 1 }.entries();

array::any([1], 1);
[1].any(1);
