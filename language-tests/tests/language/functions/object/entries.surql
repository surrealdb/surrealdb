/**
[test]

[[test.results]]
value = "[['a', 1], ['b', 2]]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[' ', 1]]"

[[test.results]]
error = "Incorrect arguments for function object::entries(). Argument 1 was the wrong type. Expected `object` but found `[]`"


*/
object::entries({ a: 1, b: 2 });
object::entries({ });
object::entries({ " ":1 });
object::entries([]);
