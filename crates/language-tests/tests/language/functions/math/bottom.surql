/**
[env]
imports = ["datasets/type_order.surql"]

[test]

[[test.results]]
error = "Incorrect arguments for function math::bottom(). The second argument must be an integer greater than 0."

[[test.results]]
value = "[1]"

[[test.results]]
value = "[2, 1]"

[[test.results]]
value = "[3, 1, 2]"

[[test.results]]
value = "[3, 1, 2]"

[[test.results]]
value = "[0, -1]"

[[test.results]]
error = "Incorrect arguments for function math::bottom(). Argument 1 was the wrong type. Expected `number` but found `[]` when coercing an element of `array<number>`"

*/

math::bottom([1,2,3], 0);
math::bottom([1,2,3], 1);
math::bottom([1,2,3], 2);
math::bottom([1,2,3], 10);
math::bottom([1,2,3], 3);
math::bottom([-1,0,1,2,3], 2);
math::bottom([[],{}], 2);

