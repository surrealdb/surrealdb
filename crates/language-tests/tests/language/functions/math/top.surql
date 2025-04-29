/**
[test]

[[test.results]]
error = "Incorrect arguments for function math::top(). The second argument must be an integer greater than 0."

[[test.results]]
value = "[3]"

[[test.results]]
value = "[2, 3]"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "[1, 3, 2]"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "[2, 3]"

[[test.results]]
error = "Incorrect arguments for function math::top(). Argument 1 was the wrong type. Expected `number` but found `[]` when coercing an element of `array<number>`"

*/
math::top([1,2,3], 0);
math::top([1,2,3], 1);
math::top([1,2,3], 2);
math::top([1,2,3], 10);
math::top([1,2,3], 3);
math::top([3,2,1], 3);
math::top([2,1,3], 3);
math::top([-1,0,1,2,3], 2);
math::top([[],{}], 2);
