/**
[test]
reason = "A function that calls itself runs, recursion and all, to its base case, and calls nested within the computation-depth budget return their value."

[[test.results]]
value = "NONE"

[[test.results]]
value = "3628800"

[[test.results]]
value = "1"

[[test.results]]
value = "[6, 120]"

*/

DEFINE FUNCTION fn::fact($n: int) { IF $n <= 1 { RETURN 1 }; RETURN $n * fn::fact($n - 1); };
fn::fact(10);
fn::fact(1);
[fn::fact(3), fn::fact(5)];
