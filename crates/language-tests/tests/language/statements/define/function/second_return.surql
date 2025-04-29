/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "'ab'"

*/

DEFINE FUNCTION fn::test($first: string, $last: string) {
    "Some other value";
    RETURN $first + $last;
    RETURN "ERROR"
};
fn::test("a","b");
