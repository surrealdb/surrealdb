/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "'ab'"

*/

DEFINE FUNCTION fn::test($first: string, $last: string) {
    1;
    2;
    $first + $last
};
fn::test("a","b");
