/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Expected `regex` but found `'a'`"

[[test.results]]
error = "Expected `regex` but found `1`"

*/
DEFINE FUNCTION fn::takes_regex($r: regex){};
fn::takes_regex(/a/);
fn::takes_regex("a");
fn::takes_regex(1);
