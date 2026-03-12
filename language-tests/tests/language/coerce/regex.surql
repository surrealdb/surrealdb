/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Incorrect arguments for function fn::takes_regex(). Failed to coerce argument `$r`: Expected `regex` but found `'a'`"

[[test.results]]
error = "Incorrect arguments for function fn::takes_regex(). Failed to coerce argument `$r`: Expected `regex` but found `1`"

*/
DEFINE FUNCTION fn::takes_regex($r: regex){};
fn::takes_regex(/a/);
fn::takes_regex("a");
fn::takes_regex(1);
