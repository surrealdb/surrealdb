/**
[test]
reason = "Test optional chaining after idiom chain parts"

[[test.results]]
value = "false"

[[test.results]]
value = "NONE"
*/

{}.prop.is_bool();
{}.prop.?.is_bool();
