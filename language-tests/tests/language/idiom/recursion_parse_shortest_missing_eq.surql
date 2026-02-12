/**
[test]
reason = "Test that shortest instruction without = target produces a parse error"

[test.results]
parsing-error = true
*/

a:1.{..+shortest};
