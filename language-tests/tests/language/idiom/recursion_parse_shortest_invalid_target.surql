/**
[test]
reason = "Test that shortest instruction with a number target produces a parse error"

[test.results]
parsing-error = true
*/

a:1.{..+shortest=123};
