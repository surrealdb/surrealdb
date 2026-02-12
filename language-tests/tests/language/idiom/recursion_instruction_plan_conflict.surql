/**
[test]
reason = "Test that combining recursion instructions with a recursion plan produces an error"

[[test.results]]
error = "Cannot construct a recursion plan when an instruction is provided"
*/

a:1.{..+path}.{ id, links: links.@ };
