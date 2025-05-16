/**
[test]

[[test.results]]
error = "Stream to large to consume, expected a size up to 1b"

[[test.results]]
value = 'b"112233"'

[[test.results]]
value = 'b"112233"'

*/
b"112233".stream().consume("1b");
b"112233".stream().consume("3b");
b"112233".stream().consume("4b");
