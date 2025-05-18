/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "'b\"\".stream()'"

[[test.results]]
value = "'Hello World!'"

*/
LET $stream = "Hello World!".to_bytes().stream();

$stream.to_string();

$stream.consume().to_string();
