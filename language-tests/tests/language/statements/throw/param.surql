/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
error = "An error occurred: there was an error"

*/
LET $err = 'there was an error';
THROW $err;
