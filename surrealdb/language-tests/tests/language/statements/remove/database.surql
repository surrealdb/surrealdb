/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The database 'example' already exists"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The database 'example' does not exist"

[[test.results]]
value = "NONE"

*/

DEFINE DATABASE example;
DEFINE DATABASE IF NOT EXISTS example;
DEFINE DATABASE OVERWRITE example;
DEFINE DATABASE example;
REMOVE DATABASE IF EXISTS `example`;
REMOVE DATABASE example;
REMOVE DATABASE IF EXISTS example;
