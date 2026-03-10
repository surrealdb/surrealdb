/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The access method 'example' already exists in the database 'test'"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The access method 'example' does not exist in the database 'test'"

[[test.results]]
value = "NONE"

*/

DEFINE ACCESS example ON DATABASE TYPE JWT ALGORITHM HS512 KEY 'secret';
DEFINE ACCESS IF NOT EXISTS example ON DATABASE TYPE JWT ALGORITHM HS512 KEY 'secret';
DEFINE ACCESS OVERWRITE example ON DATABASE TYPE JWT ALGORITHM HS512 KEY 'secret';
DEFINE ACCESS example ON DATABASE TYPE JWT ALGORITHM HS512 KEY 'secret';
REMOVE ACCESS IF EXISTS example ON DB;
REMOVE ACCESS example ON DB;
REMOVE ACCESS IF EXISTS example ON DB;
