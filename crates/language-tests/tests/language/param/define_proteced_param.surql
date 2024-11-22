/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ other: true, some: 'thing' }]"

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

*/

LET $test = { some: 'thing', other: true };
SELECT * FROM $test WHERE some = 'thing';
LET $auth = { ID: admin:tester };
