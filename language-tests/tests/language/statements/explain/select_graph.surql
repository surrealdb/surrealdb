/**
[env]

[test]

[[test.results]]
value = '''
'Project [ctx: Db]
    TableScan [ctx: Db] [table: person, direction: Forward]
''''

[[test.results]]
value = '''
'Project [ctx: Db]
    TableScan [ctx: Db] [table: person, direction: Forward]
''''

*/


EXPLAIN SELECT ->likes<->person FROM person;

EXPLAIN SELECT ->likes<-(SELECT * FROM person WHERE age > 30) FROM person;
