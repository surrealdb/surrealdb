/**
[env]

[test]

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: name, names_3rds]
    TableScan [ctx: Db] [table: person, direction: Forward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: @.{..}.{ name, knows: ->knows->person.@ }]
    TableScan [ctx: Db] [table: person, direction: Forward]
''''
*/


EXPLAIN SELECT name, @{3}(->knows->person).name AS names_3rds FROM person;

EXPLAIN SELECT VALUE @{..}.{ name, knows: ->knows->person.@ } FROM person;
