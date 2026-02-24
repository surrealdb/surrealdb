/**

[test]

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: name, names_3rds]
    Compute [ctx: Db] [fields: names_3rds = @.{3}(->knows->person).name]
      names_3rds.recurse: Recurse [ctx: Db] [depth: 3, instruction: default]
          GraphEdgeScan [ctx: Db] [direction: ->, tables: person, output: TargetId]
              GraphEdgeScan [ctx: Db] [direction: ->, tables: knows, output: TargetId]
                  CurrentValueSource [ctx: Rt]
        TableScan [ctx: Db] [table: person, direction: Forward]
''''

[[test.results]]
value = ''''ProjectValue [ctx: Db] [expr: @.{..}.{ name, knows: ->knows->person.@ }]
  expr.recurse: Recurse [ctx: Db] [depth: 1.., instruction: default, pattern: tree]
    TableScan [ctx: Db] [table: person, direction: Forward]
''''
*/


EXPLAIN SELECT name, @{3}(->knows->person).name AS names_3rds FROM person;

EXPLAIN SELECT VALUE @{..}.{ name, knows: ->knows->person.@ } FROM person;