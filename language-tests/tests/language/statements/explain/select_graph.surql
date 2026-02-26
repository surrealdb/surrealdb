/**

[test]

[[test.results]]
value = ''''Project [ctx: Db]
  field.lookup: GraphEdgeScan [ctx: Db] [direction: <->, tables: person, output: TargetId]
      GraphEdgeScan [ctx: Db] [direction: ->, tables: likes, output: TargetId]
          CurrentValueSource [ctx: Rt]
    TableScan [ctx: Db] [table: person, direction: Forward]
''''

[[test.results]]
value = ''''Project [ctx: Db]
  field.lookup: SelectProject [ctx: Db] [projections: *]
      Filter [ctx: Db] [predicate: age > 30]
          GraphEdgeScan [ctx: Db] [direction: <-, tables: person, output: FullEdge]
              GraphEdgeScan [ctx: Db] [direction: ->, tables: likes, output: TargetId]
                  CurrentValueSource [ctx: Rt]
    TableScan [ctx: Db] [table: person, direction: Forward]
''''

*/


EXPLAIN SELECT ->likes<->person FROM person;

EXPLAIN SELECT ->likes<-(SELECT * FROM person WHERE age > 30) FROM person;
