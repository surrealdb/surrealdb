/**

[test]

[[test.results]]
value = '''
'Project [ctx: Db]
  field.lookup: GraphEdgeScan [ctx: Db] [direction: <->, tables: person, output: TargetId]
  └────> GraphEdgeScan [ctx: Db] [direction: ->, tables: likes, output: TargetId]
         └────> CurrentValueSource [ctx: Rt]
└────> Scan [ctx: Db] [source: person]
''''

[[test.results]]
value = '''
'Project [ctx: Db]
  field.lookup: Project [ctx: Db]
  └────> Filter [ctx: Db] [predicate: age > 30]
         └────> GraphEdgeScan [ctx: Db] [direction: <-, tables: person, output: FullEdge]
                └────> GraphEdgeScan [ctx: Db] [direction: ->, tables: likes, output: TargetId]
                       └────> CurrentValueSource [ctx: Rt]
└────> Scan [ctx: Db] [source: person]
''''

*/


EXPLAIN SELECT ->likes<->person FROM person;

EXPLAIN SELECT ->likes<-(SELECT * FROM person WHERE age > 30) FROM person;
