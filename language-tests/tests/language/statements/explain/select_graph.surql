/**

[test]

[[test.results]]
value = '''
"Project [ctx: Db]
  field.lookup: GraphEdgeScan [ctx: Db] [direction: ->, tables: likes, output: TargetId]
  └────> CurrentValueSource [ctx: Rt]
  field.lookup: GraphEdgeScan [ctx: Db] [direction: <->, tables: person, output: TargetId]
  └────> CurrentValueSource [ctx: Rt]
└────> Scan [ctx: Db] [source: person]
"'''

*/


EXPLAIN SELECT ->likes<->person FROM person;