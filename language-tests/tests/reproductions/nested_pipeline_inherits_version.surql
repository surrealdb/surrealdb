/**
[env]
backend = ["rocksdb", "surrealkv"]
versioned = true
timeout = 5000
planner-strategy = ["all-ro"]

[test]
reason = "A pipeline planned by a nested planner is still under the enclosing VERSION, so it declines the optimisations a versioned read declines"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [predicate: a = 1 OR b = 2]
        BitmapResolve [ctx: Db] [table: t]
            BitmapOr [ctx: Db]
                BitmapIndexScan [ctx: Db] [index: ta, access: = 1]
                BitmapIndexScan [ctx: Db] [index: tb, access: = 2]
''''

[[test.results]]
value = '''
'VersionScope [ctx: Db] [version: $t1]
    SelectProject [ctx: Db] [projections: *]
        Filter [ctx: Db] [predicate: a = 1 OR b = 2]
            UnionIndexScan [ctx: Db] [table: t, branches: 2]
                IndexScan [ctx: Db] [index: ta, access: = 1, direction: Forward]
                IndexScan [ctx: Db] [index: tb, access: = 2, direction: Forward]
''''

[[test.results]]
value = '''
'VersionScope [ctx: Db] [version: $t1]
    ProjectValue [ctx: Db] [expr: (SelectProject)]
      expr.subquery: SelectProject [ctx: Db] [projections: *]
          Filter [ctx: Db] [predicate: a = 1 OR b = 2]
              UnionIndexScan [ctx: Db] [table: t, branches: 2]
                  IndexScan [ctx: Db] [index: ta, access: = 1, direction: Forward]
                  IndexScan [ctx: Db] [index: tb, access: = 2, direction: Forward]
        RecordIdScan [ctx: Db] [record_id: anchor:0, version: $t1]
''''

[[test.results]]
value = "[[t:1, t:2]]"
*/

-- Bitmap fusion resolves an OR over indexes through doc-ID mappings, which
-- are not time-travel-aware, so a versioned read declines it. The decision
-- is made where the source is planned, against that planner's own version
-- rather than the statement's, so a pipeline reached through a nested
-- planner has to carry the version it is running under. A subquery in a
-- projection is such a pipeline.

{
	DEFINE TABLE t SCHEMALESS CHANGEFEED 1h;
	DEFINE INDEX ta ON t FIELDS a;
	DEFINE INDEX tb ON t FIELDS b;
	DEFINE TABLE anchor SCHEMALESS CHANGEFEED 1h;
	CREATE t:1 SET a = 1, b = 9;
	CREATE t:2 SET a = 9, b = 2;
	CREATE anchor:0 SET k = 1;
	RETURN "OK";
};

SLEEP 50ms;
LET $t1 = time::now();

-- Unversioned: fusion applies.
EXPLAIN SELECT * FROM t WHERE a = 1 OR b = 2;

-- Versioned on the statement itself: fusion declines.
EXPLAIN SELECT * FROM t WHERE a = 1 OR b = 2 VERSION $t1;

-- Versioned through an enclosing statement: the subquery is planned by a
-- nested planner and declines it too.
EXPLAIN SELECT VALUE (SELECT * FROM t WHERE a = 1 OR b = 2) FROM anchor:0 VERSION $t1;

-- The rows the versioned subquery returns.
SELECT VALUE (SELECT VALUE id FROM t WHERE a = 1 OR b = 2) FROM anchor:0 VERSION $t1;
