/**
[env]
backend = ["surrealkv"]
versioned = true
timeout = 5000

[test]
reason = "A pipeline planned by a nested planner is still under the enclosing VERSION, so it declines the optimisations a versioned read declines: it reads the table at the stamp, not through indexes that hold the state after it"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 0, b: 9, id: t:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [predicate: a = 1 OR b = 2]
        BitmapScan [ctx: Db] [table: t]
            BitmapOr [ctx: Db]
                BitmapIndexScan [ctx: Db] [index: ta, access: = 1]
                BitmapIndexScan [ctx: Db] [index: tb, access: = 2]
''''

[[test.results]]
value = '''
'VersionScope [ctx: Db] [version: $t1]
    SelectProject [ctx: Db] [projections: *]
        TableScan [ctx: Db] [table: t, direction: Forward, predicate: a = 1 OR b = 2, pre_decode_filter: yes]
''''

[[test.results]]
value = '''
'VersionScope [ctx: Db] [version: $t1]
    ProjectValue [ctx: Db] [expr: (
  Project[*]
    Filter[a = 1 OR b = 2]
      Source[t]
)]
        RecordIdScan [ctx: Db] [record_id: anchor:0]
''''

[[test.results]]
value = "[[t:1, t:2]]"
*/

-- An index holds the state the last write left, and bitmap fusion resolves an
-- OR over indexes through doc-ID mappings of it, so a versioned read takes no
-- index path. The decision is made where the source is planned, against that
-- planner's own version rather than the statement's, so a pipeline reached
-- through a nested planner has to carry the version it is running under. A
-- subquery in a projection is such a pipeline, and the rows it returns are
-- what show it read the table at the stamp.

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
SLEEP 50ms;

-- After the stamp the indexes no longer answer what the table held at it.
UPDATE t:1 SET a = 0;
DELETE t:2;

-- Unversioned: fusion applies.
EXPLAIN SELECT * FROM t WHERE a = 1 OR b = 2;

-- Versioned on the statement itself: fusion declines.
EXPLAIN SELECT * FROM t WHERE a = 1 OR b = 2 VERSION $t1;

-- Versioned through an enclosing statement: the subquery is planned by a
-- nested planner and declines it too. A subquery in a projection renders as
-- its logical form, so the rows below are what show it read at the stamp.
EXPLAIN SELECT VALUE (SELECT * FROM t WHERE a = 1 OR b = 2) FROM anchor:0 VERSION $t1;

-- The rows the versioned subquery returns.
SELECT VALUE (SELECT VALUE id FROM t WHERE a = 1 OR b = 2) FROM anchor:0 VERSION $t1;
