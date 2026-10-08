/**
[test]
reason = "A keep-all, non-grouped ORDER BY + TEMPFILES sorts through disk when the datastore has a temporary_directory configured. EXPLAIN marks that sort `tempfiles: true`, and that mark is the load-bearing assertion: a sort with a LIMIT or a random key is bounded in memory and carries no mark."

[env]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: ord:01, n: 1 }, { id: ord:02, n: 2 }, { id: ord:03, n: 3 }, { id: ord:04, n: 4 }, { id: ord:05, n: 5 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id, n]
    Sort [ctx: Db] [sort_keys: n ASC, tempfiles: true]
        TableScan [ctx: Db] [table: ord, direction: Forward]
''''

*/

{
    CREATE ord:03 SET n = 3;
    CREATE ord:01 SET n = 1;
    CREATE ord:05 SET n = 5;
    CREATE ord:02 SET n = 2;
    CREATE ord:04 SET n = 4;
    RETURN NONE;
};

SELECT id, n FROM ord ORDER BY n ASC TEMPFILES;
EXPLAIN SELECT id, n FROM ord ORDER BY n ASC TEMPFILES;
