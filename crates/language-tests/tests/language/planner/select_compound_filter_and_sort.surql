/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ f: true, id: t:4, s: 1 }, { f: true, id: t:2, s: 3 }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx', operator: '=', value: [true] }, table: 't' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

*/
{
    DEFINE INDEX idx ON t FIELDS f, s;
    CREATE t:1 SET s=4, f=false;
    CREATE t:2 SET s=3, f=true;
    CREATE t:3 SET s=2, f=false;
    CREATE t:4 SET s=1, f=true;
    RETURN "OK";
};

SELECT * FROM t WHERE f ORDER BY s;
SELECT * FROM t WHERE f ORDER BY s EXPLAIN;
