/**
[test]
reason = "Test that FETCH clause works with WHERE + LIMIT and that LIMIT is pushed to IndexScan before FETCH"

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''"Fetch [ctx: Db] [fields: friend]
    SelectProject [ctx: Db] [projections: *]
        IndexScan [ctx: Db] [index: idx_city, access: = 'london', direction: Forward, limit: 2]
"'''

[[test.results]]
value = "[{ city: 'london', friend: { city: 'london', friend: person:charlie, id: person:bob }, id: person:alice }, { city: 'london', friend: { city: 'london', friend: person:alice, id: person:charlie }, id: person:bob }]"

[[test.results]]
value = '''"Fetch [ctx: Db] [fields: friend] {rows: 2}
    SelectProject [ctx: Db] [projections: *] {rows: 2}
        IndexScan [ctx: Db] [index: idx_city, access: = 'london', direction: Forward, limit: 2] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = '''"Fetch [ctx: Db] [fields: friend]
    SelectProject [ctx: Db] [projections: *]
        TableScan [ctx: Db] [table: person, direction: Forward, predicate: city = 'london', limit: 2]
"'''

[[test.results]]
value = ''''Fetch [ctx: Db] [fields: friend]
    SelectProject [ctx: Db] [projections: *]
        TableScan [ctx: Db] [table: person, direction: Forward, limit: 3]
''''
*/

-- Setup: define index first, then create data so the index is populated
{
    DEFINE INDEX idx_city ON TABLE person COLUMNS city;
    CREATE person:alice SET city = 'london', friend = person:bob;
    CREATE person:bob SET city = 'london', friend = person:charlie;
    CREATE person:charlie SET city = 'london', friend = person:alice;
    CREATE person:dave SET city = 'paris', friend = person:alice;
    CREATE person:eve SET city = 'london', friend = person:dave;
    RETURN "OK";
};

-- Case 1: EXPLAIN plan should show Fetch after Project, with limit pushed to IndexScan
EXPLAIN SELECT * FROM person WHERE city = 'london' LIMIT 2 FETCH friend;

-- Case 2: Verify data correctness - friend field should be resolved to full records
SELECT * FROM person WHERE city = 'london' LIMIT 2 FETCH friend;

-- Case 3: EXPLAIN ANALYZE to confirm row counts
EXPLAIN ANALYZE SELECT * FROM person WHERE city = 'london' LIMIT 2 FETCH friend;

-- Case 4: Without index, LIMIT should appear as separate operator before Fetch
EXPLAIN SELECT * FROM person WITH NO INDEX WHERE city = 'london' LIMIT 2 FETCH friend;

-- Case 5: FETCH with LIMIT but no WHERE - limit pushed to TableScan
EXPLAIN SELECT * FROM person LIMIT 3 FETCH friend;
