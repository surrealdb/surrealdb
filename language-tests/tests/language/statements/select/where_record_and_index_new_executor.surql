/**
[test]
reason = "Test WHERE with record ID field, AND conditions, and index usage (new executor)"

[[test.results]]
value = "[{ id: person:tobie, name: 'Tobie' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ author: person:tobie, id: post:1 }]"

[[test.results]]
value = "[{ author: person:tobie, id: post:2 }]"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 2}
  Filter [ctx: Db] [predicate: author = person:tobie] {rows: 2}
    IndexScan [ctx: Db] [index: author, access: = person:tobie, direction: Forward] {rows: 2}

Total rows: 2''''

[[test.results]]
value = "[{ author: person:tobie, id: post:1 }, { author: person:tobie, id: post:2 }]"

[[test.results]]
value = "[{ genre: 'm', id: person2:tobie, name: 'Tobie' }]"

[[test.results]]
value = "[{ genre: 'm', id: person2:jaime, name: 'Jaime' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 1}
  Filter [ctx: Db] [predicate: name = 'Tobie' AND genre = 'm'] {rows: 1}
    IndexScan [ctx: Db] [index: person2_name, access: = 'Tobie', direction: Forward] {rows: 1}

Total rows: 1"'''

[[test.results]]
value = "[{ name: 'Tobie' }]"

[[test.results]]
value = "[{ genre: 'm', id: person3:tobie, name: 'Tobie' }]"

[[test.results]]
value = "[{ genre: 'm', id: person3:jaime, name: 'Jaime' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 1}
  Filter [ctx: Db] [predicate: name = 'Jaime' AND genre = 'm'] {rows: 1}
    IndexScan [ctx: Db] [index: person3_name, access: = 'Jaime', direction: Forward] {rows: 1}

Total rows: 1"'''

[[test.results]]
value = "[{ name: 'Jaime' }]"
*/

-- Record field with standard index
CREATE person:tobie SET name = 'Tobie';
DEFINE INDEX author ON TABLE post COLUMNS author;
CREATE post:1 SET author = person:tobie;
CREATE post:2 SET author = person:tobie;
EXPLAIN ANALYZE SELECT * FROM post WHERE author = person:tobie;
SELECT * FROM post WHERE author = person:tobie;

-- AND with standard index
CREATE person2:tobie SET name = 'Tobie', genre='m';
CREATE person2:jaime SET name = 'Jaime', genre='m';
DEFINE INDEX person2_name ON TABLE person2 COLUMNS name;
EXPLAIN ANALYZE SELECT name FROM person2 WHERE name = 'Tobie' AND genre = 'm';
SELECT name FROM person2 WHERE name = 'Tobie' AND genre = 'm';

-- AND with unique index
CREATE person3:tobie SET name = 'Tobie', genre='m';
CREATE person3:jaime SET name = 'Jaime', genre='m';
DEFINE INDEX person3_name ON TABLE person3 COLUMNS name UNIQUE;
EXPLAIN ANALYZE SELECT name FROM person3 WHERE name = 'Jaime' AND genre = 'm';
SELECT name FROM person3 WHERE name = 'Jaime' AND genre = 'm';
