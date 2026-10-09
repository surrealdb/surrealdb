/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A statement nested in another and joined to its row by `field = $parent.path`
reads, where an index covers that field alone, through the index once per row
(an index nested loop): only the records the index lists under the row's key.
A key the index may not list every equal record under (a float, a decimal, an
array) reads the table whole for that row instead, and the join predicate
still judges every row read, so the answers are those of the hashed read the
same statement takes without the index.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: author:1, key: 1, name: 'ann' }]"

[[test.results]]
value = "[{ id: author:2, key: 2f, name: 'bob' }]"

[[test.results]]
value = "[{ id: author:3, key: [1], name: 'cat' }]"

[[test.results]]
value = "[{ id: author:4, name: 'dan' }]"

[[test.results]]
value = "[{ author: author:1, id: post:1, key: 1, title: 'a' }]"

[[test.results]]
value = "[{ author: author:2, id: post:2, key: 2, title: 'b' }]"

[[test.results]]
value = "[{ author: author:1, id: post:3, key: [1, 2], title: 'c' }]"

[[test.results]]
value = "[{ author: author:3, id: post:4, key: 1f, title: 'd' }]"

[[test.results]]
value = "[{ id: post:5, title: 'e' }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: name]
    Filter [ctx: Db] [predicate: 'c' INSIDE (subquery)]
        TableScan [ctx: Db] [table: author, direction: Forward]
        Subquery [ctx: Db] [strategy: lookup, then hash]
            ProjectValue [ctx: Db] [expr: title]
                Filter [ctx: Db] [predicate: author = $parent.id]
                    IndexLookup [ctx: Db] [table: post, index: by_author, access: author = $parent.id]
"'''

[[test.results]]
value = "[{ name: 'ann' }]"

[[test.results]]
value = "[{ name: 'ann', titles: ['a', 'c'] }, { name: 'bob', titles: ['b'] }, { name: 'cat', titles: ['d'] }, { name: 'dan', titles: [] }]"

[[test.results]]
value = "[{ name: 'ann', titles: ['a', 'c'] }, { name: 'bob', titles: ['b'] }, { name: 'cat', titles: ['d'] }, { name: 'dan', titles: [] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ name: 'ann', titles: ['a', 'd'] }, { name: 'bob', titles: ['b'] }, { name: 'cat', titles: [] }, { name: 'dan', titles: ['e'] }]"

[[test.results]]
value = "[{ name: 'ann', titles: ['a', 'd'] }, { name: 'bob', titles: ['b'] }, { name: 'cat', titles: [] }, { name: 'dan', titles: ['e'] }]"

*/

DEFINE INDEX by_author ON post FIELDS author;
CREATE author:1 SET name = 'ann', key = 1;
CREATE author:2 SET name = 'bob', key = 2.0;
CREATE author:3 SET name = 'cat', key = [1];
CREATE author:4 SET name = 'dan';
CREATE post:1 SET author = author:1, key = 1, title = 'a';
CREATE post:2 SET author = author:2, key = 2, title = 'b';
CREATE post:3 SET author = author:1, key = [1, 2], title = 'c';
CREATE post:4 SET author = author:3, key = 1.0, title = 'd';
CREATE post:5 SET title = 'e';

EXPLAIN SELECT name FROM author WHERE 'c' IN (SELECT VALUE title FROM post WHERE author = $parent.id);
SELECT name FROM author WHERE 'c' IN (SELECT VALUE title FROM post WHERE author = $parent.id);
SELECT name, (SELECT VALUE title FROM post WHERE author = $parent.id) AS titles FROM author;
SELECT name, (SELECT VALUE title FROM post WITH NOINDEX WHERE author = $parent.id) AS titles FROM author;

DEFINE INDEX by_key ON post FIELDS key;
SELECT name, (SELECT VALUE title FROM post WHERE key = $parent.key) AS titles FROM author;
SELECT name, (SELECT VALUE title FROM post WITH NOINDEX WHERE key = $parent.key) AS titles FROM author;
