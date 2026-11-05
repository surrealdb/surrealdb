/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A B-tree scan answers the conjuncts it was chosen from exactly where every
column it pins is a field declared with a kind that holds no array: those
conjuncts leave the filter above the scan, and only the rest are judged per
row. An undeclared column may hold an array, listed once per element, so its
conjuncts stay and reject the rows whose array merely holds the key.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: exact:1, score: 5, tag: 'a' }]"

[[test.results]]
value = "[{ id: exact:2, score: 5, tag: 'b' }]"

[[test.results]]
value = "[{ id: exact:3, score: 7, tag: 'a' }]"

[[test.results]]
value = "[{ id: loose:1, score: 5, tag: 'a' }]"

[[test.results]]
value = "[{ id: loose:2, score: [5, 6], tag: 'a' }]"

[[test.results]]
value = "[{ id: loose:3, score: 7, tag: 'a' }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: tag = 'a']
        IndexScan [ctx: Db] [table: exact, index: by_score, access: = 5, direction: Forward]
"'''

[[test.results]]
value = "[{ id: exact:1 }]"

[[test.results]]
value = "[{ id: exact:1 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: tag = 'a']
        IndexScan [ctx: Db] [table: exact, index: by_score, access: >5, direction: Forward]
"'''

[[test.results]]
value = "[{ id: exact:3 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: score = 5 AND tag = 'a']
        IndexScan [ctx: Db] [table: loose, index: by_score, access: = 5, direction: Forward]
"'''

[[test.results]]
value = "[{ id: loose:1 }]"

[[test.results]]
value = "[{ id: loose:1 }]"

*/

DEFINE FIELD score ON exact TYPE int;
DEFINE INDEX by_score ON exact FIELDS score;
DEFINE INDEX by_score ON loose FIELDS score;
CREATE exact:1 SET score = 5, tag = 'a';
CREATE exact:2 SET score = 5, tag = 'b';
CREATE exact:3 SET score = 7, tag = 'a';
CREATE loose:1 SET score = 5, tag = 'a';
CREATE loose:2 SET score = [5, 6], tag = 'a';
CREATE loose:3 SET score = 7, tag = 'a';

-- Declared: the equality leaves the filter; the tag is still judged.
EXPLAIN SELECT id FROM exact WHERE score = 5 AND tag = 'a';
SELECT id FROM exact WHERE score = 5 AND tag = 'a';
SELECT id FROM exact WITH NOINDEX WHERE score = 5 AND tag = 'a';

-- Declared, a range: the range leaves the filter.
EXPLAIN SELECT id FROM exact WHERE score > 5 AND tag = 'a';
SELECT id FROM exact WHERE score > 5 AND tag = 'a';

-- Undeclared: the equality stays, and the array holding 5 is rejected.
EXPLAIN SELECT id FROM loose WHERE score = 5 AND tag = 'a';
SELECT id FROM loose WHERE score = 5 AND tag = 'a';
SELECT id FROM loose WITH NOINDEX WHERE score = 5 AND tag = 'a';
