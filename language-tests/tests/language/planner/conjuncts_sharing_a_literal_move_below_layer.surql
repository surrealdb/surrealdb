/**
[env]

[test]
reason = "A conjunct that reads no computed field moves below the layer that derives them even when another conjunct of the predicate holds the same literal, so the scan serves it and the layer derives its fields only for the rows it admits. A conjunct that may raise and follows one that stays above the layer stays above it too, since AND reaches the one that stays first"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [predicate: ag2 = 2]
        ProjectLayer [ctx: Db] [columns: ag2 = age * 2 : int @computed]
            Filter [ctx: Db] [predicate: math::abs(age) > 2]
                TableScan [ctx: Db] [table: p, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [predicate: ag2 = 2 AND math::abs(age) > 2]
        ProjectLayer [ctx: Db] [columns: ag2 = age * 2 : int @computed]
            TableScan [ctx: Db] [table: p, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [predicate: ag2 = 2]
        ProjectLayer [ctx: Db] [columns: ag2 = age * 2 : int @computed]
            Filter [ctx: Db] [predicate: math::abs(age) > 2]
                IndexScan [ctx: Db] [table: p, index: p_age, access: = 3, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [predicate: ag2 = 3 AND string::len(name) > 3]
        ProjectLayer [ctx: Db] [columns: ag2 = age * 2 : int @computed]
            IndexScan [ctx: Db] [table: p, index: p_age, access: = 3, direction: Forward]
''''

*/

DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD name ON p TYPE string;
DEFINE FIELD age ON p TYPE int;
DEFINE FIELD ag2 ON p TYPE int COMPUTED age * 2;
DEFINE INDEX p_age ON p FIELDS age;

EXPLAIN SELECT * FROM p WHERE math::abs(age) > 2 AND ag2 = 2;
EXPLAIN SELECT * FROM p WHERE ag2 = 2 AND math::abs(age) > 2;
EXPLAIN SELECT * FROM p WHERE math::abs(age) > 2 AND age = 3 AND ag2 = 2;
EXPLAIN SELECT * FROM p WHERE age = 3 AND ag2 = 3 AND string::len(name) > 3;
