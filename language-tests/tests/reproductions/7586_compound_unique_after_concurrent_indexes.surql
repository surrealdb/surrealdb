/**
[env]
timeout = 15000
context-timeout = 15000

[test]
reason = "Public issue surrealdb/surrealdb#7586: a synchronous compound UNIQUE DEFINE INDEX following CONCURRENTLY definitions was reported slower than on 3.2.4. A language test cannot time a statement, so this pins the shape's correctness: every index of the batch reaches ready and answers queries, and the UNIQUE index enforces its key."
issue = 7586

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
value = "NONE"

[[test.results]]
value = "['ready', 'ready', 'ready', 'ready', 'ready', 'ready']"

[[test.results]]
value = "[{ count: 160 }]"

[[test.results]]
value = "[{ count: 8 }]"

[[test.results]]
error = "Database index `iu` already contains ['HAS_COLUMN', resource:3, attribute:24], with record `has_attribute:[3, 0]`"

*/

DEFINE TABLE resource SCHEMALESS;
DEFINE TABLE attribute SCHEMALESS;
DEFINE TABLE has_attribute TYPE RELATION IN resource OUT attribute SCHEMALESS;
FOR $m IN 0..20 {
    LET $r = type::record('resource', $m);
    CREATE $r SET name = 'model' RETURN NONE;
    FOR $c IN 0..8 {
        LET $a = type::record('attribute', $m * 8 + $c);
        CREATE $a SET name = 'column' RETURN NONE;
        INSERT RELATION INTO has_attribute { id: [$m, $c], in: $r, out: $a, source_rel: 'HAS_COLUMN' } RETURN NONE;
    };
};
DEFINE INDEX OVERWRITE i1 ON has_attribute FIELDS source_rel CONCURRENTLY;
DEFINE INDEX OVERWRITE i2 ON has_attribute FIELDS source_rel, in CONCURRENTLY;
DEFINE INDEX OVERWRITE i3 ON has_attribute FIELDS source_rel, out CONCURRENTLY;
DEFINE INDEX OVERWRITE i4 ON has_attribute FIELDS in CONCURRENTLY;
DEFINE INDEX OVERWRITE i5 ON has_attribute FIELDS out CONCURRENTLY;
DEFINE INDEX OVERWRITE iu ON has_attribute FIELDS source_rel, in, out UNIQUE;
SLEEP 1s;
RETURN ['i1', 'i2', 'i3', 'i4', 'i5', 'iu'].map(|$ix| (INFO FOR INDEX $ix ON has_attribute).building.status);
SELECT count() FROM has_attribute WITH INDEX i1 WHERE source_rel = 'HAS_COLUMN' GROUP ALL;
SELECT count() FROM has_attribute WITH INDEX iu WHERE source_rel = 'HAS_COLUMN' AND in = resource:3 GROUP ALL;
RELATE resource:3->has_attribute->attribute:24 SET source_rel = 'HAS_COLUMN';
