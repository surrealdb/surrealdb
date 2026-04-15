/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "Computed fields that dereference $this.id must not infinite-loop via document re-fetch. Covers object-keyed, string-keyed, and cross-record cases to verify cycle detection only activates for self-referential fetches."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ foo: NONE, id: test1:{ prop: 1 } }]"

[[test.results]]
value = "[{ foo: NONE, id: test1:{ prop: 1 } }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ bar: 99, echo: 99, id: test2:1 }]"

[[test.results]]
value = "[{ bar: 99, echo: 99, id: test2:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test3:hello, mirror: 'world', name: 'world' }]"

[[test.results]]
value = "[{ id: test3:hello, mirror: 'world', name: 'world' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ doubled: 10, id: test4:a, self_ref: 5, stored: 5 }]"

[[test.results]]
value = "[{ doubled: 10, id: test4:a, self_ref: 5, stored: 5 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test5:a, label: 'alpha', ref_name: NONE }]"

[[test.results]]
value = "[{ id: test5:b, label: 'beta', ref: test5:a, ref_name: 'alpha' }]"

[[test.results]]
value = "[{ id: test5:a, label: 'alpha', ref_name: NONE }, { id: test5:b, label: 'beta', ref: test5:a, ref_name: 'alpha' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test6:hello, my_id: test6:hello }]"

[[test.results]]
value = "[{ id: test6:hello, my_id: test6:hello }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test7:{ a: { b: 99 } }, nested: NONE }]"

[[test.results]]
value = "[{ id: test7:{ a: { b: 99 } }, nested: NONE }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ echo: 1, id: test8:x, val: 1 }]"

[[test.results]]
value = "[{ echo: 2, id: test8:x, val: 2 }]"

[[test.results]]
value = "[{ echo: 2, id: test8:x, val: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ arr: 'stored', id: test9:[1, 2, 3], items: 'stored' }]"

[[test.results]]
value = "[{ arr: 'stored', id: test9:[1, 2, 3], items: 'stored' }]"

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
value = "[{ echo_x: 10, echo_y: 20, id: test10:v, x: 10, y: 20 }]"

[[test.results]]
value = "[{ echo_x: 10, echo_y: 20, id: test10:v, x: 10, y: 20 }]"
*/

-- =============================================================================
-- 1. Basic self-reference: computed field reads a key-only property via $this.id
--    The document has no `prop` field — `prop` lives in the RecordId key.
--    Cycle detection returns raw stored data, so foo = NONE.
-- =============================================================================
DEFINE TABLE test1;
DEFINE FIELD foo ON test1 COMPUTED { RETURN $this.id.prop };
CREATE test1:{ prop: 1 };
SELECT * FROM test1;

-- =============================================================================
-- 2. Self-reference reading a STORED document field via $this.id
--    `bar` is a real document field, so when cycle detection returns the raw
--    record it contains the stored value for `bar`.
-- =============================================================================
DEFINE TABLE test2;
DEFINE FIELD bar ON test2 TYPE int;
DEFINE FIELD echo ON test2 COMPUTED { RETURN $this.id.bar };
CREATE test2:1 SET bar = 99;
SELECT * FROM test2;

-- =============================================================================
-- 3. String-keyed RecordId with self-referencing computed field
--    Same cycle path but the key is a plain string, not an object.
-- =============================================================================
DEFINE TABLE test3;
DEFINE FIELD name ON test3 TYPE string;
DEFINE FIELD mirror ON test3 COMPUTED { RETURN $this.id.name };
CREATE test3:hello SET name = "world";
SELECT * FROM test3;

-- =============================================================================
-- 4. Multiple computed fields — only one self-references, the others are safe
-- =============================================================================
DEFINE TABLE test4 SCHEMAFULL;
DEFINE FIELD stored ON test4 TYPE int;
DEFINE FIELD doubled ON test4 COMPUTED $this.stored * 2;
DEFINE FIELD self_ref ON test4 COMPUTED { RETURN $this.id.stored };
CREATE test4:a SET stored = 5;
SELECT * FROM test4;

-- =============================================================================
-- 5. Cross-record dereference: computed field fetches a DIFFERENT record.
--    This must NOT be short-circuited by cycle detection.
-- =============================================================================
DEFINE TABLE test5;
DEFINE FIELD ref ON test5 TYPE option<record<test5>>;
DEFINE FIELD ref_name ON test5 COMPUTED { RETURN $this.ref.label };
DEFINE FIELD label ON test5 TYPE option<string>;
CREATE test5:a SET label = "alpha";
CREATE test5:b SET ref = test5:a, label = "beta";
SELECT * FROM test5 ORDER BY id;

-- =============================================================================
-- 6. $this.id (no field access) should still return the RecordId itself
-- =============================================================================
DEFINE TABLE test6;
DEFINE FIELD my_id ON test6 COMPUTED $this.id;
CREATE test6:hello;
SELECT * FROM test6;

-- =============================================================================
-- 7. Computed field reading a nested object-keyed RecordId property
--    The key is { a: { b: 99 } }, computed accesses $this.id.a — cycle
--    detection fires, raw record has no `a` field → NONE.
-- =============================================================================
DEFINE TABLE test7;
DEFINE FIELD nested ON test7 COMPUTED { RETURN $this.id.a };
CREATE test7:{ a: { b: 99 } };
SELECT * FROM test7;

-- =============================================================================
-- 8. UPDATE triggers re-evaluation of computed field with self-reference
-- =============================================================================
DEFINE TABLE test8;
DEFINE FIELD val ON test8 TYPE int;
DEFINE FIELD echo ON test8 COMPUTED { RETURN $this.id.val };
CREATE test8:x SET val = 1;
UPDATE test8:x SET val = 2;
SELECT * FROM test8;

-- =============================================================================
-- 9. Self-referencing computed field on a record with an array key
-- =============================================================================
DEFINE TABLE test9;
DEFINE FIELD arr ON test9 COMPUTED { RETURN $this.id.items };
CREATE test9:[1, 2, 3] SET items = "stored";
SELECT * FROM test9;

-- =============================================================================
-- 10. Two computed fields both self-referencing different stored fields
-- =============================================================================
DEFINE TABLE test10;
DEFINE FIELD x ON test10 TYPE int;
DEFINE FIELD y ON test10 TYPE int;
DEFINE FIELD echo_x ON test10 COMPUTED { RETURN $this.id.x };
DEFINE FIELD echo_y ON test10 COMPUTED { RETURN $this.id.y };
CREATE test10:v SET x = 10, y = 20;
SELECT * FROM test10;
