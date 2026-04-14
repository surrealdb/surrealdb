/**
[env]

[test]
reason = "SELECT VALUE ... OMIT must apply OMIT via plan_projections (literal RecordId fast path) and plan_projections_fast (table scan without ORDER BY), including nested OMIT paths"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "['A']"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[NONE, NONE, NONE]"

[[test.results]]
value = "[{ age: 1, id: person:a }]"

[[test.results]]
value = "[{ age: 21, nested: { data: 'test' }, security: 'secure' }]"

[[test.results]]
value = "[{ age: 21, nested: {  }, security: 'secure' }]"

[[test.results]]
value = "[{ nested: {  }, security: 'secure' }]"

[[test.results]]
value = "[{ age: 21, nested: {  }, security: 'secure' }, { age: 22, nested: {  }, security: 'open' }, { age: 23, nested: {  }, security: 'private' }]"

[[test.results]]
value = "[{ nested: {  }, security: 'secure' }, { nested: {  }, security: 'open' }, { nested: {  }, security: 'private' }]"

[[test.results]]
value = "[person:a]"

[[test.results]]
value = "[person:a, person:b, person:c]"

*/

{
    CREATE person:a SET name = 'A', age = 1, opts = {
        security: 'secure',
        age: 21,
        nested: { data: 'test' }
    };
    CREATE person:b SET name = 'B', age = 2, opts = {
        security: 'open',
        age: 22,
        nested: { data: 'other' }
    };
    CREATE person:c SET name = 'C', age = 2, opts = {
        security: 'private',
        age: 23,
        nested: { data: 'more' }
    };
    RETURN "OK";
};

-- Literal RecordId path (plan_projections, no ORDER BY)
SELECT VALUE name FROM person:a;
SELECT VALUE name OMIT name FROM person:a;
-- Table scan without ORDER BY (plan_projections_fast)
SELECT VALUE name OMIT name FROM person;
-- OMIT with SELECT * (not VALUE) from literal RecordId
SELECT * OMIT name, opts FROM person:a;
-- Nested OMIT with VALUE: extract opts, omit opts.nested.data (literal RecordId)
SELECT VALUE opts FROM person:a;
SELECT VALUE opts OMIT opts.nested.data FROM person:a;
-- Nested OMIT with VALUE: multiple nested fields (literal RecordId)
SELECT VALUE opts OMIT opts.age, opts.nested.data FROM person:a;
-- Nested OMIT with VALUE: table scan without ORDER BY
SELECT VALUE opts OMIT opts.nested.data FROM person;
SELECT VALUE opts OMIT opts.age, opts.nested.data FROM person;
-- SELECT VALUE id with OMIT: id must still resolve through the OMIT early path
SELECT VALUE id OMIT name FROM person:a;
SELECT VALUE id OMIT name FROM person;
