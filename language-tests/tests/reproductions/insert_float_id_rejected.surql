/**
[env]
namespace = true
database = true

[test]
reason = "Float id values used to go through `Number::as_int()`, which silently produced wrong record-id keys via truncating/saturating casts on both the INSERT path (`extract_table_and_rid_key`) and the CREATE/UPSERT/UPDATE/RELATE path (`Value::generate`). For example `INSERT INTO test { id: 1.5 }` returned `test:1` and `CREATE test SET id = 0.0 / 0.0` returned `test:0`. All such cases must now error. Whole-number floats within i64 range are still accepted and round-trip exactly to an integer key."

[[test.results]]
value = "[{ id: test:1, v: 'whole' }]"

[[test.results]]
error = "Cannot execute INSERT statement where property 'id' is: 1.5f"

[[test.results]]
error = "Cannot execute INSERT statement where property 'id' is: NaN"

[[test.results]]
error = "Cannot execute INSERT statement where property 'id' is: 1000000000000000000000000000000f"

[[test.results]]
value = "[{ id: test:2, v: 'create-whole' }]"

[[test.results]]
error = "Found 1.5f for the Record ID but this is not a valid id"

[[test.results]]
error = "Found NaN for the Record ID but this is not a valid id"

[[test.results]]
error = "Found 1000000000000000000000000000000f for the Record ID but this is not a valid id"

[[test.results]]
value = "[{ id: test:3, v: 'upsert-whole' }]"

[[test.results]]
error = "Found 1.5f for the Record ID but this is not a valid id"
*/

-- INSERT: whole-number floats are still accepted (they round-trip exactly to i64).
INSERT INTO test { id: 1.0, v: 'whole' };

-- INSERT: a fractional float silently truncated to `test:1` in earlier versions.
INSERT INTO test { id: 1.5, v: 'fractional' };

-- INSERT: NaN previously truncated to `test:0`.
INSERT INTO test { id: (0.0 / 0.0), v: 'nan' };

-- INSERT: an out-of-i64-range float previously saturated.
INSERT INTO test { id: 1e30, v: 'huge' };

-- CREATE: whole-number floats are still accepted on the `Value::generate` path.
CREATE test SET id = 2.0, v = 'create-whole';

-- CREATE: a fractional float silently truncated to `test:1` in earlier versions.
CREATE test SET id = 1.5, v = 'create-fractional';

-- CREATE: NaN previously truncated to `test:0`.
CREATE test SET id = (0.0 / 0.0), v = 'create-nan';

-- CREATE: an out-of-i64-range float previously saturated.
CREATE test SET id = 1e30, v = 'create-huge';

-- UPSERT also flows through `Value::generate` for new record-id construction.
UPSERT test SET id = 3.0, v = 'upsert-whole';
UPSERT test SET id = 1.5, v = 'upsert-fractional';
