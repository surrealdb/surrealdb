/**
[env]
namespace = true
database = true

[test]
reason = "A domain's DEFAULT constant must be a value its base type is assigned from, and a DROP DOMAIN that is refused leaves the domain for the statements after it"

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
error = "Invalid statement: Cannot remove the type 'pos': the field 'x' on the table 't' has a type that names it"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `x` of `u:1`: Expected `pos (constraint pos_check failed)` but found `-1i32`"

[[test.results]]
value = "[{ typname: 'd_bool_text' }, { typname: 'd_num_int' }, { typname: 'd_num_text' }, { typname: 'pos' }]"

*/
CREATE DOMAIN d_num_text AS text DEFAULT 1;
CREATE DOMAIN d_bool_text AS text DEFAULT true;
CREATE DOMAIN d_num_int AS integer DEFAULT 2.5;
CREATE DOMAIN pos AS integer CHECK (VALUE > 0);
CREATE TABLE t (id int PRIMARY KEY, x pos);
DROP DOMAIN pos;
CREATE TABLE u (id int PRIMARY KEY, x pos);
INSERT INTO u (id, x) VALUES (1, -1);
SELECT typname FROM pg_type WHERE typname IN ('d_num_text', 'd_bool_text', 'd_num_int', 'pos') ORDER BY typname;
