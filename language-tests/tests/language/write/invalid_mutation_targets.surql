/**
[env]
namespace = true
database = true

[test]
reason = """
A target that is not a record id, a table, or a collection of them fails the
statement. It does not match nothing and quietly succeed, which is the failure
mode worth guarding: an updating statement whose target expression evaluated to
a scalar would otherwise report success having written nothing.

The error names the statement and the offending value, so each of UPDATE,
UPSERT, DELETE and CREATE is checked separately. NONE is rejected on the same
footing as a number or a string.

An object is a valid target only when it carries an `id`; one without is
rejected like any other non-target. An array is checked element by element, and
the error names the first bad element rather than the array.
"""

[[test.results]]
error = "Cannot execute UPDATE statement using value: 42"

[[test.results]]
error = "Cannot execute UPSERT statement using value: true"

[[test.results]]
error = "Cannot execute DELETE statement using value: 'x'"

[[test.results]]
error = "Cannot execute CREATE statement using value: 42"

[[test.results]]
error = "Cannot execute UPDATE statement using value: NONE"

[[test.results]]
error = "Cannot execute UPDATE statement using value: { a: 1 }"

[[test.results]]
error = "Cannot execute UPDATE statement using value: 1"
*/

-- A target that is not a record id, table or collection of them is rejected by
-- the statement rather than silently matching nothing.
UPDATE 42 SET n = 1;
UPSERT true SET n = 1;
DELETE "x";
CREATE 42;
UPDATE NONE SET n = 1;

-- An object is a valid target only when it carries an `id`.
UPDATE { a: 1 } SET n = 1;

-- An array is checked element by element, and the first bad element names
-- itself in the error.
UPDATE [1, 2] SET n = 1;
