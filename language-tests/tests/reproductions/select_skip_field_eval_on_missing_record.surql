/**
[test]
reason = "SELECT *, <expr> FROM record:missing must not evaluate <expr> for rows where the record is missing — Project's include_all path used to lazily-evaluate fields only after confirming the RecordId resolved to an Object, and the streaming-executor refactor must preserve that. Field expressions can carry side effects (mutating subqueries) that must not fire for rows we're about to drop."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 0, id: probe:c }]"

[[test.results]]
value = "[{ id: src:exists, name: 'real' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ bumped: [{ count: 1, id: probe:c }], id: src:exists, name: 'real' }]"

[[test.results]]
value = "[{ count: 1 }]"
*/
DEFINE TABLE probe SCHEMAFULL;
DEFINE FIELD count ON probe TYPE int DEFAULT 0;
CREATE probe:c;

CREATE src:exists SET name = 'real';

-- Bug: select on missing record with a side-effecting field expression.
SELECT *, (UPDATE probe:c SET count += 1) AS bumped FROM src:missing;

-- Verify the subquery did NOT fire.
SELECT count FROM probe:c;

-- Positive control: existing record should fire the subquery.
SELECT *, (UPDATE probe:c SET count += 1) AS bumped FROM src:exists;

-- Now probe.count should be 1.
SELECT count FROM probe:c;
