/**
[env]
namespace = true
database = true

[test]
reason = "Only the record's own `id` is known during CREATE; an `id` read through another field stays NONE"

[[test.results]]
value = "{ id: planet:earth, name: 'earth' }"

[[test.results]]
value = "{ id: planet:mars, name: 'mars', nested: {  } }"
# A nested `id` resolves to NONE, which CREATE leaves out of the record, so
# `inner`, `scalar`, `missing` and `nested.id` are absent. Were the record id
# to reach them, each would hold planet:earth or planet:mars.
*/

CREATE ONLY planet:earth SET
	name = id.id(),
	inner = child.id,
	scalar = (1).id,
	missing = nothing.id;

CREATE ONLY planet:mars SET
	name = id.id(),
	nested = { id: child.id };
