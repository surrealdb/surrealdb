/**
[test]
reason = "Test computed fields with relationship traversal and various field access syntaxes"
issue = 6479

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ computed: 'computed', dependent: 'computed', id: person:one, static: 'static' }, { computed: 'computed', dependent: 'computed', id: person:two, static: 'static' }]"

[[test.results]]
value = "[{ id: knows:bny3a5xkjvex9wy8us18, in: person:one, out: person:two }]"
skip-record-id-key = true

[[test.results]]
value = "[{ computed: 'computed', dependent: 'computed', id: person:two, static: 'static' }]"

[[test.results]]
value = "[{ id: person:two }]"

[[test.results]]
value = "[{ id: person:two, static: 'static' }]"

[[test.results]]
value = "[{ dependent: 'computed', id: person:two, static: 'static' }]"

[[test.results]]
value = "[person:two]"

[[test.results]]
value = "[{ id: person:two }]"

[[test.results]]
value = "[{ id: person:two, static: 'static' }]"

[[test.results]]
value = "[{ dependent: 'computed', id: person:two, static: 'static' }]"
*/

DEFINE FIELD computed ON person COMPUTED { 'computed' };

DEFINE FIELD dependent ON person COMPUTED { computed };

CREATE person:one, person:two SET static = 'static';

RELATE person:one->knows->person:two;

# Should return just the id field
SELECT * FROM person:one->knows->person.id;

# Should return an object with the id field
SELECT * FROM person:one->knows->person.{ id };

# Should return an object with the id and static fields
SELECT * FROM person:one->knows->person.{ id, static };

# Should return an object with the id and static fields
SELECT * FROM person:one->knows->person.{ id, static, dependent };

# Should return just the id field
RETURN person:one->knows->person.id;

# Should return an object with just the id field
RETURN person:one->knows->person.{ id };

# Should return an object with the id and static fields
RETURN person:one->knows->person.{ id, static };

# Should return an object with the id and static fields
RETURN person:one->knows->person.{ id, static, dependent };
