/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A field read off a record id held in a parameter needs the database whatever
statement holds it: a LET or RETURN of an IF, a FOR body, a block, a `.*`
read, and a field reached through a record the statement selected.
"""
issue = 7348

[[test.results]]
value = "[{ email: 'x', id: person:a, parent: person:b }]"

[[test.results]]
value = "[{ email: 'y', id: person:b }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'a'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'x'"

[[test.results]]
value = "'x'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'x'"

[[test.results]]
value = "'a'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'parent'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ email: 'z', id: person:c }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['x', 'y', 'z']"
*/
CREATE person:a SET email = 'x', parent = person:b;
CREATE person:b SET email = 'y';
LET $rec = person:a;
IF $rec.email = 'x' { 'a' } ELSE { 'b' };
LET $v = IF true { $rec.email } ELSE { NONE };
RETURN $v;
RETURN IF true { $rec.email } ELSE { NONE };
FOR $p IN [$rec] { IF $p.email = NONE { THROW 'x' }; };
{ LET $unused = 1; $rec.email };
IF $rec.* != NONE { 'a' };
LET $row = (SELECT parent FROM ONLY $rec);
IF $row.parent.email = 'y' { 'parent' };
BEGIN;
IF $rec.email = 'x' { CREATE person:c SET email = 'z' };
COMMIT;
SELECT VALUE email FROM person ORDER BY email;
