/**
[env]
imports = ["upgrade/define/3.00.00_field_reference_import.surql"]

[test]
upgrade = true
version = ">=3.4.0-a"
introduced = ">=3.0.0"
reason = """
Covers the two things a REFERENCE field written by a released binary hands to the
`MATERIALIZE INTO` clause of the current one: the stored definition, which the clause
extends and so is decoded from the older revision of the reference before it is
altered, and the reference keys, which the clause's backfill reads to fill the lists
and which every later write of a referencing record keeps in step.

The lists are compared with the COMPUTED `<~book` field the old binary defined, which
reads the same keys the lists are built from, so a key the backfill dropped, doubled or
decoded wrongly shows as a list that differs from it. The writes that follow change
references the old binary wrote: moving one, and deleting one, must take the record
out of the list it was put in by the backfill, which is the only place the older
bytes are read by the write path rather than by the scan.

The body is gated to the current checkout as the upgrade target because the clause is
spelled only by this build's parser. That costs no coverage the seed axis provides: it
sends every released writer straight to the current checkout.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[author:a, [book:1, book:2], [book:1, book:2]], [author:b, [book:3], [book:3]]]"

[[test.results]]
value = "[{ author: author:b, id: book:4, title: 'W' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ author: author:b, id: book:2, title: 'Y' }]"

[[test.results]]
value = "[[author:a, [], []], [author:b, [book:2, book:3, book:4], [book:2, book:3, book:4]]]"

[[test.results]]
match = '''string::contains($result.fields.author, "REFERENCE ON DELETE UNSET MATERIALIZE INTO author.shelf")'''

*/
-- The list is a field the table declares; the clause only fills it.
DEFINE FIELD shelf ON author TYPE array<record<book>> DEFAULT [] READONLY;
-- The older definition is read, extended and stored; its backfill reads the older keys.
ALTER FIELD author ON book REFERENCE ON DELETE UNSET MATERIALIZE INTO author.shelf;
SELECT VALUE [id, shelf, array::sort(books)] FROM author ORDER BY id;
-- A write of a referencing record the old binary wrote, and one it did not.
CREATE book:4 SET title = 'W', author = author:b;
DELETE book:1;
UPDATE book:2 SET author = author:b;
SELECT VALUE [id, shelf, array::sort(books)] FROM author ORDER BY id;
INFO FOR TABLE book;
