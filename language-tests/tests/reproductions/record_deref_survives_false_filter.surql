/**
[test]
reason = """
A record id is not an ordinary literal. Materialising one as a row fetches
the record it names, and reading a field off one fetches it too; either
derives the record's COMPUTED fields and judges its PERMISSIONS, and either
can raise. A rewrite may reorder an evaluation but may not remove one, so a
filter nothing survives must not drop a source built from a record id -- the
error is observed even where no row is.

`computed` is defined after the record is written, so the coercion it fails
is charged to the read rather than to the write. The literal source of plain
values is the control: nothing there dereferences, so the collapse still
applies to it.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: account:one }]"

[[test.results]]
value = "NONE"

# The access dereferences the record id.
[[test.results]]
error = true

# So does materialising it as a row, with no access written at all.
[[test.results]]
error = true

# A parameter the analyzer did not resolve could hold one, and here does.
[[test.results]]
value = "NONE"

[[test.results]]
error = true

# Control: a source of plain literals dereferences nothing and still collapses.
[[test.results]]
value = "[]"

*/

DEFINE TABLE account SCHEMALESS;
CREATE account:one;
DEFINE FIELD computed ON account TYPE int COMPUTED 'abc';
SELECT * FROM [(account:one).computed] WHERE false;
SELECT * FROM [account:one] WHERE false;
LET $held = account:one;
SELECT * FROM [$held] WHERE false;
SELECT * FROM [1, 2, 3] WHERE false;
