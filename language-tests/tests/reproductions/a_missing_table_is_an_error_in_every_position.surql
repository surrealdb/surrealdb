/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A record id naming a table that is not defined fails with `TbNotFound` in
every position it is read from — a scanned `FROM` source, an array of ids as
the source, a dereference, a `FETCH` target — just as a scan of that table
does. A record that is merely missing from a table that exists is not an
error: it reads as `NONE`, or yields no row as a source.
"""

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
value = "[{ id: present:1, r: missing:1, rs: [missing:1, missing:2] }]"

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
value = "[{ id: present:2, r: present:9, rs: [present:9] }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: present:2, r: NONE, rs: [present:9] }]"

[[test.results]]
value = "[{ id: present:2, r: present:9, rs: [NONE] }]"

[[test.results]]
value = "NONE"
*/

SELECT * FROM missing;
SELECT * FROM missing:1;
SELECT * FROM [missing:1];
SELECT * FROM [missing:1, missing:2];
SELECT * FROM missing:1..3;
CREATE present:1 SET r = missing:1, rs = [missing:1, missing:2];
SELECT * FROM present:1 FETCH r;
SELECT * FROM present:1 FETCH rs;
SELECT r.* FROM present:1;
SELECT * FROM missing:1.*;
missing:1.*;
[missing:1].*;

-- A missing record in a table that exists is not an error.
CREATE present:2 SET r = present:9, rs = [present:9];
SELECT * FROM [present:9];
SELECT * FROM present:2 FETCH r;
SELECT * FROM present:2 FETCH rs;
present:9.*;
