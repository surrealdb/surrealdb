/**
[env]
imports = ["language/functions/record/exists_permissions_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	`record::exists` reads the record as the caller may: a record it may not
	select does not exist for it, whether the table's permission hides the row
	or the whole table. A table that is not defined holds no record. A record
	id whose object key has an `id` component answers from the key, as a path
	read of `id` does, without reading the record.
"""

[[test.results]]
value = "[true, false]"

[[test.results]]
value = "[true, false]"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "true"
*/
[record::exists(open:1), record::exists(open:2)];
[record::exists(partial:shown), record::exists(partial:hidden)];
record::exists(closed:1);
record::exists(nowhere:1);
record::exists(open:{ id: 5 });
