/**
[env]
imports = ["reproductions/a_filter_over_records_reads_index_arguments_against_the_record_record_user_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "A record user's filter over records judges each record with the record as the document root: an index argument in an element body of its predicate reads the record, so the predicate runs. A field the user may not read, whether denied outright or read through a computed field, reads as NONE there as it does anywhere in the record, and the table's own row permission is applied first."

[[test.results]]
value = "[10]"

[[test.results]]
value = "[10, 20]"

[[test.results]]
value = "[10, 20]"
*/

SELECT VALUE n FROM open WHERE [n][WHERE $this >= [0, 8, 100][lim]] != [] ORDER BY n;
SELECT VALUE n FROM guarded WHERE [n][WHERE $this >= [0, 8, 100][lim]] != [] ORDER BY n;
SELECT VALUE n FROM guarded WHERE [n][WHERE $this >= [0, 8, 100][cl]] != [] ORDER BY n;
