/**
[test]
# Only a released binary can write this state: the current checkout refuses the
# write that would store `s:1`. Bounding the writer to released binaries also
# keeps both tests that import this out of the language-test run on the current
# build, where the import would be its own writer.
version = "<3.4.0-a"
reason = """
Setup for 3.00.00_table_view_reads_itself.surql and
3.00.00_table_view_reads_itself_current.surql. The older binary stores a view
`s` that reads its own table besides `src`, and a write to `src` that it
maintains. Every released binary accepts the definition and maintains a view's
rows without maintaining the views over them, so the write stores `s:1` once
and stops.

The view is defined before `src` holds a row: 3.0.0 cannot fill a view over
existing rows whose projection leaves out the record id.
"""
run = false
*/
DEFINE TABLE src SCHEMALESS;
DEFINE TABLE s AS SELECT n + 1 AS n FROM src, s;
CREATE src:1 SET n = 1 RETURN NONE;
