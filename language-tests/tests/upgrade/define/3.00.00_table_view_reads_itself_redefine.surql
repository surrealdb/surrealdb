/**
[test]
reason = "Repair script for 3.00.00_table_view_reads_itself.surql: the view is defined again over its source alone, which the current checkout can maintain."
run = false
*/
DEFINE TABLE OVERWRITE s AS SELECT n + 1 AS n FROM src;
