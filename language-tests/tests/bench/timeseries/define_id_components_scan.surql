/**
[env]
imports = ["bench/util/readings-array-ids-100000.surql"]

[bench]
reason = "Name the components of the record id of a table that already holds 100k records. The statement checks the key of every record against [city: string, time: datetime] and every body for a stored copy of city or time that differs from its key, inside the defining transaction, so it is a full scan of the table's records. The imported rows have array ids with no names and bodies that carry equal copies of both fields, which pass. Every iteration re-imports the 100k rows before the statement is timed, so a sample costs seconds: few samples on purpose. The scan is storage-bound, so it also runs under --backend rocksdb and --backend surrealkv."
run = true
rebuild = true
rebuild-file-backend = true
warmup = "1s"
sample-size = 5
measurement-time = "60s"
max-time = "300s"

[test]
run = false
*/

DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
