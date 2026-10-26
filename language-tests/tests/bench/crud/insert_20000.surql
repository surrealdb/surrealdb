/**
[env]
imports = ["bench/util/insert-source-20000.surql"]

[bench]
reason = """
Insert 20,000 records with a single INSERT statement, so every row is written
inside one transaction. The per-row cost must not grow with what the
transaction has already written: a cost that does turns the statement
quadratic, which this bench shows as a time that grows faster than the row
count. The rows are read from a table the import fills, so the measured
statement is the INSERT itself rather than the generation of its values.
"""
run = true
rebuild = true
warmup = "2s"
sample-size = 10
measurement-time = "30s"

[test]
run = false
*/

INSERT INTO record (SELECT * OMIT id FROM source) RETURN NONE;
