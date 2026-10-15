/**
[env]
imports = ["bench/util/records-10000.surql"]

[bench]
reason = "Update every record of a table with no field view and return the changes — the output stage shows both images, and with no view to run through takes each out of the record it holds rather than copying it"
run = true
rebuild = true
rebuild-file-backend = true
warmup = "2s"
sample-size = 10
measurement-time = "20s"

[test]
run = false
*/

UPDATE record SET active = true RETURN DIFF;
