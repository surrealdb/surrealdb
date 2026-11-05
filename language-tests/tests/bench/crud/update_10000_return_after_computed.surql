/**
[env]
imports = ["bench/util/records-10000-computed.surql"]

[bench]
reason = "Update every record of a table with computed fields and return the post-image — the output stage shows each image through the table's field view, which derives the computed fields and so runs a pipeline over a copy of the image. This guards the path that keeps its copy"
run = true
rebuild = true
rebuild-file-backend = true
warmup = "2s"
sample-size = 10
measurement-time = "20s"

[test]
run = false
*/

UPDATE record SET active = true RETURN AFTER;
