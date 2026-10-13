/**
[bench]
reason = "A ~1%-selectivity filtered traversal over a 100000-edge hub — the headline scale for the inline-property claim. The `inline` dataset evaluates `score > 9899` against the adjacency payloads and fetches only the ~1000 matching edge records; the `plain` twin fetches all 100000 to evaluate the same predicate."
run = true
rebuild = false
datasets = { inline = "bench/util/graph-filtered-100k-inline.surql", plain = "bench/util/graph-filtered-100k-plain.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT VALUE ->(knows WHERE score > 9899) FROM person:1;
