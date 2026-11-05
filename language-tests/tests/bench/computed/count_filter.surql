/**
[bench]
reason = "COUNT whose WHERE reads a read-time COMPUTED field, against the same values held in an ordinary stored field — the total cost of making a filtered field computed, which is two things at once: the value is derived for every row, and the predicate stops being answerable from revision-encoded bytes, so the stored arm rejects non-matching rows before decode (EXPLAIN: `pre_decode_filter: yes`) and the computed arm cannot (`no (computed fields)`). `computed/where_computed` + `computed/where_inline` isolate the derivation on its own, both arms there being ineligible for that filter. Both arms count the same rows"
run = true
rebuild = false
datasets = { stored = "bench/util/records-10000-bucket-stored.surql", computed = "bench/util/records-10000-computed.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT count() FROM record WHERE bucket > 5 GROUP ALL;
