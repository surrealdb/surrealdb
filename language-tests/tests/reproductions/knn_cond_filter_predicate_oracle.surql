/**
[env]
imports = ["reproductions/knn_cond_filter_predicate_oracle_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:alice" }

[test]
reason = "Regression for the ANN cond-filter permission ordering: `HnswTruthyDocumentFilter::is_record_truthy` (and the DiskANN equivalent) fetched each candidate straight from the KV layer and evaluated the caller-supplied WHERE against it BEFORE SELECT permissions were checked, so a record user could probe restricted field values via the cond. The fix in `idx/trees/{hnsw,diskann}/filter.rs::is_record_truthy` applies the table's SELECT permission BEFORE the cond, so hidden rows are skipped pre-cond and the cond is only evaluated against rows the caller is allowed to see. The probe vector [10f] is closest to two hidden rows (pts:2 / pts:3) — neither secret value may influence the visible result. Probe `secret = \"A\"` returns [pts:1] because pts:1 is public AND its own secret legitimately matches (the recall-preserving property that distinguishes this fix from approaches which drop the pushdown entirely). Probe `secret = \"B\"` returns [] because no SELECT-visible row matches."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: pts:1 }]"

[[test.results]]
value = "[]"
*/

LET $q = [10f];
SELECT id FROM pts WHERE point <|2,10|> $q AND secret = "A";
SELECT id FROM pts WHERE point <|2,10|> $q AND secret = "B";
