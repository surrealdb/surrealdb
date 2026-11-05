/**
[env]
namespace = true
database = true

[test]
reason = "An element filter in the clauses of a write, and in a field's `VALUE`, whose body reads the record through an index argument or a method argument, reads the record the write judges: the statement runs, and a closure in the filter answers what the same filter written without a closure answers. The positions are an UPDATE `WHERE`, `SET` and `RETURN`, a DELETE `WHERE`, an UPSERT `WHERE` and a field `VALUE`; a closure in a write's `WHERE` is declined, so that position holds the filter written without a closure alone."

[[test.results]]
value = "[{ id: uw_cl_i:1, idx: 1, lim: 8 }]"

[[test.results]]
value = "[{ id: uw_cl_i:2, idx: 2, lim: 1 }]"

[[test.results]]
value = "[{ id: us_cl_i:1, idx: 1, lim: 8 }]"

[[test.results]]
value = "[{ id: us_cl_i:2, idx: 2, lim: 1 }]"

[[test.results]]
value = "[{ id: ur_cl_i:1, idx: 1, lim: 8 }]"

[[test.results]]
value = "[{ id: ur_cl_i:2, idx: 2, lim: 1 }]"

[[test.results]]
value = "[{ id: dw_cl_i:1, idx: 1, lim: 8 }]"

[[test.results]]
value = "[{ id: dw_cl_i:2, idx: 2, lim: 1 }]"

[[test.results]]
value = "[{ id: fv_cl_i:1, idx: 1, lim: 8 }]"

[[test.results]]
value = "[{ id: fv_cl_i:2, idx: 2, lim: 1 }]"

[[test.results]]
value = "[{ id: uw_cf_i:1, idx: 1, lim: 8 }]"

[[test.results]]
value = "[{ id: uw_cf_i:2, idx: 2, lim: 1 }]"

[[test.results]]
value = "[{ id: us_cf_i:1, idx: 1, lim: 8 }]"

[[test.results]]
value = "[{ id: us_cf_i:2, idx: 2, lim: 1 }]"

[[test.results]]
value = "[{ id: ur_cf_i:1, idx: 1, lim: 8 }]"

[[test.results]]
value = "[{ id: ur_cf_i:2, idx: 2, lim: 1 }]"

[[test.results]]
value = "[{ id: dw_cf_i:1, idx: 1, lim: 8 }]"

[[test.results]]
value = "[{ id: dw_cf_i:2, idx: 2, lim: 1 }]"

[[test.results]]
value = "[{ id: fv_cf_i:1, idx: 1, lim: 8 }]"

[[test.results]]
value = "[{ id: fv_cf_i:2, idx: 2, lim: 1 }]"

[[test.results]]
value = "[{ id: uw_cl_m:1, idx: 1, lim: 8 }]"

[[test.results]]
value = "[{ id: uw_cl_m:2, idx: 2, lim: 1 }]"

[[test.results]]
value = "[{ id: us_cl_m:1, idx: 1, lim: 8 }]"

[[test.results]]
value = "[{ id: us_cl_m:2, idx: 2, lim: 1 }]"

[[test.results]]
value = "[{ id: ur_cl_m:1, idx: 1, lim: 8 }]"

[[test.results]]
value = "[{ id: ur_cl_m:2, idx: 2, lim: 1 }]"

[[test.results]]
value = "[{ id: dw_cl_m:1, idx: 1, lim: 8 }]"

[[test.results]]
value = "[{ id: dw_cl_m:2, idx: 2, lim: 1 }]"

[[test.results]]
value = "[{ id: pw_cl_m:1, idx: 1, lim: 8 }]"

[[test.results]]
value = "[{ id: pw_cl_m:2, idx: 2, lim: 1 }]"

[[test.results]]
value = "[{ id: fv_cl_m:1, idx: 1, lim: 8 }]"

[[test.results]]
value = "[{ id: fv_cl_m:2, idx: 2, lim: 1 }]"

[[test.results]]
value = "[{ id: uw_cf_m:1, idx: 1, lim: 8 }]"

[[test.results]]
value = "[{ id: uw_cf_m:2, idx: 2, lim: 1 }]"

[[test.results]]
value = "[{ id: us_cf_m:1, idx: 1, lim: 8 }]"

[[test.results]]
value = "[{ id: us_cf_m:2, idx: 2, lim: 1 }]"

[[test.results]]
value = "[{ id: ur_cf_m:1, idx: 1, lim: 8 }]"

[[test.results]]
value = "[{ id: ur_cf_m:2, idx: 2, lim: 1 }]"

[[test.results]]
value = "[{ id: dw_cf_m:1, idx: 1, lim: 8 }]"

[[test.results]]
value = "[{ id: dw_cf_m:2, idx: 2, lim: 1 }]"

[[test.results]]
value = "[{ id: pw_cf_m:1, idx: 1, lim: 8 }]"

[[test.results]]
value = "[{ id: pw_cf_m:2, idx: 2, lim: 1 }]"

[[test.results]]
value = "[{ id: fv_cf_m:1, idx: 1, lim: 8 }]"

[[test.results]]
value = "[{ id: fv_cf_m:2, idx: 2, lim: 1 }]"

[[test.results]]
value = "[[10, 15, 20], []]"

[[test.results]]
value = "[[10, 15, 20], []]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[10, 15, 20], []]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[10, 15, 20], []]"

[[test.results]]
value = "[[10, 15, 20], []]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[10, 15, 20], []]"

[[test.results]]
value = "[[10, 15, 20], [1, 5, 10, 15, 20]]"

[[test.results]]
value = "[[10, 15, 20], [1, 5, 10, 15, 20]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[10, 15, 20], [1, 5, 10, 15, 20]]"

[[test.results]]
value = "[uw_cf_m:2]"

[[test.results]]
value = "[[10, 15, 20], [1, 5, 10, 15, 20]]"

[[test.results]]
value = "[[10, 15, 20], [1, 5, 10, 15, 20]]"

[[test.results]]
value = "[dw_cf_m:2]"

[[test.results]]
value = "[pw_cf_m:2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[10, 15, 20], [1, 5, 10, 15, 20]]"
*/

CREATE uw_cl_i:1 SET idx = 1, lim = 8;
CREATE uw_cl_i:2 SET idx = 2, lim = 1;
CREATE us_cl_i:1 SET idx = 1, lim = 8;
CREATE us_cl_i:2 SET idx = 2, lim = 1;
CREATE ur_cl_i:1 SET idx = 1, lim = 8;
CREATE ur_cl_i:2 SET idx = 2, lim = 1;
CREATE dw_cl_i:1 SET idx = 1, lim = 8;
CREATE dw_cl_i:2 SET idx = 2, lim = 1;
CREATE fv_cl_i:1 SET idx = 1, lim = 8;
CREATE fv_cl_i:2 SET idx = 2, lim = 1;
CREATE uw_cf_i:1 SET idx = 1, lim = 8;
CREATE uw_cf_i:2 SET idx = 2, lim = 1;
CREATE us_cf_i:1 SET idx = 1, lim = 8;
CREATE us_cf_i:2 SET idx = 2, lim = 1;
CREATE ur_cf_i:1 SET idx = 1, lim = 8;
CREATE ur_cf_i:2 SET idx = 2, lim = 1;
CREATE dw_cf_i:1 SET idx = 1, lim = 8;
CREATE dw_cf_i:2 SET idx = 2, lim = 1;
CREATE fv_cf_i:1 SET idx = 1, lim = 8;
CREATE fv_cf_i:2 SET idx = 2, lim = 1;
CREATE uw_cl_m:1 SET idx = 1, lim = 8;
CREATE uw_cl_m:2 SET idx = 2, lim = 1;
CREATE us_cl_m:1 SET idx = 1, lim = 8;
CREATE us_cl_m:2 SET idx = 2, lim = 1;
CREATE ur_cl_m:1 SET idx = 1, lim = 8;
CREATE ur_cl_m:2 SET idx = 2, lim = 1;
CREATE dw_cl_m:1 SET idx = 1, lim = 8;
CREATE dw_cl_m:2 SET idx = 2, lim = 1;
CREATE pw_cl_m:1 SET idx = 1, lim = 8;
CREATE pw_cl_m:2 SET idx = 2, lim = 1;
CREATE fv_cl_m:1 SET idx = 1, lim = 8;
CREATE fv_cl_m:2 SET idx = 2, lim = 1;
CREATE uw_cf_m:1 SET idx = 1, lim = 8;
CREATE uw_cf_m:2 SET idx = 2, lim = 1;
CREATE us_cf_m:1 SET idx = 1, lim = 8;
CREATE us_cf_m:2 SET idx = 2, lim = 1;
CREATE ur_cf_m:1 SET idx = 1, lim = 8;
CREATE ur_cf_m:2 SET idx = 2, lim = 1;
CREATE dw_cf_m:1 SET idx = 1, lim = 8;
CREATE dw_cf_m:2 SET idx = 2, lim = 1;
CREATE pw_cf_m:1 SET idx = 1, lim = 8;
CREATE pw_cf_m:2 SET idx = 2, lim = 1;
CREATE fv_cf_m:1 SET idx = 1, lim = 8;
CREATE fv_cf_m:2 SET idx = 2, lim = 1;
UPDATE us_cl_i SET z = [1, 5, 10, 15, 20][WHERE array::any([$this], |$e| $e >= [0, 8, 100][idx])] RETURN VALUE z;
UPDATE ur_cl_i SET q = 1 RETURN VALUE [1, 5, 10, 15, 20][WHERE array::any([$this], |$e| $e >= [0, 8, 100][idx])];
DEFINE FIELD z ON fv_cl_i VALUE [1, 5, 10, 15, 20][WHERE array::any([$this], |$e| $e >= [0, 8, 100][idx])];
UPDATE fv_cl_i SET q = 1 RETURN VALUE z;
UPDATE uw_cf_i SET q = 1 WHERE [3][WHERE $this >= [0, 8, 100][idx]] != [] RETURN VALUE id;
UPDATE us_cf_i SET z = [1, 5, 10, 15, 20][WHERE $this >= [0, 8, 100][idx]] RETURN VALUE z;
UPDATE ur_cf_i SET q = 1 RETURN VALUE [1, 5, 10, 15, 20][WHERE $this >= [0, 8, 100][idx]];
DELETE dw_cf_i WHERE [3][WHERE $this >= [0, 8, 100][idx]] != [] RETURN VALUE $before.id;
DEFINE FIELD z ON fv_cf_i VALUE [1, 5, 10, 15, 20][WHERE $this >= [0, 8, 100][idx]];
UPDATE fv_cf_i SET q = 1 RETURN VALUE z;
UPDATE us_cl_m SET z = [1, 5, 10, 15, 20][WHERE array::any([$this], |$e| $e >= [0].concat([lim]).max())] RETURN VALUE z;
UPDATE ur_cl_m SET q = 1 RETURN VALUE [1, 5, 10, 15, 20][WHERE array::any([$this], |$e| $e >= [0].concat([lim]).max())];
DEFINE FIELD z ON fv_cl_m VALUE [1, 5, 10, 15, 20][WHERE array::any([$this], |$e| $e >= [0].concat([lim]).max())];
UPDATE fv_cl_m SET q = 1 RETURN VALUE z;
UPDATE uw_cf_m SET q = 1 WHERE [3][WHERE $this >= [0].concat([lim]).max()] != [] RETURN VALUE id;
UPDATE us_cf_m SET z = [1, 5, 10, 15, 20][WHERE $this >= [0].concat([lim]).max()] RETURN VALUE z;
UPDATE ur_cf_m SET q = 1 RETURN VALUE [1, 5, 10, 15, 20][WHERE $this >= [0].concat([lim]).max()];
DELETE dw_cf_m WHERE [3][WHERE $this >= [0].concat([lim]).max()] != [] RETURN VALUE $before.id;
UPSERT pw_cf_m SET q = 1 WHERE [3][WHERE $this >= [0].concat([lim]).max()] != [] RETURN VALUE id;
DEFINE FIELD z ON fv_cf_m VALUE [1, 5, 10, 15, 20][WHERE $this >= [0].concat([lim]).max()];
UPDATE fv_cf_m SET q = 1 RETURN VALUE z;
