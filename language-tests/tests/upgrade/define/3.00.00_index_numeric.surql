/**
[env]
imports = ["upgrade/define/3.00.00_index_numeric_import.surql"]

[test]
upgrade = true
reason = """
Numbers are stored in index keys as an order-preserving decimal encoding whose
digits come from converting each Int, Float and Decimal to a decimal
coefficient. Those digits are the persisted format: a reader that converts a
value to different digits misses the old key on lookup, leaves it behind on
update, and lets a duplicate past a UNIQUE index, all without an error. Only a
real old writer can show that the current conversion reproduces the old one.

Each lookup encodes its probe with the reader and must land on the key the old
binary wrote; the ranges cross signs and types, so a change in byte order shows
as wrong membership; the UNIQUE probes collide only if the bytes are identical,
and the final insert succeeds only if the update removed the old key."""

[[test.results]]
value = "[idx_num:1]"

[[test.results]]
value = "[idx_num:2]"

[[test.results]]
value = "[idx_num:3]"

[[test.results]]
value = "[idx_num:4]"

[[test.results]]
value = "[idx_num:5]"

[[test.results]]
value = "[idx_num:6]"

[[test.results]]
value = "[idx_num:8]"

[[test.results]]
value = "[idx_num:14]"

[[test.results]]
value = "[idx_num:15]"

[[test.results]]
value = "[idx_num:13]"

[[test.results]]
value = "[idx_num:9]"

[[test.results]]
value = "[idx_num:10]"

[[test.results]]
value = "[idx_num:11]"

[[test.results]]
value = "[idx_num:12]"

[[test.results]]
value = "[idx_num:1, idx_num:2, idx_num:3, idx_num:12]"

[[test.results]]
value = "[idx_num:9, idx_num:10]"

[[test.results]]
value = "[idx_num:5, idx_num:6]"

[[test.results]]
value = "[idx_num:7, idx_num:8, idx_num:14]"

[[test.results]]
value = "[idx_num:11, idx_num:13]"

[[test.results]]
error = "Database index `idx_numu_v` already contains 34868443281326950000000000000000000000000000000000000000000000000000f, with record `idx_numu:1`"

[[test.results]]
error = "Database index `idx_numu_v` already contains 0.000000000000000000000000000000000000000000000003244745644034682f, with record `idx_numu:2`"

[[test.results]]
error = "Database index `idx_numu_v` already contains -258069325429331200000000000000000000000000000000000000000000000000f, with record `idx_numu:3`"

[[test.results]]
error = "Database index `idx_numu_v` already contains 0.1f, with record `idx_numu:4`"

[[test.results]]
error = "Database index `idx_numu_v` already contains 1.5dec, with record `idx_numu:5`"

[[test.results]]
error = "Database index `idx_numu_v` already contains 7f, with record `idx_numu:6`"

[[test.results]]
error = "Database index `idx_numu_v` already contains -9223372036854775807, with record `idx_numu:7`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/
-- Equality through the index, one probe per kind of value.
SELECT VALUE id FROM idx_num WITH INDEX idx_num_v WHERE v = -1000;
SELECT VALUE id FROM idx_num WITH INDEX idx_num_v WHERE v = -2.5f;
SELECT VALUE id FROM idx_num WITH INDEX idx_num_v WHERE v = -0.5dec;
SELECT VALUE id FROM idx_num WITH INDEX idx_num_v WHERE v = 0;
SELECT VALUE id FROM idx_num WITH INDEX idx_num_v WHERE v = 0.1f;
SELECT VALUE id FROM idx_num WITH INDEX idx_num_v WHERE v = 1.50dec;
SELECT VALUE id FROM idx_num WITH INDEX idx_num_v WHERE v = 1000;
SELECT VALUE id FROM idx_num WITH INDEX idx_num_v WHERE v = 9223372036854775807;
SELECT VALUE id FROM idx_num WITH INDEX idx_num_v WHERE v = 0.0000001421817112187826738850dec;
SELECT VALUE id FROM idx_num WITH INDEX idx_num_v WHERE v = 1e300;
-- The floats whose key digits are not the exactly rounded expansion.
SELECT VALUE id FROM idx_num WITH INDEX idx_num_v WHERE v = 4.578586377376824e-300;
SELECT VALUE id FROM idx_num WITH INDEX idx_num_v WHERE v = 3.244745644034682e-48;
SELECT VALUE id FROM idx_num WITH INDEX idx_num_v WHERE v = 3.486844328132695e67;
SELECT VALUE id FROM idx_num WITH INDEX idx_num_v WHERE v = -2.580693254293312e65;
-- Ranges over the old keys; membership follows from their byte order.
SELECT VALUE id FROM idx_num WITH INDEX idx_num_v WHERE v < 0 ORDER BY id;
SELECT VALUE id FROM idx_num WITH INDEX idx_num_v WHERE v > 0 AND v < 1e-40 ORDER BY id;
SELECT VALUE id FROM idx_num WITH INDEX idx_num_v WHERE v > 0.05 AND v < 2 ORDER BY id;
SELECT VALUE id FROM idx_num WITH INDEX idx_num_v WHERE v >= 3 AND v <= 1e19 ORDER BY id;
SELECT VALUE id FROM idx_num WITH INDEX idx_num_v WHERE v > 1e60 ORDER BY id;
-- Every value the old binary wrote collides with the same value written now,
-- including Float 7.0 against Int 7, which share their encoding.
CREATE idx_numu:11 SET v = 3.486844328132695e67;
CREATE idx_numu:12 SET v = 3.244745644034682e-48;
CREATE idx_numu:13 SET v = -2.580693254293312e65;
CREATE idx_numu:14 SET v = 0.1f;
CREATE idx_numu:15 SET v = 1.50dec;
CREATE idx_numu:16 SET v = 7.0f;
CREATE idx_numu:17 SET v = -9223372036854775807;
-- Updating away from an old value must delete the old key, freeing the value.
UPDATE idx_numu:2 SET v = 1 RETURN NONE;
CREATE idx_numu:18 SET v = 3.244745644034682e-48 RETURN NONE;
