/**
[env]
imports = ["upgrade/define/3.00.00_index_numeric_extremes_import.surql"]

[test]
upgrade = true
reason = """
Number index keys are a persisted format, and the values at its edges are the
ones a re-implementation is most likely to encode differently: both zeros, NaN,
the infinities, the largest and smallest floats, the extremes of the integer
and decimal ranges, and decimals whose trailing zeros are part of the key. A
reader that encodes any of them differently misses the old key on lookup,
leaves it behind on update and admits a duplicate past a UNIQUE index, none of
which raises an error by itself.

Every probe is encoded by the reader and has to land on a key the old writer
produced. The ranges and the ORDER BY read keys in byte order, so a change in
ordering shows as wrong membership or a wrong sequence. The compound indexes
check that a number inside a composite key still ends where the next column
begins, and the array record ids check the same encoding in the record key
space. The writes at the end land next to old keys and must see them."""

[[test.results]]
value = "[nx:negzero, nx:zero]"

[[test.results]]
value = "[nx:negzero, nx:zero]"

[[test.results]]
value = "[nx:nan]"

[[test.results]]
value = "[nx:inf]"

[[test.results]]
value = "[nx:neginf]"

[[test.results]]
value = "[nx:fmax]"

[[test.results]]
value = "[nx:fmin]"

[[test.results]]
value = "[nx:tiny]"

[[test.results]]
value = "[nx:negtiny]"

[[test.results]]
value = "[nx:dmax]"

[[test.results]]
value = "[nx:dmin]"

[[test.results]]
value = "[nx:dsmall]"

[[test.results]]
value = "[nx:imin]"

[[test.results]]
value = "[nx:imax]"

[[test.results]]
value = "[nx:one_dec, nx:one_dec_wide, nx:one_float, nx:one_int]"

[[test.results]]
value = "[nx:one_dec, nx:one_dec_wide, nx:one_float, nx:one_int]"

[[test.results]]
value = "[nx:one_dec, nx:one_dec_wide, nx:one_float, nx:one_int]"

[[test.results]]
value = "[nx:one_dec, nx:one_dec_wide, nx:one_float, nx:one_int]"

[[test.results]]
value = "[nx:third]"

[[test.results]]
value = "[nx:third_dec]"

[[test.results]]
value = "[nx:tenth]"

[[test.results]]
value = "[nx:big]"

[[test.results]]
value = "[nx:fmin, nx:neginf]"

[[test.results]]
value = "[nx:negtiny, nx:negzero, nx:tiny, nx:zero]"

[[test.results]]
value = "[nx:one_dec, nx:one_dec_wide, nx:one_float, nx:one_int, nx:tenth, nx:third, nx:third_dec]"

[[test.results]]
value = "[nx:fmax, nx:inf, nx:nan]"

[[test.results]]
value = "[{ id: nx:imin, v: -9223372036854775808 }, { id: nx:dsmall, v: 0.0000000000000000000000000001dec }, { id: nx:tenth, v: 0.1f }, { id: nx:third_dec, v: 0.3333333333333333dec }, { id: nx:third, v: 0.3333333333333333f }, { id: nx:imax, v: 9223372036854775807 }]"

[[test.results]]
value = "[{ id: nx:third, v: 0.3333333333333333f }, { id: nx:third_dec, v: 0.3333333333333333dec }, { id: nx:tenth, v: 0.1f }, { id: nx:dsmall, v: 0.0000000000000000000000000001dec }]"

[[test.results]]
value = "[{ count: 5 }]"

[[test.results]]
value = "[{ count: 11 }]"

[[test.results]]
error = "Database index `nxu_v` already contains -0f, with record `nxu:negzero`"

[[test.results]]
error = "Database index `nxu_v` already contains 0, with record `nxu:negzero`"

[[test.results]]
error = "Database index `nxu_v` already contains NaN, with record `nxu:nan`"

[[test.results]]
error = "Database index `nxu_v` already contains Infinity, with record `nxu:inf`"

[[test.results]]
error = "Database index `nxu_v` already contains -Infinity, with record `nxu:neginf`"

[[test.results]]
error = "Database index `nxu_v` already contains 179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000f, with record `nxu:fmax`"

[[test.results]]
error = "Database index `nxu_v` already contains 0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005f, with record `nxu:tiny`"

[[test.results]]
error = "Database index `nxu_v` already contains 79228162514264337593543950335dec, with record `nxu:dmax`"

[[test.results]]
error = "Database index `nxu_v` already contains -9223372036854775808, with record `nxu:imin`"

[[test.results]]
error = "Database index `nxu_v` already contains 1dec, with record `nxu:one_dec`"

[[test.results]]
error = "Database index `nxu_v` already contains 0.1f, with record `nxu:tenth`"

[[test.results]]
error = "Database index `nxu_v` already contains 1, with record `nxu:one_dec`"

[[test.results]]
error = "Database index `nxu_v` already contains 1f, with record `nxu:one_dec`"

[[test.results]]
value = "[nxc:1]"

[[test.results]]
value = "[nxc:3]"

[[test.results]]
value = "[nxc:1, nxc:2]"

[[test.results]]
value = "[nxc:6]"

[[test.results]]
value = "[nxc:7]"

[[test.results]]
value = "[nxc:1, nxc:2, nxc:3, nxc:5]"

[[test.results]]
value = "[nxc:4]"

[[test.results]]
value = "[nxcu:2]"

[[test.results]]
value = "[nxcu:4]"

[[test.results]]
value = "[nxcu:5]"

[[test.results]]
error = "Database index `nxcu_ab` already contains [1.5dec, 2], with record `nxcu:1`"

[[test.results]]
error = "Database index `nxcu_ab` already contains [-2.25f, 0.1f], with record `nxcu:3`"

[[test.results]]
error = "Database index `nxcu_ab` already contains [3, 34868443281326950000000000000000000000000000000000000000000000000000f], with record `nxcu:4`"

[[test.results]]
error = "Database index `nxcu_ab` already contains [3f, -258069325429331200000000000000000000000000000000000000000000000000f], with record `nxcu:5`"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[2]"

[[test.results]]
value = "[4]"

[[test.results]]
value = "[5]"

[[test.results]]
value = "[6]"

[[test.results]]
value = "[1, 3]"

[[test.results]]
value = "[2]"

[[test.results]]
value = "[4, 5]"

[[test.results]]
value = "[7]"

[[test.results]]
value = "[8]"

[[test.results]]
error = "Database record `nxr:[1.5f, 2dec]` already exists"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[nx:negzero, nx:new_negzero, nx:zero]"

[[test.results]]
value = "[nx:new_one, nx:one_dec, nx:one_dec_wide, nx:one_float, nx:one_int]"

[[test.results]]
value = "[nx:new_third, nx:third]"

[[test.results]]
value = "[nx:fmax, nx:new_fmax]"

[[test.results]]
value = "[{ id: nx:new_neg, v: -3 }, { id: nx:dsmall, v: 0.0000000000000000000000000001dec }, { id: nx:tenth, v: 0.1f }, { id: nx:third_dec, v: 0.3333333333333333dec }, { id: nx:new_third, v: 0.3333333333333333f }, { id: nx:third, v: 0.3333333333333333f }, { id: nx:new_mid, v: 0.5dec }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[nx:new_fmax]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[nx:negzero, nx:new_negzero]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/
-- Zero: both signs share a key, so one probe finds both.
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v = 0 ORDER BY id;
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v = -0.0f ORDER BY id;
-- Non-finite floats.
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v = <float> "NaN";
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v = math::inf;
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v = math::neg_inf;
-- Extreme floats.
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v = 1.7976931348623157e308;
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v = -1.7976931348623157e308;
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v = 5e-324;
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v = -5e-324;
-- Extreme decimals and integers.
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v = 79228162514264337593543950335dec;
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v = -79228162514264337593543950335dec;
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v = 0.0000000000000000000000000001dec;
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v = -9223372036854775808;
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v = 9223372036854775807;
-- One, written as an int, a float and decimals with different trailing zeros.
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v = 1 ORDER BY id;
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v = 1.0f ORDER BY id;
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v = 1.0dec ORDER BY id;
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v = 1.000dec ORDER BY id;
-- Values whose binary and decimal expansions differ.
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v = 0.3333333333333333f ORDER BY id;
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v = 0.3333333333333333dec ORDER BY id;
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v = 0.1f;
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v = 123456789012345678901234567890.123456789f;
-- Ranges across kinds, including the open ends.
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v < -1e300 ORDER BY id;
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v > -1e-300 AND v < 1e-300 ORDER BY id;
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v >= 0.1 AND v <= 1 ORDER BY id;
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v > 1e30 ORDER BY id;
-- The index orders the keys: ascending and descending over distinct values.
SELECT id, v FROM nx WITH INDEX nx_v WHERE v > -1e19 AND v < 1e19 AND (v < -1e-100 OR v > 1e-100) AND v != 1 AND v != 1.0f AND v != 1.0dec AND v != 1.000dec ORDER BY v, id;
SELECT id, v FROM nx WITH INDEX nx_v WHERE v > 1e-100 AND v < 1 ORDER BY v DESC, id;
-- Counts.
SELECT count() FROM nx WHERE v < 0 GROUP ALL;
SELECT count() FROM nx WHERE v > 1e-300 AND v < 1e300 GROUP ALL;
-- UNIQUE: an old key collides with the same value written now.
CREATE nxu:n1 SET v = -0.0f;
CREATE nxu:n2 SET v = 0;
CREATE nxu:n3 SET v = <float> "NaN";
CREATE nxu:n4 SET v = math::inf;
CREATE nxu:n5 SET v = math::neg_inf;
CREATE nxu:n6 SET v = 1.7976931348623157e308;
CREATE nxu:n7 SET v = 5e-324;
CREATE nxu:n8 SET v = 79228162514264337593543950335dec;
CREATE nxu:n9 SET v = -9223372036854775808;
CREATE nxu:n10 SET v = 1.0dec;
CREATE nxu:n11 SET v = 0.1f;
-- A different spelling of the same number is a different key only where the
-- trailing zeros differ.
CREATE nxu:m1 SET v = 1;
CREATE nxu:m2 SET v = 1.0f;
-- Compound indexes: equality on both columns, equality on the leading column
-- with a range on the second, and a range on the leading column.
SELECT VALUE id FROM nxc WITH INDEX nxc_ab WHERE a = 1 AND b = 0.5 ORDER BY id;
SELECT VALUE id FROM nxc WITH INDEX nxc_ab WHERE a = 1 AND b = 2.5dec ORDER BY id;
SELECT VALUE id FROM nxc WITH INDEX nxc_ab WHERE a = 1 AND b < 1 ORDER BY id;
SELECT VALUE id FROM nxc WITH INDEX nxc_ab WHERE a = 0 AND b > 1e100 ORDER BY id;
SELECT VALUE id FROM nxc WITH INDEX nxc_ab WHERE a = -0.0f AND b = 1e-300 ORDER BY id;
SELECT VALUE id FROM nxc WITH INDEX nxc_ab WHERE a > 0.5 AND a < 3 ORDER BY id;
SELECT VALUE id FROM nxc WITH INDEX nxc_ab WHERE a < 0 ORDER BY id;
SELECT VALUE id FROM nxcu WITH INDEX nxcu_ab WHERE a = 1.5dec AND b = 3 ORDER BY id;
SELECT VALUE id FROM nxcu WITH INDEX nxcu_ab WHERE a = 3 AND b = 3.486844328132695e67 ORDER BY id;
SELECT VALUE id FROM nxcu WITH INDEX nxcu_ab WHERE a = 3 AND b = -2.580693254293312e65 ORDER BY id;
CREATE nxcu:n1 SET a = 1.5dec, b = 2;
CREATE nxcu:n2 SET a = -2.25f, b = 0.1f;
CREATE nxcu:n3 SET a = 3, b = 3.486844328132695e67;
CREATE nxcu:n4 SET a = 3.0f, b = -2.580693254293312e65;
-- Array record ids carry the same encoding in the record key.
SELECT VALUE n FROM nxr:[1.5f, 2dec];
SELECT VALUE n FROM nxr:[-1.5f, 2dec];
SELECT VALUE n FROM nxr:[3, 0.1f];
SELECT VALUE n FROM nxr:[100000000000000000000dec, 1];
SELECT VALUE n FROM nxr:[0.25f, 1e-300];
SELECT VALUE n FROM nxr:[1f, 0]..[2f, 0] ORDER BY n;
SELECT VALUE n FROM nxr:[-2f, 0]..=[0f, 0] ORDER BY n;
SELECT VALUE n FROM nxr:[2f]..[1e30] ORDER BY n;
SELECT VALUE n FROM nxr:7;
SELECT VALUE n FROM nxr:-7;
CREATE nxr:[1.5f, 2dec] SET n = 99;
-- Writes made now sit among the old keys.
CREATE nx:new_negzero SET v = 0.0f RETURN NONE;
CREATE nx:new_one SET v = 1.0f RETURN NONE;
CREATE nx:new_third SET v = 0.3333333333333333f RETURN NONE;
CREATE nx:new_fmax SET v = 1.7976931348623157e308 RETURN NONE;
CREATE nx:new_mid SET v = 0.5dec RETURN NONE;
CREATE nx:new_neg SET v = -3 RETURN NONE;
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v = 0 ORDER BY id;
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v = 1 ORDER BY id;
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v = 0.3333333333333333f ORDER BY id;
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v = 1.7976931348623157e308 ORDER BY id;
SELECT id, v FROM nx WITH INDEX nx_v WHERE v > -5 AND v < 0.7 AND (v < -1e-100 OR v > 1e-100) ORDER BY v, id;
-- Updating and deleting an old record removes its old key.
UPDATE nx:fmax SET v = 2 RETURN NONE;
DELETE nx:neginf RETURN NONE;
DELETE nx:zero RETURN NONE;
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v = 1.7976931348623157e308 ORDER BY id;
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v = math::neg_inf;
SELECT VALUE id FROM nx WITH INDEX nx_v WHERE v = 0 ORDER BY id;
DELETE nxu:negzero RETURN NONE;
UPDATE nxu:imin SET v = 9 RETURN NONE;
CREATE nxu:free1 SET v = 0 RETURN NONE;
CREATE nxu:free2 SET v = -9223372036854775808 RETURN NONE;
