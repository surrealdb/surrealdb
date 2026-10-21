/**
[env]
namespace = true
database = true

[test]
reason = "A single record over 4 MiB writes and reads back on the embedded engines. The size ceiling belongs to the gRPC message between the compute node and TiKV, a transport only TiKV has."
issue = 7422

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[5, 50000, 1000000]"
*/

DEFINE TABLE geo SCHEMALESS;
LET $r = string::repeat('x', 1000000);
CREATE geo:1 SET parts = [$r, $r, $r, $r, $r], coords = array::map(<array>0..50000, |$i| [<float>$i + 0.123456789, 1.987654321]) RETURN NONE;
[array::len(geo:1.parts), array::len(geo:1.coords), string::len(geo:1.parts[4])];
