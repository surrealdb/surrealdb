/**
[env]
namespace = true
database = true
# Extra timeout: every scan below reads 3000 records, which splits into one morsel per record when
# the suite forces the smallest morsels, and RocksDB does not finish that in the default timeout
# while the suite runs in parallel.
timeout = 30000

[test]
reason = "rows that tie on a sort key keep the order the scan reads them in, whatever width the scan runs at"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'1108190b7ab09145dc3a6a2286caadd583cec5e78c5ce39f6ac18d47fad13c0d'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'5acf33543e265f386e07ae950f33461beedbdf281b713c1bddd729f62d25f8e9'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'c87aa21905ee006d9e2b7d53c68cd78a95acb86fecf3eaddfdf334ed49a3c5de'"

[[test.results]]
value = "[t:0, t:7, t:14, t:21, t:28, t:35, t:42, t:49, t:56, t:63, t:70, t:77]"

[[test.results]]
value = "[t:1012, t:1019, t:1026, t:1033, t:1040, t:1047, t:1054, t:1061, t:1068, t:1075, t:1082, t:1089]"

[[test.results]]
value = "[t:10, t:12, t:14, t:16, t:18, t:20, t:22, t:24]"

[[test.results]]
value = "[t:33, t:39, t:45, t:51, t:57, t:63, t:69, t:75]"
*/

FOR $i IN 0..3000 { CREATE type::record('t', $i) SET k = $i % 7, b = $i % 2, n = $i };
LET $ids = SELECT VALUE <string> id FROM t ORDER BY k;
crypto::sha256(array::join($ids, ','));
LET $ids = SELECT VALUE <string> id FROM t ORDER BY k DESC;
crypto::sha256(array::join($ids, ','));
LET $ids = SELECT VALUE <string> id FROM t ORDER BY k, b DESC;
crypto::sha256(array::join($ids, ','));
SELECT VALUE id FROM t ORDER BY k LIMIT 12;
SELECT VALUE id FROM t ORDER BY k DESC LIMIT 12 START 1000;
SELECT VALUE id FROM t ORDER BY b LIMIT 8 START 5;
SELECT VALUE id FROM t WHERE n % 3 = 0 ORDER BY b DESC LIMIT 8 START 5;
