/**
[env]
imports = ["upgrade/define/index_diskann_import.surql"]

[test]
upgrade = true
reason = "DiskAnn vector index (catalog::Index::DiskAnn variant) round-trips through upgrade: definition string and on-disk graph both survive."
# DISKANN syntax was introduced in 3.1.0-beta.3. The `importing-version`
# clause is the upgrade harness's "starting at" gate: a hop only runs when
# its from-version satisfies this requirement (and since adjacent-pair hops
# always have to >= from, the to-version is covered automatically). Until
# 3.1.0-beta.3 is added to `LANG_UPGRADE_VERSIONS`, this test is inert.
importing-version = ">=3.1.0-beta.3"

[[test.results]]
match = '''string::starts_with($result.indexes.dann_pts, "DEFINE INDEX dann_pts ON pts FIELDS point DISKANN DIMENSION 4 DIST EUCLIDEAN TYPE F32")'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: pts:1 }]"

*/
INFO FOR TABLE pts;
LET $pt = [2.0, 3.0, 4.0, 5.0];
-- Single nearest neighbor — pts:1 is closest to [2,3,4,5].
SELECT id FROM pts WHERE point <|1,100|> $pt;
