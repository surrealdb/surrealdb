/**
[env]
imports = ["upgrade/define/index/mtree_import.surql"]

[test]
upgrade = true

[[test.results]]
value = "[{ dist: 2f, id: pts:1 }, { dist: 4f, id: pts:2 }]"

[[test.results]]
value = "[{ dist: 2f, id: pts:1 }, { dist: 4f, id: pts:2 }]"
*/

SELECT id, vector::distance::euclidean(point, [2,3,4,5]) AS dist FROM pts WHERE point <|2|> [2,3,4,5];
SELECT id, vector::distance::euclidean(point, [2,3,4,5]) AS dist FROM pts WHERE point <|2,EUCLIDEAN|> [2,3,4,5];
