/**
[test]
reason = "Chain setup for index_diskann.surql: creates the DiskANN index on the first walked version that supports the syntax (3.1.0-beta.3 onwards)."
run = false
*/
DEFINE INDEX chain_dann ON chain_pts FIELDS point DISKANN DIMENSION 4 DIST EUCLIDEAN TYPE F32;
