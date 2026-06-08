/**
[test]

[[test.results]]
value = "[{ id: pts:3, point: [8, 9, 10, 11] }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The index 'diskann_pts' already exists"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { diskann_pts: 'DEFINE INDEX diskann_pts ON pts FIELDS point DISKANN DIMENSION 4 DIST EUCLIDEAN TYPE F32 DEGREE 8 L_BUILD 20 ALPHA 1.4f HASHED_VECTOR' }, lives: {  }, tables: {  } }"

*/
CREATE pts:3 SET point = [8,9,10,11];
DEFINE INDEX IF NOT EXISTS diskann_pts ON pts FIELDS point DISKANN DIMENSION 4 DIST EUCLIDEAN TYPE F32 DEGREE 8 L_BUILD 20 ALPHA 1.4 HASHED_VECTOR;
DEFINE INDEX diskann_pts ON pts FIELDS point DISKANN DIMENSION 4 DIST EUCLIDEAN TYPE F32 DEGREE 8 L_BUILD 20 ALPHA 1.4 HASHED_VECTOR;
DEFINE INDEX IF NOT EXISTS diskann_pts ON pts FIELDS point DISKANN DIMENSION 4 DIST EUCLIDEAN TYPE F32 DEGREE 8 L_BUILD 20 ALPHA 1.4 HASHED_VECTOR;
REMOVE INDEX diskann_pts ON pts;
DEFINE INDEX diskann_pts ON pts FIELDS point DISKANN DIMENSION 4 DIST EUCLIDEAN TYPE F32 DEGREE 8 L_BUILD 20 ALPHA 1.4 HASHED_VECTOR;
INFO FOR TABLE pts;
