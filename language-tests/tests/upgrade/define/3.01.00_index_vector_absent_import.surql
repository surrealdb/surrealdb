/**
[test]
introduced = ">=3.1.0"
reason = """
Setup for 3.01.00_index_vector_absent.surql and its _current sibling: the old
binary defines an HNSW and a DiskANN index over tables whose every record holds
a vector, and over tables with one record that holds none, which the index
stores no entry for. The old binary records no value shapes for any of them."""
run = false
*/
DEFINE INDEX vabs_full_h_ix ON vabs_full_h FIELDS point HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;
DEFINE INDEX vabs_none_h_ix ON vabs_none_h FIELDS point HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;
DEFINE INDEX vabs_full_d_ix ON vabs_full_d FIELDS point DISKANN DIMENSION 2 DIST EUCLIDEAN TYPE F32;
DEFINE INDEX vabs_none_d_ix ON vabs_none_d FIELDS point DISKANN DIMENSION 2 DIST EUCLIDEAN TYPE F32;
CREATE vabs_full_h:1 SET point = [1f, 1f];
CREATE vabs_full_h:2 SET point = [3f, 3f];
CREATE vabs_full_h:3 SET point = [2f, 2f];
CREATE vabs_none_h:1 SET point = [1f, 1f];
CREATE vabs_none_h:2 SET point = [3f, 3f];
CREATE vabs_none_h:3 SET other = 1;
CREATE vabs_full_d:1 SET point = [1f, 1f];
CREATE vabs_full_d:2 SET point = [3f, 3f];
CREATE vabs_full_d:3 SET point = [2f, 2f];
CREATE vabs_none_d:1 SET point = [1f, 1f];
CREATE vabs_none_d:2 SET point = [3f, 3f];
CREATE vabs_none_d:3 SET other = 1;
