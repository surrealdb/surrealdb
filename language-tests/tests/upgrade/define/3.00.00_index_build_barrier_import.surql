/**
[test]
reason = """
Setup for 3.00.00_index_build_barrier.surql and
3.00.00_index_build_barrier_current.surql: the old binary builds a standard and
a unique index over its rows. A release before the write barrier records their
build state with no proof that the build waited for older write transactions
(`!bw`), and no barrier key at all."""
run = false
*/
DEFINE TABLE ib SCHEMALESS;
DEFINE INDEX ib_f ON TABLE ib COLUMNS f;
DEFINE INDEX ib_u ON TABLE ib COLUMNS u UNIQUE;
CREATE ib:1 SET f = 'a', u = 1, g = 'x';
CREATE ib:2 SET f = 'a', u = 2, g = 'x';
CREATE ib:3 SET f = 'b', u = 3, g = 'x';
CREATE ib:4 SET f = 'a', u = 4, g = 'y';
CREATE ib:5 SET f = 'c', u = 5, g = 'x';
CREATE ib:6 SET f = 'b', u = 6, g = 'y';
